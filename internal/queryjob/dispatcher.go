package queryjob

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DispatcherConfig holds tuning parameters for the Outbox Dispatcher.
// All durations must be positive; batchSize must be >= 1.
type DispatcherConfig struct {
	// PollInterval is how often the dispatcher wakes to claim and publish events.
	PollInterval time.Duration
	// BatchSize is the maximum number of events claimed per poll cycle.
	BatchSize int
	// LeaseTTL is how long a claimed event is held before another instance may
	// take it over. Must be > PollInterval.
	LeaseTTL time.Duration
	// BaseBackoff is the initial retry delay after a publish failure.
	BaseBackoff time.Duration
	// MaxBackoff caps the exponential retry delay.
	MaxBackoff time.Duration
	// PublishedRetain is how long published events are kept before cleanup.
	PublishedRetain time.Duration
	// CleanBatch is the maximum number of old published events deleted per cycle.
	CleanBatch int
	// ConfirmTimeout is forwarded to the publisher channel.
	ConfirmTimeout time.Duration
}

// Dispatcher polls outbox_events, publishes each pending event to RabbitMQ using
// Publisher Confirms, and marks them published. It runs inside the API process
// as a background goroutine. Multiple API instances may run concurrently; the
// SELECT FOR UPDATE SKIP LOCKED claim prevents double-publishing.
type Dispatcher struct {
	outbox OutboxRepository
	mqURL  string
	cfg    DispatcherConfig

	mu   sync.Mutex
	pub  *RabbitMQPublisher // nil when RabbitMQ is unavailable
	conn *amqp.Connection   // underlying connection for the publisher

	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

// NewDispatcher creates a Dispatcher. RabbitMQ connectivity is established lazily;
// the API can start and accept requests even when RabbitMQ is unreachable.
func NewDispatcher(outbox OutboxRepository, mqURL string, cfg DispatcherConfig) *Dispatcher {
	return &Dispatcher{
		outbox: outbox,
		mqURL:  mqURL,
		cfg:    cfg,
		stop:   make(chan struct{}),
	}
}

// Start begins the dispatch loop in a background goroutine.
func (d *Dispatcher) Start() {
	d.wg.Add(1)
	go d.loop()
}

// Stop signals the dispatch loop to stop, waits up to the given timeout for
// in-flight publishing to complete, and releases any claimed-but-unpublished
// events back to pending.
func (d *Dispatcher) Stop(ctx context.Context) {
	d.once.Do(func() { close(d.stop) })

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("dispatcher: shutdown timed out; unclaimed events will be reclaimed via lease expiry")
	}

	d.mu.Lock()
	if d.pub != nil {
		_ = d.pub.Close()
		d.pub = nil
	}
	if d.conn != nil && !d.conn.IsClosed() {
		_ = d.conn.Close()
		d.conn = nil
	}
	d.mu.Unlock()
}

func (d *Dispatcher) loop() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			d.runOnce(context.Background())
		}
	}
}

// runOnce performs one claim-publish-cleanup cycle.
func (d *Dispatcher) runOnce(ctx context.Context) {
	now := time.Now().UTC()

	events, err := d.outbox.ClaimBatch(ctx, d.cfg.BatchSize, d.cfg.LeaseTTL, now)
	if err != nil {
		slog.Error("dispatcher: claim batch failed", "err", err)
		return
	}
	if len(events) == 0 {
		// No events ready; run cleanup pass on published events.
		d.cleanOld(ctx)
		return
	}

	// Ensure we have a working publisher before entering the publish loop.
	pub, err := d.ensurePublisher()
	if err != nil {
		slog.Error("dispatcher: rabbitmq unavailable, releasing claimed events", "err", err)
		for _, e := range events {
			d.releaseEvent(ctx, e, "rabbitmq unavailable: "+err.Error())
		}
		return
	}

	for _, e := range events {
		select {
		case <-d.stop:
			// Graceful shutdown: release remaining events.
			d.releaseEvent(ctx, e, "dispatcher stopping")
			continue
		default:
		}

		token := ""
		if e.LeaseToken != nil {
			token = *e.LeaseToken
		}

		publishErr := pub.publishOutbox(ctx, e)
		if publishErr != nil {
			slog.Error("dispatcher: publish failed", "event_id", e.ID, "job_id", e.JobID, "err", publishErr)
			// Invalidate publisher on channel-level errors; reconnect next cycle.
			d.invalidatePublisher()
			d.releaseEvent(ctx, e, publishErr.Error())
			continue
		}

		// Confirm succeeded: CAS mark published.
		if markErr := d.outbox.MarkPublished(ctx, e.ID, token, time.Now().UTC()); markErr != nil {
			// Lease may have been taken over by another instance. Log and move on;
			// at-least-once semantics allow duplicate publishes caught by processed_messages.
			slog.Warn("dispatcher: mark published failed (possible duplicate publish)",
				"event_id", e.ID, "job_id", e.JobID, "err", markErr)
		}
	}

	d.cleanOld(ctx)
}

// releaseEvent reverts a claimed event to pending with incremented backoff.
func (d *Dispatcher) releaseEvent(ctx context.Context, e OutboxEvent, reason string) {
	token := ""
	if e.LeaseToken != nil {
		token = *e.LeaseToken
	}
	next := d.backoffFor(e.AttemptCount + 1)
	nextAt := time.Now().UTC().Add(next)

	if err := d.outbox.ReleaseWithRetry(ctx, e.ID, token, e.AttemptCount+1, nextAt, reason); err != nil {
		slog.Error("dispatcher: release retry failed; lease expiry will reclaim",
			"event_id", e.ID, "err", err)
	}
}

// backoffFor computes capped exponential backoff for the given attempt number.
func (d *Dispatcher) backoffFor(attempt int) time.Duration {
	if attempt <= 0 {
		return d.cfg.BaseBackoff
	}
	// 2^(attempt-1) * BaseBackoff, capped at MaxBackoff.
	exp := math.Pow(2, float64(attempt-1))
	dur := time.Duration(float64(d.cfg.BaseBackoff) * exp)
	if dur <= 0 || dur > d.cfg.MaxBackoff { // overflow guard
		return d.cfg.MaxBackoff
	}
	return dur
}

// cleanOld batch-deletes old published events.
func (d *Dispatcher) cleanOld(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-d.cfg.PublishedRetain)
	n, err := d.outbox.DeleteOldPublished(ctx, cutoff, d.cfg.CleanBatch)
	if err != nil {
		slog.Error("dispatcher: clean old published failed", "err", err)
		return
	}
	if n > 0 {
		slog.Info("dispatcher: cleaned published events", "count", n)
	}
}

// ensurePublisher returns the cached publisher, or creates a new one if the
// connection is closed. RabbitMQ reconnect happens here so the API continues
// accepting requests even when the broker is temporarily unavailable.
func (d *Dispatcher) ensurePublisher() (*RabbitMQPublisher, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.pub != nil && d.conn != nil && !d.conn.IsClosed() {
		return d.pub, nil
	}

	// Close stale resources before reconnecting.
	if d.pub != nil {
		_ = d.pub.Close()
		d.pub = nil
	}
	if d.conn != nil && !d.conn.IsClosed() {
		_ = d.conn.Close()
	}
	d.conn = nil

	conn, err := amqp.Dial(d.mqURL)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dispatcher: open channel: %w", err)
	}
	pub, err := NewRabbitMQPublisher(ch, d.cfg.ConfirmTimeout)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("dispatcher: new publisher: %w", err)
	}
	d.conn = conn
	d.pub = pub
	slog.Info("dispatcher: rabbitmq connected")
	return pub, nil
}

// invalidatePublisher marks the current publisher as unusable so ensurePublisher
// will reconnect on the next cycle.
func (d *Dispatcher) invalidatePublisher() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pub != nil {
		_ = d.pub.Close()
		d.pub = nil
	}
}

// publishOutbox builds a stable AMQP message from an outbox event (reusing the
// stored message_id and occurred_at) and publishes it with Publisher Confirms.
func (p *RabbitMQPublisher) publishOutbox(ctx context.Context, e OutboxEvent) error {
	msg := QueryJobMessage{
		MessageID:  e.MessageID,
		Type:       e.EventType,
		Version:    e.Version,
		OccurredAt: e.OccurredAt,
		Payload:    JobPayload{JobID: e.JobID},
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("dispatcher: marshal outbox event id=%d: %w", e.ID, err)
	}

	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.MessageID,
		Type:         msg.Type,
		Timestamp:    msg.OccurredAt,
		Body:         body,
	}
	if err := p.cp.publish(ctx, mqExchange, mqRoutingKey, pub); err != nil {
		return fmt.Errorf("dispatcher: publish outbox event id=%d job_id=%d: %w", e.ID, e.JobID, err)
	}
	return nil
}
