package queryjob

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ─── Exchange / queue / routing-key constants ────────────────────────────────

// Main topology — must never be modified; changing existing queue arguments
// after the queue has been declared causes PRECONDITION_FAILED.
const (
	mqExchange   = "askdb.events"
	mqQueue      = "askdb.query.execution"
	mqRoutingKey = "query.execution.requested"
)

// Retry topology — new in Phase 7. TTL on askdb.query.retry causes messages to
// expire and be re-routed via DLX back to the existing main exchange/queue.
const (
	mqRetryExchange   = "askdb.retry"
	mqRetryQueue      = "askdb.query.retry"
	mqRetryRoutingKey = "query.execution.retry"

	mqDLQExchange   = "askdb.dlq"
	mqDLQQueue      = "askdb.query.dlq"
	mqDLQRoutingKey = "query.execution.dlq"
)

// ─── Interfaces ──────────────────────────────────────────────────────────────

// Publisher publishes query job messages to the message broker.
type Publisher interface {
	Publish(ctx context.Context, jobID uint64) error
	Close() error
}

// RetryPublisher publishes retry and dead-letter messages from the worker.
// Both methods use Publisher Confirms with mandatory=true; the caller may only
// ACK the original message after a successful return.
type RetryPublisher interface {
	// PublishRetry sends a message to the fixed-TTL retry queue.
	// attempt is the x-attempt header value to embed (already incremented).
	PublishRetry(ctx context.Context, jobID uint64, messageID string, attempt int) error
	// PublishDLQ sends a message to the dead-letter queue.
	PublishDLQ(ctx context.Context, jobID uint64, messageID string, attempt int) error
	Close() error
}

// ─── internal confirm publisher ──────────────────────────────────────────────

// confirmPub wraps a single AMQP channel in confirm mode and serialises all
// publishes via a mutex. It detects both broker NACKs and Basic.Return.
type confirmPub struct {
	ch      *amqp.Channel
	mu      sync.Mutex
	timeout time.Duration
	returns <-chan amqp.Return
	once    sync.Once
}

func newConfirmPub(ch *amqp.Channel, timeout time.Duration) (*confirmPub, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("confirm pub: enable confirm mode: %w", err)
	}
	ret := ch.NotifyReturn(make(chan amqp.Return, 1))
	return &confirmPub{ch: ch, timeout: timeout, returns: ret}, nil
}

// publish sends to exchange/routingKey with mandatory=true, then waits for
// broker ACK. Returns an error on broker NACK, Basic.Return, or timeout.
func (p *confirmPub) publish(ctx context.Context, exchange, routingKey string, pub amqp.Publishing) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, pub)
	if err != nil {
		return fmt.Errorf("confirm pub: publish: %w", err)
	}

	timer := time.NewTimer(p.timeout)
	defer timer.Stop()

	select {
	case ret := <-p.returns:
		return fmt.Errorf("confirm pub: message returned reply_code=%d: %s", ret.ReplyCode, ret.ReplyText)
	case <-conf.Done():
		if !conf.Acked() {
			return fmt.Errorf("confirm pub: broker nacked")
		}
		// Drain a simultaneous return that may have arrived before Done fired.
		select {
		case ret := <-p.returns:
			return fmt.Errorf("confirm pub: message returned reply_code=%d: %s", ret.ReplyCode, ret.ReplyText)
		default:
			return nil
		}
	case <-timer.C:
		return fmt.Errorf("confirm pub: confirm timeout after %v", p.timeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *confirmPub) close() error {
	var err error
	p.once.Do(func() {
		if e := p.ch.Close(); e != nil {
			err = fmt.Errorf("confirm pub: close: %w", e)
		}
	})
	return err
}

// ─── Main (API) publisher ────────────────────────────────────────────────────

// RabbitMQPublisher sends query job messages to RabbitMQ using a dedicated
// confirm-mode channel. It is safe for concurrent use.
type RabbitMQPublisher struct {
	cp *confirmPub
}

// NewRabbitMQPublisher declares the main topology, puts the channel into
// confirm mode, and returns a publisher ready for use. confirmTimeout is the
// maximum time to wait for a broker ACK before treating the publish as failed.
func NewRabbitMQPublisher(ch *amqp.Channel, confirmTimeout time.Duration) (*RabbitMQPublisher, error) {
	if err := declareMQTopology(ch); err != nil {
		return nil, fmt.Errorf("publisher: declare topology: %w", err)
	}
	cp, err := newConfirmPub(ch, confirmTimeout)
	if err != nil {
		return nil, fmt.Errorf("publisher: %w", err)
	}
	return &RabbitMQPublisher{cp: cp}, nil
}

// Publish serialises a QueryJobMessage and sends it to the exchange with
// mandatory=true. It blocks until the broker ACKs or the confirm times out.
// No sensitive data (question, SQL, DSN, credentials) is included.
func (p *RabbitMQPublisher) Publish(ctx context.Context, jobID uint64) error {
	msg := QueryJobMessage{
		MessageID:  newMessageID(),
		Type:       MessageTypeQueryExecutionRequested,
		Version:    messageVersion,
		OccurredAt: time.Now().UTC(),
		Payload:    JobPayload{JobID: jobID},
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("publisher: marshal: %w", err)
	}

	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.MessageID,
		Type:         msg.Type,
		Timestamp:    msg.OccurredAt,
		Body:         body,
		// No x-attempt header: initial publish is always attempt 0.
	}

	if err := p.cp.publish(ctx, mqExchange, mqRoutingKey, pub); err != nil {
		return fmt.Errorf("publisher: publish job_id=%d: %w", jobID, err)
	}
	return nil
}

// Close closes the underlying AMQP channel. It is idempotent.
func (p *RabbitMQPublisher) Close() error {
	return p.cp.close()
}

// ─── Retry / DLQ publisher (worker) ─────────────────────────────────────────

// RabbitMQRetryPublisher publishes retry and dead-letter messages from the
// worker. It uses a dedicated confirm-mode channel.
type RabbitMQRetryPublisher struct {
	cp *confirmPub
}

// NewRabbitMQRetryPublisher declares the retry and DLQ topology and returns a
// publisher ready for use. confirmTimeout is the maximum time to wait for a
// broker ACK. The retry queue is configured with a fixed TTL via x-message-ttl;
// expired messages are re-routed to the main exchange/queue via DLX.
func NewRabbitMQRetryPublisher(ch *amqp.Channel, retryDelayMs int64, confirmTimeout time.Duration) (*RabbitMQRetryPublisher, error) {
	if err := declareRetryTopology(ch, retryDelayMs); err != nil {
		return nil, fmt.Errorf("retry publisher: declare topology: %w", err)
	}
	cp, err := newConfirmPub(ch, confirmTimeout)
	if err != nil {
		return nil, fmt.Errorf("retry publisher: %w", err)
	}
	return &RabbitMQRetryPublisher{cp: cp}, nil
}

// PublishRetry sends the message to the fixed-TTL retry queue.
// attempt is the already-incremented retry count stored as x-attempt.
func (p *RabbitMQRetryPublisher) PublishRetry(ctx context.Context, jobID uint64, messageID string, attempt int) error {
	pub := buildWorkerPublishing(jobID, messageID, attempt)
	if err := p.cp.publish(ctx, mqRetryExchange, mqRetryRoutingKey, pub); err != nil {
		return fmt.Errorf("retry publisher: publish retry job_id=%d: %w", jobID, err)
	}
	return nil
}

// PublishDLQ sends the message to the dead-letter queue.
func (p *RabbitMQRetryPublisher) PublishDLQ(ctx context.Context, jobID uint64, messageID string, attempt int) error {
	pub := buildWorkerPublishing(jobID, messageID, attempt)
	if err := p.cp.publish(ctx, mqDLQExchange, mqDLQRoutingKey, pub); err != nil {
		return fmt.Errorf("retry publisher: publish dlq job_id=%d: %w", jobID, err)
	}
	return nil
}

// Close closes the underlying AMQP channel. It is idempotent.
func (p *RabbitMQRetryPublisher) Close() error {
	return p.cp.close()
}

// ─── topology declarations ───────────────────────────────────────────────────

// declareMQTopology idempotently declares the main exchange, queue, and binding.
// These parameters must never change after initial declaration.
func declareMQTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		mqExchange, "direct",
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("exchange declare: %w", err)
	}

	if _, err := ch.QueueDeclare(
		mqQueue,
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}

	if err := ch.QueueBind(mqQueue, mqRoutingKey, mqExchange, false, nil); err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}
	return nil
}

// declareRetryTopology idempotently declares the retry and DLQ exchanges/queues.
// The retry queue uses x-message-ttl for fixed-delay backoff: on expiry, the DLX
// routes messages back to the main exchange/queue without modifying main's args.
func declareRetryTopology(ch *amqp.Channel, retryDelayMs int64) error {
	// Retry exchange and queue.
	if err := ch.ExchangeDeclare(
		mqRetryExchange, "direct",
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("retry exchange declare: %w", err)
	}
	retryArgs := amqp.Table{
		"x-message-ttl":             retryDelayMs,
		"x-dead-letter-exchange":    mqExchange,
		"x-dead-letter-routing-key": mqRoutingKey,
	}
	if _, err := ch.QueueDeclare(
		mqRetryQueue,
		true, false, false, false, retryArgs,
	); err != nil {
		return fmt.Errorf("retry queue declare: %w", err)
	}
	if err := ch.QueueBind(mqRetryQueue, mqRetryRoutingKey, mqRetryExchange, false, nil); err != nil {
		return fmt.Errorf("retry queue bind: %w", err)
	}

	// DLQ exchange and queue.
	if err := ch.ExchangeDeclare(
		mqDLQExchange, "direct",
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("dlq exchange declare: %w", err)
	}
	if _, err := ch.QueueDeclare(
		mqDLQQueue,
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("dlq queue declare: %w", err)
	}
	if err := ch.QueueBind(mqDLQQueue, mqDLQRoutingKey, mqDLQExchange, false, nil); err != nil {
		return fmt.Errorf("dlq queue bind: %w", err)
	}
	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// buildWorkerPublishing builds an amqp.Publishing for retry/DLQ messages.
// The original message_id is preserved; only the x-attempt header changes.
func buildWorkerPublishing(jobID uint64, messageID string, attempt int) amqp.Publishing {
	// Reconstruct the original body (job_id only; no sensitive data).
	msg := QueryJobMessage{
		MessageID:  messageID,
		Type:       MessageTypeQueryExecutionRequested,
		Version:    messageVersion,
		OccurredAt: time.Now().UTC(),
		Payload:    JobPayload{JobID: jobID},
	}
	body, _ := json.Marshal(msg) // QueryJobMessage is always marshallable
	return amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Type:         msg.Type,
		Timestamp:    msg.OccurredAt,
		Headers:      amqp.Table{HeaderAttempt: int32(attempt)},
		Body:         body,
	}
}

// newMessageID returns a random 16-byte hex string for use as a unique
// message identifier. crypto/rand is used; no UUID dependency needed.
func newMessageID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", b)
}
