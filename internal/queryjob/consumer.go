package queryjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const consumerTag = "worker-query-consumer"

// deliveryOutcome instructs the run loop how to respond to a delivery.
type deliveryOutcome int

const (
	outcomeAck         deliveryOutcome = iota // ACK: terminal state persisted
	outcomeNackRequeue                        // NACK with requeue=true
	outcomeFatal                              // close channel (broker requeues); stop consumer
)

// Consumer subscribes to the query execution queue and delegates each message
// to a ProcessService. It is safe to call Stop concurrently with Start.
type Consumer struct {
	ch       *amqp.Channel
	svc      ProcessService
	retryPub RetryPublisher
	pmRepo   ProcessedMessageRepository
	wg       sync.WaitGroup
	once     sync.Once
}

// NewConsumer declares the MQ topology on ch and returns a Consumer ready to
// be started. ch must be a dedicated channel; it must not be shared with the
// publisher or health check. retryPub is used to defer messages when a lease
// is held and to DLQ invalid deliveries.
func NewConsumer(ch *amqp.Channel, svc ProcessService, retryPub RetryPublisher, pmRepo ProcessedMessageRepository) (*Consumer, error) {
	if err := declareMQTopology(ch); err != nil {
		return nil, fmt.Errorf("consumer: declare topology: %w", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("consumer: set qos: %w", err)
	}
	return &Consumer{ch: ch, svc: svc, retryPub: retryPub, pmRepo: pmRepo}, nil
}

// Start begins consuming messages in a background goroutine.
func (c *Consumer) Start() error {
	deliveries, err := c.ch.Consume(
		mqQueue, consumerTag,
		false, // autoAck
		false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("consumer: start consume: %w", err)
	}

	go func() {
		for d := range deliveries {
			c.wg.Add(1)
			outcome, err := c.handle(d)
			c.wg.Done()

			switch outcome {
			case outcomeAck:
				if ackErr := d.Ack(false); ackErr != nil {
					slog.Error("consumer: ack failed", "err", ackErr)
				}
			case outcomeNackRequeue:
				if nackErr := d.Nack(false, true); nackErr != nil {
					slog.Error("consumer: nack requeue failed", "err", nackErr)
				}
			case outcomeFatal:
				slog.Error("consumer: fatal error, closing channel", "err", err)
				c.once.Do(func() { c.ch.Close() })
				return
			}
		}
		slog.Info("consumer: delivery channel closed, exiting")
	}()

	return nil
}

// Stop cancels the consumer subscription, waits for in-flight work, then
// closes the channel. It is idempotent.
func (c *Consumer) Stop() {
	c.once.Do(func() {
		if err := c.ch.Cancel(consumerTag, false); err != nil {
			slog.Warn("consumer: cancel failed", "err", err)
		}
		c.wg.Wait()
		if err := c.ch.Close(); err != nil {
			slog.Warn("consumer: close channel failed", "err", err)
		}
	})
}

// handle processes a single delivery and returns the appropriate outcome.
// It never calls Ack/Nack itself; the caller is responsible.
func (c *Consumer) handle(d amqp.Delivery) (deliveryOutcome, error) {
	// ── 1. Parse envelope ────────────────────────────────────────────────────
	var msg QueryJobMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		slog.Error("consumer: malformed message body", "err", err)
		return c.dlqDirect(context.Background(), "invalid-id", 0, 0, "malformed JSON body")
	}

	// ── 2. Validate type / version ───────────────────────────────────────────
	if msg.Type != MessageTypeQueryExecutionRequested || msg.Version != messageVersion {
		slog.Error("consumer: unsupported message type or version",
			"type", msg.Type, "version", msg.Version)
		return c.dlqDirect(context.Background(), msg.MessageID, msg.Payload.JobID, 0, "unsupported type/version")
	}
	if msg.Payload.JobID == 0 {
		slog.Error("consumer: invalid job_id=0")
		return c.dlqDirect(context.Background(), msg.MessageID, 0, 0, "job_id=0")
	}

	// ── 3. Extract and validate x-attempt header ─────────────────────────────
	attempt, err := extractAttempt(d.Headers)
	if err != nil {
		slog.Error("consumer: invalid x-attempt header", "err", err)
		return c.dlqDirect(context.Background(), msg.MessageID, msg.Payload.JobID, 0, "invalid x-attempt header")
	}

	// ── 4. PM Claim ──────────────────────────────────────────────────────────
	now := time.Now()
	leaseToken, claimResult, claimErr := c.pmRepo.Claim(
		context.Background(), msg.MessageID, msg.Type, msg.Payload.JobID, uint8(attempt), now,
	)
	if claimErr != nil {
		slog.Error("consumer: pm claim error", "job_id", msg.Payload.JobID, "err", claimErr)
		return outcomeFatal, claimErr
	}

	switch claimResult {
	case ClaimAlreadyDone:
		// Duplicate delivery of a completed message; ACK idempotently.
		slog.Info("consumer: duplicate delivery, already done", "job_id", msg.Payload.JobID)
		return outcomeAck, nil

	case ClaimLeaseHeld:
		// Another worker is processing this message. Defer by publishing the
		// same attempt back to the retry queue (no attempt increment).
		if pubErr := c.retryPub.PublishRetry(
			context.Background(), msg.Payload.JobID, msg.MessageID, attempt,
		); pubErr != nil {
			slog.Error("consumer: defer-retry publish failed (lease held)", "job_id", msg.Payload.JobID, "err", pubErr)
			return outcomeNackRequeue, pubErr
		}
		slog.Info("consumer: lease held, deferred to retry queue", "job_id", msg.Payload.JobID)
		return outcomeAck, nil

	case ClaimConflict:
		// Same job_id, different message_id: this is an unexpected duplicate publish.
		slog.Error("consumer: job_id conflict, routing to DLQ", "job_id", msg.Payload.JobID)
		return c.dlqDirect(context.Background(), msg.MessageID, msg.Payload.JobID, attempt, "job_id conflict")
	}

	// ClaimGranted, ClaimTakenOver, ClaimResumed — proceed with processing.

	// ── 5. Lease renewal goroutine ───────────────────────────────────────────
	procCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var leaseLost atomic.Bool
	go func() {
		ticker := time.NewTicker(leaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-procCtx.Done():
				return
			case <-ticker.C:
				if err := c.pmRepo.Renew(procCtx, msg.MessageID, leaseToken, time.Now()); err != nil {
					if errors.Is(err, ErrLeaseLost) {
						leaseLost.Store(true)
						cancel()
					}
					return
				}
			}
		}
	}()

	// ── 6. Delegate to WorkerService ─────────────────────────────────────────
	req := ProcessRequest{
		JobID:     msg.Payload.JobID,
		MessageID: msg.MessageID,
		Attempt:   attempt,
	}
	procErr := c.svc.Process(procCtx, req)
	cancel() // stop renewal goroutine regardless of outcome

	// ── 7. Lease-lost check ──────────────────────────────────────────────────
	if leaseLost.Load() {
		slog.Warn("consumer: lease lost during processing, requeueing",
			"job_id", msg.Payload.JobID)
		return outcomeNackRequeue, fmt.Errorf("lease lost during processing job_id=%d", msg.Payload.JobID)
	}

	// ── 8. Map process outcome to ACK/NACK ───────────────────────────────────
	switch {
	case procErr == nil:
		// Terminal state persisted. Mark PM completed before ACK.
		if err := c.pmRepo.MarkCompleted(context.Background(), msg.MessageID, leaseToken, time.Now()); err != nil {
			slog.Error("consumer: mark completed failed", "job_id", msg.Payload.JobID, "err", err)
			return outcomeNackRequeue, err
		}
		return outcomeAck, nil

	case errors.Is(procErr, ErrRetryScheduled):
		// WorkerService published retry and updated DB. Mark PM retry_scheduled.
		if err := c.pmRepo.MarkRetryScheduled(context.Background(), msg.MessageID, leaseToken); err != nil {
			slog.Error("consumer: mark retry_scheduled failed", "job_id", msg.Payload.JobID, "err", err)
			// PM state inconsistency; NACK so message is redelivered. The retry
			// message is already in the queue — the lease will expire and the
			// retry will be processed or timed out.
			return outcomeNackRequeue, err
		}
		return outcomeAck, nil

	case errors.Is(procErr, ErrDLQScheduled):
		// DLQ published + SetFailed done. Mark PM completed.
		if err := c.pmRepo.MarkCompleted(context.Background(), msg.MessageID, leaseToken, time.Now()); err != nil {
			slog.Error("consumer: mark completed after dlq failed", "job_id", msg.Payload.JobID, "err", err)
			return outcomeNackRequeue, err
		}
		return outcomeAck, nil

	case errors.Is(procErr, ErrJobNotFound):
		// Job does not exist. Route to DLQ then mark completed.
		slog.Error("consumer: job not found, routing to DLQ", "job_id", msg.Payload.JobID)
		return c.dlqThenComplete(context.Background(), msg, attempt, leaseToken)

	default:
		// Fatal: DB write failure, unexpected status conflict, etc. Do not ACK.
		slog.Error("consumer: fatal process error", "job_id", msg.Payload.JobID, "err", procErr)
		return outcomeFatal, procErr
	}
}

// dlqDirect publishes to the DLQ without a PM record (for pre-claim failures
// such as malformed messages or invalid envelopes). ACKs on confirm success;
// NACKs requeue if the publish fails.
func (c *Consumer) dlqDirect(ctx context.Context, messageID string, jobID uint64, attempt int, reason string) (deliveryOutcome, error) {
	if pubErr := c.retryPub.PublishDLQ(ctx, jobID, messageID, attempt); pubErr != nil {
		slog.Error("consumer: dlq publish failed", "reason", reason, "err", pubErr)
		return outcomeNackRequeue, pubErr
	}
	slog.Info("consumer: message routed to DLQ", "reason", reason, "job_id", jobID)
	return outcomeAck, nil
}

// dlqThenComplete publishes to the DLQ (for ErrJobNotFound), then marks PM
// completed. ACKs on success; NACKs requeue if either step fails.
func (c *Consumer) dlqThenComplete(ctx context.Context, msg QueryJobMessage, attempt int, leaseToken string) (deliveryOutcome, error) {
	if pubErr := c.retryPub.PublishDLQ(ctx, msg.Payload.JobID, msg.MessageID, attempt); pubErr != nil {
		slog.Error("consumer: dlq publish failed for missing job", "job_id", msg.Payload.JobID, "err", pubErr)
		return outcomeNackRequeue, pubErr
	}
	if err := c.pmRepo.MarkCompleted(ctx, msg.MessageID, leaseToken, time.Now()); err != nil {
		slog.Error("consumer: mark completed after job-not-found dlq failed", "job_id", msg.Payload.JobID, "err", err)
		return outcomeNackRequeue, err
	}
	return outcomeAck, nil
}

// extractAttempt reads the x-attempt AMQP header and returns its value as int.
// Strict validation: must be int32, non-negative, and within sane bounds.
func extractAttempt(headers amqp.Table) (int, error) {
	if headers == nil {
		return 0, nil // initial publish has no x-attempt header; treat as 0
	}
	raw, ok := headers[HeaderAttempt]
	if !ok {
		return 0, nil
	}
	// The amqp091-go library decodes AMQP integers to int32 by default.
	v, ok := raw.(int32)
	if !ok {
		return 0, fmt.Errorf("x-attempt header has unexpected type %T", raw)
	}
	if v < 0 {
		return 0, fmt.Errorf("x-attempt header is negative: %d", v)
	}
	// Sane upper bound to catch corrupted headers.
	const maxSaneAttempt = 100
	if v > maxSaneAttempt {
		return 0, fmt.Errorf("x-attempt header exceeds sane limit: %d", v)
	}
	return int(v), nil
}
