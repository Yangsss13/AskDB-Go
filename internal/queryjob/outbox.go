package queryjob

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
)

const (
	outboxStatusPending    = "pending"
	outboxStatusPublishing = "publishing"
	outboxStatusPublished  = "published"
)

// OutboxEvent is the GORM model for the outbox_events table.
// message_id and occurred_at are stable: generated once at Submit time and never
// regenerated. The Dispatcher must reconstruct the same message envelope on retry.
type OutboxEvent struct {
	ID             uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	MessageID      string     `gorm:"column:message_id"`
	EventType      string     `gorm:"column:event_type"`
	Version        int        `gorm:"column:version"`
	JobID          uint64     `gorm:"column:job_id"`
	Payload        string     `gorm:"column:payload"`
	Status         string     `gorm:"column:status"`
	AttemptCount   int        `gorm:"column:attempt_count"`
	NextRetryAt    time.Time  `gorm:"column:next_retry_at"`
	LeaseToken     *string    `gorm:"column:lease_token"`
	LeaseExpiresAt *time.Time `gorm:"column:lease_expires_at"`
	LastError      *string    `gorm:"column:last_error"`
	CreatedAt      time.Time  `gorm:"column:created_at"`
	PublishedAt    *time.Time `gorm:"column:published_at"`
	OccurredAt     time.Time  `gorm:"column:occurred_at"`
}

// TableName pins the GORM table name.
func (OutboxEvent) TableName() string { return "outbox_events" }

// OutboxRepository manages persistence for outbox events.
type OutboxRepository interface {
	// ClaimBatch selects up to batchSize pending events with next_retry_at <= now
	// using SELECT FOR UPDATE SKIP LOCKED, marks them publishing, sets a shared
	// lease_token and lease_expires_at, and returns them. Short transaction.
	ClaimBatch(ctx context.Context, batchSize int, leaseTTL time.Duration, now time.Time) ([]OutboxEvent, error)

	// MarkPublished CAS-updates status to published for the given event.
	// Returns ErrLeaseLost when lease_token no longer matches (stale caller).
	MarkPublished(ctx context.Context, id uint64, leaseToken string, publishedAt time.Time) error

	// ReleaseWithRetry reverts an event to pending, increments attempt_count,
	// sets next_retry_at, records lastError, and clears the lease.
	// No-op (not an error) if the lease_token no longer matches — another
	// instance may have already taken over or published the event.
	ReleaseWithRetry(ctx context.Context, id uint64, leaseToken string, attemptCount int, nextRetryAt time.Time, lastError string) error

	// DeleteOldPublished batch-deletes published events older than before.
	// Only deletes events with status=published; never touches pending/publishing.
	DeleteOldPublished(ctx context.Context, before time.Time, batchSize int) (int64, error)
}

// GORMOutboxRepository implements OutboxRepository via GORM + raw SQL for
// FOR UPDATE SKIP LOCKED and atomic attempt_count increments.
type GORMOutboxRepository struct {
	db *gorm.DB
}

// NewGORMOutboxRepository returns a repository backed by the given GORM handle.
func NewGORMOutboxRepository(db *gorm.DB) *GORMOutboxRepository {
	return &GORMOutboxRepository{db: db}
}

// newOutboxEvent constructs the outbox row for a freshly created job.
// messageID must be the stable ID generated at Submit time (same one stored in
// the AMQP envelope so the Dispatcher can reconstruct it exactly on retry).
func newOutboxEvent(jobID uint64, messageID string, now time.Time) (*OutboxEvent, error) {
	payload, err := marshalOutboxPayload(jobID)
	if err != nil {
		return nil, fmt.Errorf("outbox: marshal payload: %w", err)
	}
	return &OutboxEvent{
		MessageID:    messageID,
		EventType:    MessageTypeQueryExecutionRequested,
		Version:      messageVersion,
		JobID:        jobID,
		Payload:      string(payload),
		Status:       outboxStatusPending,
		AttemptCount: 0,
		NextRetryAt:  now,
		CreatedAt:    now,
		OccurredAt:   now,
	}, nil
}

// marshalOutboxPayload keeps the durable outbox payload limited to the job ID.
// It is also used after the database assigns the job's auto-increment ID.
func marshalOutboxPayload(jobID uint64) (string, error) {
	payload, err := json.Marshal(map[string]any{"job_id": jobID})
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

// ClaimBatch claims up to batchSize events in a single short transaction.
// It acquires two categories using SELECT FOR UPDATE SKIP LOCKED:
//   - status='pending'    with next_retry_at  <= now  (normal path)
//   - status='publishing' with lease_expires_at <= now (crash-recovery takeover)
//
// Parentheses around each OR branch prevent AND/OR precedence bugs.
// Takeover generates a fresh lease_token; the old token becomes stale so any
// concurrent MarkPublished / ReleaseWithRetry from the crashed instance fails CAS.
// Takeover never increments attempt_count — only a real publish failure does that.
func (r *GORMOutboxRepository) ClaimBatch(
	ctx context.Context,
	batchSize int,
	leaseTTL time.Duration,
	now time.Time,
) ([]OutboxEvent, error) {
	var events []OutboxEvent

	txErr := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Both idx_ob_dispatch (status, next_retry_at) and idx_ob_lease
		// (status, lease_expires_at) serve each branch of the OR.
		if err := tx.Raw(
			`SELECT * FROM outbox_events
			 WHERE (status = 'pending'    AND next_retry_at    <= ?)
			    OR (status = 'publishing' AND lease_expires_at <= ?)
			 LIMIT ?
			 FOR UPDATE SKIP LOCKED`,
			now, now, batchSize,
		).Scan(&events).Error; err != nil {
			return fmt.Errorf("outbox: claim select: %w", err)
		}
		if len(events) == 0 {
			return nil
		}

		ids := make([]uint64, len(events))
		for i, e := range events {
			ids[i] = e.ID
		}
		token := newLeaseToken()
		expires := now.Add(leaseTTL)

		// CAS update guards both branches so rows that somehow changed state
		// between SELECT and UPDATE (impossible within FOR UPDATE but safe) are skipped.
		if err := tx.Exec(
			`UPDATE outbox_events
			 SET status='publishing', lease_token=?, lease_expires_at=?
			 WHERE id IN ? AND (
			   status = 'pending'
			   OR (status = 'publishing' AND lease_expires_at <= ?)
			 )`,
			token, expires, ids, now,
		).Error; err != nil {
			return fmt.Errorf("outbox: claim update: %w", err)
		}

		for i := range events {
			events[i].Status = outboxStatusPublishing
			events[i].LeaseToken = &token
			events[i].LeaseExpiresAt = &expires
		}
		return nil
	})
	if txErr != nil {
		return nil, fmt.Errorf("queryjob: outbox claim batch: %w", txErr)
	}
	return events, nil
}

// MarkPublished CAS-updates the event to published using leaseToken.
func (r *GORMOutboxRepository) MarkPublished(
	ctx context.Context,
	id uint64,
	leaseToken string,
	publishedAt time.Time,
) error {
	res := r.db.WithContext(ctx).Exec(
		`UPDATE outbox_events
		 SET status='published', published_at=?, lease_token=NULL, lease_expires_at=NULL
		 WHERE id=? AND lease_token=? AND status='publishing'`,
		publishedAt, id, leaseToken,
	)
	if res.Error != nil {
		return fmt.Errorf("outbox: mark published id=%d: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("outbox: mark published id=%d: %w", id, ErrLeaseLost)
	}
	return nil
}

// ReleaseWithRetry reverts an event to pending with incremented backoff.
// No-op (nil) when lease_token no longer matches.
func (r *GORMOutboxRepository) ReleaseWithRetry(
	ctx context.Context,
	id uint64,
	leaseToken string,
	attemptCount int,
	nextRetryAt time.Time,
	lastError string,
) error {
	res := r.db.WithContext(ctx).Exec(
		`UPDATE outbox_events
		 SET status='pending', attempt_count=?, next_retry_at=?,
		     last_error=?, lease_token=NULL, lease_expires_at=NULL
		 WHERE id=? AND lease_token=?`,
		attemptCount, nextRetryAt, lastError, id, leaseToken,
	)
	if res.Error != nil {
		return fmt.Errorf("outbox: release retry id=%d: %w", id, res.Error)
	}
	return nil
}

// DeleteOldPublished batch-deletes published events whose published_at is before the cutoff.
func (r *GORMOutboxRepository) DeleteOldPublished(
	ctx context.Context,
	before time.Time,
	batchSize int,
) (int64, error) {
	res := r.db.WithContext(ctx).Exec(
		`DELETE FROM outbox_events
		 WHERE status='published' AND published_at < ?
		 LIMIT ?`,
		before, batchSize,
	)
	if res.Error != nil {
		return 0, fmt.Errorf("outbox: delete old published: %w", res.Error)
	}
	return res.RowsAffected, nil
}
