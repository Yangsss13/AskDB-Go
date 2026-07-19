package queryjob

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// newLeaseToken returns a random 16-byte hex string used as an unforgeable
// lease token. Concurrent workers with stale tokens cannot update the lease.
func newLeaseToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("fallback-lease-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", b)
}

// pmStatus values mirror the processed_messages.status ENUM in the DB.
const (
	pmStatusProcessing     = "processing"
	pmStatusRetryScheduled = "retry_scheduled"
	pmStatusCompleted      = "completed"
)

// leaseTTL is the duration a worker may hold a processing lease.
// Workers must renew at leaseTTL/3 intervals.
const leaseTTL = 30 * time.Second

// ClaimResult describes the outcome of a Claim call.
type ClaimResult int

const (
	// ClaimGranted: new row inserted; caller holds the lease.
	ClaimGranted ClaimResult = iota
	// ClaimAlreadyDone: status=completed; caller should ACK immediately.
	ClaimAlreadyDone
	// ClaimLeaseHeld: processing, lease not expired; defer processing.
	ClaimLeaseHeld
	// ClaimTakenOver: processing, lease expired; caller now holds the lease.
	ClaimTakenOver
	// ClaimResumed: retry_scheduled; caller may re-process (retry message).
	ClaimResumed
	// ClaimConflict: same job_id+type exists with a different message_id and
	// the existing record is in a terminal or active state that blocks this
	// message. Consumer must route to DLQ.
	ClaimConflict
)

// ProcessedMessage is the GORM model for the processed_messages table.
type ProcessedMessage struct {
	MessageID      string     `gorm:"column:message_id;primaryKey"`
	MessageType    string     `gorm:"column:message_type"`
	JobID          uint64     `gorm:"column:job_id"`
	Attempt        uint8      `gorm:"column:attempt"`
	Status         string     `gorm:"column:status"`
	LeaseToken     string     `gorm:"column:lease_token"`
	LeaseExpiresAt time.Time  `gorm:"column:lease_expires_at"`
	CompletedAt    *time.Time `gorm:"column:completed_at"`
}

// TableName pins the GORM table name.
func (ProcessedMessage) TableName() string { return "processed_messages" }

// ProcessedMessageRepository manages idempotency records for consumed messages.
type ProcessedMessageRepository interface {
	// Claim atomically tries to acquire a processing lease for the message.
	// Returns the lease token (non-empty when claim is Granted, TakenOver or Resumed)
	// and the claim result.
	Claim(ctx context.Context, messageID, messageType string, jobID uint64, attempt uint8, now time.Time) (leaseToken string, result ClaimResult, err error)

	// Renew extends the lease for the given message_id, authenticated by leaseToken.
	// Returns an error if the token no longer matches (lease was taken over).
	Renew(ctx context.Context, messageID, leaseToken string, now time.Time) error

	// MarkRetryScheduled transitions the record to retry_scheduled.
	// Authenticated by leaseToken.
	MarkRetryScheduled(ctx context.Context, messageID, leaseToken string) error

	// MarkCompleted transitions the record to completed.
	// Authenticated by leaseToken.
	MarkCompleted(ctx context.Context, messageID, leaseToken string, now time.Time) error

	// FindByMessageID loads the record, returning ErrPMNotFound if absent.
	FindByMessageID(ctx context.Context, messageID string) (*ProcessedMessage, error)
}

// ErrPMNotFound is returned when no processed_messages row matches.
var ErrPMNotFound = errors.New("queryjob: processed_message not found")

// ErrLeaseLost is returned when a lease operation fails because the token
// no longer matches — the lease was taken over by another worker.
var ErrLeaseLost = errors.New("queryjob: lease lost")

// GORMProcessedMessageRepository implements ProcessedMessageRepository via GORM.
type GORMProcessedMessageRepository struct {
	db *gorm.DB
}

// NewGORMProcessedMessageRepository returns the repository.
func NewGORMProcessedMessageRepository(db *gorm.DB) *GORMProcessedMessageRepository {
	return &GORMProcessedMessageRepository{db: db}
}

// Claim atomically acquires or takes over a processing lease.
func (r *GORMProcessedMessageRepository) Claim(
	ctx context.Context,
	messageID, messageType string,
	jobID uint64,
	attempt uint8,
	now time.Time,
) (string, ClaimResult, error) {
	token := newLeaseToken()
	expires := now.Add(leaseTTL)

	var result ClaimResult
	var outToken string

	txErr := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Try to load the existing record (lock for update).
		var existing ProcessedMessage
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("message_id = ?", messageID).
			First(&existing).Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			// No record for this message_id. Try INSERT.
			pm := ProcessedMessage{
				MessageID:      messageID,
				MessageType:    messageType,
				JobID:          jobID,
				Attempt:        attempt,
				Status:         pmStatusProcessing,
				LeaseToken:     token,
				LeaseExpiresAt: expires,
			}
			insertErr := tx.Create(&pm).Error
			if insertErr == nil {
				result = ClaimGranted
				outToken = token
				return nil
			}
			// INSERT failed — likely unique constraint on (message_type, job_id).
			// Look up the conflicting row by job key.
			var conflicting ProcessedMessage
			if selErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("message_type = ? AND job_id = ?", messageType, jobID).
				First(&conflicting).Error; selErr != nil {
				return fmt.Errorf("claim: lookup conflict row: %w", selErr)
			}
			// A different message_id already owns this job.
			return r.handleConflictingRow(tx, conflicting, &result)
		}
		if err != nil {
			return fmt.Errorf("claim: select for update: %w", err)
		}

		// Record found for this message_id.
		// Validate business keys haven't changed (message_id reuse with wrong job).
		if existing.MessageType != messageType || existing.JobID != jobID {
			result = ClaimConflict
			return nil
		}

		return r.handleExistingRow(tx, existing, token, expires, &result, &outToken)
	})

	if txErr != nil {
		return "", 0, fmt.Errorf("queryjob: pm claim: %w", txErr)
	}
	return outToken, result, nil
}

// handleExistingRow resolves an existing PM row for the same message_id.
func (r *GORMProcessedMessageRepository) handleExistingRow(
	tx *gorm.DB,
	row ProcessedMessage,
	newToken string,
	expires time.Time,
	result *ClaimResult,
	outToken *string,
) error {
	switch row.Status {
	case pmStatusCompleted:
		*result = ClaimAlreadyDone
		return nil
	case pmStatusRetryScheduled:
		// Retry message re-delivered: resume processing with a fresh lease.
		if err := tx.Model(&ProcessedMessage{}).
			Where("message_id = ? AND status = ?", row.MessageID, pmStatusRetryScheduled).
			Updates(map[string]any{
				"status":           pmStatusProcessing,
				"lease_token":      newToken,
				"lease_expires_at": expires,
			}).Error; err != nil {
			return fmt.Errorf("claim resume: %w", err)
		}
		*result = ClaimResumed
		*outToken = newToken
	case pmStatusProcessing:
		if time.Now().Before(row.LeaseExpiresAt) {
			*result = ClaimLeaseHeld
			return nil
		}
		// Lease expired: take over.
		if err := tx.Model(&ProcessedMessage{}).
			Where("message_id = ? AND lease_token = ?", row.MessageID, row.LeaseToken).
			Updates(map[string]any{
				"lease_token":      newToken,
				"lease_expires_at": expires,
			}).Error; err != nil {
			return fmt.Errorf("claim takeover: %w", err)
		}
		*result = ClaimTakenOver
		*outToken = newToken
	}
	return nil
}

// handleConflictingRow resolves a (message_type, job_id) conflict where a
// different message_id already exists for the same job.
// Completed = job done (ACK); anything else = conflict (route to DLQ).
func (r *GORMProcessedMessageRepository) handleConflictingRow(
	_ *gorm.DB,
	row ProcessedMessage,
	result *ClaimResult,
) error {
	switch row.Status {
	case pmStatusCompleted:
		*result = ClaimAlreadyDone
	default:
		// processing (lease active or expired) or retry_scheduled:
		// a different message_id is already active for this job.
		// Do NOT mutate the row; route the conflicting message to DLQ.
		if row.Status == pmStatusProcessing && !time.Now().Before(row.LeaseExpiresAt) {
			// Lease expired — same DLQ outcome. The original unacked message
			// will be redelivered by the broker and handleExistingRow will
			// perform an authenticated CAS takeover at that point.
		}
		*result = ClaimConflict
	}
	return nil
}

// Renew extends the lease. Returns ErrLeaseLost if leaseToken no longer matches.
func (r *GORMProcessedMessageRepository) Renew(ctx context.Context, messageID, leaseToken string, now time.Time) error {
	res := r.db.WithContext(ctx).
		Model(&ProcessedMessage{}).
		Where("message_id = ? AND lease_token = ? AND status = ?",
			messageID, leaseToken, pmStatusProcessing).
		Update("lease_expires_at", now.Add(leaseTTL))
	if res.Error != nil {
		return fmt.Errorf("queryjob: pm renew: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("queryjob: pm renew id=%s: %w", messageID, ErrLeaseLost)
	}
	return nil
}

// MarkRetryScheduled transitions to retry_scheduled, authenticated by leaseToken.
func (r *GORMProcessedMessageRepository) MarkRetryScheduled(ctx context.Context, messageID, leaseToken string) error {
	res := r.db.WithContext(ctx).
		Model(&ProcessedMessage{}).
		Where("message_id = ? AND lease_token = ? AND status = ?",
			messageID, leaseToken, pmStatusProcessing).
		Update("status", pmStatusRetryScheduled)
	if res.Error != nil {
		return fmt.Errorf("queryjob: pm retry_scheduled: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("queryjob: pm retry_scheduled id=%s: %w", messageID, ErrLeaseLost)
	}
	return nil
}

// MarkCompleted transitions to completed, authenticated by leaseToken.
func (r *GORMProcessedMessageRepository) MarkCompleted(ctx context.Context, messageID, leaseToken string, now time.Time) error {
	res := r.db.WithContext(ctx).
		Model(&ProcessedMessage{}).
		Where("message_id = ? AND lease_token = ?", messageID, leaseToken).
		Updates(map[string]any{
			"status":       pmStatusCompleted,
			"completed_at": now,
		})
	if res.Error != nil {
		return fmt.Errorf("queryjob: pm complete: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("queryjob: pm complete id=%s: %w", messageID, ErrLeaseLost)
	}
	return nil
}

// FindByMessageID loads a record by message_id, returning ErrPMNotFound if absent.
func (r *GORMProcessedMessageRepository) FindByMessageID(ctx context.Context, messageID string) (*ProcessedMessage, error) {
	var pm ProcessedMessage
	err := r.db.WithContext(ctx).Where("message_id = ?", messageID).First(&pm).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrPMNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("queryjob: pm find: %w", err)
	}
	return &pm, nil
}
