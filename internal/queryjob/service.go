package queryjob

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/Yangsss13/askdb-go/internal/queryresult"
)

// Repository persists query jobs. The interface is declared here, on the
// consuming side; the GORM implementation lives in repository.go.
type Repository interface {
	Create(ctx context.Context, job *QueryJob) error
	FindByID(ctx context.Context, id uint64) (*QueryJob, error)
	// TransitionStatus conditionally updates status from `from` to `to`.
	// Returns ErrStatusConflict when no rows were affected.
	TransitionStatus(ctx context.Context, id uint64, from, to Status) error
	// SetRetrying transitions the job to retrying and records the attempt count.
	// Returns ErrStatusConflict when no rows were affected.
	SetRetrying(ctx context.Context, id uint64, from Status, attemptCount uint8, nextRetryAt time.Time) error
	// SetSucceeded atomically writes the success terminal state.
	SetSucceeded(ctx context.Context, id uint64, from Status, generatedSQL string, rowCount, durationMs int64, finishedAt time.Time, resultExpiresAt *time.Time) error
	// SetFailed atomically writes the failure terminal state.
	SetFailed(ctx context.Context, id uint64, from Status, errorCode, errorMessage string, finishedAt time.Time) error
}

// DataSourceChecker verifies data-source ownership. Declared on the consuming
// side to keep the queryjob package free of datasource imports.
type DataSourceChecker interface {
	ExistsForUser(ctx context.Context, dataSourceID, userID uint64) (bool, error)
}

// LLMClient turns a natural-language question into SQL.
type LLMClient interface {
	GenerateSQL(ctx context.Context, question string) (string, error)
}

// QueryExecutor runs a read-only query and returns columns (in order) and rows.
type QueryExecutor interface {
	Execute(ctx context.Context, query string) (columns []string, rows [][]any, err error)
}

// ResultReader is the interface used by ResultService to read cached results.
type ResultReader interface {
	Get(ctx context.Context, jobID uint64) (*queryresult.CachedQueryResult, error)
}

// ProcessRequest carries the context a consumer needs to process a delivery.
// It contains only the minimal routing information; sensitive fields must never
// appear here (no question, SQL, DSN, credentials, or tokens).
type ProcessRequest struct {
	JobID     uint64
	MessageID string
	Attempt   int
}

// ProcessService is the interface the Consumer uses to process a queued job.
// Declared here so the Consumer can be tested without a real WorkerService.
//
// Return semantics (Consumer maps these to ACK/NACK):
//   - nil              → terminal state persisted; Consumer marks PM completed, ACKs.
//   - ErrRetryScheduled → retry published, DB set to retrying; Consumer marks PM
//     retry_scheduled, ACKs.
//   - ErrDLQScheduled  → DLQ published, SetFailed done; Consumer marks PM
//     completed, ACKs.
//   - ErrJobNotFound   → Consumer publishes to DLQ, marks PM completed, ACKs
//     (or NACKs if DLQ publish fails).
//   - any other error  → fatal; Consumer NACKs (channel close / requeue).
type ProcessService interface {
	Process(ctx context.Context, req ProcessRequest) error
}

// Service handles the API side of the query job lifecycle: validate the
// question, create the job, update it to queued, and publish a message.
type Service struct {
	repo    Repository
	pub     Publisher
	dsCheck DataSourceChecker
	now     func() time.Time
}

// NewService wires the API-side service dependencies.
func NewService(repo Repository, pub Publisher, dsCheck DataSourceChecker) *Service {
	return &Service{repo: repo, pub: pub, dsCheck: dsCheck, now: time.Now}
}

// Submit validates the question and dataSourceID, creates a pending job owned
// by userID, advances it to queued, and publishes a message.
func (s *Service) Submit(ctx context.Context, userID uint64, question string, dataSourceID uint64) (*QueryJob, error) {
	trimmed := strings.TrimSpace(question)
	if trimmed == "" || len([]rune(trimmed)) > maxQuestionLen {
		return nil, newServiceError(ErrCodeInvalidQuestion, "question must be 1-500 characters")
	}
	if dataSourceID == 0 {
		return nil, newServiceError(ErrCodeMissingDataSource, "data_source_id is required")
	}

	exists, err := s.dsCheck.ExistsForUser(ctx, dataSourceID, userID)
	if err != nil {
		return nil, newServiceError(ErrCodeInternal, msgInternal)
	}
	if !exists {
		return nil, newServiceError(ErrCodeDataSourceNotFound, "data source not found")
	}

	now := s.now()
	job := &QueryJob{
		Question:     trimmed,
		Status:       string(StatusPending),
		CreatedAt:    now,
		UpdatedAt:    now,
		UserID:       sql.NullInt64{Int64: int64(userID), Valid: true},
		DataSourceID: sql.NullInt64{Int64: int64(dataSourceID), Valid: true},
	}
	if err := s.repo.Create(ctx, job); err != nil {
		return nil, newServiceError(ErrCodeInternal, msgInternal)
	}

	if err := s.repo.TransitionStatus(ctx, job.ID, StatusPending, StatusQueued); err != nil {
		return nil, newServiceError(ErrCodeInternal, msgInternal)
	}
	job.Status = string(StatusQueued)

	if err := s.pub.Publish(ctx, job.ID); err != nil {
		finished := s.now()
		_ = s.repo.SetFailed(ctx, job.ID, StatusQueued, ErrCodePublishFailed, msgPublishFailed, finished)
		return nil, newServiceError(ErrCodePublishFailed, msgPublishFailed)
	}

	return job, nil
}

// Get returns the persisted job by ID for the given caller.
func (s *Service) Get(ctx context.Context, callerID uint64, id uint64) (*QueryJob, error) {
	job, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ownsJob(job, callerID) {
		return nil, ErrJobNotFound
	}
	return job, nil
}

// ResultService retrieves cached query results.
type ResultService struct {
	repo  Repository
	store ResultReader
	now   func() time.Time
}

// NewResultService wires the result-fetch dependencies.
func NewResultService(repo Repository, store ResultReader) *ResultService {
	return &ResultService{repo: repo, store: store, now: time.Now}
}

// GetResult fetches the cached query result for a succeeded job owned by callerID.
func (s *ResultService) GetResult(ctx context.Context, callerID uint64, jobID uint64) (*queryresult.CachedQueryResult, error) {
	job, err := s.repo.FindByID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if !ownsJob(job, callerID) {
		return nil, ErrJobNotFound
	}

	status := Status(job.Status)

	if status == StatusFailed {
		return nil, newServiceError(ErrCodeQueryJobFailed, "query job failed")
	}
	if !status.IsTerminal() {
		return nil, newServiceError(ErrCodeResultNotReady, "result is not ready yet")
	}

	if !job.ResultExpiresAt.Valid {
		return nil, newServiceError(ErrCodeResultUnavailable, "result is not available")
	}

	result, err := s.store.Get(ctx, jobID)
	if err != nil {
		return nil, s.mapStoreError(err, job)
	}
	return result, nil
}

func (s *ResultService) mapStoreError(err error, job *QueryJob) *ServiceError {
	switch {
	case errors.Is(err, queryresult.ErrResultNotFound):
		if s.now().Before(job.ResultExpiresAt.Time) {
			return newServiceError(ErrCodeResultUnavailable, "result is not available")
		}
		return newServiceError(ErrCodeResultExpired, "result has expired")
	case errors.Is(err, queryresult.ErrResultCorrupted):
		return newServiceError(ErrCodeResultCorrupted, "result data is corrupted")
	default:
		return newServiceError(ErrCodeResultStoreUnavail, "result store is unavailable")
	}
}

func ownsJob(job *QueryJob, callerID uint64) bool {
	return job.UserID.Valid && uint64(job.UserID.Int64) == callerID
}
