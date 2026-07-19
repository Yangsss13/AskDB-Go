package queryjob

import (
	"errors"

	"github.com/Yangsss13/askdb-go/internal/llm"
	"github.com/Yangsss13/askdb-go/internal/sqlguard"
)

// isRetryableError reports whether err represents a transient infrastructure
// failure that should be retried. Classification uses errors.Is/As only —
// never error string inspection.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Context cancellation and deadline exceeded are not infra faults; the
	// caller's context timed out, so we let them propagate as fatal.
	if errors.Is(err, errors.ErrUnsupported) {
		return false
	}
	// Deterministic business rejections are not retryable.
	if isDeterministicFailure(err) {
		return false
	}
	// ErrStatusConflict means another worker is processing; not a transient error.
	if errors.Is(err, ErrStatusConflict) {
		return false
	}
	// ErrJobNotFound is permanent; retrying won't make the job appear.
	if errors.Is(err, ErrJobNotFound) {
		return false
	}
	// Everything else is treated as a transient infrastructure error.
	return true
}

// isDeterministicFailure reports whether err is a predictable business-level
// rejection that will not succeed on retry regardless of infrastructure state.
func isDeterministicFailure(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, llm.ErrUnsupportedQuestion) ||
		errors.Is(err, sqlguard.ErrRejected)
}
