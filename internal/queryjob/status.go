package queryjob

// Status is the lifecycle state of a query job.
//
// Logical flow (Phase 7: with retry):
//
//	pending -> queued -> generating -> validating -> executing -> succeeded
//	pending / queued / generating / validating / executing -> failed
//	generating / validating / executing -> retrying -> generating
//
// retrying is a non-terminal holding state: the job is waiting for its retry
// message to be re-delivered from the fixed-TTL retry queue.
type Status string

const (
	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusGenerating Status = "generating"
	StatusValidating Status = "validating"
	StatusExecuting  Status = "executing"
	StatusRetrying   Status = "retrying"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
)

// validTransitions maps each status to the set of statuses it may move to.
var validTransitions = map[Status][]Status{
	StatusPending:    {StatusQueued, StatusFailed},
	StatusQueued:     {StatusGenerating, StatusFailed},
	StatusGenerating: {StatusValidating, StatusRetrying, StatusFailed},
	StatusValidating: {StatusExecuting, StatusRetrying, StatusFailed},
	StatusExecuting:  {StatusSucceeded, StatusRetrying, StatusFailed},
	// retrying re-enters the pipeline at generating when the retry fires.
	StatusRetrying:  {StatusGenerating, StatusFailed},
	StatusSucceeded: {},
	StatusFailed:    {},
}

// IsTerminal reports whether s is a final state that cannot transition further.
func (s Status) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed
}

// CanTransition reports whether moving from s to next is a legal transition.
func (s Status) CanTransition(next Status) bool {
	for _, allowed := range validTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}
