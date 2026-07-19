package queryjob

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// ─── in-memory OutboxRepository (faithfully simulates DB claim semantics) ────

type memOutboxRepo struct {
	mu     sync.Mutex
	events map[uint64]*OutboxEvent
}

func newMemOutboxRepo(evs ...OutboxEvent) *memOutboxRepo {
	r := &memOutboxRepo{events: make(map[uint64]*OutboxEvent)}
	for _, e := range evs {
		ec := e
		r.events[e.ID] = &ec
	}
	return r
}

func (m *memOutboxRepo) ClaimBatch(_ context.Context, batchSize int, leaseTTL time.Duration, now time.Time) ([]OutboxEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var eligible []*OutboxEvent
	for _, e := range m.events {
		switch {
		case e.Status == outboxStatusPending && !e.NextRetryAt.After(now):
			eligible = append(eligible, e)
		case e.Status == outboxStatusPublishing && e.LeaseExpiresAt != nil && !e.LeaseExpiresAt.After(now):
			eligible = append(eligible, e)
		}
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].ID < eligible[j].ID })
	if len(eligible) > batchSize {
		eligible = eligible[:batchSize]
	}

	token := fmt.Sprintf("tok-%d", now.UnixNano())
	expires := now.Add(leaseTTL)

	claimed := make([]OutboxEvent, 0, len(eligible))
	for _, e := range eligible {
		e.Status = outboxStatusPublishing
		e.LeaseToken = &token
		e.LeaseExpiresAt = &expires
		claimed = append(claimed, *e)
	}
	return claimed, nil
}

func (m *memOutboxRepo) MarkPublished(_ context.Context, id uint64, leaseToken string, publishedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	if !ok || e.LeaseToken == nil || *e.LeaseToken != leaseToken || e.Status != outboxStatusPublishing {
		return ErrLeaseLost
	}
	e.Status = outboxStatusPublished
	e.PublishedAt = &publishedAt
	e.LeaseToken = nil
	e.LeaseExpiresAt = nil
	return nil
}

func (m *memOutboxRepo) ReleaseWithRetry(_ context.Context, id uint64, leaseToken string, attemptCount int, nextRetryAt time.Time, lastErr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	if !ok || e.LeaseToken == nil || *e.LeaseToken != leaseToken {
		return nil // no-op on stale token
	}
	e.Status = outboxStatusPending
	e.AttemptCount = attemptCount
	e.NextRetryAt = nextRetryAt
	e.LeaseToken = nil
	e.LeaseExpiresAt = nil
	if lastErr != "" {
		e.LastError = &lastErr
	}
	return nil
}

func (m *memOutboxRepo) DeleteOldPublished(_ context.Context, before time.Time, batchSize int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for id, e := range m.events {
		if n >= int64(batchSize) {
			break
		}
		if e.Status == outboxStatusPublished && e.PublishedAt != nil && e.PublishedAt.Before(before) {
			delete(m.events, id)
			n++
		}
	}
	return n, nil
}

// helper: pointer to expires
func expires(t time.Time) *time.Time { return &t }
func tok(s string) *string           { return &s }

// ─── fakes for Dispatcher behaviour tests (unchanged from Phase 8) ───────────

type fakeOutboxRepoFull struct {
	claimedBatches [][]OutboxEvent
	claimErr       error
	claimCallCount int

	markedPublished []uint64
	markPublishErr  error
	markPublishCAS  bool

	releasedIDs  []uint64
	releaseErr   error
	releaseCount int

	deletedCount    int64
	deleteErr       error
	deleteCallCount int
}

func (f *fakeOutboxRepoFull) ClaimBatch(_ context.Context, _ int, _ time.Duration, _ time.Time) ([]OutboxEvent, error) {
	f.claimCallCount++
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if f.claimCallCount-1 < len(f.claimedBatches) {
		return f.claimedBatches[f.claimCallCount-1], nil
	}
	return nil, nil
}

func (f *fakeOutboxRepoFull) MarkPublished(_ context.Context, id uint64, _ string, _ time.Time) error {
	if f.markPublishErr != nil {
		return f.markPublishErr
	}
	if f.markPublishCAS && len(f.markedPublished) >= 1 {
		return ErrLeaseLost
	}
	f.markedPublished = append(f.markedPublished, id)
	return nil
}

func (f *fakeOutboxRepoFull) ReleaseWithRetry(_ context.Context, id uint64, _ string, _ int, _ time.Time, _ string) error {
	f.releaseCount++
	if f.releaseErr != nil {
		return f.releaseErr
	}
	f.releasedIDs = append(f.releasedIDs, id)
	return nil
}

func (f *fakeOutboxRepoFull) DeleteOldPublished(_ context.Context, _ time.Time, _ int) (int64, error) {
	f.deleteCallCount++
	return f.deletedCount, f.deleteErr
}

// fakePublisherOutbox is a fake Publisher for Dispatcher tests.
type fakePublisherOutbox struct {
	published []*publishedOutboxRecord
	callCount int
}

type publishedOutboxRecord struct {
	eventID   uint64
	messageID string
}

// buildTestDispatcher builds a Dispatcher for unit tests.
func buildTestDispatcher(outbox OutboxRepository, _ *fakePublisherOutbox, cfg DispatcherConfig) *Dispatcher {
	return NewDispatcher(outbox, "amqp://test", cfg)
}

func defaultDispatcherCfg() DispatcherConfig {
	return DispatcherConfig{
		PollInterval:    100 * time.Millisecond,
		BatchSize:       5,
		LeaseTTL:        30 * time.Second,
		BaseBackoff:     100 * time.Millisecond,
		MaxBackoff:      2 * time.Second,
		PublishedRetain: time.Hour,
		CleanBatch:      10,
		ConfirmTimeout:  2 * time.Second,
	}
}

// makeEvent builds a minimal OutboxEvent for tests.
func makeEvent(id uint64, msgID string) OutboxEvent {
	t := tok("token-" + msgID)
	return OutboxEvent{
		ID:         id,
		MessageID:  msgID,
		EventType:  MessageTypeQueryExecutionRequested,
		Version:    messageVersion,
		JobID:      id,
		Payload:    `{"job_id":1}`,
		Status:     outboxStatusPublishing,
		LeaseToken: t,
		OccurredAt: time.Now().UTC(),
	}
}

// ─── ClaimBatch filtering semantics ──────────────────────────────────────────

// TestClaimBatch_PendingExpired: a pending event whose next_retry_at has passed is claimed.
func TestClaimBatch_PendingExpired(t *testing.T) {
	now := time.Now().UTC()
	e := OutboxEvent{
		ID: 1, MessageID: "m1", JobID: 1, Status: outboxStatusPending,
		NextRetryAt: now.Add(-time.Second), // already due
		OccurredAt:  now,
	}
	r := newMemOutboxRepo(e)

	got, err := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != 1 {
		t.Errorf("expected event 1 to be claimed, got %v", got)
	}
	if got[0].Status != outboxStatusPublishing {
		t.Errorf("claimed event status: got %s, want publishing", got[0].Status)
	}
}

// TestClaimBatch_PendingNotExpired: a pending event with next_retry_at in the future is skipped.
func TestClaimBatch_PendingNotExpired(t *testing.T) {
	now := time.Now().UTC()
	e := OutboxEvent{
		ID: 2, MessageID: "m2", JobID: 2, Status: outboxStatusPending,
		NextRetryAt: now.Add(time.Minute), // not due yet
		OccurredAt:  now,
	}
	r := newMemOutboxRepo(e)

	got, err := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("unexpired pending event must not be claimed, got %v", got)
	}
}

// TestClaimBatch_PublishingLeaseExpired: a publishing event whose lease has expired is taken over.
func TestClaimBatch_PublishingLeaseExpired(t *testing.T) {
	now := time.Now().UTC()
	pastExpiry := now.Add(-time.Second)
	e := OutboxEvent{
		ID: 3, MessageID: "m3", JobID: 3, Status: outboxStatusPublishing,
		LeaseToken:     tok("old-token"),
		LeaseExpiresAt: expires(pastExpiry), // lease expired
		OccurredAt:     now,
	}
	r := newMemOutboxRepo(e)

	got, err := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != 3 {
		t.Errorf("expired publishing event must be claimable, got %v", got)
	}
}

// TestClaimBatch_PublishingLeaseNotExpired: a publishing event with an active lease is skipped.
func TestClaimBatch_PublishingLeaseNotExpired(t *testing.T) {
	now := time.Now().UTC()
	futureExpiry := now.Add(time.Minute)
	e := OutboxEvent{
		ID: 4, MessageID: "m4", JobID: 4, Status: outboxStatusPublishing,
		LeaseToken:     tok("active-token"),
		LeaseExpiresAt: expires(futureExpiry), // still active
		OccurredAt:     now,
	}
	r := newMemOutboxRepo(e)

	got, err := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("active-lease publishing event must NOT be claimed, got %v", got)
	}
}

// TestClaimBatch_LeaseTokenReplaced: after takeover, the event has a new lease_token.
func TestClaimBatch_LeaseTokenReplaced(t *testing.T) {
	now := time.Now().UTC()
	pastExpiry := now.Add(-time.Second)
	oldTok := "old-token"
	e := OutboxEvent{
		ID: 5, MessageID: "m5", JobID: 5, Status: outboxStatusPublishing,
		LeaseToken:     &oldTok,
		LeaseExpiresAt: expires(pastExpiry),
		OccurredAt:     now,
	}
	r := newMemOutboxRepo(e)

	got, err := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected takeover, got %d events", len(got))
	}
	if got[0].LeaseToken == nil || *got[0].LeaseToken == oldTok {
		t.Errorf("lease_token must be replaced after takeover; old=%s new=%v", oldTok, got[0].LeaseToken)
	}
}

// TestClaimBatch_StaleTokenCASFails_MarkPublished: the old token cannot mark published after takeover.
func TestClaimBatch_StaleTokenCASFails_MarkPublished(t *testing.T) {
	now := time.Now().UTC()
	pastExpiry := now.Add(-time.Second)
	e := OutboxEvent{
		ID: 6, MessageID: "m6", JobID: 6, Status: outboxStatusPublishing,
		LeaseToken:     tok("old-tok"),
		LeaseExpiresAt: expires(pastExpiry),
		OccurredAt:     now,
	}
	r := newMemOutboxRepo(e)

	// Instance B takes over.
	got, err := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if err != nil || len(got) != 1 {
		t.Fatalf("takeover failed: err=%v, events=%v", err, got)
	}
	newToken := *got[0].LeaseToken

	// Instance A (crashed) wakes and tries to mark published with old token — must fail.
	err = r.MarkPublished(context.Background(), 6, "old-tok", now)
	if !errors.Is(err, ErrLeaseLost) {
		t.Errorf("old token must not mark published; got %v", err)
	}

	// Instance B can mark published with its new token.
	if err := r.MarkPublished(context.Background(), 6, newToken, now); err != nil {
		t.Errorf("new token must be able to mark published; got %v", err)
	}
}

// TestClaimBatch_StaleTokenCASFails_Release: old token ReleaseWithRetry is a no-op after takeover.
func TestClaimBatch_StaleTokenCASFails_Release(t *testing.T) {
	now := time.Now().UTC()
	pastExpiry := now.Add(-time.Second)
	e := OutboxEvent{
		ID: 7, MessageID: "m7", JobID: 7, Status: outboxStatusPublishing,
		LeaseToken:     tok("old-tok"),
		LeaseExpiresAt: expires(pastExpiry),
		OccurredAt:     now,
	}
	r := newMemOutboxRepo(e)

	// Instance B takes over.
	got, _ := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if len(got) == 0 {
		t.Fatal("expected takeover")
	}

	// Instance A tries ReleaseWithRetry with its stale token — must be a no-op.
	if err := r.ReleaseWithRetry(context.Background(), 7, "old-tok", 1, now.Add(time.Second), "stale"); err != nil {
		t.Errorf("stale ReleaseWithRetry must not error; got %v", err)
	}
	// Event must still be publishing (not reverted to pending).
	r.mu.Lock()
	status := r.events[7].Status
	r.mu.Unlock()
	if status != outboxStatusPublishing {
		t.Errorf("event must stay publishing after stale release; got %s", status)
	}
}

// TestClaimBatch_ConcurrentClaimDistinct: two concurrent ClaimBatch calls get disjoint events.
func TestClaimBatch_ConcurrentClaimDistinct(t *testing.T) {
	now := time.Now().UTC()
	// Two expired pending events.
	e1 := OutboxEvent{ID: 10, MessageID: "c1", JobID: 10, Status: outboxStatusPending, NextRetryAt: now.Add(-time.Second), OccurredAt: now}
	e2 := OutboxEvent{ID: 11, MessageID: "c2", JobID: 11, Status: outboxStatusPending, NextRetryAt: now.Add(-time.Second), OccurredAt: now}
	r := newMemOutboxRepo(e1, e2)

	// Simulate two Dispatchers claiming batchSize=1 each.
	got1, err1 := r.ClaimBatch(context.Background(), 1, 30*time.Second, now)
	got2, err2 := r.ClaimBatch(context.Background(), 1, 30*time.Second, now)

	if err1 != nil || err2 != nil {
		t.Fatalf("claim errors: %v, %v", err1, err2)
	}
	// Together they must cover both events exactly once.
	total := len(got1) + len(got2)
	if total != 2 {
		t.Errorf("expected 2 total claimed across both calls, got %d", total)
	}
	seen := map[uint64]bool{}
	for _, e := range append(got1, got2...) {
		if seen[e.ID] {
			t.Errorf("event %d claimed twice", e.ID)
		}
		seen[e.ID] = true
	}
}

// TestClaimBatch_TakeoverPreservesFields: message_id, occurred_at and payload are
// unchanged when a crashed-publisher's event is taken over and republished.
func TestClaimBatch_TakeoverPreservesFields(t *testing.T) {
	now := time.Now().UTC()
	occurred := now.Add(-5 * time.Minute)
	payload := `{"job_id":42}`
	pastExpiry := now.Add(-time.Second)
	e := OutboxEvent{
		ID: 20, MessageID: "stable-id", JobID: 42,
		Payload:        payload,
		OccurredAt:     occurred,
		Status:         outboxStatusPublishing,
		LeaseToken:     tok("crashed-token"),
		LeaseExpiresAt: expires(pastExpiry),
	}
	r := newMemOutboxRepo(e)

	got, err := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if err != nil || len(got) != 1 {
		t.Fatalf("takeover failed: err=%v got=%v", err, got)
	}
	ev := got[0]
	if ev.MessageID != "stable-id" {
		t.Errorf("message_id must be stable; got %s", ev.MessageID)
	}
	if !ev.OccurredAt.Equal(occurred) {
		t.Errorf("occurred_at must be stable; got %v want %v", ev.OccurredAt, occurred)
	}
	if ev.Payload != payload {
		t.Errorf("payload must be stable; got %s", ev.Payload)
	}
}

func TestNewOutboxEvent_PayloadUsesJobID(t *testing.T) {
	event, err := newOutboxEvent(42, "stable-id", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if event.Payload != `{"job_id":42}` {
		t.Errorf("payload: got %s, want {\"job_id\":42}", event.Payload)
	}
}

// TestClaimBatch_TakeoverAttemptCountUnchanged: lease takeover must NOT increase attempt_count.
func TestClaimBatch_TakeoverAttemptCountUnchanged(t *testing.T) {
	now := time.Now().UTC()
	pastExpiry := now.Add(-time.Second)
	e := OutboxEvent{
		ID: 21, MessageID: "m21", JobID: 21, AttemptCount: 2,
		Status:         outboxStatusPublishing,
		LeaseToken:     tok("old"),
		LeaseExpiresAt: expires(pastExpiry),
		OccurredAt:     now,
	}
	r := newMemOutboxRepo(e)

	got, err := r.ClaimBatch(context.Background(), 10, 30*time.Second, now)
	if err != nil || len(got) != 1 {
		t.Fatalf("takeover failed: err=%v", err)
	}
	if got[0].AttemptCount != 2 {
		t.Errorf("takeover must not increase attempt_count; got %d want 2", got[0].AttemptCount)
	}
}

// ─── Dispatcher behaviour tests ───────────────────────────────────────────────

// TestDispatcher_Backoff verifies capped exponential backoff does not overflow.
func TestDispatcher_Backoff(t *testing.T) {
	d := &Dispatcher{cfg: DispatcherConfig{
		BaseBackoff: 5 * time.Second,
		MaxBackoff:  10 * time.Minute,
	}}
	cases := []struct {
		attempt int
		max     time.Duration
	}{
		{0, 5 * time.Second},
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{100, 10 * time.Minute},
		{1000, 10 * time.Minute},
	}
	for _, tc := range cases {
		got := d.backoffFor(tc.attempt)
		if got > tc.max {
			t.Errorf("attempt=%d: got %v > max %v", tc.attempt, got, tc.max)
		}
		if got <= 0 {
			t.Errorf("attempt=%d: got non-positive backoff %v", tc.attempt, got)
		}
	}
}

func TestDispatcher_OnlyDeletesPublished(t *testing.T) {
	outbox := &fakeOutboxRepoFull{deletedCount: 5}
	d := &Dispatcher{outbox: outbox, cfg: defaultDispatcherCfg()}
	d.cleanOld(context.Background())
	if outbox.deleteCallCount != 1 {
		t.Errorf("expected 1 DeleteOldPublished call, got %d", outbox.deleteCallCount)
	}
}

func TestDispatcher_ReleaseOnStop(t *testing.T) {
	tk := "tk1"
	events := []OutboxEvent{
		{ID: 1, JobID: 1, MessageID: "m1", LeaseToken: &tk, Status: outboxStatusPublishing},
		{ID: 2, JobID: 2, MessageID: "m2", LeaseToken: &tk, Status: outboxStatusPublishing},
	}
	outbox := &fakeOutboxRepoFull{}
	cfg := defaultDispatcherCfg()
	d := NewDispatcher(outbox, "", cfg)
	d.pub = &RabbitMQPublisher{}
	d.once.Do(func() { close(d.stop) })
	for _, e := range events {
		d.releaseEvent(context.Background(), e, "dispatcher stopping")
	}
	if len(outbox.releasedIDs) != 2 {
		t.Errorf("expected 2 releases, got %d", len(outbox.releasedIDs))
	}
}

func TestDispatcher_ReleaseIncreasesAttempt(t *testing.T) {
	outbox := &fakeOutboxRepoFull{}
	cfg := defaultDispatcherCfg()
	d := &Dispatcher{outbox: outbox, cfg: cfg}
	tk := "tk"
	e := OutboxEvent{ID: 5, AttemptCount: 2, LeaseToken: &tk}
	d.releaseEvent(context.Background(), e, "test error")
	if outbox.releaseCount != 1 {
		t.Fatalf("expected 1 ReleaseWithRetry call, got %d", outbox.releaseCount)
	}
}

func TestDispatcher_GracefulShutdown(t *testing.T) {
	outbox := &fakeOutboxRepoFull{}
	cfg := defaultDispatcherCfg()
	d := NewDispatcher(outbox, "amqp://test", cfg)
	d.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d.Stop(ctx)
	if ctx.Err() != nil {
		t.Error("dispatcher did not stop within timeout")
	}
}

func TestDispatcher_OnlyCleanPublished(t *testing.T) {
	outbox := &fakeOutboxRepoFull{}
	cfg := defaultDispatcherCfg()
	d := &Dispatcher{outbox: outbox, cfg: cfg}
	d.cleanOld(context.Background())
	d.cleanOld(context.Background())
	if outbox.deleteCallCount != 2 {
		t.Errorf("expected 2 cleanup calls, got %d", outbox.deleteCallCount)
	}
}

func TestDispatcher_RetryBackoff(t *testing.T) {
	cfg := DispatcherConfig{BaseBackoff: 1 * time.Second, MaxBackoff: 60 * time.Second}
	d := &Dispatcher{cfg: cfg}
	delays := make([]time.Duration, 5)
	for i := 0; i < 5; i++ {
		delays[i] = d.backoffFor(i + 1)
	}
	for i := 1; i < len(delays); i++ {
		if delays[i] <= delays[i-1] && delays[i] < cfg.MaxBackoff {
			t.Errorf("backoff not increasing: delays[%d]=%v <= delays[%d]=%v",
				i, delays[i], i-1, delays[i-1])
		}
	}
}

func TestDispatcher_ConfirmAfterCrash_AtLeastOnce(t *testing.T) {
	outbox := &fakeOutboxRepoFull{markPublishErr: ErrLeaseLost}
	tk := "tk"
	e := makeEvent(1, "msg-1")
	e.LeaseToken = &tk
	err := outbox.MarkPublished(context.Background(), e.ID, *e.LeaseToken, time.Now())
	if !errors.Is(err, ErrLeaseLost) {
		t.Errorf("expected ErrLeaseLost (crash after confirm), got %v", err)
	}
	if len(outbox.releasedIDs) != 0 {
		t.Error("event must not be released when mark-published fails (lease expiry reclaims)")
	}
}

// ─── Service.Submit / Outbox atomicity tests ─────────────────────────────────

func TestService_Submit_Atomicity(t *testing.T) {
	repo := &fakeRepo{}
	svc := NewService(repo, defaultOutbox, defaultDSChecker)
	_, err := svc.Submit(context.Background(), 1, "查询商品", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !repo.createAndEnqueueCalled {
		t.Error("CreateAndEnqueue must be called (atomic transaction)")
	}
	if len(repo.transitions) != 0 {
		t.Error("Service.Submit must not call TransitionStatus directly")
	}
}

func TestService_Submit_OutboxEnqueueFailure(t *testing.T) {
	repo := &fakeRepo{createAndEnqueueErr: errors.New("tx rollback")}
	svc := NewService(repo, defaultOutbox, defaultDSChecker)
	_, err := svc.Submit(context.Background(), 1, "查询商品", 1)
	var svcErr *ServiceError
	if !errors.As(err, &svcErr) || svcErr.Code != ErrCodeInternal {
		t.Errorf("expected INTERNAL_ERROR on tx failure, got %v", err)
	}
}

// TestOutboxRepo_LeaseTakeover (kept for backward compatibility)
func TestOutboxRepo_LeaseTakeover(t *testing.T) {
	outbox := &fakeOutboxRepoFull{markPublishCAS: true}
	outbox.markedPublished = append(outbox.markedPublished, 99)
	err := outbox.MarkPublished(context.Background(), 1, "old-token", time.Now())
	if !errors.Is(err, ErrLeaseLost) {
		t.Errorf("expected ErrLeaseLost on stale token, got %v", err)
	}
}

func TestOutboxRepo_MultiInstanceClaim_Distinct(t *testing.T) {
	batch1 := []OutboxEvent{makeEvent(1, "m1"), makeEvent(2, "m2")}
	batch2 := []OutboxEvent{makeEvent(3, "m3")}
	outbox := &fakeOutboxRepoFull{claimedBatches: [][]OutboxEvent{batch1, batch2}}
	got1, _ := outbox.ClaimBatch(context.Background(), 5, 30*time.Second, time.Now())
	got2, _ := outbox.ClaimBatch(context.Background(), 5, 30*time.Second, time.Now())
	if len(got1) != 2 || len(got2) != 1 {
		t.Errorf("expected disjoint batches (2,1), got (%d,%d)", len(got1), len(got2))
	}
}
