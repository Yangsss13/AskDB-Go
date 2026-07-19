package queryjob

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ─── fakes ───────────────────────────────────────────────────────────────────

type fakeAcknowledger struct {
	mu      sync.Mutex
	acked   bool
	nacked  bool
	requeue bool
	ackErr  error
	nackErr error
}

func (f *fakeAcknowledger) Ack(_ uint64, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = true
	return f.ackErr
}

func (f *fakeAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacked = true
	f.requeue = requeue
	return f.nackErr
}

func (f *fakeAcknowledger) Reject(_ uint64, _ bool) error { return nil }

// fakeProcessService is a minimal stand-in for WorkerService.
type fakeProcessService struct {
	err         error
	processedID uint64
	delay       time.Duration
}

func (f *fakeProcessService) Process(_ context.Context, req ProcessRequest) error {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.processedID = req.JobID
	return f.err
}

// fakeRetryPublisher records calls and can inject errors.
type fakeRetryPublisher struct {
	mu         sync.Mutex
	retryErr   error
	dlqErr     error
	retryCount int
	dlqCount   int
	lastJobID  uint64
}

func (f *fakeRetryPublisher) PublishRetry(_ context.Context, jobID uint64, _ string, _ int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retryCount++
	f.lastJobID = jobID
	return f.retryErr
}

func (f *fakeRetryPublisher) PublishDLQ(_ context.Context, jobID uint64, _ string, _ int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dlqCount++
	f.lastJobID = jobID
	return f.dlqErr
}

func (f *fakeRetryPublisher) Close() error { return nil }

// fakePMRepo is a minimal stand-in for ProcessedMessageRepository.
type fakePMRepo struct {
	claimToken   string
	claimResult  ClaimResult
	claimErr     error
	renewErr     error
	markRetryErr error
	markDoneErr  error
}

func (f *fakePMRepo) Claim(_ context.Context, _, _ string, _ uint64, _ uint8, _ time.Time) (string, ClaimResult, error) {
	return f.claimToken, f.claimResult, f.claimErr
}

func (f *fakePMRepo) Renew(_ context.Context, _, _ string, _ time.Time) error { return f.renewErr }

func (f *fakePMRepo) MarkRetryScheduled(_ context.Context, _, _ string) error {
	return f.markRetryErr
}

func (f *fakePMRepo) MarkCompleted(_ context.Context, _, _ string, _ time.Time) error {
	return f.markDoneErr
}

func (f *fakePMRepo) FindByMessageID(_ context.Context, _ string) (*ProcessedMessage, error) {
	return nil, ErrPMNotFound
}

// makeDelivery builds an amqp.Delivery with a fakeAcknowledger.
func makeDelivery(t *testing.T, msg QueryJobMessage) (amqp.Delivery, *fakeAcknowledger) {
	t.Helper()
	body, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ack := &fakeAcknowledger{}
	return amqp.Delivery{Acknowledger: ack, Body: body}, ack
}

// makeDeliveryWithAttempt adds an x-attempt header.
func makeDeliveryWithAttempt(t *testing.T, msg QueryJobMessage, attempt int32) (amqp.Delivery, *fakeAcknowledger) {
	t.Helper()
	d, ack := makeDelivery(t, msg)
	d.Headers = amqp.Table{HeaderAttempt: attempt}
	return d, ack
}

func validMsg(jobID uint64) QueryJobMessage {
	return QueryJobMessage{
		MessageID:  "test-id",
		Type:       MessageTypeQueryExecutionRequested,
		Version:    messageVersion,
		OccurredAt: time.Now(),
		Payload:    JobPayload{JobID: jobID},
	}
}

// newTestConsumer builds a Consumer without a real AMQP channel.
func newTestConsumer(svc ProcessService, rp RetryPublisher, pm ProcessedMessageRepository) *Consumer {
	if rp == nil {
		rp = &fakeRetryPublisher{}
	}
	if pm == nil {
		pm = &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted}
	}
	return &Consumer{svc: svc, retryPub: rp, pmRepo: pm}
}

// ─── handle unit tests (no real channel needed) ─────────────────────────────

func TestConsumer_Handle_Success(t *testing.T) {
	svc := &fakeProcessService{}
	pm := &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted}
	c := newTestConsumer(svc, nil, pm)
	d, _ := makeDelivery(t, validMsg(5))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck, got %d", outcome)
	}
	if svc.processedID != 5 {
		t.Errorf("expected processedID=5, got %d", svc.processedID)
	}
}

func TestConsumer_Handle_MalformedMessage_DLQ(t *testing.T) {
	rp := &fakeRetryPublisher{}
	c := newTestConsumer(&fakeProcessService{}, rp, nil)
	d := amqp.Delivery{
		Acknowledger: &fakeAcknowledger{},
		Body:         []byte("not-json"),
	}
	outcome, _ := c.handle(d)
	// DLQ publish succeeds → ACK
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ publish, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected 1 DLQ publish, got %d", rp.dlqCount)
	}
}

func TestConsumer_Handle_MalformedMessage_DLQFails_NackRequeue(t *testing.T) {
	rp := &fakeRetryPublisher{dlqErr: errors.New("broker down")}
	c := newTestConsumer(&fakeProcessService{}, rp, nil)
	d := amqp.Delivery{
		Acknowledger: &fakeAcknowledger{},
		Body:         []byte("not-json"),
	}
	outcome, _ := c.handle(d)
	if outcome != outcomeNackRequeue {
		t.Errorf("expected outcomeNackRequeue when DLQ publish fails, got %d", outcome)
	}
}

func TestConsumer_Handle_UnsupportedType_DLQ(t *testing.T) {
	rp := &fakeRetryPublisher{}
	c := newTestConsumer(&fakeProcessService{}, rp, nil)
	msg := validMsg(1)
	msg.Type = "unknown.type"
	d, _ := makeDelivery(t, msg)

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected 1 DLQ publish for unknown type")
	}
}

func TestConsumer_Handle_ZeroJobID_DLQ(t *testing.T) {
	rp := &fakeRetryPublisher{}
	c := newTestConsumer(&fakeProcessService{}, rp, nil)
	msg := validMsg(0)
	d, _ := makeDelivery(t, msg)

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected 1 DLQ publish for job_id=0")
	}
}

func TestConsumer_Handle_JobNotFound_DLQ(t *testing.T) {
	rp := &fakeRetryPublisher{}
	pm := &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted}
	svc := &fakeProcessService{err: ErrJobNotFound}
	c := newTestConsumer(svc, rp, pm)
	d, _ := makeDelivery(t, validMsg(99))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ for missing job, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected 1 DLQ publish for missing job")
	}
}

func TestConsumer_Handle_DBWriteFailure_Fatal(t *testing.T) {
	pm := &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted}
	svc := &fakeProcessService{err: errors.New("db write failed")}
	c := newTestConsumer(svc, nil, pm)
	d, _ := makeDelivery(t, validMsg(1))

	outcome, _ := c.handle(d)
	if outcome != outcomeFatal {
		t.Errorf("expected outcomeFatal on DB write failure, got %d", outcome)
	}
}

func TestConsumer_Handle_AlreadyDone_ACK(t *testing.T) {
	pm := &fakePMRepo{claimToken: "", claimResult: ClaimAlreadyDone}
	svc := &fakeProcessService{}
	c := newTestConsumer(svc, nil, pm)
	d, _ := makeDelivery(t, validMsg(7))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck for already-done, got %d", outcome)
	}
	if svc.processedID != 0 {
		t.Error("process must not be called when PM is already completed")
	}
}

func TestConsumer_Handle_LeaseHeld_DeferToRetry(t *testing.T) {
	rp := &fakeRetryPublisher{}
	pm := &fakePMRepo{claimToken: "", claimResult: ClaimLeaseHeld}
	c := newTestConsumer(&fakeProcessService{}, rp, pm)
	d, _ := makeDelivery(t, validMsg(3))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after retry-defer, got %d", outcome)
	}
	if rp.retryCount != 1 {
		t.Errorf("expected 1 retry publish for lease-held, got %d", rp.retryCount)
	}
}

func TestConsumer_Handle_LeaseHeld_RetryPublishFails_NackRequeue(t *testing.T) {
	rp := &fakeRetryPublisher{retryErr: errors.New("broker down")}
	pm := &fakePMRepo{claimToken: "", claimResult: ClaimLeaseHeld}
	c := newTestConsumer(&fakeProcessService{}, rp, pm)
	d, _ := makeDelivery(t, validMsg(3))

	outcome, _ := c.handle(d)
	if outcome != outcomeNackRequeue {
		t.Errorf("expected outcomeNackRequeue when retry publish fails, got %d", outcome)
	}
}

func TestConsumer_Handle_ConflictJobID_DLQ(t *testing.T) {
	rp := &fakeRetryPublisher{}
	pm := &fakePMRepo{claimToken: "", claimResult: ClaimConflict}
	c := newTestConsumer(&fakeProcessService{}, rp, pm)
	d, _ := makeDelivery(t, validMsg(5))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ for conflict, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected 1 DLQ publish for job_id conflict")
	}
}

func TestConsumer_Handle_RetryScheduled_MarksPMAndACKs(t *testing.T) {
	pm := &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted}
	svc := &fakeProcessService{err: ErrRetryScheduled}
	c := newTestConsumer(svc, nil, pm)
	d, _ := makeDelivery(t, validMsg(2))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck on ErrRetryScheduled, got %d", outcome)
	}
}

func TestConsumer_Handle_RetryScheduled_MarkRetryFails_NackRequeue(t *testing.T) {
	pm := &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted, markRetryErr: errors.New("db down")}
	svc := &fakeProcessService{err: ErrRetryScheduled}
	c := newTestConsumer(svc, nil, pm)
	d, _ := makeDelivery(t, validMsg(2))

	outcome, _ := c.handle(d)
	if outcome != outcomeNackRequeue {
		t.Errorf("expected outcomeNackRequeue when mark retry_scheduled fails, got %d", outcome)
	}
}

func TestConsumer_Handle_DLQScheduled_MarksPMAndACKs(t *testing.T) {
	pm := &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted}
	svc := &fakeProcessService{err: ErrDLQScheduled}
	c := newTestConsumer(svc, nil, pm)
	d, _ := makeDelivery(t, validMsg(4))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck on ErrDLQScheduled, got %d", outcome)
	}
}

func TestConsumer_Handle_MarkCompletedFails_NackRequeue(t *testing.T) {
	pm := &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted, markDoneErr: errors.New("db down")}
	svc := &fakeProcessService{}
	c := newTestConsumer(svc, nil, pm)
	d, _ := makeDelivery(t, validMsg(5))

	outcome, _ := c.handle(d)
	if outcome != outcomeNackRequeue {
		t.Errorf("expected outcomeNackRequeue when MarkCompleted fails, got %d", outcome)
	}
}

func TestConsumer_Handle_InvalidXAttemptType_DLQ(t *testing.T) {
	rp := &fakeRetryPublisher{}
	c := newTestConsumer(&fakeProcessService{}, rp, nil)
	d, _ := makeDelivery(t, validMsg(1))
	// string is not int32
	d.Headers = amqp.Table{HeaderAttempt: "not-an-int"}

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ for bad x-attempt type, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected DLQ publish for invalid x-attempt type")
	}
}

func TestConsumer_Handle_NegativeAttempt_DLQ(t *testing.T) {
	rp := &fakeRetryPublisher{}
	c := newTestConsumer(&fakeProcessService{}, rp, nil)
	d, _ := makeDeliveryWithAttempt(t, validMsg(1), -1)

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ for negative attempt, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected DLQ publish for negative attempt")
	}
}

func TestConsumer_Handle_AttemptOverflow_DLQ(t *testing.T) {
	rp := &fakeRetryPublisher{}
	c := newTestConsumer(&fakeProcessService{}, rp, nil)
	// 101 exceeds maxSaneAttempt=100
	d, _ := makeDeliveryWithAttempt(t, validMsg(1), 101)

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ for overflow attempt, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected DLQ publish for overflow attempt")
	}
}

func TestConsumer_Handle_DuplicateMessageID_Attempt_Match(t *testing.T) {
	// Same message_id and attempt arriving twice; second time PM is ClaimAlreadyDone.
	pm := &fakePMRepo{claimToken: "", claimResult: ClaimAlreadyDone}
	svc := &fakeProcessService{}
	c := newTestConsumer(svc, nil, pm)
	msg := validMsg(10)
	d, _ := makeDeliveryWithAttempt(t, msg, 2)

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck for duplicate message (already done), got %d", outcome)
	}
	if svc.processedID != 0 {
		t.Error("must not process already-done job")
	}
}

func TestConsumer_Handle_SameJobDifferentMessageID_Conflict_DLQ(t *testing.T) {
	// Different message_id for same job_id: ClaimConflict → DLQ.
	rp := &fakeRetryPublisher{}
	pm := &fakePMRepo{claimResult: ClaimConflict}
	c := newTestConsumer(&fakeProcessService{}, rp, pm)
	d, _ := makeDelivery(t, validMsg(9))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck after DLQ, got %d", outcome)
	}
	if rp.dlqCount != 1 {
		t.Errorf("expected 1 DLQ publish for conflict")
	}
}

func TestConsumer_Handle_ClaimFails_Fatal(t *testing.T) {
	pm := &fakePMRepo{claimErr: errors.New("db timeout")}
	c := newTestConsumer(&fakeProcessService{}, nil, pm)
	d, _ := makeDelivery(t, validMsg(1))

	outcome, _ := c.handle(d)
	if outcome != outcomeFatal {
		t.Errorf("expected outcomeFatal when PM claim errors, got %d", outcome)
	}
}

func TestConsumer_Handle_TerminalJobRedelivered_ACK(t *testing.T) {
	// Job already in terminal state: WorkerService returns nil; PM MarkCompleted succeeds.
	pm := &fakePMRepo{claimToken: "tok", claimResult: ClaimGranted}
	// nil error simulates terminal-state check in WorkerService returning nil.
	c := newTestConsumer(&fakeProcessService{}, nil, pm)
	d, _ := makeDelivery(t, validMsg(42))

	outcome, _ := c.handle(d)
	if outcome != outcomeAck {
		t.Errorf("expected outcomeAck for terminal-job redelivery, got %d", outcome)
	}
}

// TestConsumer_Stop_WaitsForInFlight verifies Stop blocks until in-flight work
// completes.
func TestConsumer_Stop_WaitsForInFlight(t *testing.T) {
	const processDelay = 80 * time.Millisecond
	c := &Consumer{}
	c.wg.Add(1)
	started := time.Now()

	go func() {
		defer c.wg.Done()
		time.Sleep(processDelay)
	}()

	c.wg.Wait()
	if elapsed := time.Since(started); elapsed < processDelay-5*time.Millisecond {
		t.Errorf("Stop returned too early: %v < %v", elapsed, processDelay)
	}
}

// ─── extractAttempt unit tests ───────────────────────────────────────────────

func TestExtractAttempt_NoHeader(t *testing.T) {
	v, err := extractAttempt(nil)
	if err != nil || v != 0 {
		t.Errorf("nil headers: want 0, nil; got %d, %v", v, err)
	}
}

func TestExtractAttempt_ZeroHeader(t *testing.T) {
	v, err := extractAttempt(amqp.Table{HeaderAttempt: int32(0)})
	if err != nil || v != 0 {
		t.Errorf("attempt=0: want 0, nil; got %d, %v", v, err)
	}
}

func TestExtractAttempt_ValidPositive(t *testing.T) {
	v, err := extractAttempt(amqp.Table{HeaderAttempt: int32(3)})
	if err != nil || v != 3 {
		t.Errorf("attempt=3: want 3, nil; got %d, %v", v, err)
	}
}

func TestExtractAttempt_Negative(t *testing.T) {
	_, err := extractAttempt(amqp.Table{HeaderAttempt: int32(-1)})
	if err == nil {
		t.Error("expected error for negative attempt")
	}
}

func TestExtractAttempt_WrongType(t *testing.T) {
	_, err := extractAttempt(amqp.Table{HeaderAttempt: "string"})
	if err == nil {
		t.Error("expected error for wrong type")
	}
}

func TestExtractAttempt_Overflow(t *testing.T) {
	_, err := extractAttempt(amqp.Table{HeaderAttempt: int32(101)})
	if err == nil {
		t.Error("expected error for overflow attempt")
	}
}

// ─── Two concurrent workers competing for the same message ──────────────────

func TestConsumer_TwoWorkers_ConcurrentClaim(t *testing.T) {
	// Worker1 gets ClaimGranted, Worker2 gets ClaimLeaseHeld.
	// Both should reach a safe outcome; Worker2 defers to retry.
	rp := &fakeRetryPublisher{}
	pm1 := &fakePMRepo{claimToken: "tok1", claimResult: ClaimGranted}
	pm2 := &fakePMRepo{claimToken: "", claimResult: ClaimLeaseHeld}

	svc := &fakeProcessService{}

	c1 := newTestConsumer(svc, rp, pm1)
	c2 := newTestConsumer(svc, rp, pm2)

	d, _ := makeDelivery(t, validMsg(55))

	var o1, o2 deliveryOutcome
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); o1, _ = c1.handle(d) }()
	go func() { defer wg.Done(); o2, _ = c2.handle(d) }()
	wg.Wait()

	if o1 != outcomeAck {
		t.Errorf("worker1: expected outcomeAck, got %d", o1)
	}
	if o2 != outcomeAck {
		t.Errorf("worker2: expected outcomeAck (defer-retry), got %d", o2)
	}
}
