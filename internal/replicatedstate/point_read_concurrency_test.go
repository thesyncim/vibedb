package replicatedstate

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

// blockingPointReadValidator makes the read-side critical section observable
// without adding a hook to production durable collections. Its ownership
// method is called while Machine.mu is held and its mutation methods remain
// the ordinary deterministic fixture validator.
type blockingPointReadValidator struct {
	acceptAllMutationValidator
	blockKey      string
	entered       chan struct{}
	secondEntered chan struct{}
	enteredOnce   sync.Once
	secondOnce    sync.Once
	release       chan struct{}
	releaseOnce   sync.Once
	active        atomic.Int32
	maximumActive atomic.Int32
}

func newBlockingPointReadValidator(blockKey string) *blockingPointReadValidator {
	return &blockingPointReadValidator{
		blockKey:      blockKey,
		entered:       make(chan struct{}),
		secondEntered: make(chan struct{}),
		release:       make(chan struct{}),
	}
}

func (validator *blockingPointReadValidator) ValidatePointOwnership(
	key []byte, _ distribution.KeyRange,
) MutationValidation {
	if validator.blockKey == "" || string(key) != validator.blockKey {
		return MutationValidationAccept
	}
	active := validator.active.Add(1)
	for {
		maximum := validator.maximumActive.Load()
		if active <= maximum || validator.maximumActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	validator.enteredOnce.Do(func() { close(validator.entered) })
	if active >= 2 {
		validator.secondOnce.Do(func() { close(validator.secondEntered) })
	}
	<-validator.release
	validator.active.Add(-1)
	return MutationValidationAccept
}

func (validator *blockingPointReadValidator) unblock() {
	validator.releaseOnce.Do(func() { close(validator.release) })
}

func pointReadFixture(t testing.TB) machineFixture {
	t.Helper()
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":1}`),
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatalf("seed point row: %v", err)
	}
	return fixture
}

func pointReadResult(fixture machineFixture, key string) (PointReadResult, error) {
	return fixture.machine.PointReadInto(
		1, []byte(key), fixture.machine.Published().Applied,
		fixture.user.Limits.MaxDocumentBytes, nil,
	)
}

func waitPointReadSignal(t *testing.T, signal <-chan struct{}, context string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", context)
	}
}

func TestPointReadIntoAllowsIndependentReaders(t *testing.T) {
	fixture := pointReadFixture(t)
	validator := newBlockingPointReadValidator("same")
	fixture.machine.relations[0].target.Validator = validator

	results := make(chan PointReadResult, 2)
	errs := make(chan error, 2)
	var readers sync.WaitGroup
	t.Cleanup(func() {
		validator.unblock()
		readers.Wait()
	})
	read := func() {
		defer readers.Done()
		result, err := fixture.machine.PointReadInto(
			1, []byte("same"), fixture.machine.Published().Applied,
			fixture.user.Limits.MaxDocumentBytes, nil,
		)
		results <- result
		errs <- err
	}
	readers.Add(1)
	go read()
	waitPointReadSignal(t, validator.entered, "first point reader")
	readers.Add(1)
	go read()
	waitPointReadSignal(t, validator.secondEntered, "second point reader")
	validator.unblock()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("point reader error: %v", err)
		}
		result := <-results
		if result.Found || len(result.Value) != 0 {
			t.Fatalf("missing point result = %+v", result)
		}
	}
	if got := validator.maximumActive.Load(); got < 2 {
		t.Fatalf("maximum concurrent point readers = %d, want at least 2", got)
	}
}

func TestPointReadIntoHoldsCoherentCutAcrossApply(t *testing.T) {
	fixture := pointReadFixture(t)
	validator := newBlockingPointReadValidator("k")
	fixture.machine.relations[0].target.Validator = validator
	minimum := fixture.machine.Published().Applied
	readDone := make(chan struct{})
	var readResult PointReadResult
	var readErr error
	var operations sync.WaitGroup
	operations.Add(1)
	t.Cleanup(func() {
		validator.unblock()
		operations.Wait()
	})
	go func() {
		defer operations.Done()
		readResult, readErr = fixture.machine.PointReadInto(
			1, []byte("k"), minimum, fixture.user.Limits.MaxDocumentBytes, nil,
		)
		close(readDone)
	}()
	waitPointReadSignal(t, validator.entered, "coherent point reader")

	command := testCommand(fixture.binding, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":2}`),
	})
	applyDone := make(chan struct{})
	var applied raftmodel.Publication
	var applyErr error
	operations.Add(1)
	go func() {
		defer operations.Done()
		applied, applyErr = fixture.machine.ApplyNormal(normalMeta(4), command)
		close(applyDone)
	}()
	select {
	case <-applyDone:
		t.Fatal("apply completed while point reader still held its cut")
	case <-time.After(100 * time.Millisecond):
	}

	validator.unblock()
	waitPointReadSignal(t, readDone, "coherent point reader completion")
	waitPointReadSignal(t, applyDone, "apply completion")
	if readErr != nil || !readResult.Found || !bytes.Equal(readResult.Value, []byte(`{"n":1}`)) {
		t.Fatalf("point result=%+v err=%v, want pre-apply row", readResult, readErr)
	}
	if readResult.Fence.Applied != minimum {
		t.Fatalf("point fence applied=%d, want %d", readResult.Fence.Applied, minimum)
	}
	if applyErr != nil || applied.Applied != 4 {
		t.Fatalf("apply publication=%+v err=%v", applied, applyErr)
	}
	result, err := fixture.machine.PointReadInto(
		1, []byte("k"), applied.Applied, fixture.user.Limits.MaxDocumentBytes, nil,
	)
	if err != nil || !result.Found || !bytes.Equal(result.Value, []byte(`{"n":2}`)) {
		t.Fatalf("post-apply point result=%+v err=%v", result, err)
	}
}

func TestPointReadIntoPublishesPoisonBeforeReleasingSharedReader(t *testing.T) {
	fixture := pointReadFixture(t)
	validator := newBlockingPointReadValidator("hold")
	fixture.machine.relations[0].target.Validator = validator
	intentKey, err := TransactionIntentStorageKey(1, []byte("bad"))
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(intentKey[:], []byte("corrupt-intent"))
	}); err != nil {
		t.Fatalf("install corrupt intent: %v", err)
	}

	heldDone := make(chan struct{})
	var operations sync.WaitGroup
	operations.Add(1)
	t.Cleanup(func() {
		validator.unblock()
		operations.Wait()
	})
	go func() {
		defer operations.Done()
		_, _ = pointReadResult(fixture, "hold")
		close(heldDone)
	}()
	waitPointReadSignal(t, validator.entered, "shared point reader")

	fatalDone := make(chan struct{})
	var fatalErr error
	operations.Add(1)
	go func() {
		defer operations.Done()
		_, fatalErr = pointReadResult(fixture, "bad")
		close(fatalDone)
	}()
	waitPointReadSignal(t, fatalDone, "fatal point reader")
	if !errors.Is(fatalErr, ErrTransactionStateCorrupt) {
		t.Fatalf("fatal point read error=%v, want transaction corruption", fatalErr)
	}
	firstPoison := fixture.machine.poisonError()
	if !errors.Is(firstPoison, ErrTransactionStateCorrupt) {
		t.Fatal("fatal point reader returned before publishing poison")
	}
	if got := fixture.machine.fail(ErrStateCorrupt); got != ErrStateCorrupt {
		t.Fatalf("second poison return error = %v, want supplied error", got)
	}
	if got := fixture.machine.poisonError(); got != firstPoison {
		t.Fatalf("poison cause changed from %v to %v", firstPoison, got)
	}
	if got := fixture.machine.CheckpointAppliedIndex(); got != 0 {
		t.Fatalf("poisoned checkpoint applied index=%d, want 0", got)
	}

	before := fixture.machine.Published()
	applyDone := make(chan struct{})
	var applied raftmodel.Publication
	var applyErr error
	command := testCommand(fixture.binding, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("after-poison"), Value: []byte(`{"n":3}`),
	})
	operations.Add(1)
	go func() {
		defer operations.Done()
		applied, applyErr = fixture.machine.ApplyNormal(normalMeta(4), command)
		close(applyDone)
	}()
	select {
	case <-applyDone:
		t.Fatal("apply unexpectedly completed before shared reader release")
	case <-time.After(100 * time.Millisecond):
	}
	validator.unblock()
	waitPointReadSignal(t, heldDone, "shared point reader completion")
	waitPointReadSignal(t, applyDone, "poisoned apply completion")
	if !errors.Is(applyErr, ErrApplyPoisoned) {
		t.Fatalf("queued apply error=%v, want ErrApplyPoisoned", applyErr)
	}
	after := fixture.machine.Published()
	if applied != (raftmodel.Publication{}) || after != before {
		t.Fatalf("poisoned apply changed publication: returned=%+v before=%+v after=%+v", applied, before, after)
	}
}
