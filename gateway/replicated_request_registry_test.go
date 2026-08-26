package gateway

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type registryTestOrchestrator struct {
	executeCalls atomic.Int64
	recoverCalls atomic.Int64
	execute      func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error)
	recover      func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error)
}

func (orchestrator *registryTestOrchestrator) Execute(
	ctx context.Context,
	generation uint64,
	participants []ReplicatedTransactionParticipant,
) (ReplicatedTransactionResult, error) {
	orchestrator.executeCalls.Add(1)
	return orchestrator.execute(ctx, generation, participants)
}

func (orchestrator *registryTestOrchestrator) Recover(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
) (ReplicatedTransactionResult, error) {
	orchestrator.recoverCalls.Add(1)
	return orchestrator.recover(ctx, handle)
}

func registryTestID(value byte) replication.ID128 {
	var id replication.ID128
	id[0] = value
	return id
}

func registryTestDigest(value byte) replication.Digest {
	var digest replication.Digest
	digest[0] = value
	return digest
}

func registryAuthorityContext(t *testing.T, node byte, generation uint64) context.Context {
	t.Helper()
	ctx, err := serviceauthz.WithAuthority(context.Background(), serviceauthz.Authority{
		Node: rafttransport.NodeID{node}, Generation: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func newRegistryForTest(
	t *testing.T,
	orchestrator ReplicatedTransactionRequestOrchestrator,
	maximum int,
) *ReplicatedTransactionRequestRegistry {
	t.Helper()
	registry, err := NewReplicatedTransactionRequestRegistry(
		ReplicatedTransactionRequestRegistryOptions{
			Orchestrator: orchestrator, MaxEntries: maximum,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func registryUnknownOutcome(
	handle *ReplicatedTransactionRecoveryHandle,
	cause error,
) (ReplicatedTransactionResult, error) {
	return ReplicatedTransactionResult{Recovery: handle}, &ReplicatedTransactionError{
		Recovery: handle, Cause: cause,
	}
}

func TestReplicatedTransactionRequestRegistryCoalescesConcurrentExactDuplicates(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			close(started)
			<-release
			return ReplicatedTransactionResult{Committed: true, AffectedRows: 17}, nil
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			t.Fatal("unexpected recovery")
			return ReplicatedTransactionResult{}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 8)
	id, digest := registryTestID(1), registryTestDigest(2)

	const callers = 48
	results := make(chan ReplicatedTransactionRequestOutcome, callers)
	errs := make(chan error, callers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			result, err := registry.Execute(context.Background(), id, digest, 3, nil)
			results <- result
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	<-started
	for {
		if registry.Stats().Waiting == callers-1 {
			break
		}
		runtime.Gosched()
	}
	close(release)
	for range callers {
		result := <-results
		if err := <-errs; err != nil || !result.Committed || result.AffectedRows != 17 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if calls := orchestrator.executeCalls.Load(); calls != 1 {
		t.Fatalf("execute calls=%d, want 1", calls)
	}
	if got := registry.Stats(); got.Entries != 1 || got.Terminal != 1 {
		t.Fatalf("stats=%+v", got)
	}
}

func TestReplicatedTransactionRequestRegistryRejectsDigestConflictBeforeOrchestration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			close(started)
			<-release
			return ReplicatedTransactionResult{Committed: true}, nil
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			t.Fatal("unexpected recovery")
			return ReplicatedTransactionResult{}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id := registryTestID(3)
	done := make(chan error, 1)
	go func() {
		_, err := registry.Execute(context.Background(), id, registryTestDigest(4), 1, nil)
		done <- err
	}()
	<-started
	if _, err := registry.Execute(
		context.Background(), id, registryTestDigest(5), 1, nil,
	); !errors.Is(err, ErrReplicatedTransactionRequestConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	if calls := orchestrator.executeCalls.Load(); calls != 1 {
		t.Fatalf("execute calls=%d, want 1", calls)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(
		context.Background(), id, registryTestDigest(5), 1, nil,
	); !errors.Is(err, ErrReplicatedTransactionRequestConflict) {
		t.Fatalf("terminal conflict error=%v", err)
	}
}

func TestReplicatedTransactionRequestRegistryStrictCapacityAndResolvedForget(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	orchestrator := &registryTestOrchestrator{}
	orchestrator.execute = func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
		if orchestrator.executeCalls.Load() == 1 {
			close(started)
			<-release
		}
		return ReplicatedTransactionResult{Committed: true}, nil
	}
	orchestrator.recover = func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
		t.Fatal("unexpected recovery")
		return ReplicatedTransactionResult{}, nil
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id1, digest1 := registryTestID(6), registryTestDigest(7)
	id2, digest2 := registryTestID(8), registryTestDigest(9)
	done := make(chan error, 1)
	go func() {
		_, err := registry.Execute(context.Background(), id1, digest1, 1, nil)
		done <- err
	}()
	<-started
	if err := registry.Forget(context.Background(), id1, digest1); !errors.Is(err, ErrReplicatedTransactionRequestUnresolved) {
		t.Fatalf("active forget error=%v", err)
	}
	if _, err := registry.Execute(
		context.Background(), id2, digest2, 1, nil,
	); !errors.Is(err, ErrReplicatedTransactionRequestCapacity) {
		t.Fatalf("capacity error=%v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(
		context.Background(), id2, digest2, 1, nil,
	); !errors.Is(err, ErrReplicatedTransactionRequestCapacity) {
		t.Fatalf("retained terminal capacity error=%v", err)
	}
	if err := registry.Forget(context.Background(), id1, registryTestDigest(10)); !errors.Is(err, ErrReplicatedTransactionRequestConflict) {
		t.Fatalf("wrong-digest forget error=%v", err)
	}
	if err := registry.Forget(context.Background(), id1, digest1); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(context.Background(), id2, digest2, 1, nil); err != nil {
		t.Fatal(err)
	}
	if calls := orchestrator.executeCalls.Load(); calls != 2 {
		t.Fatalf("execute calls=%d, want 2", calls)
	}
}

func TestReplicatedTransactionRequestRegistryDoesNotCacheUnprovedPreAdmissionError(t *testing.T) {
	wantErr := errors.New("transient pre-admission refusal")
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			return ReplicatedTransactionResult{}, wantErr
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			t.Fatal("unexpected recovery")
			return ReplicatedTransactionResult{}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(11), registryTestDigest(12)
	for range 2 {
		if _, err := registry.Execute(context.Background(), id, digest, 1, nil); err != wantErr {
			t.Fatalf("execution error=%v, want exact %v", err, wantErr)
		}
	}
	if calls := orchestrator.executeCalls.Load(); calls != 2 {
		t.Fatalf("execute calls=%d, want 2", calls)
	}
	if stats := registry.Stats(); stats.Entries != 0 {
		t.Fatalf("pre-admission error retained: %+v", stats)
	}
}

func TestReplicatedTransactionRequestRegistryScopesIdentityByPrincipalNotPolicyGeneration(t *testing.T) {
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			return ReplicatedTransactionResult{Committed: true, AffectedRows: 1}, nil
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			t.Fatal("unexpected recovery")
			return ReplicatedTransactionResult{}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 4)
	id, digest := registryTestID(91), registryTestDigest(92)
	first := registryAuthorityContext(t, 1, 7)
	rotated := registryAuthorityContext(t, 1, 8)
	foreign := registryAuthorityContext(t, 2, 8)
	if _, err := registry.Execute(first, id, digest, 11, nil); err != nil {
		t.Fatal(err)
	}
	if outcome, found, err := registry.Replay(rotated, id, digest); err != nil ||
		!found || !outcome.Committed || outcome.CatalogGeneration != 11 {
		t.Fatalf("rotated replay outcome=%+v found=%v err=%v", outcome, found, err)
	}
	if _, found, err := registry.Replay(rotated, id, registryTestDigest(99)); !found || !errors.Is(err, ErrReplicatedTransactionRequestConflict) {
		t.Fatalf("replay conflict found=%v err=%v", found, err)
	}
	if _, found, err := registry.Replay(foreign, id, digest); err != nil || found {
		t.Fatalf("foreign replay found=%v err=%v", found, err)
	}
	if _, err := registry.Execute(foreign, id, digest, 12, nil); err != nil {
		t.Fatal(err)
	}
	local := WithLocalReplicatedTransactionRequestScope(first)
	if _, found, err := registry.Replay(local, id, digest); err != nil || found {
		t.Fatalf("local replay found=%v err=%v", found, err)
	}
	if _, err := registry.Execute(local, id, digest, 13, nil); err != nil {
		t.Fatal(err)
	}
	if _, found, err := registry.Replay(context.Background(), id, digest); err != nil || !found {
		t.Fatalf("implicit local replay found=%v err=%v", found, err)
	}
	if calls := orchestrator.executeCalls.Load(); calls != 3 {
		t.Fatalf("execute calls=%d, want two principals plus isolated local scope", calls)
	}
}

func TestReplicatedTransactionRequestRegistryReplayJoinsExecutingCall(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			close(started)
			<-release
			return ReplicatedTransactionResult{Committed: true, AffectedRows: 5}, nil
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			t.Fatal("unexpected recovery")
			return ReplicatedTransactionResult{}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(95), registryTestDigest(96)
	executeDone := make(chan error, 1)
	go func() {
		_, err := registry.Execute(context.Background(), id, digest, 21,
			[]ReplicatedTransactionParticipant{{}, {}})
		executeDone <- err
	}()
	<-started
	type replayResult struct {
		outcome ReplicatedTransactionRequestOutcome
		found   bool
		err     error
	}
	replayDone := make(chan replayResult, 1)
	go func() {
		outcome, found, err := registry.Replay(context.Background(), id, digest)
		replayDone <- replayResult{outcome: outcome, found: found, err: err}
	}()
	for registry.Stats().Waiting != 1 {
		runtime.Gosched()
	}
	close(release)
	if err := <-executeDone; err != nil {
		t.Fatal(err)
	}
	replayed := <-replayDone
	if replayed.err != nil || !replayed.found || !replayed.outcome.Committed ||
		replayed.outcome.AffectedRows != 5 || replayed.outcome.CatalogGeneration != 21 ||
		replayed.outcome.ShardsFanned != 2 {
		t.Fatalf("replay=%+v", replayed)
	}
	if calls := orchestrator.executeCalls.Load(); calls != 1 {
		t.Fatalf("execute calls=%d, want 1", calls)
	}
}

func TestReplicatedTransactionRequestRegistrySharesTransientFailureThenAllowsRetry(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	transient := errors.New("proposal admission unavailable")
	orchestrator := &registryTestOrchestrator{}
	orchestrator.execute = func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
		if orchestrator.executeCalls.Load() == 1 {
			close(started)
			<-release
			return ReplicatedTransactionResult{}, transient
		}
		return ReplicatedTransactionResult{Committed: true, AffectedRows: 3}, nil
	}
	orchestrator.recover = func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
		t.Fatal("unexpected recovery")
		return ReplicatedTransactionResult{}, nil
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(93), registryTestDigest(94)
	const callers = 24
	errs := make(chan error, callers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			_, err := registry.Execute(context.Background(), id, digest, 1, nil)
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	<-started
	for {
		if registry.Stats().Waiting == callers-1 {
			break
		}
		runtime.Gosched()
	}
	close(release)
	for range callers {
		if err := <-errs; err != transient {
			t.Fatalf("coalesced transient error=%v, want exact %v", err, transient)
		}
	}
	if calls := orchestrator.executeCalls.Load(); calls != 1 {
		t.Fatalf("transient execute calls=%d, want 1", calls)
	}
	if stats := registry.Stats(); stats.Entries != 0 {
		t.Fatalf("transient entry retained: %+v", stats)
	}
	if outcome, err := registry.Execute(context.Background(), id, digest, 1, nil); err != nil ||
		!outcome.Committed || outcome.AffectedRows != 3 {
		t.Fatalf("retry outcome=%+v err=%v", outcome, err)
	}
	if calls := orchestrator.executeCalls.Load(); calls != 2 {
		t.Fatalf("retry execute calls=%d, want 2", calls)
	}
}

func TestReplicatedTransactionRequestRegistryRetainsUnknownAndRetryRecovers(t *testing.T) {
	unknown := errors.New("outcome unknown")
	handle := new(ReplicatedTransactionRecoveryHandle)
	var recoveredHandle *ReplicatedTransactionRecoveryHandle
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			return registryUnknownOutcome(handle, unknown)
		},
		recover: func(_ context.Context, got *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			recoveredHandle = got
			return ReplicatedTransactionResult{Committed: true, AffectedRows: 23}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(13), registryTestDigest(14)
	result, err := registry.Execute(context.Background(), id, digest, 1, nil)
	if !errors.Is(err, unknown) || result.Recovery != nil {
		t.Fatalf("first result=%+v err=%v", result, err)
	}
	var transactionErr *ReplicatedTransactionError
	if !errors.As(err, &transactionErr) || transactionErr.Recovery != nil {
		t.Fatalf("public transaction error=%+v", transactionErr)
	}
	if got := registry.Stats(); got.PendingRecovery != 1 {
		t.Fatalf("pending stats=%+v", got)
	}
	result, err = registry.Execute(context.Background(), id, digest, 1, nil)
	if err != nil || !result.Committed || result.AffectedRows != 23 ||
		recoveredHandle != handle {
		t.Fatalf("recovered result=%+v err=%v handle=%p want=%p",
			result, err, recoveredHandle, handle)
	}
	if _, err = registry.Execute(context.Background(), id, digest, 1, nil); err != nil {
		t.Fatal(err)
	}
	if orchestrator.executeCalls.Load() != 1 || orchestrator.recoverCalls.Load() != 1 {
		t.Fatalf("execute=%d recover=%d", orchestrator.executeCalls.Load(), orchestrator.recoverCalls.Load())
	}
	if err = registry.Forget(context.Background(), id, digest); err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats(); got.Entries != 0 {
		t.Fatalf("stats after forget=%+v", got)
	}
}

func TestReplicatedTransactionRequestRegistryRetainsCommittedCleanupHandle(t *testing.T) {
	cleanup := errors.New("committed cleanup incomplete")
	handle := new(ReplicatedTransactionRecoveryHandle)
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			return ReplicatedTransactionResult{Committed: true, Recovery: handle},
				&ReplicatedTransactionError{Committed: true, Recovery: handle, Cause: cleanup}
		},
		recover: func(_ context.Context, got *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			if got != handle {
				t.Fatalf("recovery handle=%p, want %p", got, handle)
			}
			return ReplicatedTransactionResult{Committed: true, AffectedRows: 29}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(33), registryTestDigest(34)
	result, err := registry.Execute(context.Background(), id, digest, 1, nil)
	var transactionErr *ReplicatedTransactionError
	if !errors.Is(err, cleanup) || !errors.As(err, &transactionErr) ||
		!transactionErr.Committed || transactionErr.Recovery != nil || result.Recovery != nil {
		t.Fatalf("committed result=%+v error=%+v", result, transactionErr)
	}
	result, err = registry.Execute(context.Background(), id, digest, 1, nil)
	if err != nil || !result.Committed || result.AffectedRows != 29 {
		t.Fatalf("recovered result=%+v err=%v", result, err)
	}
}

func TestReplicatedTransactionRequestRegistryCoalescesConcurrentRecovery(t *testing.T) {
	unknown := errors.New("recoverable")
	handle := new(ReplicatedTransactionRecoveryHandle)
	recoverStarted := make(chan struct{})
	releaseRecover := make(chan struct{})
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			return registryUnknownOutcome(handle, unknown)
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			close(recoverStarted)
			<-releaseRecover
			return ReplicatedTransactionResult{Committed: true, AffectedRows: 31}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 2)
	id, digest := registryTestID(15), registryTestDigest(16)
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); !errors.Is(err, unknown) {
		t.Fatal(err)
	}
	const callers = 32
	results := make(chan ReplicatedTransactionRequestOutcome, callers)
	errs := make(chan error, callers)
	for range callers {
		go func() {
			result, err := registry.Execute(context.Background(), id, digest, 1, nil)
			results <- result
			errs <- err
		}()
	}
	<-recoverStarted
	close(releaseRecover)
	for range callers {
		result := <-results
		if err := <-errs; err != nil || !result.Committed || result.AffectedRows != 31 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if calls := orchestrator.recoverCalls.Load(); calls != 1 {
		t.Fatalf("recover calls=%d, want 1", calls)
	}
}

func TestReplicatedTransactionRequestRegistryCanceledWaiterDoesNotCancelOwner(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			close(started)
			<-release
			return ReplicatedTransactionResult{Committed: true}, nil
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			t.Fatal("unexpected recovery")
			return ReplicatedTransactionResult{}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(17), registryTestDigest(18)
	ownerDone := make(chan error, 1)
	go func() {
		_, err := registry.Execute(context.Background(), id, digest, 1, nil)
		ownerDone <- err
	}()
	<-started
	waiterContext, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := registry.Execute(waiterContext, id, digest, 1, nil)
		waiterDone <- err
	}()
	cancel()
	if err := <-waiterDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error=%v", err)
	}
	if got := registry.Stats(); got.Executing != 1 {
		t.Fatalf("owner was disturbed: %+v", got)
	}
	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); err != nil {
		t.Fatal(err)
	}
	if calls := orchestrator.executeCalls.Load(); calls != 1 {
		t.Fatalf("execute calls=%d, want 1", calls)
	}
}

func TestReplicatedTransactionRequestRegistryNeverHoldsLockAcrossOrchestrator(t *testing.T) {
	unknown := errors.New("unknown")
	handle := new(ReplicatedTransactionRecoveryHandle)
	var registry *ReplicatedTransactionRequestRegistry
	assertUnlocked := func(wantExecuting, wantRecovering int) {
		t.Helper()
		observed := make(chan ReplicatedTransactionRequestRegistryStats, 1)
		go func() { observed <- registry.Stats() }()
		select {
		case stats := <-observed:
			if stats.Executing != wantExecuting || stats.Recovering != wantRecovering {
				t.Fatalf("stats during orchestrator call=%+v", stats)
			}
		case <-time.After(time.Second):
			t.Fatal("registry lock held across orchestrator call")
		}
	}
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			assertUnlocked(1, 0)
			return registryUnknownOutcome(handle, unknown)
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			assertUnlocked(0, 1)
			return ReplicatedTransactionResult{Committed: true}, nil
		},
	}
	registry = newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(31), registryTestDigest(32)
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); !errors.Is(err, unknown) {
		t.Fatal(err)
	}
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedTransactionRequestRegistryRecoverPendingOneAttemptPerSweep(t *testing.T) {
	unknown := errors.New("still unknown")
	handle1, handle2 := new(ReplicatedTransactionRecoveryHandle), new(ReplicatedTransactionRecoveryHandle)
	var executeIndex atomic.Int64
	var recoveryMu sync.Mutex
	recoveryCalls := make(map[*ReplicatedTransactionRecoveryHandle]int)
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			if executeIndex.Add(1) == 1 {
				return registryUnknownOutcome(handle1, unknown)
			}
			return registryUnknownOutcome(handle2, unknown)
		},
		recover: func(_ context.Context, handle *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			recoveryMu.Lock()
			recoveryCalls[handle]++
			call := recoveryCalls[handle]
			recoveryMu.Unlock()
			if handle == handle1 && call == 1 {
				return registryUnknownOutcome(handle, unknown)
			}
			return ReplicatedTransactionResult{Committed: true}, nil
		},
	}
	registry := newRegistryForTest(t, orchestrator, 2)
	pairs := []struct {
		id     replication.ID128
		digest replication.Digest
	}{
		{registryTestID(19), registryTestDigest(20)},
		{registryTestID(21), registryTestDigest(22)},
	}
	for index, pair := range pairs {
		if _, err := registry.Execute(
			context.Background(), pair.id, pair.digest, uint64(index+1), nil,
		); !errors.Is(err, unknown) {
			t.Fatalf("initial %d error=%v", index, err)
		}
	}
	attempted, err := registry.RecoverPending(context.Background())
	if attempted != 2 || !errors.Is(err, unknown) {
		t.Fatalf("first sweep attempted=%d err=%v", attempted, err)
	}
	if got := registry.Stats(); got.PendingRecovery != 1 || got.Terminal != 1 {
		t.Fatalf("first sweep stats=%+v", got)
	}
	if err := registry.Forget(context.Background(), pairs[1].id, pairs[1].digest); err != nil {
		t.Fatalf("forget first terminal: %v", err)
	}
	if got := registry.Stats(); got.PendingRecovery != 1 || got.Entries != 1 {
		t.Fatalf("pending survived expiry stats=%+v", got)
	}
	attempted, err = registry.RecoverPending(context.Background())
	if attempted != 1 || err != nil {
		t.Fatalf("second sweep attempted=%d err=%v", attempted, err)
	}
	if err := registry.Forget(context.Background(), pairs[0].id, pairs[0].digest); err != nil {
		t.Fatalf("forget second terminal: %v", err)
	}
	if got := registry.Stats(); got.Entries != 0 {
		t.Fatalf("final stats=%+v", got)
	}
}

func TestReplicatedTransactionRequestRegistryNeverDiscardsActiveRecovery(t *testing.T) {
	unknown := errors.New("recovery incomplete")
	handle := new(ReplicatedTransactionRecoveryHandle)
	recoverStarted := make(chan struct{})
	releaseRecover := make(chan struct{})
	orchestrator := &registryTestOrchestrator{}
	orchestrator.execute = func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
		return registryUnknownOutcome(handle, unknown)
	}
	orchestrator.recover = func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
		if orchestrator.recoverCalls.Load() == 1 {
			close(recoverStarted)
			<-releaseRecover
			return registryUnknownOutcome(handle, unknown)
		}
		return ReplicatedTransactionResult{Committed: true}, nil
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(23), registryTestDigest(24)
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); !errors.Is(err, unknown) {
		t.Fatal(err)
	}
	retryDone := make(chan error, 1)
	go func() {
		_, err := registry.Execute(context.Background(), id, digest, 1, nil)
		retryDone <- err
	}()
	<-recoverStarted
	if err := registry.Forget(context.Background(), id, digest); !errors.Is(err, ErrReplicatedTransactionRequestUnresolved) {
		t.Fatalf("active recovery forget=%v", err)
	}
	if attempted, err := registry.RecoverPending(context.Background()); attempted != 0 || err != nil {
		t.Fatalf("parallel sweep attempted=%d err=%v", attempted, err)
	}
	close(releaseRecover)
	if err := <-retryDone; !errors.Is(err, unknown) {
		t.Fatalf("first recovery error=%v", err)
	}
	if got := registry.Stats(); got.PendingRecovery != 1 {
		t.Fatalf("post-recovery stats=%+v", got)
	}
	if attempted, err := registry.RecoverPending(context.Background()); attempted != 1 || err != nil {
		t.Fatalf("terminal sweep attempted=%d err=%v", attempted, err)
	}
	if err := registry.Forget(context.Background(), id, digest); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedTransactionRequestRegistryRetainsPriorHandleWhenRecoveryReturnsNoHandle(t *testing.T) {
	unknown := errors.New("unknown")
	malformed := errors.New("malformed recovery response")
	handle := new(ReplicatedTransactionRecoveryHandle)
	orchestrator := &registryTestOrchestrator{}
	orchestrator.execute = func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
		return registryUnknownOutcome(handle, unknown)
	}
	orchestrator.recover = func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
		if orchestrator.recoverCalls.Load() == 1 {
			return ReplicatedTransactionResult{}, malformed
		}
		return ReplicatedTransactionResult{Committed: true}, nil
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	id, digest := registryTestID(25), registryTestDigest(26)
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); !errors.Is(err, unknown) {
		t.Fatal(err)
	}
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); !errors.Is(err, malformed) || errors.Is(err, ErrReplicatedTransactionRequestRecovery) {
		t.Fatalf("malformed recovery error=%v", err)
	}
	if got := registry.Stats(); got.PendingRecovery != 1 {
		t.Fatalf("prior handle was lost: %+v", got)
	}
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); err != nil {
		t.Fatal(err)
	}
	if calls := orchestrator.recoverCalls.Load(); calls != 2 {
		t.Fatalf("recover calls=%d, want 2", calls)
	}
}

func TestReplicatedTransactionRequestRegistryValidationAndTerminalExpiry(t *testing.T) {
	orchestrator := &registryTestOrchestrator{
		execute: func(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error) {
			return ReplicatedTransactionResult{Committed: true}, nil
		},
		recover: func(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error) {
			return ReplicatedTransactionResult{}, nil
		},
	}
	for _, options := range []ReplicatedTransactionRequestRegistryOptions{
		{},
		{Orchestrator: orchestrator},
		{Orchestrator: orchestrator, MaxEntries: AbsoluteMaxReplicatedTransactionRequestEntries + 1},
	} {
		if _, err := NewReplicatedTransactionRequestRegistry(options); !errors.Is(err, ErrReplicatedTransactionRequestRegistry) {
			t.Fatalf("options=%+v error=%v", options, err)
		}
	}
	registry := newRegistryForTest(t, orchestrator, 1)
	if _, err := registry.Execute(
		context.Background(), replication.ID128{}, registryTestDigest(27), 1, nil,
	); !errors.Is(err, ErrReplicatedTransactionRequestRegistry) {
		t.Fatalf("zero ID error=%v", err)
	}
	if _, err := registry.Execute(
		context.Background(), registryTestID(28), replication.Digest{}, 1, nil,
	); !errors.Is(err, ErrReplicatedTransactionRequestRegistry) {
		t.Fatalf("zero digest error=%v", err)
	}
	id, digest := registryTestID(29), registryTestDigest(30)
	if _, err := registry.Execute(context.Background(), id, digest, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := registry.Forget(context.Background(), id, digest); err != nil {
		t.Fatalf("explicit forget error=%v", err)
	}
}
