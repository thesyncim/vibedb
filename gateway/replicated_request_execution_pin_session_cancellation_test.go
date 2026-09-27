package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type executionPinObservedDoneContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func newExecutionPinObservedDoneContext(ctx context.Context) *executionPinObservedDoneContext {
	return &executionPinObservedDoneContext{Context: ctx, observed: make(chan struct{})}
}

func (ctx *executionPinObservedDoneContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}

type executionPinCancelOnDoneContext struct {
	context.Context
	cancel context.CancelCauseFunc
	cause  error
	once   sync.Once
}

func (ctx *executionPinCancelOnDoneContext) Done() <-chan struct{} {
	ctx.once.Do(func() { ctx.cancel(ctx.cause) })
	return ctx.Context.Done()
}

type executionPinJournalFileSnapshot struct {
	present [2]bool
	bytes   [2][]byte
}

func snapshotExecutionPinJournalFiles(t *testing.T, base string) executionPinJournalFileSnapshot {
	t.Helper()
	var snapshot executionPinJournalFileSnapshot
	for slot := range snapshot.bytes {
		raw, err := os.ReadFile(base + "." + string(rune('0'+slot)))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		snapshot.present[slot] = true
		snapshot.bytes[slot] = raw
	}
	return snapshot
}

func assertExecutionPinJournalFilesEqual(
	t *testing.T, base string, want executionPinJournalFileSnapshot,
) {
	t.Helper()
	got := snapshotExecutionPinJournalFiles(t, base)
	for slot := range want.bytes {
		if got.present[slot] != want.present[slot] || !bytes.Equal(got.bytes[slot], want.bytes[slot]) {
			t.Fatalf("journal slot %d changed: present %v->%v bytes_equal=%v",
				slot, want.present[slot], got.present[slot], bytes.Equal(want.bytes[slot], got.bytes[slot]))
		}
	}
}

func newExecutionPinSessionFactoryFixture(
	t *testing.T,
) (*JournaledDurableRequestExecutionPinSessionFactory, DurableRequestTypedExecutionContext,
	ReplicatedRoute, *routeSessionMachineClient, string) {
	t.Helper()
	route, machine, _ := newRouteSessionMachine(t)
	client := &routeSessionDropClient{base: machine}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{7}, Generation: 1}
	factory, err := NewJournaledDurableRequestExecutionPinSessionFactory(executor, t.TempDir(), principal)
	if err != nil {
		t.Fatal(err)
	}
	execution, _ := bindTypedExecutionPin(t, typedExecutionFixture(t), route)
	_, _, release, err := factory.OpenExecutionPinSession(t.Context(), execution, route)
	if err != nil {
		t.Fatal(err)
	}
	release()
	identity := durableExecutionPinSessionIdentity(execution.ExecutionPinLease.PinID, principal)
	base := filepath.Join(factory.directory, hex.EncodeToString(identity[:]))
	journalPath, _, present, err := executionPinJournalPath(base)
	if err != nil || !present {
		t.Fatalf("execution-pin journal path=%q present=%v err=%v", journalPath, present, err)
	}
	if snapshot := snapshotExecutionPinJournalFiles(t, journalPath); !snapshot.present[0] && !snapshot.present[1] {
		t.Fatal("seeded execution-pin journal has no slot files")
	}
	return factory, execution, route, machine, journalPath
}

func TestCanceledContendedExecutionPinOpenReturnsBeforeStripeRelease(t *testing.T) {
	factory, execution, route, machine, base := newExecutionPinSessionFactoryFixture(t)
	identity := durableExecutionPinSessionIdentity(execution.ExecutionPinLease.PinID, factory.principal)
	stripe := &factory.stripes[durableExecutionPinSessionStripe(identity)]
	if err := stripe.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	locked := true
	unlock := func() {
		if locked {
			locked = false
			stripe.release()
		}
	}
	defer unlock()
	before := snapshotExecutionPinJournalFiles(t, base)
	appliedBefore := machine.state.Applied

	cause := errors.New("cancel queued execution-pin open")
	openCtx, cancel := context.WithCancelCause(context.Background())
	observedCtx := newExecutionPinObservedDoneContext(openCtx)
	result := make(chan error, 1)
	go func() {
		_, _, release, openErr := factory.OpenExecutionPinSession(observedCtx, execution, route)
		if release != nil {
			release()
		}
		result <- openErr
	}()
	select {
	case <-observedCtx.observed:
		cancel(cause)
	case <-time.After(time.Second):
		cancel(cause)
		unlock()
		<-result
		t.Fatal("open never reached a cancellation-aware wait on the held factory stripe")
	}
	select {
	case openErr := <-result:
		if !errors.Is(openErr, cause) {
			t.Fatalf("canceled open error=%v, want custom cancellation cause", openErr)
		}
		assertExecutionPinJournalFilesEqual(t, base, before)
		if machine.state.Applied != appliedBefore {
			t.Fatalf("replicated applied index changed while open was canceled: %d -> %d", appliedBefore, machine.state.Applied)
		}
	case <-time.After(time.Second):
		unlock()
		openErr := <-result
		t.Fatalf("open returned only after stripe release, error=%v", openErr)
	}
	unlock()
	_, _, release, err := factory.OpenExecutionPinSession(t.Context(), execution, route)
	if err != nil {
		t.Fatalf("session could not be reopened after canceled open: %v", err)
	}
	release()
}

func TestPreCanceledExecutionPinFactoryOperationsPreserveCause(t *testing.T) {
	factory, execution, route, machine, base := newExecutionPinSessionFactoryFixture(t)
	before := snapshotExecutionPinJournalFiles(t, base)
	appliedBefore := machine.state.Applied
	cause := errors.New("pre-canceled execution-pin factory operation")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)

	_, _, release, err := factory.OpenExecutionPinSession(ctx, execution, route)
	if release != nil {
		release()
	}
	if !errors.Is(err, cause) {
		t.Fatalf("pre-canceled ordinary open error=%v, want custom cause", err)
	}
	if err = factory.RetireAcknowledgedExecutionPinSession(
		ctx, execution.ExecutionPinLease.PinID, route, replication.Digest{1},
	); !errors.Is(err, cause) {
		t.Fatalf("pre-canceled ACK retirement error=%v, want custom cause", err)
	}
	assertExecutionPinJournalFilesEqual(t, base, before)
	if machine.state.Applied != appliedBefore {
		t.Fatalf("replicated applied index changed during pre-canceled operations: %d -> %d",
			appliedBefore, machine.state.Applied)
	}
}

func TestCanceledContendedAcknowledgedRetirementReturnsBeforeStripeRelease(t *testing.T) {
	factory, execution, route, machine, base := newExecutionPinSessionFactoryFixture(t)
	identity := durableExecutionPinSessionIdentity(execution.ExecutionPinLease.PinID, factory.principal)
	stripe := &factory.stripes[durableExecutionPinSessionStripe(identity)]
	if err := stripe.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	locked := true
	unlock := func() {
		if locked {
			locked = false
			stripe.release()
		}
	}
	defer unlock()
	before := snapshotExecutionPinJournalFiles(t, base)
	appliedBefore := machine.state.Applied

	cause := errors.New("cancel queued acknowledged execution-pin retirement")
	retireCtx, cancel := context.WithCancelCause(context.Background())
	observedCtx := newExecutionPinObservedDoneContext(retireCtx)
	result := make(chan error, 1)
	go func() {
		result <- factory.RetireAcknowledgedExecutionPinSession(
			observedCtx, execution.ExecutionPinLease.PinID, route, replication.Digest{1},
		)
	}()
	select {
	case <-observedCtx.observed:
		cancel(cause)
	case <-time.After(time.Second):
		cancel(cause)
		unlock()
		<-result
		t.Fatal("ACK retirement never reached a cancellation-aware wait on the held factory stripe")
	}
	select {
	case retireErr := <-result:
		if !errors.Is(retireErr, cause) {
			t.Fatalf("canceled ACK retirement error=%v, want custom cancellation cause", retireErr)
		}
		assertExecutionPinJournalFilesEqual(t, base, before)
		if machine.state.Applied != appliedBefore {
			t.Fatalf("replicated applied index changed while ACK retirement was canceled: %d -> %d",
				appliedBefore, machine.state.Applied)
		}
	case <-time.After(time.Second):
		unlock()
		retireErr := <-result
		t.Fatalf("ACK retirement returned only after stripe release, error=%v", retireErr)
	}
	unlock()
	_, _, release, err := factory.OpenExecutionPinSession(t.Context(), execution, route)
	if err != nil {
		t.Fatalf("session could not be reopened after canceled ACK retirement: %v", err)
	}
	release()
}

func TestCanceledContendedTerminalExecutionPinRetirementReturnsBeforeStripeRelease(t *testing.T) {
	factory, execution, route, machine, base := newExecutionPinSessionFactoryFixture(t)
	identity := durableExecutionPinSessionIdentity(execution.ExecutionPinLease.PinID, factory.principal)
	stripe := &factory.stripes[durableExecutionPinSessionStripe(identity)]
	if err := stripe.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	locked := true
	unlock := func() {
		if locked {
			locked = false
			stripe.release()
		}
	}
	defer unlock()
	before := snapshotExecutionPinJournalFiles(t, base)
	appliedBefore := machine.state.Applied

	cause := errors.New("cancel queued terminal execution-pin retirement")
	retireCtx, cancel := context.WithCancelCause(context.Background())
	observedCtx := newExecutionPinObservedDoneContext(retireCtx)
	result := make(chan error, 1)
	go func() {
		result <- factory.RetireTerminalExecutionPinSession(observedCtx, execution, route)
	}()
	select {
	case <-observedCtx.observed:
		cancel(cause)
	case <-time.After(time.Second):
		cancel(cause)
		unlock()
		<-result
		t.Fatal("terminal retirement never reached a cancellation-aware wait on the held factory stripe")
	}
	select {
	case retireErr := <-result:
		if !errors.Is(retireErr, cause) {
			t.Fatalf("canceled terminal retirement error=%v, want custom cancellation cause", retireErr)
		}
		assertExecutionPinJournalFilesEqual(t, base, before)
		if machine.state.Applied != appliedBefore {
			t.Fatalf("replicated applied index changed while terminal retirement was canceled: %d -> %d",
				appliedBefore, machine.state.Applied)
		}
	case <-time.After(time.Second):
		unlock()
		retireErr := <-result
		t.Fatalf("terminal retirement returned only after stripe release, error=%v", retireErr)
	}
	unlock()
	_, _, release, err := factory.OpenExecutionPinSession(t.Context(), execution, route)
	if err != nil {
		t.Fatalf("session could not be reopened after canceled retirement: %v", err)
	}
	release()
}

func TestExecutionPinSessionLatchCancellationCauseAndReacquisition(t *testing.T) {
	var latch executionPinSessionLatch
	preCanceledCause := errors.New("pre-canceled execution-pin acquisition")
	preCanceledCtx, cancelPreCanceled := context.WithCancelCause(context.Background())
	cancelPreCanceled(preCanceledCause)
	if err := latch.acquire(preCanceledCtx); !errors.Is(err, preCanceledCause) {
		t.Fatalf("pre-canceled acquisition error=%v, want custom cause", err)
	}
	if latch.token != nil {
		t.Fatal("pre-canceled acquisition initialized its token channel")
	}

	if err := latch.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("cancel waiting execution-pin acquisition")
	waitCtx, cancelWait := context.WithCancelCause(context.Background())
	observedCtx := newExecutionPinObservedDoneContext(waitCtx)
	result := make(chan error, 1)
	go func() { result <- latch.acquire(observedCtx) }()
	select {
	case <-observedCtx.observed:
		cancelWait(cause)
	case <-time.After(time.Second):
		latch.release()
		<-result
		t.Fatal("waiting acquisition did not enter its cancellation select")
	}
	select {
	case err := <-result:
		if !errors.Is(err, cause) {
			t.Fatalf("waiting acquisition error=%v, want custom cause", err)
		}
	case <-time.After(time.Second):
		latch.release()
		<-result
		t.Fatal("waiting acquisition did not return after cancellation")
	}
	latch.release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := latch.acquire(ctx); err != nil {
		t.Fatalf("latch could not be reacquired after canceled wait: %v", err)
	}
	latch.release()
}

func TestExecutionPinSessionLatchTokenCancellationRaceRetainsPermit(t *testing.T) {
	var latch executionPinSessionLatch
	if err := latch.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	latch.release()
	for attempt := range 64 {
		cause := fmt.Errorf("cancel-at-select-%d", attempt)
		ctx, cancel := context.WithCancelCause(context.Background())
		racingCtx := &executionPinCancelOnDoneContext{Context: ctx, cancel: cancel, cause: cause}
		if err := latch.acquire(racingCtx); !errors.Is(err, cause) {
			cancel(nil)
			t.Fatalf("racing acquisition error=%v, want cancellation cause %v", err, cause)
		}
		cancel(nil)
		healthyCtx, cancelHealthy := context.WithTimeout(context.Background(), time.Second)
		if err := latch.acquire(healthyCtx); err != nil {
			cancelHealthy()
			t.Fatalf("permit lost in token/cancellation race on attempt %d: %v", attempt, err)
		}
		latch.release()
		cancelHealthy()
	}
}

func TestExecutionPinSessionLatchWarmedAcquireReleaseAllocations(t *testing.T) {
	ctx := context.Background()
	var latch executionPinSessionLatch
	if err := latch.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	latch.release()
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := latch.acquire(ctx); err != nil {
			panic(err)
		}
		latch.release()
	}); allocations != 0 {
		t.Fatalf("warmed acquisition/release allocations=%v, want 0", allocations)
	}

	cause := errors.New("already canceled")
	canceledCtx, cancelCanceled := context.WithCancelCause(context.Background())
	cancelCanceled(cause)
	var canceledErr error
	if allocations := testing.AllocsPerRun(1000, func() {
		canceledErr = latch.acquire(canceledCtx)
	}); allocations != 0 {
		t.Fatalf("preconstructed canceled acquisition allocations=%v, want 0", allocations)
	}
	if !errors.Is(canceledErr, cause) {
		t.Fatalf("preconstructed canceled acquisition error=%v, want custom cause", canceledErr)
	}

	coldAllocations := testing.AllocsPerRun(1000, func() {
		var cold executionPinSessionLatch
		if err := cold.acquire(ctx); err != nil {
			panic(err)
		}
		cold.release()
	})
	t.Logf("cold acquisition/release allocations=%v (bounded first-use initialization)", coldAllocations)
	if coldAllocations > 2 {
		t.Fatalf("cold acquisition/release allocations=%v, want at most two", coldAllocations)
	}
}

func TestExecutionPinSessionReleaseIsConcurrentAndIdempotent(t *testing.T) {
	factory, execution, route, _, _ := newExecutionPinSessionFactoryFixture(t)
	_, _, release, err := factory.OpenExecutionPinSession(t.Context(), execution, route)
	if err != nil {
		t.Fatal(err)
	}
	var releases sync.WaitGroup
	for range 16 {
		releases.Add(1)
		go func() {
			defer releases.Done()
			release()
		}()
	}
	releases.Wait()
	_, _, nextRelease, err := factory.OpenExecutionPinSession(t.Context(), execution, route)
	if err != nil {
		t.Fatalf("latch was not available after repeated release: %v", err)
	}
	nextRelease()
}
