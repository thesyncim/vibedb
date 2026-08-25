package main

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestOutputHeaderRequiresResolvedDurabilityAndCheckpoint(t *testing.T) {
	var out bytes.Buffer
	printHeader(&out)
	fields := strings.Fields(out.String())
	contains := func(want string) bool {
		for _, field := range fields {
			if field == want {
				return true
			}
		}
		return false
	}
	if !contains("durability") {
		t.Fatalf("header omits resolved durability column: %q", out.String())
	}
	if !contains("checkpoint") {
		t.Fatalf("header omits checkpoint interval column: %q", out.String())
	}
	if !contains("forced-cp") {
		t.Fatalf("header omits forced checkpoint column: %q", out.String())
	}
	for _, column := range []string{"document-shape", "exact-indexes", "p99.9-us", "max-us", "durability-payload-known", "logical-write-B", "durability-payload-B", "durability-payload/logical"} {
		if !contains(column) {
			t.Fatalf("header omits %s column: %q", column, out.String())
		}
	}
	if contains("sync") {
		t.Fatalf("header retained ambiguous sync column: %q", out.String())
	}
}

func TestMeasuredLogicalMutationBytesCountsSubmittedKeysAndValues(t *testing.T) {
	docs := []competitive.Doc{
		{Key: "a", JSON: []byte(`{"v":1}`)},
		{Key: "bb", JSON: []byte(`{"v":22}`)},
	}
	choices := []int{opUpdate, opReadModifyWrite, opChurn, opRead}
	states := []*clientState{{
		warmupOps: 1, measuredOps: 3,
		keyTrace: []int{0, 1, 0, 1},
	}}
	// RMW(bb) submits key+value, churn(a) submits delete key plus upsert
	// key+value, and the final read contributes no logical mutation bytes.
	want := uint64(len("bb") + len(docs[1].JSON) + 2*len("a") + len(docs[0].JSON))
	if got := measuredLogicalMutationBytes(states, choices, docs); got != want {
		t.Fatalf("logical mutation bytes=%d want=%d", got, want)
	}
}

func TestBuildTelemetryRecordUsesCounterDeltasAndExplicitHighWaters(t *testing.T) {
	runtimeBefore := runtime.MemStats{TotalAlloc: 100, Mallocs: 10}
	runtimeAfter := runtime.MemStats{TotalAlloc: 175, Mallocs: 17}
	durableBefore := durable.Stats{
		ConcurrentPrimaryScalarPatchAttempts: 10,
		ConcurrentPrimaryScalarPatches:       9,
		ConcurrentPrimaryPublishGroups:       3,
		ConcurrentPrimaryLargestPublishGroup: 4,
		JournalAcks:                          20,
		JournalSyncs:                         5,
		JournalLargestGroup:                  6,
		JournalDeltaRecords:                  30,
		JournalDeltaBytes:                    4096,
		JournalDeltaFullFallbacks:            1,
		DeviceBytes:                          8192,
		ConcurrentPrimaryStripeWaitNS: durable.StatsHistogram{
			Count: 2, Sum: 10, Max: 8,
		},
	}
	durableAfter := durableBefore
	durableAfter.ConcurrentPrimaryScalarPatchAttempts += 11
	durableAfter.ConcurrentPrimaryScalarPatches += 10
	durableAfter.ConcurrentPrimaryPublishGroups += 7
	durableAfter.ConcurrentPrimaryLargestPublishGroup = 12
	durableAfter.JournalAcks += 40
	durableAfter.JournalSyncs += 8
	durableAfter.JournalLargestGroup = 14
	durableAfter.JournalDeltaRecords += 50
	durableAfter.JournalDeltaBytes += 12_288
	durableAfter.JournalDeltaFullFallbacks += 2
	durableAfter.DeviceBytes += 16_384
	durableAfter.ConcurrentPrimaryStripeWaitNS.Count += 3
	durableAfter.ConcurrentPrimaryStripeWaitNS.Sum += 30
	durableAfter.ConcurrentPrimaryStripeWaitNS.Max = 16
	durableAfter.ConcurrentPrimaryStripeWaitNS.Buckets[4] = 3

	got := buildTelemetryRecord(
		"vibedb", 8, runtimeBefore, runtimeAfter,
		durableBefore, durableAfter, true,
	)
	if got.Engine != "vibedb" || got.Clients != 8 || !got.Available ||
		got.RuntimeTotalAllocBytes != 75 || got.RuntimeMallocs != 7 ||
		got.ScalarPatchAttempts != 11 || got.ScalarPatchAccepts != 10 ||
		got.PublishGroups != 7 || got.PublishGroupMaxBefore != 4 ||
		got.PublishGroupMax != 12 || got.JournalAcks != 40 ||
		got.JournalSyncs != 8 || got.JournalGroupMaxBefore != 6 ||
		got.JournalGroupMax != 14 || got.JournalDeltaRecords != 50 ||
		got.JournalDeltaBytes != 12_288 || got.JournalDeltaFallbacks != 2 ||
		!got.DurabilityPayloadKnown || got.DurabilityPayloadBytes != 16_384 {
		t.Fatalf("telemetry = %+v", got)
	}
	stripe := got.Histograms["concurrent-stripe-wait-ns"]
	if stripe.Count != 3 || stripe.Sum != 30 || stripe.MaxBefore != 8 ||
		stripe.Max != 16 || len(stripe.Buckets) != durable.StatsHistogramBuckets ||
		stripe.Buckets[4] != 3 {
		t.Fatalf("stripe wait histogram = %+v", stripe)
	}
	if got := counterDelta(10, 9); got != 0 {
		t.Fatalf("reset counter delta = %d, want 0", got)
	}
	if delta, known := counterDeltaKnown(10, 9); delta != 0 || known {
		t.Fatalf("reset payload delta = %d known=%t, want 0,false", delta, known)
	}
	durableAfter.DeviceBytes = durableBefore.DeviceBytes - 1
	regressed := buildTelemetryRecord(
		"vibedb", 8, runtimeBefore, runtimeAfter,
		durableBefore, durableAfter, true,
	)
	if regressed.DurabilityPayloadKnown || regressed.DurabilityPayloadBytes != 0 {
		t.Fatalf("regressed durability payload = %+v, want unknown zero", regressed)
	}
}

func TestMutationCount(t *testing.T) {
	tests := []struct {
		operation int
		want      int
	}{
		{operation: opRead, want: 0},
		{operation: opUpdate, want: 1},
		{operation: opReadModifyWrite, want: 1},
		{operation: opChurn, want: 2},
		{operation: opScan, want: 0},
	}
	for _, test := range tests {
		if got := mutationCount(test.operation); got != test.want {
			t.Fatalf("mutationCount(%d) = %d, want %d", test.operation, got, test.want)
		}
	}
}

type blockingCheckpointer struct {
	calls   atomic.Int64
	started chan struct{}
	release chan struct{}
	err     error
}

func newBlockingCheckpointer() *blockingCheckpointer {
	return &blockingCheckpointer{
		started: make(chan struct{}, 8),
		release: make(chan struct{}, 8),
	}
}

func (b *blockingCheckpointer) Checkpoint() error {
	b.calls.Add(1)
	b.started <- struct{}{}
	<-b.release
	return b.err
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertBlocked[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case value := <-ch:
		t.Fatalf("%s completed early: %v", what, value)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestCheckpointEpochDrainsExactBudgetBeforeNextAdmission(t *testing.T) {
	engine := newBlockingCheckpointer()
	coord := newCheckpointCoordinator(engine, 3, true)

	first, err := coord.admit(1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := coord.admit(1)
	if err != nil {
		t.Fatal(err)
	}
	third, err := coord.admit(1)
	if err != nil {
		t.Fatal(err)
	}

	nextAdmitted := make(chan error, 1)
	nextAttempted := make(chan struct{})
	var next mutationAdmission
	go func() {
		close(nextAttempted)
		var admitErr error
		next, admitErr = coord.admit(1)
		nextAdmitted <- admitErr
	}()
	awaitSignal(t, nextAttempted, "next epoch admission attempt")
	assertBlocked(t, nextAdmitted, "next epoch admission while mutations are active")

	if err := coord.complete(first); err != nil {
		t.Fatal(err)
	}
	if err := coord.complete(second); err != nil {
		t.Fatal(err)
	}
	assertBlocked(t, nextAdmitted, "next epoch admission before the epoch drains")

	completed := make(chan error, 1)
	go func() { completed <- coord.complete(third) }()
	awaitSignal(t, engine.started, "checkpoint after exact three-mutation epoch")
	assertBlocked(t, completed, "last epoch completion during checkpoint")
	assertBlocked(t, nextAdmitted, "next epoch admission during checkpoint")

	engine.release <- struct{}{}
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if err := <-nextAdmitted; err != nil {
		t.Fatal(err)
	}
	if next.epoch != 1 {
		t.Fatalf("next admission epoch = %d, want 1", next.epoch)
	}
	if got := engine.calls.Load(); got != 1 {
		t.Fatalf("checkpoint calls = %d, want 1", got)
	}

	if err := coord.complete(next); err != nil {
		t.Fatal(err)
	}
	final := make(chan error, 1)
	go func() { final <- coord.finalFlush() }()
	awaitSignal(t, engine.started, "trailing partial-epoch checkpoint")
	engine.release <- struct{}{}
	if err := <-final; err != nil {
		t.Fatal(err)
	}
}

func TestZeroCheckpointIntervalMeansFinalOnly(t *testing.T) {
	engine := newBlockingCheckpointer()
	coord := newCheckpointCoordinator(engine, 0, true)
	for range 4 {
		admission, err := coord.admit(1)
		if err != nil {
			t.Fatal(err)
		}
		if admission.coordinated {
			t.Fatal("zero checkpoint interval unexpectedly used epoch admission")
		}
		if err := coord.complete(admission); err != nil {
			t.Fatal(err)
		}
	}
	if got := engine.calls.Load(); got != 0 {
		t.Fatalf("checkpoint calls before final flush = %d, want 0", got)
	}

	done := make(chan error, 1)
	go func() { done <- coord.finalFlush() }()
	awaitSignal(t, engine.started, "final-only checkpoint")
	engine.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := engine.calls.Load(); got != 1 {
		t.Fatalf("checkpoint calls = %d, want 1", got)
	}
}

type recordingSession struct {
	deleted  chan error
	restored chan error
	scanned  int
}

func (s *recordingSession) Get(dst []byte, key string) ([]byte, error) {
	return dst, nil
}

func (s *recordingSession) Put(string, []byte) error { return nil }

func (s *recordingSession) Upsert(string, []byte) error {
	s.restored <- nil
	return nil
}

func (s *recordingSession) Delete(string) error {
	s.deleted <- nil
	return nil
}

func (s *recordingSession) ScanAllBytes() (int, error) { return s.scanned, nil }

func TestRunScanEnforcesConcurrentCountBounds(t *testing.T) {
	docs := make([]competitive.Doc, 8)
	for _, test := range []struct {
		name    string
		scanned int
		wantErr bool
	}{
		{name: "lower bound", scanned: 5},
		{name: "complete", scanned: 8},
		{name: "too short", scanned: 4, wantErr: true},
		{name: "too long", scanned: 9, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &clientState{session: &recordingSession{scanned: test.scanned}}
			kind, mutations, err := state.run(
				docs, nil, 5, nil, opScan, 0,
			)
			if (err != nil) != test.wantErr {
				t.Fatalf("run scan error = %v, wantErr %v", err, test.wantErr)
			}
			if kind != opScan || mutations != 0 {
				t.Fatalf("run scan result = kind %d, mutations %d", kind, mutations)
			}
		})
	}
}

func TestDeleteRestoreUsesTwoExactMutationEpochs(t *testing.T) {
	engine := newBlockingCheckpointer()
	coord := newCheckpointCoordinator(engine, 1, true)
	session := &recordingSession{
		deleted: make(chan error, 1), restored: make(chan error, 1),
	}
	state := &clientState{session: session}
	docs := []competitive.Doc{{Key: "doc:00000000", JSON: []byte(`{"id":0}`)}}
	updated := []bool{true}
	type result struct {
		kind, mutations int
		err             error
	}
	done := make(chan result, 1)
	go func() {
		kind, mutations, err := state.run(
			docs, updated, len(docs), coord, opChurn, 0,
		)
		done <- result{kind: kind, mutations: mutations, err: err}
	}()

	if err := <-session.deleted; err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, engine.started, "delete checkpoint")
	assertBlocked(t, session.restored, "restore before delete checkpoint")
	engine.release <- struct{}{}

	if err := <-session.restored; err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, engine.started, "restore checkpoint")
	assertBlocked(t, done, "delete+restore acknowledgement during restore checkpoint")
	engine.release <- struct{}{}

	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.kind != opChurn || got.mutations != 2 {
		t.Fatalf("delete+restore result = kind %d, mutations %d", got.kind, got.mutations)
	}
	if updated[0] {
		t.Fatal("delete+restore did not restore the content oracle")
	}
	if calls := engine.calls.Load(); calls != 2 {
		t.Fatalf("checkpoint calls = %d, want 2", calls)
	}
}

func TestCheckpointEpochRejectsMultiMutationAdmission(t *testing.T) {
	coord := newCheckpointCoordinator(newBlockingCheckpointer(), 64, false)
	if _, err := coord.admit(2); err == nil {
		t.Fatal("multi-mutation admission succeeded; callers must split state changes")
	}
}

func TestCheckpointEpochConcurrentAdmissionsNeverExceedBudget(t *testing.T) {
	engine := newBlockingCheckpointer()
	coord := newCheckpointCoordinator(engine, 8, false)

	var wg sync.WaitGroup
	admissions := make(chan mutationAdmission, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			admission, err := coord.admit(1)
			if err != nil {
				t.Errorf("admit: %v", err)
				return
			}
			admissions <- admission
		}()
	}
	wg.Wait()
	close(admissions)
	admitted, active := unpackCheckpointCounts(coord.state.Load())
	if admitted != 8 || active != 8 {
		t.Fatalf("admitted/active = %d/%d, want 8/8", admitted, active)
	}

	var all []mutationAdmission
	for admission := range admissions {
		all = append(all, admission)
	}
	for _, admission := range all[:len(all)-1] {
		if err := coord.complete(admission); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- coord.complete(all[len(all)-1]) }()
	awaitSignal(t, engine.started, "concurrent exact-budget checkpoint")
	engine.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointEpochOversubscriptionAdvancesExactFairCohorts(t *testing.T) {
	const (
		every  = 4
		epochs = 3
		total  = every * epochs
	)
	engine := newBlockingCheckpointer()
	coord := newCheckpointCoordinator(engine, every, false)
	type admittedResult struct {
		admission mutationAdmission
		err       error
	}
	admitted := make(chan admittedResult, total)
	completed := make(chan error, total)
	releaseMutation := make(chan struct{}, total)
	for range total {
		go func() {
			admission, err := coord.admit(1)
			admitted <- admittedResult{admission: admission, err: err}
			if err != nil {
				completed <- err
				return
			}
			<-releaseMutation
			completed <- coord.complete(admission)
		}()
	}

	for epoch := range epochs {
		for range every {
			result := awaitChannel(t, admitted, "oversubscribed admission")
			if result.err != nil {
				t.Fatal(result.err)
			}
			if result.admission.epoch != uint64(epoch) {
				t.Fatalf(
					"cohort admission epoch = %d, want %d",
					result.admission.epoch, epoch,
				)
			}
		}
		assertBlocked(t, admitted, "admission beyond exact epoch budget")
		for range every {
			releaseMutation <- struct{}{}
		}
		awaitSignal(t, engine.started, "oversubscribed epoch checkpoint")
		assertBlocked(t, admitted, "next cohort during checkpoint")
		engine.release <- struct{}{}
	}

	for range total {
		if err := awaitChannel(t, completed, "oversubscribed completion"); err != nil {
			t.Fatal(err)
		}
	}
	if calls := engine.calls.Load(); calls != epochs {
		t.Fatalf("checkpoint calls = %d, want %d", calls, epochs)
	}
	if waiters := coord.waiters.Load(); waiters != 0 {
		t.Fatalf("waiters after drain = %d, want 0", waiters)
	}
}

func awaitChannel[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		var zero T
		t.Fatalf("timed out waiting for %s", what)
		return zero
	}
}

func TestCheckpointFailureWakesAllAdmissions(t *testing.T) {
	sentinel := errors.New("checkpoint failed")
	engine := newBlockingCheckpointer()
	engine.err = sentinel
	coord := newCheckpointCoordinator(engine, 2, false)
	first, err := coord.admit(1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := coord.admit(1)
	if err != nil {
		t.Fatal(err)
	}

	const waiterCount = 16
	waiterErrors := make(chan error, waiterCount)
	for range waiterCount {
		go func() {
			_, err := coord.admit(1)
			waiterErrors <- err
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for coord.waiters.Load() != waiterCount {
		if time.Now().After(deadline) {
			t.Fatalf(
				"queued waiters = %d, want %d",
				coord.waiters.Load(), waiterCount,
			)
		}
		runtime.Gosched()
	}
	if err := coord.complete(first); err != nil {
		t.Fatal(err)
	}
	last := make(chan error, 1)
	go func() { last <- coord.complete(second) }()
	awaitSignal(t, engine.started, "failing checkpoint")
	assertBlocked(t, waiterErrors, "waiter before checkpoint failure")
	engine.release <- struct{}{}
	if err := awaitChannel(t, last, "failing elected completion"); !errors.Is(err, sentinel) {
		t.Fatalf("elected completion error = %v, want %v", err, sentinel)
	}
	for range waiterCount {
		if err := awaitChannel(t, waiterErrors, "failed admission wakeup"); !errors.Is(err, sentinel) {
			t.Fatalf("waiter error = %v, want %v", err, sentinel)
		}
	}
	if _, err := coord.admit(1); !errors.Is(err, sentinel) {
		t.Fatalf("post-failure admission error = %v, want %v", err, sentinel)
	}
	if err := coord.finalFlush(); !errors.Is(err, sentinel) {
		t.Fatalf("post-failure final flush error = %v, want %v", err, sentinel)
	}
}

type instantCheckpointer struct{ calls atomic.Int64 }

func (c *instantCheckpointer) Checkpoint() error {
	c.calls.Add(1)
	return nil
}

func TestCheckpointCompletionValidatesEpochBeforeDecrement(t *testing.T) {
	engine := &instantCheckpointer{}
	coord := newCheckpointCoordinator(engine, 4, false)
	admission, err := coord.admit(1)
	if err != nil {
		t.Fatal(err)
	}
	wrong := admission
	wrong.epoch++
	if err := coord.complete(wrong); err == nil {
		t.Fatal("wrong-epoch completion succeeded")
	}
	admitted, active := unpackCheckpointCounts(coord.state.Load())
	if admitted != 1 || active != 1 {
		t.Fatalf(
			"state after rejected completion = %d/%d, want 1/1",
			admitted, active,
		)
	}
	if err := coord.complete(admission); err != nil {
		t.Fatal(err)
	}
	if err := coord.finalFlush(); err != nil {
		t.Fatal(err)
	}
	if calls := engine.calls.Load(); calls != 1 {
		t.Fatalf("checkpoint calls = %d, want 1", calls)
	}
}

type exactCutCheckpointer struct {
	every        int64
	active       atomic.Int64
	completed    atomic.Int64
	inCheckpoint atomic.Bool

	mu      sync.Mutex
	cuts    []int64
	failure error
}

func (c *exactCutCheckpointer) fail(format string, args ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure == nil {
		c.failure = fmt.Errorf(format, args...)
	}
	return c.failure
}

func (c *exactCutCheckpointer) beginMutation() error {
	if c.inCheckpoint.Load() {
		return c.fail("mutation began during checkpoint")
	}
	c.active.Add(1)
	if c.inCheckpoint.Load() {
		c.active.Add(-1)
		return c.fail("mutation crossed checkpoint start")
	}
	return nil
}

func (c *exactCutCheckpointer) finishMutation() {
	c.completed.Add(1)
	c.active.Add(-1)
}

func (c *exactCutCheckpointer) Checkpoint() error {
	if !c.inCheckpoint.CompareAndSwap(false, true) {
		return c.fail("overlapping checkpoints")
	}
	defer c.inCheckpoint.Store(false)
	if active := c.active.Load(); active != 0 {
		return c.fail("checkpoint began with %d active mutations", active)
	}
	cut := c.completed.Load()
	if cut%c.every != 0 {
		return c.fail("checkpoint cut %d is not a multiple of %d", cut, c.every)
	}
	for range 4 {
		runtime.Gosched()
	}
	if active := c.active.Load(); active != 0 {
		return c.fail("mutation entered during checkpoint: active=%d", active)
	}
	c.mu.Lock()
	c.cuts = append(c.cuts, cut)
	c.mu.Unlock()
	return nil
}

func TestCheckpointEpochRaceStressPreservesExactCuts(t *testing.T) {
	const (
		every       = 64
		workers     = 32
		perWorker   = 128
		total       = workers * perWorker
		checkpoints = total / every
	)
	engine := &exactCutCheckpointer{every: every}
	coord := newCheckpointCoordinator(engine, every, false)
	start := make(chan struct{})
	errorsSeen := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range perWorker {
				admission, err := coord.admit(1)
				if err != nil {
					errorsSeen <- err
					return
				}
				if err := engine.beginMutation(); err != nil {
					_ = coord.complete(admission)
					errorsSeen <- err
					return
				}
				runtime.Gosched()
				engine.finishMutation()
				if err := coord.complete(admission); err != nil {
					errorsSeen <- err
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatal(err)
	}
	if err := coord.finalFlush(); err != nil {
		t.Fatal(err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.failure != nil {
		t.Fatal(engine.failure)
	}
	if len(engine.cuts) != checkpoints {
		t.Fatalf("checkpoint cuts = %d, want %d", len(engine.cuts), checkpoints)
	}
	for i, cut := range engine.cuts {
		want := int64((i + 1) * every)
		if cut != want {
			t.Fatalf("checkpoint %d cut = %d, want %d", i, cut, want)
		}
	}
}

func TestCheckpointCoordinatorPartialEpochIsAllocationFree(t *testing.T) {
	coord := newCheckpointCoordinator(
		&instantCheckpointer{}, int(uint64(^uint32(0))), false,
	)
	failed := false
	if allocs := testing.AllocsPerRun(1_000, func() {
		admission, err := coord.admit(1)
		if err != nil || coord.complete(admission) != nil {
			failed = true
		}
	}); allocs != 0 {
		t.Fatalf("admit+complete allocations = %.1f, want 0", allocs)
	}
	if failed {
		t.Fatal("allocation run failed")
	}
}
