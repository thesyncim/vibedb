package multiraft

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func runtimeForLane(t testing.TB, set *ExecutionLanes, lane int, start byte) *fakeRuntime {
	t.Helper()
	for offset := 0; offset < 240; offset++ {
		candidate := newFakeRuntime(start + byte(offset))
		candidate.identity.MemberID = uint64(start) + uint64(offset) + 1
		got, err := set.Lane(candidate.identity.Group)
		if err != nil {
			t.Fatal(err)
		}
		if got == lane {
			return candidate
		}
	}
	t.Fatalf("no group mapped to lane %d", lane)
	return nil
}

func TestExecutionLanesDeterministicAssignmentAndValidation(t *testing.T) {
	if runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64" {
		stride := unsafe.Sizeof(executionLane{})
		if stride != 256 {
			t.Fatalf("execution lane size=%d, want 256-byte stride", stride)
		}
		offset := unsafe.Offsetof(executionLane{}.counters)
		counterSize := unsafe.Sizeof(laneCounters{})
		if offset != 16 || counterSize != 64 || stride-counterSize < 128 {
			t.Fatalf("counter layout offset=%d size=%d", offset, unsafe.Sizeof(laneCounters{}))
		}
	}
	limits := testHostLimits()
	for _, count := range []int{1, 2, 4, 8} {
		first, err := NewExecutionLanes(count, limits)
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewExecutionLanes(count, limits)
		if err != nil {
			t.Fatal(err)
		}
		seen := make([]bool, count)
		for seed := 0; seed < 128; seed++ {
			key := newFakeRuntime(byte(seed)).identity.Group
			left, leftErr := first.Lane(key)
			right, rightErr := second.Lane(key)
			if leftErr != nil || rightErr != nil || left != right || left < 0 || left >= count {
				t.Fatalf("count=%d seed=%d lanes=%d/%d errors=%v/%v", count, seed, left, right, leftErr, rightErr)
			}
			seen[left] = true
		}
		for lane, found := range seen {
			if !found {
				t.Fatalf("count=%d lane=%d received no deterministic groups", count, lane)
			}
		}
		_ = first.Close()
		_ = second.Close()
	}
	stable, err := NewExecutionLanes(8, limits)
	if err != nil {
		t.Fatal(err)
	}
	for seed, want := range []int{2, 1, 0, 7, 6, 5, 4, 3} {
		got, laneErr := stable.Lane(newFakeRuntime(byte(seed)).identity.Group)
		if laneErr != nil || got != want {
			t.Fatalf("stable seed=%d lane=%d want=%d err=%v", seed, got, want, laneErr)
		}
	}
	_ = stable.Close()
	for _, count := range []int{-1, 0, 3, 6, AbsoluteMaxExecutionLanes + 1} {
		if set, err := NewExecutionLanes(count, limits); set != nil || !errors.Is(err, ErrInvalidExecutionLanes) {
			t.Fatalf("count=%d set=%v err=%v", count, set, err)
		}
	}
	set, err := NewExecutionLanes(2, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := set.Lane(raftmember.GroupKey{}); !errors.Is(err, ErrExecutionLane) {
		t.Fatalf("zero group lane error=%v", err)
	}
	_ = set.Close()
}

func TestExecutionLanesEnforceDistinctGroupAndAggregateByteBounds(t *testing.T) {
	limits := testHostLimits()
	limits.MaxGroupBytes = raftmodel.MaxInboundMessageBytes
	limits.MaxQueueBytes = 2 * limits.MaxGroupBytes
	set, err := NewExecutionLanes(2, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	first := runtimeForLane(t, set, 0, 1)
	second := runtimeForLane(t, set, 0, 41)
	third := runtimeForLane(t, set, 0, 81)
	independent := runtimeForLane(t, set, 1, 121)
	for _, member := range []*fakeRuntime{first, second, third, independent} {
		if err := set.addRuntime(member); err != nil {
			t.Fatal(err)
		}
	}
	half := make([]byte, limits.MaxGroupBytes/2)
	remainder := make([]byte, limits.MaxGroupBytes-int64(len(half)))
	for _, payload := range [][]byte{half, remainder} {
		if err := set.EnqueueProposal(first.identity.Group, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := set.EnqueueProposal(first.identity.Group, []byte{1}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("group byte saturation=%v", err)
	}
	for _, payload := range [][]byte{half, remainder} {
		if err := set.EnqueueProposal(second.identity.Group, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := set.EnqueueProposal(third.identity.Group, []byte{1}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("aggregate byte saturation=%v", err)
	}
	if err := set.EnqueueProposal(independent.identity.Group, []byte{2}); err != nil {
		t.Fatalf("independent lane admission=%v", err)
	}
	stats := set.StatsInto(make([]ExecutionLaneStats, 0, 2))
	if stats[0].QueueBytes != limits.MaxQueueBytes || stats[1].QueueBytes != 1 {
		t.Fatalf("saturated stats=%+v", stats)
	}
	progress, done, err := set.RunOne(0)
	if err != nil || !done || progress.Kind != ProgressProposal ||
		progress.ProposalBytes != int64(len(half)) {
		t.Fatalf("release progress=%+v done=%t err=%v", progress, done, err)
	}
	if err := set.EnqueueProposal(third.identity.Group, []byte{3}); err != nil {
		t.Fatalf("released aggregate capacity=%v", err)
	}
	stats = set.StatsInto(stats[:0])
	if stats[0].QueueBytes != limits.MaxQueueBytes-int64(len(half))+1 {
		t.Fatalf("released stats=%+v", stats)
	}
}

func TestExecutionLanesEnforceIndependentQueueAndByteAdmission(t *testing.T) {
	limits := testHostLimits()
	limits.MaxQueueItems = 1
	limits.MaxGroupItems = 1
	limits.MaxPendingTicks = 1
	set, err := NewExecutionLanes(2, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	runtimes := []*fakeRuntime{runtimeForLane(t, set, 0, 1), runtimeForLane(t, set, 1, 97)}
	for _, member := range runtimes {
		if err := set.addRuntime(member); err != nil {
			t.Fatal(err)
		}
	}
	if err := set.EnqueueProposal(runtimes[0].identity.Group, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := set.EnqueueProposal(runtimes[1].identity.Group, []byte{4, 5, 6, 7}); err != nil {
		t.Fatal(err)
	}
	if err := set.EnqueueProposal(runtimes[0].identity.Group, []byte{8}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("same-lane overflow=%v", err)
	}
	stats := set.StatsInto(make([]ExecutionLaneStats, 0, 2))
	if len(stats) != 2 || stats[0].QueueItems != 1 || stats[1].QueueItems != 1 ||
		stats[0].QueueBytes != 3 || stats[1].QueueBytes != 4 ||
		stats[0].Rejected != 1 || stats[1].Rejected != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	for lane := range 2 {
		progress, done, runErr := set.RunOne(lane)
		if runErr != nil || !done || progress.Kind != ProgressProposal {
			t.Fatalf("lane=%d progress=%+v done=%t err=%v", lane, progress, done, runErr)
		}
	}
	stats = set.StatsInto(stats[:0])
	if stats[0].QueueItems != 0 || stats[1].QueueItems != 0 ||
		stats[0].Progressed != 1 || stats[1].Progressed != 1 {
		t.Fatalf("drained stats=%+v", stats)
	}
}

func TestExecutionLanesQueryWrappersUseOwningLane(t *testing.T) {
	set, err := NewExecutionLanes(2, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	member := runtimeForLane(t, set, 1, 37)
	member.publication = raftmodel.Publication{Applied: 11, ReplicaSetVersion: 12}
	member.snapshotState = replicatedstate.State{Applied: 13}
	member.status.Term = 14
	member.progress = map[uint64]raftmodel.MemberProgress{
		15: {Match: 16, Next: 17},
	}
	if err := set.addRuntime(member); err != nil {
		t.Fatal(err)
	}
	publication, err := set.Publication(member.identity.Group)
	if err != nil || publication.Applied != 11 || publication.ReplicaSetVersion != 12 {
		t.Fatalf("publication=%+v err=%v", publication, err)
	}
	state, err := set.SnapshotState(member.identity.Group)
	if err != nil || state.Applied != 13 {
		t.Fatalf("snapshot state=%+v err=%v", state, err)
	}
	status, err := set.Status(member.identity.Group)
	if err != nil || status.Term != 14 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	progress, found, err := set.Progress(member.identity.Group, 15)
	if err != nil || !found || progress.Match != 16 || progress.Next != 17 {
		t.Fatalf("progress=%+v found=%t err=%v", progress, found, err)
	}
}

func TestExecutionLanesHotGroupCannotBlockAnotherLane(t *testing.T) {
	set, err := NewExecutionLanes(2, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	hot := runtimeForLane(t, set, 0, 11)
	cold := runtimeForLane(t, set, 1, 111)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hot.driveHook = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	if err := set.addRuntime(hot); err != nil {
		t.Fatal(err)
	}
	if err := set.addRuntime(cold); err != nil {
		t.Fatal(err)
	}
	hotDone := make(chan error, 1)
	go func() {
		_, _, runErr := set.RunOne(0)
		hotDone <- runErr
	}()
	<-entered
	if err := set.RequestTick(cold.identity.Group); err != nil {
		t.Fatal(err)
	}
	coldDone := make(chan error, 1)
	go func() {
		progress, done, runErr := set.RunOne(1)
		if runErr == nil && (!done || progress.Kind != ProgressTick) {
			runErr = fmt.Errorf("cold progress=%+v done=%t", progress, done)
		}
		coldDone <- runErr
	}()
	select {
	case err := <-coldDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cold lane blocked behind hot lane")
	}
	close(release)
	if err := <-hotDone; err != nil {
		t.Fatal(err)
	}
}

func TestExecutionLanesPreserveExactGroupInputOrder(t *testing.T) {
	set, err := NewExecutionLanes(4, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	member := runtimeForLane(t, set, 2, 51)
	if err := set.addRuntime(member); err != nil {
		t.Fatal(err)
	}
	first, second := []byte("first"), []byte("second")
	if err := set.EnqueueProposal(member.identity.Group, first); err != nil {
		t.Fatal(err)
	}
	if err := set.EnqueueProposal(member.identity.Group, second); err != nil {
		t.Fatal(err)
	}
	progress, done, err := set.RunOne(2)
	if err != nil || !done || progress.Kind != ProgressProposal || progress.ProposalCount != 2 ||
		len(member.proposals) != 2 || !bytes.Equal(member.proposals[0], first) ||
		!bytes.Equal(member.proposals[1], second) {
		t.Fatalf("progress=%+v proposals=%q done=%t err=%v", progress, member.proposals, done, err)
	}
}

func TestExecutionLanesCloseWaitsForInFlightLaneOwnership(t *testing.T) {
	set, err := NewExecutionLanes(1, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	member := runtimeForLane(t, set, 0, 61)
	entered := make(chan struct{})
	release := make(chan struct{})
	member.driveHook = func() {
		close(entered)
		<-release
	}
	if err := set.addRuntime(member); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, _, runErr := set.RunOne(0)
		runDone <- runErr
	}()
	<-entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- set.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("close returned before lane owner: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil || member.closeCalls != 1 {
		t.Fatalf("close=%v calls=%d", err, member.closeCalls)
	}
}

func TestExecutionLanesClosePendingSettlementPreflightIsAtomic(t *testing.T) {
	set, err := NewExecutionLanes(2, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	ordinary := runtimeForLane(t, set, 0, 71)
	pending := runtimeForLane(t, set, 1, 171)
	pending.pendingSettlement = true
	if err := set.addRuntime(ordinary); err != nil {
		t.Fatal(err)
	}
	if err := set.addRuntime(pending); err != nil {
		t.Fatal(err)
	}
	if err := set.EnqueueProposal(ordinary.identity.Group, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	before := set.StatsInto(make([]ExecutionLaneStats, 0, 2))
	if err := set.Close(); !errors.Is(err, ErrGroupBusy) ||
		!errors.Is(err, raftmember.ErrResultSettlementPending) {
		t.Fatalf("pending close=%v", err)
	}
	after := set.StatsInto(make([]ExecutionLaneStats, 0, 2))
	if set.state.Load() != executionLanesOpen || ordinary.closeCalls != 0 || pending.closeCalls != 0 ||
		after[0].QueueItems != before[0].QueueItems || after[0].QueueBytes != before[0].QueueBytes {
		t.Fatalf("preflight mutated state=%d closes=%d/%d before=%+v after=%+v",
			set.state.Load(), ordinary.closeCalls, pending.closeCalls, before, after)
	}
	pending.driveHook = func() { pending.pendingSettlement = false }
	if _, _, err := set.RunOne(1); err != nil || pending.pendingSettlement {
		t.Fatalf("settlement retry err=%v pending=%t", err, pending.pendingSettlement)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if set.state.Load() != executionLanesClosed || ordinary.closeCalls != 1 || pending.closeCalls != 1 {
		t.Fatalf("terminal close state=%d closes=%d/%d", set.state.Load(), ordinary.closeCalls, pending.closeCalls)
	}
}

func TestExecutionLanesClosePreflightWaitsForInFlightSettlementRetry(t *testing.T) {
	set, err := NewExecutionLanes(1, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	pending := runtimeForLane(t, set, 0, 91)
	pending.pendingSettlement = true
	entered := make(chan struct{})
	release := make(chan struct{})
	pending.driveHook = func() {
		close(entered)
		<-release
		pending.pendingSettlement = false
	}
	if err := set.addRuntime(pending); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, _, runErr := set.RunOne(0)
		runDone <- runErr
	}()
	<-entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- set.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("close bypassed in-flight lane: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil || set.state.Load() != executionLanesClosed || pending.closeCalls != 1 {
		t.Fatalf("close=%v state=%d calls=%d", err, set.state.Load(), pending.closeCalls)
	}
}

func TestExecutionLanesCloseOwnsEveryRuntimeAndRetriesFailures(t *testing.T) {
	set, err := NewExecutionLanes(2, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	first := runtimeForLane(t, set, 0, 21)
	second := runtimeForLane(t, set, 1, 121)
	closeErr := errors.New("close retry")
	first.closeErrs = []error{closeErr, nil}
	if err := set.addRuntime(first); err != nil {
		t.Fatal(err)
	}
	if err := set.addRuntime(second); err != nil {
		t.Fatal(err)
	}
	if err := set.EnqueueProposal(first.identity.Group, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := set.EnqueueProposal(second.identity.Group, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := set.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("first close=%v", err)
	}
	stats := set.StatsInto(make([]ExecutionLaneStats, 0, 2))
	for _, stat := range stats {
		if stat.QueueItems != 0 || stat.QueueBytes != 0 || stat.OutboxItems != 0 || stat.OutboxBytes != 0 {
			t.Fatalf("closed lane retained payload: %+v", stat)
		}
	}
	if first.closeCalls != 1 || second.closeCalls != 1 {
		t.Fatalf("close calls=%d/%d", first.closeCalls, second.closeCalls)
	}
	if set.state.Load() != executionLanesClosing {
		t.Fatalf("failed close state=%d", set.state.Load())
	}
	if err := set.Close(); err != nil || first.closeCalls != 2 || second.closeCalls != 1 {
		t.Fatalf("retry close=%v calls=%d/%d", err, first.closeCalls, second.closeCalls)
	}
	if set.state.Load() != executionLanesClosed {
		t.Fatalf("retried close state=%d", set.state.Load())
	}
	if err := set.RequestTick(first.identity.Group); !errors.Is(err, ErrHostClosed) {
		t.Fatalf("post-close admission=%v", err)
	}
}

func TestExecutionLanesStatsIntoReusesCapacityWithoutAllocating(t *testing.T) {
	set, err := NewExecutionLanes(4, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	dst := make([]ExecutionLaneStats, 0, 4)
	if allocations := testing.AllocsPerRun(1000, func() {
		dst = set.StatsInto(dst[:0])
	}); allocations != 0 {
		t.Fatalf("StatsInto allocations=%v", allocations)
	}
	if len(dst) != 4 {
		t.Fatalf("stats length=%d", len(dst))
	}
}

func TestExecutionLanesConcurrentStatsWorkAndClose(t *testing.T) {
	set, err := NewExecutionLanes(2, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	members := []*fakeRuntime{runtimeForLane(t, set, 0, 101), runtimeForLane(t, set, 1, 201)}
	for _, member := range members {
		if err := set.addRuntime(member); err != nil {
			t.Fatal(err)
		}
	}
	var wait sync.WaitGroup
	start := make(chan struct{})
	for lane := range members {
		wait.Add(1)
		go func(lane int) {
			defer wait.Done()
			<-start
			for {
				if err := set.RequestTick(members[lane].identity.Group); err != nil {
					if !errors.Is(err, ErrHostClosed) {
						t.Errorf("tick lane=%d: %v", lane, err)
					}
					return
				}
				if _, _, err := set.RunOne(lane); err != nil {
					if !errors.Is(err, ErrHostClosed) {
						t.Errorf("run lane=%d: %v", lane, err)
					}
					return
				}
				members[lane].inputs = members[lane].inputs[:0]
			}
		}(lane)
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		dst := make([]ExecutionLaneStats, 0, 2)
		for set.state.Load() == executionLanesOpen {
			dst = set.StatsInto(dst[:0])
			if len(dst) != 2 {
				t.Errorf("stats length=%d", len(dst))
				return
			}
		}
	}()
	close(start)
	time.Sleep(10 * time.Millisecond)
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	if set.state.Load() != executionLanesClosed {
		t.Fatalf("close state=%d", set.state.Load())
	}
}

func TestExecutionLanesWarmTickHasNoAllocations(t *testing.T) {
	set, err := NewExecutionLanes(1, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	member := runtimeForLane(t, set, 0, 31)
	member.discardProposals = true
	if err := set.addRuntime(member); err != nil {
		t.Fatal(err)
	}
	if _, _, err := set.RunOne(0); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		if err := set.RequestTick(member.identity.Group); err != nil {
			panic(err)
		}
		progress, done, err := set.RunOne(0)
		if err != nil || !done || progress.Kind != ProgressTick {
			panic("tick did not progress")
		}
		member.inputs = member.inputs[:0]
	})
	if allocations != 0 {
		t.Fatalf("warm tick allocations=%v", allocations)
	}
	stats := set.StatsInto(make([]ExecutionLaneStats, 0, 1))
	if len(stats) != 1 || stats[0].QueueItems != 0 || stats[0].QueueBytes != 0 {
		t.Fatalf("space accounting=%+v", stats)
	}
}

func BenchmarkExecutionLanesScaling(b *testing.B) {
	for _, count := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("lanes_%d", count), func(b *testing.B) {
			limits := testHostLimits()
			set, err := NewExecutionLanes(count, limits)
			if err != nil {
				b.Fatal(err)
			}
			members := make([]*fakeRuntime, count)
			for lane := range count {
				member := runtimeForLane(b, set, lane, byte(17*lane+1))
				var work uint64 = uint64(lane + 1)
				member.tickHook = func() {
					for range 2048 {
						work = work*6364136223846793005 + 1442695040888963407
					}
					member.publication.Applied = work
				}
				members[lane] = member
				if err := set.addRuntime(member); err != nil {
					b.Fatal(err)
				}
				_, _, _ = set.RunOne(lane)
			}
			previous := runtime.GOMAXPROCS(count)
			b.Cleanup(func() { runtime.GOMAXPROCS(previous); _ = set.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			var wait sync.WaitGroup
			wait.Add(count)
			for lane := range count {
				go func(lane int) {
					defer wait.Done()
					for operation := lane; operation < b.N; operation += count {
						if err := set.RequestTick(members[lane].identity.Group); err != nil {
							b.Error(err)
							return
						}
						if progress, done, err := set.RunOne(lane); err != nil || !done || progress.Kind != ProgressTick {
							b.Errorf("progress=%+v done=%t err=%v", progress, done, err)
							return
						}
						members[lane].inputs = members[lane].inputs[:0]
					}
				}(lane)
			}
			wait.Wait()
			b.StopTimer()
			b.ReportMetric(float64(unsafe.Sizeof(executionLane{})), "lane-B")
		})
	}
}

func BenchmarkExecutionLanesHotShardIsolation(b *testing.B) {
	set, err := NewExecutionLanes(2, testHostLimits())
	if err != nil {
		b.Fatal(err)
	}
	hot := runtimeForLane(b, set, 0, 41)
	cold := runtimeForLane(b, set, 1, 141)
	var hotWork uint64 = 1
	hot.tickHook = func() {
		for range 8192 {
			hotWork = hotWork*6364136223846793005 + 1442695040888963407
		}
		hot.publication.Applied = hotWork
	}
	if err := set.addRuntime(hot); err != nil {
		b.Fatal(err)
	}
	if err := set.addRuntime(cold); err != nil {
		b.Fatal(err)
	}
	_, _, _ = set.RunOne(0)
	_, _, _ = set.RunOne(1)
	previous := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previous)
	var stop atomic.Bool
	var hotOperations atomic.Uint64
	hotDone := make(chan struct{})
	go func() {
		defer close(hotDone)
		for !stop.Load() {
			if set.RequestTick(hot.identity.Group) != nil {
				return
			}
			if _, done, runErr := set.RunOne(0); runErr != nil || !done {
				return
			}
			hot.inputs = hot.inputs[:0]
			hotOperations.Add(1)
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := set.RequestTick(cold.identity.Group); err != nil {
			b.Fatal(err)
		}
		if progress, done, err := set.RunOne(1); err != nil || !done || progress.Kind != ProgressTick {
			b.Fatalf("cold progress=%+v done=%t err=%v", progress, done, err)
		}
		cold.inputs = cold.inputs[:0]
	}
	b.StopTimer()
	stop.Store(true)
	<-hotDone
	b.ReportMetric(float64(hotOperations.Load()), "concurrent-hot-ops")
	b.ReportMetric(float64(unsafe.Sizeof(executionLane{})), "lane-B")
	stats := set.StatsInto(make([]ExecutionLaneStats, 0, 2))
	for _, stat := range stats {
		if stat.QueueItems != 0 || stat.QueueBytes != 0 {
			b.Fatalf("retained lane payload=%+v", stat)
		}
	}
	if err := set.Close(); err != nil {
		b.Fatal(err)
	}
}
