package storeio

import (
	"errors"
	"sync"
	"testing"
)

func newBufferedCheckpointLabTest(
	t testing.TB, options BufferedCheckpointLabOptions,
) *BufferedCheckpointLab {
	t.Helper()
	lab, err := NewBufferedCheckpointLab(options)
	if err != nil {
		t.Fatal(err)
	}
	return lab
}

func bufferedCheckpointLabRead(
	t testing.TB, lab *BufferedCheckpointLab, root BufferedCheckpointLabRoot,
	key int,
) uint64 {
	t.Helper()
	value, err := lab.ReadRoot(root, key)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestBufferedCheckpointLabVisibleCoalescesBeforeDurability(
	t *testing.T,
) {
	lab := newBufferedCheckpointLabTest(t, BufferedCheckpointLabOptions{
		Keys: 2, FrameCapacity: 12, MaxDirtyFrames: 4,
	})
	const mutations = 100
	for value := uint64(1); value <= mutations; value++ {
		mutation, err := lab.Mutate(0, value)
		if err != nil {
			t.Fatal(err)
		}
		if mutation.Mode != BufferedCheckpointLabMaterialized ||
			mutation.Coalesced != (value != 1) {
			t.Fatalf("mutation %d = %+v", value, mutation)
		}
	}
	visible, err := lab.ReadVisible(0)
	if err != nil || visible != mutations {
		t.Fatalf("visible = %d,%v", visible, err)
	}
	durable, err := lab.ReadDurable(0)
	if err != nil || durable != 0 {
		t.Fatalf("durable before checkpoint = %d,%v", durable, err)
	}

	plan, err := lab.SealCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if plan.PageWrites != 1 || plan.ShadowWrites != 1 ||
		plan.LogicalMutations != mutations ||
		plan.CoalescedMutations != mutations-1 ||
		plan.BeforeValues[0] != 0 ||
		plan.AfterValues[0] != mutations {
		t.Fatalf("checkpoint = %+v", plan)
	}
	if err := lab.CompleteCheckpoint(plan.Sequence); err != nil {
		t.Fatal(err)
	}
	durable, err = lab.ReadDurable(0)
	if err != nil || durable != mutations {
		t.Fatalf("durable after checkpoint = %d,%v", durable, err)
	}
	if stats := lab.Stats(); stats.DurableGeneration != mutations+1 ||
		stats.VisibleGeneration != mutations+1 ||
		stats.ActiveDirty != 0 || stats.SealedGeneration != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestBufferedCheckpointLabSnapshotForcesOneCopyThenOwned(
	t *testing.T,
) {
	lab := newBufferedCheckpointLabTest(t, BufferedCheckpointLabOptions{
		Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 2,
	})
	snapshot, err := lab.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first, err := lab.Mutate(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if first.Mode != BufferedCheckpointLabCopied {
		t.Fatalf("first = %+v", first)
	}
	second, err := lab.Mutate(0, 11)
	if err != nil {
		t.Fatal(err)
	}
	if second.Mode != BufferedCheckpointLabMaterialized ||
		!second.Coalesced {
		t.Fatalf("second = %+v", second)
	}
	old, err := snapshot.Read(0)
	if err != nil || old != 0 {
		t.Fatalf("snapshot = %d,%v", old, err)
	}
	current, err := lab.ReadVisible(0)
	if err != nil || current != 11 {
		t.Fatalf("visible = %d,%v", current, err)
	}
	if !snapshot.Close() || snapshot.Close() {
		t.Fatal("snapshot close was not exactly once")
	}
	if stats := lab.Stats(); stats.Copied != 1 ||
		stats.Materialized != 1 || stats.ActiveDirty != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestBufferedCheckpointLabCheckpointIsSnapshot(
	t *testing.T,
) {
	lab := newBufferedCheckpointLabTest(t, BufferedCheckpointLabOptions{
		Keys: 2, FrameCapacity: 12, MaxDirtyFrames: 2,
	})
	if _, err := lab.Mutate(0, 10); err != nil {
		t.Fatal(err)
	}
	first, err := lab.SealCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if first.ShadowWrites != 1 {
		t.Fatalf("first checkpoint = %+v", first)
	}
	mutation, err := lab.Mutate(0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Mode != BufferedCheckpointLabCopied {
		t.Fatalf("post-seal mutation = %+v", mutation)
	}
	if got := bufferedCheckpointLabRead(
		t, lab, first.Sealed, 0,
	); got != 10 {
		t.Fatalf("sealed value = %d", got)
	}
	if got, _ := lab.ReadVisible(0); got != 20 {
		t.Fatalf("visible value = %d", got)
	}
	if err := lab.CompleteCheckpoint(first.Sequence); err != nil {
		t.Fatal(err)
	}
	if got, _ := lab.ReadDurable(0); got != 10 {
		t.Fatalf("first durable value = %d", got)
	}
	if got, _ := lab.ReadVisible(0); got != 20 {
		t.Fatalf("visible after first checkpoint = %d", got)
	}

	second, err := lab.SealCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if second.PageWrites != 1 || second.ShadowWrites != 0 {
		t.Fatalf("second checkpoint = %+v", second)
	}
	if err := lab.CompleteCheckpoint(second.Sequence); err != nil {
		t.Fatal(err)
	}
	if got, _ := lab.ReadDurable(0); got != 20 {
		t.Fatalf("second durable value = %d", got)
	}
}

func TestBufferedCheckpointLabTwoEpochBackpressure(
	t *testing.T,
) {
	lab := newBufferedCheckpointLabTest(t, BufferedCheckpointLabOptions{
		Keys: 3, FrameCapacity: 10, MaxDirtyFrames: 1,
	})
	if _, err := lab.Mutate(0, 10); err != nil {
		t.Fatal(err)
	}
	plan, err := lab.SealCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lab.Mutate(1, 20); err != nil {
		t.Fatal(err)
	}
	if mutation, err := lab.Mutate(1, 21); err != nil ||
		!mutation.Coalesced {
		t.Fatalf("coalesced at hard limit = %+v,%v", mutation, err)
	}
	before := lab.VisibleRoot()
	if _, err := lab.Mutate(2, 30); !errors.Is(
		err, ErrBufferedCheckpointLabBackpressure,
	) {
		t.Fatalf("hard limit error = %v", err)
	}
	if after := lab.VisibleRoot(); after != before {
		t.Fatal("rejected mutation changed visible root")
	}
	if _, err := lab.SealCheckpoint(); !errors.Is(
		err, ErrBufferedCheckpointLabBackpressure,
	) {
		t.Fatalf("second sealed epoch error = %v", err)
	}
	if err := lab.CompleteCheckpoint(plan.Sequence); err != nil {
		t.Fatal(err)
	}
	if stats := lab.Stats(); stats.ActiveDirty != 1 ||
		stats.BackpressureEvents != 2 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestBufferedCheckpointLabCrashCutsSelectOnlyCompleteRoot(
	t *testing.T,
) {
	lab := newBufferedCheckpointLabTest(t, BufferedCheckpointLabOptions{
		Keys: 2, FrameCapacity: 12, MaxDirtyFrames: 2,
		InitialValueOffset: 100,
	})
	if _, err := lab.Mutate(0, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.Mutate(1, 2000); err != nil {
		t.Fatal(err)
	}
	plan, err := lab.SealCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for phase := BufferedCheckpointLabBeforeData; phase <= BufferedCheckpointLabAfterRoot; phase++ {
		recovered, recoverErr :=
			RecoverBufferedCheckpointLabCheckpoint(plan, phase)
		if recoverErr != nil {
			t.Fatal(recoverErr)
		}
		if phase == BufferedCheckpointLabAfterRoot {
			if recovered.Root != plan.After ||
				recovered.Values[0] != 1000 ||
				recovered.Values[1] != 2000 {
				t.Fatalf("after-root recovery = %+v", recovered)
			}
		} else if recovered.Root != plan.Before ||
			recovered.Values[0] != 100 ||
			recovered.Values[1] != 101 {
			t.Fatalf("phase %d recovery = %+v", phase, recovered)
		}
	}
	tampered := plan
	tampered.PageWrites = 0
	if _, err := RecoverBufferedCheckpointLabCheckpoint(
		tampered, BufferedCheckpointLabAfterRoot,
	); !errors.Is(err, ErrBufferedCheckpointLabState) {
		t.Fatalf("tampered recovery error = %v", err)
	}
}

func TestBufferedCheckpointLabDirtyHistoricalFrameIsBounded(
	t *testing.T,
) {
	lab := newBufferedCheckpointLabTest(t, BufferedCheckpointLabOptions{
		Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 1,
	})
	if _, err := lab.Mutate(0, 1); err != nil {
		t.Fatal(err)
	}
	snapshot, err := lab.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := lab.Mutate(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Mode != BufferedCheckpointLabCopied ||
		mutation.DirtyFrames != 1 {
		t.Fatalf("copy with dirty credit = %+v", mutation)
	}
	if stats := lab.Stats(); stats.HistoricalFrames != 1 ||
		stats.ActiveDirty != 1 {
		t.Fatalf("retained stats = %+v", stats)
	}
	if !snapshot.Close() {
		t.Fatal("close")
	}
	if stats := lab.Stats(); stats.HistoricalFrames != 0 ||
		stats.LiveFrames != 1 {
		t.Fatalf("reclaimed stats = %+v", stats)
	}
}

func TestBufferedCheckpointLabHotPathsAllocateNothing(
	t *testing.T,
) {
	lab := newBufferedCheckpointLabTest(t, BufferedCheckpointLabOptions{
		Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 2,
	})
	if _, err := lab.Mutate(0, 1); err != nil {
		t.Fatal(err)
	}
	value := uint64(1)
	if allocations := testing.AllocsPerRun(1000, func() {
		value++
		if _, err := lab.Mutate(0, value); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("owned mutation allocations = %g", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		snapshot, err := lab.Snapshot()
		if err != nil {
			panic(err)
		}
		if !snapshot.Close() {
			panic("close")
		}
	}); allocations != 0 {
		t.Fatalf("snapshot allocations = %g", allocations)
	}
}

func TestBufferedCheckpointLabConcurrentSnapshotRemainsExact(
	t *testing.T,
) {
	lab := newBufferedCheckpointLabTest(t, BufferedCheckpointLabOptions{
		Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 2,
	})
	snapshot, err := lab.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	wait.Add(1)
	readErr := make(chan error, 1)
	go func() {
		defer wait.Done()
		for range 1000 {
			value, err := snapshot.Read(0)
			if err != nil {
				readErr <- err
				return
			}
			if value != 0 {
				readErr <- ErrBufferedCheckpointLabState
				return
			}
		}
	}()
	for value := uint64(1); value <= 1000; value++ {
		if _, err := lab.Mutate(0, value); err != nil {
			t.Fatal(err)
		}
	}
	wait.Wait()
	close(readErr)
	for err := range readErr {
		t.Fatal(err)
	}
	if !snapshot.Close() {
		t.Fatal("close")
	}
	if value, err := lab.ReadVisible(0); err != nil || value != 1000 {
		t.Fatalf("visible = %d,%v", value, err)
	}
}
