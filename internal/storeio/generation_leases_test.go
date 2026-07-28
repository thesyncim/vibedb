package storeio

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

func TestGenerationLeasesLifecycleAndMinimum(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 2})
	if err != nil {
		t.Fatal(err)
	}
	first, err := leases.Acquire(5)
	if err != nil {
		t.Fatal(err)
	}
	second, err := leases.Acquire(7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leases.Acquire(9); !errors.Is(err, ErrLeaseCapacity) {
		t.Fatalf("third Acquire = %v, want %v", err, ErrLeaseCapacity)
	}
	if got := leases.Minimum(10); got != 5 {
		t.Fatalf("Minimum = %d, want 5", got)
	}
	stats := leases.Stats(10)
	if stats.Capacity != 2 || stats.Active != 2 || stats.MinimumGeneration != 5 {
		t.Fatalf("Stats = %+v", stats)
	}
	if err := leases.Close(); !errors.Is(err, ErrLeasesActive) {
		t.Fatalf("Close active = %v, want %v", err, ErrLeasesActive)
	}
	if _, err := leases.Acquire(11); !errors.Is(err, ErrGenerationLeasesClosed) {
		t.Fatalf("Acquire while closing = %v, want %v", err, ErrGenerationLeasesClosed)
	}
	first.Release()
	first.Release()
	if got := leases.Minimum(10); got != 7 {
		t.Fatalf("Minimum after first release = %d, want 7", got)
	}
	second.Release()
	if got := leases.Minimum(10); got != 11 {
		t.Fatalf("Minimum without readers = %d, want 11", got)
	}
	if err := leases.Close(); err != nil {
		t.Fatal(err)
	}
	if err := leases.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationLeasesConcurrent(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 64})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := range 16 {
		group.Add(1)
		go func(generation uint64) {
			defer group.Done()
			for range 100 {
				lease, acquireErr := leases.Acquire(generation)
				if acquireErr != nil {
					t.Errorf("Acquire: %v", acquireErr)
					return
				}
				if lease.Generation() != generation {
					t.Errorf("Generation = %d, want %d", lease.Generation(), generation)
				}
				lease.Release()
			}
		}(uint64(worker + 1))
	}
	group.Wait()
	if stats := leases.Stats(20); stats.Active != 0 || stats.MinimumGeneration != 21 {
		t.Fatalf("Stats after workers = %+v", stats)
	}
}

func TestGenerationLeasesSafeFromSnapshotsBoundaries(t *testing.T) {
	var nilLeases *GenerationLeases
	if nilLeases.SafeFromSnapshots(0) {
		t.Fatal("nil leases accepted generation zero")
	}
	if !nilLeases.SafeFromSnapshots(1) {
		t.Fatal("nil leases rejected a valid generation")
	}

	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 3})
	if err != nil {
		t.Fatal(err)
	}
	if leases.SafeFromSnapshots(0) {
		t.Fatal("empty leases accepted generation zero")
	}
	if !leases.SafeFromSnapshots(1) {
		t.Fatal("empty leases rejected generation one")
	}
	lease5, err := leases.Acquire(5)
	if err != nil {
		t.Fatal(err)
	}
	lease7, err := leases.Acquire(7)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		generation uint64
		want       bool
	}{
		{1, false},
		{5, false},
		{6, false},
		{7, false},
		{8, true},
	} {
		if got := leases.SafeFromSnapshots(test.generation); got != test.want {
			t.Fatalf(
				"SafeFromSnapshots(%d) = %v, want %v",
				test.generation, got, test.want,
			)
		}
	}
	lease7.Release()
	if !leases.SafeFromSnapshots(6) {
		t.Fatal("older generation-five snapshot fenced generation six")
	}
	if leases.SafeFromSnapshots(5) {
		t.Fatal("equal generation-five snapshot was treated as safe")
	}
	lease5.Release()
	if !leases.SafeFromSnapshots(1) {
		t.Fatal("released leases still fenced generation one")
	}

	nearMaximum, err := leases.Acquire(^uint64(0) - 1)
	if err != nil {
		t.Fatal(err)
	}
	if leases.SafeFromSnapshots(^uint64(0) - 1) {
		t.Fatal("equal near-maximum snapshot was treated as safe")
	}
	if !leases.SafeFromSnapshots(^uint64(0)) {
		t.Fatal("strictly older near-maximum snapshot fenced maximum generation")
	}
	nearMaximum.Release()
	maximum, err := leases.Acquire(^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if leases.SafeFromSnapshots(^uint64(0)) {
		t.Fatal("equal maximum snapshot was treated as safe")
	}
	maximum.Release()
}

func TestGenerationLeasesSafeFromSnapshotsSummaryBoundaries(t *testing.T) {
	leases, err := NewGenerationLeases(
		GenerationLeaseOptions{MaxLeases: 1024},
	)
	if err != nil {
		t.Fatal(err)
	}
	older, err := leases.Acquire(5)
	if err != nil {
		t.Fatal(err)
	}
	newest, err := leases.Acquire(100)
	if err != nil {
		t.Fatal(err)
	}
	if !leases.SafeFromSnapshots(101) {
		t.Fatal("page born after every observed generation was fenced")
	}
	if leases.SafeFromSnapshots(100) {
		t.Fatal("active newest generation was treated as safe")
	}
	newest.Release()
	if !leases.SafeFromSnapshots(6) {
		t.Fatal("released high-water generation fenced an unrelated new page")
	}
	if leases.SafeFromSnapshots(5) {
		t.Fatal("active older generation was treated as safe")
	}
	older.Release()
	for generation := uint64(1); generation <= 101; generation++ {
		if !leases.SafeFromSnapshots(generation) {
			t.Fatalf("empty lease table fenced generation %d", generation)
		}
	}
}

func TestGenerationLeasesSafeFromSnapshotsConcurrent(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 32})
	if err != nil {
		t.Fatal(err)
	}
	const target = uint64(100)
	blocker, err := leases.Acquire(target)
	if err != nil {
		t.Fatal(err)
	}

	var unsafeTrue atomic.Bool
	var group sync.WaitGroup
	group.Go(func() {
		for range 20_000 {
			if leases.SafeFromSnapshots(target) {
				unsafeTrue.Store(true)
				return
			}
		}
	})
	for worker := range 8 {
		group.Add(1)
		go func(generation uint64) {
			defer group.Done()
			for range 2_000 {
				lease, acquireErr := leases.Acquire(generation)
				if acquireErr != nil {
					t.Errorf("Acquire: %v", acquireErr)
					return
				}
				lease.Release()
			}
		}(uint64(worker + 1))
	}
	group.Wait()
	if unsafeTrue.Load() {
		t.Fatal("query reported safety while an equal-generation lease was active")
	}
	blocker.Release()
	if !leases.SafeFromSnapshots(target) {
		t.Fatal("query remained fenced after every lease was released")
	}
}

func BenchmarkGenerationLeases(b *testing.B) {
	b.Run("acquire-release", func(b *testing.B) {
		leases, err := NewGenerationLeases(
			GenerationLeaseOptions{MaxLeases: 1024},
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			lease, acquireErr := leases.Acquire(100)
			if acquireErr != nil {
				b.Fatal(acquireErr)
			}
			lease.Release()
		}
	})
	b.Run("safe-no-active", func(b *testing.B) {
		leases, err := NewGenerationLeases(
			GenerationLeaseOptions{MaxLeases: 1024},
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if !leases.SafeFromSnapshots(1) {
				b.Fatal("empty table fenced page")
			}
		}
	})
	b.Run("safe-new-page-with-active-history", func(b *testing.B) {
		leases, err := NewGenerationLeases(
			GenerationLeaseOptions{MaxLeases: 1024},
		)
		if err != nil {
			b.Fatal(err)
		}
		lease, err := leases.Acquire(100)
		if err != nil {
			b.Fatal(err)
		}
		defer lease.Release()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if !leases.SafeFromSnapshots(101) {
				b.Fatal("new page was fenced")
			}
		}
	})
	b.Run("safe-historical-worst-case", func(b *testing.B) {
		leases, err := NewGenerationLeases(
			GenerationLeaseOptions{MaxLeases: 1024},
		)
		if err != nil {
			b.Fatal(err)
		}
		holders := make([]GenerationLease, 1024)
		for i := range holders {
			generation := uint64(99)
			if i == len(holders)-1 {
				generation = 100
			}
			holders[i], err = leases.Acquire(generation)
			if err != nil {
				b.Fatal(err)
			}
		}
		defer func() {
			for i := range holders {
				holders[i].Release()
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if leases.SafeFromSnapshots(100) {
				b.Fatal("equal-generation reader did not fence page")
			}
		}
	})
}

func TestGenerationLeaseStaleCopyCannotReleaseReusedSlot(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := leases.Acquire(5)
	stale := first
	first.Release()
	second, err := leases.Acquire(5)
	if err != nil {
		t.Fatal(err)
	}
	stale.Release()
	if stats := leases.Stats(5); stats.Active != 1 || stats.MinimumGeneration != 5 {
		t.Fatalf("stale release changed active lease: %+v", stats)
	}
	second.Release()
}

func TestExtentReclaimerRespectsReadersAndRecoveryRoots(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 2})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(leases, ExtentReclaimerOptions{MaxRetiredExtents: 3})
	if err != nil {
		t.Fatal(err)
	}
	reader5, _ := leases.Acquire(5)
	reader7, _ := leases.Acquire(7)
	for generation := uint64(4); generation <= 6; generation++ {
		if err := reclaimer.Retire(FreeExtent{
			Offset: generation * 4096, Length: 4096, RetiredGeneration: generation,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reclaimer.Retire(FreeExtent{Offset: 10 * 4096, Length: 4096, RetiredGeneration: 8}); !errors.Is(err, ErrRetiredExtentCapacity) {
		t.Fatalf("Retire over capacity = %v, want %v", err, ErrRetiredExtentCapacity)
	}

	reusable := make([]FreeExtent, 0, 3)
	reusable, err = reclaimer.AppendReusable(
		reusable, 7, 6, len(reusable)+16,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(reusable) != 1 || reusable[0].RetiredGeneration != 4 {
		t.Fatalf("first reusable = %+v, want generation 4", reusable)
	}
	reader5.Release()
	reusable, err = reclaimer.AppendReusable(reusable[:0], 7, 6, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(reusable) != 1 || reusable[0].RetiredGeneration != 5 {
		t.Fatalf("second reusable = %+v, want generation 5", reusable)
	}
	reader7.Release()
	reusable, err = reclaimer.AppendReusable(reusable[:0], 7, 7, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(reusable) != 1 || reusable[0].RetiredGeneration != 6 {
		t.Fatalf("third reusable = %+v, want generation 6", reusable)
	}
	if stats := reclaimer.Stats(); stats.Pending != 0 || stats.PendingBytes != 0 {
		t.Fatalf("final Stats = %+v", stats)
	}
}

func TestExtentReclaimerRejectsOverlap(t *testing.T) {
	leases, _ := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	reclaimer, _ := NewExtentReclaimer(leases, ExtentReclaimerOptions{MaxRetiredExtents: 2})
	if err := reclaimer.Retire(FreeExtent{Offset: 4096, Length: 8192, RetiredGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.Retire(FreeExtent{Offset: 8192, Length: 4096, RetiredGeneration: 2}); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("overlap = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestExtentReclaimerBatchIsAtomicAndCancelable(t *testing.T) {
	leases, _ := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	reclaimer, _ := NewExtentReclaimer(leases, ExtentReclaimerOptions{MaxRetiredExtents: 3})
	batch := []FreeExtent{
		{Offset: 4096, Length: 4096, RetiredGeneration: 4},
		{Offset: 8192, Length: 4096, RetiredGeneration: 4},
	}
	if err := reclaimer.RetireBatch(batch); err != nil {
		t.Fatal(err)
	}
	if stats := reclaimer.Stats(); stats.Pending != 2 {
		t.Fatalf("Stats after batch = %+v", stats)
	}
	if err := reclaimer.RetireBatch([]FreeExtent{
		{Offset: 12288, Length: 4096, RetiredGeneration: 5},
		{Offset: 16384, Length: 4096, RetiredGeneration: 5},
	}); !errors.Is(err, ErrRetiredExtentCapacity) {
		t.Fatalf("over-capacity batch = %v, want %v", err, ErrRetiredExtentCapacity)
	}
	if stats := reclaimer.Stats(); stats.Pending != 2 {
		t.Fatalf("failed batch changed Stats = %+v", stats)
	}
	if err := reclaimer.CancelRetiredGeneration(4); err != nil {
		t.Fatal(err)
	}
	if stats := reclaimer.Stats(); stats.Pending != 0 {
		t.Fatalf("Stats after cancel = %+v", stats)
	}
}

func TestGenerationLeaseAndReclaimerSteadyAllocation(t *testing.T) {
	leases, _ := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	reclaimer, _ := NewExtentReclaimer(leases, ExtentReclaimerOptions{MaxRetiredExtents: 1})
	dst := make([]FreeExtent, 0, 1)
	if allocs := testing.AllocsPerRun(1000, func() {
		lease, err := leases.Acquire(2)
		if err != nil {
			panic(err)
		}
		lease.Release()
		if err := reclaimer.Retire(FreeExtent{Offset: 4096, Length: 4096, RetiredGeneration: 1}); err != nil {
			panic(err)
		}
		var reclaimErr error
		dst, reclaimErr = reclaimer.AppendReusable(
			dst[:0], 2, 2, cap(dst),
		)
		if reclaimErr != nil {
			panic(reclaimErr)
		}
		if len(dst) != 1 {
			panic("extent not reclaimed")
		}
	}); allocs != 0 {
		t.Fatalf("lease/reclaimer steady allocations = %g, want 0", allocs)
	}
}

func TestExtentReclaimerPinnedFragmentationDoesNotDisturbOrder(t *testing.T) {
	const count = 1 << 14
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: count},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Replay is offset ordered, not generation ordered. Deliberately alternate
	// generations so Restore has to establish the reclaimer's own ordering.
	extents := make([]FreeExtent, count)
	var wantBytes uint64
	for i := range extents {
		generation := uint64(2 + i/2)
		if i&1 == 0 {
			generation += count
		}
		extents[i] = FreeExtent{
			Offset:            uint64(i+1) * 4096,
			Length:            4096,
			RetiredGeneration: generation,
		}
		wantBytes += extents[i].Length
	}
	if err := reclaimer.Restore(extents); err != nil {
		t.Fatal(err)
	}
	blocker, err := leases.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]FreeExtent, 0, 7)
	for range 100 {
		dst, err = reclaimer.AppendReusable(
			dst[:0], 2*count+2, 2*count+2, cap(dst),
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(dst) != 0 {
			t.Fatalf("pinned reclamation moved %d extents", len(dst))
		}
		stats := reclaimer.Stats()
		if stats.Pending != count || stats.PendingBytes != wantBytes ||
			stats.OldestRetired != 2 {
			t.Fatalf("pinned stats = %+v", stats)
		}
	}
	blocker.Release()

	var previous uint64
	moved := 0
	for reclaimer.PendingCount() != 0 {
		dst, err = reclaimer.AppendReusable(
			dst[:0], 2*count+2, 2*count+2, cap(dst),
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(dst) == 0 {
			t.Fatal("unfenced fragmented queue made no progress")
		}
		for _, extent := range dst {
			if extent.RetiredGeneration < previous {
				t.Fatalf(
					"reclamation generation regressed from %d to %d",
					previous, extent.RetiredGeneration,
				)
			}
			previous = extent.RetiredGeneration
		}
		moved += len(dst)
	}
	if moved != count {
		t.Fatalf("moved %d extents, want %d", moved, count)
	}
	if stats := reclaimer.Stats(); stats.Pending != 0 ||
		stats.PendingBytes != 0 || stats.OldestRetired != 0 {
		t.Fatalf("drained stats = %+v", stats)
	}
}

func TestExtentReclaimerReordersReturnedOlderGenerationAfterHeadDrain(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 5},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.Restore([]FreeExtent{
		{Offset: 4 * 4096, Length: 4096, RetiredGeneration: 4},
		{Offset: 5 * 4096, Length: 4096, RetiredGeneration: 5},
		{Offset: 6 * 4096, Length: 4096, RetiredGeneration: 6},
	}); err != nil {
		t.Fatal(err)
	}
	dst := make([]FreeExtent, 0, 5)
	dst, err = reclaimer.AppendReusable(dst, 5, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(dst) != 1 || dst[0].RetiredGeneration != 4 {
		t.Fatalf("head drain = %+v", dst)
	}
	// A fold-budget trim returns already-drained work through RetireBatch. It
	// must be inserted before the newer fenced generations even though there is
	// reclaimable capacity before the queue's physical head.
	returned := dst[0]
	returned.Offset = 7 * 4096
	if err := reclaimer.Retire(returned); err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.Retire(FreeExtent{
		Offset: 8 * 4096, Length: 4096, RetiredGeneration: 7,
	}); err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.CancelRetiredGeneration(7); err != nil {
		t.Fatal(err)
	}
	dst, err = reclaimer.AppendReusable(dst[:0], 8, 8, cap(dst))
	if err != nil {
		t.Fatal(err)
	}
	if len(dst) != 3 {
		t.Fatalf("final drain = %+v", dst)
	}
	for i, generation := range []uint64{4, 5, 6} {
		if dst[i].RetiredGeneration != generation {
			t.Fatalf("final drain[%d] = %+v, want generation %d",
				i, dst[i], generation)
		}
	}
	if stats := reclaimer.Stats(); stats.Pending != 0 ||
		stats.PendingBytes != 0 || stats.OldestRetired != 0 {
		t.Fatalf("drained stats = %+v", stats)
	}
}

func TestExtentReclaimerAppendPendingContract(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	restored := []FreeExtent{
		{Offset: 12 * 4096, Length: 4096, RetiredGeneration: 9},
		{Offset: 8 * 4096, Length: 4096, RetiredGeneration: 5},
		{Offset: 4 * 4096, Length: 4096, RetiredGeneration: 5},
	}
	if err := reclaimer.Restore(restored); err != nil {
		t.Fatal(err)
	}
	prefix := FreeExtent{
		Offset: 1, Length: 1, RetiredGeneration: 1,
	}
	got := reclaimer.AppendPending([]FreeExtent{prefix})
	if len(got) != 4 || got[0] != prefix {
		t.Fatalf("AppendPending prefix/completeness = %+v", got)
	}
	pending := got[1:]
	for i := 1; i < len(pending); i++ {
		if pending[i].RetiredGeneration < pending[i-1].RetiredGeneration {
			t.Fatalf("AppendPending generation order = %+v", pending)
		}
	}
	slices.SortFunc(pending, func(a, b FreeExtent) int {
		return int(a.Offset/4096) - int(b.Offset/4096)
	})
	want := []FreeExtent{
		{Offset: 4 * 4096, Length: 4096, RetiredGeneration: 5},
		{Offset: 8 * 4096, Length: 4096, RetiredGeneration: 5},
		{Offset: 12 * 4096, Length: 4096, RetiredGeneration: 9},
	}
	if !slices.Equal(pending, want) {
		t.Fatalf("offset-sorted AppendPending = %+v, want %+v", pending, want)
	}

	moved, err := reclaimer.AppendReusable(
		make([]FreeExtent, 0, 1), 6, 6, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(moved) != 1 || moved[0].RetiredGeneration != 5 {
		t.Fatalf("bounded head reclaim = %+v, want generation 5", moved)
	}
	got = reclaimer.AppendPending(got[:0])
	if len(got) != 2 || got[0].RetiredGeneration != 5 ||
		got[1].RetiredGeneration != 9 {
		t.Fatalf("AppendPending after head reclaim = %+v", got)
	}
}

func BenchmarkExtentReclaimerPinnedFragmentation(b *testing.B) {
	for _, count := range []int{256, 4 << 10, 64 << 10} {
		b.Run(fmt.Sprintf("extents=%d", count), func(b *testing.B) {
			leases, err := NewGenerationLeases(
				GenerationLeaseOptions{MaxLeases: 1},
			)
			if err != nil {
				b.Fatal(err)
			}
			reclaimer, err := NewExtentReclaimer(
				leases, ExtentReclaimerOptions{MaxRetiredExtents: count},
			)
			if err != nil {
				b.Fatal(err)
			}
			extents := make([]FreeExtent, count)
			for i := range extents {
				extents[i] = FreeExtent{
					Offset:            uint64(i+1) * 4096,
					Length:            4096,
					RetiredGeneration: uint64(i + 2),
				}
			}
			if err := reclaimer.Restore(extents); err != nil {
				b.Fatal(err)
			}
			blocker, err := leases.Acquire(1)
			if err != nil {
				b.Fatal(err)
			}
			defer blocker.Release()
			dst := make([]FreeExtent, 0, 256)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				dst, err = reclaimer.AppendReusable(
					dst[:0], uint64(count+2), uint64(count+2), cap(dst),
				)
				if err != nil {
					b.Fatal(err)
				}
				stats := reclaimer.Stats()
				if len(dst) != 0 || stats.Pending != uint64(count) {
					b.Fatal("pinned free-space state changed")
				}
			}
		})
	}
}

func BenchmarkExtentReclaimerSteadyChurn(b *testing.B) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		b.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 1},
	)
	if err != nil {
		b.Fatal(err)
	}
	extent := FreeExtent{
		Offset: 4096, Length: 4096, RetiredGeneration: 1,
	}
	dst := make([]FreeExtent, 0, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := reclaimer.Retire(extent); err != nil {
			b.Fatal(err)
		}
		if stats := reclaimer.Stats(); stats.Pending != 1 ||
			stats.PendingBytes != extent.Length {
			b.Fatal("retirement accounting changed")
		}
		dst, err = reclaimer.AppendReusable(dst[:0], 2, 2, 1)
		if err != nil {
			b.Fatal(err)
		}
		if len(dst) != 1 {
			b.Fatal("retired extent was not reusable")
		}
	}
}
