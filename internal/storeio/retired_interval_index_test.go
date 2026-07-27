package storeio

import (
	"math/rand"
	"testing"
	"unsafe"
)

func TestRetiredIntervalIndexNodeSize(t *testing.T) {
	if got := unsafe.Sizeof(retiredIntervalNode{}); got != 24 {
		t.Fatalf("retired interval node = %d bytes, want 24", got)
	}
	if got := RetiredIntervalIndexStorageBytes(1 << 24); got != 24<<24 {
		t.Fatalf("maximum interval storage = %d, want %d", got, 24<<24)
	}
	if got := RetiredExtentStorageBytes(1 << 24); got != 24<<24 {
		t.Fatalf("maximum extent storage = %d, want %d", got, 24<<24)
	}
	if RetiredIntervalIndexStorageBytes(1<<24+1) != 0 ||
		RetiredExtentStorageBytes(1<<24+1) != 0 {
		t.Fatal("storage helper accepted capacity beyond packed-rank limit")
	}
	index := retiredIntervalIndex{nodes: make([]retiredIntervalNode, 1)}
	index.setNodeLeft(0, 1<<24-1)
	index.setNodeHeight(0, 33)
	if got := index.nodeLeft(0); got != 1<<24-1 {
		t.Fatalf("maximum packed link = %d, want %d", got, 1<<24-1)
	}
	if got := index.nodeHeight(0); got != 33 {
		t.Fatalf("packed height = %d, want 33", got)
	}
}

func TestRetiredIntervalIndexBoundariesAndReuse(t *testing.T) {
	index := newRetiredIntervalIndex(3)
	if !index.insert(100, 20) || !index.insert(200, 40) {
		t.Fatal("insert failed")
	}
	for _, test := range []struct {
		offset, length uint64
		overlap        bool
	}{
		{1, 99, false},
		{80, 20, false},
		{80, 21, true},
		{100, 1, true},
		{119, 1, true},
		{120, 80, false},
		{199, 1, false},
		{199, 2, true},
		{239, 1, true},
		{240, 1, false},
	} {
		if got := index.overlaps(test.offset, test.length); got != test.overlap {
			t.Errorf("overlaps(%d,%d) = %v, want %v",
				test.offset, test.length, got, test.overlap)
		}
	}
	if !index.contains(100, 20) || index.contains(100, 19) {
		t.Fatal("exact membership differs")
	}
	if index.insert(110, 1) {
		t.Fatal("overlapping insert succeeded")
	}
	if index.remove(200, 39) || !index.contains(200, 40) {
		t.Fatal("inexact removal changed interval ownership")
	}
	if !index.remove(100, 20) || index.remove(100, 20) || index.len() != 1 {
		t.Fatalf("remove/count = %d", index.len())
	}
	if !index.insert(120, 80) || index.len() != 2 {
		t.Fatal("freed node was not reusable")
	}
	index.reset()
	if index.len() != 0 || index.overlaps(200, 1) ||
		!index.insert(1, 1) {
		t.Fatal("reset did not restore empty capacity")
	}
}

func TestRetiredIntervalIndexRandomizedDifferential(t *testing.T) {
	const count = 10_000
	index := newRetiredIntervalIndex(count)
	order := rand.New(rand.NewSource(0x51a7)).Perm(count)
	for _, rank := range order {
		offset := uint64(rank*3 + 1)
		if !index.insert(offset, 2) {
			t.Fatalf("insert %d failed", rank)
		}
	}
	if index.len() != count {
		t.Fatalf("len = %d, want %d", index.len(), count)
	}
	assertRetiredIntervalIndex(t, &index)
	for rank := range count {
		offset := uint64(rank*3 + 1)
		if !index.contains(offset, 2) ||
			!index.overlaps(offset, 1) ||
			!index.overlaps(offset+1, 1) {
			t.Fatalf("extent %d not found", rank)
		}
		if index.overlaps(offset+2, 1) {
			t.Fatalf("gap after extent %d overlaps", rank)
		}
	}
	for _, rank := range order {
		if rank&1 == 0 && !index.remove(uint64(rank*3+1), 2) {
			t.Fatalf("remove %d failed", rank)
		}
	}
	for rank := range count {
		offset := uint64(rank*3 + 1)
		if got := index.contains(offset, 2); got != (rank&1 != 0) {
			t.Fatalf("membership %d = %v", rank, got)
		}
	}
	assertRetiredIntervalIndex(t, &index)
}

func TestRetiredIntervalIndexIterativeDeepChain(t *testing.T) {
	const count = 1 << 18
	index := retiredIntervalIndex{
		nodes: make([]retiredIntervalNode, count),
		root:  0, free: retiredIntervalNone, count: count,
		high: int32(count), initialized: true,
	}
	for rank := range count {
		right := retiredIntervalNone
		if rank+1 < count {
			right = int32(rank + 1)
		}
		index.nodes[rank] = retiredIntervalNode{
			offset: uint64(rank*2 + 1), length: 1,
			leftHeight: uint32(1) << retiredIntervalLinkBits,
			right:      right,
		}
	}
	tailOffset := uint64((count-1)*2 + 1)
	if !index.overlaps(tailOffset, 1) {
		t.Fatal("deep-chain tail was not found")
	}
	if index.remove(tailOffset, 1) || index.len() != count {
		t.Fatal("malformed deep chain did not fail closed at the bounded path")
	}
}

func TestRetiredIntervalIndexAdversarialOrdersStayBalanced(t *testing.T) {
	const count = 1 << 17
	for _, order := range []struct {
		name string
		rank func(int) int
	}{
		{name: "ascending", rank: func(rank int) int { return rank }},
		{name: "descending", rank: func(rank int) int { return count - 1 - rank }},
		{name: "outside-in", rank: func(rank int) int {
			if rank&1 == 0 {
				return rank / 2
			}
			return count - 1 - rank/2
		}},
	} {
		t.Run(order.name, func(t *testing.T) {
			index := newRetiredIntervalIndex(count)
			for position := range count {
				rank := order.rank(position)
				if !index.insert(uint64(rank*3+1), 2) {
					t.Fatalf("insert %d failed", rank)
				}
			}
			assertRetiredIntervalIndex(t, &index)
			if height := index.nodeHeight(index.root); height > 32 {
				t.Fatalf("height = %d for %d nodes", height, count)
			}
			for position := range count {
				rank := order.rank(position)
				if !index.remove(uint64(rank*3+1), 2) {
					t.Fatalf("remove %d failed", rank)
				}
			}
			if index.len() != 0 || index.root != retiredIntervalNone {
				t.Fatal("adversarial drain retained intervals")
			}
		})
	}
}

func TestExtentReclaimerIntervalIndexRejectsExistingAndRestoredOverlap(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 8},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.RetireBatch([]FreeExtent{
		{Offset: 4096, Length: 4096, RetiredGeneration: 1},
		{Offset: 16384, Length: 8192, RetiredGeneration: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.Retire(FreeExtent{
		Offset: 8191, Length: 2, RetiredGeneration: 2,
	}); err == nil {
		t.Fatal("overlap with existing retirement succeeded")
	}
	if reclaimer.PendingCount() != 2 || reclaimer.indexed ||
		reclaimer.intervals.len() != 0 {
		t.Fatal("failed retirement changed the atomic set")
	}
	if err := reclaimer.CancelRetiredGeneration(1); err != nil {
		t.Fatal(err)
	}
	if reclaimer.PendingCount() != 0 || reclaimer.intervals.len() != 0 {
		t.Fatal("cancel left interval ownership behind")
	}
	if err := reclaimer.Restore([]FreeExtent{
		{Offset: 4096, Length: 8192, RetiredGeneration: 1},
		{Offset: 8192, Length: 4096, RetiredGeneration: 2},
	}); err == nil {
		t.Fatal("overlapping restore succeeded")
	}
	if reclaimer.PendingCount() != 0 || reclaimer.intervals.len() != 0 {
		t.Fatal("failed restore retained partial ownership")
	}
	if err := reclaimer.RetireBatch([]FreeExtent{
		{Offset: 4096, Length: 8192, RetiredGeneration: 3},
		{Offset: 8192, Length: 4096, RetiredGeneration: 3},
	}); err == nil {
		t.Fatal("overlapping incoming batch succeeded")
	}
	if reclaimer.PendingCount() != 0 || reclaimer.intervals.len() != 0 {
		t.Fatal("failed incoming batch retained partial ownership")
	}
}

func TestExtentReclaimerIntervalIndexOperationsAllocateZero(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 8},
	)
	if err != nil {
		t.Fatal(err)
	}
	extent := FreeExtent{Offset: 4096, Length: 4096, RetiredGeneration: 2}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := reclaimer.Retire(extent); err != nil {
			panic(err)
		}
		if err := reclaimer.CancelRetiredGeneration(extent.RetiredGeneration); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("retire/cancel allocations = %.2f, want 0", allocations)
	}
	if reclaimer.indexed || reclaimer.intervals.high != 0 {
		t.Fatal("small steady churn maintained the large-set interval index")
	}
	if stats := reclaimer.Stats(); stats.Pending != 0 ||
		stats.PendingBytes != 0 {
		t.Fatalf("post-cancel stats = %+v", stats)
	}
}

func TestExtentReclaimerBuildsIndexOnlyUnderPressureAndKeepsDrainCheap(t *testing.T) {
	const count = retiredIntervalIndexThreshold + 1
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
	restored := make([]FreeExtent, count)
	for rank := range restored {
		restored[rank] = FreeExtent{
			Offset: uint64(rank+1) * 8192,
			Length: 4096, RetiredGeneration: 1,
		}
	}
	if err := reclaimer.Restore(restored); err != nil {
		t.Fatal(err)
	}
	if !reclaimer.indexed || reclaimer.intervals.len() != count {
		t.Fatal("large fenced set did not build its interval index")
	}
	high := reclaimer.intervals.high
	reused := make([]FreeExtent, 0, count)
	reused, err = reclaimer.AppendReusable(reused, 2, 2, count)
	if err != nil {
		t.Fatal(err)
	}
	if len(reused) != count || reclaimer.PendingCount() != 0 ||
		reclaimer.indexed || reclaimer.intervals.len() != 0 {
		t.Fatal("full drain did not release indexed ownership")
	}
	if reclaimer.intervals.high != high {
		t.Fatal("full drain rewrote the fixed interval capacity")
	}
}

func TestExtentReclaimerThresholdCrossingAndIndexedLifecycle(t *testing.T) {
	const held = retiredIntervalIndexThreshold
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: held + 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	restored := make([]FreeExtent, held)
	for rank := range restored {
		restored[rank] = FreeExtent{
			Offset: uint64(rank+1) * 8192,
			Length: 4096, RetiredGeneration: 1,
		}
	}
	if err := reclaimer.Restore(restored); err != nil {
		t.Fatal(err)
	}
	if reclaimer.indexed || reclaimer.intervals.len() != 0 {
		t.Fatal("threshold-sized set built an index early")
	}
	crossing := []FreeExtent{
		{
			Offset: uint64(held+2) * 8192,
			Length: 4096, RetiredGeneration: 2,
		},
		{
			Offset: uint64(held+2)*8192 + 1,
			Length: 1, RetiredGeneration: 2,
		},
	}
	if err := reclaimer.RetireBatch(crossing); err == nil {
		t.Fatal("overlapping threshold-crossing batch succeeded")
	}
	if reclaimer.PendingCount() != held || reclaimer.indexed ||
		reclaimer.intervals.len() != 0 {
		t.Fatal("failed threshold crossing changed the held set")
	}
	if err := reclaimer.Retire(crossing[0]); err != nil {
		t.Fatal(err)
	}
	if !reclaimer.indexed || reclaimer.intervals.len() != held+1 {
		t.Fatal("successful threshold crossing did not index the complete set")
	}

	reused := make([]FreeExtent, 0, held+4)
	reused, err = reclaimer.AppendReusable(reused, 3, 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(reused) != 100 || reclaimer.PendingCount() != held+1-100 ||
		!reclaimer.indexed ||
		reclaimer.intervals.len() != held+1-100 {
		t.Fatal("partial indexed drain changed index mode or count")
	}
	older := FreeExtent{
		Offset: uint64(held+3) * 8192,
		Length: 4096, RetiredGeneration: 1,
	}
	if err := reclaimer.Retire(older); err != nil {
		t.Fatal(err)
	}
	if !reclaimer.indexed ||
		reclaimer.intervals.len() != reclaimer.PendingCount() {
		t.Fatal("older returned generation broke the interval index")
	}
	tail := []FreeExtent{
		{Offset: uint64(held+4) * 8192, Length: 4096, RetiredGeneration: 3},
		{Offset: uint64(held+5) * 8192, Length: 4096, RetiredGeneration: 3},
		{Offset: uint64(held+6) * 8192, Length: 4096, RetiredGeneration: 3},
	}
	if err := reclaimer.RetireBatch(tail); err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.CancelRetiredGeneration(3); err != nil {
		t.Fatal(err)
	}
	if !reclaimer.indexed ||
		reclaimer.intervals.len() != reclaimer.PendingCount() {
		t.Fatal("compaction/cancel broke indexed ownership")
	}
	wantReused := len(reused) + reclaimer.PendingCount()
	reused, err = reclaimer.AppendReusable(
		reused, 4, 4, held+4-len(reused),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(reused) != wantReused ||
		reclaimer.PendingCount() != 0 || reclaimer.indexed ||
		reclaimer.intervals.len() != 0 {
		t.Fatal("final drain did not return to unindexed mode")
	}
}

func TestExtentReclaimerIndexedDrainErrorIsAtomicAndRepairsIndex(t *testing.T) {
	const count = retiredIntervalIndexThreshold + 1
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
	extents := make([]FreeExtent, count)
	for rank := range extents {
		extents[rank] = FreeExtent{
			Offset: uint64(rank+1) * 8192,
			Length: 4096, RetiredGeneration: 1,
		}
	}
	if err := reclaimer.Restore(extents); err != nil {
		t.Fatal(err)
	}
	target := extents[count/2]
	at := reclaimer.intervals.root
	for at != retiredIntervalNone &&
		reclaimer.intervals.nodes[at].offset != target.Offset {
		if target.Offset < reclaimer.intervals.nodes[at].offset {
			at = reclaimer.intervals.nodeLeft(at)
		} else {
			at = reclaimer.intervals.nodes[at].right
		}
	}
	if at == retiredIntervalNone {
		t.Fatal("target interval not found")
	}
	// Simulate a detected internal inconsistency. The external-arena contract
	// forbids caller mutation; this direct package-level edit exists only to
	// prove the error path does not partially drain authoritative ownership.
	reclaimer.intervals.nodes[at].length++
	dst := make([]FreeExtent, 0, count)
	dst, err = reclaimer.AppendReusable(dst, 2, 2, count)
	if err == nil {
		t.Fatal("inconsistent interval index drained successfully")
	}
	if len(dst) != 0 || reclaimer.PendingCount() != count ||
		reclaimer.intervals.len() != count ||
		!reclaimer.intervals.contains(target.Offset, target.Length) {
		t.Fatal("failed indexed drain changed ownership or did not repair index")
	}
	assertRetiredIntervalIndex(t, &reclaimer.intervals)
}

func TestExtentReclaimerRejectsPendingByteOverflow(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	reclaimer.pendingBytes = ^uint64(0) - 1024
	if err := reclaimer.Retire(FreeExtent{
		Offset: 4096, Length: 4096, RetiredGeneration: 1,
	}); err == nil {
		t.Fatal("pending byte overflow succeeded")
	}
	if reclaimer.PendingCount() != 0 || reclaimer.intervals.len() != 0 {
		t.Fatal("overflow failure changed retirement ownership")
	}
}

var extentReclaimerTestSink *ExtentReclaimer

func TestExtentReclaimerExternalStorageAvoidsArenaAllocations(t *testing.T) {
	const capacity = 512
	indexStorage := make([]byte, RetiredIntervalIndexStorageBytes(capacity))
	extentStorage := make([]byte, RetiredExtentStorageBytes(capacity))
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	external, err := NewExtentReclaimer(
		leases,
		ExtentReclaimerOptions{
			MaxRetiredExtents:    capacity,
			IntervalIndexStorage: indexStorage,
			RetiredExtentStorage: extentStorage,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if external.intervals.initialized ||
		uintptr(unsafe.Pointer(unsafe.SliceData(external.intervals.nodes))) !=
			uintptr(unsafe.Pointer(unsafe.SliceData(indexStorage))) ||
		uintptr(unsafe.Pointer(
			unsafe.SliceData(external.pending[:cap(external.pending)]),
		)) != uintptr(unsafe.Pointer(unsafe.SliceData(extentStorage))) {
		t.Fatal("external storage was copied, touched, or ignored")
	}
	standalone, err := NewExtentReclaimer(
		leases,
		ExtentReclaimerOptions{MaxRetiredExtents: capacity},
	)
	if err != nil {
		t.Fatal(err)
	}
	if cap(standalone.pending) != capacity ||
		len(standalone.intervals.nodes) != capacity ||
		RetiredExtentStorageBytes(capacity)+
			RetiredIntervalIndexStorageBytes(capacity) != 48*capacity {
		t.Fatal("standalone fallback does not own exact 48-byte extent metadata")
	}
	externalAllocs := testing.AllocsPerRun(100, func() {
		var constructErr error
		extentReclaimerTestSink, constructErr = NewExtentReclaimer(
			leases,
			ExtentReclaimerOptions{
				MaxRetiredExtents:    capacity,
				IntervalIndexStorage: indexStorage,
				RetiredExtentStorage: extentStorage,
			},
		)
		if constructErr != nil {
			panic(constructErr)
		}
	})
	ownedAllocs := testing.AllocsPerRun(100, func() {
		var constructErr error
		extentReclaimerTestSink, constructErr = NewExtentReclaimer(
			leases,
			ExtentReclaimerOptions{MaxRetiredExtents: capacity},
		)
		if constructErr != nil {
			panic(constructErr)
		}
	})
	if externalAllocs >= ownedAllocs {
		t.Fatalf("external/owned constructor allocations = %.1f/%.1f",
			externalAllocs, ownedAllocs)
	}
	if _, err := NewExtentReclaimer(
		leases,
		ExtentReclaimerOptions{
			MaxRetiredExtents:    capacity,
			IntervalIndexStorage: indexStorage[:len(indexStorage)-1],
		},
	); err == nil {
		t.Fatal("short external interval storage succeeded")
	}
	if _, err := NewExtentReclaimer(
		leases,
		ExtentReclaimerOptions{
			MaxRetiredExtents:    capacity,
			IntervalIndexStorage: indexStorage,
			RetiredExtentStorage: extentStorage[:len(extentStorage)-1],
		},
	); err == nil {
		t.Fatal("short external retired extent storage succeeded")
	}
	overlap := make([]byte, RetiredExtentStorageBytes(capacity))
	if _, err := NewExtentReclaimer(
		leases,
		ExtentReclaimerOptions{
			MaxRetiredExtents:    capacity,
			IntervalIndexStorage: overlap,
			RetiredExtentStorage: overlap,
		},
	); err == nil {
		t.Fatal("overlapping external metadata arenas succeeded")
	}
	misaligned := make(
		[]byte, RetiredIntervalIndexStorageBytes(capacity)+1,
	)
	if _, err := NewExtentReclaimer(
		leases,
		ExtentReclaimerOptions{
			MaxRetiredExtents:    capacity,
			IntervalIndexStorage: misaligned[1 : RetiredIntervalIndexStorageBytes(capacity)+1],
		},
	); err == nil {
		t.Fatal("misaligned external interval storage succeeded")
	}
}

func TestExtentReclaimerAppendReusableHonorsDestinationCapacity(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.Retire(FreeExtent{
		Offset: 4096, Length: 4096, RetiredGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		empty, appendErr := reclaimer.AppendReusable(nil, 2, 2, 1)
		if appendErr != nil {
			panic(appendErr)
		}
		if len(empty) != 0 {
			panic("zero-capacity destination changed")
		}
	}); allocations != 0 {
		t.Fatalf("zero-capacity AppendReusable allocations = %.2f, want 0",
			allocations)
	}
	if reclaimer.PendingCount() != 1 {
		t.Fatal("zero-capacity destination lost retirement ownership")
	}
	dst := make([]FreeExtent, 0, 1)
	dst, err = reclaimer.AppendReusable(dst, 2, 2, 2)
	if err != nil || len(dst) != 1 || reclaimer.PendingCount() != 0 {
		t.Fatalf("bounded destination reclaim = %+v/%v", dst, err)
	}
}

func assertRetiredIntervalIndex(
	t *testing.T, index *retiredIntervalIndex,
) {
	t.Helper()
	if index == nil || !index.initialized {
		t.Fatal("interval index is not initialized")
	}
	if index.count == 0 {
		if index.root != retiredIntervalNone {
			t.Fatal("empty interval index has a root")
		}
		return
	}
	if index.root == retiredIntervalNone {
		t.Fatal("non-empty interval index has no root")
	}
	type frame struct {
		rank     int32
		lower    uint64
		upper    uint64
		hasLower bool
		hasUpper bool
	}
	stack := []frame{{rank: index.root}}
	live := make([]bool, int(index.high))
	reachable := 0
	for len(stack) != 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current.rank < 0 || int(current.rank) >= int(index.high) ||
			live[current.rank] {
			t.Fatal("invalid or duplicate reachable interval node")
		}
		live[current.rank] = true
		reachable++
		node := &index.nodes[current.rank]
		if current.hasLower && node.offset <= current.lower ||
			current.hasUpper && node.offset >= current.upper {
			t.Fatal("interval tree violates strict offset order")
		}
		for _, child := range []struct {
			rank int32
			left bool
		}{
			{rank: index.nodeLeft(current.rank), left: true},
			{rank: node.right},
		} {
			if child.rank == retiredIntervalNone {
				continue
			}
			next := frame{rank: child.rank}
			if child.left {
				next.lower, next.hasLower = current.lower, current.hasLower
				next.upper, next.hasUpper = node.offset, true
			} else {
				next.lower, next.hasLower = node.offset, true
				next.upper, next.hasUpper = current.upper, current.hasUpper
			}
			stack = append(stack, next)
		}
		leftHeight := index.nodeHeight(index.nodeLeft(current.rank))
		rightHeight := index.nodeHeight(node.right)
		if balance := leftHeight - rightHeight; balance < -1 || balance > 1 {
			t.Fatal("interval tree violates AVL balance")
		}
		if got, want := index.nodeHeight(current.rank), max(leftHeight, rightHeight)+1; got != want {
			t.Fatalf("interval node height = %d, want %d", got, want)
		}
	}
	if reachable != index.count {
		t.Fatalf("reachable intervals = %d, want %d", reachable, index.count)
	}
	free := 0
	for at := index.free; at != retiredIntervalNone; at = index.nodeLeft(at) {
		if at < 0 || int(at) >= int(index.high) || live[at] {
			t.Fatal("interval free list overlaps live ownership")
		}
		live[at] = true
		free++
		if free > int(index.high) {
			t.Fatal("interval free list cycle")
		}
	}
	if reachable+free != int(index.high) {
		t.Fatalf("live/free/high = %d/%d/%d",
			reachable, free, index.high)
	}
}
