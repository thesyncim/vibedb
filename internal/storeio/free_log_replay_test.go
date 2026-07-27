package storeio

import (
	"errors"
	"os"
	"testing"
)

// freeLogTestWriter writes image and delta pages straight to a file and hands
// back a cache over it, so a replay is exercised against bytes on disk rather
// than against whatever a transaction happened to leave in a staging buffer.
type freeLogTestWriter struct {
	t     *testing.T
	file  *os.File
	cache *PageCache
	next  uint64
	logic uint64
}

func newFreeLogTestWriter(t *testing.T) *freeLogTestWriter {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "free-log-replay-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := file.Truncate(int64(freeLogTestFileEnd)); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: int(testSuperblockPageSize), ResidentBytes: 128 * int64(testSuperblockPageSize),
		StoreID: testStoreID, ReadConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return &freeLogTestWriter{t: t, file: file, cache: cache, next: 4, logic: 2}
}

const (
	freeLogTestPages   = 4096
	freeLogTestFileEnd = uint64(freeLogTestPages) * uint64(testSuperblockPageSize)
)

func (w *freeLogTestWriter) bounds() FreeLogBounds {
	return FreeLogBounds{FileEnd: freeLogTestFileEnd, NextLogicalID: w.logic + 1}
}

func (w *freeLogTestWriter) write(kind PageKind, encode func([]byte, FreeLogHeader) error) PageRef {
	w.t.Helper()
	page := make([]byte, testSuperblockPageSize)
	header := FreeLogHeader{
		StoreID: testStoreID, Generation: 3, LogicalID: w.logic, PageSize: testSuperblockPageSize,
	}
	if err := encode(page, header); err != nil {
		w.t.Fatal(err)
	}
	offset := w.next * uint64(testSuperblockPageSize)
	if _, err := w.file.WriteAt(page, int64(offset)); err != nil {
		w.t.Fatal(err)
	}
	ref := PageRef{
		Offset: offset, LogicalID: w.logic, Generation: 3,
		Length: testSuperblockPageSize, Kind: kind,
	}
	w.next++
	w.logic++
	return ref
}

func (w *freeLogTestWriter) image(extents []FreeExtent) PageRef {
	return w.write(PageFreeImage, func(page []byte, header FreeLogHeader) error {
		_, err := EncodeFreeImagePage(
			page, header, extents, freeLogTestFileEnd, w.logic+1)
		return err
	})
}

// index writes one segment-index page naming the segments already written, in
// ascending first offset, and returns its reference.
func (w *freeLogTestWriter) index(segments []FreeSegment, prev PageRef) PageRef {
	return w.write(PageFreeIndex, func(page []byte, header FreeLogHeader) error {
		_, err := EncodeFreeIndexPage(
			page, header, segments, prev, freeLogTestFileEnd, w.logic+1)
		return err
	})
}

// segmentOf describes a just-written segment page for the index.
func segmentOf(ref PageRef, extents []FreeExtent) FreeSegment {
	largest := uint64(0)
	for _, extent := range extents {
		largest = max(largest, extent.Length)
	}
	return FreeSegment{
		Ref: ref, FirstOffset: extents[0].Offset,
		LargestFree: largest, Count: uint32(len(extents)),
	}
}

func (w *freeLogTestWriter) delta(deltas []FreeDelta, prev, indexHead PageRef) PageRef {
	return w.write(PageFreeDelta, func(page []byte, header FreeLogHeader) error {
		_, err := EncodeFreeDeltaPage(
			page, header, deltas, prev, indexHead, freeLogTestFileEnd, w.logic+1)
		return err
	})
}

func freeLogTestExtent(page, pages, generation uint64) FreeExtent {
	return FreeExtent{
		Offset:            page * uint64(testSuperblockPageSize),
		Length:            pages * uint64(testSuperblockPageSize),
		RetiredGeneration: generation,
	}
}

// Given a multi-page image and a chain of deltas that set, replace, and delete
// extents across it, when the chain is replayed from its newest page, then the
// result is the offset-ordered set the records describe, with the newest record
// for each offset winning.
func TestReplayFreeLogAppliesChainOldestToNewest(t *testing.T) {
	w := newFreeLogTestWriter(t)
	// Two segments, named from one index page. Segments carry no reference to
	// each other: the index is what orders them, which is what lets a fold
	// rewrite one and leave the other's bytes alone.
	lower := []FreeExtent{
		freeLogTestExtent(20, 1, 1),
		freeLogTestExtent(40, 2, 1),
		freeLogTestExtent(60, 1, 1),
	}
	upper := []FreeExtent{
		freeLogTestExtent(200, 1, 1),
		freeLogTestExtent(300, 4, 1),
	}
	first := w.image(lower)
	second := w.image(upper)
	index := w.index([]FreeSegment{segmentOf(first, lower), segmentOf(second, upper)}, PageRef{})

	// Oldest delta: drop one image extent, shrink another, add a new one.
	oldest := w.delta([]FreeDelta{
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 20 * uint64(testSuperblockPageSize)}},
		{Op: FreeOpSet, Extent: freeLogTestExtent(40, 1, 2)},
		{Op: FreeOpSet, Extent: freeLogTestExtent(500, 3, 2)},
	}, PageRef{}, index)
	// Middle delta: supersede the new extent within the same chain.
	middle := w.delta([]FreeDelta{
		{Op: FreeOpSet, Extent: freeLogTestExtent(500, 1, 4)},
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 300 * uint64(testSuperblockPageSize)}},
	}, oldest, index)
	// Newest delta: two records for one offset, the later of which must win.
	newest := w.delta([]FreeDelta{
		{Op: FreeOpSet, Extent: freeLogTestExtent(60, 8, 5)},
		{Op: FreeOpSet, Extent: freeLogTestExtent(60, 2, 6)},
	}, middle, index)

	got, pages, err := ReplayFreeLog(w.cache, newest, w.bounds(), nil, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	want := []FreeExtent{
		freeLogTestExtent(40, 1, 2),
		freeLogTestExtent(60, 2, 6),
		freeLogTestExtent(200, 1, 1),
		freeLogTestExtent(500, 1, 4),
	}
	if len(got) != len(want) {
		t.Fatalf("replayed %d extents %+v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("extent %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if len(pages.Index) != 1 || pages.Index[0] != index {
		t.Fatalf("index pages = %+v", pages.Index)
	}
	if len(pages.Segments) != 2 || pages.Segments[0].Ref != first || pages.Segments[1].Ref != second {
		t.Fatalf("segments = %+v", pages.Segments)
	}
	if len(pages.Delta) != 3 || pages.Delta[0] != oldest || pages.Delta[2] != newest {
		t.Fatalf("delta pages = %+v", pages.Delta)
	}
}

// Given a chain whose pages disagree about which image they are relative to,
// when it is replayed, then the replay fails instead of applying one chain's
// diff to another chain's image, which would advertise live space as free.
func TestReplayFreeLogRejectsSplicedChainAndOverlap(t *testing.T) {
	w := newFreeLogTestWriter(t)
	otherExtents := []FreeExtent{freeLogTestExtent(700, 1, 1)}
	baseExtents := []FreeExtent{freeLogTestExtent(20, 1, 1)}
	other := w.index([]FreeSegment{segmentOf(w.image(otherExtents), otherExtents)}, PageRef{})
	base := w.index([]FreeSegment{segmentOf(w.image(baseExtents), baseExtents)}, PageRef{})
	oldest := w.delta([]FreeDelta{{Op: FreeOpSet, Extent: freeLogTestExtent(40, 1, 2)}}, PageRef{}, other)
	newest := w.delta([]FreeDelta{{Op: FreeOpSet, Extent: freeLogTestExtent(60, 1, 2)}}, oldest, base)
	if _, _, err := ReplayFreeLog(w.cache, newest, w.bounds(), nil, 64, 64); !errors.Is(err, ErrFreeLogCorrupt) {
		t.Fatalf("spliced chain = %v, want %v", err, ErrFreeLogCorrupt)
	}

	// A record that overlaps the extent below it must fail closed for the same
	// reason: under-reporting free space leaks, over-reporting corrupts.
	overlapping := newFreeLogTestWriter(t)
	overlapExtents := []FreeExtent{freeLogTestExtent(20, 4, 1)}
	index := overlapping.index(
		[]FreeSegment{segmentOf(overlapping.image(overlapExtents), overlapExtents)}, PageRef{})
	head := overlapping.delta(
		[]FreeDelta{{Op: FreeOpSet, Extent: freeLogTestExtent(22, 4, 2)}}, PageRef{}, index)
	if _, _, err := ReplayFreeLog(overlapping.cache, head, overlapping.bounds(), nil, 64, 64); !errors.Is(err, ErrFreeLogCorrupt) {
		t.Fatalf("overlapping replay = %v, want %v", err, ErrFreeLogCorrupt)
	}
}

// Given a replay whose result would exceed the caller's fixed arena, when it is
// run, then it reports capacity rather than growing the destination, because
// the destination is off-heap and a grown copy would silently move the free set
// onto the Go heap.
func TestReplayFreeLogRespectsCallerCapacity(t *testing.T) {
	w := newFreeLogTestWriter(t)
	extents := make([]FreeExtent, 0, 8)
	for i := range 8 {
		extents = append(extents, freeLogTestExtent(uint64(20+2*i), 1, 1))
	}
	index := w.index([]FreeSegment{segmentOf(w.image(extents), extents)}, PageRef{})
	head := w.delta([]FreeDelta{{Op: FreeOpSet, Extent: freeLogTestExtent(600, 1, 2)}}, PageRef{}, index)
	if _, _, err := ReplayFreeLog(w.cache, head, w.bounds(), nil, 4, 64); !errors.Is(err, ErrRetiredExtentCapacity) {
		t.Fatalf("bounded replay = %v, want %v", err, ErrRetiredExtentCapacity)
	}
	got, _, err := ReplayFreeLog(w.cache, head, w.bounds(), nil, 9, 64)
	if err != nil || len(got) != 9 {
		t.Fatalf("replay = (%d,%v), want 9 extents", len(got), err)
	}
}

// Given a superblock whose free reference names a page that is not a delta,
// when recovery selects a root, then that candidate is rejected: the free
// reference changed shape with the format, and a free B+tree node would
// otherwise pass every structural check the superblock performs.
func TestRecoverStateRootValidatesFreeLogHead(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-log-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := uint64(testSuperblockPageSize)
	fileEnd := 7 * pageSize
	state := StateRoot{
		StoreID: testStoreID, Generation: 1, PageSize: testSuperblockPageSize,
		MaxPageSize: 64 << 10, NextLogicalID: 3, ChunkDocuments: 64,
	}
	statePage := make([]byte, testSuperblockPageSize)
	if _, err := EncodeStateRootPage(statePage, state, fileEnd); err != nil {
		t.Fatal(err)
	}
	freePage := make([]byte, testSuperblockPageSize)
	header := FreeLogHeader{
		StoreID: testStoreID, Generation: 1, LogicalID: 2, PageSize: testSuperblockPageSize,
	}
	if _, err := EncodeFreeDeltaPage(freePage, header,
		[]FreeDelta{{Op: FreeOpSet, Extent: FreeExtent{
			Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 1,
		}}}, PageRef{}, PageRef{}, fileEnd, state.NextLogicalID); err != nil {
		t.Fatal(err)
	}
	root := testSuperblock(1, 4*pageSize, statePage)
	root.FileEnd = fileEnd
	root.FreeOffset = 5 * pageSize
	root.FreeLength = testSuperblockPageSize
	root.FreeChecksum = PageChecksum(freePage)
	encodedRoot := encodeTestSuperblock(t, root)
	if err := file.Truncate(int64(fileEnd)); err != nil {
		t.Fatal(err)
	}
	writeAtTest(t, file, encodedRoot[:], 0)
	writeAtTest(t, file, statePage, int64(root.StateOffset))
	writeAtTest(t, file, freePage, int64(root.FreeOffset))
	scratch := make([]byte, testSuperblockPageSize)
	gotRoot, gotState, slot, err := RecoverStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || gotRoot != root || gotState != state || slot != 0 {
		t.Fatalf("RecoverStateRoot = (%+v,%+v,%d,%v)", gotRoot, gotState, slot, err)
	}

	// Corrupt the operation byte of the single record: the checksum still has to
	// be repaired for the page to reach the schema check at all.
	freePage[PageHeaderSize+FreeDeltaPayloadHeaderSize] = 0
	resealTestPage(freePage)
	root.FreeChecksum = PageChecksum(freePage)
	encodedRoot = encodeTestSuperblock(t, root)
	writeAtTest(t, file, freePage, int64(root.FreeOffset))
	writeAtTest(t, file, encodedRoot[:], 0)
	if _, _, _, err := RecoverStateRoot(file, testSuperblockPageSize, scratch); !errors.Is(err, ErrSuperblockNotFound) {
		t.Fatalf("semantic free corruption = %v, want %v", err, ErrSuperblockNotFound)
	}
}
