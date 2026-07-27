package storeio

import (
	"errors"
	"testing"
)

func testFreeLogHeader(logicalID uint64) FreeLogHeader {
	return FreeLogHeader{
		StoreID: testStoreID, Generation: 11, LogicalID: logicalID,
		PageSize: testSuperblockPageSize,
	}
}

func testFreeIndexRef(logicalID, page, generation uint64) PageRef {
	return PageRef{
		Offset: page * uint64(testSuperblockPageSize), LogicalID: logicalID, Generation: generation,
		Length: testSuperblockPageSize, Kind: PageFreeIndex,
	}
}

func testFreeImageRef(logicalID, page, generation uint64) PageRef {
	return PageRef{
		Offset: page * uint64(testSuperblockPageSize), LogicalID: logicalID, Generation: generation,
		Length: testSuperblockPageSize, Kind: PageFreeImage,
	}
}

func testFreeDeltaRef(logicalID, page, generation uint64) PageRef {
	return PageRef{
		Offset: page * uint64(testSuperblockPageSize), LogicalID: logicalID, Generation: generation,
		Length: testSuperblockPageSize, Kind: PageFreeDelta,
	}
}

// Given one image segment, when it is encoded and reopened, then every extent
// survives exactly. A segment names nothing: the index orders segments, which
// is what lets a fold rewrite one and leave its neighbours' bytes alone.
func TestFreeImagePageRoundTrip(t *testing.T) {
	header := testFreeLogHeader(40)
	pageSize := uint64(testSuperblockPageSize)
	extents := []FreeExtent{
		{Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 7},
		{Offset: 5 * pageSize, Length: 2 * pageSize, RetiredGeneration: 9},
		{Offset: 10 * pageSize, Length: pageSize, RetiredGeneration: 10},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeFreeImagePage(
		page, header, extents, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenFreeImagePage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(extents) {
		t.Fatalf("image = (%+v,%d)", view.Header(), view.Len())
	}
	for rank, want := range extents {
		got, ok := view.ExtentAt(rank)
		if !ok || got != want {
			t.Fatalf("ExtentAt(%d) = (%+v,%v), want %+v", rank, got, ok, want)
		}
	}
	if _, ok := view.ExtentAt(len(extents)); ok {
		t.Fatal("ExtentAt past the end reported a record")
	}
}

// Given a commit's diff, when it is encoded and reopened, then both operations,
// the predecessor, and the index reference survive exactly.
func TestFreeDeltaPageRoundTrip(t *testing.T) {
	header := testFreeLogHeader(50)
	pageSize := uint64(testSuperblockPageSize)
	prev := testFreeDeltaRef(49, 30, 10)
	indexHead := testFreeIndexRef(40, 20, 9)
	deltas := []FreeDelta{
		{Op: FreeOpSet, Extent: FreeExtent{Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 11}},
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 9 * pageSize}},
		// A later record for an offset an earlier one already named is legal:
		// a commit's diff is applied in order, and coalescing legitimately
		// deletes an extent and re-sets a longer one at the same offset.
		{Op: FreeOpSet, Extent: FreeExtent{Offset: 9 * pageSize, Length: 3 * pageSize, RetiredGeneration: 12}},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeFreeDeltaPage(
		page, header, deltas, prev, indexHead, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenFreeDeltaPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(deltas) ||
		view.Prev() != prev || view.IndexHead() != indexHead {
		t.Fatalf("delta = (%+v,%d,%+v,%+v)", view.Header(), view.Len(), view.Prev(), view.IndexHead())
	}
	for rank, want := range deltas {
		got, ok := view.DeltaAt(rank)
		if !ok || got != want {
			t.Fatalf("DeltaAt(%d) = (%+v,%v), want %+v", rank, got, ok, want)
		}
	}
}

// Given the chain's first delta after a fold and the index's first page, when
// each is encoded with a zero link, then the zero link round-trips as the
// terminator rather than being rejected as a malformed reference.
func TestFreeLogZeroLinksTerminateTheChain(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	page := make([]byte, testSuperblockPageSize)

	segment := make([]byte, testSuperblockPageSize)
	segmentExtents := []FreeExtent{{Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 7}}
	if _, err := EncodeFreeImagePage(segment, testFreeLogHeader(40), segmentExtents,
		testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeFreeIndexPage(page, testFreeLogHeader(41), []FreeSegment{{
		Ref: testFreeImageRef(40, 20, 11), FirstOffset: 4 * pageSize,
		LargestFree: pageSize, Count: 1,
	}}, PageRef{}, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	index, err := OpenFreeIndexPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil || index.Prev() != (PageRef{}) {
		t.Fatalf("first index page = (%+v,%v)", index.Prev(), err)
	}

	encoded, err = EncodeFreeDeltaPage(page, testFreeLogHeader(50),
		[]FreeDelta{{Op: FreeOpDelete, Extent: FreeExtent{Offset: 4 * pageSize}}},
		PageRef{}, PageRef{}, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := OpenFreeDeltaPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil || delta.Prev() != (PageRef{}) || delta.IndexHead() != (PageRef{}) {
		t.Fatalf("first delta = (%+v,%+v,%v)", delta.Prev(), delta.IndexHead(), err)
	}
}

// Given a page filled to its computed capacity, when one more record is added,
// then the encoder rejects it — so the commit path can size a chain from
// FreeDeltaRecordCapacity and trust the arithmetic.
func TestFreeLogRecordCapacityIsExact(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	page := make([]byte, testSuperblockPageSize)

	imageCap := FreeImageRecordCapacity(testSuperblockPageSize)
	if imageCap <= 0 {
		t.Fatalf("image capacity = %d", imageCap)
	}
	// A full page of extents needs a file long enough to contain them; the
	// shared fixture end is only a few dozen pages.
	fileEnd := uint64(imageCap+FreeDeltaRecordCapacity(testSuperblockPageSize)+8) * pageSize
	full := make([]FreeExtent, imageCap)
	for i := range full {
		full[i] = FreeExtent{
			Offset: uint64(i+4) * pageSize, Length: pageSize, RetiredGeneration: 7,
		}
	}
	if _, err := EncodeFreeImagePage(page, testFreeLogHeader(40), full,
		fileEnd, testKeyDirectoryNextLogicalID); err != nil {
		t.Fatalf("exactly %d image extents rejected: %v", imageCap, err)
	}
	over := append(full, FreeExtent{
		Offset: uint64(imageCap+4) * pageSize, Length: pageSize, RetiredGeneration: 7,
	})
	if _, err := EncodeFreeImagePage(page, testFreeLogHeader(40), over,
		fileEnd, testKeyDirectoryNextLogicalID); err == nil {
		t.Fatalf("%d image extents accepted past a capacity of %d", len(over), imageCap)
	}

	deltaCap := FreeDeltaRecordCapacity(testSuperblockPageSize)
	if deltaCap <= 0 {
		t.Fatalf("delta capacity = %d", deltaCap)
	}
	records := make([]FreeDelta, deltaCap)
	for i := range records {
		records[i] = FreeDelta{Op: FreeOpDelete, Extent: FreeExtent{Offset: uint64(i+4) * pageSize}}
	}
	if _, err := EncodeFreeDeltaPage(page, testFreeLogHeader(50), records, PageRef{}, PageRef{},
		fileEnd, testKeyDirectoryNextLogicalID); err != nil {
		t.Fatalf("exactly %d delta records rejected: %v", deltaCap, err)
	}
	records = append(records, FreeDelta{
		Op: FreeOpDelete, Extent: FreeExtent{Offset: uint64(deltaCap+4) * pageSize},
	})
	if _, err := EncodeFreeDeltaPage(page, testFreeLogHeader(50), records, PageRef{}, PageRef{},
		fileEnd, testKeyDirectoryNextLogicalID); err == nil {
		t.Fatalf("%d delta records accepted past a capacity of %d", len(records), deltaCap)
	}
}

// Given malformed input, when a page is encoded, then it is rejected rather
// than producing a page a later replay would misread.
func TestFreeLogEncodeRejectsMalformedInput(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	page := make([]byte, testSuperblockPageSize)
	good := FreeExtent{Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 7}

	imageCases := []struct {
		name    string
		extents []FreeExtent
	}{
		{"descending offsets", []FreeExtent{
			{Offset: 8 * pageSize, Length: pageSize, RetiredGeneration: 7}, good,
		}},
		{"overlapping extents", []FreeExtent{
			{Offset: 4 * pageSize, Length: 2 * pageSize, RetiredGeneration: 7},
			{Offset: 5 * pageSize, Length: pageSize, RetiredGeneration: 7},
		}},
		{"zero length", []FreeExtent{{Offset: 4 * pageSize, RetiredGeneration: 7}}},
		{"unaligned offset", []FreeExtent{
			{Offset: 4*pageSize + 1, Length: pageSize, RetiredGeneration: 7},
		}},
		{"zero retired generation", []FreeExtent{{Offset: 4 * pageSize, Length: pageSize}}},
	}
	for _, c := range imageCases {
		t.Run("image/"+c.name, func(t *testing.T) {
			if _, err := EncodeFreeImagePage(page, testFreeLogHeader(40), c.extents,
				testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID); err == nil {
				t.Fatal("accepted")
			}
		})
	}

	deltaCases := []struct {
		name      string
		deltas    []FreeDelta
		prev      PageRef
		indexHead PageRef
	}{
		{"unknown operation", []FreeDelta{{Op: FreeOp(9), Extent: good}}, PageRef{}, PageRef{}},
		{"zero operation", []FreeDelta{{Extent: good}}, PageRef{}, PageRef{}},
		{"delete carrying a length", []FreeDelta{
			{Op: FreeOpDelete, Extent: FreeExtent{Offset: 4 * pageSize, Length: pageSize}},
		}, PageRef{}, PageRef{}},
		{"delete carrying a generation", []FreeDelta{
			{Op: FreeOpDelete, Extent: FreeExtent{Offset: 4 * pageSize, RetiredGeneration: 7}},
		}, PageRef{}, PageRef{}},
		{"set with zero length", []FreeDelta{
			{Op: FreeOpSet, Extent: FreeExtent{Offset: 4 * pageSize, RetiredGeneration: 7}},
		}, PageRef{}, PageRef{}},
		{"predecessor of the wrong kind", []FreeDelta{{Op: FreeOpSet, Extent: good}},
			testFreeImageRef(49, 30, 10), PageRef{}},
		{"index reference of the wrong kind", []FreeDelta{{Op: FreeOpSet, Extent: good}},
			PageRef{}, testFreeImageRef(40, 20, 9)},
	}
	for _, c := range deltaCases {
		t.Run("delta/"+c.name, func(t *testing.T) {
			if _, err := EncodeFreeDeltaPage(page, testFreeLogHeader(50), c.deltas, c.prev, c.indexHead,
				testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// Given a corrupted encoded page, when it is opened, then the corruption is
// reported rather than silently yielding a plausible free set. Handing back a
// wrong free set is the one failure that loses data: it either hides space
// forever or hands out a range that is still live.
func TestFreeLogOpenRejectsCorruption(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeFreeDeltaPage(page, testFreeLogHeader(50),
		[]FreeDelta{{Op: FreeOpSet, Extent: FreeExtent{
			Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 7,
		}}},
		testFreeDeltaRef(49, 30, 10), testFreeIndexRef(40, 20, 9),
		testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}

	corruptions := []struct {
		name  string
		apply func([]byte)
	}{
		{"version", func(b []byte) { b[PageHeaderSize] ^= 0xFF }},
		{"reserved payload byte", func(b []byte) { b[PageHeaderSize+5] = 1 }},
		{"record count", func(b []byte) { b[PageHeaderSize+6] = 0xFF }},
		{"operation byte", func(b []byte) { b[PageHeaderSize+FreeDeltaPayloadHeaderSize] = 0 }},
		{"record reserved bytes", func(b []byte) { b[PageHeaderSize+FreeDeltaPayloadHeaderSize+1] = 1 }},
		{"extent offset", func(b []byte) { b[PageHeaderSize+FreeDeltaPayloadHeaderSize+8] ^= 0x01 }},
	}
	for _, c := range corruptions {
		t.Run(c.name, func(t *testing.T) {
			damaged := make([]byte, len(encoded))
			copy(damaged, encoded)
			c.apply(damaged)
			if _, err := OpenFreeDeltaPage(
				damaged, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID,
			); err == nil {
				t.Fatal("corrupted delta page accepted")
			}
		})
	}
}

// Given a delta naming itself as its predecessor, when it is opened, then it is
// rejected: the replay walks predecessor links, and a self-loop would never
// terminate.
func TestFreeLogRejectsSelfReferencingDelta(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	header := testFreeLogHeader(50)
	self := PageRef{
		Offset: 30 * uint64(testSuperblockPageSize), LogicalID: header.LogicalID,
		Generation: header.Generation, Length: testSuperblockPageSize, Kind: PageFreeDelta,
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeFreeDeltaPage(page, header,
		[]FreeDelta{{Op: FreeOpDelete, Extent: FreeExtent{Offset: 4 * pageSize}}},
		self, PageRef{}, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFreeDeltaPage(
		encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID,
	); !errors.Is(err, ErrFreeLogCorrupt) {
		t.Fatalf("self-referencing delta accepted: %v", err)
	}
}

// Given an image page, when it is opened as a delta and vice versa, then the
// page kind is enforced, so a reference of the wrong kind cannot be followed
// into a misparse.
func TestFreeLogEnforcesPageKind(t *testing.T) {
	pageSize := uint64(testSuperblockPageSize)
	page := make([]byte, testSuperblockPageSize)
	image, err := EncodeFreeImagePage(page, testFreeLogHeader(40),
		[]FreeExtent{{Offset: 4 * pageSize, Length: pageSize, RetiredGeneration: 7}},
		testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFreeDeltaPage(
		image, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID,
	); err == nil {
		t.Fatal("image page opened as a delta")
	}

	other := make([]byte, testSuperblockPageSize)
	delta, err := EncodeFreeDeltaPage(other, testFreeLogHeader(50),
		[]FreeDelta{{Op: FreeOpDelete, Extent: FreeExtent{Offset: 4 * pageSize}}},
		PageRef{}, PageRef{}, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFreeImagePage(
		delta, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID,
	); err == nil {
		t.Fatal("delta page opened as an image")
	}
}
