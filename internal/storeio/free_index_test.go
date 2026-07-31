package storeio

import (
	"errors"
	"testing"
)

const (
	testFreeIndexNextLogicalID = uint64(100)
	testFreeIndexFileEnd       = uint64(32 * testSuperblockPageSize)
)

func testFreeSegment(logicalID, page, first, largest uint64, count uint32) FreeSegment {
	return FreeSegment{
		Ref:         testFreeImageRef(logicalID, page, 9),
		FirstOffset: first * uint64(testSuperblockPageSize),
		LargestFree: largest * uint64(testSuperblockPageSize),
		Count:       count,
	}
}

// Given a page of the segment index, when it is encoded and reopened, then
// every descriptor and the predecessor link survive exactly.
func TestFreeIndexPageRoundTrip(t *testing.T) {
	header := testFreeLogHeader(60)
	segments := []FreeSegment{
		testFreeSegment(40, 20, 4, 2, 3),
		testFreeSegment(41, 21, 9, 1, 7),
		testFreeSegment(42, 22, 15, 4, uint32(FreeImageRecordCapacity(testSuperblockPageSize))),
	}
	prev := testFreeIndexRef(59, 19, 8)
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeFreeIndexPage(
		page, header, segments, prev, testFreeIndexFileEnd, testFreeIndexNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenFreeIndexPage(encoded, testFreeIndexFileEnd, testFreeIndexNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(segments) || view.Prev() != prev {
		t.Fatalf("index = (%+v,%d,%+v)", view.Header(), view.Len(), view.Prev())
	}
	for rank, want := range segments {
		got, ok := view.SegmentAt(rank)
		if !ok || got != want {
			t.Fatalf("SegmentAt(%d) = (%+v,%v), want %+v", rank, got, ok, want)
		}
	}
	if _, ok := view.SegmentAt(len(segments)); ok {
		t.Fatal("SegmentAt past the end reported a record")
	}
}

// Given an index page filled to its computed capacity, when one more segment is
// added, then the encoder rejects it — so a fold can size the index from
// FreeIndexRecordCapacity and trust the arithmetic before it allocates.
func TestFreeIndexRecordCapacityIsExact(t *testing.T) {
	capacity := FreeIndexRecordCapacity(testSuperblockPageSize)
	if capacity <= 0 {
		t.Fatalf("index capacity = %d", capacity)
	}
	pageSize := uint64(testSuperblockPageSize)
	fileEnd := uint64(capacity+8) * 4 * pageSize
	full := make([]FreeSegment, capacity)
	for i := range full {
		full[i] = FreeSegment{
			Ref: PageRef{
				Offset: uint64(i+4) * pageSize, LogicalID: uint64(i + 2), Generation: 9,
				Length: testSuperblockPageSize, Kind: PageFreeImage,
			},
			FirstOffset: uint64(i+4) * pageSize, LargestFree: pageSize, Count: 1,
		}
	}
	page := make([]byte, testSuperblockPageSize)
	nextLogicalID := uint64(capacity + 8)
	if _, err := EncodeFreeIndexPage(
		page, testFreeLogHeader(2), full, PageRef{}, fileEnd, nextLogicalID); err != nil {
		t.Fatalf("exactly %d segments rejected: %v", capacity, err)
	}
	over := append(full, FreeSegment{
		Ref: PageRef{
			Offset: uint64(capacity+4) * pageSize, LogicalID: uint64(capacity + 2), Generation: 9,
			Length: testSuperblockPageSize, Kind: PageFreeImage,
		},
		FirstOffset: uint64(capacity+4) * pageSize, LargestFree: pageSize, Count: 1,
	})
	if _, err := EncodeFreeIndexPage(
		page, testFreeLogHeader(2), over, PageRef{}, fileEnd, nextLogicalID); err == nil {
		t.Fatalf("%d segments accepted past a capacity of %d", len(over), capacity)
	}
}

// Given malformed segment descriptors, when an index page is encoded, then it is
// rejected rather than producing an index a later replay would misread.
//
// The offset partition is the thing being protected. Segment i owns
// [FirstOffset(i), FirstOffset(i+1)), so a repeated or descending first offset
// gives one offset two owners, and a fold routing a changed extent to the wrong
// owner publishes a segment that still advertises it — free space overlapping a
// live page, which is the one failure that cannot be recovered from.
func TestFreeIndexEncodeRejectsMalformedSegments(t *testing.T) {
	page := make([]byte, testSuperblockPageSize)
	cases := []struct {
		name     string
		segments []FreeSegment
		prev     PageRef
	}{
		{"descending first offsets", []FreeSegment{
			testFreeSegment(41, 21, 9, 1, 7), testFreeSegment(40, 20, 4, 2, 3),
		}, PageRef{}},
		{"repeated first offset", []FreeSegment{
			testFreeSegment(40, 20, 9, 1, 3), testFreeSegment(41, 21, 9, 1, 7),
		}, PageRef{}},
		{"empty segment", []FreeSegment{testFreeSegment(40, 20, 4, 2, 0)}, PageRef{}},
		{"segment page of the wrong kind", []FreeSegment{{
			Ref:         testFreeDeltaRef(40, 20, 9),
			FirstOffset: 4 * uint64(testSuperblockPageSize),
			LargestFree: uint64(testSuperblockPageSize), Count: 1,
		}}, PageRef{}},
		{"count past what the segment page holds", []FreeSegment{
			testFreeSegment(40, 20, 4, 2,
				uint32(FreeImageRecordCapacity(testSuperblockPageSize))+1),
		}, PageRef{}},
		{"zero largest free extent", []FreeSegment{testFreeSegment(40, 20, 4, 0, 3)}, PageRef{}},
		{"predecessor of the wrong kind", []FreeSegment{testFreeSegment(40, 20, 4, 2, 3)},
			testFreeImageRef(59, 19, 8)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := EncodeFreeIndexPage(page, testFreeLogHeader(60), c.segments, c.prev,
				testFreeIndexFileEnd, testFreeIndexNextLogicalID); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// Given an index page naming itself as its predecessor, when it is opened, then
// it is rejected: the replay walks predecessor links, and a self-loop would
// never terminate.
func TestFreeIndexRejectsSelfReferencingPage(t *testing.T) {
	header := testFreeLogHeader(60)
	self := PageRef{
		Offset: 30 * uint64(testSuperblockPageSize), LogicalID: header.LogicalID,
		Generation: header.Generation, Length: testSuperblockPageSize, Kind: PageFreeIndex,
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeFreeIndexPage(page, header,
		[]FreeSegment{testFreeSegment(40, 20, 4, 2, 3)}, self,
		testFreeIndexFileEnd, testFreeIndexNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFreeIndexPage(
		encoded, testFreeIndexFileEnd, testFreeIndexNextLogicalID,
	); !errors.Is(err, ErrFreeLogCorrupt) {
		t.Fatalf("self-referencing index page accepted: %v", err)
	}
}

// Given an index whose two pages disagree about ordering across their join, when
// it is replayed, then the replay fails. Page-local order is checked by the
// opener; this is the check that the concatenation is still a partition, which
// a per-page check cannot see.
func TestReplayFreeLogRejectsIndexOrderAcrossPages(t *testing.T) {
	w := newFreeLogTestWriter(t)
	high := []FreeExtent{freeLogTestExtent(300, 1, 1)}
	low := []FreeExtent{freeLogTestExtent(20, 1, 1)}
	// The chain runs low-to-high with the head at the high end, so writing the
	// high segment into the first page and the low one into the second inverts
	// exactly the join the per-page check cannot see.
	first := w.index([]FreeSegment{segmentOf(w.image(high), high)}, PageRef{})
	head := w.index([]FreeSegment{segmentOf(w.image(low), low)}, first)
	delta := w.delta([]FreeDelta{{Op: FreeOpSet, Extent: freeLogTestExtent(600, 1, 2)}}, PageRef{}, head)
	if _, _, err := replayExternalFreeLog(w.cache, delta, w.bounds(), nil, 64, 64); !errors.Is(err, ErrFreeLogCorrupt) {
		t.Fatalf("inverted index join = %v, want %v", err, ErrFreeLogCorrupt)
	}
}

// Given an index whose descriptor disagrees with the segment page it names, when
// it is replayed, then the replay fails rather than merging two states.
//
// The two are written in the same commit, so disagreement means the index and
// the image came from different publications — a grafted or torn fold. Trusting
// either one would produce a free set that describes neither.
func TestReplayFreeLogRejectsSegmentDisagreeingWithIndex(t *testing.T) {
	w := newFreeLogTestWriter(t)
	extents := []FreeExtent{freeLogTestExtent(20, 1, 1), freeLogTestExtent(40, 1, 1)}
	segment := segmentOf(w.image(extents), extents)
	segment.Count = 1
	head := w.index([]FreeSegment{segment}, PageRef{})
	delta := w.delta([]FreeDelta{{Op: FreeOpSet, Extent: freeLogTestExtent(600, 1, 2)}}, PageRef{}, head)
	if _, _, err := replayExternalFreeLog(w.cache, delta, w.bounds(), nil, 64, 64); !errors.Is(err, ErrFreeLogCorrupt) {
		t.Fatalf("count disagreement = %v, want %v", err, ErrFreeLogCorrupt)
	}

	// The same check on the other field the index publishes: a first offset that
	// does not match the segment's lowest extent would misroute every later
	// change into a neighbouring segment.
	other := newFreeLogTestWriter(t)
	moved := segmentOf(other.image(extents), extents)
	moved.FirstOffset += uint64(testSuperblockPageSize)
	movedHead := other.index([]FreeSegment{moved}, PageRef{})
	movedDelta := other.delta(
		[]FreeDelta{{Op: FreeOpSet, Extent: freeLogTestExtent(600, 1, 2)}}, PageRef{}, movedHead)
	if _, _, err := replayExternalFreeLog(other.cache, movedDelta, other.bounds(), nil, 64, 64); !errors.Is(err, ErrFreeLogCorrupt) {
		t.Fatalf("first-offset disagreement = %v, want %v", err, ErrFreeLogCorrupt)
	}
}
