package storeio

import (
	"encoding/binary"
	"fmt"
)

// The free image's segment index, and why the image stopped being a linked list.
//
// The image used to be a chain of pages, each naming its successor. That shape
// made every rewrite total: changing one extent changed the page holding it,
// which changed that page's physical offset, which changed the reference in the
// page before it, all the way back to the head. A fold therefore rewrote the
// whole image no matter how little of it had moved, and the only way to keep
// that affordable was to keep the image small — sixteen pages, roughly 2,700
// extents, about 11 MiB of trackable free space at the 4 KiB page size. That
// bound is what a multi-terabyte store runs into: ordinary fragmentation there
// is millions of extents, and past the bound the store cannot write down free
// space it has reclaimed, so it leaks it.
//
// Naming the segments from an index instead removes the coupling. Segments are
// disjoint, offset-ordered runs of extents, each one page; the index is a chain
// of pages listing every segment's reference, its first offset, and its largest
// free extent. A commit that changes extents inside one segment rewrites that
// segment and the index, and leaves every other segment's bytes exactly where
// they are. Fold cost becomes proportional to what changed rather than to what
// exists, which is the property the flat image could not have.
//
// The index keeps the self-allocation argument the flat log was built around.
// Nothing here is a tree and nothing splits: a segment that outgrows one page
// becomes two segments, which appends one record to an index that is rewritten
// whole regardless, so no insert propagates. The delta chain in front of the
// index still absorbs every ordinary commit, so the index is rewritten only at a
// fold, and a fold — like every other free-log write — allocates its pages
// before it fills them, so the records describing those allocations fit in the
// same commit.
//
// The index is deliberately chained rather than indexed in turn. One more level
// would buy a cheaper partial rewrite of the index itself, but the index is
// already smaller than the thing it describes by the segment fan-out, so a store
// whose free set needs 4,480 segments needs 64 index pages. Paying 64 page
// writes per fold to avoid paying 4,480 is the whole win; a third level would
// shave a constant off an already-small term and add a second place for the
// self-allocation fixed point to fail to converge.

const (
	// FreeIndexPayloadHeaderSize leaves segment records 8-byte aligned from the
	// start of the page.
	FreeIndexPayloadHeaderSize = 64
	// FreeIndexRecordSize is one segment descriptor: where its page is, where
	// its extents start, and how much it can serve without being read.
	FreeIndexRecordSize = 56

	freeIndexPrevOffset = 16
)

// FreeLogMaxIndexPages bounds the segment index, and with it the free set.
//
// It is the one remaining hard cap, and it is deliberately placed where a cap
// costs least. The old bound applied to the image itself: sixteen pages, about
// 2,700 extents, roughly 11 MiB of trackable free space at the 4 KiB page size.
// The bound now applies to the index, and a 4 KiB index page names 70 segments
// of 165 extents each, so eight index pages describe 560 segments and exactly
// 92,400 extents — 360.9 MiB of one-page holes. The
// difference is entirely the segment fan-out: what must fit inside one commit is
// no longer the free set but a directory of it.
//
// The current worst-case fold reserve is 28 pages: eight index pages,
// FreeLogMaxFoldSegments (16) segments, and FreeLogMaxDeltaPages (4) deltas.
// Raising the index cap is therefore a policy edit against transaction staging,
// while a second index level would remove the cap at the price of one more
// place for the self-allocation fixed point to converge.
const FreeLogMaxIndexPages = 8

// FreeSegment is one descriptor in the free image's index.
//
// FirstOffset is the lowest extent offset the segment holds, and it is what
// makes the index a partition rather than a list: segment i owns every offset
// in [FirstOffset(i), FirstOffset(i+1)), the first segment owns everything
// below its own first offset as well, and the last owns everything above. A
// commit routes each changed extent to exactly one segment by that rule, which
// is what lets it rewrite one segment and leave the rest untouched.
//
// LargestFree is carried so an allocator can rule a segment out without reading
// it. Count is carried so a fold knows a segment's headroom before it decides
// whether the segment must split.
type FreeSegment struct {
	Ref         PageRef
	FirstOffset uint64
	LargestFree uint64
	Count       uint32
}

// FreeIndexView is one checksum-verified borrowed index page.
type FreeIndexView struct {
	header  FreeLogHeader
	payload []byte
	prev    PageRef
	count   uint16
}

// FreeIndexRecordCapacity reports how many segments one index page names.
func FreeIndexRecordCapacity(pageSize uint32) int {
	usable := int(pageSize) - PageHeaderSize - PageTrailerSize - FreeIndexPayloadHeaderSize
	if usable < 0 {
		return 0
	}
	return usable / FreeIndexRecordSize
}

// EncodeFreeIndexPage writes one page of the segment index. Segments must be
// ordered by FirstOffset and must not repeat one, because the replay treats the
// concatenation of every index page as a partition of the offset space and a
// repeated boundary would give one offset two owners.
//
// prev names the index page holding the segments below these and is the zero
// ref on the first page, so the newest page alone locates the whole index. The
// chain runs low-to-high with the head at the high end for the same reason the
// delta chain runs oldest-to-newest with the head at the newest: a page can only
// name something that already exists, and the pages are written in that order.
func EncodeFreeIndexPage(
	dst []byte, header FreeLogHeader, segments []FreeSegment, prev PageRef,
	fileEnd, nextLogicalID uint64,
) ([]byte, error) {
	if err := validateFreeLogHeader(header, len(segments), FreeIndexRecordSize,
		FreeIndexPayloadHeaderSize, nextLogicalID); err != nil {
		return nil, err
	}
	var previousFirst uint64
	for i, segment := range segments {
		if err := validateFreeSegment(segment, header.PageSize, fileEnd, nextLogicalID); err != nil {
			return nil, err
		}
		if i != 0 && segment.FirstOffset <= previousFirst {
			return nil, fmt.Errorf("%w: free index segment order", ErrInvalidWrite)
		}
		previousFirst = segment.FirstOffset
	}
	if prev != (PageRef{}) && !validFreeLogPageRef(
		prev, PageFreeIndex, header, fileEnd, nextLogicalID,
	) {
		return nil, fmt.Errorf("%w: free index predecessor kind", ErrInvalidWrite)
	}
	payloadLength := FreeIndexPayloadHeaderSize + len(segments)*FreeIndexRecordSize
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadLength), Kind: PageFreeIndex,
	})
	if err != nil {
		return nil, err
	}
	encodeFreeLogPayloadHeader(payload, header, len(segments))
	encodePageRef(payload[freeIndexPrevOffset:freeIndexPrevOffset+PageRefSize], prev)
	for i, segment := range segments {
		record := payload[FreeIndexPayloadHeaderSize+i*FreeIndexRecordSize:]
		encodePageRef(record[0:PageRefSize], segment.Ref)
		binary.LittleEndian.PutUint64(record[32:40], segment.FirstOffset)
		binary.LittleEndian.PutUint64(record[40:48], segment.LargestFree)
		binary.LittleEndian.PutUint32(record[48:52], segment.Count)
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenFreeIndexPage validates one index page.
func OpenFreeIndexPage(src []byte, fileEnd, nextLogicalID uint64) (FreeIndexView, error) {
	header, payload, count, err := openFreeLogPage(
		src, PageFreeIndex, FreeIndexRecordSize, FreeIndexPayloadHeaderSize, nextLogicalID)
	if err != nil {
		return FreeIndexView{}, err
	}
	prev := decodePageRef(payload[freeIndexPrevOffset : freeIndexPrevOffset+PageRefSize])
	if prev != (PageRef{}) && (!validFreeLogPageRef(
		prev, PageFreeIndex, header, fileEnd, nextLogicalID,
	) ||
		!pageRefReservedZero(payload[freeIndexPrevOffset:freeIndexPrevOffset+PageRefSize])) {
		return FreeIndexView{}, fmt.Errorf("%w: index predecessor", ErrFreeLogCorrupt)
	}
	// A page must not name itself: the replay walks prev links, and a self-loop
	// would never terminate.
	if prev != (PageRef{}) && prev.LogicalID == header.LogicalID && prev.Generation == header.Generation {
		return FreeIndexView{}, fmt.Errorf("%w: self-referencing index page", ErrFreeLogCorrupt)
	}
	var previousFirst uint64
	for i := range count {
		segment := decodeFreeSegment(payload[FreeIndexPayloadHeaderSize+i*FreeIndexRecordSize:])
		if err := validateFreeSegment(segment, header.PageSize, fileEnd, nextLogicalID); err != nil ||
			i != 0 && segment.FirstOffset <= previousFirst {
			return FreeIndexView{}, fmt.Errorf("%w: index segment order or bounds", ErrFreeLogCorrupt)
		}
		previousFirst = segment.FirstOffset
	}
	return FreeIndexView{header: header, payload: payload, prev: prev, count: uint16(count)}, nil
}

// Header returns value-only page metadata.
func (v FreeIndexView) Header() FreeLogHeader { return v.header }

// Len returns the number of segments named by this index page.
func (v FreeIndexView) Len() int { return int(v.count) }

// Prev returns the index page below this one, or the zero ref on the first.
func (v FreeIndexView) Prev() PageRef { return v.prev }

// SegmentAt returns one segment descriptor at rank.
func (v FreeIndexView) SegmentAt(rank int) (FreeSegment, bool) {
	if rank < 0 || rank >= int(v.count) {
		return FreeSegment{}, false
	}
	return decodeFreeSegment(v.payload[FreeIndexPayloadHeaderSize+rank*FreeIndexRecordSize:]), true
}

func decodeFreeSegment(src []byte) FreeSegment {
	return FreeSegment{
		Ref:         decodePageRef(src[0:PageRefSize]),
		FirstOffset: binary.LittleEndian.Uint64(src[32:40]),
		LargestFree: binary.LittleEndian.Uint64(src[40:48]),
		Count:       binary.LittleEndian.Uint32(src[48:52]),
	}
}

func validateFreeSegment(segment FreeSegment, pageSize uint32, fileEnd, nextLogicalID uint64) error {
	// A segment with no extents has no page to point at and no offset to own, so
	// it must never be written down: an empty descriptor would claim a slice of
	// the offset partition that no extent can ever be routed out of again.
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		return err
	}
	if segment.Ref.Kind != PageFreeImage || segment.Count == 0 ||
		segment.Ref.Offset < layout.DataStart ||
		segment.Ref.Offset%uint64(pageSize) != 0 ||
		segment.Ref.Length == 0 || segment.Ref.Length%pageSize != 0 ||
		uint64(segment.Ref.Length) > fileEnd || segment.Ref.Offset > fileEnd-uint64(segment.Ref.Length) ||
		segment.Ref.LogicalID <= StateRootLogicalID || segment.Ref.LogicalID >= nextLogicalID ||
		segment.Ref.Generation == 0 || segment.Ref.Flags != 0 || segment.Ref.Aux != 0 ||
		int(segment.Count) > FreeImageRecordCapacity(segment.Ref.Length) ||
		segment.FirstOffset%uint64(pageSize) != 0 ||
		segment.FirstOffset < layout.DataStart ||
		segment.FirstOffset >= fileEnd ||
		segment.LargestFree == 0 || segment.LargestFree%uint64(pageSize) != 0 ||
		segment.LargestFree > fileEnd {
		return fmt.Errorf("%w: free index segment", ErrInvalidWrite)
	}
	return nil
}
