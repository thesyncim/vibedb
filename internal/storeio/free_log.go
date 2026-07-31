package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// The durable free set, as a base image plus a chain of per-commit deltas.
//
// It replaces a B+tree whose ordered lookup nothing used: the store materializes
// the whole free set into one in-memory slice at open and queries only that, so
// the tree bought sorted access at the price of being a tree. Mutating one cost
// pages, which is why a commit could persist exactly one edit, which is why a
// transaction could consume from exactly one free extent, which is why extents
// reclaimed beyond the first were never written down and were lost at every
// restart.
//
// The property that breaks the cycle is that a flat record can describe its own
// allocation. Content here is fixed once an extent is chosen, so d deltas need
// p = ceil(d / recordsPerPage) pages, allocating those adds at most p more
// deltas, and recomputing converges in two rounds. A tree cannot do this:
// writing it may split, splitting allocates, allocating changes the content,
// and changed content changes the shape.
//
// A commit therefore records its complete free-set diff, however many extents
// it touched. The image is rewritten only when the chain grows past a fold
// threshold, and a fold is a straight-line dump of the in-memory set rather
// than an incremental replay, because that set is already authoritative.
//
// The image is not one object. It is a set of independent segments named by an
// index, so a fold rewrites the segments whose extents moved and leaves the rest
// where they are; free_index.go carries the argument for why, and what the
// linked image it replaced cost. The consequence here is small: an image page
// names nothing, and a delta names the index rather than the image.
//
// Layout. Each delta page names the previous delta and the segment index the
// chain is relative to, so the chain is self-describing from its newest page
// alone. The superblock's existing free reference points at that newest page,
// which is why this representation needs no superblock or state-root change and
// keeps the O(1) checksum-and-reject recovery those already perform.

const (
	// FreeImagePayloadHeaderSize and FreeDeltaPayloadHeaderSize leave the
	// records 8-byte aligned from the start of the page.
	FreeImagePayloadHeaderSize = 64
	FreeDeltaPayloadHeaderSize = 96
	// FreeImageRecordSize matches FreeDirectoryLeafRecordSize: an image record
	// is exactly one extent.
	FreeImageRecordSize = 24
	// FreeDeltaRecordSize adds the operation to an extent, padded so a record
	// never straddles a cache line boundary mid-field.
	FreeDeltaRecordSize = 32

	freeLogVersion = DevelopmentFormatVersion

	freeDeltaPrevOffset      = 16
	freeDeltaIndexHeadOffset = freeDeltaPrevOffset + PageRefSize
)

// ErrFreeLogCorrupt reports a malformed durable free-set image or delta.
var ErrFreeLogCorrupt = errors.New("vibejson: corrupt Store free log")

// A FreeOp is the effect one delta record has on the free set.
type FreeOp uint8

const (
	// FreeOpSet inserts the extent, replacing any extent at the same offset.
	// Reclaiming an extent and growing one by coalescing are both this.
	FreeOpSet FreeOp = iota + 1
	// FreeOpDelete removes the extent at the record's offset. Consuming an
	// extent whole, and retiring the low half of a coalesced pair, are this.
	FreeOpDelete
)

// A FreeDelta is one recorded change to the free set. Length and
// RetiredGeneration are meaningful only for FreeOpSet; a delete is keyed by
// offset alone, because offset is what identifies an extent for the lifetime
// of that extent — allocation takes from an extent's tail precisely so its
// offset never moves.
type FreeDelta struct {
	Op     FreeOp
	Extent FreeExtent
}

// FreeLogHeader is the identity shared by image and delta pages.
type FreeLogHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageSize   uint32
}

// FreeImageView is one checksum-verified borrowed image segment.
type FreeImageView struct {
	header  FreeLogHeader
	payload []byte
	count   uint16
}

// FreeDeltaView is one checksum-verified borrowed delta page.
type FreeDeltaView struct {
	header    FreeLogHeader
	payload   []byte
	prev      PageRef
	indexHead PageRef
	count     uint16
}

// FreeImageRecordCapacity reports how many extents one image page holds.
func FreeImageRecordCapacity(pageSize uint32) int {
	usable := int(pageSize) - PageHeaderSize - PageTrailerSize - FreeImagePayloadHeaderSize
	if usable < 0 {
		return 0
	}
	return usable / FreeImageRecordSize
}

// FreeDeltaRecordCapacity reports how many records one delta page holds. The
// commit path needs this to size its delta chain before allocating, since the
// pages it allocates become records themselves.
func FreeDeltaRecordCapacity(pageSize uint32) int {
	usable := int(pageSize) - PageHeaderSize - PageTrailerSize - FreeDeltaPayloadHeaderSize
	if usable < 0 {
		return 0
	}
	return usable / FreeDeltaRecordSize
}

// EncodeFreeImagePage writes one segment of the image. Extents must be ordered
// by offset and non-overlapping, so a replay can merge deltas into them without
// sorting first.
//
// A segment names nothing. It used to name the next image page, which is what
// made every rewrite total and kept the whole free set inside sixteen pages;
// the segment index now names segments from outside, so a segment's bytes stay
// put when its neighbours change.
func EncodeFreeImagePage(
	dst []byte, header FreeLogHeader, extents []FreeExtent,
	fileEnd, nextLogicalID uint64,
) ([]byte, error) {
	if err := validateFreeLogHeader(header, len(extents), FreeImageRecordSize,
		FreeImagePayloadHeaderSize, nextLogicalID); err != nil {
		return nil, err
	}
	var previousEnd uint64
	for i, extent := range extents {
		if err := validateFreeExtent(extent, header.PageSize, fileEnd); err != nil ||
			i != 0 && extent.Offset < previousEnd {
			return nil, fmt.Errorf("%w: free image extent order or bounds", ErrInvalidWrite)
		}
		previousEnd = extent.Offset + extent.Length
	}
	payloadLength := FreeImagePayloadHeaderSize + len(extents)*FreeImageRecordSize
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadLength), Kind: PageFreeImage,
	})
	if err != nil {
		return nil, err
	}
	encodeFreeLogPayloadHeader(payload, header, len(extents))
	for i, extent := range extents {
		record := payload[FreeImagePayloadHeaderSize+i*FreeImageRecordSize:]
		binary.LittleEndian.PutUint64(record[0:8], extent.Offset)
		binary.LittleEndian.PutUint64(record[8:16], extent.Length)
		binary.LittleEndian.PutUint64(record[16:24], extent.RetiredGeneration)
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// EncodeFreeDeltaPage writes one commit's free-set diff. prev names the delta
// this one supersedes and is the zero ref on the first delta after a fold;
// indexHead names the newest page of the segment index the whole chain is
// relative to, and is repeated on every delta so the newest page alone locates
// the image.
//
// Records are not required to be ordered: a commit's diff is applied in the
// order it was produced, and a later record in the same commit legitimately
// supersedes an earlier one for the same offset.
func EncodeFreeDeltaPage(
	dst []byte, header FreeLogHeader, deltas []FreeDelta, prev, indexHead PageRef,
	fileEnd, nextLogicalID uint64,
) ([]byte, error) {
	if err := validateFreeLogHeader(header, len(deltas), FreeDeltaRecordSize,
		FreeDeltaPayloadHeaderSize, nextLogicalID); err != nil {
		return nil, err
	}
	for _, delta := range deltas {
		if err := validateFreeDeltaRecord(delta, header.PageSize, fileEnd); err != nil {
			return nil, err
		}
	}
	if prev != (PageRef{}) && !validFreeLogPageRef(
		prev, PageFreeDelta, header, fileEnd, nextLogicalID,
	) {
		return nil, fmt.Errorf("%w: free delta predecessor kind", ErrInvalidWrite)
	}
	if indexHead != (PageRef{}) && !validFreeLogPageRef(
		indexHead, PageFreeIndex, header, fileEnd, nextLogicalID,
	) {
		return nil, fmt.Errorf("%w: free delta index kind", ErrInvalidWrite)
	}
	payloadLength := FreeDeltaPayloadHeaderSize + len(deltas)*FreeDeltaRecordSize
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadLength), Kind: PageFreeDelta,
	})
	if err != nil {
		return nil, err
	}
	encodeFreeLogPayloadHeader(payload, header, len(deltas))
	encodePageRef(payload[freeDeltaPrevOffset:freeDeltaPrevOffset+PageRefSize], prev)
	encodePageRef(payload[freeDeltaIndexHeadOffset:freeDeltaIndexHeadOffset+PageRefSize], indexHead)
	for i, delta := range deltas {
		record := payload[FreeDeltaPayloadHeaderSize+i*FreeDeltaRecordSize:]
		record[0] = byte(delta.Op)
		binary.LittleEndian.PutUint64(record[8:16], delta.Extent.Offset)
		binary.LittleEndian.PutUint64(record[16:24], delta.Extent.Length)
		binary.LittleEndian.PutUint64(record[24:32], delta.Extent.RetiredGeneration)
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenFreeImagePage validates one image page.
func OpenFreeImagePage(src []byte, fileEnd, nextLogicalID uint64) (FreeImageView, error) {
	header, payload, count, err := openFreeLogPage(
		src, PageFreeImage, FreeImageRecordSize, FreeImagePayloadHeaderSize, nextLogicalID)
	if err != nil {
		return FreeImageView{}, err
	}
	var previousEnd uint64
	for i := range count {
		record := payload[FreeImagePayloadHeaderSize+i*FreeImageRecordSize:]
		extent := decodeFreeExtent(record)
		if err := validateFreeExtent(extent, header.PageSize, fileEnd); err != nil ||
			i != 0 && extent.Offset < previousEnd {
			return FreeImageView{}, fmt.Errorf("%w: image extent order or bounds", ErrFreeLogCorrupt)
		}
		previousEnd = extent.Offset + extent.Length
	}
	return FreeImageView{header: header, payload: payload, count: uint16(count)}, nil
}

// OpenFreeDeltaPage validates one delta page.
func OpenFreeDeltaPage(src []byte, fileEnd, nextLogicalID uint64) (FreeDeltaView, error) {
	header, payload, count, err := openFreeLogPage(
		src, PageFreeDelta, FreeDeltaRecordSize, FreeDeltaPayloadHeaderSize, nextLogicalID)
	if err != nil {
		return FreeDeltaView{}, err
	}
	prev := decodePageRef(payload[freeDeltaPrevOffset : freeDeltaPrevOffset+PageRefSize])
	indexHead := decodePageRef(payload[freeDeltaIndexHeadOffset : freeDeltaIndexHeadOffset+PageRefSize])
	if prev != (PageRef{}) && !validFreeLogPageRef(
		prev, PageFreeDelta, header, fileEnd, nextLogicalID,
	) {
		return FreeDeltaView{}, fmt.Errorf("%w: delta predecessor", ErrFreeLogCorrupt)
	}
	if indexHead != (PageRef{}) && !validFreeLogPageRef(
		indexHead, PageFreeIndex, header, fileEnd, nextLogicalID,
	) {
		return FreeDeltaView{}, fmt.Errorf("%w: delta index reference", ErrFreeLogCorrupt)
	}
	// A delta must not name itself as its predecessor: the replay walks prev
	// links, and a self-loop would never terminate.
	if prev != (PageRef{}) && prev.LogicalID == header.LogicalID && prev.Generation == header.Generation {
		return FreeDeltaView{}, fmt.Errorf("%w: self-referencing delta", ErrFreeLogCorrupt)
	}
	for i := range count {
		record := payload[FreeDeltaPayloadHeaderSize+i*FreeDeltaRecordSize:]
		delta, ok := decodeFreeDelta(record)
		if !ok {
			return FreeDeltaView{}, fmt.Errorf("%w: delta operation or reserved bytes", ErrFreeLogCorrupt)
		}
		if err := validateFreeDeltaRecord(delta, header.PageSize, fileEnd); err != nil {
			return FreeDeltaView{}, fmt.Errorf("%w: delta record bounds", ErrFreeLogCorrupt)
		}
	}
	return FreeDeltaView{
		header: header, payload: payload, prev: prev, indexHead: indexHead, count: uint16(count),
	}, nil
}

// Header returns value-only page metadata.
func (v FreeImageView) Header() FreeLogHeader { return v.header }

// Len returns the number of extents in this segment.
func (v FreeImageView) Len() int { return int(v.count) }

// ExtentAt returns one extent at rank.
func (v FreeImageView) ExtentAt(rank int) (FreeExtent, bool) {
	if rank < 0 || rank >= int(v.count) {
		return FreeExtent{}, false
	}
	return decodeFreeExtent(v.payload[FreeImagePayloadHeaderSize+rank*FreeImageRecordSize:]), true
}

// Header returns value-only page metadata.
func (v FreeDeltaView) Header() FreeLogHeader { return v.header }

// Len returns the number of records on this delta page.
func (v FreeDeltaView) Len() int { return int(v.count) }

// Prev returns the superseded delta, or the zero ref on the chain's oldest.
func (v FreeDeltaView) Prev() PageRef { return v.prev }

// IndexHead returns the newest segment-index page this delta chain is relative
// to.
func (v FreeDeltaView) IndexHead() PageRef { return v.indexHead }

// DeltaAt returns one record at rank.
func (v FreeDeltaView) DeltaAt(rank int) (FreeDelta, bool) {
	if rank < 0 || rank >= int(v.count) {
		return FreeDelta{}, false
	}
	return decodeFreeDelta(v.payload[FreeDeltaPayloadHeaderSize+rank*FreeDeltaRecordSize:])
}

func encodeFreeLogPayloadHeader(payload []byte, header FreeLogHeader, count int) {
	binary.LittleEndian.PutUint32(payload[0:4], freeLogVersion)
	binary.LittleEndian.PutUint16(payload[6:8], uint16(count))
}

func openFreeLogPage(
	src []byte, kind PageKind, recordSize, payloadHeaderSize int, nextLogicalID uint64,
) (FreeLogHeader, []byte, int, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return FreeLogHeader{}, nil, 0, fmt.Errorf("%w: %w", ErrFreeLogCorrupt, err)
	}
	if pageHeader.Kind != kind || len(payload) < payloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != freeLogVersion ||
		!allZero(payload[4:6]) || !allZero(payload[8:16]) {
		return FreeLogHeader{}, nil, 0, fmt.Errorf("%w: header, version, or reserved bytes", ErrFreeLogCorrupt)
	}
	header := FreeLogHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
	}
	count := int(binary.LittleEndian.Uint16(payload[6:8]))
	if err := validateFreeLogHeader(header, count, recordSize, payloadHeaderSize, nextLogicalID); err != nil ||
		len(payload) != payloadHeaderSize+count*recordSize {
		return FreeLogHeader{}, nil, 0, fmt.Errorf("%w: page bounds", ErrFreeLogCorrupt)
	}
	return header, payload, count, nil
}

func validateFreeLogHeader(
	header FreeLogHeader, count, recordSize, payloadHeaderSize int, nextLogicalID uint64,
) error {
	capacity := (int(header.PageSize) - PageHeaderSize - PageTrailerSize - payloadHeaderSize) / recordSize
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID == 0 || header.LogicalID >= nextLogicalID ||
		!validPhysicalPageSize(header.PageSize) ||
		count < 0 || count > capacity {
		return fmt.Errorf("%w: free log page identity or capacity", ErrInvalidWrite)
	}
	return nil
}

func validFreeLogPageRef(
	ref PageRef,
	kind PageKind,
	header FreeLogHeader,
	fileEnd, nextLogicalID uint64,
) bool {
	layout, err := MutableStoreLayout(header.PageSize)
	if err != nil {
		return false
	}
	pageSize := uint64(header.PageSize)
	return ref.Kind == kind &&
		ref.Length == header.PageSize &&
		ref.Generation != 0 && ref.Generation <= header.Generation &&
		ref.LogicalID != 0 && ref.LogicalID < nextLogicalID &&
		ref.Offset >= layout.DataStart && ref.Offset%pageSize == 0 &&
		ref.Offset <= maxSuperblockFileOffset &&
		pageSize <= fileEnd && ref.Offset <= fileEnd-pageSize
}

func validateFreeDeltaRecord(delta FreeDelta, pageSize uint32, fileEnd uint64) error {
	switch delta.Op {
	case FreeOpSet:
		return validateFreeExtent(delta.Extent, pageSize, fileEnd)
	case FreeOpDelete:
		// A delete carries only its key. Length and generation must be zero so
		// a record cannot claim two meanings, and the offset must still be a
		// legal extent start.
		quantum := uint64(pageSize)
		layout, err := MutableStoreLayout(pageSize)
		if err != nil {
			return err
		}
		if delta.Extent.Length != 0 || delta.Extent.RetiredGeneration != 0 ||
			delta.Extent.Offset%quantum != 0 ||
			delta.Extent.Offset < layout.DataStart ||
			delta.Extent.Offset > fileEnd {
			return fmt.Errorf("%w: free delete record", ErrInvalidWrite)
		}
		return nil
	default:
		return fmt.Errorf("%w: free delta operation", ErrInvalidWrite)
	}
}

func decodeFreeDelta(record []byte) (FreeDelta, bool) {
	op := FreeOp(record[0])
	if op != FreeOpSet && op != FreeOpDelete || !allZero(record[1:8]) {
		return FreeDelta{}, false
	}
	return FreeDelta{Op: op, Extent: FreeExtent{
		Offset:            binary.LittleEndian.Uint64(record[8:16]),
		Length:            binary.LittleEndian.Uint64(record[16:24]),
		RetiredGeneration: binary.LittleEndian.Uint64(record[24:32]),
	}}, true
}
