package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	// InlineSuperblockSize is the checksummed prefix stored in each of the two
	// fixed root pages. The complete StateRoot payload lives inside this record,
	// so publishing a generation needs no separately allocated state-root page.
	//
	// The tail carries a cumulative free-set delta; bytes beyond its bounded
	// record count stay zero and are validated zero. A torn write therefore
	// cannot overlap the alternate copy, while direct I/O still writes one
	// aligned physical page.
	InlineSuperblockSize = 4096

	inlineSuperblockMagic        = "SJINL001"
	inlineSuperblockVersion      = DevelopmentFormatVersion
	inlineSuperblockKnownFlags   = uint32(0)
	inlineSuperblockStateOffset  = 96
	inlineSuperblockStateEnd     = inlineSuperblockStateOffset + StateRootPayloadSize
	inlineSuperblockChecksumFrom = InlineSuperblockSize - 8

	inlineFreeDeltaOffset          = inlineSuperblockStateEnd
	inlineFreeDeltaVersion         = DevelopmentFormatVersion
	inlineFreeDeltaCountOffset     = inlineFreeDeltaOffset + 4
	inlineFreeDeltaPrevOffset      = inlineFreeDeltaOffset + 8
	inlineFreeDeltaIndexHeadOffset = inlineFreeDeltaPrevOffset + PageRefSize
	inlineFreeDeltaRecordsOffset   = inlineFreeDeltaIndexHeadOffset + PageRefSize

	// InlineFreeDeltaCapacity is the maximum cumulative free-set diff carried
	// by one fixed root page at the minimum 4 KiB page size.
	InlineFreeDeltaCapacity = (inlineSuperblockChecksumFrom - inlineFreeDeltaRecordsOffset) / FreeDeltaRecordSize
)

// ErrInlineFreeDeltaFull tells a writer to spill the cumulative records into
// one external PageFreeDelta and begin a new inline cumulative run.
var ErrInlineFreeDeltaFull = errors.New("vibejson: inline free delta is full")

// InlineFreeDelta is a fixed-capacity, allocation-free cumulative free-set
// diff. ExternalPrev is the newest external delta page preceding every inline
// record; IndexHead is the segment image that external and inline records share.
//
// Append keeps at most one record per extent offset in ascending offset order.
// A later operation replaces the earlier operation in place, preserving the
// free log's latest-wins semantics while producing one canonical encoding for
// the final cumulative map.
type InlineFreeDelta struct {
	externalPrev PageRef
	indexHead    PageRef
	count        uint16
	records      [InlineFreeDeltaCapacity * FreeDeltaRecordSize]byte
}

// NewInlineFreeDelta starts an empty cumulative run after externalPrev.
func NewInlineFreeDelta(externalPrev, indexHead PageRef) InlineFreeDelta {
	return InlineFreeDelta{externalPrev: externalPrev, indexHead: indexHead}
}

// Reset discards the inline run after it has been spilled and selects the new
// external predecessor and image index.
func (d *InlineFreeDelta) Reset(externalPrev, indexHead PageRef) {
	if d == nil {
		return
	}
	*d = NewInlineFreeDelta(externalPrev, indexHead)
}

// ExternalPrev returns the external delta immediately preceding this
// cumulative inline run.
func (d *InlineFreeDelta) ExternalPrev() PageRef {
	if d == nil {
		return PageRef{}
	}
	return d.externalPrev
}

// IndexHead returns the segment-image index shared by the external chain and
// this cumulative inline run.
func (d *InlineFreeDelta) IndexHead() PageRef {
	if d == nil {
		return PageRef{}
	}
	return d.indexHead
}

// Len returns the number of canonical cumulative records.
func (d *InlineFreeDelta) Len() int {
	if d == nil {
		return 0
	}
	return int(d.count)
}

// DeltaAt returns one cumulative record in stable production order.
func (d *InlineFreeDelta) DeltaAt(rank int) (FreeDelta, bool) {
	if d == nil || rank < 0 || rank >= int(d.count) {
		return FreeDelta{}, false
	}
	return decodeFreeDelta(d.records[rank*FreeDeltaRecordSize:])
}

// Latest returns the surviving operation for offset.
func (d *InlineFreeDelta) Latest(offset uint64) (FreeDelta, bool) {
	if d == nil {
		return FreeDelta{}, false
	}
	rank, found := d.rank(offset)
	if !found {
		return FreeDelta{}, false
	}
	return d.DeltaAt(rank)
}

// Append adds changes to the cumulative run without allocating. Duplicate
// offsets, including duplicates within records, retain only the last operation.
// Failure is transactional: d is unchanged if a record is invalid, the final
// set overlaps itself, or the canonical run exceeds InlineFreeDeltaCapacity.
func (d *InlineFreeDelta) Append(records []FreeDelta, pageSize uint32, fileEnd uint64) error {
	if d == nil {
		return ErrInvalidWrite
	}
	next := *d
	for _, delta := range records {
		if err := validateFreeDeltaRecord(delta, pageSize, fileEnd); err != nil {
			return err
		}
		rank, found := next.rank(delta.Extent.Offset)
		if !found {
			if int(next.count) == InlineFreeDeltaCapacity {
				return ErrInlineFreeDeltaFull
			}
			start := rank * FreeDeltaRecordSize
			end := int(next.count) * FreeDeltaRecordSize
			copy(
				next.records[start+FreeDeltaRecordSize:end+FreeDeltaRecordSize],
				next.records[start:end],
			)
			next.count++
		}
		encodeInlineFreeDeltaRecord(next.records[rank*FreeDeltaRecordSize:], delta)
	}
	if err := validateInlineFreeDeltaRecords(&next, pageSize, fileEnd); err != nil {
		return err
	}
	*d = next
	return nil
}

func (d *InlineFreeDelta) rank(offset uint64) (int, bool) {
	low, high := 0, int(d.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		delta, _ := d.DeltaAt(middle)
		if delta.Extent.Offset < offset {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low < int(d.count) {
		delta, _ := d.DeltaAt(low)
		return low, delta.Extent.Offset == offset
	}
	return low, false
}

// InlineSuperblock is the failure-atomic root of one Store generation. State
// and the cumulative free-set diff are encoded in the same fixed root page
// rather than separately allocated immutable pages. FileEnd consequently
// describes data and external spill/image extents only.
//
// This is a strict development-format codec. It neither recognizes nor emits
// the external-state Superblock format.
type InlineSuperblock struct {
	StoreID    [16]byte
	Generation uint64
	FileEnd    uint64
	PageSize   uint32
	Flags      uint32
	State      StateRoot
	FreeDelta  InlineFreeDelta
}

// EncodeInlineSuperblock writes one deterministic inline root record into dst.
// It performs no allocation.
func EncodeInlineSuperblock(dst []byte, root InlineSuperblock) ([]byte, error) {
	if len(dst) < InlineSuperblockSize {
		return nil, fmt.Errorf("%w: inline superblock buffer has %d bytes", ErrInvalidWrite, len(dst))
	}
	if err := validateInlineSuperblock(root); err != nil {
		return nil, err
	}
	dst = dst[:InlineSuperblockSize]
	clear(dst)
	copy(dst[0:8], inlineSuperblockMagic)
	binary.LittleEndian.PutUint32(dst[8:12], inlineSuperblockVersion)
	binary.LittleEndian.PutUint32(dst[12:16], InlineSuperblockSize)
	binary.LittleEndian.PutUint32(dst[16:20], root.Flags)
	binary.LittleEndian.PutUint32(dst[20:24], root.PageSize)
	binary.LittleEndian.PutUint64(dst[24:32], root.Generation)
	binary.LittleEndian.PutUint64(dst[32:40], ^root.Generation)
	binary.LittleEndian.PutUint64(dst[40:48], root.FileEnd)
	copy(dst[72:88], root.StoreID[:])
	encodeStateRootPayload(dst[inlineSuperblockStateOffset:inlineSuperblockStateEnd], root.State)
	encodeInlineFreeDelta(dst, &root.FreeDelta)
	checksum := PageChecksum(dst[:inlineSuperblockChecksumFrom])
	binary.LittleEndian.PutUint32(dst[inlineSuperblockChecksumFrom:inlineSuperblockChecksumFrom+4], checksum)
	binary.LittleEndian.PutUint32(dst[inlineSuperblockChecksumFrom+4:InlineSuperblockSize], ^checksum)
	return dst, nil
}

// DecodeInlineSuperblock verifies and decodes one inline root record. Unknown
// flags, non-zero reserved bytes, impossible references, and checksum
// mismatches fail closed before any embedded offset is trusted.
func DecodeInlineSuperblock(src []byte) (InlineSuperblock, error) {
	if len(src) < InlineSuperblockSize {
		return InlineSuperblock{}, fmt.Errorf("%w: short inline record", ErrSuperblockCorrupt)
	}
	src = src[:InlineSuperblockSize]
	if string(src[0:8]) != inlineSuperblockMagic {
		return InlineSuperblock{}, fmt.Errorf("%w: inline magic", ErrSuperblockCorrupt)
	}
	if binary.LittleEndian.Uint32(src[8:12]) != inlineSuperblockVersion ||
		binary.LittleEndian.Uint32(src[12:16]) != InlineSuperblockSize {
		return InlineSuperblock{}, fmt.Errorf("%w: inline version or size", ErrSuperblockCorrupt)
	}
	checksum := binary.LittleEndian.Uint32(src[inlineSuperblockChecksumFrom : inlineSuperblockChecksumFrom+4])
	if binary.LittleEndian.Uint32(src[inlineSuperblockChecksumFrom+4:InlineSuperblockSize]) != ^checksum ||
		PageChecksum(src[:inlineSuperblockChecksumFrom]) != checksum {
		return InlineSuperblock{}, fmt.Errorf("%w: inline checksum", ErrSuperblockCorrupt)
	}
	if !allZero(src[48:72]) || !allZero(src[88:inlineSuperblockStateOffset]) {
		return InlineSuperblock{}, fmt.Errorf("%w: inline reserved bytes", ErrSuperblockCorrupt)
	}
	root := InlineSuperblock{
		Generation: binary.LittleEndian.Uint64(src[24:32]),
		FileEnd:    binary.LittleEndian.Uint64(src[40:48]),
		Flags:      binary.LittleEndian.Uint32(src[16:20]),
		PageSize:   binary.LittleEndian.Uint32(src[20:24]),
	}
	copy(root.StoreID[:], src[72:88])
	if binary.LittleEndian.Uint64(src[32:40]) != ^root.Generation {
		return InlineSuperblock{}, fmt.Errorf("%w: inline complement", ErrSuperblockCorrupt)
	}
	state, err := decodeStateRootPayload(
		src[inlineSuperblockStateOffset:inlineSuperblockStateEnd],
		root.StoreID, root.Generation, root.PageSize, root.FileEnd,
	)
	if err != nil {
		return InlineSuperblock{}, fmt.Errorf("%w: %v", ErrSuperblockCorrupt, err)
	}
	root.State = state
	freeDelta, err := decodeInlineFreeDelta(src, root)
	if err != nil {
		return InlineSuperblock{}, fmt.Errorf("%w: %v", ErrSuperblockCorrupt, err)
	}
	root.FreeDelta = freeDelta
	if err := validateInlineSuperblock(root); err != nil {
		return InlineSuperblock{}, fmt.Errorf("%w: %v", ErrSuperblockCorrupt, err)
	}
	return root, nil
}

func encodeInlineFreeDelta(dst []byte, delta *InlineFreeDelta) {
	binary.LittleEndian.PutUint32(
		dst[inlineFreeDeltaOffset:inlineFreeDeltaCountOffset], inlineFreeDeltaVersion,
	)
	binary.LittleEndian.PutUint16(
		dst[inlineFreeDeltaCountOffset:inlineFreeDeltaCountOffset+2], delta.count,
	)
	encodePageRef(
		dst[inlineFreeDeltaPrevOffset:inlineFreeDeltaIndexHeadOffset], delta.externalPrev,
	)
	encodePageRef(
		dst[inlineFreeDeltaIndexHeadOffset:inlineFreeDeltaRecordsOffset], delta.indexHead,
	)
	used := int(delta.count) * FreeDeltaRecordSize
	copy(dst[inlineFreeDeltaRecordsOffset:inlineFreeDeltaRecordsOffset+used], delta.records[:used])
}

func decodeInlineFreeDelta(src []byte, root InlineSuperblock) (InlineFreeDelta, error) {
	if binary.LittleEndian.Uint32(
		src[inlineFreeDeltaOffset:inlineFreeDeltaCountOffset],
	) != inlineFreeDeltaVersion ||
		!allZero(src[inlineFreeDeltaCountOffset+2:inlineFreeDeltaPrevOffset]) {
		return InlineFreeDelta{}, fmt.Errorf("%w: inline free header", ErrFreeLogCorrupt)
	}
	count := int(binary.LittleEndian.Uint16(
		src[inlineFreeDeltaCountOffset : inlineFreeDeltaCountOffset+2],
	))
	if count > InlineFreeDeltaCapacity {
		return InlineFreeDelta{}, fmt.Errorf("%w: inline free capacity", ErrFreeLogCorrupt)
	}
	usedEnd := inlineFreeDeltaRecordsOffset + count*FreeDeltaRecordSize
	if !allZero(src[usedEnd:inlineSuperblockChecksumFrom]) {
		return InlineFreeDelta{}, fmt.Errorf("%w: inline free tail", ErrFreeLogCorrupt)
	}
	delta := InlineFreeDelta{
		externalPrev: decodePageRef(src[inlineFreeDeltaPrevOffset:inlineFreeDeltaIndexHeadOffset]),
		indexHead:    decodePageRef(src[inlineFreeDeltaIndexHeadOffset:inlineFreeDeltaRecordsOffset]),
		count:        uint16(count),
	}
	copy(delta.records[:], src[inlineFreeDeltaRecordsOffset:usedEnd])
	if err := validateInlineFreeDelta(&delta, &root); err != nil {
		return InlineFreeDelta{}, fmt.Errorf("%w: %v", ErrFreeLogCorrupt, err)
	}
	return delta, nil
}

func encodeInlineFreeDeltaRecord(dst []byte, delta FreeDelta) {
	clear(dst[:FreeDeltaRecordSize])
	dst[0] = byte(delta.Op)
	binary.LittleEndian.PutUint64(dst[8:16], delta.Extent.Offset)
	binary.LittleEndian.PutUint64(dst[16:24], delta.Extent.Length)
	binary.LittleEndian.PutUint64(dst[24:32], delta.Extent.RetiredGeneration)
}

func validateInlineFreeDelta(delta *InlineFreeDelta, root *InlineSuperblock) error {
	if int(delta.count) > InlineFreeDeltaCapacity {
		return fmt.Errorf("%w: inline free capacity", ErrInvalidWrite)
	}
	if err := validateInlineFreeRef(
		delta.externalPrev, PageFreeDelta, root,
	); err != nil {
		return err
	}
	if err := validateInlineFreeRef(
		delta.indexHead, PageFreeIndex, root,
	); err != nil {
		return err
	}
	if delta.externalPrev != (PageRef{}) && delta.indexHead != (PageRef{}) &&
		(delta.externalPrev.Offset == delta.indexHead.Offset ||
			delta.externalPrev.LogicalID == delta.indexHead.LogicalID) {
		return fmt.Errorf("%w: duplicate inline free reference", ErrInvalidWrite)
	}
	if err := validateInlineFreeDeltaRecords(
		delta, root.PageSize, root.FileEnd,
	); err != nil {
		return err
	}
	for rank := 0; rank < int(delta.count); rank++ {
		record, _ := delta.DeltaAt(rank)
		if record.Op == FreeOpSet {
			if record.Extent.RetiredGeneration > root.Generation {
				return fmt.Errorf("%w: future inline retirement", ErrInvalidWrite)
			}
			if inlineExtentOverlapsRoot(record.Extent, root) {
				return fmt.Errorf("%w: inline free extent reaches live root", ErrInvalidWrite)
			}
		}
	}
	return nil
}

func validateInlineFreeRef(ref PageRef, kind PageKind, root *InlineSuperblock) error {
	if ref == (PageRef{}) {
		return nil
	}
	pageSize := uint64(root.PageSize)
	layout, err := MutableStoreLayout(root.PageSize)
	if err != nil {
		return err
	}
	if ref.Kind != kind || ref.Flags != 0 || ref.Aux != 0 ||
		ref.Length != root.PageSize || ref.Generation == 0 ||
		ref.Generation > root.Generation || ref.LogicalID <= StateRootLogicalID ||
		ref.LogicalID >= root.State.NextLogicalID ||
		ref.Offset < layout.DataStart ||
		ref.Offset%pageSize != 0 || ref.Offset > maxSuperblockFileOffset ||
		ref.Offset > root.FileEnd-pageSize {
		return fmt.Errorf("%w: invalid inline free reference", ErrInvalidWrite)
	}
	refExtent := FreeExtent{Offset: ref.Offset, Length: uint64(ref.Length)}
	stateRefs := [...]PageRef{
		root.State.PrimaryRoot,
		root.State.ExactIndexRoot,
	}
	for _, stateRef := range stateRefs {
		if stateRef == (PageRef{}) {
			continue
		}
		stateExtent := FreeExtent{
			Offset: stateRef.Offset, Length: uint64(stateRef.Length),
		}
		if ref.LogicalID == stateRef.LogicalID ||
			extentsOverlap(refExtent, stateExtent) {
			return fmt.Errorf("%w: inline free reference overlaps state", ErrInvalidWrite)
		}
	}
	if catalogExtent, logicalEnd, ok :=
		stateRootPageCatalogRun(root.State); ok {
		if extentsOverlap(refExtent, catalogExtent) ||
			ref.LogicalID >= root.State.PageCatalogHead.LogicalID &&
				ref.LogicalID < logicalEnd {
			return fmt.Errorf(
				"%w: inline free reference overlaps catalog",
				ErrInvalidWrite,
			)
		}
	}
	return nil
}

func validateInlineFreeDeltaRecords(
	delta *InlineFreeDelta, pageSize uint32, fileEnd uint64,
) error {
	var previousOffset uint64
	var previousSetEnd uint64
	for rank := 0; rank < int(delta.count); rank++ {
		record, ok := delta.DeltaAt(rank)
		if !ok {
			return fmt.Errorf("%w: inline free operation or reserved bytes", ErrInvalidWrite)
		}
		if err := validateFreeDeltaRecord(record, pageSize, fileEnd); err != nil {
			return err
		}
		if rank != 0 && record.Extent.Offset <= previousOffset {
			return fmt.Errorf("%w: unordered inline free offset", ErrInvalidWrite)
		}
		previousOffset = record.Extent.Offset
		if record.Op == FreeOpSet {
			if record.Extent.Offset < previousSetEnd {
				return fmt.Errorf("%w: overlapping inline free extents", ErrInvalidWrite)
			}
			previousSetEnd = record.Extent.Offset + record.Extent.Length
		}
	}
	return nil
}

func extentsOverlap(left, right FreeExtent) bool {
	return left.Offset < right.Offset+right.Length &&
		right.Offset < left.Offset+left.Length
}

func inlineExtentOverlapsRoot(extent FreeExtent, root *InlineSuperblock) bool {
	refs := [...]PageRef{
		root.State.PrimaryRoot,
		root.State.ExactIndexRoot,
		root.FreeDelta.externalPrev,
		root.FreeDelta.indexHead,
	}
	for _, ref := range refs {
		if ref == (PageRef{}) {
			continue
		}
		refExtent := FreeExtent{Offset: ref.Offset, Length: uint64(ref.Length)}
		if extentsOverlap(extent, refExtent) {
			return true
		}
	}
	if catalogExtent, _, ok :=
		stateRootPageCatalogRun(root.State); ok {
		return extentsOverlap(extent, catalogExtent)
	}
	return false
}

func validateInlineSuperblock(root InlineSuperblock) error {
	if root.Generation == 0 || root.Flags&^inlineSuperblockKnownFlags != 0 ||
		!validPhysicalPageSize(root.PageSize) {
		return fmt.Errorf("%w: invalid inline generation, flags, or page size", ErrInvalidWrite)
	}
	if root.StoreID == ([16]byte{}) {
		return fmt.Errorf("%w: zero inline Store id", ErrInvalidWrite)
	}
	pageSize := uint64(root.PageSize)
	layout, err := MutableStoreLayout(root.PageSize)
	if err != nil {
		return err
	}
	dataStart := layout.DataStart
	if root.FileEnd < dataStart || root.FileEnd > maxSuperblockFileOffset ||
		root.FileEnd%pageSize != 0 {
		return fmt.Errorf("%w: inline file high-water mark", ErrInvalidWrite)
	}
	if root.State.StoreID != root.StoreID || root.State.Generation != root.Generation ||
		root.State.PageSize != root.PageSize {
		return fmt.Errorf("%w: inline state identity", ErrInvalidWrite)
	}
	if err := validateStateRoot(root.State, root.FileEnd); err != nil {
		return err
	}
	if err := validateInlineFreeDelta(&root.FreeDelta, &root); err != nil {
		return err
	}
	return nil
}

type inlineSuperblockCandidate struct {
	root InlineSuperblock
	slot int
}

// SelectInlineSuperblock returns the newest structurally valid inline root.
// It does not recognize the external-state superblock format.
func SelectInlineSuperblock(first, second []byte) (InlineSuperblock, int, error) {
	candidates, _, err := orderedInlineSuperblocks(first, second)
	if err != nil {
		return InlineSuperblock{}, -1, err
	}
	return candidates[0].root, candidates[0].slot, nil
}

// RecoverInlineStateRoot validates both fixed inline roots newest-to-oldest,
// including top-level page references and any external free-log anchor. A torn
// or semantically invalid newest generation falls back to the preceding valid
// generation without reading separately allocated state or routine delta pages.
func RecoverInlineStateRoot(
	file *os.File, pageSize uint32, pageScratch []byte,
) (InlineSuperblock, StateRoot, int, error) {
	root, state, slot, _, err := RecoverInlineStateRootWithFallback(file, pageSize, pageScratch)
	return root, state, slot, err
}

// RecoverInlineStateRootWithFallback additionally returns the generation of
// the other fully validated recovery root. If only one is valid, its generation
// is returned conservatively as the fallback fence.
func RecoverInlineStateRootWithFallback(
	file *os.File, pageSize uint32, pageScratch []byte,
) (InlineSuperblock, StateRoot, int, uint64, error) {
	layout, layoutErr := MutableStoreLayout(pageSize)
	if file == nil || layoutErr != nil {
		return InlineSuperblock{}, StateRoot{}, -1, 0,
			fmt.Errorf("%w: invalid inline recovery file or page size", ErrInvalidWrite)
	}
	if uint64(len(pageScratch)) < uint64(pageSize) {
		return InlineSuperblock{}, StateRoot{}, -1, 0,
			fmt.Errorf("%w: have=%d need=%d", ErrRecoveryBufferTooSmall, len(pageScratch), pageSize)
	}
	var headers [superblockCopies * InlineSuperblockSize]byte
	for slot := range superblockCopies {
		buf := headers[slot*InlineSuperblockSize : (slot+1)*InlineSuperblockSize]
		n, err := file.ReadAt(buf, int64(layout.RootOffsets[slot]))
		if err != nil && !errors.Is(err, io.EOF) {
			return InlineSuperblock{}, StateRoot{}, -1, 0, err
		}
		if n < len(buf) {
			clear(buf[n:])
		}
	}
	candidates, count, err := orderedInlineSuperblocks(
		headers[:InlineSuperblockSize], headers[InlineSuperblockSize:],
	)
	if err != nil {
		return InlineSuperblock{}, StateRoot{}, -1, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return InlineSuperblock{}, StateRoot{}, -1, 0, err
	}
	if info.Size() < 0 {
		return InlineSuperblock{}, StateRoot{}, -1, 0, ErrSuperblockNotFound
	}
	fileSize := uint64(info.Size())
	var selected InlineSuperblock
	selectedSlot := -1
	var catalogErr error
	for i := range count {
		candidate := candidates[i]
		root := candidate.root
		state := root.State
		if root.PageSize != pageSize || root.FileEnd > fileSize {
			continue
		}
		refsOK, refsErr := readStateRootRefs(
			file, state, root.FileEnd, pageScratch,
		)
		if refsErr != nil {
			if errors.Is(refsErr, ErrPageCatalogCorrupt) {
				catalogErr = errors.Join(catalogErr, refsErr)
				continue
			}
			return InlineSuperblock{}, StateRoot{}, -1, 0, refsErr
		}
		if !refsOK {
			continue
		}
		indexHead := root.FreeDelta.IndexHead()
		if indexHead != (PageRef{}) {
			buf := pageScratch[:indexHead.Length]
			n, readErr := file.ReadAt(buf, int64(indexHead.Offset))
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return InlineSuperblock{}, StateRoot{}, -1, 0, readErr
			}
			if n != len(buf) {
				continue
			}
			index, indexOpenErr := OpenFreeIndexPage(
				buf, root.FileEnd, state.NextLogicalID,
			)
			indexHeader := index.Header()
			if indexOpenErr != nil || indexHeader.StoreID != root.StoreID ||
				indexHeader.PageSize != root.PageSize ||
				indexHeader.Generation > root.Generation ||
				indexHeader.LogicalID != indexHead.LogicalID ||
				indexHeader.Generation != indexHead.Generation {
				continue
			}
		}
		externalPrev := root.FreeDelta.ExternalPrev()
		if externalPrev != (PageRef{}) {
			buf := pageScratch[:externalPrev.Length]
			n, readErr := file.ReadAt(buf, int64(externalPrev.Offset))
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return InlineSuperblock{}, StateRoot{}, -1, 0, readErr
			}
			if n != len(buf) {
				continue
			}
			free, freeOpenErr := OpenFreeDeltaPage(buf, root.FileEnd, state.NextLogicalID)
			freeHeader := free.Header()
			if freeOpenErr != nil || freeHeader.StoreID != root.StoreID ||
				freeHeader.PageSize != root.PageSize || freeHeader.Generation > root.Generation ||
				freeHeader.LogicalID != externalPrev.LogicalID ||
				freeHeader.Generation != externalPrev.Generation ||
				indexHead != free.IndexHead() {
				continue
			}
		}
		if selectedSlot < 0 {
			selected, selectedSlot = root, candidate.slot
			continue
		}
		return selected, selected.State, selectedSlot, root.Generation, nil
	}
	if selectedSlot >= 0 {
		return selected, selected.State, selectedSlot, selected.Generation, nil
	}
	if catalogErr != nil {
		return InlineSuperblock{}, StateRoot{}, -1, 0,
			errors.Join(ErrSuperblockNotFound, catalogErr)
	}
	return InlineSuperblock{}, StateRoot{}, -1, 0, ErrSuperblockNotFound
}

func orderedInlineSuperblocks(
	first, second []byte,
) ([superblockCopies]inlineSuperblockCandidate, int, error) {
	var candidates [superblockCopies]inlineSuperblockCandidate
	firstRoot, firstErr := DecodeInlineSuperblock(first)
	secondRoot, secondErr := DecodeInlineSuperblock(second)
	count := 0
	if firstErr == nil {
		candidates[count] = inlineSuperblockCandidate{root: firstRoot, slot: 0}
		count++
	}
	if secondErr == nil {
		candidates[count] = inlineSuperblockCandidate{root: secondRoot, slot: 1}
		count++
	}
	if count == 0 {
		return candidates, 0, fmt.Errorf(
			"%w: inline slot 0: %v; inline slot 1: %v",
			ErrSuperblockNotFound, firstErr, secondErr,
		)
	}
	if count == 2 {
		if candidates[0].root.StoreID != candidates[1].root.StoreID ||
			candidates[0].root.PageSize != candidates[1].root.PageSize ||
			!sameImmutableInlineConfiguration(
				candidates[0].root, candidates[1].root,
			) {
			return candidates, 0, ErrSuperblockConflict
		}
		if candidates[0].root.Generation == candidates[1].root.Generation &&
			candidates[0].root != candidates[1].root {
			return candidates, 0, ErrSuperblockConflict
		}
		if candidates[1].root.Generation > candidates[0].root.Generation {
			candidates[0], candidates[1] = candidates[1], candidates[0]
		}
	}
	return candidates, count, nil
}

func sameImmutableInlineConfiguration(
	left, right InlineSuperblock,
) bool {
	return left.State.MaxPageSize == right.State.MaxPageSize &&
		left.State.MaxKeyBytes == right.State.MaxKeyBytes &&
		left.State.InlineValueBytes == right.State.InlineValueBytes &&
		left.State.MaxDocumentBytes == right.State.MaxDocumentBytes &&
		left.State.Options == right.State.Options &&
		left.State.ChunkDocuments == right.State.ChunkDocuments &&
		left.State.IndexCount == right.State.IndexCount &&
		left.State.IndexMaxDepth == right.State.IndexMaxDepth &&
		left.State.IndexCatalogHash == right.State.IndexCatalogHash &&
		left.State.MaterializationDamageGranule ==
			right.State.MaterializationDamageGranule &&
		left.State.PageCatalogHead == right.State.PageCatalogHead &&
		left.State.PageCatalogDigest == right.State.PageCatalogDigest &&
		left.State.PageCatalogBytes == right.State.PageCatalogBytes
}
