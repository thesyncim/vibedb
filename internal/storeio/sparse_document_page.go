package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"unsafe"
)

const (
	// SparseDocumentPagePayloadHeaderSize stores only format magic, ChunkID,
	// and the live bitmap. Count, descriptor width, heap start, damage sector,
	// and used bytes are fixed or derivable and are deliberately not duplicated
	// on disk.
	SparseDocumentPagePayloadHeaderSize = 16
	SparseDocumentPageRecordSize        = 6
	SparseDocumentPageSlotCount         = 64
	// The heap begins immediately after the common header, sparse header, and
	// all 64 descriptors: 64 + 16 + 64*6 = 464.
	SparseDocumentPageHeapStart    = 464
	SparseDocumentPageDamageSector = 512

	sparseDocumentPageMagic = uint32(0x31504453) // "SDP1", little endian.
)

var (
	// ErrSparseDocumentPageCorrupt reports a checksum-valid common page whose
	// sparse document payload is not canonical.
	ErrSparseDocumentPageCorrupt = errors.New("vibejson: corrupt sparse document page")

	// ErrSparseDocumentPageNoSpace lets a materializing writer fall back to
	// ordinary copy-on-write when page-local gaps cannot hold a replacement.
	ErrSparseDocumentPageNoSpace = errors.New("vibejson: sparse document page has no suitable gap")
)

// SparseDocumentPageView is a checksum-admitted stable-slot document page.
// Lookups borrow the one canonical page image and allocate nothing.
type SparseDocumentPageView struct {
	header DocumentPageHeader
	page   []byte
	count  uint8
}

// SparseSectorRange is one contiguous range of changed damage sectors in a
// complete after-image. Offset is relative to the beginning of the page.
type SparseSectorRange struct {
	Offset uint32
	Length uint32
}

// SparseDocumentMutationOptions supplies the state-root bounds needed to
// validate the before-image and the physical damage granule to report.
type SparseDocumentMutationOptions struct {
	TargetGeneration  uint64
	ChunkHighWater    uint32
	NextLogicalID     uint64
	FileEnd           uint64
	AllocationQuantum uint32
	SectorSize        uint32
	AllowOverflow     bool
}

type sparseDocumentInterval struct {
	start uint16
	end   uint16
}

// SparseDocumentWorkspace is bounded writer-only scratch. It avoids a
// per-mutation heap object graph while finding/coalescing gaps, rebuilding a
// fragmented heap, and returning changed sector runs.
//
// A workspace is borrowed by a single writer call. The result's Changed slice
// aliases it and remains valid only until the next call using that workspace.
type SparseDocumentWorkspace struct {
	intervals [SparseDocumentPageSlotCount]sparseDocumentInterval
	changed   [128]SparseSectorRange // 64 KiB maximum page / 512-byte sector.
	candidate [128]bool
}

// SparseDocumentMutationPlan contains a complete, checksum-valid canonical
// after-image and the exact sectors that differ from the before-image.
type SparseDocumentMutationPlan struct {
	After        []byte
	Changed      []SparseSectorRange
	ChangedBytes uint32
}

// EncodeSparseDocumentPage creates a packed initial sparse page. Later
// materialized updates retain each row's heap offset while it still fits and
// leave canonical zero gaps when it does not.
func EncodeSparseDocumentPage(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID uint64) ([]byte, error) {
	return encodeSparseDocumentPage(dst, header, rows, nextLogicalID, 0, 0, false)
}

// EncodeSparseDocumentPageWithOverflow is the overflow-capable form.
// valueLen==0 in the six-byte descriptor is the explicit overflow marker;
// inline JSON is required to be non-empty, so it is unambiguous.
func EncodeSparseDocumentPageWithOverflow(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID, fileEnd uint64, allocationQuantum uint32) ([]byte, error) {
	return encodeSparseDocumentPage(dst, header, rows, nextLogicalID, fileEnd, allocationQuantum, true)
}

func encodeSparseDocumentPage(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) ([]byte, error) {
	if header.PageSize > math.MaxUint16+1 || header.PageSize < SparseDocumentPageHeapStart+PageTrailerSize {
		return nil, fmt.Errorf("%w: sparse page size", ErrInvalidWrite)
	}
	if err := validateDocumentPageHeader(header, len(rows), header.ChunkID+1, nextLogicalID); err != nil {
		return nil, err
	}
	if allowOverflow && (!validPhysicalPageSize(allocationQuantum) ||
		header.PageSize < allocationQuantum || header.PageSize%allocationQuantum != 0) {
		return nil, fmt.Errorf("%w: sparse document allocation geometry", ErrInvalidWrite)
	}

	used := 0
	live := header.Live
	for index := range rows {
		row := &rows[index]
		slot := uint8(bits.TrailingZeros64(live))
		if row.Slot != slot || len(row.Key) > math.MaxUint16 {
			return nil, fmt.Errorf("%w: sparse rows do not match stable slots", ErrInvalidWrite)
		}
		overflow := row.Overflow != (PageRef{})
		valueLength := len(row.JSON)
		if !overflow {
			if valueLength == 0 || valueLength > math.MaxUint16 || row.JSONLength != 0 {
				return nil, fmt.Errorf("%w: sparse inline document value", ErrInvalidWrite)
			}
		} else {
			if !allowOverflow || valueLength != 0 || row.JSONLength == 0 ||
				!validDocumentOverflowRef(header, row.Overflow, fileEnd, nextLogicalID, allocationQuantum) {
				return nil, fmt.Errorf("%w: sparse overflow document value", ErrInvalidWrite)
			}
			valueLength = DocumentOverflowDescriptorSize
		}
		used += len(row.Key) + valueLength
		live &= live - 1
	}
	if used > int(header.PageSize)-SparseDocumentPageHeapStart-PageTrailerSize {
		return nil, fmt.Errorf("%w: sparse document data does not fit", ErrInvalidWrite)
	}

	payloadLength := int(header.PageSize) - PageHeaderSize - PageTrailerSize
	payload, err := InitPage(dst, PageHeader{
		StoreID:       header.StoreID,
		Generation:    header.Generation,
		LogicalID:     header.LogicalID,
		PageSize:      header.PageSize,
		PayloadLength: uint32(payloadLength),
		Kind:          PageDocument,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], sparseDocumentPageMagic)
	binary.LittleEndian.PutUint32(payload[4:8], header.ChunkID)
	binary.LittleEndian.PutUint64(payload[8:16], header.Live)

	page := dst[:int(header.PageSize)]
	cursor := SparseDocumentPageHeapStart
	for rank := range rows {
		row := &rows[rank]
		overflow := row.Overflow != (PageRef{})
		valueLength := len(row.JSON)
		if overflow {
			valueLength = DocumentOverflowDescriptorSize
		}
		recordLength := len(row.Key) + valueLength
		descriptor := PageHeaderSize + SparseDocumentPagePayloadHeaderSize + rank*SparseDocumentPageRecordSize
		binary.LittleEndian.PutUint16(page[descriptor:descriptor+2], uint16(cursor))
		binary.LittleEndian.PutUint16(page[descriptor+2:descriptor+4], uint16(len(row.Key)))
		if !overflow {
			binary.LittleEndian.PutUint16(page[descriptor+4:descriptor+6], uint16(len(row.JSON)))
		}
		copy(page[cursor:cursor+len(row.Key)], row.Key)
		valueStart := cursor + len(row.Key)
		if !overflow {
			copy(page[valueStart:valueStart+len(row.JSON)], row.JSON)
		} else {
			encodePageRef(page[valueStart:valueStart+PageRefSize], row.Overflow)
			binary.LittleEndian.PutUint64(page[valueStart+PageRefSize:valueStart+DocumentOverflowDescriptorSize], row.JSONLength)
		}
		cursor += recordLength
	}
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func sparseDocumentValueLength(header DocumentPageHeader, row DocumentRecord, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) (int, error) {
	if row.Overflow == (PageRef{}) {
		if len(row.JSON) == 0 || len(row.JSON) > math.MaxUint16 || row.JSONLength != 0 {
			return 0, fmt.Errorf("%w: sparse inline document value", ErrInvalidWrite)
		}
		return len(row.JSON), nil
	}
	if !allowOverflow || len(row.JSON) != 0 || row.JSONLength == 0 ||
		!validDocumentOverflowRef(header, row.Overflow, fileEnd, nextLogicalID, allocationQuantum) {
		return 0, fmt.Errorf("%w: sparse overflow document value", ErrInvalidWrite)
	}
	return DocumentOverflowDescriptorSize, nil
}

// OpenSparseDocumentPage verifies the common checksum and the complete sparse
// payload. Repeated point reads then touch this one page only.
func OpenSparseDocumentPage(src []byte, chunkHighWater uint32, nextLogicalID uint64) (SparseDocumentPageView, error) {
	return openSparseDocumentPage(src, chunkHighWater, nextLogicalID, 0, 0, false)
}

// OpenSparseDocumentPageWithOverflow additionally validates every referenced
// overflow head against the selecting state root.
func OpenSparseDocumentPageWithOverflow(src []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32) (SparseDocumentPageView, error) {
	return openSparseDocumentPage(src, chunkHighWater, nextLogicalID, fileEnd, allocationQuantum, true)
}

func openSparseDocumentPage(src []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) (SparseDocumentPageView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return SparseDocumentPageView{}, fmt.Errorf("%w: %w", ErrSparseDocumentPageCorrupt, err)
	}
	if pageHeader.Kind != PageDocument || pageHeader.PageSize > math.MaxUint16+1 ||
		len(payload) != int(pageHeader.PageSize)-PageHeaderSize-PageTrailerSize ||
		len(payload) < SparseDocumentPageHeapStart-PageHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != sparseDocumentPageMagic {
		return SparseDocumentPageView{}, fmt.Errorf("%w: header or geometry", ErrSparseDocumentPageCorrupt)
	}
	if allowOverflow && (!validPhysicalPageSize(allocationQuantum) ||
		pageHeader.PageSize < allocationQuantum || pageHeader.PageSize%allocationQuantum != 0) {
		return SparseDocumentPageView{}, fmt.Errorf("%w: allocation geometry", ErrSparseDocumentPageCorrupt)
	}
	header := DocumentPageHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
		ChunkID: binary.LittleEndian.Uint32(payload[4:8]),
		Live:    binary.LittleEndian.Uint64(payload[8:16]), Flags: pageHeader.Flags,
	}
	count := bits.OnesCount64(header.Live)
	if err := validateDocumentPageHeader(header, count, chunkHighWater, nextLogicalID); err != nil {
		return SparseDocumentPageView{}, fmt.Errorf("%w: %v", ErrSparseDocumentPageCorrupt, err)
	}
	page := src[:int(pageHeader.PageSize)]
	directoryStart := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	directoryEnd := directoryStart + SparseDocumentPageSlotCount*SparseDocumentPageRecordSize
	if !allZero(page[directoryStart+count*SparseDocumentPageRecordSize:directoryEnd]) ||
		!allZero(page[directoryEnd:SparseDocumentPageHeapStart]) {
		return SparseDocumentPageView{}, fmt.Errorf("%w: non-zero reserved directory", ErrSparseDocumentPageCorrupt)
	}

	var occupied [SparseDocumentPageSlotCount]sparseDocumentInterval
	trailer := len(page) - PageTrailerSize
	for rank := range count {
		descriptor := directoryStart + rank*SparseDocumentPageRecordSize
		start := int(binary.LittleEndian.Uint16(page[descriptor : descriptor+2]))
		keyLength := int(binary.LittleEndian.Uint16(page[descriptor+2 : descriptor+4]))
		valueLength := int(binary.LittleEndian.Uint16(page[descriptor+4 : descriptor+6]))
		recordLength := keyLength + valueLength
		if valueLength == 0 {
			if !allowOverflow {
				return SparseDocumentPageView{}, fmt.Errorf("%w: overflow marker", ErrSparseDocumentPageCorrupt)
			}
			recordLength = keyLength + DocumentOverflowDescriptorSize
		}
		end := start + recordLength
		if start < SparseDocumentPageHeapStart || recordLength <= keyLength || end > trailer {
			return SparseDocumentPageView{}, fmt.Errorf("%w: record bounds", ErrSparseDocumentPageCorrupt)
		}
		if valueLength == 0 {
			encoded := page[start+keyLength : end]
			if !pageRefReservedZero(encoded[:PageRefSize]) ||
				binary.LittleEndian.Uint64(encoded[PageRefSize:]) == 0 ||
				!validDocumentOverflowRef(header, decodePageRef(encoded[:PageRefSize]), fileEnd, nextLogicalID, allocationQuantum) {
				return SparseDocumentPageView{}, fmt.Errorf("%w: overflow descriptor", ErrSparseDocumentPageCorrupt)
			}
		}
		occupied[rank] = sparseDocumentInterval{start: uint16(start), end: uint16(end)}
	}
	sortSparseDocumentIntervals(occupied[:count])
	cursor := SparseDocumentPageHeapStart
	for _, interval := range occupied[:count] {
		start, end := int(interval.start), int(interval.end)
		if start < cursor || !allZero(page[cursor:start]) {
			return SparseDocumentPageView{}, fmt.Errorf("%w: overlap or non-zero gap", ErrSparseDocumentPageCorrupt)
		}
		cursor = end
	}
	if !allZero(page[cursor:trailer]) {
		return SparseDocumentPageView{}, fmt.Errorf("%w: non-zero trailing gap", ErrSparseDocumentPageCorrupt)
	}
	return SparseDocumentPageView{header: header, page: page, count: uint8(count)}, nil
}

// AdmittedSparseDocumentPage reconstructs a view after PageCache has already
// verified this exact sparse payload. Calling it on arbitrary bytes is invalid.
func AdmittedSparseDocumentPage(src []byte) SparseDocumentPageView {
	pageHeader, _ := decodePageHeader(src)
	payload := src[PageHeaderSize : PageHeaderSize+int(pageHeader.PayloadLength)]
	return SparseDocumentPageView{
		header: DocumentPageHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			ChunkID: binary.LittleEndian.Uint32(payload[4:8]),
			Live:    binary.LittleEndian.Uint64(payload[8:16]), Flags: pageHeader.Flags,
		},
		page:  src[:int(pageHeader.PageSize)],
		count: uint8(bits.OnesCount64(binary.LittleEndian.Uint64(payload[8:16]))),
	}
}

func sortSparseDocumentIntervals(intervals []sparseDocumentInterval) {
	for index := 1; index < len(intervals); index++ {
		value := intervals[index]
		position := index
		for position > 0 && intervals[position-1].start > value.start {
			intervals[position] = intervals[position-1]
			position--
		}
		intervals[position] = value
	}
}

// Header returns the page identity and stable-slot occupancy.
func (v SparseDocumentPageView) Header() DocumentPageHeader { return v.header }

// Len returns the number of live rows.
func (v SparseDocumentPageView) Len() int { return int(v.count) }

// Lookup returns one borrowed row with one bitmap rank and one six-byte
// descriptor load. It never consults a delta, journal, or tombstone page.
func (v SparseDocumentPageView) Lookup(slot uint8) (DocumentRecord, bool) {
	if slot >= SparseDocumentPageSlotCount {
		return DocumentRecord{}, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return DocumentRecord{}, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	return v.recordAt(rank, slot)
}

// LookupJSON is the allocation-free inline point-read path.
func (v SparseDocumentPageView) LookupJSON(slot uint8) ([]byte, bool) {
	if slot >= SparseDocumentPageSlotCount {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	start, keyLength, valueLength := sparseDocumentDescriptor(v.page, rank)
	if valueLength == 0 {
		return nil, false
	}
	valueStart := start + keyLength
	end := valueStart + valueLength
	return v.page[valueStart:end:end], true
}

// LookupKey verifies the complete key after an index/fingerprint hit.
func (v SparseDocumentPageView) LookupKey(slot uint8, key []byte) ([]byte, bool) {
	if slot >= SparseDocumentPageSlotCount {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	start, keyLength, valueLength := sparseDocumentDescriptor(v.page, rank)
	if valueLength == 0 || !bytes.Equal(v.page[start:start+keyLength], key) {
		return nil, false
	}
	valueStart := start + keyLength
	end := valueStart + valueLength
	return v.page[valueStart:end:end], true
}

// LookupString is the allocation-free string-key form.
func (v SparseDocumentPageView) LookupString(slot uint8, key string) ([]byte, bool) {
	if slot >= SparseDocumentPageSlotCount {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	start, keyLength, valueLength := sparseDocumentDescriptor(v.page, rank)
	if valueLength == 0 || string(v.page[start:start+keyLength]) != key {
		return nil, false
	}
	valueStart := start + keyLength
	end := valueStart + valueLength
	return v.page[valueStart:end:end], true
}

func (v SparseDocumentPageView) recordAt(rank int, slot uint8) (DocumentRecord, bool) {
	if rank < 0 || rank >= int(v.count) {
		return DocumentRecord{}, false
	}
	start, keyLength, valueLength := sparseDocumentDescriptor(v.page, rank)
	keyEnd := start + keyLength
	if valueLength == 0 {
		end := keyEnd + DocumentOverflowDescriptorSize
		encoded := v.page[keyEnd:end]
		return DocumentRecord{
			Key: v.page[start:keyEnd:keyEnd], Overflow: decodePageRef(encoded[:PageRefSize]),
			JSONLength: binary.LittleEndian.Uint64(encoded[PageRefSize:]), Slot: slot,
		}, true
	}
	end := keyEnd + valueLength
	return DocumentRecord{
		Key: v.page[start:keyEnd:keyEnd], JSON: v.page[keyEnd:end:end], Slot: slot,
	}, true
}

// SparseDocumentPageRows is an allocation-free stable-slot scan.
type SparseDocumentPageRows struct {
	page     []byte
	selected uint64
	pending  uint64
	rank     int
	overflow bool
}

// SparseDocumentPageAllRows is the full ordered-scan fast path. Its descriptor
// cursor advances by six bytes per live row, avoiding bitmap rank computation
// while stable slots still come directly from the live word.
type SparseDocumentPageAllRows struct {
	page       []byte
	slots      uint64
	descriptor int
	overflow   bool
}

// AllRows returns the lowest-overhead full stable-slot iterator.
func (v SparseDocumentPageView) AllRows() SparseDocumentPageAllRows {
	return SparseDocumentPageAllRows{
		page: v.page, slots: v.header.Live,
		descriptor: PageHeaderSize + SparseDocumentPagePayloadHeaderSize,
	}
}

// Next returns the next full-scan row.
func (i *SparseDocumentPageAllRows) Next() (slot uint8, key, json []byte, overflow, ok bool) {
	if i == nil || i.slots == 0 {
		return 0, nil, nil, false, false
	}
	bit := i.slots & -i.slots
	i.slots &^= bit
	encoded := binary.LittleEndian.Uint64(i.page[i.descriptor : i.descriptor+8])
	i.descriptor += SparseDocumentPageRecordSize
	start := int(uint16(encoded))
	keyLength := int(uint16(encoded >> 16))
	valueLength := int(uint16(encoded >> 32))
	keyEnd := start + keyLength
	end := keyEnd + valueLength
	i.overflow = valueLength == 0
	if i.overflow {
		end = keyEnd + DocumentOverflowDescriptorSize
	}
	return uint8(bits.TrailingZeros64(bit)),
		i.page[start:keyEnd:keyEnd], i.page[keyEnd:end:end], i.overflow, true
}

// OverflowDescriptor decodes an overflow value returned by Next.
func (i *SparseDocumentPageAllRows) OverflowDescriptor(encoded []byte) (PageRef, uint64, bool) {
	if i == nil || !i.overflow || len(encoded) != DocumentOverflowDescriptorSize {
		return PageRef{}, 0, false
	}
	return decodePageRef(encoded[:PageRefSize]), binary.LittleEndian.Uint64(encoded[PageRefSize:]), true
}

// Rows returns rows selected by mask in stable-slot order.
func (v SparseDocumentPageView) Rows(mask uint64) SparseDocumentPageRows {
	return SparseDocumentPageRows{page: v.page, selected: v.header.Live & mask, pending: v.header.Live}
}

// Next returns the next selected borrowed key/value slices. This deliberately
// avoids returning DocumentRecord's large by-value representation on the scan
// hot path.
func (i *SparseDocumentPageRows) Next() (slot uint8, key, json []byte, overflow, ok bool) {
	if i == nil || i.selected == 0 {
		return 0, nil, nil, false, false
	}
	bit := i.selected & -i.selected
	i.selected &^= bit
	skipped := i.pending & (bit - 1)
	i.rank += bits.OnesCount64(skipped)
	i.pending &^= skipped | bit
	rank := i.rank
	i.rank++
	slot = uint8(bits.TrailingZeros64(bit))
	start, keyLength, valueLength := sparseDocumentDescriptor(i.page, rank)
	keyEnd := start + keyLength
	end := keyEnd + valueLength
	i.overflow = valueLength == 0
	if i.overflow {
		end = keyEnd + DocumentOverflowDescriptorSize
	}
	return slot, i.page[start:keyEnd:keyEnd], i.page[keyEnd:end:end], i.overflow, true
}

// sparseDocumentDescriptor uses one unaligned 64-bit load for the six-byte
// descriptor. The fixed zero tail after descriptor 63 makes the two-byte
// over-read safe even for the final rank.
func sparseDocumentDescriptor(page []byte, rank int) (start, keyLength, valueLength int) {
	offset := PageHeaderSize + SparseDocumentPagePayloadHeaderSize + rank*SparseDocumentPageRecordSize
	encoded := binary.LittleEndian.Uint64(page[offset : offset+8])
	return int(uint16(encoded)), int(uint16(encoded >> 16)), int(uint16(encoded >> 32))
}

// OverflowDescriptor decodes the value returned by Next for an overflow row.
func (i *SparseDocumentPageRows) OverflowDescriptor(encoded []byte) (PageRef, uint64, bool) {
	if i == nil || !i.overflow || len(encoded) != DocumentOverflowDescriptorSize {
		return PageRef{}, 0, false
	}
	return decodePageRef(encoded[:PageRefSize]), binary.LittleEndian.Uint64(encoded[PageRefSize:]), true
}

// PlanSparseDocumentPageUpdate writes a checksum-valid after-image. The key
// and stable slot must remain unchanged; routing or index changes therefore
// cannot be accidentally hidden inside this page-local optimization.
func PlanSparseDocumentPageUpdate(dst, src []byte, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	view, err := openSparseDocumentForMutation(src, options)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return planAdmittedSparseDocumentPageUpdate(dst, view, row, options, workspace)
}

// PlanAdmittedSparseDocumentPageUpdate is the cache-resident fast path. The
// borrowed view must have been admitted once by OpenSparseDocumentPage or the
// page cache; it deliberately avoids checksumming the immutable before-image a
// second time.
func PlanAdmittedSparseDocumentPageUpdate(dst []byte, view SparseDocumentPageView, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	return planAdmittedSparseDocumentPageUpdate(dst, view, row, options, workspace)
}

func planAdmittedSparseDocumentPageUpdate(dst []byte, view SparseDocumentPageView, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if workspace == nil || options.TargetGeneration <= view.header.Generation ||
		len(dst) < len(view.page) || slicesOverlap(dst[:len(view.page)], view.page) {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: sparse update buffers, workspace, or generation", ErrInvalidWrite)
	}
	current, ok := view.Lookup(row.Slot)
	if !ok || !bytes.Equal(current.Key, row.Key) {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: sparse update changes key or missing slot", ErrInvalidWrite)
	}
	valueLength, err := sparseDocumentValueLength(
		view.header,
		row, options.NextLogicalID, options.FileEnd, options.AllocationQuantum, options.AllowOverflow,
	)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	newLength := len(row.Key) + valueLength
	if newLength > math.MaxUint16 {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: sparse replacement length", ErrInvalidWrite)
	}
	rank := bits.OnesCount64(view.header.Live & ((uint64(1) << row.Slot) - 1))
	descriptor := PageHeaderSize + SparseDocumentPagePayloadHeaderSize + rank*SparseDocumentPageRecordSize
	oldStart := int(binary.LittleEndian.Uint16(view.page[descriptor : descriptor+2]))
	oldKeyLength := int(binary.LittleEndian.Uint16(view.page[descriptor+2 : descriptor+4]))
	oldValueLength := int(binary.LittleEndian.Uint16(view.page[descriptor+4 : descriptor+6]))
	if oldValueLength == 0 {
		oldValueLength = DocumentOverflowDescriptorSize
	}
	oldLength := oldKeyLength + oldValueLength
	newStart := oldStart
	if newLength > oldLength {
		newStart, err = sparseDocumentFindGap(view, rank, oldStart, newLength, workspace)
		if err != nil {
			return SparseDocumentMutationPlan{}, err
		}
	}

	page := dst[:len(view.page)]
	copy(page, view.page)
	clear(page[oldStart : oldStart+oldLength])
	copy(page[newStart:newStart+len(row.Key)], row.Key)
	valueStart := newStart + len(row.Key)
	if row.Overflow == (PageRef{}) {
		copy(page[valueStart:valueStart+len(row.JSON)], row.JSON)
	} else {
		encodePageRef(page[valueStart:valueStart+PageRefSize], row.Overflow)
		binary.LittleEndian.PutUint64(page[valueStart+PageRefSize:valueStart+DocumentOverflowDescriptorSize], row.JSONLength)
	}
	binary.LittleEndian.PutUint16(page[descriptor:descriptor+2], uint16(newStart))
	binary.LittleEndian.PutUint16(page[descriptor+2:descriptor+4], uint16(len(row.Key)))
	binary.LittleEndian.PutUint16(page[descriptor+4:descriptor+6], 0)
	if row.Overflow == (PageRef{}) {
		binary.LittleEndian.PutUint16(page[descriptor+4:descriptor+6], uint16(len(row.JSON)))
	}
	if _, err := SealPage(page); err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return sparseDocumentChangedSpanPlan(
		page, view.page, options.SectorSize, workspace,
		oldStart, oldStart+oldLength, newStart, newStart+newLength,
	)
}

// PlanSparseDocumentPageDelete removes a live descriptor and zeroes its heap
// extent. No tombstone survives in the canonical after-image. Deleting the
// final row is rejected so the caller can retire the now-empty page normally.
func PlanSparseDocumentPageDelete(dst, src []byte, slot uint8, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	view, err := openSparseDocumentForMutation(src, options)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return planAdmittedSparseDocumentPageDelete(dst, view, slot, options, workspace)
}

// PlanAdmittedSparseDocumentPageDelete is the cache-resident fast path.
func PlanAdmittedSparseDocumentPageDelete(dst []byte, view SparseDocumentPageView, slot uint8, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	return planAdmittedSparseDocumentPageDelete(dst, view, slot, options, workspace)
}

func planAdmittedSparseDocumentPageDelete(dst []byte, view SparseDocumentPageView, slot uint8, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if workspace == nil || options.TargetGeneration <= view.header.Generation ||
		len(dst) < len(view.page) || slicesOverlap(dst[:len(view.page)], view.page) ||
		view.count <= 1 || slot >= SparseDocumentPageSlotCount {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: sparse delete buffers, workspace, generation, or final row", ErrInvalidWrite)
	}
	bit := uint64(1) << slot
	if view.header.Live&bit == 0 {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: sparse delete missing slot", ErrInvalidWrite)
	}
	rank := bits.OnesCount64(view.header.Live & (bit - 1))
	directoryStart := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	descriptor := directoryStart + rank*SparseDocumentPageRecordSize
	start := int(binary.LittleEndian.Uint16(view.page[descriptor : descriptor+2]))
	keyLength := int(binary.LittleEndian.Uint16(view.page[descriptor+2 : descriptor+4]))
	valueLength := int(binary.LittleEndian.Uint16(view.page[descriptor+4 : descriptor+6]))
	if valueLength == 0 {
		valueLength = DocumentOverflowDescriptorSize
	}
	recordLength := keyLength + valueLength

	page := dst[:len(view.page)]
	copy(page, view.page)
	clear(page[start : start+recordLength])
	lastDescriptor := directoryStart + (int(view.count)-1)*SparseDocumentPageRecordSize
	copy(page[descriptor:lastDescriptor], page[descriptor+SparseDocumentPageRecordSize:lastDescriptor+SparseDocumentPageRecordSize])
	clear(page[lastDescriptor : lastDescriptor+SparseDocumentPageRecordSize])
	liveOffset := PageHeaderSize + 8
	binary.LittleEndian.PutUint64(page[liveOffset:liveOffset+8], view.header.Live&^bit)
	if _, err := SealPage(page); err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return sparseDocumentChangedSpanPlan(
		page, view.page, options.SectorSize, workspace,
		start, start+recordLength, 0, 0,
	)
}

func openSparseDocumentForMutation(src []byte, options SparseDocumentMutationOptions) (SparseDocumentPageView, error) {
	if options.AllowOverflow {
		return OpenSparseDocumentPageWithOverflow(
			src, options.ChunkHighWater, options.NextLogicalID,
			options.FileEnd, options.AllocationQuantum,
		)
	}
	return OpenSparseDocumentPage(src, options.ChunkHighWater, options.NextLogicalID)
}

func sparseDocumentFindGap(view SparseDocumentPageView, excludedRank, oldStart, needed int, workspace *SparseDocumentWorkspace) (int, error) {
	count := 0
	directoryStart := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	for rank := 0; rank < int(view.count); rank++ {
		if rank == excludedRank {
			continue
		}
		descriptor := directoryStart + rank*SparseDocumentPageRecordSize
		start := int(binary.LittleEndian.Uint16(view.page[descriptor : descriptor+2]))
		keyLength := int(binary.LittleEndian.Uint16(view.page[descriptor+2 : descriptor+4]))
		valueLength := int(binary.LittleEndian.Uint16(view.page[descriptor+4 : descriptor+6]))
		if valueLength == 0 {
			valueLength = DocumentOverflowDescriptorSize
		}
		workspace.intervals[count] = sparseDocumentInterval{start: uint16(start), end: uint16(start + keyLength + valueLength)}
		count++
	}
	intervals := workspace.intervals[:count]
	sortSparseDocumentIntervals(intervals)
	trailer := len(view.page) - PageTrailerSize
	bestStart, bestWidth := -1, math.MaxInt
	cursor := SparseDocumentPageHeapStart
	for index := 0; index <= len(intervals); index++ {
		end := trailer
		if index < len(intervals) {
			end = int(intervals[index].start)
		}
		width := end - cursor
		if oldStart >= cursor && oldStart+needed <= end {
			return oldStart, nil
		}
		if width >= needed && width < bestWidth {
			bestStart, bestWidth = cursor, width
		}
		if index < len(intervals) {
			cursor = int(intervals[index].end)
		}
	}
	if bestStart < 0 {
		return 0, ErrSparseDocumentPageNoSpace
	}
	return bestStart, nil
}

// RebuildSparseDocumentPage compacts all gaps using only bounded workspace.
// It is a writer maintenance primitive, not a read-time indirection.
func RebuildSparseDocumentPage(dst, src []byte, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	view, err := openSparseDocumentForMutation(src, options)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	if workspace == nil || options.TargetGeneration <= view.header.Generation ||
		len(dst) < len(view.page) || slicesOverlap(dst[:len(view.page)], view.page) {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: sparse rebuild buffers, workspace, or generation", ErrInvalidWrite)
	}
	if err := rebuildSparseDocumentPageImage(dst, view); err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return sparseDocumentChangedPlan(dst[:len(view.page)], view.page, options.SectorSize, workspace)
}

func rebuildSparseDocumentPageImage(dst []byte, view SparseDocumentPageView) error {
	payload, err := InitPage(dst, PageHeader{
		StoreID: view.header.StoreID, Generation: view.header.Generation,
		LogicalID: view.header.LogicalID, PageSize: view.header.PageSize,
		PayloadLength: uint32(len(view.page) - PageHeaderSize - PageTrailerSize),
		Kind:          PageDocument,
	})
	if err != nil {
		return err
	}
	copy(payload[:SparseDocumentPagePayloadHeaderSize],
		view.page[PageHeaderSize:PageHeaderSize+SparseDocumentPagePayloadHeaderSize])
	page := dst[:len(view.page)]
	cursor := SparseDocumentPageHeapStart
	iterator := view.AllRows()
	rank := 0
	for {
		_, key, value, overflow, ok := iterator.Next()
		if !ok {
			break
		}
		descriptor := PageHeaderSize + SparseDocumentPagePayloadHeaderSize + rank*SparseDocumentPageRecordSize
		binary.LittleEndian.PutUint16(page[descriptor:descriptor+2], uint16(cursor))
		binary.LittleEndian.PutUint16(page[descriptor+2:descriptor+4], uint16(len(key)))
		if !overflow {
			binary.LittleEndian.PutUint16(page[descriptor+4:descriptor+6], uint16(len(value)))
		}
		copy(page[cursor:cursor+len(key)], key)
		copy(page[cursor+len(key):cursor+len(key)+len(value)], value)
		cursor += len(key) + len(value)
		rank++
	}
	_, err = sealInitializedPage(page)
	return err
}

func sparseDocumentChangedSpanPlan(after, before []byte, sectorSize uint32, workspace *SparseDocumentWorkspace, oldStart, oldEnd, newStart, newEnd int) (SparseDocumentMutationPlan, error) {
	if sectorSize < SparseDocumentPageDamageSector || sectorSize&(sectorSize-1) != 0 ||
		len(after) != len(before) || len(after)%int(sectorSize) != 0 ||
		len(after)/int(sectorSize) > len(workspace.changed) {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: sparse damage sector geometry", ErrInvalidWrite)
	}
	sectorBytes := int(sectorSize)
	sectorCount := len(after) / sectorBytes
	clear(workspace.candidate[:sectorCount])
	workspace.candidate[0] = true
	workspace.candidate[sectorCount-1] = true
	mark := func(start, end int) {
		if start < 0 || end <= start || end > len(after) {
			return
		}
		first := start / sectorBytes
		last := (end - 1) / sectorBytes
		for sector := first; sector <= last; sector++ {
			workspace.candidate[sector] = true
		}
	}
	mark(oldStart, oldEnd)
	mark(newStart, newEnd)

	count := 0
	for sector, candidate := range workspace.candidate[:sectorCount] {
		if !candidate {
			continue
		}
		offset := sector * sectorBytes
		end := offset + sectorBytes
		if bytes.Equal(after[offset:end], before[offset:end]) {
			continue
		}
		if count != 0 {
			previous := &workspace.changed[count-1]
			if previous.Offset+previous.Length == uint32(offset) {
				previous.Length += sectorSize
				continue
			}
		}
		workspace.changed[count] = SparseSectorRange{Offset: uint32(offset), Length: sectorSize}
		count++
	}
	return SparseDocumentMutationPlan{
		After: after, Changed: workspace.changed[:count], ChangedBytes: uint32(countChangedBytes(workspace.changed[:count])),
	}, nil
}

func sparseDocumentChangedPlan(after, before []byte, sectorSize uint32, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if sectorSize < SparseDocumentPageDamageSector || sectorSize&(sectorSize-1) != 0 ||
		len(after) != len(before) || len(after)%int(sectorSize) != 0 ||
		len(after)/int(sectorSize) > len(workspace.changed) {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: sparse damage sector geometry", ErrInvalidWrite)
	}
	count := 0
	for offset := 0; offset < len(after); offset += int(sectorSize) {
		end := offset + int(sectorSize)
		if bytes.Equal(after[offset:end], before[offset:end]) {
			continue
		}
		if count != 0 {
			previous := &workspace.changed[count-1]
			if previous.Offset+previous.Length == uint32(offset) {
				previous.Length += sectorSize
				continue
			}
		}
		workspace.changed[count] = SparseSectorRange{Offset: uint32(offset), Length: sectorSize}
		count++
	}
	return SparseDocumentMutationPlan{
		After: after, Changed: workspace.changed[:count], ChangedBytes: uint32(countChangedBytes(workspace.changed[:count])),
	}, nil
}

func countChangedBytes(ranges []SparseSectorRange) uint64 {
	var total uint64
	for _, changed := range ranges {
		total += uint64(changed.Length)
	}
	return total
}

func slicesOverlap(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	leftStart := uintptr(unsafe.Pointer(unsafe.SliceData(left)))
	rightStart := uintptr(unsafe.Pointer(unsafe.SliceData(right)))
	leftEnd := leftStart + uintptr(len(left))
	rightEnd := rightStart + uintptr(len(right))
	return leftStart < rightEnd && rightStart < leftEnd
}
