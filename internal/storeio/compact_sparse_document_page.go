package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
)

const (
	// CompactSparseDocumentPageRecordSize packs one absolute heap offset,
	// key length, and value length into a single little-endian word:
	//
	//   bits  0..11  heap offset
	//   bits 12..19  key length
	//   bits 20..31  value length (zero is the overflow marker)
	//
	// The common 64-byte page header plus the 16-byte sparse header and all 64
	// descriptors consume 336 bytes. That is 272 bytes of payload metadata, or
	// 4.25 bytes per row at full occupancy.
	CompactSparseDocumentPageRecordSize = 4
	CompactSparseDocumentPageHeapStart  = PageHeaderSize +
		SparseDocumentPagePayloadHeaderSize +
		SparseDocumentPageSlotCount*CompactSparseDocumentPageRecordSize
	CompactSparseDocumentPageMaxOffset = 1<<12 - 1
	CompactSparseDocumentPageMaxKey    = 1<<8 - 1
	CompactSparseDocumentPageMaxValue  = 1<<12 - 1
	compactSparseDocumentPageMagic     = uint32(0x32504453) // "SDP2", little endian.
	compactSparseColumnsPageMagic      = uint32(0x33504453) // "SDP3", little endian.
	compactSparseColumnsFooterSize     = 4
)

// SparseDocumentDescriptorFormat identifies the descriptor selected by the
// v2 encoder. Callers that keep the admitted concrete view can avoid a format
// branch on every point lookup or scan row.
type SparseDocumentDescriptorFormat uint8

const (
	SparseDocumentDescriptorCompact4 SparseDocumentDescriptorFormat = iota + 1
	SparseDocumentDescriptorWide6
)

// CompactSparseDocumentPageView is the branch-free admitted view of an SDP2
// page. It supports the same inline and overflow document semantics as SDP1.
type CompactSparseDocumentPageView struct {
	header       DocumentPageHeader
	page         []byte
	count        uint8
	float64Start uint16
	float64Count uint16
}

// SparseDocumentPageV2View is the automatic compact-or-wide read wrapper.
// Performance-sensitive callers can select Compact or Wide once after open
// and retain that concrete view.
type SparseDocumentPageV2View struct {
	Compact CompactSparseDocumentPageView
	Wide    SparseDocumentPageView
	format  SparseDocumentDescriptorFormat
}

// Format returns the admitted descriptor format.
func (v SparseDocumentPageV2View) Format() SparseDocumentDescriptorFormat { return v.format }

// Header returns the page identity and stable-slot occupancy.
func (v SparseDocumentPageV2View) Header() DocumentPageHeader {
	if v.format == SparseDocumentDescriptorCompact4 {
		return v.Compact.Header()
	}
	return v.Wide.Header()
}

// Len returns the number of live rows.
func (v SparseDocumentPageV2View) Len() int {
	if v.format == SparseDocumentDescriptorCompact4 {
		return v.Compact.Len()
	}
	return v.Wide.Len()
}

// Float64ColumnCount returns the number of covering columns in the compact
// representation. The SDP1 fallback has no typed sidecar.
func (v SparseDocumentPageV2View) Float64ColumnCount() int {
	if v.format == SparseDocumentDescriptorCompact4 {
		return v.Compact.Float64ColumnCount()
	}
	return 0
}

// Float64Column returns one compact covering column. SDP1 fallback pages
// return false.
func (v SparseDocumentPageV2View) Float64Column(column int) (DocumentFloat64ColumnView, bool) {
	if v.format == SparseDocumentDescriptorCompact4 {
		return v.Compact.Float64Column(column)
	}
	return DocumentFloat64ColumnView{}, false
}

// LookupJSON is the automatic-format point-read helper. Hot callers should
// retain the concrete Compact or Wide member after checking Format once.
func (v SparseDocumentPageV2View) LookupJSON(slot uint8) ([]byte, bool) {
	if v.format == SparseDocumentDescriptorCompact4 {
		return v.Compact.LookupJSON(slot)
	}
	return v.Wide.LookupJSON(slot)
}

// Lookup returns one borrowed row from either descriptor representation.
func (v SparseDocumentPageV2View) Lookup(slot uint8) (DocumentRecord, bool) {
	if v.format == SparseDocumentDescriptorCompact4 {
		return v.Compact.Lookup(slot)
	}
	return v.Wide.Lookup(slot)
}

// LookupKey verifies the exact byte key in either representation.
func (v SparseDocumentPageV2View) LookupKey(slot uint8, key []byte) ([]byte, bool) {
	if v.format == SparseDocumentDescriptorCompact4 {
		return v.Compact.LookupKey(slot, key)
	}
	return v.Wide.LookupKey(slot, key)
}

// LookupString verifies the exact string key in either representation.
func (v SparseDocumentPageV2View) LookupString(slot uint8, key string) ([]byte, bool) {
	if v.format == SparseDocumentDescriptorCompact4 {
		return v.Compact.LookupString(slot, key)
	}
	return v.Wide.LookupString(slot, key)
}

// EncodeSparseDocumentPageV2 selects the four-byte descriptor when every
// offset and length is exactly representable, and otherwise falls back to the
// existing six-byte SDP1 representation.
func EncodeSparseDocumentPageV2(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID uint64) ([]byte, SparseDocumentDescriptorFormat, error) {
	if compactSparseDocumentRowsFit(header, rows, false) {
		page, err := encodeCompactSparseDocumentPage(
			dst, header, rows, DocumentFloat64Columns{},
			nextLogicalID, 0, 0, false,
		)
		return page, SparseDocumentDescriptorCompact4, err
	}
	page, err := EncodeSparseDocumentPage(dst, header, rows, nextLogicalID)
	return page, SparseDocumentDescriptorWide6, err
}

// EncodeSparseDocumentPageV2WithOverflow is the overflow-capable automatic
// encoder. Overflow descriptors use the same explicit zero value-length marker
// in both formats.
func EncodeSparseDocumentPageV2WithOverflow(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID, fileEnd uint64, allocationQuantum uint32) ([]byte, SparseDocumentDescriptorFormat, error) {
	if compactSparseDocumentRowsFit(header, rows, true) {
		page, err := encodeCompactSparseDocumentPage(
			dst, header, rows, DocumentFloat64Columns{},
			nextLogicalID, fileEnd, allocationQuantum, true,
		)
		return page, SparseDocumentDescriptorCompact4, err
	}
	page, err := EncodeSparseDocumentPageWithOverflow(
		dst, header, rows, nextLogicalID, fileEnd, allocationQuantum,
	)
	return page, SparseDocumentDescriptorWide6, err
}

// EncodeCompactSparseDocumentPage requires the compact representation instead
// of falling back. It is useful when a caller has already separated page-size
// classes and wants a concrete branch-free admitted view.
func EncodeCompactSparseDocumentPage(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID uint64) ([]byte, error) {
	return encodeCompactSparseDocumentPage(
		dst, header, rows, DocumentFloat64Columns{},
		nextLogicalID, 0, 0, false,
	)
}

// EncodeCompactSparseDocumentPageWithOverflow is the overflow-capable compact
// encoder.
func EncodeCompactSparseDocumentPageWithOverflow(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID, fileEnd uint64, allocationQuantum uint32) ([]byte, error) {
	return encodeCompactSparseDocumentPage(
		dst, header, rows, DocumentFloat64Columns{},
		nextLogicalID, fileEnd, allocationQuantum, true,
	)
}

// EncodeCompactSparseDocumentPageWithColumns writes the exact float64
// covering sidecar at the tail of an SDP3 page. The four-byte document
// descriptors and 4.25-byte-per-row directory are unchanged. The optional
// sidecar costs four bytes per page, eight bytes per column mask, and eight
// bytes per present value.
func EncodeCompactSparseDocumentPageWithColumns(dst []byte, header DocumentPageHeader, rows []DocumentRecord, columns DocumentFloat64Columns, nextLogicalID, fileEnd uint64, allocationQuantum uint32) ([]byte, error) {
	if len(columns.Masks) == 0 {
		return nil, fmt.Errorf("%w: compact sparse columns are empty", ErrInvalidWrite)
	}
	return encodeCompactSparseDocumentPage(
		dst, header, rows, columns,
		nextLogicalID, fileEnd, allocationQuantum, true,
	)
}

func compactSparseDocumentRowsFit(header DocumentPageHeader, rows []DocumentRecord, allowOverflow bool) bool {
	if header.PageSize > CompactSparseDocumentPageMaxOffset+1 ||
		header.PageSize < CompactSparseDocumentPageHeapStart+PageTrailerSize {
		return false
	}
	used := 0
	for index := range rows {
		row := &rows[index]
		if len(row.Key) > CompactSparseDocumentPageMaxKey {
			return false
		}
		valueLength := len(row.JSON)
		if row.Overflow != (PageRef{}) {
			if !allowOverflow {
				return false
			}
			valueLength = DocumentOverflowDescriptorSize
		} else if valueLength == 0 || valueLength > CompactSparseDocumentPageMaxValue {
			return false
		}
		used += len(row.Key) + valueLength
	}
	return used <= int(header.PageSize)-CompactSparseDocumentPageHeapStart-PageTrailerSize
}

func encodeCompactSparseDocumentPage(dst []byte, header DocumentPageHeader, rows []DocumentRecord, columns DocumentFloat64Columns, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) ([]byte, error) {
	if header.PageSize > CompactSparseDocumentPageMaxOffset+1 ||
		header.PageSize < CompactSparseDocumentPageHeapStart+PageTrailerSize {
		return nil, fmt.Errorf("%w: compact sparse page size", ErrInvalidWrite)
	}
	if err := validateDocumentPageHeader(header, len(rows), header.ChunkID+1, nextLogicalID); err != nil {
		return nil, err
	}
	if allowOverflow && (!validPhysicalPageSize(allocationQuantum) ||
		header.PageSize < allocationQuantum || header.PageSize%allocationQuantum != 0) {
		return nil, fmt.Errorf("%w: compact sparse allocation geometry", ErrInvalidWrite)
	}

	float64Length, err := compactSparseFloat64WriteLength(header.Live, columns)
	if err != nil {
		return nil, err
	}
	columnsLength := float64Length
	if columnsLength != 0 {
		columnsLength += compactSparseColumnsFooterSize
	}
	used := 0
	live := header.Live
	for index := range rows {
		row := &rows[index]
		slot := uint8(bits.TrailingZeros64(live))
		if row.Slot != slot || len(row.Key) > CompactSparseDocumentPageMaxKey {
			return nil, fmt.Errorf("%w: compact sparse rows do not match stable slots", ErrInvalidWrite)
		}
		overflow := row.Overflow != (PageRef{})
		valueLength := len(row.JSON)
		if !overflow {
			if valueLength == 0 || valueLength > CompactSparseDocumentPageMaxValue ||
				row.JSONLength != 0 {
				return nil, fmt.Errorf("%w: compact sparse inline document value", ErrInvalidWrite)
			}
		} else {
			if !allowOverflow || valueLength != 0 || row.JSONLength == 0 ||
				!validDocumentOverflowRef(
					header, row.Overflow, fileEnd, nextLogicalID, allocationQuantum,
				) {
				return nil, fmt.Errorf("%w: compact sparse overflow document value", ErrInvalidWrite)
			}
			valueLength = DocumentOverflowDescriptorSize
		}
		used += len(row.Key) + valueLength
		live &= live - 1
	}
	if used+columnsLength > int(header.PageSize)-CompactSparseDocumentPageHeapStart-PageTrailerSize {
		return nil, fmt.Errorf("%w: compact sparse document data does not fit", ErrInvalidWrite)
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
	magic := compactSparseDocumentPageMagic
	if float64Length != 0 {
		magic = compactSparseColumnsPageMagic
	}
	binary.LittleEndian.PutUint32(payload[0:4], magic)
	binary.LittleEndian.PutUint32(payload[4:8], header.ChunkID)
	binary.LittleEndian.PutUint64(payload[8:16], header.Live)

	page := dst[:int(header.PageSize)]
	cursor := CompactSparseDocumentPageHeapStart
	for rank := range rows {
		row := &rows[rank]
		overflow := row.Overflow != (PageRef{})
		valueLength := len(row.JSON)
		encodedValueLength := valueLength
		if overflow {
			valueLength = DocumentOverflowDescriptorSize
			encodedValueLength = 0
		}
		descriptor := PageHeaderSize + SparseDocumentPagePayloadHeaderSize +
			rank*CompactSparseDocumentPageRecordSize
		binary.LittleEndian.PutUint32(
			page[descriptor:descriptor+CompactSparseDocumentPageRecordSize],
			packCompactSparseDocumentDescriptor(cursor, len(row.Key), encodedValueLength),
		)
		copy(page[cursor:cursor+len(row.Key)], row.Key)
		valueStart := cursor + len(row.Key)
		if !overflow {
			copy(page[valueStart:valueStart+valueLength], row.JSON)
		} else {
			encodePageRef(page[valueStart:valueStart+PageRefSize], row.Overflow)
			binary.LittleEndian.PutUint64(
				page[valueStart+PageRefSize:valueStart+DocumentOverflowDescriptorSize],
				row.JSONLength,
			)
		}
		cursor += len(row.Key) + valueLength
	}
	if float64Length != 0 {
		trailer := len(page) - PageTrailerSize
		float64Start := trailer - compactSparseColumnsFooterSize - float64Length
		encodeCompactSparseFloat64Columns(page[float64Start:], columns)
		binary.LittleEndian.PutUint16(
			page[trailer-compactSparseColumnsFooterSize:trailer-2],
			uint16(float64Length),
		)
		binary.LittleEndian.PutUint16(page[trailer-2:trailer], uint16(len(columns.Masks)))
	}
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func compactSparseDocumentValueLength(header DocumentPageHeader, row DocumentRecord, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) (int, error) {
	if len(row.Key) > CompactSparseDocumentPageMaxKey {
		return 0, fmt.Errorf("%w: compact sparse key length", ErrInvalidWrite)
	}
	if row.Overflow == (PageRef{}) {
		if len(row.JSON) == 0 || len(row.JSON) > CompactSparseDocumentPageMaxValue ||
			row.JSONLength != 0 {
			return 0, fmt.Errorf("%w: compact sparse inline document value", ErrInvalidWrite)
		}
		return len(row.JSON), nil
	}
	if !allowOverflow || len(row.JSON) != 0 || row.JSONLength == 0 ||
		!validDocumentOverflowRef(header, row.Overflow, fileEnd, nextLogicalID, allocationQuantum) {
		return 0, fmt.Errorf("%w: compact sparse overflow document value", ErrInvalidWrite)
	}
	return DocumentOverflowDescriptorSize, nil
}

func packCompactSparseDocumentDescriptor(start, keyLength, valueLength int) uint32 {
	return uint32(start) | uint32(keyLength)<<12 | uint32(valueLength)<<20
}

func compactSparseDocumentDescriptor(page []byte, rank int) (start, keyLength, valueLength int) {
	offset := PageHeaderSize + SparseDocumentPagePayloadHeaderSize +
		rank*CompactSparseDocumentPageRecordSize
	encoded := binary.LittleEndian.Uint32(page[offset : offset+CompactSparseDocumentPageRecordSize])
	return int(encoded & CompactSparseDocumentPageMaxOffset),
		int((encoded >> 12) & CompactSparseDocumentPageMaxKey),
		int(encoded >> 20)
}

// OpenSparseDocumentPageV2 admits either the compact SDP2 representation or
// the wide SDP1 fallback.
func OpenSparseDocumentPageV2(src []byte, chunkHighWater uint32, nextLogicalID uint64) (SparseDocumentPageV2View, error) {
	if compactSparseDocumentMagic(src) {
		view, err := OpenCompactSparseDocumentPage(src, chunkHighWater, nextLogicalID)
		return SparseDocumentPageV2View{Compact: view, format: SparseDocumentDescriptorCompact4}, err
	}
	view, err := OpenSparseDocumentPage(src, chunkHighWater, nextLogicalID)
	return SparseDocumentPageV2View{Wide: view, format: SparseDocumentDescriptorWide6}, err
}

// OpenSparseDocumentPageV2WithOverflow is the overflow-capable automatic
// opener.
func OpenSparseDocumentPageV2WithOverflow(src []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32) (SparseDocumentPageV2View, error) {
	if compactSparseDocumentMagic(src) {
		view, err := OpenCompactSparseDocumentPageWithOverflow(
			src, chunkHighWater, nextLogicalID, fileEnd, allocationQuantum,
		)
		return SparseDocumentPageV2View{Compact: view, format: SparseDocumentDescriptorCompact4}, err
	}
	view, err := OpenSparseDocumentPageWithOverflow(
		src, chunkHighWater, nextLogicalID, fileEnd, allocationQuantum,
	)
	return SparseDocumentPageV2View{Wide: view, format: SparseDocumentDescriptorWide6}, err
}

func compactSparseDocumentMagic(src []byte) bool {
	if len(src) < PageHeaderSize+4 {
		return false
	}
	magic := binary.LittleEndian.Uint32(src[PageHeaderSize : PageHeaderSize+4])
	return magic == compactSparseDocumentPageMagic ||
		magic == compactSparseColumnsPageMagic
}

// OpenCompactSparseDocumentPage verifies the common checksum and every compact
// payload invariant.
func OpenCompactSparseDocumentPage(src []byte, chunkHighWater uint32, nextLogicalID uint64) (CompactSparseDocumentPageView, error) {
	return openCompactSparseDocumentPage(src, chunkHighWater, nextLogicalID, 0, 0, false)
}

// OpenCompactSparseDocumentPageWithOverflow additionally validates every
// overflow head against the selecting state root.
func OpenCompactSparseDocumentPageWithOverflow(src []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32) (CompactSparseDocumentPageView, error) {
	return openCompactSparseDocumentPage(
		src, chunkHighWater, nextLogicalID, fileEnd, allocationQuantum, true,
	)
}

func openCompactSparseDocumentPage(src []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) (CompactSparseDocumentPageView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return CompactSparseDocumentPageView{}, fmt.Errorf("%w: %w", ErrSparseDocumentPageCorrupt, err)
	}
	magic := uint32(0)
	if len(payload) >= 4 {
		magic = binary.LittleEndian.Uint32(payload[0:4])
	}
	if pageHeader.Kind != PageDocument ||
		pageHeader.PageSize > CompactSparseDocumentPageMaxOffset+1 ||
		len(payload) != int(pageHeader.PageSize)-PageHeaderSize-PageTrailerSize ||
		len(payload) < CompactSparseDocumentPageHeapStart-PageHeaderSize ||
		(magic != compactSparseDocumentPageMagic &&
			magic != compactSparseColumnsPageMagic) {
		return CompactSparseDocumentPageView{}, fmt.Errorf("%w: compact header or geometry", ErrSparseDocumentPageCorrupt)
	}
	if allowOverflow && (!validPhysicalPageSize(allocationQuantum) ||
		pageHeader.PageSize < allocationQuantum || pageHeader.PageSize%allocationQuantum != 0) {
		return CompactSparseDocumentPageView{}, fmt.Errorf("%w: compact allocation geometry", ErrSparseDocumentPageCorrupt)
	}
	header := DocumentPageHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
		ChunkID: binary.LittleEndian.Uint32(payload[4:8]),
		Live:    binary.LittleEndian.Uint64(payload[8:16]), Flags: pageHeader.Flags,
	}
	count := bits.OnesCount64(header.Live)
	if err := validateDocumentPageHeader(header, count, chunkHighWater, nextLogicalID); err != nil {
		return CompactSparseDocumentPageView{}, fmt.Errorf("%w: %v", ErrSparseDocumentPageCorrupt, err)
	}
	page := src[:int(pageHeader.PageSize)]
	float64Start, float64Count, err := openCompactSparseFloat64Columns(
		page, header.Live, magic,
	)
	if err != nil {
		return CompactSparseDocumentPageView{}, err
	}
	directoryStart := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	directoryEnd := directoryStart +
		SparseDocumentPageSlotCount*CompactSparseDocumentPageRecordSize
	if !allZero(page[directoryStart+count*CompactSparseDocumentPageRecordSize:directoryEnd]) ||
		!allZero(page[directoryEnd:CompactSparseDocumentPageHeapStart]) {
		return CompactSparseDocumentPageView{}, fmt.Errorf("%w: compact non-zero reserved directory", ErrSparseDocumentPageCorrupt)
	}

	var occupied [SparseDocumentPageSlotCount]sparseDocumentInterval
	heapEnd := float64Start
	for rank := range count {
		start, keyLength, valueLength := compactSparseDocumentDescriptor(page, rank)
		recordLength := keyLength + valueLength
		if valueLength == 0 {
			if !allowOverflow {
				return CompactSparseDocumentPageView{}, fmt.Errorf("%w: compact overflow marker", ErrSparseDocumentPageCorrupt)
			}
			recordLength = keyLength + DocumentOverflowDescriptorSize
		}
		end := start + recordLength
		if start < CompactSparseDocumentPageHeapStart ||
			recordLength <= keyLength || end > heapEnd {
			return CompactSparseDocumentPageView{}, fmt.Errorf("%w: compact record bounds", ErrSparseDocumentPageCorrupt)
		}
		if valueLength == 0 {
			encoded := page[start+keyLength : end]
			if !pageRefReservedZero(encoded[:PageRefSize]) ||
				binary.LittleEndian.Uint64(encoded[PageRefSize:]) == 0 ||
				!validDocumentOverflowRef(
					header, decodePageRef(encoded[:PageRefSize]),
					fileEnd, nextLogicalID, allocationQuantum,
				) {
				return CompactSparseDocumentPageView{}, fmt.Errorf("%w: compact overflow descriptor", ErrSparseDocumentPageCorrupt)
			}
		}
		occupied[rank] = sparseDocumentInterval{start: uint16(start), end: uint16(end)}
	}
	sortSparseDocumentIntervals(occupied[:count])
	cursor := CompactSparseDocumentPageHeapStart
	for _, interval := range occupied[:count] {
		start, end := int(interval.start), int(interval.end)
		if start < cursor || !allZero(page[cursor:start]) {
			return CompactSparseDocumentPageView{}, fmt.Errorf("%w: compact overlap or non-zero gap", ErrSparseDocumentPageCorrupt)
		}
		cursor = end
	}
	if !allZero(page[cursor:heapEnd]) {
		return CompactSparseDocumentPageView{}, fmt.Errorf("%w: compact non-zero trailing gap", ErrSparseDocumentPageCorrupt)
	}
	return CompactSparseDocumentPageView{
		header: header, page: page, count: uint8(count),
		float64Start: uint16(float64Start), float64Count: float64Count,
	}, nil
}

// AdmittedCompactSparseDocumentPage reconstructs a concrete view after the
// exact compact page image has already been admitted.
func AdmittedCompactSparseDocumentPage(src []byte) CompactSparseDocumentPageView {
	pageHeader, _ := decodePageHeader(src)
	payload := src[PageHeaderSize : PageHeaderSize+int(pageHeader.PayloadLength)]
	page := src[:int(pageHeader.PageSize)]
	float64Start, float64Count := admittedCompactSparseFloat64Columns(
		page, binary.LittleEndian.Uint32(payload[0:4]),
	)
	return CompactSparseDocumentPageView{
		header: DocumentPageHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			ChunkID: binary.LittleEndian.Uint32(payload[4:8]),
			Live:    binary.LittleEndian.Uint64(payload[8:16]), Flags: pageHeader.Flags,
		},
		page: page, count: uint8(bits.OnesCount64(binary.LittleEndian.Uint64(payload[8:16]))),
		float64Start: uint16(float64Start), float64Count: float64Count,
	}
}

// Header returns the page identity and stable-slot occupancy.
func (v CompactSparseDocumentPageView) Header() DocumentPageHeader { return v.header }

// Len returns the number of live rows.
func (v CompactSparseDocumentPageView) Len() int { return int(v.count) }

// Float64ColumnCount returns the number of exact typed covering columns in
// this page. SDP2 pages return zero.
func (v CompactSparseDocumentPageView) Float64ColumnCount() int {
	return int(v.float64Count)
}

// Float64Column returns one borrowed, allocation-free stable-slot column.
func (v CompactSparseDocumentPageView) Float64Column(column int) (DocumentFloat64ColumnView, bool) {
	if column < 0 || column >= int(v.float64Count) {
		return DocumentFloat64ColumnView{}, false
	}
	cursor := int(v.float64Start)
	for current := 0; current < int(v.float64Count); current++ {
		mask := binary.LittleEndian.Uint64(v.page[cursor : cursor+8])
		cursor += 8
		valueBytes := bits.OnesCount64(mask) * 8
		if current == column {
			return DocumentFloat64ColumnView{
				mask:   mask,
				values: v.page[cursor : cursor+valueBytes : cursor+valueBytes],
			}, true
		}
		cursor += valueBytes
	}
	return DocumentFloat64ColumnView{}, false
}

// Lookup returns one borrowed stable-slot row.
func (v CompactSparseDocumentPageView) Lookup(slot uint8) (DocumentRecord, bool) {
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

// LookupJSON is the allocation-free, one-32-bit-descriptor point-read path.
func (v CompactSparseDocumentPageView) LookupJSON(slot uint8) ([]byte, bool) {
	if slot >= SparseDocumentPageSlotCount {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	start, keyLength, valueLength := compactSparseDocumentDescriptor(v.page, rank)
	if valueLength == 0 {
		return nil, false
	}
	valueStart := start + keyLength
	end := valueStart + valueLength
	return v.page[valueStart:end:end], true
}

// LookupKey verifies the complete key after an index/fingerprint hit.
func (v CompactSparseDocumentPageView) LookupKey(slot uint8, key []byte) ([]byte, bool) {
	if slot >= SparseDocumentPageSlotCount {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	start, keyLength, valueLength := compactSparseDocumentDescriptor(v.page, rank)
	if valueLength == 0 || !bytes.Equal(v.page[start:start+keyLength], key) {
		return nil, false
	}
	valueStart := start + keyLength
	end := valueStart + valueLength
	return v.page[valueStart:end:end], true
}

// LookupString is the allocation-free string-key form.
func (v CompactSparseDocumentPageView) LookupString(slot uint8, key string) ([]byte, bool) {
	if slot >= SparseDocumentPageSlotCount {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	start, keyLength, valueLength := compactSparseDocumentDescriptor(v.page, rank)
	if valueLength == 0 || string(v.page[start:start+keyLength]) != key {
		return nil, false
	}
	valueStart := start + keyLength
	end := valueStart + valueLength
	return v.page[valueStart:end:end], true
}

func (v CompactSparseDocumentPageView) recordAt(rank int, slot uint8) (DocumentRecord, bool) {
	if rank < 0 || rank >= int(v.count) {
		return DocumentRecord{}, false
	}
	start, keyLength, valueLength := compactSparseDocumentDescriptor(v.page, rank)
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

// CompactSparseDocumentPageAllRows is the branch-free full ordered iterator.
type CompactSparseDocumentPageAllRows struct {
	page       []byte
	slots      uint64
	descriptor int
	overflow   bool
}

// AllRows returns the lowest-overhead full stable-slot iterator.
func (v CompactSparseDocumentPageView) AllRows() CompactSparseDocumentPageAllRows {
	return CompactSparseDocumentPageAllRows{
		page: v.page, slots: v.header.Live,
		descriptor: PageHeaderSize + SparseDocumentPagePayloadHeaderSize,
	}
}

// Next returns the next full-scan row.
func (i *CompactSparseDocumentPageAllRows) Next() (slot uint8, key, json []byte, overflow, ok bool) {
	if i == nil || i.slots == 0 {
		return 0, nil, nil, false, false
	}
	bit := i.slots & -i.slots
	i.slots &^= bit
	encoded := binary.LittleEndian.Uint32(
		i.page[i.descriptor : i.descriptor+CompactSparseDocumentPageRecordSize],
	)
	i.descriptor += CompactSparseDocumentPageRecordSize
	start := int(encoded & CompactSparseDocumentPageMaxOffset)
	keyLength := int((encoded >> 12) & CompactSparseDocumentPageMaxKey)
	valueLength := int(encoded >> 20)
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
func (i *CompactSparseDocumentPageAllRows) OverflowDescriptor(encoded []byte) (PageRef, uint64, bool) {
	if i == nil || !i.overflow || len(encoded) != DocumentOverflowDescriptorSize {
		return PageRef{}, 0, false
	}
	return decodePageRef(encoded[:PageRefSize]),
		binary.LittleEndian.Uint64(encoded[PageRefSize:]), true
}

// PlanCompactSparseDocumentPageUpdate creates a checksum-valid compact
// after-image while preserving the stable slot and exact key.
func PlanCompactSparseDocumentPageUpdate(dst, src []byte, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	view, err := openCompactSparseDocumentForMutation(src, options)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return planAdmittedCompactSparseDocumentPageUpdate(dst, view, row, options, workspace)
}

// PlanAdmittedCompactSparseDocumentPageUpdate is the admitted compact fast
// path and does not checksum the before-image again.
func PlanAdmittedCompactSparseDocumentPageUpdate(dst []byte, view CompactSparseDocumentPageView, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	return planAdmittedCompactSparseDocumentPageUpdate(dst, view, row, options, workspace)
}

// PlanSparseDocumentPageV2Update dispatches once to the compact or wide
// mutation planner. The returned after-image keeps the page's admitted format.
func PlanSparseDocumentPageV2Update(dst, src []byte, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if compactSparseDocumentMagic(src) {
		return PlanCompactSparseDocumentPageUpdate(dst, src, row, options, workspace)
	}
	return PlanSparseDocumentPageUpdate(dst, src, row, options, workspace)
}

// PlanAdmittedSparseDocumentPageV2Update is the automatic admitted planner.
func PlanAdmittedSparseDocumentPageV2Update(dst []byte, view SparseDocumentPageV2View, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if view.format == SparseDocumentDescriptorCompact4 {
		return PlanAdmittedCompactSparseDocumentPageUpdate(
			dst, view.Compact, row, options, workspace,
		)
	}
	return PlanAdmittedSparseDocumentPageUpdate(
		dst, view.Wide, row, options, workspace,
	)
}

func planAdmittedCompactSparseDocumentPageUpdate(dst []byte, view CompactSparseDocumentPageView, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	return planAdmittedCompactSparseDocumentPageUpdateMode(
		dst, view, row, options, workspace, true,
	)
}

func planAdmittedCompactSparseDocumentPageUpdateMode(dst []byte, view CompactSparseDocumentPageView, row DocumentRecord, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace, finalize bool) (SparseDocumentMutationPlan, error) {
	if workspace == nil || options.TargetGeneration <= view.header.Generation ||
		len(dst) < len(view.page) || slicesOverlap(dst[:len(view.page)], view.page) {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: compact update buffers, workspace, or generation", ErrInvalidWrite)
	}
	if row.Slot >= SparseDocumentPageSlotCount {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: compact update changes key or missing slot", ErrInvalidWrite)
	}
	bit := uint64(1) << row.Slot
	if view.header.Live&bit == 0 {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: compact update changes key or missing slot", ErrInvalidWrite)
	}
	rank := bits.OnesCount64(view.header.Live & (bit - 1))
	oldStart, oldKeyLength, encodedOldValueLength :=
		compactSparseDocumentDescriptor(view.page, rank)
	if oldKeyLength != len(row.Key) ||
		!bytes.Equal(view.page[oldStart:oldStart+oldKeyLength], row.Key) {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: compact update changes key or missing slot", ErrInvalidWrite)
	}
	valueLength, err := compactSparseDocumentValueLength(
		view.header, row, options.NextLogicalID, options.FileEnd,
		options.AllocationQuantum, options.AllowOverflow,
	)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	newLength := len(row.Key) + valueLength
	oldValueLength := encodedOldValueLength
	if oldValueLength == 0 {
		oldValueLength = DocumentOverflowDescriptorSize
	}
	oldLength := oldKeyLength + oldValueLength
	newStart := oldStart
	if newLength > oldLength {
		newStart, err = compactSparseDocumentFindGap(
			view, rank, oldStart, newLength, workspace,
		)
		if err != nil {
			return SparseDocumentMutationPlan{}, err
		}
	}

	page := dst[:len(view.page)]
	copy(page, view.page)
	clear(page[oldStart : oldStart+oldLength])
	copy(page[newStart:newStart+len(row.Key)], row.Key)
	valueStart := newStart + len(row.Key)
	encodedValueLength := len(row.JSON)
	if row.Overflow == (PageRef{}) {
		copy(page[valueStart:valueStart+len(row.JSON)], row.JSON)
	} else {
		encodedValueLength = 0
		encodePageRef(page[valueStart:valueStart+PageRefSize], row.Overflow)
		binary.LittleEndian.PutUint64(
			page[valueStart+PageRefSize:valueStart+DocumentOverflowDescriptorSize],
			row.JSONLength,
		)
	}
	descriptor := PageHeaderSize + SparseDocumentPagePayloadHeaderSize +
		rank*CompactSparseDocumentPageRecordSize
	binary.LittleEndian.PutUint32(
		page[descriptor:descriptor+CompactSparseDocumentPageRecordSize],
		packCompactSparseDocumentDescriptor(newStart, len(row.Key), encodedValueLength),
	)
	if !finalize {
		return SparseDocumentMutationPlan{After: page}, nil
	}
	if _, err := SealPage(page); err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return sparseDocumentChangedSpanPlan(
		page, view.page, options.SectorSize, workspace,
		oldStart, oldStart+oldLength, newStart, newStart+newLength,
	)
}

// PlanCompactSparseDocumentPageDelete removes one stable slot without leaving
// a tombstone.
func PlanCompactSparseDocumentPageDelete(dst, src []byte, slot uint8, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	view, err := openCompactSparseDocumentForMutation(src, options)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return planAdmittedCompactSparseDocumentPageDelete(dst, view, slot, options, workspace)
}

// PlanAdmittedCompactSparseDocumentPageDelete is the admitted compact fast
// path.
func PlanAdmittedCompactSparseDocumentPageDelete(dst []byte, view CompactSparseDocumentPageView, slot uint8, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	return planAdmittedCompactSparseDocumentPageDelete(dst, view, slot, options, workspace)
}

// PlanSparseDocumentPageV2Delete dispatches once to the compact or wide
// no-tombstone delete planner.
func PlanSparseDocumentPageV2Delete(dst, src []byte, slot uint8, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if compactSparseDocumentMagic(src) {
		return PlanCompactSparseDocumentPageDelete(dst, src, slot, options, workspace)
	}
	return PlanSparseDocumentPageDelete(dst, src, slot, options, workspace)
}

// PlanAdmittedSparseDocumentPageV2Delete is the automatic admitted planner.
func PlanAdmittedSparseDocumentPageV2Delete(dst []byte, view SparseDocumentPageV2View, slot uint8, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if view.format == SparseDocumentDescriptorCompact4 {
		return PlanAdmittedCompactSparseDocumentPageDelete(
			dst, view.Compact, slot, options, workspace,
		)
	}
	return PlanAdmittedSparseDocumentPageDelete(
		dst, view.Wide, slot, options, workspace,
	)
}

func planAdmittedCompactSparseDocumentPageDelete(dst []byte, view CompactSparseDocumentPageView, slot uint8, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if workspace == nil || options.TargetGeneration <= view.header.Generation ||
		len(dst) < len(view.page) || slicesOverlap(dst[:len(view.page)], view.page) ||
		view.count <= 1 || slot >= SparseDocumentPageSlotCount {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: compact delete buffers, workspace, generation, or final row", ErrInvalidWrite)
	}
	bit := uint64(1) << slot
	if view.header.Live&bit == 0 {
		return SparseDocumentMutationPlan{}, fmt.Errorf("%w: compact delete missing slot", ErrInvalidWrite)
	}
	rank := bits.OnesCount64(view.header.Live & (bit - 1))
	start, keyLength, valueLength := compactSparseDocumentDescriptor(view.page, rank)
	if valueLength == 0 {
		valueLength = DocumentOverflowDescriptorSize
	}
	recordLength := keyLength + valueLength

	page := dst[:len(view.page)]
	copy(page, view.page)
	clear(page[start : start+recordLength])
	directoryStart := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	descriptor := directoryStart + rank*CompactSparseDocumentPageRecordSize
	lastDescriptor := directoryStart +
		(int(view.count)-1)*CompactSparseDocumentPageRecordSize
	copy(
		page[descriptor:lastDescriptor],
		page[descriptor+CompactSparseDocumentPageRecordSize:lastDescriptor+CompactSparseDocumentPageRecordSize],
	)
	clear(page[lastDescriptor : lastDescriptor+CompactSparseDocumentPageRecordSize])
	liveOffset := PageHeaderSize + 8
	binary.LittleEndian.PutUint64(page[liveOffset:liveOffset+8], view.header.Live&^bit)
	columnStart := 0
	columnEnd := 0
	if view.float64Count != 0 {
		var err error
		columnStart, err = rewriteCompactSparseFloat64Delete(page, view, slot)
		if err != nil {
			return SparseDocumentMutationPlan{}, err
		}
		columnEnd = len(page) - PageTrailerSize
	}
	if _, err := SealPage(page); err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return sparseDocumentChangedSpanPlan(
		page, view.page, options.SectorSize, workspace,
		start, start+recordLength, columnStart, columnEnd,
	)
}

func openCompactSparseDocumentForMutation(src []byte, options SparseDocumentMutationOptions) (CompactSparseDocumentPageView, error) {
	if options.AllowOverflow {
		return OpenCompactSparseDocumentPageWithOverflow(
			src, options.ChunkHighWater, options.NextLogicalID,
			options.FileEnd, options.AllocationQuantum,
		)
	}
	return OpenCompactSparseDocumentPage(
		src, options.ChunkHighWater, options.NextLogicalID,
	)
}

func compactSparseDocumentFindGap(view CompactSparseDocumentPageView, excludedRank, oldStart, needed int, workspace *SparseDocumentWorkspace) (int, error) {
	count := 0
	for rank := 0; rank < int(view.count); rank++ {
		if rank == excludedRank {
			continue
		}
		start, keyLength, valueLength := compactSparseDocumentDescriptor(view.page, rank)
		if valueLength == 0 {
			valueLength = DocumentOverflowDescriptorSize
		}
		workspace.intervals[count] = sparseDocumentInterval{
			start: uint16(start), end: uint16(start + keyLength + valueLength),
		}
		count++
	}
	intervals := workspace.intervals[:count]
	sortSparseDocumentIntervals(intervals)
	trailer := int(view.float64Start)
	bestStart, bestWidth := -1, math.MaxInt
	cursor := CompactSparseDocumentPageHeapStart
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
