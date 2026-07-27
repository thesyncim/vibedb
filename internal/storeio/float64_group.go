package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
)

const (
	Float64GroupPayloadHeaderSize = 48
	Float64GroupChunkSize         = 8

	float64GroupVersion = DevelopmentFormatVersion
	// DocumentGroupFlagFloat64Sidecar means a document-group reference derives
	// an independently checksummed, column-major PageFloat64Group. The
	// remaining known flag bits encode log2(sidecar pages), while PageRef.Aux
	// holds bounded physical and logical deltas. Many adjacent document
	// groups can therefore share one typed extent without a second tree.
	DocumentGroupFlagFloat64Sidecar     = uint16(1 << 0)
	documentGroupFloat64OrderShift      = 1
	documentGroupFloat64OrderMask       = uint16(0x0f << documentGroupFloat64OrderShift)
	documentGroupFloat64LogicalShift    = 5
	documentGroupFloat64LogicalHighMask = uint16(0x07 << documentGroupFloat64LogicalShift)

	documentGroupFloat64OffsetBits     = 11
	documentGroupFloat64OffsetMask     = uint16(1<<documentGroupFloat64OffsetBits - 1)
	documentGroupFloat64LogicalLowMask = uint16(0x1f)

	float64GroupDirectoryEndMask = uint32(1<<30 - 1)
)

// DocumentGroupFloat64MaxForwardBytes returns the largest physical distance
// representable by a document-group sidecar derivation. Zero reports an
// invalid allocation quantum.
func DocumentGroupFloat64MaxForwardBytes(allocationQuantum uint32) uint64 {
	if !validPhysicalPageSize(allocationQuantum) {
		return 0
	}
	return uint64(documentGroupFloat64OffsetMask) * uint64(allocationQuantum)
}

// Float64GroupEncoding is the exact dense encoding of one detached column.
// Unsigned widths are selected only when every finite JSON number in the
// extent round-trips exactly; all other columns retain IEEE float64 bytes.
type Float64GroupEncoding uint8

const (
	Float64GroupFloat64LE Float64GroupEncoding = iota
	Float64GroupUint8
	Float64GroupUint16
	Float64GroupUint32
)

// ByteWidth returns the dense bytes occupied by one encoded value.
func (e Float64GroupEncoding) ByteWidth() int {
	switch e {
	case Float64GroupUint8:
		return 1
	case Float64GroupUint16:
		return 2
	case Float64GroupUint32:
		return 4
	case Float64GroupFloat64LE:
		return 8
	default:
		return 0
	}
}

// ErrFloat64GroupCorrupt reports a checksum-valid detached covering extent
// whose chunk, column-directory, validity-mask, or finite-value invariant is
// invalid.
var ErrFloat64GroupCorrupt = errors.New("vibejson: corrupt Store float64 group")

// Float64GroupHeader identifies one immutable typed sidecar. Its coverage may
// span several adjacent document groups. Values are laid out column-major so
// a one-column reduction does not read unrelated JSON or unrelated columns.
type Float64GroupHeader struct {
	StoreID     [16]byte
	Generation  uint64
	LogicalID   uint64
	PageSize    uint32
	FirstChunk  uint32
	ChunkCount  uint16
	RowCount    uint16
	ColumnCount uint16
}

// AttachDocumentGroupFloat64Sidecar validates one bounded forward derivation
// and returns the document-group reference with routing bits set. Adjacent
// document groups within the encoded 2,047-page/256-logical-id window may
// derive the same sidecar without admitting a document extent or walking a
// second persistent tree.
func AttachDocumentGroupFloat64Sidecar(group, sidecar PageRef, allocationQuantum uint32) (PageRef, error) {
	if group.Kind != PageDocumentGroup || group.Flags != 0 ||
		group.Aux != 0 ||
		sidecar.Kind != PageFloat64Group || sidecar.Flags != 0 ||
		sidecar.Aux != 0 ||
		!validPhysicalPageSize(allocationQuantum) ||
		sidecar.Generation != group.Generation ||
		sidecar.LogicalID <= group.LogicalID ||
		sidecar.Offset <= group.Offset ||
		(sidecar.Offset-group.Offset)%uint64(allocationQuantum) != 0 ||
		sidecar.Length < allocationQuantum || sidecar.Length%allocationQuantum != 0 {
		return PageRef{}, fmt.Errorf("%w: float64 sidecar identity or order", ErrInvalidWrite)
	}
	offsetPages := (sidecar.Offset - group.Offset) / uint64(allocationQuantum)
	logicalDelta := sidecar.LogicalID - group.LogicalID
	if offsetPages == 0 || offsetPages > uint64(documentGroupFloat64OffsetMask) ||
		logicalDelta == 0 || logicalDelta > 256 {
		return PageRef{}, fmt.Errorf("%w: float64 sidecar delta", ErrInvalidWrite)
	}
	pages := sidecar.Length / allocationQuantum
	if pages == 0 || pages&(pages-1) != 0 {
		return PageRef{}, fmt.Errorf("%w: float64 sidecar size", ErrInvalidWrite)
	}
	order := bits.TrailingZeros32(pages)
	if order > int(documentGroupFloat64OrderMask>>documentGroupFloat64OrderShift) {
		return PageRef{}, fmt.Errorf("%w: float64 sidecar order", ErrInvalidWrite)
	}
	logical := uint16(logicalDelta - 1)
	group.Flags = uint8(DocumentGroupFlagFloat64Sidecar |
		uint16(order)<<documentGroupFloat64OrderShift |
		(logical>>5)<<documentGroupFloat64LogicalShift)
	group.Aux = uint16(offsetPages) |
		(logical&documentGroupFloat64LogicalLowMask)<<documentGroupFloat64OffsetBits
	return group, nil
}

// DocumentGroupFloat64Sidecar derives the independently checksummed typed
// extent named by group. The boolean is false only when group has no sidecar;
// malformed encoded flags return ErrDocumentGroupCorrupt.
func DocumentGroupFloat64Sidecar(group PageRef, allocationQuantum uint32) (PageRef, bool, error) {
	flags := uint16(group.Flags)
	if flags == 0 {
		if group.Aux != 0 {
			return PageRef{}, false, ErrDocumentGroupCorrupt
		}
		return PageRef{}, false, nil
	}
	if group.Kind != PageDocumentGroup || !validPhysicalPageSize(allocationQuantum) ||
		flags&^documentGroupKnownFlags != 0 ||
		flags&DocumentGroupFlagFloat64Sidecar == 0 ||
		group.Aux == 0 {
		return PageRef{}, false, ErrDocumentGroupCorrupt
	}
	order := (flags & documentGroupFloat64OrderMask) >> documentGroupFloat64OrderShift
	if order >= 32 {
		return PageRef{}, false, ErrDocumentGroupCorrupt
	}
	length := uint64(allocationQuantum) << order
	if length > math.MaxUint32 || !validPhysicalPageSize(uint32(length)) {
		return PageRef{}, false, ErrDocumentGroupCorrupt
	}
	offsetPages := uint64(group.Aux & documentGroupFloat64OffsetMask)
	logical := uint64(group.Aux >> documentGroupFloat64OffsetBits)
	logical |= uint64(
		(flags&documentGroupFloat64LogicalHighMask)>>documentGroupFloat64LogicalShift,
	) << 5
	logicalDelta := logical + 1
	offsetDelta := offsetPages * uint64(allocationQuantum)
	if offsetPages == 0 || offsetDelta > math.MaxUint64-group.Offset ||
		logicalDelta > math.MaxUint64-group.LogicalID {
		return PageRef{}, false, ErrDocumentGroupCorrupt
	}
	return PageRef{
		Offset: group.Offset + offsetDelta, LogicalID: group.LogicalID + logicalDelta,
		Generation: group.Generation, Length: uint32(length), Kind: PageFloat64Group,
	}, true, nil
}

// Float64GroupSize returns the smallest power-of-two physical extent that can
// hold the typed columns in chunks. It returns false for absent or malformed
// columns. The calculation performs no allocation.
func Float64GroupSize(chunks []DocumentGroupChunk, allocationQuantum uint32) (uint32, bool) {
	payload, _, _, ok := float64GroupLayout(chunks)
	if !ok || !validPhysicalPageSize(allocationQuantum) {
		return 0, false
	}
	required := uint64(PageHeaderSize) + uint64(payload) + PageTrailerSize
	size := uint64(allocationQuantum)
	for size < required {
		size <<= 1
		if size > math.MaxUint32 {
			return 0, false
		}
	}
	return uint32(size), validPhysicalPageSize(uint32(size))
}

// EncodeFloat64Group writes a complete column-major sidecar into caller-owned
// storage. Chunk validity words are stored once; each column then contains one
// mask and its dense finite values per chunk. No allocation is performed.
func EncodeFloat64Group(dst []byte, header Float64GroupHeader, chunks []DocumentGroupChunk, nextLogicalID uint64) ([]byte, error) {
	payloadBytes, rows, columns, ok := float64GroupLayout(chunks)
	if !ok || header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID <= StateRootLogicalID || header.LogicalID >= nextLogicalID ||
		!validPhysicalPageSize(header.PageSize) ||
		header.FirstChunk != chunks[0].ChunkID ||
		int(header.ChunkCount) != len(chunks) || int(header.RowCount) != rows ||
		int(header.ColumnCount) != columns ||
		uint64(payloadBytes) > uint64(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return nil, fmt.Errorf("%w: float64 group header or layout", ErrInvalidWrite)
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadBytes), Kind: PageFloat64Group,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], float64GroupVersion)
	binary.LittleEndian.PutUint32(payload[4:8], header.FirstChunk)
	binary.LittleEndian.PutUint16(payload[8:10], header.ChunkCount)
	binary.LittleEndian.PutUint16(payload[10:12], header.ColumnCount)
	binary.LittleEndian.PutUint16(payload[12:14], header.RowCount)
	binary.LittleEndian.PutUint16(payload[14:16], Float64GroupChunkSize)
	chunkBytes := len(chunks) * Float64GroupChunkSize
	directoryBytes := columns * 4
	binary.LittleEndian.PutUint32(payload[16:20], uint32(chunkBytes))
	binary.LittleEndian.PutUint32(payload[20:24], uint32(directoryBytes))
	binary.LittleEndian.PutUint32(payload[24:28], uint32(payloadBytes-Float64GroupPayloadHeaderSize-chunkBytes-directoryBytes))
	chunkStart := Float64GroupPayloadHeaderSize
	directoryStart := chunkStart + chunkBytes
	dataStart := directoryStart + directoryBytes
	for ordinal, chunk := range chunks {
		binary.LittleEndian.PutUint64(
			payload[chunkStart+ordinal*Float64GroupChunkSize:],
			chunk.Live,
		)
	}
	cursor := dataStart
	for column := 0; column < columns; column++ {
		encoding := float64GroupColumnEncoding(chunks, column)
		width := encoding.ByteWidth()
		for _, chunk := range chunks {
			mask := chunk.Columns.Masks[column]
			binary.LittleEndian.PutUint64(payload[cursor:cursor+8], mask)
			cursor += 8
		}
		for _, chunk := range chunks {
			mask := chunk.Columns.Masks[column]
			base := column * 64
			for slots := mask; slots != 0; slots &= slots - 1 {
				slot := bits.TrailingZeros64(slots)
				value := chunk.Columns.Values[base+slot]
				switch encoding {
				case Float64GroupUint8:
					payload[cursor] = byte(value)
				case Float64GroupUint16:
					binary.LittleEndian.PutUint16(payload[cursor:cursor+2], uint16(value))
				case Float64GroupUint32:
					binary.LittleEndian.PutUint32(payload[cursor:cursor+4], uint32(value))
				default:
					binary.LittleEndian.PutUint64(payload[cursor:cursor+8], math.Float64bits(value))
				}
				cursor += width
			}
		}
		binary.LittleEndian.PutUint32(
			payload[directoryStart+column*4:directoryStart+(column+1)*4],
			uint32(cursor-dataStart)|uint32(encoding)<<30,
		)
	}
	if cursor != len(payload) {
		return nil, fmt.Errorf("%w: float64 group encoding drift", ErrInvalidWrite)
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func float64GroupLayout(chunks []DocumentGroupChunk) (payload, rows, columns int, ok bool) {
	if len(chunks) < 2 || len(chunks) > math.MaxUint16 {
		return 0, 0, 0, false
	}
	first := chunks[0].ChunkID
	columns = len(chunks[0].Columns.Masks)
	if columns == 0 || columns > math.MaxUint16 {
		return 0, 0, 0, false
	}
	dataBytes := uint64(len(chunks) * columns * 8)
	for ordinal, chunk := range chunks {
		if chunk.ChunkID != first+uint32(ordinal) || chunk.Live == 0 ||
			len(chunk.Columns.Masks) != columns ||
			len(chunk.Columns.Values) != columns*64 {
			return 0, 0, 0, false
		}
		rows += bits.OnesCount64(chunk.Live)
		if rows > math.MaxUint16 {
			return 0, 0, 0, false
		}
		for column, mask := range chunk.Columns.Masks {
			if mask&^chunk.Live != 0 {
				return 0, 0, 0, false
			}
			base := column * 64
			for slots := mask; slots != 0; slots &= slots - 1 {
				value := chunk.Columns.Values[base+bits.TrailingZeros64(slots)]
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return 0, 0, 0, false
				}
			}
		}
	}
	for column := 0; column < columns; column++ {
		width := float64GroupColumnEncoding(chunks, column).ByteWidth()
		for _, chunk := range chunks {
			dataBytes += uint64(bits.OnesCount64(chunk.Columns.Masks[column]) * width)
		}
	}
	payload64 := uint64(Float64GroupPayloadHeaderSize) +
		uint64(len(chunks))*Float64GroupChunkSize + uint64(columns)*4 + dataBytes
	if payload64 > math.MaxUint32 || payload64 > uint64(^uint(0)>>1) {
		return 0, 0, 0, false
	}
	return int(payload64), rows, columns, true
}

// Float64GroupView is one admitted detached covering extent. It borrows one
// page and retains no pointer per chunk, column, or value.
type Float64GroupView struct {
	header         Float64GroupHeader
	payload        []byte
	chunkStart     int
	directoryStart int
	dataStart      int
}

// OpenFloat64Group verifies the common page and complete typed payload.
func OpenFloat64Group(src []byte, chunkHighWater uint32, nextLogicalID uint64) (Float64GroupView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return Float64GroupView{}, fmt.Errorf("%w: %w", ErrFloat64GroupCorrupt, err)
	}
	return openFloat64GroupPayload(pageHeader, payload, chunkHighWater, nextLogicalID)
}

// OpenAdmittedFloat64Group validates the typed payload after PageCache has
// already admitted the common envelope and CRC32C.
func OpenAdmittedFloat64Group(src []byte, chunkHighWater uint32, nextLogicalID uint64) (Float64GroupView, error) {
	pageHeader, ok := decodePageHeader(src)
	if !ok || len(src) != int(pageHeader.PageSize) {
		return Float64GroupView{}, fmt.Errorf("%w: admitted common header", ErrFloat64GroupCorrupt)
	}
	end := PageHeaderSize + int(pageHeader.PayloadLength)
	return openFloat64GroupPayload(pageHeader, src[PageHeaderSize:end:end], chunkHighWater, nextLogicalID)
}

// AdmittedFloat64Group reconstructs a view after one-time cache admission.
func AdmittedFloat64Group(src []byte) Float64GroupView {
	pageHeader, _ := decodePageHeader(src)
	end := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:end:end]
	chunkBytes := int(binary.LittleEndian.Uint32(payload[16:20]))
	directoryBytes := int(binary.LittleEndian.Uint32(payload[20:24]))
	return Float64GroupView{
		header: Float64GroupHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			FirstChunk:  binary.LittleEndian.Uint32(payload[4:8]),
			ChunkCount:  binary.LittleEndian.Uint16(payload[8:10]),
			ColumnCount: binary.LittleEndian.Uint16(payload[10:12]),
			RowCount:    binary.LittleEndian.Uint16(payload[12:14]),
		},
		payload: payload, chunkStart: Float64GroupPayloadHeaderSize,
		directoryStart: Float64GroupPayloadHeaderSize + chunkBytes,
		dataStart:      Float64GroupPayloadHeaderSize + chunkBytes + directoryBytes,
	}
}

func openFloat64GroupPayload(pageHeader PageHeader, payload []byte, chunkHighWater uint32, nextLogicalID uint64) (Float64GroupView, error) {
	if pageHeader.Kind != PageFloat64Group || pageHeader.Flags != 0 ||
		len(payload) < Float64GroupPayloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != float64GroupVersion ||
		binary.LittleEndian.Uint16(payload[14:16]) != Float64GroupChunkSize ||
		!allZero(payload[28:Float64GroupPayloadHeaderSize]) {
		return Float64GroupView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrFloat64GroupCorrupt)
	}
	chunks := int(binary.LittleEndian.Uint16(payload[8:10]))
	columns := int(binary.LittleEndian.Uint16(payload[10:12]))
	rows := int(binary.LittleEndian.Uint16(payload[12:14]))
	first := binary.LittleEndian.Uint32(payload[4:8])
	chunkBytes64 := uint64(binary.LittleEndian.Uint32(payload[16:20]))
	directoryBytes64 := uint64(binary.LittleEndian.Uint32(payload[20:24]))
	dataBytes64 := uint64(binary.LittleEndian.Uint32(payload[24:28]))
	if chunks < 2 || columns == 0 || rows == 0 ||
		chunkBytes64 != uint64(chunks)*Float64GroupChunkSize ||
		directoryBytes64 != uint64(columns)*4 ||
		uint64(first)+uint64(chunks) > uint64(chunkHighWater) ||
		pageHeader.LogicalID <= StateRootLogicalID || pageHeader.LogicalID >= nextLogicalID ||
		chunkBytes64+directoryBytes64 > uint64(len(payload)-Float64GroupPayloadHeaderSize) ||
		dataBytes64 != uint64(len(payload)-Float64GroupPayloadHeaderSize)-chunkBytes64-directoryBytes64 {
		return Float64GroupView{}, fmt.Errorf("%w: identity, counts, or section sizes", ErrFloat64GroupCorrupt)
	}
	chunkBytes := int(chunkBytes64)
	directoryBytes := int(directoryBytes64)
	view := Float64GroupView{
		header: Float64GroupHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			FirstChunk: first, ChunkCount: uint16(chunks),
			RowCount: uint16(rows), ColumnCount: uint16(columns),
		},
		payload: payload, chunkStart: Float64GroupPayloadHeaderSize,
		directoryStart: Float64GroupPayloadHeaderSize + chunkBytes,
		dataStart:      Float64GroupPayloadHeaderSize + chunkBytes + directoryBytes,
	}
	if !view.valid() {
		return Float64GroupView{}, fmt.Errorf("%w: chunk, directory, mask, or value", ErrFloat64GroupCorrupt)
	}
	return view, nil
}

func (v Float64GroupView) valid() bool {
	rows := 0
	for chunk := 0; chunk < int(v.header.ChunkCount); chunk++ {
		live := binary.LittleEndian.Uint64(v.payload[v.chunkStart+chunk*Float64GroupChunkSize:])
		if live == 0 {
			return false
		}
		rows += bits.OnesCount64(live)
	}
	if rows != int(v.header.RowCount) {
		return false
	}
	cursor := v.dataStart
	previous := uint32(0)
	for column := 0; column < int(v.header.ColumnCount); column++ {
		entry := binary.LittleEndian.Uint32(
			v.payload[v.directoryStart+column*4 : v.directoryStart+(column+1)*4],
		)
		end := entry & float64GroupDirectoryEndMask
		encoding := Float64GroupEncoding(entry >> 30)
		width := encoding.ByteWidth()
		if end <= previous || uint64(end) > uint64(len(v.payload)-v.dataStart) {
			return false
		}
		limit := v.dataStart + int(end)
		masksStart := cursor
		valuesStart := masksStart + int(v.header.ChunkCount)*8
		if valuesStart > limit {
			return false
		}
		cursor = valuesStart
		for chunk := 0; chunk < int(v.header.ChunkCount); chunk++ {
			live := binary.LittleEndian.Uint64(v.payload[v.chunkStart+chunk*Float64GroupChunkSize:])
			maskOffset := masksStart + chunk*8
			mask := binary.LittleEndian.Uint64(v.payload[maskOffset : maskOffset+8])
			if mask&^live != 0 {
				return false
			}
			valueBytes := bits.OnesCount64(mask) * width
			if cursor+valueBytes > limit {
				return false
			}
			if encoding == Float64GroupFloat64LE {
				for valueEnd := cursor + valueBytes; cursor < valueEnd; cursor += 8 {
					value := math.Float64frombits(binary.LittleEndian.Uint64(v.payload[cursor : cursor+8]))
					if math.IsNaN(value) || math.IsInf(value, 0) {
						return false
					}
				}
			} else {
				cursor += valueBytes
			}
		}
		if cursor != limit {
			return false
		}
		previous = end
	}
	return cursor == len(v.payload)
}

// Header returns the value-only group identity and coverage.
func (v Float64GroupView) Header() Float64GroupHeader { return v.header }

// Chunk returns one allocation-free logical chunk view.
func (v Float64GroupView) Chunk(chunkID uint32) (Float64GroupChunkView, bool) {
	if chunkID < v.header.FirstChunk ||
		uint64(chunkID-v.header.FirstChunk) >= uint64(v.header.ChunkCount) {
		return Float64GroupChunkView{}, false
	}
	ordinal := int(chunkID - v.header.FirstChunk)
	return Float64GroupChunkView{
		group: v, ordinal: ordinal,
		live: binary.LittleEndian.Uint64(v.payload[v.chunkStart+ordinal*Float64GroupChunkSize:]),
	}, true
}

// Float64GroupChunkView addresses one logical stable-slot chunk in a detached
// column-major sidecar.
type Float64GroupChunkView struct {
	group   Float64GroupView
	ordinal int
	live    uint64
}

// Float64ColumnCount returns the complete frozen covering catalog width.
func (v Float64GroupChunkView) Float64ColumnCount() int {
	return int(v.group.header.ColumnCount)
}

// Live returns the stable-slot live mask covered by this typed chunk.
func (v Float64GroupChunkView) Live() uint64 { return v.live }

// GroupHeader returns the value-only identity and complete shared coverage of
// the typed extent backing this chunk.
func (v Float64GroupChunkView) GroupHeader() Float64GroupHeader {
	return v.group.header
}

// Float64Column returns one borrowed typed covering column for this chunk.
func (v Float64GroupChunkView) Float64Column(column int) (DocumentFloat64ColumnView, bool) {
	if column < 0 || column >= int(v.group.header.ColumnCount) {
		return DocumentFloat64ColumnView{}, false
	}
	start := uint32(0)
	if column != 0 {
		start = binary.LittleEndian.Uint32(
			v.group.payload[v.group.directoryStart+(column-1)*4:v.group.directoryStart+column*4],
		) & float64GroupDirectoryEndMask
	}
	entry := binary.LittleEndian.Uint32(
		v.group.payload[v.group.directoryStart+column*4 : v.group.directoryStart+(column+1)*4],
	)
	encoding := Float64GroupEncoding(entry >> 30)
	width := encoding.ByteWidth()
	masksStart := v.group.dataStart + int(start)
	valuesStart := masksStart + int(v.group.header.ChunkCount)*8
	valueOffset := 0
	for chunk := 0; chunk < v.ordinal; chunk++ {
		maskOffset := masksStart + chunk*8
		mask := binary.LittleEndian.Uint64(v.group.payload[maskOffset : maskOffset+8])
		valueOffset += bits.OnesCount64(mask) * width
	}
	maskOffset := masksStart + v.ordinal*8
	mask := binary.LittleEndian.Uint64(v.group.payload[maskOffset : maskOffset+8])
	valueBytes := bits.OnesCount64(mask) * width
	return DocumentFloat64ColumnView{
		mask: mask, encoding: encoding,
		values: v.group.payload[valuesStart+valueOffset : valuesStart+valueOffset+valueBytes : valuesStart+valueOffset+valueBytes],
	}, true
}

// Float64ColumnValues returns every densely encoded value for column in
// stable chunk/slot order. It is the full-group reduction path: masks remain
// separately addressable while the hot numeric bytes form one contiguous
// vectorizable run.
func (v Float64GroupView) Float64ColumnValues(column int) ([]byte, Float64GroupEncoding, bool) {
	return v.Float64ColumnRangeValues(column, v.header.FirstChunk, uint32(v.header.ChunkCount))
}

// Float64ColumnRangeValues returns one contiguous stable-order value run for
// a chunk subrange of column. The format stores masks before values, so
// selecting a document-group run only counts bounded masks and never copies.
func (v Float64GroupView) Float64ColumnRangeValues(column int, first, count uint32) ([]byte, Float64GroupEncoding, bool) {
	if column < 0 || column >= int(v.header.ColumnCount) {
		return nil, 0, false
	}
	if count == 0 || first < v.header.FirstChunk {
		return nil, 0, false
	}
	ordinal := uint64(first - v.header.FirstChunk)
	if ordinal+uint64(count) > uint64(v.header.ChunkCount) {
		return nil, 0, false
	}
	start := uint32(0)
	if column != 0 {
		start = binary.LittleEndian.Uint32(
			v.payload[v.directoryStart+(column-1)*4:v.directoryStart+column*4],
		) & float64GroupDirectoryEndMask
	}
	entry := binary.LittleEndian.Uint32(
		v.payload[v.directoryStart+column*4 : v.directoryStart+(column+1)*4],
	)
	end := entry & float64GroupDirectoryEndMask
	encoding := Float64GroupEncoding(entry >> 30)
	width := encoding.ByteWidth()
	masksStart := v.dataStart + int(start)
	valuesStart := masksStart + int(v.header.ChunkCount)*8
	valueOffset := 0
	for chunk := uint64(0); chunk < ordinal; chunk++ {
		mask := binary.LittleEndian.Uint64(v.payload[masksStart+int(chunk)*8:])
		valueOffset += bits.OnesCount64(mask) * width
	}
	valueBytes := 0
	for chunk := ordinal; chunk < ordinal+uint64(count); chunk++ {
		mask := binary.LittleEndian.Uint64(v.payload[masksStart+int(chunk)*8:])
		valueBytes += bits.OnesCount64(mask) * width
	}
	valuesEnd := valuesStart + valueOffset + valueBytes
	if valuesEnd > v.dataStart+int(end) {
		return nil, 0, false
	}
	return v.payload[valuesStart+valueOffset : valuesEnd : valuesEnd], encoding, true
}

func float64GroupColumnEncoding(chunks []DocumentGroupChunk, column int) Float64GroupEncoding {
	maximum := uint64(0)
	for _, chunk := range chunks {
		mask := chunk.Columns.Masks[column]
		base := column * 64
		for slots := mask; slots != 0; slots &= slots - 1 {
			value := chunk.Columns.Values[base+bits.TrailingZeros64(slots)]
			if math.Signbit(value) || value > math.MaxUint32 || value != math.Trunc(value) {
				return Float64GroupFloat64LE
			}
			maximum = max(maximum, uint64(value))
		}
	}
	switch {
	case maximum <= math.MaxUint8:
		return Float64GroupUint8
	case maximum <= math.MaxUint16:
		return Float64GroupUint16
	default:
		return Float64GroupUint32
	}
}

func decodeFloat64GroupValue(src []byte, offset int, encoding Float64GroupEncoding) float64 {
	switch encoding {
	case Float64GroupUint8:
		return float64(src[offset])
	case Float64GroupUint16:
		return float64(binary.LittleEndian.Uint16(src[offset : offset+2]))
	case Float64GroupUint32:
		return float64(binary.LittleEndian.Uint32(src[offset : offset+4]))
	default:
		return math.Float64frombits(binary.LittleEndian.Uint64(src[offset : offset+8]))
	}
}
