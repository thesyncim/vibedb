package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	Float64StripePayloadHeaderSize = 64
	Float64StripeColumnSize        = 12
	float64StripeVersion           = DevelopmentFormatVersion
)

var ErrFloat64StripeCorrupt = errors.New("vibejson: corrupt Store float64 scan stripe")

// Float64StripeColumn is one transient dense column supplied to the encoder.
// Values are already in Encoding and are borrowed only for the call.
type Float64StripeColumn struct {
	Encoding Float64GroupEncoding
	Values   []byte
}

type Float64StripeHeader struct {
	StoreID     [16]byte
	Generation  uint64
	LogicalID   uint64
	PageSize    uint32
	FirstChunk  uint32
	ChunkCount  uint32
	RowCount    uint32
	ColumnCount uint16
}

type Float64StripeView struct {
	header         Float64StripeHeader
	payload        []byte
	directoryStart int
	dataStart      int
}

// EncodeFloat64Stripe writes an aggregate-only clean-generation projection.
// It deliberately omits stable-slot masks; the authoritative detached groups
// retain those for mutation overlays. No allocation is performed.
func EncodeFloat64Stripe(
	dst []byte,
	header Float64StripeHeader,
	columns []Float64StripeColumn,
	nextLogicalID uint64,
) ([]byte, error) {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID <= StateRootLogicalID || header.LogicalID >= nextLogicalID ||
		!validPhysicalPageSize(header.PageSize) || header.ChunkCount == 0 ||
		header.RowCount == 0 || len(columns) == 0 ||
		len(columns) > math.MaxUint16 || int(header.ColumnCount) != len(columns) {
		return nil, fmt.Errorf("%w: float64 stripe header", ErrInvalidWrite)
	}
	dataBytes := 0
	for _, column := range columns {
		width := column.Encoding.ByteWidth()
		if width == 0 || len(column.Values)%width != 0 ||
			uint64(len(column.Values)/width) > uint64(header.RowCount) {
			return nil, fmt.Errorf("%w: float64 stripe column", ErrInvalidWrite)
		}
		if column.Encoding == Float64GroupFloat64LE {
			for offset := 0; offset < len(column.Values); offset += 8 {
				value := math.Float64frombits(binary.LittleEndian.Uint64(column.Values[offset : offset+8]))
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return nil, fmt.Errorf("%w: non-finite float64 stripe", ErrInvalidWrite)
				}
			}
		}
		dataBytes += len(column.Values)
	}
	payloadBytes := Float64StripePayloadHeaderSize +
		len(columns)*Float64StripeColumnSize + dataBytes
	if payloadBytes > int(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return nil, fmt.Errorf("%w: float64 stripe extent", ErrInvalidWrite)
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadBytes), Kind: PageFloat64Stripe,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], float64StripeVersion)
	binary.LittleEndian.PutUint32(payload[4:8], header.FirstChunk)
	binary.LittleEndian.PutUint32(payload[8:12], header.ChunkCount)
	binary.LittleEndian.PutUint32(payload[12:16], header.RowCount)
	binary.LittleEndian.PutUint16(payload[16:18], header.ColumnCount)
	binary.LittleEndian.PutUint16(payload[18:20], Float64StripeColumnSize)
	binary.LittleEndian.PutUint32(payload[20:24], uint32(len(columns)*Float64StripeColumnSize))
	binary.LittleEndian.PutUint32(payload[24:28], uint32(dataBytes))
	directoryStart := Float64StripePayloadHeaderSize
	cursor := directoryStart + len(columns)*Float64StripeColumnSize
	dataStart := cursor
	for ordinal, column := range columns {
		copy(payload[cursor:], column.Values)
		cursor += len(column.Values)
		entry := directoryStart + ordinal*Float64StripeColumnSize
		binary.LittleEndian.PutUint32(payload[entry:entry+4], uint32(cursor-dataStart))
		binary.LittleEndian.PutUint32(
			payload[entry+4:entry+8], uint32(len(column.Values)/column.Encoding.ByteWidth()),
		)
		payload[entry+8] = byte(column.Encoding)
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func OpenFloat64Stripe(src []byte, chunkHighWater uint32, nextLogicalID uint64) (Float64StripeView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return Float64StripeView{}, fmt.Errorf("%w: %w", ErrFloat64StripeCorrupt, err)
	}
	return openFloat64StripePayload(pageHeader, payload, chunkHighWater, nextLogicalID)
}

func OpenAdmittedFloat64Stripe(src []byte, chunkHighWater uint32, nextLogicalID uint64) (Float64StripeView, error) {
	pageHeader, ok := decodePageHeader(src)
	if !ok || len(src) != int(pageHeader.PageSize) {
		return Float64StripeView{}, ErrFloat64StripeCorrupt
	}
	end := PageHeaderSize + int(pageHeader.PayloadLength)
	return openFloat64StripePayload(
		pageHeader, src[PageHeaderSize:end:end], chunkHighWater, nextLogicalID,
	)
}

func openFloat64StripePayload(
	pageHeader PageHeader,
	payload []byte,
	chunkHighWater uint32,
	nextLogicalID uint64,
) (Float64StripeView, error) {
	if pageHeader.Kind != PageFloat64Stripe || pageHeader.Flags != 0 ||
		len(payload) < Float64StripePayloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != float64StripeVersion ||
		binary.LittleEndian.Uint16(payload[18:20]) != Float64StripeColumnSize ||
		!allZero(payload[28:Float64StripePayloadHeaderSize]) {
		return Float64StripeView{}, fmt.Errorf("%w: header or reserved bytes", ErrFloat64StripeCorrupt)
	}
	first := binary.LittleEndian.Uint32(payload[4:8])
	chunks := binary.LittleEndian.Uint32(payload[8:12])
	rows := binary.LittleEndian.Uint32(payload[12:16])
	columns := int(binary.LittleEndian.Uint16(payload[16:18]))
	directoryBytes := int(binary.LittleEndian.Uint32(payload[20:24]))
	dataBytes := int(binary.LittleEndian.Uint32(payload[24:28]))
	if chunks == 0 || rows == 0 || columns == 0 ||
		uint64(first)+uint64(chunks) > uint64(chunkHighWater) ||
		pageHeader.LogicalID <= StateRootLogicalID || pageHeader.LogicalID >= nextLogicalID ||
		directoryBytes != columns*Float64StripeColumnSize ||
		directoryBytes > len(payload)-Float64StripePayloadHeaderSize ||
		dataBytes != len(payload)-Float64StripePayloadHeaderSize-directoryBytes {
		return Float64StripeView{}, fmt.Errorf("%w: counts or section sizes", ErrFloat64StripeCorrupt)
	}
	view := Float64StripeView{
		header: Float64StripeHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			FirstChunk: first, ChunkCount: chunks, RowCount: rows, ColumnCount: uint16(columns),
		},
		payload: payload, directoryStart: Float64StripePayloadHeaderSize,
		dataStart: Float64StripePayloadHeaderSize + directoryBytes,
	}
	if !view.valid() {
		return Float64StripeView{}, fmt.Errorf("%w: column directory or values", ErrFloat64StripeCorrupt)
	}
	return view, nil
}

func (v Float64StripeView) valid() bool {
	previous := uint32(0)
	for column := 0; column < int(v.header.ColumnCount); column++ {
		entry := v.directoryStart + column*Float64StripeColumnSize
		end := binary.LittleEndian.Uint32(v.payload[entry : entry+4])
		count := binary.LittleEndian.Uint32(v.payload[entry+4 : entry+8])
		encoding := Float64GroupEncoding(v.payload[entry+8])
		width := encoding.ByteWidth()
		if !allZero(v.payload[entry+9:entry+Float64StripeColumnSize]) ||
			width == 0 ||
			count > v.header.RowCount ||
			end < previous || uint64(end) > uint64(len(v.payload)-v.dataStart) ||
			uint64(end-previous) != uint64(count)*uint64(width) {
			return false
		}
		if encoding == Float64GroupFloat64LE {
			for offset := v.dataStart + int(previous); offset < v.dataStart+int(end); offset += 8 {
				value := math.Float64frombits(binary.LittleEndian.Uint64(v.payload[offset : offset+8]))
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return false
				}
			}
		}
		previous = end
	}
	return int(previous) == len(v.payload)-v.dataStart
}

func AdmittedFloat64Stripe(src []byte) Float64StripeView {
	pageHeader, _ := decodePageHeader(src)
	end := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:end:end]
	directoryBytes := int(binary.LittleEndian.Uint32(payload[20:24]))
	return Float64StripeView{
		header: Float64StripeHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			FirstChunk:  binary.LittleEndian.Uint32(payload[4:8]),
			ChunkCount:  binary.LittleEndian.Uint32(payload[8:12]),
			RowCount:    binary.LittleEndian.Uint32(payload[12:16]),
			ColumnCount: binary.LittleEndian.Uint16(payload[16:18]),
		},
		payload: payload, directoryStart: Float64StripePayloadHeaderSize,
		dataStart: Float64StripePayloadHeaderSize + directoryBytes,
	}
}

func (v Float64StripeView) Header() Float64StripeHeader { return v.header }

func (v Float64StripeView) ColumnValues(column int) ([]byte, Float64GroupEncoding, bool) {
	if column < 0 || column >= int(v.header.ColumnCount) {
		return nil, 0, false
	}
	start := uint32(0)
	if column != 0 {
		previous := v.directoryStart + (column-1)*Float64StripeColumnSize
		start = binary.LittleEndian.Uint32(v.payload[previous : previous+4])
	}
	entry := v.directoryStart + column*Float64StripeColumnSize
	end := binary.LittleEndian.Uint32(v.payload[entry : entry+4])
	encoding := Float64GroupEncoding(v.payload[entry+8])
	return v.payload[v.dataStart+int(start) : v.dataStart+int(end) : v.dataStart+int(end)], encoding, true
}
