package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
)

const (
	DocumentPagePayloadHeaderSize  = 32
	DocumentPageRecordSize         = 8
	DocumentOverflowDescriptorSize = PageRefSize + 8
	documentPageVersionV1          = uint32(1)
	documentPageVersion            = DevelopmentFormatVersion
	documentPageKnownFlags         = uint8(0)
	documentPageOverflowBit        = uint32(1 << 31)
)

// ErrDocumentPageCorrupt reports a checksum-valid common page whose document
// payload violates the stable-slot or packed-data format.
var ErrDocumentPageCorrupt = errors.New("vibejson: corrupt Store document page")

// DocumentPageHeader describes one immutable logical chunk. Live is the exact
// stable-slot occupancy word; no tombstone or empty-row descriptor is stored.
type DocumentPageHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageSize   uint32
	ChunkID    uint32
	Live       uint64
	Flags      uint8
}

// DocumentRecord is a transient row view. EncodeDocumentPage borrows Key and
// JSON only for the call. Views returned by DocumentPageView alias the admitted
// page and are invalid when its backing frame is reused.
type DocumentRecord struct {
	Key        []byte
	JSON       []byte
	Overflow   PageRef
	JSONLength uint64
	Slot       uint8
}

// DocumentValue is either an inline JSON spelling or a reference to the first
// separately checksummed overflow piece. Length is the complete JSON byte
// length in both cases.
type DocumentValue struct {
	Inline   []byte
	Overflow PageRef
	Length   uint64
}

// DocumentFloat64Columns is the pointer-minimal input for typed covering
// columns. Masks has one stable-slot validity word per column. Values is a
// dense column-major matrix with exactly 64 float64 values per mask; only
// values selected by a mask are encoded. Callers retain both slices.
type DocumentFloat64Columns struct {
	Masks  []uint64
	Values []float64
}

// DocumentPageView is an admitted, checksum-verified micro-page. One borrowed
// payload slice represents all rows; it does not create a pointer per key or
// document.
type DocumentPageView struct {
	header       DocumentPageHeader
	payload      []byte
	dataStart    int
	dataEnd      int
	float64Start int
	count        uint8
	float64Count uint16
}

// EncodeDocumentPage writes one complete stable-slot micro-page into caller
// storage. rows must be ordered by increasing Slot and exactly match
// header.Live. Keys may be empty; JSON must be non-empty and already validated
// by the Store mutation path. Input byte slices must not overlap dst because
// deterministic page initialization clears dst before copying. No allocation
// is performed when rows and dst are caller-owned.
func EncodeDocumentPage(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID uint64) ([]byte, error) {
	return encodeDocumentPage(dst, header, rows, DocumentFloat64Columns{}, nextLogicalID, 0, 0, false)
}

// EncodeDocumentPageWithOverflow writes a document page that may reference
// overflow chains. Each overflow row has an empty JSON, a non-zero Overflow,
// and its complete JSONLength. Inline rows leave those fields zero.
func EncodeDocumentPageWithOverflow(dst []byte, header DocumentPageHeader, rows []DocumentRecord, nextLogicalID, fileEnd uint64, allocationQuantum uint32) ([]byte, error) {
	return encodeDocumentPage(dst, header, rows, DocumentFloat64Columns{}, nextLogicalID, fileEnd, allocationQuantum, true)
}

// EncodeDocumentPageWithColumns writes overflow-capable rows plus float64
// covering columns into the same immutable extent. Keeping typed data beside
// its stable-slot JSON makes document replacement, index maintenance, and
// column publication one copy-on-write generation.
func EncodeDocumentPageWithColumns(dst []byte, header DocumentPageHeader, rows []DocumentRecord, columns DocumentFloat64Columns, nextLogicalID, fileEnd uint64, allocationQuantum uint32) ([]byte, error) {
	return encodeDocumentPage(dst, header, rows, columns, nextLogicalID, fileEnd, allocationQuantum, true)
}

func encodeDocumentPage(dst []byte, header DocumentPageHeader, rows []DocumentRecord, columns DocumentFloat64Columns, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) ([]byte, error) {
	dataLength, float64Length, err := validateDocumentPageWrite(
		header, rows, columns, nextLogicalID, fileEnd, allocationQuantum, allowOverflow,
	)
	if err != nil {
		return nil, err
	}
	count := len(rows)
	payloadLength := DocumentPagePayloadHeaderSize + count*DocumentPageRecordSize + dataLength + float64Length
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
	version := documentPageVersionV1
	if len(columns.Masks) != 0 {
		version = documentPageVersion
	}
	binary.LittleEndian.PutUint32(payload[0:4], version)
	binary.LittleEndian.PutUint32(payload[4:8], header.ChunkID)
	binary.LittleEndian.PutUint64(payload[8:16], header.Live)
	binary.LittleEndian.PutUint32(payload[16:20], uint32(dataLength))
	payload[20] = uint8(count)
	payload[21] = header.Flags
	binary.LittleEndian.PutUint16(payload[22:24], DocumentPageRecordSize)

	dataStart := DocumentPagePayloadHeaderSize + count*DocumentPageRecordSize
	dataEnd := 0
	var overflowCount uint8
	for rank, row := range rows {
		copy(payload[dataStart+dataEnd:], row.Key)
		dataEnd += len(row.Key)
		descriptor := DocumentPagePayloadHeaderSize + rank*DocumentPageRecordSize
		binary.LittleEndian.PutUint32(payload[descriptor:descriptor+4], uint32(dataEnd))
		if row.Overflow == (PageRef{}) {
			copy(payload[dataStart+dataEnd:], row.JSON)
			dataEnd += len(row.JSON)
			binary.LittleEndian.PutUint32(payload[descriptor+4:descriptor+8], uint32(dataEnd))
		} else {
			overflowCount++
			encoded := payload[dataStart+dataEnd:]
			encodePageRef(encoded[:PageRefSize], row.Overflow)
			binary.LittleEndian.PutUint64(encoded[PageRefSize:DocumentOverflowDescriptorSize], row.JSONLength)
			dataEnd += DocumentOverflowDescriptorSize
			binary.LittleEndian.PutUint32(payload[descriptor+4:descriptor+8], documentPageOverflowBit|uint32(dataEnd))
		}
	}
	payload[24] = overflowCount
	if len(columns.Masks) != 0 {
		binary.LittleEndian.PutUint16(payload[25:27], uint16(len(columns.Masks)))
		binary.LittleEndian.PutUint32(payload[28:32], uint32(float64Length))
		cursor := dataStart + dataLength
		for column, mask := range columns.Masks {
			binary.LittleEndian.PutUint64(payload[cursor:cursor+8], mask)
			cursor += 8
			base := column * 64
			for slots := mask; slots != 0; slots &= slots - 1 {
				slot := bits.TrailingZeros64(slots)
				binary.LittleEndian.PutUint64(
					payload[cursor:cursor+8], math.Float64bits(columns.Values[base+slot]),
				)
				cursor += 8
			}
		}
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenDocumentPage verifies a common page and its complete packed row
// directory once. chunkHighWater and nextLogicalID come from the selecting
// state root and reject pages outside that graph. The returned view borrows src
// and repeated lookups allocate nothing.
func OpenDocumentPage(src []byte, chunkHighWater uint32, nextLogicalID uint64) (DocumentPageView, error) {
	return openDocumentPage(src, chunkHighWater, nextLogicalID, 0, 0, false)
}

// OpenDocumentPageWithOverflow admits document pages containing overflow
// references and validates each reference against the selected state root.
func OpenDocumentPageWithOverflow(src []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32) (DocumentPageView, error) {
	return openDocumentPage(src, chunkHighWater, nextLogicalID, fileEnd, allocationQuantum, true)
}

func openDocumentPage(src []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) (DocumentPageView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return DocumentPageView{}, fmt.Errorf("%w: %w", ErrDocumentPageCorrupt, err)
	}
	return openDocumentPagePayload(
		pageHeader, payload, chunkHighWater, nextLogicalID, fileEnd, allocationQuantum, allowOverflow,
	)
}

// OpenAdmittedDocumentPageWithOverflow validates the typed document payload of
// a page already admitted by PageCache. Admission has verified common framing,
// identity, bounds, and CRC32C; this function deliberately does not checksum
// the immutable cache bytes a second time. It still validates every document,
// overflow descriptor, and covering-column invariant before returning a view.
func OpenAdmittedDocumentPageWithOverflow(src []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32) (DocumentPageView, error) {
	pageHeader, ok := decodePageHeader(src)
	if !ok || len(src) != int(pageHeader.PageSize) {
		return DocumentPageView{}, fmt.Errorf("%w: admitted common header", ErrDocumentPageCorrupt)
	}
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	return openDocumentPagePayload(
		pageHeader, payload, chunkHighWater, nextLogicalID, fileEnd, allocationQuantum, true,
	)
}

func openDocumentPagePayload(pageHeader PageHeader, payload []byte, chunkHighWater uint32, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) (DocumentPageView, error) {
	if pageHeader.Kind != PageDocument || len(payload) < DocumentPagePayloadHeaderSize {
		return DocumentPageView{}, fmt.Errorf("%w: header or payload size", ErrDocumentPageCorrupt)
	}
	if allowOverflow && (!validPhysicalPageSize(allocationQuantum) ||
		pageHeader.PageSize < allocationQuantum ||
		pageHeader.PageSize%allocationQuantum != 0) {
		return DocumentPageView{}, fmt.Errorf("%w: allocation geometry", ErrDocumentPageCorrupt)
	}
	version := binary.LittleEndian.Uint32(payload[0:4])
	if version != documentPageVersionV1 && version != documentPageVersion ||
		binary.LittleEndian.Uint16(payload[22:24]) != DocumentPageRecordSize {
		return DocumentPageView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrDocumentPageCorrupt)
	}
	float64Count := uint16(0)
	float64Length := uint64(0)
	if version == documentPageVersionV1 {
		if !allZero(payload[25:DocumentPagePayloadHeaderSize]) {
			return DocumentPageView{}, fmt.Errorf("%w: version-one reserved bytes", ErrDocumentPageCorrupt)
		}
	} else {
		float64Count = binary.LittleEndian.Uint16(payload[25:27])
		float64Length = uint64(binary.LittleEndian.Uint32(payload[28:32]))
		if float64Count == 0 || payload[27] != 0 || float64Length < uint64(float64Count)*8 {
			return DocumentPageView{}, fmt.Errorf("%w: float64 covering header", ErrDocumentPageCorrupt)
		}
	}
	header := DocumentPageHeader{
		StoreID:    pageHeader.StoreID,
		Generation: pageHeader.Generation,
		LogicalID:  pageHeader.LogicalID,
		PageSize:   pageHeader.PageSize,
		ChunkID:    binary.LittleEndian.Uint32(payload[4:8]),
		Live:       binary.LittleEndian.Uint64(payload[8:16]),
		Flags:      payload[21],
	}
	count := int(payload[20])
	dataLength := uint64(binary.LittleEndian.Uint32(payload[16:20]))
	dataStart := uint64(DocumentPagePayloadHeaderSize + count*DocumentPageRecordSize)
	if err := validateDocumentPageHeader(header, count, chunkHighWater, nextLogicalID); err != nil {
		return DocumentPageView{}, fmt.Errorf("%w: %v", ErrDocumentPageCorrupt, err)
	}
	dataEnd := dataStart + dataLength
	float64End := dataEnd + float64Length
	if float64End != uint64(len(payload)) {
		return DocumentPageView{}, fmt.Errorf("%w: payload length", ErrDocumentPageCorrupt)
	}
	var previousEnd uint32
	var overflowCount uint8
	for rank := 0; rank < count; rank++ {
		descriptor := DocumentPagePayloadHeaderSize + rank*DocumentPageRecordSize
		keyEnd := binary.LittleEndian.Uint32(payload[descriptor : descriptor+4])
		encodedEnd := binary.LittleEndian.Uint32(payload[descriptor+4 : descriptor+8])
		overflow := encodedEnd&documentPageOverflowBit != 0
		recordEnd := encodedEnd &^ documentPageOverflowBit
		if keyEnd < previousEnd || recordEnd <= keyEnd || uint64(recordEnd) > dataLength ||
			overflow && (!allowOverflow || recordEnd-keyEnd != DocumentOverflowDescriptorSize) {
			return DocumentPageView{}, fmt.Errorf("%w: non-canonical record bounds", ErrDocumentPageCorrupt)
		}
		if overflow {
			overflowCount++
			encoded := payload[int(dataStart)+int(keyEnd) : int(dataStart)+int(recordEnd)]
			if !pageRefReservedZero(encoded[:PageRefSize]) || binary.LittleEndian.Uint64(encoded[PageRefSize:]) == 0 {
				return DocumentPageView{}, fmt.Errorf("%w: overflow descriptor", ErrDocumentPageCorrupt)
			}
			ref := decodePageRef(encoded[:PageRefSize])
			if !validDocumentOverflowRef(header, ref, fileEnd, nextLogicalID, allocationQuantum) {
				return DocumentPageView{}, fmt.Errorf("%w: overflow reference", ErrDocumentPageCorrupt)
			}
		}
		previousEnd = recordEnd
	}
	if uint64(previousEnd) != dataLength || overflowCount != payload[24] {
		return DocumentPageView{}, fmt.Errorf("%w: unreferenced packed data", ErrDocumentPageCorrupt)
	}
	cursor := int(dataEnd)
	for range int(float64Count) {
		if cursor+8 > int(float64End) {
			return DocumentPageView{}, fmt.Errorf("%w: truncated float64 mask", ErrDocumentPageCorrupt)
		}
		mask := binary.LittleEndian.Uint64(payload[cursor : cursor+8])
		cursor += 8
		if mask&^header.Live != 0 {
			return DocumentPageView{}, fmt.Errorf("%w: float64 mask outside live rows", ErrDocumentPageCorrupt)
		}
		valueBytes := bits.OnesCount64(mask) * 8
		if cursor+valueBytes > int(float64End) {
			return DocumentPageView{}, fmt.Errorf("%w: truncated float64 values", ErrDocumentPageCorrupt)
		}
		for end := cursor + valueBytes; cursor < end; cursor += 8 {
			value := math.Float64frombits(binary.LittleEndian.Uint64(payload[cursor : cursor+8]))
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return DocumentPageView{}, fmt.Errorf("%w: non-finite float64 cover", ErrDocumentPageCorrupt)
			}
		}
	}
	if cursor != int(float64End) {
		return DocumentPageView{}, fmt.Errorf("%w: unreferenced float64 data", ErrDocumentPageCorrupt)
	}
	return DocumentPageView{
		header: header, payload: payload, dataStart: int(dataStart), dataEnd: int(dataEnd),
		float64Start: int(dataEnd), count: uint8(count), float64Count: float64Count,
	}, nil
}

// AdmittedDocumentPage reconstructs a view of a page already validated by
// PageCache admission. Calling it on arbitrary bytes is invalid. The packed
// row directory is checked once per admission, not once per point read.
func AdmittedDocumentPage(src []byte) DocumentPageView {
	pageHeader, _ := decodePageHeader(src)
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	count := int(payload[20])
	dataStart := DocumentPagePayloadHeaderSize + count*DocumentPageRecordSize
	dataEnd := dataStart + int(binary.LittleEndian.Uint32(payload[16:20]))
	float64Count := uint16(0)
	if binary.LittleEndian.Uint32(payload[0:4]) == documentPageVersion {
		float64Count = binary.LittleEndian.Uint16(payload[25:27])
	}
	return DocumentPageView{
		header: DocumentPageHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			ChunkID: binary.LittleEndian.Uint32(payload[4:8]),
			Live:    binary.LittleEndian.Uint64(payload[8:16]), Flags: payload[21],
		},
		payload: payload, dataStart: dataStart, dataEnd: dataEnd,
		float64Start: dataEnd, count: uint8(count), float64Count: float64Count,
	}
}

// Header returns the value-only identity and stable-slot metadata of the view.
func (v DocumentPageView) Header() DocumentPageHeader { return v.header }

// Len returns the number of live rows.
func (v DocumentPageView) Len() int { return int(v.count) }

// Float64ColumnCount returns the number of typed covering columns stored in
// this page. Version-one pages return zero.
func (v DocumentPageView) Float64ColumnCount() int { return int(v.float64Count) }

// DocumentFloat64ColumnView is one borrowed stable-slot numeric column.
type DocumentFloat64ColumnView struct {
	mask     uint64
	values   []byte
	encoding Float64GroupEncoding
}

// Mask returns the stable slots with a finite numeric value.
func (v DocumentFloat64ColumnView) Mask() uint64 { return v.mask }

// Lookup returns one covered value by stable slot.
func (v DocumentFloat64ColumnView) Lookup(slot uint8) (float64, bool) {
	if slot >= 64 {
		return 0, false
	}
	bit := uint64(1) << slot
	if v.mask&bit == 0 {
		return 0, false
	}
	rank := bits.OnesCount64(v.mask & (bit - 1))
	width := v.encoding.ByteWidth()
	return decodeFloat64GroupValue(v.values, rank*width, v.encoding), true
}

// DocumentFloat64Iterator walks covered values in stable-slot order.
type DocumentFloat64Iterator struct {
	slots    uint64
	values   []byte
	offset   int
	encoding Float64GroupEncoding
}

// Iterator returns an allocation-free stable-slot iterator.
func (v DocumentFloat64ColumnView) Iterator() DocumentFloat64Iterator {
	return DocumentFloat64Iterator{slots: v.mask, values: v.values, encoding: v.encoding}
}

// Next returns the next stable slot and value.
func (i *DocumentFloat64Iterator) Next() (uint8, float64, bool) {
	if i == nil || i.slots == 0 {
		return 0, 0, false
	}
	slot := uint8(bits.TrailingZeros64(i.slots))
	value := decodeFloat64GroupValue(i.values, i.offset, i.encoding)
	i.slots &= i.slots - 1
	i.offset += i.encoding.ByteWidth()
	return slot, value, true
}

// DocumentFloat64ValueIterator walks only the densely packed values. It is
// the reduction path: callers that do not need stable slot identities avoid
// one trailing-zero count and one mask update per value while retaining the
// exact stable-slot order used by Iterator.
type DocumentFloat64ValueIterator struct {
	values   []byte
	offset   int
	encoding Float64GroupEncoding
}

// Values returns an allocation-free dense value iterator.
func (v DocumentFloat64ColumnView) Values() DocumentFloat64ValueIterator {
	return DocumentFloat64ValueIterator{values: v.values, encoding: v.encoding}
}

// Next returns the next covered value in stable-slot order.
func (i *DocumentFloat64ValueIterator) Next() (float64, bool) {
	if i == nil || i.offset == len(i.values) {
		return 0, false
	}
	value := decodeFloat64GroupValue(i.values, i.offset, i.encoding)
	i.offset += i.encoding.ByteWidth()
	return value, true
}

// Float64Column returns one borrowed typed covering column.
func (v DocumentPageView) Float64Column(column int) (DocumentFloat64ColumnView, bool) {
	if column < 0 || column >= int(v.float64Count) {
		return DocumentFloat64ColumnView{}, false
	}
	cursor := v.float64Start
	for current := 0; current < int(v.float64Count); current++ {
		mask := binary.LittleEndian.Uint64(v.payload[cursor : cursor+8])
		cursor += 8
		valueBytes := bits.OnesCount64(mask) * 8
		if current == column {
			return DocumentFloat64ColumnView{
				mask: mask, values: v.payload[cursor : cursor+valueBytes : cursor+valueBytes],
			}, true
		}
		cursor += valueBytes
	}
	return DocumentFloat64ColumnView{}, false
}

// Lookup returns the row at stable slot. It uses one bitmap probe, one popcount
// rank, and at most three cumulative-end loads; no scan or allocation occurs.
func (v DocumentPageView) Lookup(slot uint8) (DocumentRecord, bool) {
	if slot >= 64 {
		return DocumentRecord{}, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return DocumentRecord{}, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	return v.recordAt(rank, slot)
}

// LookupJSON returns only the JSON bytes for a stable slot. Point reads use
// this narrower form after a trusted compiled-key hit so they do not construct
// or copy key metadata. The returned slice is capacity-clipped to the document.
func (v DocumentPageView) LookupJSON(slot uint8) ([]byte, bool) {
	if slot >= 64 {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	descriptor := DocumentPagePayloadHeaderSize + rank*DocumentPageRecordSize
	keyEnd := binary.LittleEndian.Uint32(v.payload[descriptor : descriptor+4])
	jsonEnd := binary.LittleEndian.Uint32(v.payload[descriptor+4 : descriptor+8])
	if jsonEnd&documentPageOverflowBit != 0 {
		return nil, false
	}
	start := v.dataStart + int(keyEnd)
	end := v.dataStart + int(jsonEnd)
	if start >= end || end > v.dataEnd {
		return nil, false
	}
	return v.payload[start:end:end], true
}

// LookupKey verifies the complete key at a candidate stable slot and returns
// its JSON. Hash or fingerprint collisions therefore cannot return another
// document. The admitted-page fast path allocates nothing.
func (v DocumentPageView) LookupKey(slot uint8, key []byte) ([]byte, bool) {
	if slot >= 64 {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	descriptor := DocumentPagePayloadHeaderSize + rank*DocumentPageRecordSize
	rowStart := uint32(0)
	if rank != 0 {
		rowStart = binary.LittleEndian.Uint32(v.payload[descriptor-4:descriptor]) &^ documentPageOverflowBit
	}
	keyEnd := binary.LittleEndian.Uint32(v.payload[descriptor : descriptor+4])
	jsonEnd := binary.LittleEndian.Uint32(v.payload[descriptor+4 : descriptor+8])
	if jsonEnd&documentPageOverflowBit != 0 {
		return nil, false
	}
	keyStart := v.dataStart + int(rowStart)
	jsonStart := v.dataStart + int(keyEnd)
	end := v.dataStart + int(jsonEnd)
	if keyStart > jsonStart || jsonStart >= end || end > v.dataEnd ||
		!bytes.Equal(v.payload[keyStart:jsonStart], key) {
		return nil, false
	}
	return v.payload[jsonStart:end:end], true
}

// LookupString verifies the complete string key at a candidate stable slot and
// returns its JSON. The byte-slice/string comparison is allocation-free; this
// is the direct bridge from the Store's ordinary keyed API to a resident page.
func (v DocumentPageView) LookupString(slot uint8, key string) ([]byte, bool) {
	if slot >= 64 {
		return nil, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return nil, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	descriptor := DocumentPagePayloadHeaderSize + rank*DocumentPageRecordSize
	rowStart := uint32(0)
	if rank != 0 {
		rowStart = binary.LittleEndian.Uint32(v.payload[descriptor-4:descriptor]) &^ documentPageOverflowBit
	}
	keyEnd := binary.LittleEndian.Uint32(v.payload[descriptor : descriptor+4])
	jsonEnd := binary.LittleEndian.Uint32(v.payload[descriptor+4 : descriptor+8])
	if jsonEnd&documentPageOverflowBit != 0 {
		return nil, false
	}
	keyStart := v.dataStart + int(rowStart)
	jsonStart := v.dataStart + int(keyEnd)
	end := v.dataStart + int(jsonEnd)
	if keyStart > jsonStart || jsonStart >= end || end > v.dataEnd ||
		string(v.payload[keyStart:jsonStart]) != key {
		return nil, false
	}
	return v.payload[jsonStart:end:end], true
}

// DocumentPageRows walks the live rows of an admitted document page in
// ascending stable-slot order.
//
// It exists because DocumentPageView.Lookup is the wrong shape for a scan.
// Lookup is a random-access primitive: it converts a stable slot to a packed
// rank with a popcount, then re-reads the *previous* row's cumulative end to
// find where this row's key starts, then returns a 96-byte DocumentRecord by
// value. A sequential scan already knows both of those things — the next
// selected row's rank is one more than the last one it consumed, and the
// previous row's end is the value it just computed — so it was paying a
// popcount, a redundant load, and a fat struct copy per document to rediscover
// state it was holding. Profiling a 20,000-document durable scan attributed
// 83% of samples to that call chain (Lookup, recordAt, and the two layers of
// by-value record structs above them) and only 2.4% to the page cache.
//
// The iterator carries rank and the previous end across rows and hands back
// borrowed key/JSON slices directly, so the common inline row costs two uint32
// loads, three comparisons, and two slice constructions.
//
// Rows borrow the admitted page and are invalid once its frame is released.
type DocumentPageRows struct {
	payload []byte
	// selected is the not-yet-visited subset of live rows this walk will
	// report; pending is the not-yet-ranked subset of *all* live rows, which
	// is what keeps rank correct when a mask skips over live rows.
	selected uint64
	pending  uint64
	// prevEnd is the cumulative data end of packed rank prevRank, which is
	// where the next row's key begins only when that next row is prevRank+1.
	// A mask that skips live rows breaks that adjacency, so prevRank is kept
	// alongside and the boundary is reloaded from the skipped-over descriptor
	// when they disagree. Assuming adjacency unconditionally made a masked
	// scan hand back key slices that spanned every skipped row.
	prevEnd   uint32
	prevRank  int
	rank      int
	dataStart int
	dataEnd   int
	count     int
	overflow  bool
	corrupt   bool
}

// Rows returns an iterator over the live rows selected by mask, in ascending
// stable-slot order. Pass ^uint64(0) to visit every live row.
func (v DocumentPageView) Rows(mask uint64) DocumentPageRows {
	live := v.header.Live
	return DocumentPageRows{
		payload: v.payload, selected: live & mask, pending: live, prevRank: -1,
		dataStart: v.dataStart, dataEnd: v.dataEnd, count: int(v.count),
	}
}

// Next reports the next selected row. ok is false at the end of the page and
// also when the page is structurally inconsistent, which Err distinguishes.
// key and json borrow the page. When overflow is true the row's JSON is not
// inline and json instead holds the overflow descriptor bytes, which
// OverflowDescriptor decodes.
func (it *DocumentPageRows) Next() (slot uint8, key, json []byte, overflow, ok bool) {
	if it.selected == 0 {
		return 0, nil, nil, false, false
	}
	bit := it.selected & -it.selected
	it.selected &^= bit
	// Live rows skipped by the mask still occupy packed ranks, so advance rank
	// past exactly the live-but-unselected rows below this one. For a full
	// scan the skipped set is always empty and this popcount folds away to
	// zero, leaving a plain increment.
	skipped := it.pending & (bit - 1)
	it.rank += bits.OnesCount64(skipped)
	it.pending &^= skipped | bit
	rank := it.rank
	it.rank++

	descriptor := DocumentPagePayloadHeaderSize + rank*DocumentPageRecordSize
	if rank >= it.count || descriptor+DocumentPageRecordSize > it.dataStart {
		it.corrupt = true
		return 0, nil, nil, false, false
	}
	if rank != it.prevRank+1 {
		// The mask skipped at least one live row, so this row's key does not
		// begin where the last reported row ended. Recover the true boundary
		// from the immediately preceding rank's cumulative end.
		it.prevEnd = 0
		if rank != 0 {
			previous := descriptor - DocumentPageRecordSize
			it.prevEnd = binary.LittleEndian.Uint32(
				it.payload[previous+4:previous+8],
			) &^ documentPageOverflowBit
		}
	}
	it.prevRank = rank
	keyEnd := binary.LittleEndian.Uint32(it.payload[descriptor : descriptor+4])
	encodedEnd := binary.LittleEndian.Uint32(it.payload[descriptor+4 : descriptor+8])
	rowEnd := encodedEnd &^ documentPageOverflowBit
	keyStart := it.dataStart + int(it.prevEnd)
	jsonStart := it.dataStart + int(keyEnd)
	end := it.dataStart + int(rowEnd)
	if keyStart > jsonStart || jsonStart >= end || end > it.dataEnd {
		it.corrupt = true
		return 0, nil, nil, false, false
	}
	it.prevEnd = rowEnd
	it.overflow = encodedEnd&documentPageOverflowBit != 0
	return uint8(bits.TrailingZeros64(bit)),
		it.payload[keyStart:jsonStart:jsonStart],
		it.payload[jsonStart:end:end],
		it.overflow, true
}

// OverflowDescriptor decodes the reference and complete JSON length of the row
// Next reported with overflow set. json is that row's descriptor bytes.
func (it *DocumentPageRows) OverflowDescriptor(json []byte) (PageRef, uint64, bool) {
	if !it.overflow || len(json) != DocumentOverflowDescriptorSize {
		return PageRef{}, 0, false
	}
	return decodePageRef(json[:PageRefSize]), binary.LittleEndian.Uint64(json[PageRefSize:]), true
}

// Err reports whether iteration stopped on a structurally inconsistent row
// rather than at the end of the page.
func (it *DocumentPageRows) Err() bool { return it.corrupt }

// RecordAt returns the packed-rank row and its stable slot.
func (v DocumentPageView) RecordAt(rank int) (DocumentRecord, bool) {
	if rank < 0 || rank >= int(v.count) {
		return DocumentRecord{}, false
	}
	live := v.header.Live
	for range rank {
		live &= live - 1
	}
	return v.recordAt(rank, uint8(bits.TrailingZeros64(live)))
}

// LookupValue returns either inline JSON or the first overflow reference for a
// stable slot. The inline slice aliases the leased document page.
func (v DocumentPageView) LookupValue(slot uint8) (DocumentValue, bool) {
	if slot >= 64 {
		return DocumentValue{}, false
	}
	bit := uint64(1) << slot
	if v.header.Live&bit == 0 {
		return DocumentValue{}, false
	}
	rank := bits.OnesCount64(v.header.Live & (bit - 1))
	return v.valueAt(rank)
}

// LookupKeyValue verifies the complete key before returning an inline or
// overflow value descriptor.
func (v DocumentPageView) LookupKeyValue(slot uint8, key []byte) (DocumentValue, bool) {
	record, ok := v.Lookup(slot)
	if !ok || !bytes.Equal(record.Key, key) {
		return DocumentValue{}, false
	}
	if record.Overflow != (PageRef{}) {
		return DocumentValue{Overflow: record.Overflow, Length: record.JSONLength}, true
	}
	return DocumentValue{Inline: record.JSON, Length: uint64(len(record.JSON))}, true
}

// LookupStringValue is the string-key form of LookupKeyValue.
func (v DocumentPageView) LookupStringValue(slot uint8, key string) (DocumentValue, bool) {
	record, ok := v.Lookup(slot)
	if !ok || string(record.Key) != key {
		return DocumentValue{}, false
	}
	if record.Overflow != (PageRef{}) {
		return DocumentValue{Overflow: record.Overflow, Length: record.JSONLength}, true
	}
	return DocumentValue{Inline: record.JSON, Length: uint64(len(record.JSON))}, true
}

func (v DocumentPageView) recordAt(rank int, slot uint8) (DocumentRecord, bool) {
	descriptor := DocumentPagePayloadHeaderSize + rank*DocumentPageRecordSize
	if rank < 0 || rank >= int(v.count) || descriptor+DocumentPageRecordSize > v.dataStart {
		return DocumentRecord{}, false
	}
	start := uint32(0)
	if rank != 0 {
		start = binary.LittleEndian.Uint32(v.payload[descriptor-4:descriptor]) &^ documentPageOverflowBit
	}
	keyEnd := binary.LittleEndian.Uint32(v.payload[descriptor : descriptor+4])
	encodedEnd := binary.LittleEndian.Uint32(v.payload[descriptor+4 : descriptor+8])
	recordEnd := encodedEnd &^ documentPageOverflowBit
	keyStart := v.dataStart + int(start)
	jsonStart := v.dataStart + int(keyEnd)
	end := v.dataStart + int(recordEnd)
	if keyStart > jsonStart || jsonStart >= end || end > v.dataEnd {
		return DocumentRecord{}, false
	}
	if encodedEnd&documentPageOverflowBit != 0 {
		encoded := v.payload[jsonStart:end]
		if len(encoded) != DocumentOverflowDescriptorSize {
			return DocumentRecord{}, false
		}
		return DocumentRecord{
			Key: v.payload[keyStart:jsonStart:jsonStart], Overflow: decodePageRef(encoded[:PageRefSize]),
			JSONLength: binary.LittleEndian.Uint64(encoded[PageRefSize:]), Slot: slot,
		}, true
	}
	return DocumentRecord{
		Key:  v.payload[keyStart:jsonStart:jsonStart],
		JSON: v.payload[jsonStart:end:end],
		Slot: slot,
	}, true
}

func (v DocumentPageView) valueAt(rank int) (DocumentValue, bool) {
	record, ok := v.recordAt(rank, 0)
	if !ok {
		return DocumentValue{}, false
	}
	if record.Overflow != (PageRef{}) {
		return DocumentValue{Overflow: record.Overflow, Length: record.JSONLength}, true
	}
	return DocumentValue{Inline: record.JSON, Length: uint64(len(record.JSON))}, true
}

func validateDocumentPageWrite(header DocumentPageHeader, rows []DocumentRecord, columns DocumentFloat64Columns, nextLogicalID, fileEnd uint64, allocationQuantum uint32, allowOverflow bool) (int, int, error) {
	if err := validateDocumentPageHeader(header, len(rows), header.ChunkID+1, nextLogicalID); err != nil {
		return 0, 0, err
	}
	if allowOverflow && (!validPhysicalPageSize(allocationQuantum) ||
		header.PageSize < allocationQuantum ||
		header.PageSize%allocationQuantum != 0) {
		return 0, 0, fmt.Errorf("%w: document allocation geometry", ErrInvalidWrite)
	}
	live := header.Live
	dataLength := uint64(0)
	for _, row := range rows {
		slot := uint8(bits.TrailingZeros64(live))
		if row.Slot != slot {
			return 0, 0, fmt.Errorf("%w: rows do not match stable slots", ErrInvalidWrite)
		}
		rowLength := len(row.JSON)
		if row.Overflow == (PageRef{}) {
			if rowLength == 0 || row.JSONLength != 0 {
				return 0, 0, fmt.Errorf("%w: inline document value", ErrInvalidWrite)
			}
		} else {
			if !allowOverflow || rowLength != 0 || row.JSONLength == 0 ||
				!validDocumentOverflowRef(header, row.Overflow, fileEnd, nextLogicalID, allocationQuantum) {
				return 0, 0, fmt.Errorf("%w: overflow document value", ErrInvalidWrite)
			}
			rowLength = DocumentOverflowDescriptorSize
		}
		dataLength += uint64(len(row.Key)) + uint64(rowLength)
		if dataLength >= uint64(documentPageOverflowBit) {
			return 0, 0, fmt.Errorf("%w: document data length", ErrInvalidWrite)
		}
		live &= live - 1
	}
	if len(columns.Masks) > math.MaxUint16 ||
		len(columns.Masks) == 0 && len(columns.Values) != 0 ||
		len(columns.Masks) != 0 && len(columns.Values) != len(columns.Masks)*64 {
		return 0, 0, fmt.Errorf("%w: float64 covering shape", ErrInvalidWrite)
	}
	float64Length := uint64(0)
	for column, mask := range columns.Masks {
		if mask&^header.Live != 0 {
			return 0, 0, fmt.Errorf("%w: float64 mask outside live rows", ErrInvalidWrite)
		}
		float64Length += 8 + uint64(bits.OnesCount64(mask))*8
		base := column * 64
		for slots := mask; slots != 0; slots &= slots - 1 {
			value := columns.Values[base+bits.TrailingZeros64(slots)]
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return 0, 0, fmt.Errorf("%w: non-finite float64 cover", ErrInvalidWrite)
			}
		}
	}
	payloadLength := uint64(DocumentPagePayloadHeaderSize+len(rows)*DocumentPageRecordSize) + dataLength + float64Length
	if payloadLength > uint64(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return 0, 0, fmt.Errorf("%w: document payload does not fit", ErrInvalidWrite)
	}
	return int(dataLength), int(float64Length), nil
}

func validDocumentOverflowRef(header DocumentPageHeader, ref PageRef, fileEnd, nextLogicalID uint64, allocationQuantum uint32) bool {
	if !validPhysicalPageSize(allocationQuantum) ||
		!validPageExtentSize(PageDocument, header.PageSize) ||
		header.PageSize < allocationQuantum || header.PageSize%allocationQuantum != 0 {
		return false
	}
	overflowHeader := OverflowPageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize,
	}
	return pageRefWithinFile(ref, PageOverflow, overflowHeader, fileEnd, nextLogicalID, allocationQuantum)
}

func validateDocumentPageHeader(header DocumentPageHeader, count int, chunkHighWater uint32, nextLogicalID uint64) error {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID <= StateRootLogicalID || header.LogicalID >= nextLogicalID ||
		!validPageExtentSize(PageDocument, header.PageSize) ||
		header.Flags&^documentPageKnownFlags != 0 {
		return fmt.Errorf("%w: document identity, page size, or flags", ErrInvalidWrite)
	}
	if header.Live == 0 || count != bits.OnesCount64(header.Live) || count > 64 ||
		chunkHighWater == 0 || header.ChunkID >= chunkHighWater {
		return fmt.Errorf("%w: document chunk or live rows", ErrInvalidWrite)
	}
	return nil
}
