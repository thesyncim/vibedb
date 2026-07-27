package vnext

import (
	"bytes"
	"encoding/binary"
	"math/bits"
)

const (
	PackedBlockPayloadHeaderSize = 32
	// A packed slot needs only its row start and key end. The JSON middle ends
	// at the next live slot's row start (or dataBytes for the final row), just
	// like a raw block infers JSON end. Keeping the same four-byte directory
	// avoids paying an extra 256 bytes merely because a block is compressed.
	PackedBlockSlotRecordSize    = 4
	PackedBlockSlotDirectorySize = RawBlockSlotCount * PackedBlockSlotRecordSize
	PackedBlockFixedBytes        = FrameHeaderSize + FrameTrailerSize +
		PackedBlockPayloadHeaderSize + PackedBlockSlotDirectorySize +
		RawBlockOrderSize

	packedBlockVersion      = uint32(1)
	packedBlockAbsentRecord = uint32(0xffffffff)
)

// BlockEncoding is the one canonical on-disk representation selected for a
// record block. Readers never consult both representations.
type BlockEncoding uint8

const (
	BlockEncodingRaw BlockEncoding = iota + 1
	BlockEncodingPacked
)

// BlockPlan is the deterministic raw-versus-packed decision made before a
// block replacement is published.
type BlockPlan struct {
	Encoding     BlockEncoding
	Span         int
	EncodedBytes int
	RawSpan      int
	PackedSpan   int
}

// BlockPlanPolicy separates a build/CPU-qualified read gate from the
// deterministic physical-space gate. Packed stays disabled unless its
// integrated reader has been measured for the target production configuration.
type BlockPlanPolicy struct {
	PackedReadGatePassed  bool
	MinimumSavingPermille uint16
}

// PlanCanonicalBlock selects packed only when it removes at least one physical
// quantum, clears the requested fractional saving, and the caller supplies a
// measured read-gate capability. A zero threshold selects 12.5%. This prevents
// weak compression from trading read simplicity for padding that the allocator
// would have discarded anyway.
func PlanCanonicalBlock(
	rows []RawRow,
	geometry GeometryPolicy,
	policy BlockPlanPolicy,
) (BlockPlan, error) {
	minimumSavingPermille := policy.MinimumSavingPermille
	if minimumSavingPermille == 0 {
		minimumSavingPermille = 125
	}
	if minimumSavingPermille > 1000 {
		return BlockPlan{}, ErrInvalidFrame
	}
	rawBytes, err := RawBlockEncodedBytes(rows)
	if err != nil {
		return BlockPlan{}, err
	}
	rawSpan, err := geometry.SelectSpan(rawBytes)
	if err != nil {
		return BlockPlan{}, err
	}
	plan := BlockPlan{
		Encoding: BlockEncodingRaw, Span: rawSpan, EncodedBytes: rawBytes,
		RawSpan: rawSpan, PackedSpan: rawSpan,
	}
	if !policy.PackedReadGatePassed {
		return plan, nil
	}
	packedBytes, err := PackedBlockEncodedBytes(rows)
	if err != nil {
		return plan, nil
	}
	packedSpan, err := geometry.SelectSpan(packedBytes)
	if err != nil {
		return plan, nil
	}
	plan.PackedSpan = packedSpan
	saving := rawSpan - packedSpan
	if saving >= Quantum && saving*1000 >= rawSpan*int(minimumSavingPermille) {
		plan.Encoding = BlockEncodingPacked
		plan.Span = packedSpan
		plan.EncodedBytes = packedBytes
	}
	return plan, nil
}

// PackedBlockView stores one block-wide common JSON prefix and suffix plus one
// independently decodable middle per stable row. It is deliberately a narrow
// codec candidate: blocks that do not clear the space and read gates stay raw.
type PackedBlockView struct {
	identity  Identity
	payload   []byte
	order     []byte
	data      []byte
	blockID   uint32
	live      uint64
	count     uint8
	prefixEnd uint16
	suffixEnd uint16
}

// PackedBlockIter is an allocation-free lexical iterator over exact borrowed
// keys. JSON reconstruction remains explicit through AppendJSON.
type PackedBlockIter struct {
	view PackedBlockView
	rank uint8
}

// PackedBlockEncodedBytes returns exact non-padding bytes for the candidate.
func PackedBlockEncodedBytes(rows []RawRow) (int, error) {
	prefixLength, suffixLength, ok := commonJSONEdges(rows)
	if !ok || len(rows) > RawBlockSlotCount {
		return 0, ErrInvalidFrame
	}
	bytes := PackedBlockFixedBytes + prefixLength + suffixLength
	previous := -1
	for _, row := range rows {
		if int(row.Slot) <= previous || row.Slot >= RawBlockSlotCount ||
			len(row.JSON) == 0 || prefixLength+suffixLength > len(row.JSON) {
			return 0, ErrInvalidFrame
		}
		bytes += len(row.Key) + len(row.JSON) - prefixLength - suffixLength
		previous = int(row.Slot)
	}
	if bytes > MaxSpan {
		return 0, ErrInvalidFrame
	}
	return bytes, nil
}

// EncodePackedBlock writes a prefix/suffix-packed candidate block. Keys remain
// exact and uncompressed; JSON is reconstructed as prefix + row middle + suffix.
func EncodePackedBlock(
	dst []byte,
	identity Identity,
	blockID uint32,
	rows []RawRow,
) ([]byte, error) {
	if blockID == 0 || len(rows) == 0 || len(rows) > RawBlockSlotCount ||
		!validSpan(len(dst)) {
		return nil, ErrInvalidFrame
	}
	prefixLength, suffixLength, ok := commonJSONEdges(rows)
	if !ok {
		return nil, ErrInvalidFrame
	}
	dataBytes := prefixLength + suffixLength
	previous := -1
	for _, row := range rows {
		if int(row.Slot) <= previous || row.Slot >= RawBlockSlotCount ||
			len(row.JSON) == 0 || prefixLength+suffixLength > len(row.JSON) {
			return nil, ErrInvalidFrame
		}
		dataBytes += len(row.Key) + len(row.JSON) - prefixLength - suffixLength
		previous = int(row.Slot)
	}
	if dataBytes >= 1<<16 {
		return nil, ErrInvalidFrame
	}
	var order [RawBlockSlotCount]uint8
	for index := range rows {
		order[index] = uint8(index)
	}
	for index := 1; index < len(rows); index++ {
		current := order[index]
		at := index
		for at > 0 && bytes.Compare(rows[order[at-1]].Key, rows[current].Key) > 0 {
			order[at] = order[at-1]
			at--
		}
		order[at] = current
	}
	for index := 1; index < len(rows); index++ {
		if bytes.Equal(rows[order[index-1]].Key, rows[order[index]].Key) {
			return nil, ErrInvalidFrame
		}
	}
	payloadLength := PackedBlockPayloadHeaderSize + PackedBlockSlotDirectorySize +
		RawBlockOrderSize + dataBytes
	if payloadLength > len(dst)-FrameHeaderSize-FrameTrailerSize {
		return nil, ErrInvalidFrame
	}
	payload, err := initFrame(dst, identity, framePackedBlock, payloadLength)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], packedBlockVersion)
	binary.LittleEndian.PutUint32(payload[4:8], blockID)
	binary.LittleEndian.PutUint16(payload[16:18], uint16(prefixLength))
	binary.LittleEndian.PutUint16(payload[18:20], uint16(suffixLength))
	binary.LittleEndian.PutUint16(payload[20:22], uint16(dataBytes))
	payload[22] = byte(len(rows))
	directory := payload[PackedBlockPayloadHeaderSize:]
	for slot := range RawBlockSlotCount {
		binary.LittleEndian.PutUint32(
			directory[slot*PackedBlockSlotRecordSize:],
			packedBlockAbsentRecord,
		)
	}
	lexical := directory[PackedBlockSlotDirectorySize:]
	for rank := range rows {
		putRawBlockOrder(lexical, rank, rows[order[rank]].Slot)
	}
	data := lexical[RawBlockOrderSize:]
	cursor := 0
	cursor += copy(data[cursor:], rows[0].JSON[:prefixLength])
	cursor += copy(data[cursor:], rows[0].JSON[len(rows[0].JSON)-suffixLength:])
	live := uint64(0)
	for _, row := range rows {
		live |= uint64(1) << row.Slot
		keyStart := cursor
		cursor += copy(data[cursor:], row.Key)
		keyEnd := cursor
		cursor += copy(
			data[cursor:],
			row.JSON[prefixLength:len(row.JSON)-suffixLength],
		)
		record := uint32(uint16(keyStart)) | uint32(uint16(keyEnd))<<16
		binary.LittleEndian.PutUint32(
			directory[int(row.Slot)*PackedBlockSlotRecordSize:],
			record,
		)
	}
	binary.LittleEndian.PutUint64(payload[8:16], live)
	if err := sealFrame(dst); err != nil {
		return nil, err
	}
	return dst, nil
}

// OpenPackedBlock validates the complete directory once.
func OpenPackedBlock(src []byte) (PackedBlockView, error) {
	header, payload, err := openFrame(src, framePackedBlock)
	if err != nil {
		return PackedBlockView{}, err
	}
	if len(payload) < PackedBlockPayloadHeaderSize+
		PackedBlockSlotDirectorySize+RawBlockOrderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != packedBlockVersion ||
		binary.LittleEndian.Uint32(payload[4:8]) == 0 ||
		!allZero(payload[23:PackedBlockPayloadHeaderSize]) {
		return PackedBlockView{}, corrupt("packed block header")
	}
	blockID := binary.LittleEndian.Uint32(payload[4:8])
	live := binary.LittleEndian.Uint64(payload[8:16])
	prefixLength := binary.LittleEndian.Uint16(payload[16:18])
	suffixLength := binary.LittleEndian.Uint16(payload[18:20])
	dataBytes := binary.LittleEndian.Uint16(payload[20:22])
	count := payload[22]
	if int(count) != bits.OnesCount64(live) ||
		len(payload) != PackedBlockPayloadHeaderSize+
			PackedBlockSlotDirectorySize+RawBlockOrderSize+int(dataBytes) ||
		uint32(prefixLength)+uint32(suffixLength) > uint32(dataBytes) {
		return PackedBlockView{}, corrupt("packed block length")
	}
	directory := payload[PackedBlockPayloadHeaderSize:]
	edges := int(prefixLength) + int(suffixLength)
	previousKeyEnd := -1
	presentCount := 0
	for slot := range RawBlockSlotCount {
		record := binary.LittleEndian.Uint32(directory[slot*PackedBlockSlotRecordSize:])
		present := live&(uint64(1)<<slot) != 0
		if !present {
			if record != packedBlockAbsentRecord {
				return PackedBlockView{}, corrupt("packed block absent slot")
			}
			continue
		}
		keyStart := int(uint16(record))
		keyEnd := int(uint16(record >> 16))
		if keyStart > keyEnd || keyEnd > int(dataBytes) ||
			presentCount == 0 && keyStart != edges ||
			presentCount != 0 && (keyStart < previousKeyEnd ||
				edges+keyStart-previousKeyEnd == 0) {
			return PackedBlockView{}, corrupt("packed block record")
		}
		previousKeyEnd = keyEnd
		presentCount++
	}
	if presentCount == 0 || previousKeyEnd > int(dataBytes) ||
		edges+int(dataBytes)-previousKeyEnd == 0 {
		return PackedBlockView{}, corrupt("packed block final record")
	}
	order := directory[PackedBlockSlotDirectorySize:]
	view := PackedBlockView{
		identity:  header.identity,
		payload:   payload,
		order:     order[:RawBlockOrderSize:RawBlockOrderSize],
		data:      order[RawBlockOrderSize:],
		blockID:   blockID,
		live:      live,
		count:     count,
		prefixEnd: prefixLength,
		suffixEnd: prefixLength + suffixLength,
	}
	var ordered uint64
	var previousKey []byte
	for rank := 0; rank < int(count); rank++ {
		slot := rawBlockOrder(view.order, rank)
		if slot >= RawBlockSlotCount || ordered&(uint64(1)<<slot) != 0 ||
			live&(uint64(1)<<slot) == 0 {
			return PackedBlockView{}, corrupt("packed block lexical slot")
		}
		ordered |= uint64(1) << slot
		key, ok := view.Key(slot)
		if !ok || rank != 0 && bytes.Compare(previousKey, key) >= 0 {
			return PackedBlockView{}, corrupt("packed block lexical order")
		}
		previousKey = key
	}
	if ordered != live || !rawBlockOrderTailZero(view.order, int(count)) {
		return PackedBlockView{}, corrupt("packed block lexical coverage")
	}
	return view, nil
}

// Key returns the exact borrowed key for one stable slot.
func (v PackedBlockView) Key(slot uint8) ([]byte, bool) {
	if slot >= RawBlockSlotCount || v.live&(uint64(1)<<slot) == 0 {
		return nil, false
	}
	directory := v.payload[PackedBlockPayloadHeaderSize:]
	record := binary.LittleEndian.Uint32(
		directory[int(slot)*PackedBlockSlotRecordSize:],
	)
	keyStart := int(uint16(record))
	keyEnd := int(uint16(record >> 16))
	return v.data[keyStart:keyEnd:keyEnd], true
}

// LowerBound returns the stable slot of the first complete key greater than or
// equal to target.
func (v PackedBlockView) LowerBound(target []byte) (uint8, bool) {
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		slot := rawBlockOrder(v.order, middle)
		key, ok := v.Key(slot)
		if !ok {
			return 0, false
		}
		if bytes.Compare(key, target) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == int(v.count) {
		return 0, false
	}
	return rawBlockOrder(v.order, low), true
}

// Iterator returns a lexical exact-key reader before the first row.
func (v PackedBlockView) Iterator() PackedBlockIter {
	return PackedBlockIter{view: v}
}

// Next returns the next stable slot and borrowed exact key.
func (it *PackedBlockIter) Next() (slot uint8, key []byte, ok bool) {
	if it == nil || it.rank >= it.view.count {
		return 0, nil, false
	}
	slot = rawBlockOrder(it.view.order, int(it.rank))
	it.rank++
	key, ok = it.view.Key(slot)
	return slot, key, ok
}

// AppendJSON verifies the complete key and reconstructs one JSON value into
// caller-owned space. It never exposes a shared decode buffer.
func (v PackedBlockView) AppendJSON(dst []byte, slot uint8, want string) ([]byte, bool) {
	if slot >= RawBlockSlotCount || v.live&(uint64(1)<<slot) == 0 {
		return dst, false
	}
	directory := v.payload[PackedBlockPayloadHeaderSize:]
	record := binary.LittleEndian.Uint32(directory[int(slot)*PackedBlockSlotRecordSize:])
	keyStart := int(uint16(record))
	keyEnd := int(uint16(record >> 16))
	if keyEnd-keyStart != len(want) || string(v.data[keyStart:keyEnd]) != want {
		return dst, false
	}
	middleEnd := len(v.data)
	after := v.live & bitsAbove(slot)
	if after != 0 {
		next := bits.TrailingZeros64(after)
		nextRecord := binary.LittleEndian.Uint32(
			directory[next*PackedBlockSlotRecordSize:],
		)
		middleEnd = int(uint16(nextRecord))
	}
	dst = append(dst, v.data[:v.prefixEnd]...)
	dst = append(dst, v.data[keyEnd:middleEnd]...)
	dst = append(dst, v.data[v.prefixEnd:v.suffixEnd]...)
	return dst, true
}

func commonJSONEdges(rows []RawRow) (prefix, suffix int, ok bool) {
	if len(rows) == 0 || len(rows[0].JSON) == 0 {
		return 0, 0, false
	}
	first := rows[0].JSON
	prefix = len(first)
	for _, row := range rows[1:] {
		prefix = min(prefix, commonPrefix(first[:prefix], row.JSON))
	}
	minimum := len(first)
	for _, row := range rows[1:] {
		minimum = min(minimum, len(row.JSON))
	}
	suffix = minimum - prefix
	for _, row := range rows[1:] {
		candidate := 0
		limit := min(suffix, len(row.JSON)-prefix)
		for candidate < limit &&
			first[len(first)-candidate-1] == row.JSON[len(row.JSON)-candidate-1] {
			candidate++
		}
		suffix = candidate
	}
	return prefix, suffix, true
}

func commonPrefix(a, b []byte) int {
	limit := min(len(a), len(b))
	index := 0
	for index+8 <= limit &&
		binary.LittleEndian.Uint64(a[index:index+8]) ==
			binary.LittleEndian.Uint64(b[index:index+8]) {
		index += 8
	}
	for index < limit && a[index] == b[index] {
		index++
	}
	return index
}
