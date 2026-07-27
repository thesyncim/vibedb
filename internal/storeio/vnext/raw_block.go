package vnext

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
)

const (
	RawBlockPayloadHeaderSize = 32
	RawBlockSlotCount         = 64
	// Each stable slot stores a uint16 row start and uint16 key end. JSON ends
	// at the next live slot's row start (or recordBytes for the final row).
	// This is half the dense directory bytes of two uint32 cumulative ends while
	// retaining a direct, varint-free exact-key lookup.
	RawBlockSlotRecordSize    = 4
	RawBlockSlotDirectorySize = RawBlockSlotCount * RawBlockSlotRecordSize
	// The lexical permutation stores one six-bit stable slot per rank. Keeping
	// order separate from identity means an insertion never relabels existing
	// route rows, while scans reuse the authoritative key bytes already present
	// in this block instead of duplicating complete keys in an ordered tree.
	RawBlockOrderBits  = 6
	RawBlockOrderSize  = RawBlockSlotCount * RawBlockOrderBits / 8
	RawBlockFixedBytes = FrameHeaderSize + FrameTrailerSize +
		RawBlockPayloadHeaderSize + RawBlockSlotDirectorySize +
		RawBlockOrderSize

	rawBlockVersion      = uint32(1)
	rawBlockAbsentRecord = uint32(0xffffffff)
)

// RawRow is one stable-slot record. Key and JSON are borrowed for EncodeRawBlock.
type RawRow struct {
	Slot uint8
	Key  []byte
	JSON []byte
}

// RawBlockView is a checksum- and structure-verified borrowed raw block.
type RawBlockView struct {
	identity    Identity
	payload     []byte
	order       []byte
	data        []byte
	blockID     uint32
	live        uint64
	count       uint8
	recordBytes uint16
}

// RawBlockIter is an allocation-free lexical iterator. Key and JSON remain
// borrowed from the immutable block until its owner releases the generation.
type RawBlockIter struct {
	view RawBlockView
	rank uint8
}

// RawBlockEncodedBytes returns the exact non-padding bytes for rows. Sparse
// blocks pay one fixed 256-byte slot directory and 48-byte lexical permutation;
// neither structure grows with key or JSON length.
func RawBlockEncodedBytes(rows []RawRow) (int, error) {
	bytes := RawBlockFixedBytes
	previous := -1
	for _, row := range rows {
		if int(row.Slot) <= previous || row.Slot >= RawBlockSlotCount ||
			uint64(len(row.Key)) > uint64(^uint32(0)) || len(row.JSON) == 0 {
			return 0, ErrInvalidFrame
		}
		bytes += len(row.Key) + len(row.JSON)
		previous = int(row.Slot)
	}
	if bytes > MaxSpan {
		return 0, ErrInvalidFrame
	}
	return bytes, nil
}

// EncodeRawBlock writes one canonical contiguous record block. Span may be any
// 4 KiB multiple through 64 KiB; it need not be a power of two.
func EncodeRawBlock(
	dst []byte,
	identity Identity,
	blockID uint32,
	rows []RawRow,
) ([]byte, error) {
	if blockID == 0 || len(rows) > RawBlockSlotCount || !validSpan(len(dst)) {
		return nil, ErrInvalidFrame
	}
	used, err := RawBlockEncodedBytes(rows)
	if err != nil || used > len(dst) {
		return nil, ErrInvalidFrame
	}
	recordBytes := used - RawBlockFixedBytes
	if recordBytes >= 1<<16 {
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
	payloadLength := RawBlockPayloadHeaderSize + RawBlockSlotDirectorySize +
		RawBlockOrderSize + recordBytes
	payload, err := initFrame(dst, identity, frameRawBlock, payloadLength)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], rawBlockVersion)
	binary.LittleEndian.PutUint32(payload[4:8], blockID)
	binary.LittleEndian.PutUint32(payload[16:20], uint32(recordBytes))
	binary.LittleEndian.PutUint16(payload[20:22], uint16(len(rows)))
	offsets := payload[RawBlockPayloadHeaderSize:]
	for slot := range RawBlockSlotCount {
		binary.LittleEndian.PutUint32(
			offsets[slot*RawBlockSlotRecordSize:],
			rawBlockAbsentRecord,
		)
	}
	lexical := offsets[RawBlockSlotDirectorySize:]
	for rank := range rows {
		putRawBlockOrder(lexical, rank, rows[order[rank]].Slot)
	}
	data := lexical[RawBlockOrderSize:]
	cursor := 0
	live := uint64(0)
	for _, row := range rows {
		live |= uint64(1) << row.Slot
		start := cursor
		cursor += copy(data[cursor:], row.Key)
		keyEnd := cursor
		cursor += copy(data[cursor:], row.JSON)
		binary.LittleEndian.PutUint32(
			offsets[int(row.Slot)*RawBlockSlotRecordSize:],
			uint32(start)|uint32(keyEnd)<<16,
		)
	}
	binary.LittleEndian.PutUint64(payload[8:16], live)
	if err := sealFrame(dst); err != nil {
		return nil, err
	}
	return dst, nil
}

// OpenRawBlock validates offsets and every record boundary once. Point lookups
// then use one bitmap probe, one trailing-zero operation, and two offset loads.
func OpenRawBlock(src []byte) (RawBlockView, error) {
	header, payload, err := openFrame(src, frameRawBlock)
	if err != nil {
		return RawBlockView{}, err
	}
	if len(payload) < RawBlockPayloadHeaderSize+RawBlockSlotDirectorySize+
		RawBlockOrderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != rawBlockVersion ||
		binary.LittleEndian.Uint32(payload[4:8]) == 0 ||
		!allZero(payload[22:RawBlockPayloadHeaderSize]) {
		return RawBlockView{}, corrupt("raw block header")
	}
	blockID := binary.LittleEndian.Uint32(payload[4:8])
	live := binary.LittleEndian.Uint64(payload[8:16])
	recordBytes32 := binary.LittleEndian.Uint32(payload[16:20])
	count := binary.LittleEndian.Uint16(payload[20:22])
	if count != uint16(bits.OnesCount64(live)) || recordBytes32 >= 1<<16 {
		return RawBlockView{}, corrupt("raw block count")
	}
	recordBytes := int(recordBytes32)
	wantLength := RawBlockPayloadHeaderSize + RawBlockSlotDirectorySize +
		RawBlockOrderSize + recordBytes
	if len(payload) != wantLength {
		return RawBlockView{}, corrupt("raw block length")
	}
	offsets := payload[RawBlockPayloadHeaderSize:]
	order := offsets[RawBlockSlotDirectorySize:]
	data := order[RawBlockOrderSize:]
	previousStart, previousKeyEnd, previousSlot := -1, -1, -1
	for slot := range RawBlockSlotCount {
		record := binary.LittleEndian.Uint32(offsets[slot*RawBlockSlotRecordSize:])
		present := live&(uint64(1)<<slot) != 0
		if !present {
			if record != rawBlockAbsentRecord {
				return RawBlockView{}, corrupt("raw block absent slot")
			}
			continue
		}
		start := int(uint16(record))
		keyEnd := int(uint16(record >> 16))
		if start > keyEnd || keyEnd >= recordBytes ||
			previousStart >= start || previousKeyEnd >= start ||
			previousStart < 0 && start != 0 {
			return RawBlockView{}, corrupt("raw block offsets")
		}
		previousStart, previousKeyEnd, previousSlot = start, keyEnd, slot
	}
	if previousSlot >= 0 {
		if previousKeyEnd >= recordBytes {
			return RawBlockView{}, corrupt("raw block final record")
		}
	} else if recordBytes != 0 {
		return RawBlockView{}, corrupt("raw block empty data")
	}
	view := RawBlockView{
		identity:    header.identity,
		payload:     payload,
		order:       order[:RawBlockOrderSize:RawBlockOrderSize],
		data:        data,
		blockID:     blockID,
		live:        live,
		count:       uint8(count),
		recordBytes: uint16(recordBytes),
	}
	var ordered uint64
	var previousKey []byte
	for rank := 0; rank < int(count); rank++ {
		slot := rawBlockOrder(view.order, rank)
		if slot >= RawBlockSlotCount || ordered&(uint64(1)<<slot) != 0 ||
			live&(uint64(1)<<slot) == 0 {
			return RawBlockView{}, corrupt("raw block lexical slot")
		}
		ordered |= uint64(1) << slot
		key, _, ok := view.Lookup(slot)
		if !ok || rank != 0 && bytes.Compare(previousKey, key) >= 0 {
			return RawBlockView{}, corrupt("raw block lexical order")
		}
		previousKey = key
	}
	if ordered != live || !rawBlockOrderTailZero(view.order, int(count)) {
		return RawBlockView{}, corrupt("raw block lexical coverage")
	}
	return view, nil
}

// Lookup returns borrowed exact key and JSON slices for stable slot.
func (v RawBlockView) Lookup(slot uint8) (key, json []byte, ok bool) {
	if slot >= RawBlockSlotCount || v.live&(uint64(1)<<slot) == 0 {
		return nil, nil, false
	}
	offsets := v.payload[RawBlockPayloadHeaderSize:]
	record := binary.LittleEndian.Uint32(offsets[int(slot)*RawBlockSlotRecordSize:])
	start := int(uint16(record))
	keyEnd := int(uint16(record >> 16))
	end := int(v.recordBytes)
	after := v.live & bitsAbove(slot)
	if after != 0 {
		next := bits.TrailingZeros64(after)
		nextRecord := binary.LittleEndian.Uint32(offsets[next*RawBlockSlotRecordSize:])
		end = int(uint16(nextRecord))
	}
	return v.data[start:keyEnd:keyEnd], v.data[keyEnd:end:end], true
}

// LookupKey verifies the full key in the record block and returns only JSON.
func (v RawBlockView) LookupKey(slot uint8, want string) ([]byte, bool) {
	if slot >= RawBlockSlotCount || v.live&(uint64(1)<<slot) == 0 {
		return nil, false
	}
	offsets := v.payload[RawBlockPayloadHeaderSize:]
	record := binary.LittleEndian.Uint32(offsets[int(slot)*RawBlockSlotRecordSize:])
	start := int(uint16(record))
	keyEnd := int(uint16(record >> 16))
	end := int(v.recordBytes)
	after := v.live & bitsAbove(slot)
	if after != 0 {
		next := bits.TrailingZeros64(after)
		nextRecord := binary.LittleEndian.Uint32(offsets[next*RawBlockSlotRecordSize:])
		end = int(uint16(nextRecord))
	}
	if keyEnd-start != len(want) || string(v.data[start:keyEnd]) != want {
		return nil, false
	}
	return v.data[keyEnd:end:end], true
}

// LowerBound returns the stable slot of the first complete key greater than or
// equal to target. It binary-searches at most 64 authoritative block records,
// with no reconstructed-key buffer and no allocation.
func (v RawBlockView) LowerBound(target []byte) (uint8, bool) {
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		slot := rawBlockOrder(v.order, middle)
		key, _, ok := v.Lookup(slot)
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

// Iterator returns a lexical forward reader positioned before the first row.
func (v RawBlockView) Iterator() RawBlockIter { return RawBlockIter{view: v} }

// Next returns the next stable slot and its borrowed complete key and JSON.
func (it *RawBlockIter) Next() (slot uint8, key, json []byte, ok bool) {
	if it == nil || it.rank >= it.view.count {
		return 0, nil, nil, false
	}
	slot = rawBlockOrder(it.view.order, int(it.rank))
	it.rank++
	key, json, ok = it.view.Lookup(slot)
	return slot, key, json, ok
}

func putRawBlockOrder(dst []byte, rank int, slot uint8) {
	offset := rank * RawBlockOrderBits
	at, shift := offset>>3, uint(offset&7)
	word := uint16(dst[at])
	if at+1 < len(dst) {
		word |= uint16(dst[at+1]) << 8
	}
	mask := uint16(RawBlockSlotCount-1) << shift
	word = word&^mask | uint16(slot)<<shift
	dst[at] = byte(word)
	if shift > 2 {
		dst[at+1] = byte(word >> 8)
	}
}

func rawBlockOrder(src []byte, rank int) uint8 {
	offset := rank * RawBlockOrderBits
	at, shift := offset>>3, uint(offset&7)
	word := uint16(src[at])
	if shift > 2 {
		word |= uint16(src[at+1]) << 8
	}
	return uint8(word>>shift) & (RawBlockSlotCount - 1)
}

func rawBlockOrderTailZero(src []byte, count int) bool {
	usedBits := count * RawBlockOrderBits
	for bit := usedBits; bit < RawBlockSlotCount*RawBlockOrderBits; bit++ {
		if src[bit>>3]&(byte(1)<<uint(bit&7)) != 0 {
			return false
		}
	}
	return true
}

func bitsAbove(slot uint8) uint64 {
	if slot == 63 {
		return 0
	}
	return ^uint64(0) << (slot + 1)
}

// GeometryPolicy selects an arbitrary-quantum physical span from exact encoded
// bytes. TargetFillPermille reserves mutation headroom without reintroducing
// power-of-two rounding.
type GeometryPolicy struct {
	TargetFillPermille uint16
	MinSpan            int
	MaxSpan            int
}

// SelectSpan returns the smallest 4 KiB multiple meeting the fill target.
func (p GeometryPolicy) SelectSpan(encodedBytes int) (int, error) {
	if p.TargetFillPermille == 0 {
		p.TargetFillPermille = 850
	}
	if p.MinSpan == 0 {
		p.MinSpan = Quantum
	}
	if p.MaxSpan == 0 {
		p.MaxSpan = MaxSpan
	}
	if p.TargetFillPermille > 1000 || !validSpan(p.MinSpan) || !validSpan(p.MaxSpan) ||
		p.MinSpan > p.MaxSpan || encodedBytes < RawBlockFixedBytes ||
		encodedBytes > p.MaxSpan {
		return 0, fmt.Errorf("%w: block geometry", ErrInvalidFrame)
	}
	targetBytes := (encodedBytes*1000 + int(p.TargetFillPermille) - 1) /
		int(p.TargetFillPermille)
	span := max(p.MinSpan, ((targetBytes+Quantum-1)/Quantum)*Quantum)
	if span > p.MaxSpan {
		span = ((encodedBytes + Quantum - 1) / Quantum) * Quantum
	}
	if span > p.MaxSpan {
		return 0, fmt.Errorf("%w: block span", ErrInvalidFrame)
	}
	return span, nil
}
