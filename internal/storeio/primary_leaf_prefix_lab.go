package storeio

import (
	"bytes"
	"errors"
	"fmt"
)

// PrimaryLeafPrefixLab is an isolated restart-prefix experiment for the
// promoted CommonPrimaryLeafNarrow layout. It deliberately has no page kind,
// production encoder hook, or durable format discriminator.
//
// The metadata geometry is the narrow common-primary-leaf geometry. Within
// each lexical record, one shared-prefix byte precedes the usual optional
// wide-key length byte and the key suffix. Restart ranks store shared=0.
const primaryLeafPrefixLabExtent = CommonPrimaryLeafWideBytes

var ErrPrimaryLeafPrefixLabCorrupt = errors.New(
	"vibejson: corrupt primary leaf prefix lab",
)

type PrimaryLeafPrefixLabView struct {
	seed    [16]byte
	payload []byte
	count   uint16
	restart uint8
	layout  commonPrimaryLeafLayout
}

// EncodePrimaryLeafPrefixLab encodes records already placed and sorted using
// the promoted narrow leaf's stable-slot rules.
func EncodePrimaryLeafPrefixLab(
	seed [16]byte, restart int, records []CommonPrimaryLeafRecord,
) ([]byte, error) {
	if seed == ([16]byte{}) ||
		(restart != 4 && restart != 8 && restart != 16) ||
		len(records) > CommonPrimaryLeafNarrowLive {
		return nil, fmt.Errorf("%w: prefix lab parameters", ErrInvalidWrite)
	}
	layout := commonPrimaryLeafLayoutFor(
		CommonPrimaryLeafNarrow, len(records), primaryLeafPrefixLabExtent,
	)
	heapEnd := layout.heapStart
	for rank := range records {
		record := &records[rank]
		valueBytes := commonPrimaryLeafValueBytes(record.Value)
		if len(record.Key) == 0 ||
			len(record.Key) > CommonPrimaryLeafMaxKeyBytes ||
			valueBytes == 0 ||
			rank != 0 && bytes.Compare(records[rank-1].Key, record.Key) >= 0 {
			return nil, fmt.Errorf("%w: prefix lab record", ErrInvalidWrite)
		}
		shared := 0
		if rank%restart != 0 {
			shared = primaryLeafPrefixLabShared(
				records[rank-1].Key, record.Key,
			)
		}
		heapEnd += 1 + len(record.Key) - shared + valueBytes
		if len(record.Key) >= commonPrimaryLeafEscapeLength {
			heapEnd++
		}
	}
	if heapEnd > primaryLeafPrefixLabExtent-PageHeaderSize-PageTrailerSize {
		return nil, ErrCommonPrimaryLeafNeedsWide
	}
	payload := make([]byte, heapEnd)
	payload[0] = byte(len(records))
	payload[2] = byte(restart)
	cursor := layout.heapStart
	stashCount := 0
	var occupied [4]uint64
	for rank := range records {
		record := &records[rank]
		slot := int(record.Slot)
		if slot >= CommonPrimaryLeafNarrowSlots {
			return nil, fmt.Errorf("%w: prefix lab slot", ErrInvalidWrite)
		}
		bit := uint64(1) << uint(slot&63)
		if occupied[slot>>6]&bit != 0 {
			return nil, fmt.Errorf("%w: duplicate prefix lab slot", ErrInvalidWrite)
		}
		occupied[slot>>6] |= bit
		hash := commonPrimaryLeafHash(seed, record.Key)
		if slot < CommonPrimaryLeafNormalSlots {
			if !commonPrimaryLeafNormalCandidate(hash, record.Slot) {
				return nil, fmt.Errorf("%w: prefix lab candidate", ErrInvalidWrite)
			}
			payload[layout.controlStart+slot] =
				commonPrimaryLeafControlLive | byte(hash>>57)
			payload[layout.normalRankStart+slot] = byte(rank)
		} else {
			stash := slot - CommonPrimaryLeafNormalSlots
			payload[layout.stashRankStart+stash] = byte(rank + 1)
			payload[layout.stashTagStart+stash] = byte(hash >> 56)
			commonPrimaryLeafFilterAdd(
				payload[layout.filterStart:layout.filterStart+layout.filterLen],
				hash,
			)
			stashCount++
		}
		commonPrimaryLeafPutKeyLength(payload, &layout, rank, len(record.Key))
		if record.Value.IsOverflow() {
			payload[layout.overflowStart+rank/8] |= byte(1) << uint(rank&7)
		}
		commonPrimaryLeafPutBoundary(payload, &layout, rank, uint16(cursor))
		shared := 0
		if rank%restart != 0 {
			shared = primaryLeafPrefixLabShared(
				records[rank-1].Key, record.Key,
			)
		}
		payload[cursor] = byte(shared)
		cursor++
		if len(record.Key) >= commonPrimaryLeafEscapeLength {
			payload[cursor] = byte(len(record.Key) - 1)
			cursor++
		}
		copy(payload[cursor:], record.Key[shared:])
		cursor += len(record.Key) - shared
		if record.Value.IsOverflow() {
			encodePageRef(payload[cursor:cursor+PageRefSize], record.Value.Overflow)
			cursor += PageRefSize
		} else {
			copy(payload[cursor:], record.Value.Inline)
			cursor += len(record.Value.Inline)
		}
	}
	commonPrimaryLeafPutBoundary(payload, &layout, len(records), uint16(cursor))
	commonPrimaryLeafBuildCheckpoints(payload, &layout, len(records)+1)
	payload[1] = byte(stashCount)
	return payload, nil
}

func primaryLeafPrefixLabShared(a, b []byte) int {
	n := min(len(a), len(b))
	if n > 255 {
		n = 255
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func OpenPrimaryLeafPrefixLab(
	payload []byte, seed [16]byte,
) (PrimaryLeafPrefixLabView, error) {
	if seed == ([16]byte{}) || len(payload) < commonPrimaryLeafPayloadHeader {
		return PrimaryLeafPrefixLabView{}, ErrPrimaryLeafPrefixLabCorrupt
	}
	restart := int(payload[2])
	if restart != 4 && restart != 8 && restart != 16 {
		return PrimaryLeafPrefixLabView{}, ErrPrimaryLeafPrefixLabCorrupt
	}
	count := int(payload[0])
	if count > CommonPrimaryLeafNarrowLive {
		return PrimaryLeafPrefixLabView{}, ErrPrimaryLeafPrefixLabCorrupt
	}
	layout := commonPrimaryLeafLayoutFor(
		CommonPrimaryLeafNarrow, count, primaryLeafPrefixLabExtent,
	)
	if layout.heapStart == 0 || len(payload) < layout.heapStart {
		return PrimaryLeafPrefixLabView{}, ErrPrimaryLeafPrefixLabCorrupt
	}
	v := PrimaryLeafPrefixLabView{
		seed: seed, payload: payload, count: uint16(count),
		restart: uint8(restart), layout: layout,
	}
	if !v.validate() {
		return PrimaryLeafPrefixLabView{}, ErrPrimaryLeafPrefixLabCorrupt
	}
	return v, nil
}

func (v *PrimaryLeafPrefixLabView) validate() bool {
	first, ok := v.boundary(0)
	if !ok || int(first) != v.layout.heapStart {
		return false
	}
	last, ok := v.boundary(int(v.count))
	if !ok || int(last) != len(v.payload) {
		return false
	}
	previousLen := 0
	var seen [4]uint64
	live, stash := 0, 0
	for slot := 0; slot < CommonPrimaryLeafNormalSlots; slot++ {
		control := v.payload[v.layout.controlStart+slot]
		if control == 0 {
			continue
		}
		rank := int(v.payload[v.layout.normalRankStart+slot])
		if rank >= int(v.count) {
			return false
		}
		bit := uint64(1) << uint(rank&63)
		if seen[rank>>6]&bit != 0 {
			return false
		}
		seen[rank>>6] |= bit
		live++
	}
	for index := 0; index < CommonPrimaryLeafNarrowSlots-CommonPrimaryLeafNormalSlots; index++ {
		code := v.payload[v.layout.stashRankStart+index]
		if code == 0 {
			continue
		}
		rank := int(code) - 1
		if rank >= int(v.count) {
			return false
		}
		bit := uint64(1) << uint(rank&63)
		if seen[rank>>6]&bit != 0 {
			return false
		}
		seen[rank>>6] |= bit
		live++
		stash++
	}
	if live != int(v.count) || stash != int(v.payload[1]) {
		return false
	}
	for rank := 0; rank < int(v.count); rank++ {
		start, end, ok := v.recordBounds(rank)
		if !ok || start >= end {
			return false
		}
		full := commonPrimaryLeafKeyCode(v.payload, &v.layout, rank)
		if full == 0 {
			return false
		}
		shared := int(v.payload[start])
		start++
		if full == commonPrimaryLeafEscapeLength {
			if start >= end {
				return false
			}
			full = int(v.payload[start]) + 1
			start++
		}
		if full > CommonPrimaryLeafMaxKeyBytes ||
			rank%int(v.restart) == 0 && shared != 0 ||
			shared > previousLen || shared > full ||
			start+full-shared >= end {
			return false
		}
		previousLen = full
	}
	var prior [CommonPrimaryLeafMaxKeyBytes]byte
	priorLen := 0
	for rank := 0; rank < int(v.count); rank++ {
		var key [CommonPrimaryLeafMaxKeyBytes]byte
		n, _, _, ok := v.reconstruct(rank, &key)
		if !ok || rank != 0 && bytes.Compare(prior[:priorLen], key[:n]) >= 0 {
			return false
		}
		copy(prior[:], key[:n])
		priorLen = n
	}
	return true
}

func (v *PrimaryLeafPrefixLabView) Len() int {
	if v == nil {
		return 0
	}
	return int(v.count)
}

func (v *PrimaryLeafPrefixLabView) EncodedBytes() int {
	if v == nil {
		return 0
	}
	return len(v.payload)
}

func (v *PrimaryLeafPrefixLabView) boundary(index int) (uint16, bool) {
	if index < 0 || index > int(v.count) {
		return 0, false
	}
	checkpoint := index / v.layout.checkpointStride
	base := checkpoint * v.layout.checkpointStride
	at := v.layout.checkpointStart + checkpoint*v.layout.checkpointWidth
	position := int(v.payload[at])
	if v.layout.checkpointWidth == 2 {
		position = int(v.payload[at]) | int(v.payload[at+1])<<8
	}
	for current := base; current < index; current++ {
		var ok bool
		position, ok = commonPrimaryLeafNextHighBitAt(
			v.payload, v.layout.highStart, v.layout.highBytes, position+1,
		)
		if !ok {
			return 0, false
		}
	}
	high := position - index
	if high < 0 {
		return 0, false
	}
	return uint16(high<<commonPrimaryLeafLowerBits) |
		uint16(v.payload[v.layout.lowStart+index]), true
}

func (v *PrimaryLeafPrefixLabView) recordBounds(rank int) (int, int, bool) {
	start, ok := v.boundary(rank)
	if !ok || rank < 0 || rank >= int(v.count) {
		return 0, 0, false
	}
	position := int(start>>commonPrimaryLeafLowerBits) + rank
	next, ok := commonPrimaryLeafNextHighBitAt(
		v.payload, v.layout.highStart, v.layout.highBytes, position+1,
	)
	if !ok {
		return 0, 0, false
	}
	end := uint16((next-rank-1)<<commonPrimaryLeafLowerBits) |
		uint16(v.payload[v.layout.lowStart+rank+1])
	return int(start), int(end), end > start
}

func (v *PrimaryLeafPrefixLabView) decodeEntry(
	rank int, key *[CommonPrimaryLeafMaxKeyBytes]byte, previousLen int,
) (keyLen, valueStart, end int, ok bool) {
	start, end, ok := v.recordBounds(rank)
	if !ok || start >= end {
		return 0, 0, 0, false
	}
	shared := int(v.payload[start])
	start++
	full := commonPrimaryLeafKeyCode(v.payload, &v.layout, rank)
	if full == commonPrimaryLeafEscapeLength {
		if start >= end {
			return 0, 0, 0, false
		}
		full = int(v.payload[start]) + 1
		start++
	}
	suffix := full - shared
	if full <= 0 || full > len(key) || shared > previousLen ||
		shared > full || start+suffix >= end {
		return 0, 0, 0, false
	}
	copy(key[shared:full], v.payload[start:start+suffix])
	return full, start + suffix, end, true
}

func (v *PrimaryLeafPrefixLabView) reconstruct(
	rank int, key *[CommonPrimaryLeafMaxKeyBytes]byte,
) (keyLen, valueStart, end int, ok bool) {
	if rank < 0 || rank >= int(v.count) {
		return 0, 0, 0, false
	}
	startRank := rank - rank%int(v.restart)
	n := 0
	for current := startRank; current <= rank; current++ {
		n, valueStart, end, ok = v.decodeEntry(current, key, n)
		if !ok {
			return 0, 0, 0, false
		}
	}
	return n, valueStart, end, true
}

func (v *PrimaryLeafPrefixLabView) Lookup(
	key []byte,
) (slot uint8, raw []byte, overflow, ok bool) {
	if v == nil || len(key) == 0 || len(key) > CommonPrimaryLeafMaxKeyBytes {
		return 0, nil, false, false
	}
	return v.LookupHashed(commonPrimaryLeafHash(v.seed, key), key)
}

func (v *PrimaryLeafPrefixLabView) LookupHashed(
	hash uint64, key []byte,
) (slot uint8, raw []byte, overflow, ok bool) {
	if v == nil || v.count == 0 {
		return 0, nil, false, false
	}
	want := commonPrimaryLeafControlLive | byte(hash>>57)
	first, second := commonPrimaryLeafGroups(hash)
	firstHome := uint8(hash>>16) & (commonPrimaryLeafGroupSize - 1)
	secondHome := uint8(hash>>20) & (commonPrimaryLeafGroupSize - 1)
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group, home := first, firstHome
		if groupIndex != 0 {
			group, home = second, secondHome
		}
		base := group * commonPrimaryLeafGroupSize
		for ordinal := uint8(0); ordinal < commonPrimaryLeafGroupSize; ordinal++ {
			candidate := base + (home+ordinal)&(commonPrimaryLeafGroupSize-1)
			if v.payload[v.layout.controlStart+int(candidate)] != want {
				continue
			}
			rank := int(v.payload[v.layout.normalRankStart+int(candidate)])
			if raw, overflow, ok = v.lookupRank(rank, key); ok {
				return candidate, raw, overflow, true
			}
		}
	}
	if v.payload[1] == 0 || !commonPrimaryLeafFilterMayContain(
		v.payload[v.layout.filterStart:v.layout.filterStart+v.layout.filterLen],
		hash,
	) {
		return 0, nil, false, false
	}
	tag := byte(hash >> 56)
	for stash := 0; stash < CommonPrimaryLeafNarrowSlots-CommonPrimaryLeafNormalSlots; stash++ {
		code := v.payload[v.layout.stashRankStart+stash]
		if code == 0 || v.payload[v.layout.stashTagStart+stash] != tag {
			continue
		}
		if raw, overflow, ok = v.lookupRank(int(code)-1, key); ok {
			return uint8(CommonPrimaryLeafNormalSlots + stash),
				raw, overflow, true
		}
	}
	return 0, nil, false, false
}

func (v *PrimaryLeafPrefixLabView) lookupRank(
	rank int, want []byte,
) ([]byte, bool, bool) {
	var key [CommonPrimaryLeafMaxKeyBytes]byte
	n, valueStart, end, ok := v.reconstruct(rank, &key)
	if !ok || !bytes.Equal(key[:n], want) {
		return nil, false, false
	}
	overflow := v.payload[v.layout.overflowStart+rank/8]&
		(byte(1)<<uint(rank&7)) != 0
	return v.payload[valueStart:end:end], overflow, true
}

func (v *PrimaryLeafPrefixLabView) LowerBound(want []byte) int {
	if v == nil {
		return 0
	}
	low, high := 0, int(v.count)
	var key [CommonPrimaryLeafMaxKeyBytes]byte
	for low < high {
		middle := int(uint(low+high) >> 1)
		n, _, _, _ := v.reconstruct(middle, &key)
		if bytes.Compare(key[:n], want) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

type PrimaryLeafPrefixLabIterator struct {
	view     *PrimaryLeafPrefixLabView
	rank     uint16
	keyLen   uint16
	bitPos   uint16
	offset   uint16
	key      [CommonPrimaryLeafMaxKeyBytes]byte
	finished bool
}

func (v *PrimaryLeafPrefixLabView) AllRows() PrimaryLeafPrefixLabIterator {
	return v.IteratorAt(0)
}

func (v *PrimaryLeafPrefixLabView) IteratorAt(
	rank int,
) PrimaryLeafPrefixLabIterator {
	it := PrimaryLeafPrefixLabIterator{view: v}
	if v == nil || rank < 0 || rank >= int(v.count) {
		it.finished = true
		return it
	}
	start := rank - rank%int(v.restart)
	for current := start; current < rank; current++ {
		n, _, _, ok := v.decodeEntry(current, &it.key, int(it.keyLen))
		if !ok {
			it.finished = true
			return it
		}
		it.keyLen = uint16(n)
	}
	offset, ok := v.boundary(rank)
	if !ok {
		it.finished = true
		return it
	}
	it.rank = uint16(rank)
	it.offset = offset
	it.bitPos = uint16(int(offset>>commonPrimaryLeafLowerBits) + rank)
	return it
}

func (it *PrimaryLeafPrefixLabIterator) NextRawBorrowed() (
	key, raw []byte, overflow, ok bool,
) {
	if it == nil || it.finished || int(it.rank) >= int(it.view.count) {
		return nil, nil, false, false
	}
	rank := int(it.rank)
	nextPosition, ok := commonPrimaryLeafNextHighBitAt(
		it.view.payload, it.view.layout.highStart, it.view.layout.highBytes,
		int(it.bitPos)+1,
	)
	if !ok {
		it.finished = true
		return nil, nil, false, false
	}
	nextRank := rank + 1
	end := uint16(
		(nextPosition-nextRank)<<commonPrimaryLeafLowerBits,
	) | uint16(it.view.payload[it.view.layout.lowStart+nextRank])
	start := int(it.offset)
	shared := int(it.view.payload[start])
	start++
	n := commonPrimaryLeafKeyCode(
		it.view.payload, &it.view.layout, rank,
	)
	if n == commonPrimaryLeafEscapeLength {
		n = int(it.view.payload[start]) + 1
		start++
	}
	suffix := n - shared
	if n <= 0 || n > len(it.key) || shared > int(it.keyLen) ||
		shared > n || start+suffix >= int(end) {
		it.finished = true
		return nil, nil, false, false
	}
	copy(it.key[shared:n], it.view.payload[start:start+suffix])
	valueStart := start + suffix
	it.keyLen = uint16(n)
	it.rank++
	it.bitPos = uint16(nextPosition)
	it.offset = end
	if it.rank >= it.view.count {
		it.finished = true
	}
	overflow = it.view.payload[it.view.layout.overflowStart+rank/8]&
		(byte(1)<<uint(rank&7)) != 0
	return it.key[:n:n],
		it.view.payload[valueStart:int(end):int(end)], overflow, true
}
