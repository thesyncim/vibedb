package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

// The hash-bucket lab is deliberately isolated from the durable Store format.
// It tests whether clustering exact keys and values behind one hash-routed page
// can replace both a separate fingerprint leaf and a document-page location.
//
// The four-byte descriptor is exact, but only because records are packed in
// ascending stable-slot order:
//
//	bits  0..13  absolute record offset
//	bits 14..21  key length
//	bits 22..27  six-bit keyed-hash tag
//	bit      28  live
//	bit      29  overflow reference
//	bits 30..31  reserved
//
// Value length is the next live slot's offset minus this offset and key length,
// or heapEnd for the final live slot. Trying to add an explicit value length to
// 14+8+6 bits does not fit in 32 bits; the canonical packing invariant is what
// makes the descriptor honest.
const (
	HashBucketLabPageSize       = 16 << 10
	HashBucketLabSlotCount      = 256
	HashBucketLabDescriptorSize = 4
	HashBucketLabHeaderSize     = 64
	HashBucketLabBitmapSize     = HashBucketLabSlotCount / 8
	HashBucketLabTrailerSize    = 8
	HashBucketLabHeapStart      = HashBucketLabHeaderSize +
		HashBucketLabBitmapSize +
		HashBucketLabSlotCount*HashBucketLabDescriptorSize
	HashBucketLabMetadataBytes = HashBucketLabHeapStart + HashBucketLabTrailerSize
	HashBucketLabMaxOffset     = 1<<14 - 1
	HashBucketLabMaxKeyLength  = 1<<8 - 1
	// Overflow references are opaque in the lab. Fixing their width makes an
	// admitted overflow descriptor structurally unambiguous.
	HashBucketLabOverflowReferenceSize = 16

	hashBucketLabBitmapStart     = HashBucketLabHeaderSize
	hashBucketLabDescriptorStart = hashBucketLabBitmapStart + HashBucketLabBitmapSize
	hashBucketLabTrailerStart    = HashBucketLabPageSize - HashBucketLabTrailerSize

	hashBucketLabOffsetMask   = uint32(1<<14 - 1)
	hashBucketLabKeyMask      = uint32(1<<8-1) << 14
	hashBucketLabTagMask      = uint32(1<<6-1) << 22
	hashBucketLabLiveFlag     = uint32(1) << 28
	hashBucketLabOverflowFlag = uint32(1) << 29
	hashBucketLabReservedMask = uint32(3) << 30

	hashBucketLabMagic = "HBLAB001"
)

var (
	// ErrHashBucketLabCorrupt reports a checksum-valid but non-canonical lab
	// bucket, as well as ordinary checksum/header corruption.
	ErrHashBucketLabCorrupt = errors.New("vibejson: corrupt hash-bucket lab page")
	// ErrHashBucketLabNeedsWide is the explicit signal that a key cannot fit
	// the descriptor's exact eight-bit key length.
	ErrHashBucketLabNeedsWide = errors.New("vibejson: hash-bucket lab key needs wide descriptor")
	// ErrHashBucketLabNeedsOverflow is the explicit signal that an inline
	// record cannot fit this bucket and should be externalized or split.
	ErrHashBucketLabNeedsOverflow = errors.New("vibejson: hash-bucket lab value needs overflow or split")
	// ErrHashBucketLabNoSpace reports a bucket that cannot admit another
	// inline record without a split.
	ErrHashBucketLabNoSpace = errors.New("vibejson: hash-bucket lab page is full")
	// ErrHashBucketLabKeyNotFound is returned when a slot is empty or its
	// complete key does not match. Hash tags are never authoritative.
	ErrHashBucketLabKeyNotFound = errors.New("vibejson: hash-bucket lab key not found")
)

// HashBucketLabHeader is the identity and keyed-hash seed of one isolated lab
// bucket. Generation and Seed must be non-zero. BucketID zero is valid.
type HashBucketLabHeader struct {
	BucketID   uint64
	Generation uint64
	Seed       [16]byte
}

// HashBucketLabRecord is deterministic encoder input. Slot is a stable logical
// address within the bucket. Overflow values must contain exactly one opaque
// HashBucketLabOverflowReferenceSize-byte reference.
type HashBucketLabRecord struct {
	Slot     uint8
	Key      []byte
	Value    []byte
	Overflow bool
}

// HashBucketLabRow is one borrowed record produced in canonical
// (BucketID, Slot) order. This is stable posting order, not lexical key order.
type HashBucketLabRow struct {
	BucketID uint64
	Slot     uint8
	Key      []byte
	Value    []byte
	Overflow bool
}

// HashBucketLabView is a checksum- and structure-admitted borrowed page.
// Resident lookups, scans, updates, and deletes allocate nothing.
type HashBucketLabView struct {
	header  HashBucketLabHeader
	page    []byte
	live    [4]uint64
	count   uint16
	heapEnd uint16
}

// HashBucketLabIterator visits one admitted bucket in canonical stable-slot
// order. It is not a range-scan cursor: a production lexical leaf needs a
// separate slot-to-rank mapping. Its slices borrow the page and are invalidated
// by a mutation.
type HashBucketLabIterator struct {
	page      []byte
	bucketID  uint64
	heapEnd   uint16
	nextSlot  uint16
	nextStart uint16
}

type hashBucketLabPlacer struct {
	records  []HashBucketLabRecord
	seed     [16]byte
	owner    [HashBucketLabSlotCount]int16
	assigned [HashBucketLabSlotCount]uint8
	visited  [HashBucketLabSlotCount]uint16
	epoch    uint16
}

// HashBucketLabMetadataBytesPerLiveKey reports fixed page metadata, including
// the checksum trailer, amortized over live keys. It deliberately does not
// pretend unused data capacity is metadata.
func HashBucketLabMetadataBytesPerLiveKey(live int) float64 {
	if live <= 0 || live > HashBucketLabSlotCount {
		return 0
	}
	return float64(HashBucketLabMetadataBytes) / float64(live)
}

// PlaceHashBucketLabRecords assigns each record to one of its two deterministic
// 16-slot candidate groups. The augmenting-path matcher reaches a placement
// whenever one exists, rather than relying on insertion order or leaving probe
// tombstones. It mutates only Slot after finding a complete placement, so a
// failure leaves caller records unchanged.
//
// Once encoded, slots are stable across page-local update and delete. A future
// bucket split may deliberately assign a new bucket-local identity.
func PlaceHashBucketLabRecords(seed [16]byte, records []HashBucketLabRecord) error {
	if seed == ([16]byte{}) || len(records) > HashBucketLabSlotCount {
		return fmt.Errorf("%w: hash-bucket lab placement seed or count", ErrInvalidWrite)
	}
	placer := hashBucketLabPlacer{records: records, seed: seed}
	for slot := range placer.owner {
		placer.owner[slot] = -1
	}
	for index := range records {
		if len(records[index].Key) == 0 {
			return fmt.Errorf("%w: empty hash-bucket lab placement key", ErrInvalidWrite)
		}
		for previous := 0; previous < index; previous++ {
			if bytes.Equal(records[previous].Key, records[index].Key) {
				return fmt.Errorf("%w: duplicate hash-bucket lab key", ErrInvalidWrite)
			}
		}
		placer.epoch++
		if !placer.place(index) {
			return fmt.Errorf("%w: two candidate groups exhausted", ErrHashBucketLabNoSpace)
		}
	}
	for index := range records {
		records[index].Slot = placer.assigned[index]
	}
	return nil
}

func (p *hashBucketLabPlacer) place(index int) bool {
	hash := KeyHashBytes(p.seed, p.records[index].Key)
	for ordinal := 0; ordinal < 32; ordinal++ {
		slot := hashBucketLabCandidate(hash, ordinal)
		if p.visited[slot] == p.epoch {
			continue
		}
		p.visited[slot] = p.epoch
		previous := p.owner[slot]
		if previous < 0 || p.place(int(previous)) {
			p.owner[slot] = int16(index)
			p.assigned[index] = slot
			return true
		}
	}
	return false
}

func hashBucketLabCandidateGroups(hash uint64) (first, second uint8) {
	first = uint8(hash) & 0xf0
	second = uint8(hash>>8) & 0xf0
	if second == first {
		// The alternative half-page group remains deterministic while ensuring
		// every key has exactly 32 distinct candidates.
		second ^= 0x80
	}
	return first, second
}

func hashBucketLabCandidate(hash uint64, ordinal int) uint8 {
	first, second := hashBucketLabCandidateGroups(hash)
	group := first
	home := uint8(hash>>16) & 0x0f
	if ordinal >= 16 {
		group = second
		home = uint8(hash>>20) & 0x0f
		ordinal -= 16
	}
	return group | (home+uint8(ordinal))&0x0f
}

func hashBucketLabSlotIsCandidate(hash uint64, slot uint8) bool {
	first, second := hashBucketLabCandidateGroups(hash)
	return slot&0xf0 == first || slot&0xf0 == second
}

// EncodeHashBucketLab creates one canonical image. Input order is irrelevant:
// records are found by stable slot and emitted in slot order without sorting or
// allocating. Duplicate slots and invalid identities fail closed.
func EncodeHashBucketLab(dst []byte, header HashBucketLabHeader, records []HashBucketLabRecord) ([]byte, error) {
	if len(dst) < HashBucketLabPageSize {
		return nil, fmt.Errorf("%w: short destination", ErrInvalidWrite)
	}
	if header.Generation == 0 || header.Seed == ([16]byte{}) ||
		len(records) > HashBucketLabSlotCount {
		return nil, fmt.Errorf("%w: hash-bucket lab identity or count", ErrInvalidWrite)
	}
	page := dst[:HashBucketLabPageSize]
	clear(page)
	copy(page[0:8], hashBucketLabMagic)
	binary.LittleEndian.PutUint32(page[8:12], DevelopmentFormatVersion)
	binary.LittleEndian.PutUint16(page[12:14], HashBucketLabHeaderSize)
	binary.LittleEndian.PutUint16(page[14:16], HashBucketLabSlotCount)
	binary.LittleEndian.PutUint16(page[16:18], uint16(len(records)))
	binary.LittleEndian.PutUint16(page[20:22], HashBucketLabHeapStart)
	binary.LittleEndian.PutUint64(page[24:32], header.BucketID)
	binary.LittleEndian.PutUint64(page[32:40], header.Generation)
	copy(page[40:56], header.Seed[:])

	cursor := HashBucketLabHeapStart
	encoded := 0
	for slot := 0; slot < HashBucketLabSlotCount; slot++ {
		index := -1
		for candidate := range records {
			if int(records[candidate].Slot) == slot {
				if index >= 0 {
					return nil, fmt.Errorf("%w: duplicate hash-bucket lab slot", ErrInvalidWrite)
				}
				index = candidate
			}
		}
		if index < 0 {
			continue
		}
		record := &records[index]
		if len(record.Key) == 0 {
			return nil, fmt.Errorf("%w: empty hash-bucket lab key", ErrInvalidWrite)
		}
		if len(record.Key) > HashBucketLabMaxKeyLength {
			return nil, fmt.Errorf("%w: key length %d", ErrHashBucketLabNeedsWide, len(record.Key))
		}
		if len(record.Value) == 0 {
			return nil, fmt.Errorf("%w: empty hash-bucket lab value", ErrInvalidWrite)
		}
		if record.Overflow && len(record.Value) != HashBucketLabOverflowReferenceSize {
			return nil, fmt.Errorf("%w: overflow reference width", ErrInvalidWrite)
		}
		recordLength := len(record.Key) + len(record.Value)
		if recordLength > hashBucketLabTrailerStart-HashBucketLabHeapStart {
			return nil, fmt.Errorf("%w: record length %d", ErrHashBucketLabNeedsOverflow, recordLength)
		}
		if cursor > hashBucketLabTrailerStart-recordLength {
			return nil, fmt.Errorf("%w: %w", ErrHashBucketLabNoSpace, ErrHashBucketLabNeedsOverflow)
		}
		if cursor > HashBucketLabMaxOffset {
			return nil, fmt.Errorf("%w: heap offset %d", ErrHashBucketLabNeedsWide, cursor)
		}
		hash := KeyHashBytes(header.Seed, record.Key)
		if !hashBucketLabSlotIsCandidate(hash, record.Slot) {
			return nil, fmt.Errorf("%w: slot is outside its two candidate groups", ErrInvalidWrite)
		}
		for previous := range records {
			if previous == index {
				continue
			}
			if bytes.Equal(records[previous].Key, record.Key) {
				return nil, fmt.Errorf("%w: duplicate hash-bucket lab key", ErrInvalidWrite)
			}
		}
		tag := uint32(hash >> 58)
		flags := hashBucketLabLiveFlag
		if record.Overflow {
			flags |= hashBucketLabOverflowFlag
		}
		descriptor := uint32(cursor) |
			uint32(len(record.Key))<<14 |
			tag<<22 |
			flags
		putHashBucketLabDescriptor(page, uint8(slot), descriptor)
		page[hashBucketLabBitmapStart+slot/8] |= byte(1) << uint(slot&7)
		copy(page[cursor:cursor+len(record.Key)], record.Key)
		cursor += len(record.Key)
		copy(page[cursor:cursor+len(record.Value)], record.Value)
		cursor += len(record.Value)
		encoded++
	}
	if encoded != len(records) {
		return nil, fmt.Errorf("%w: hash-bucket lab slot coverage", ErrInvalidWrite)
	}
	binary.LittleEndian.PutUint16(page[22:24], uint16(cursor))
	sealHashBucketLab(page)
	return page, nil
}

// OpenHashBucketLab verifies the checksum, fixed layout, live bitmap,
// descriptor flags, canonical packed offsets, exact overflow widths, keyed
// tags, zero padding, and record count once.
func OpenHashBucketLab(src []byte) (HashBucketLabView, error) {
	if len(src) < HashBucketLabPageSize {
		return HashBucketLabView{}, fmt.Errorf("%w: short page", ErrHashBucketLabCorrupt)
	}
	page := src[:HashBucketLabPageSize:HashBucketLabPageSize]
	checksum := binary.LittleEndian.Uint32(page[hashBucketLabTrailerStart : hashBucketLabTrailerStart+4])
	if binary.LittleEndian.Uint32(page[hashBucketLabTrailerStart+4:]) != ^checksum ||
		PageChecksum(page[:hashBucketLabTrailerStart]) != checksum {
		return HashBucketLabView{}, fmt.Errorf("%w: checksum", ErrHashBucketLabCorrupt)
	}
	var seed [16]byte
	copy(seed[:], page[40:56])
	count := binary.LittleEndian.Uint16(page[16:18])
	heapEnd := binary.LittleEndian.Uint16(page[22:24])
	if string(page[0:8]) != hashBucketLabMagic ||
		binary.LittleEndian.Uint32(page[8:12]) != DevelopmentFormatVersion ||
		binary.LittleEndian.Uint16(page[12:14]) != HashBucketLabHeaderSize ||
		binary.LittleEndian.Uint16(page[14:16]) != HashBucketLabSlotCount ||
		count > HashBucketLabSlotCount ||
		binary.LittleEndian.Uint16(page[18:20]) != 0 ||
		binary.LittleEndian.Uint16(page[20:22]) != HashBucketLabHeapStart ||
		heapEnd < HashBucketLabHeapStart || heapEnd > hashBucketLabTrailerStart ||
		binary.LittleEndian.Uint64(page[32:40]) == 0 ||
		seed == ([16]byte{}) || !allZero(page[56:64]) {
		return HashBucketLabView{}, fmt.Errorf("%w: header", ErrHashBucketLabCorrupt)
	}

	view := HashBucketLabView{
		header: HashBucketLabHeader{
			BucketID:   binary.LittleEndian.Uint64(page[24:32]),
			Generation: binary.LittleEndian.Uint64(page[32:40]),
			Seed:       seed,
		},
		page:    page,
		count:   count,
		heapEnd: heapEnd,
	}
	for word := range view.live {
		start := hashBucketLabBitmapStart + word*8
		view.live[word] = binary.LittleEndian.Uint64(page[start : start+8])
	}
	if bits.OnesCount64(view.live[0])+bits.OnesCount64(view.live[1])+
		bits.OnesCount64(view.live[2])+bits.OnesCount64(view.live[3]) != int(count) {
		return HashBucketLabView{}, fmt.Errorf("%w: live count", ErrHashBucketLabCorrupt)
	}

	previousSlot := -1
	previousStart := 0
	previousKeyLength := 0
	previousOverflow := false
	previousTag := uint32(0)
	seen := 0
	for slot := 0; slot < HashBucketLabSlotCount; slot++ {
		descriptor := hashBucketLabDescriptor(page, uint8(slot))
		live := view.live[slot>>6]&(uint64(1)<<uint(slot&63)) != 0
		if !live {
			if descriptor != 0 {
				return HashBucketLabView{}, fmt.Errorf("%w: dead descriptor", ErrHashBucketLabCorrupt)
			}
			continue
		}
		if descriptor&hashBucketLabLiveFlag == 0 ||
			descriptor&hashBucketLabReservedMask != 0 {
			return HashBucketLabView{}, fmt.Errorf("%w: descriptor flags", ErrHashBucketLabCorrupt)
		}
		start := int(descriptor & hashBucketLabOffsetMask)
		keyLength := int(descriptor&hashBucketLabKeyMask) >> 14
		overflow := descriptor&hashBucketLabOverflowFlag != 0
		if keyLength == 0 || start < HashBucketLabHeapStart ||
			start > HashBucketLabMaxOffset {
			return HashBucketLabView{}, fmt.Errorf("%w: descriptor bounds", ErrHashBucketLabCorrupt)
		}
		if previousSlot < 0 {
			if start != HashBucketLabHeapStart {
				return HashBucketLabView{}, fmt.Errorf("%w: first record offset", ErrHashBucketLabCorrupt)
			}
		} else if err := validateHashBucketLabRecord(
			page, view.header.Seed, previousStart, start,
			previousKeyLength, previousOverflow, previousTag, uint8(previousSlot),
		); err != nil {
			return HashBucketLabView{}, err
		}
		previousSlot = slot
		previousStart = start
		previousKeyLength = keyLength
		previousOverflow = overflow
		previousTag = (descriptor & hashBucketLabTagMask) >> 22
		seen++
	}
	if previousSlot < 0 {
		if heapEnd != HashBucketLabHeapStart {
			return HashBucketLabView{}, fmt.Errorf("%w: empty heap end", ErrHashBucketLabCorrupt)
		}
	} else if err := validateHashBucketLabRecord(
		page, view.header.Seed, previousStart, int(heapEnd),
		previousKeyLength, previousOverflow, previousTag, uint8(previousSlot),
	); err != nil {
		return HashBucketLabView{}, err
	}
	if seen != int(count) || !allZero(page[int(heapEnd):hashBucketLabTrailerStart]) {
		return HashBucketLabView{}, fmt.Errorf("%w: count or non-zero padding", ErrHashBucketLabCorrupt)
	}
	return view, nil
}

func validateHashBucketLabRecord(page []byte, seed [16]byte, start, end, keyLength int, overflow bool, tag uint32, slot uint8) error {
	if end <= start+keyLength || end > hashBucketLabTrailerStart {
		return fmt.Errorf("%w: record interval", ErrHashBucketLabCorrupt)
	}
	if overflow && end-start-keyLength != HashBucketLabOverflowReferenceSize {
		return fmt.Errorf("%w: overflow width", ErrHashBucketLabCorrupt)
	}
	key := page[start : start+keyLength]
	hash := KeyHashBytes(seed, key)
	if uint32(hash>>58) != tag || !hashBucketLabSlotIsCandidate(hash, slot) {
		return fmt.Errorf("%w: keyed tag or candidate group", ErrHashBucketLabCorrupt)
	}
	return nil
}

// Header returns the bucket identity.
func (v HashBucketLabView) Header() HashBucketLabHeader { return v.header }

// Len returns the number of live stable slots.
func (v HashBucketLabView) Len() int { return int(v.count) }

// Lookup hashes and probes one exact key. It touches exactly two deterministic
// 16-slot groups (two 64-byte descriptor regions), not the full 1 KiB table.
// Deletion needs no tombstone because an empty slot never terminates either
// bounded group. Only matching six-bit tags trigger complete key comparisons.
func (v HashBucketLabView) Lookup(key []byte) (slot uint8, value []byte, overflow, ok bool) {
	return v.LookupHashed(KeyHashBytes(v.header.Seed, key), key)
}

// LookupHashed is Lookup for a hash already computed by a radix router.
func (v HashBucketLabView) LookupHashed(hash uint64, key []byte) (slot uint8, value []byte, overflow, ok bool) {
	if len(key) == 0 || len(key) > HashBucketLabMaxKeyLength || v.count == 0 {
		return 0, nil, false, false
	}
	tagAndLive := uint32(hash>>58)<<22 | hashBucketLabLiveFlag
	first, second := hashBucketLabCandidateGroups(hash)
	firstHome := uint8(hash>>16) & 0x0f
	secondHome := uint8(hash>>20) & 0x0f
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group, home := first, firstHome
		if groupIndex != 0 {
			group, home = second, secondHome
		}
		for ordinal := uint8(0); ordinal < 16; ordinal++ {
			candidate := group | (home+ordinal)&0x0f
			descriptor := hashBucketLabDescriptor(v.page, candidate)
			if descriptor&(hashBucketLabTagMask|hashBucketLabLiveFlag) != tagAndLive ||
				int(descriptor&hashBucketLabKeyMask)>>14 != len(key) {
				continue
			}
			start, end, keyLength, candidateOverflow := v.recordBounds(candidate, descriptor)
			if bytes.Equal(v.page[start:start+keyLength], key) {
				return candidate, v.page[start+keyLength : end : end], candidateOverflow, true
			}
		}
	}
	return 0, nil, false, false
}

// AllRows returns a zero-allocation canonical (BucketID, Slot) iterator.
func (v HashBucketLabView) AllRows() HashBucketLabIterator {
	iterator := HashBucketLabIterator{
		page:     v.page,
		bucketID: v.header.BucketID,
		heapEnd:  v.heapEnd,
		nextSlot: HashBucketLabSlotCount,
	}
	for slot := 0; slot < HashBucketLabSlotCount; slot++ {
		descriptor := hashBucketLabDescriptor(v.page, uint8(slot))
		if descriptor&hashBucketLabLiveFlag != 0 {
			iterator.nextSlot = uint16(slot)
			iterator.nextStart = uint16(descriptor & hashBucketLabOffsetMask)
			break
		}
	}
	return iterator
}

// Next returns the next borrowed row.
func (it *HashBucketLabIterator) Next() (HashBucketLabRow, bool) {
	bucketID, slot, key, value, overflow, ok := it.NextBorrowed()
	return HashBucketLabRow{
		BucketID: bucketID,
		Slot:     slot,
		Key:      key,
		Value:    value,
		Overflow: overflow,
	}, ok
}

// NextBorrowed is the lowest-overhead canonical scan API. Returning fields
// directly lets a query loop avoid constructing the convenience Row value.
func (it *HashBucketLabIterator) NextBorrowed() (bucketID uint64, slot uint8, key, value []byte, overflow, ok bool) {
	if it == nil || it.nextSlot == HashBucketLabSlotCount {
		return 0, 0, nil, nil, false, false
	}
	slot = uint8(it.nextSlot)
	start := int(it.nextStart)
	descriptor := hashBucketLabDescriptor(it.page, slot)
	keyLength := int(descriptor&hashBucketLabKeyMask) >> 14
	overflow = descriptor&hashBucketLabOverflowFlag != 0
	end := int(it.heapEnd)
	it.nextSlot = HashBucketLabSlotCount
	for candidate := int(slot) + 1; candidate < HashBucketLabSlotCount; candidate++ {
		next := hashBucketLabDescriptor(it.page, uint8(candidate))
		if next&hashBucketLabLiveFlag != 0 {
			it.nextSlot = uint16(candidate)
			it.nextStart = uint16(next & hashBucketLabOffsetMask)
			end = int(it.nextStart)
			break
		}
	}
	return it.bucketID, slot,
		it.page[start : start+keyLength : start+keyLength],
		it.page[start+keyLength : end : end],
		overflow, true
}

// Update replaces one exact key's value inside the same page. Growing or
// shrinking shifts only later records and rewrites their offsets; stable slots
// do not change. The resulting image is byte-identical to a fresh canonical
// encode with the same rows.
func (v *HashBucketLabView) Update(slot uint8, key, value []byte, overflow bool) error {
	if len(value) == 0 || overflow && len(value) != HashBucketLabOverflowReferenceSize {
		return fmt.Errorf("%w: hash-bucket lab update value", ErrInvalidWrite)
	}
	descriptor := hashBucketLabDescriptor(v.page, slot)
	if descriptor&hashBucketLabLiveFlag == 0 {
		return ErrHashBucketLabKeyNotFound
	}
	start, end, keyLength, _ := v.recordBounds(slot, descriptor)
	if !bytes.Equal(v.page[start:start+keyLength], key) {
		return ErrHashBucketLabKeyNotFound
	}
	oldValueLength := end - start - keyLength
	delta := len(value) - oldValueLength
	oldHeapEnd := int(v.heapEnd)
	newHeapEnd := oldHeapEnd + delta
	if newHeapEnd > hashBucketLabTrailerStart {
		return fmt.Errorf("%w: %w", ErrHashBucketLabNoSpace, ErrHashBucketLabNeedsOverflow)
	}
	if delta != 0 {
		copy(v.page[end+delta:newHeapEnd], v.page[end:oldHeapEnd])
		for candidate := int(slot) + 1; candidate < HashBucketLabSlotCount; candidate++ {
			next := hashBucketLabDescriptor(v.page, uint8(candidate))
			if next&hashBucketLabLiveFlag == 0 {
				continue
			}
			offset := int(next&hashBucketLabOffsetMask) + delta
			next = next&^hashBucketLabOffsetMask | uint32(offset)
			putHashBucketLabDescriptor(v.page, uint8(candidate), next)
		}
	}
	copy(v.page[start+keyLength:start+keyLength+len(value)], value)
	if delta < 0 {
		clear(v.page[newHeapEnd:oldHeapEnd])
	}
	flags := descriptor &^ hashBucketLabOverflowFlag
	if overflow {
		flags |= hashBucketLabOverflowFlag
	}
	putHashBucketLabDescriptor(v.page, slot, flags)
	v.heapEnd = uint16(newHeapEnd)
	binary.LittleEndian.PutUint16(v.page[22:24], v.heapEnd)
	sealHashBucketLab(v.page)
	return nil
}

// Delete removes one exact key, compacts the local heap, and clears both the
// descriptor and live bit. Other stable slots do not move and no tombstone is
// retained.
func (v *HashBucketLabView) Delete(slot uint8, key []byte) error {
	descriptor := hashBucketLabDescriptor(v.page, slot)
	if descriptor&hashBucketLabLiveFlag == 0 {
		return ErrHashBucketLabKeyNotFound
	}
	start, end, keyLength, _ := v.recordBounds(slot, descriptor)
	if !bytes.Equal(v.page[start:start+keyLength], key) {
		return ErrHashBucketLabKeyNotFound
	}
	oldHeapEnd := int(v.heapEnd)
	removed := end - start
	newHeapEnd := oldHeapEnd - removed
	copy(v.page[start:newHeapEnd], v.page[end:oldHeapEnd])
	clear(v.page[newHeapEnd:oldHeapEnd])
	for candidate := int(slot) + 1; candidate < HashBucketLabSlotCount; candidate++ {
		next := hashBucketLabDescriptor(v.page, uint8(candidate))
		if next&hashBucketLabLiveFlag == 0 {
			continue
		}
		offset := int(next&hashBucketLabOffsetMask) - removed
		next = next&^hashBucketLabOffsetMask | uint32(offset)
		putHashBucketLabDescriptor(v.page, uint8(candidate), next)
	}
	putHashBucketLabDescriptor(v.page, slot, 0)
	word := int(slot >> 6)
	bit := uint(slot & 63)
	v.live[word] &^= uint64(1) << bit
	bitmap := hashBucketLabBitmapStart + word*8
	binary.LittleEndian.PutUint64(v.page[bitmap:bitmap+8], v.live[word])
	v.count--
	v.heapEnd = uint16(newHeapEnd)
	binary.LittleEndian.PutUint16(v.page[16:18], v.count)
	binary.LittleEndian.PutUint16(v.page[22:24], v.heapEnd)
	sealHashBucketLab(v.page)
	return nil
}

func (v HashBucketLabView) recordBounds(slot uint8, descriptor uint32) (start, end, keyLength int, overflow bool) {
	start = int(descriptor & hashBucketLabOffsetMask)
	end = int(v.heapEnd)
	if next, ok := v.nextLiveSlot(slot); ok {
		end = int(hashBucketLabDescriptor(v.page, next) & hashBucketLabOffsetMask)
	}
	keyLength = int(descriptor&hashBucketLabKeyMask) >> 14
	overflow = descriptor&hashBucketLabOverflowFlag != 0
	return
}

func (v HashBucketLabView) nextLiveSlot(slot uint8) (uint8, bool) {
	word := int(slot >> 6)
	bit := uint(slot & 63)
	mask := v.live[word] & (^uint64(0) << (bit + 1))
	for {
		if mask != 0 {
			return uint8(word*64 + bits.TrailingZeros64(mask)), true
		}
		word++
		if word == len(v.live) {
			return 0, false
		}
		mask = v.live[word]
	}
}

func hashBucketLabDescriptor(page []byte, slot uint8) uint32 {
	start := hashBucketLabDescriptorStart + int(slot)*HashBucketLabDescriptorSize
	return binary.LittleEndian.Uint32(page[start : start+HashBucketLabDescriptorSize])
}

func putHashBucketLabDescriptor(page []byte, slot uint8, descriptor uint32) {
	start := hashBucketLabDescriptorStart + int(slot)*HashBucketLabDescriptorSize
	binary.LittleEndian.PutUint32(page[start:start+HashBucketLabDescriptorSize], descriptor)
}

func sealHashBucketLab(page []byte) {
	checksum := PageChecksum(page[:hashBucketLabTrailerStart])
	binary.LittleEndian.PutUint32(page[hashBucketLabTrailerStart:hashBucketLabTrailerStart+4], checksum)
	binary.LittleEndian.PutUint32(page[hashBucketLabTrailerStart+4:], ^checksum)
}
