package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"unsafe"
)

// The ordered-hash leaf lab tests the production-primary layout proposed for
// replacing the separate fingerprint and document-location structures. It is
// deliberately outside the durable page-kind registry until its read, space,
// and mutation gates pass.
//
// Stable hash slots are independent of lexical ranks. Controls and slotRank
// provide bounded exact lookup, while the heap and all rank-indexed metadata
// are physically lexical. The monotone record boundaries use a fixed-lower-bit
// Elias-Fano representation over the 16-bit page address space.
const (
	OrderedHashLeafLabPageSize        = 64 << 10
	OrderedHashLeafLabExtentQuantum   = 4 << 10
	OrderedHashLeafLabSlotCount       = 256
	OrderedHashLeafLabNormalSlotCount = 240
	OrderedHashLeafLabStashSlots      = 16
	OrderedHashLeafLabHeaderSize      = 40
	// The trailer keeps two independently refreshable CRC32C regions followed
	// by a CRC/complement over that table. Owned materialization can therefore
	// reseal only the changed half of the admitted extent.
	OrderedHashLeafLabTrailerSize     = 16
	OrderedHashLeafLabMaxKeyLength    = 127
	OrderedHashLeafLabOverflowRefSize = 16

	orderedHashLeafLabGroupSize       = 16
	orderedHashLeafLabGroupCount      = OrderedHashLeafLabNormalSlotCount / orderedHashLeafLabGroupSize
	orderedHashLeafLabEFCheckpoint    = 16
	orderedHashLeafLabEFLowerBits     = 8
	orderedHashLeafLabEFLowerUniverse = 1 << orderedHashLeafLabEFLowerBits

	orderedHashLeafLabControlStart  = OrderedHashLeafLabHeaderSize
	orderedHashLeafLabRankStart     = orderedHashLeafLabControlStart + OrderedHashLeafLabSlotCount
	orderedHashLeafLabVariableStart = orderedHashLeafLabRankStart +
		OrderedHashLeafLabSlotCount

	orderedHashLeafLabControlLive = byte(0x80)
	orderedHashLeafLabControlTag  = byte(0x7f)
	orderedHashLeafLabEmptyRank   = byte(0xff)

	orderedHashLeafLabMagic = "OHLAB001"
)

var (
	ErrOrderedHashLeafLabCorrupt  = errors.New("vibejson: corrupt ordered-hash leaf lab")
	ErrOrderedHashLeafLabNotFound = errors.New(
		"vibejson: ordered-hash leaf lab key not found",
	)
	ErrOrderedHashLeafLabNeedsWide = errors.New(
		"vibejson: ordered-hash leaf lab key needs wide representation",
	)
	ErrOrderedHashLeafLabSplit = errors.New(
		"vibejson: ordered-hash leaf lab needs split",
	)
	ErrOrderedHashLeafLabResize = errors.New(
		"vibejson: ordered-hash leaf lab needs resized COW extent",
	)
)

// OrderedHashLeafLabHeader is the stable identity of one lexical leaf. Seed is
// a store-root property and is intentionally not repeated in every leaf.
type OrderedHashLeafLabHeader struct {
	BucketID   uint64
	Generation uint64
}

// OrderedHashLeafLabRecord is canonical encoder input. Records must be in
// strict bytewise lexical key order. Slot is stable until a structural
// split/reclass operation.
type OrderedHashLeafLabRecord struct {
	Slot     uint8
	Key      []byte
	Value    []byte
	Overflow bool
}

// OrderedHashLeafLabRow is one borrowed lexical scan result. Stable slots are
// intentionally absent: ordinary Range does not need them, and persisting the
// inverse rank-to-slot permutation would cost another byte per live key.
type OrderedHashLeafLabRow struct {
	Key      []byte
	Value    []byte
	Overflow bool
}

type orderedHashLeafLabLayout struct {
	keyLengthStart   int
	overflowStart    int
	lowStart         int
	highStart        int
	highBytes        int
	checkpointStart  int
	checkpointCount  int
	checkpointStride int
	heapStart        int
}

// OrderedHashLeafLabView is a checksum- and structure-admitted borrowed page.
// Lookup, lower-bound search, and lexical iteration allocate nothing.
type OrderedHashLeafLabView struct {
	header     OrderedHashLeafLabHeader
	seed       [16]byte
	page       []byte
	count      uint16
	stashCount uint16
	heapEnd    uint16
	layout     orderedHashLeafLabLayout
}

// ExtentBytes is the admitted physical durable length. Canonical COW encoders
// choose the smallest 4 KiB quantum containing metadata and live key/value
// bytes. An owned shrink retains its already-published extent.
func (v *OrderedHashLeafLabView) ExtentBytes() int {
	if v == nil {
		return 0
	}
	return len(v.page)
}

// MinimalExtentBytes is the smallest 4 KiB quantum that can contain the
// current live image. It can be smaller than ExtentBytes after an owned shrink:
// an existing PageRef cannot change its physical length in place.
func (v *OrderedHashLeafLabView) MinimalExtentBytes() int {
	if v == nil {
		return 0
	}
	return orderedHashLeafLabExtentForHeapEnd(int(v.heapEnd))
}

// PersistentBytes returns the checksum-admitted durable image. The returned
// bytes borrow the caller's admitted backing memory.
func (v *OrderedHashLeafLabView) PersistentBytes() []byte {
	if v == nil {
		return nil
	}
	return v.page
}

// OrderedHashLeafLabIterator streams one lexical interval. Slices borrow the
// admitted page. upper is exclusive; prefix, when non-empty, is exact.
type OrderedHashLeafLabIterator struct {
	page     []byte
	layout   orderedHashLeafLabLayout
	count    uint16
	rank     uint16
	bitPos   uint16
	offset   uint16
	upper    []byte
	prefix   []byte
	finished bool
}

type orderedHashLeafLabPlacer struct {
	records  []OrderedHashLeafLabRecord
	seed     [16]byte
	owner    [OrderedHashLeafLabNormalSlotCount]int16
	assigned [OrderedHashLeafLabSlotCount]uint8
	visited  [OrderedHashLeafLabNormalSlotCount]uint16
	epoch    uint16
}

// OrderedHashLeafLabMetadataBytes returns structural bytes for one exact live
// count. It excludes key/value bytes and unused page capacity, but includes
// controls, both rank indexes, overflow state, Elias-Fano boundaries,
// checkpoints, header, and checksum trailer.
func OrderedHashLeafLabMetadataBytes(live int) int {
	if live <= 0 || live > OrderedHashLeafLabSlotCount {
		return 0
	}
	layout := orderedHashLeafLabMakeLayout(live)
	return layout.heapStart + OrderedHashLeafLabTrailerSize
}

// OrderedHashLeafLabMetadataBytesPerLiveKey is structural metadata divided by
// live rows. Whole-page bytes per row are deliberately reported separately.
func OrderedHashLeafLabMetadataBytesPerLiveKey(live int) float64 {
	bytes := OrderedHashLeafLabMetadataBytes(live)
	if bytes == 0 {
		return 0
	}
	return float64(bytes) / float64(live)
}

func orderedHashLeafLabMakeLayout(live int) orderedHashLeafLabLayout {
	boundaries := live + 1
	keyBytes := (live*7 + 7) / 8
	overflowBytes := (live + 7) / 8
	highBytes := (boundaries + orderedHashLeafLabEFLowerUniverse + 7) / 8
	checkpointStride := orderedHashLeafLabEFCheckpoint * 2
	// At the 90% production occupancy target there is enough metadata budget
	// for dense eight-row select checkpoints. Lower occupancies use a 32-row
	// stride so their structural cost stays at or below five bytes/key.
	if live >= 228 {
		checkpointStride = orderedHashLeafLabEFCheckpoint / 2
	}
	checkpointCount := (boundaries + checkpointStride - 1) /
		checkpointStride
	layout := orderedHashLeafLabLayout{
		keyLengthStart: orderedHashLeafLabVariableStart,
	}
	layout.overflowStart = layout.keyLengthStart + keyBytes
	layout.lowStart = layout.overflowStart + overflowBytes
	layout.highStart = layout.lowStart + boundaries
	layout.highBytes = highBytes
	layout.checkpointStart = layout.highStart + highBytes
	layout.checkpointCount = checkpointCount
	layout.checkpointStride = checkpointStride
	layout.heapStart = layout.checkpointStart + checkpointCount*2
	return layout
}

func orderedHashLeafLabExtentForHeapEnd(heapEnd int) int {
	need := heapEnd + OrderedHashLeafLabTrailerSize
	extent := (need + OrderedHashLeafLabExtentQuantum - 1) &
		^(OrderedHashLeafLabExtentQuantum - 1)
	if extent < OrderedHashLeafLabExtentQuantum {
		return OrderedHashLeafLabExtentQuantum
	}
	return extent
}

func orderedHashLeafLabTrailerStart(page []byte) int {
	return len(page) - OrderedHashLeafLabTrailerSize
}

func orderedHashLeafLabChecksumSplit(page []byte) int {
	return orderedHashLeafLabTrailerStart(page) / 2
}

// PlaceOrderedHashLeafLabRecords assigns stable slots before publication.
// The augmenting matcher uses two deterministic 16-slot groups; records that
// cannot be placed there use the bounded 16-slot stash. No live identity exists
// yet, so bulk placement may move provisional assignments.
func PlaceOrderedHashLeafLabRecords(seed [16]byte, records []OrderedHashLeafLabRecord) error {
	if seed == ([16]byte{}) || len(records) > OrderedHashLeafLabSlotCount {
		return fmt.Errorf("%w: ordered-hash leaf placement seed/count", ErrInvalidWrite)
	}
	placer := orderedHashLeafLabPlacer{records: records, seed: seed}
	for index := range placer.owner {
		placer.owner[index] = -1
	}
	for index := range records {
		if len(records[index].Key) == 0 {
			return fmt.Errorf("%w: empty ordered-hash leaf key", ErrInvalidWrite)
		}
		placer.epoch++
		if placer.place(index) {
			continue
		}
		stash := index - countOrderedHashLeafLabAssignedNormal(placer.owner)
		if stash < 0 || stash >= OrderedHashLeafLabStashSlots {
			return ErrOrderedHashLeafLabSplit
		}
		placer.assigned[index] = uint8(OrderedHashLeafLabNormalSlotCount + stash)
	}
	for index := range records {
		records[index].Slot = placer.assigned[index]
	}
	return nil
}

func countOrderedHashLeafLabAssignedNormal(owner [OrderedHashLeafLabNormalSlotCount]int16) int {
	count := 0
	for _, record := range owner {
		if record >= 0 {
			count++
		}
	}
	return count
}

func (p *orderedHashLeafLabPlacer) place(index int) bool {
	hash := orderedHashLeafLabKeyHash(p.seed, p.records[index].Key)
	for ordinal := 0; ordinal < orderedHashLeafLabGroupSize*2; ordinal++ {
		slot := orderedHashLeafLabCandidate(hash, ordinal)
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

func orderedHashLeafLabGroups(hash uint64) (uint8, uint8) {
	first := uint8(hash) % orderedHashLeafLabGroupCount
	second := uint8(hash>>8) % orderedHashLeafLabGroupCount
	if second == first {
		second++
		if second == orderedHashLeafLabGroupCount {
			second = 0
		}
	}
	return first, second
}

func orderedHashLeafLabCandidate(hash uint64, ordinal int) uint8 {
	first, second := orderedHashLeafLabGroups(hash)
	group := first
	home := uint8(hash>>16) & 0x0f
	if ordinal >= orderedHashLeafLabGroupSize {
		group = second
		home = uint8(hash>>20) & 0x0f
		ordinal -= orderedHashLeafLabGroupSize
	}
	return group*orderedHashLeafLabGroupSize +
		(home+uint8(ordinal))&(orderedHashLeafLabGroupSize-1)
}

func orderedHashLeafLabSlotIsCandidate(hash uint64, slot uint8) bool {
	if slot >= OrderedHashLeafLabNormalSlotCount {
		return true
	}
	first, second := orderedHashLeafLabGroups(hash)
	group := slot / orderedHashLeafLabGroupSize
	return group == first || group == second
}

// orderedHashLeafLabKeyHash is the existing keyed SipHash with the common
// exactly-eight-byte key case unrolled. The general primitive deliberately
// handles arbitrary slices; removing its loop/tail dispatch here matters
// because the production anchor router and this leaf share the same hash.
func orderedHashLeafLabKeyHash(seed [16]byte, key []byte) uint64 {
	if len(key) != 8 {
		return KeyHashBytes(seed, key)
	}
	k0 := binary.LittleEndian.Uint64(seed[0:8])
	k1 := binary.LittleEndian.Uint64(seed[8:16])
	v0 := k0 ^ 0x736f6d6570736575
	v1 := k1 ^ 0x646f72616e646f6d
	v2 := k0 ^ 0x6c7967656e657261
	v3 := k1 ^ 0x7465646279746573
	message := binary.LittleEndian.Uint64(key)
	v3 ^= message
	orderedHashLeafLabSipRoundFront(&v0, &v1, &v2, &v3)
	orderedHashLeafLabSipRoundBack(&v0, &v1, &v2, &v3)
	v0 ^= message
	last := uint64(8) << 56
	v3 ^= last
	orderedHashLeafLabSipRoundFront(&v0, &v1, &v2, &v3)
	orderedHashLeafLabSipRoundBack(&v0, &v1, &v2, &v3)
	v0 ^= last
	v2 ^= 0xff
	orderedHashLeafLabSipRoundFront(&v0, &v1, &v2, &v3)
	orderedHashLeafLabSipRoundBack(&v0, &v1, &v2, &v3)
	orderedHashLeafLabSipRoundFront(&v0, &v1, &v2, &v3)
	orderedHashLeafLabSipRoundBack(&v0, &v1, &v2, &v3)
	orderedHashLeafLabSipRoundFront(&v0, &v1, &v2, &v3)
	orderedHashLeafLabSipRoundBack(&v0, &v1, &v2, &v3)
	return v0 ^ v1 ^ v2 ^ v3
}

func orderedHashLeafLabSipRoundFront(v0, v1, v2, v3 *uint64) {
	*v0 += *v1
	*v1 = bits.RotateLeft64(*v1, 13)
	*v1 ^= *v0
	*v0 = bits.RotateLeft64(*v0, 32)
	*v2 += *v3
	*v3 = bits.RotateLeft64(*v3, 16)
	*v3 ^= *v2
}

func orderedHashLeafLabSipRoundBack(v0, v1, v2, v3 *uint64) {
	*v0 += *v3
	*v3 = bits.RotateLeft64(*v3, 21)
	*v3 ^= *v0
	*v2 += *v1
	*v1 = bits.RotateLeft64(*v1, 17)
	*v1 ^= *v2
	*v2 = bits.RotateLeft64(*v2, 32)
}

// EncodeOrderedHashLeafLab writes one canonical lexical leaf. Records must be
// strictly ordered by key and carry unique, valid stable slots.
func EncodeOrderedHashLeafLab(
	dst []byte,
	header OrderedHashLeafLabHeader,
	seed [16]byte,
	records []OrderedHashLeafLabRecord,
) ([]byte, error) {
	if header.Generation == 0 || seed == ([16]byte{}) ||
		len(records) > OrderedHashLeafLabSlotCount {
		return nil, fmt.Errorf("%w: ordered-hash leaf identity/count", ErrInvalidWrite)
	}
	layout := orderedHashLeafLabMakeLayout(len(records))
	heapEnd := layout.heapStart
	for rank := range records {
		record := &records[rank]
		if len(record.Key) == 0 || len(record.Key) > OrderedHashLeafLabMaxKeyLength {
			return nil, fmt.Errorf("%w: key length %d", ErrOrderedHashLeafLabNeedsWide, len(record.Key))
		}
		if len(record.Value) == 0 ||
			record.Overflow && len(record.Value) != OrderedHashLeafLabOverflowRefSize {
			return nil, fmt.Errorf("%w: ordered-hash leaf value", ErrInvalidWrite)
		}
		if rank != 0 && bytes.Compare(records[rank-1].Key, record.Key) >= 0 {
			return nil, fmt.Errorf("%w: keys not strictly lexical", ErrInvalidWrite)
		}
		heapEnd += len(record.Key) + len(record.Value)
	}
	extent := orderedHashLeafLabExtentForHeapEnd(heapEnd)
	if extent > OrderedHashLeafLabPageSize {
		return nil, ErrOrderedHashLeafLabSplit
	}
	if len(dst) < extent {
		return nil, fmt.Errorf("%w: short ordered-hash leaf destination", ErrInvalidWrite)
	}
	page := dst[:extent]
	clear(page)
	copy(page[:8], orderedHashLeafLabMagic)
	binary.LittleEndian.PutUint32(page[8:12], DevelopmentFormatVersion)
	binary.LittleEndian.PutUint16(page[12:14], OrderedHashLeafLabHeaderSize)
	binary.LittleEndian.PutUint16(page[14:16], OrderedHashLeafLabSlotCount)
	binary.LittleEndian.PutUint16(page[16:18], uint16(len(records)))
	binary.LittleEndian.PutUint64(page[20:28], header.BucketID)
	binary.LittleEndian.PutUint64(page[28:36], header.Generation)
	binary.LittleEndian.PutUint16(page[36:38], uint16(layout.heapStart))
	for slot := 0; slot < OrderedHashLeafLabSlotCount; slot++ {
		page[orderedHashLeafLabRankStart+slot] = orderedHashLeafLabEmptyRank
	}

	cursor := layout.heapStart
	stashCount := 0
	var occupied [4]uint64
	for rank := range records {
		record := &records[rank]
		slot := int(record.Slot)
		bit := uint64(1) << uint(slot&63)
		if occupied[slot>>6]&bit != 0 {
			return nil, fmt.Errorf("%w: duplicate ordered-hash leaf slot", ErrInvalidWrite)
		}
		occupied[slot>>6] |= bit
		hash := orderedHashLeafLabKeyHash(seed, record.Key)
		if !orderedHashLeafLabSlotIsCandidate(hash, record.Slot) {
			return nil, fmt.Errorf("%w: slot outside candidate groups", ErrInvalidWrite)
		}
		if record.Slot >= OrderedHashLeafLabNormalSlotCount {
			stashCount++
		}
		page[orderedHashLeafLabControlStart+slot] =
			orderedHashLeafLabControlLive | byte(hash>>57)
		page[orderedHashLeafLabRankStart+slot] = byte(rank)
		putOrderedHashLeafLabKeyLength(page, layout, rank, uint8(len(record.Key)))
		if record.Overflow {
			page[layout.overflowStart+rank/8] |= byte(1) << uint(rank&7)
		}
		putOrderedHashLeafLabBoundary(page, layout, rank, uint16(cursor))
		copy(page[cursor:], record.Key)
		cursor += len(record.Key)
		copy(page[cursor:], record.Value)
		cursor += len(record.Value)
	}
	putOrderedHashLeafLabBoundary(page, layout, len(records), uint16(cursor))
	buildOrderedHashLeafLabCheckpoints(page, layout, len(records)+1)
	binary.LittleEndian.PutUint16(page[18:20], uint16(stashCount))
	binary.LittleEndian.PutUint16(page[38:40], uint16(cursor))
	sealOrderedHashLeafLab(page)
	return page, nil
}

func putOrderedHashLeafLabKeyLength(
	page []byte, layout orderedHashLeafLabLayout, rank int, length uint8,
) {
	bit := rank * 7
	at := layout.keyLengthStart + bit/8
	shift := uint(bit & 7)
	word := uint16(page[at])
	if at+1 < layout.overflowStart {
		word |= uint16(page[at+1]) << 8
	}
	word |= uint16(length) << shift
	page[at] = byte(word)
	if at+1 < layout.overflowStart {
		page[at+1] = byte(word >> 8)
	}
}

func orderedHashLeafLabKeyLength(
	page []byte, layout orderedHashLeafLabLayout, rank int,
) int {
	bit := rank * 7
	at := layout.keyLengthStart + bit/8
	shift := uint(bit & 7)
	word := uint16(page[at])
	if at+1 < layout.overflowStart {
		word |= uint16(page[at+1]) << 8
	}
	return int(word>>shift) & 0x7f
}

func putOrderedHashLeafLabBoundary(
	page []byte, layout orderedHashLeafLabLayout, index int, offset uint16,
) {
	page[layout.lowStart+index] = byte(offset)
	bitPosition := int(offset>>orderedHashLeafLabEFLowerBits) + index
	page[layout.highStart+bitPosition/8] |= byte(1) << uint(bitPosition&7)
}

func buildOrderedHashLeafLabCheckpoints(
	page []byte, layout orderedHashLeafLabLayout, boundaries int,
) {
	for index := 0; index < boundaries; index += layout.checkpointStride {
		position, ok := orderedHashLeafLabSelectFromStart(page, layout, index)
		if !ok {
			panic("ordered-hash leaf encoder produced invalid Elias-Fano bitmap")
		}
		binary.LittleEndian.PutUint16(
			page[layout.checkpointStart+(index/layout.checkpointStride)*2:],
			uint16(position),
		)
	}
}

func orderedHashLeafLabSelectFromStart(
	page []byte, layout orderedHashLeafLabLayout, target int,
) (int, bool) {
	remaining := target
	for byteIndex := 0; byteIndex < layout.highBytes; byteIndex++ {
		value := page[layout.highStart+byteIndex]
		count := bits.OnesCount8(value)
		if remaining >= count {
			remaining -= count
			continue
		}
		for value != 0 {
			bit := bits.TrailingZeros8(value)
			if remaining == 0 {
				return byteIndex*8 + bit, true
			}
			value &= value - 1
			remaining--
		}
	}
	return 0, false
}

func sealOrderedHashLeafLab(page []byte) {
	trailerStart := orderedHashLeafLabTrailerStart(page)
	checksumSplit := orderedHashLeafLabChecksumSplit(page)
	first := PageChecksum(page[:checksumSplit])
	second := PageChecksum(
		page[checksumSplit:trailerStart],
	)
	binary.LittleEndian.PutUint32(
		page[trailerStart:trailerStart+4],
		first,
	)
	binary.LittleEndian.PutUint32(
		page[trailerStart+4:trailerStart+8],
		second,
	)
	sealOrderedHashLeafLabChecksumTable(page)
}

func resealOrderedHashLeafLabOwned(page []byte, changedStart, changedEnd int) {
	trailerStart := orderedHashLeafLabTrailerStart(page)
	checksumSplit := orderedHashLeafLabChecksumSplit(page)
	if changedStart < checksumSplit && changedEnd > 0 {
		first := PageChecksum(page[:checksumSplit])
		binary.LittleEndian.PutUint32(
			page[trailerStart:trailerStart+4],
			first,
		)
	}
	if changedStart < trailerStart && changedEnd > checksumSplit {
		second := PageChecksum(
			page[checksumSplit:trailerStart],
		)
		binary.LittleEndian.PutUint32(
			page[trailerStart+4:trailerStart+8],
			second,
		)
	}
	sealOrderedHashLeafLabChecksumTable(page)
}

func sealOrderedHashLeafLabChecksumTable(page []byte) {
	trailerStart := orderedHashLeafLabTrailerStart(page)
	table := page[trailerStart : trailerStart+8]
	checksum := PageChecksum(table)
	binary.LittleEndian.PutUint32(
		page[trailerStart+8:trailerStart+12],
		checksum,
	)
	binary.LittleEndian.PutUint32(
		page[trailerStart+12:], ^checksum,
	)
}

// OpenOrderedHashLeafLab verifies the checksum, complete lexical order,
// controls, slot/rank bijection, tags, candidate placement, succinct
// boundaries, overflow widths, and zero padding once.
func OpenOrderedHashLeafLab(src []byte, seed [16]byte) (OrderedHashLeafLabView, error) {
	if len(src) < OrderedHashLeafLabExtentQuantum ||
		len(src) > OrderedHashLeafLabPageSize ||
		len(src)%OrderedHashLeafLabExtentQuantum != 0 ||
		seed == ([16]byte{}) {
		return OrderedHashLeafLabView{}, fmt.Errorf("%w: short page or seed", ErrOrderedHashLeafLabCorrupt)
	}
	page := src
	trailerStart := orderedHashLeafLabTrailerStart(page)
	checksumSplit := orderedHashLeafLabChecksumSplit(page)
	firstChecksum := binary.LittleEndian.Uint32(
		page[trailerStart : trailerStart+4],
	)
	secondChecksum := binary.LittleEndian.Uint32(
		page[trailerStart+4 : trailerStart+8],
	)
	tableChecksum := binary.LittleEndian.Uint32(
		page[trailerStart+8 : trailerStart+12],
	)
	count := int(binary.LittleEndian.Uint16(page[16:18]))
	stashCount := int(binary.LittleEndian.Uint16(page[18:20]))
	layout := orderedHashLeafLabMakeLayout(count)
	heapEnd := int(binary.LittleEndian.Uint16(page[38:40]))
	if binary.LittleEndian.Uint32(page[trailerStart+12:]) !=
		^tableChecksum ||
		PageChecksum(page[trailerStart:trailerStart+8]) != tableChecksum ||
		PageChecksum(page[:checksumSplit]) != firstChecksum ||
		PageChecksum(page[checksumSplit:trailerStart]) != secondChecksum {
		return OrderedHashLeafLabView{}, fmt.Errorf("%w: checksum", ErrOrderedHashLeafLabCorrupt)
	}
	if string(page[:8]) != orderedHashLeafLabMagic ||
		binary.LittleEndian.Uint32(page[8:12]) != DevelopmentFormatVersion ||
		binary.LittleEndian.Uint16(page[12:14]) != OrderedHashLeafLabHeaderSize ||
		binary.LittleEndian.Uint16(page[14:16]) != OrderedHashLeafLabSlotCount ||
		count > OrderedHashLeafLabSlotCount || stashCount > OrderedHashLeafLabStashSlots ||
		binary.LittleEndian.Uint64(page[28:36]) == 0 ||
		int(binary.LittleEndian.Uint16(page[36:38])) != layout.heapStart ||
		heapEnd < layout.heapStart || heapEnd > trailerStart ||
		orderedHashLeafLabExtentForHeapEnd(heapEnd) > len(page) {
		return OrderedHashLeafLabView{}, fmt.Errorf("%w: header", ErrOrderedHashLeafLabCorrupt)
	}

	view := OrderedHashLeafLabView{
		header: OrderedHashLeafLabHeader{
			BucketID:   binary.LittleEndian.Uint64(page[20:28]),
			Generation: binary.LittleEndian.Uint64(page[28:36]),
		},
		seed: seed, page: page, count: uint16(count),
		stashCount: uint16(stashCount), heapEnd: uint16(heapEnd), layout: layout,
	}
	if err := view.validate(); err != nil {
		return OrderedHashLeafLabView{}, err
	}
	return view, nil
}

func (v OrderedHashLeafLabView) validate() error {
	boundaries := int(v.count) + 1
	highCount := 0
	for index := 0; index < v.layout.highBytes; index++ {
		highCount += bits.OnesCount8(v.page[v.layout.highStart+index])
	}
	if highCount != boundaries ||
		!orderedHashLeafLabUnusedBitsZero(
			v.page[v.layout.keyLengthStart:v.layout.overflowStart],
			int(v.count)*7,
		) ||
		!orderedHashLeafLabUnusedBitsZero(
			v.page[v.layout.overflowStart:v.layout.lowStart],
			int(v.count),
		) {
		return fmt.Errorf("%w: succinct metadata", ErrOrderedHashLeafLabCorrupt)
	}
	for index := 0; index < boundaries; index += v.layout.checkpointStride {
		want, ok := orderedHashLeafLabSelectFromStart(v.page, v.layout, index)
		checkpoint := index / v.layout.checkpointStride
		got := int(binary.LittleEndian.Uint16(
			v.page[v.layout.checkpointStart+checkpoint*2:],
		))
		if !ok || got != want {
			return fmt.Errorf("%w: select checkpoint", ErrOrderedHashLeafLabCorrupt)
		}
	}
	var seenRanks [4]uint64
	live := 0
	stash := 0
	for slot := 0; slot < OrderedHashLeafLabSlotCount; slot++ {
		control := v.page[orderedHashLeafLabControlStart+slot]
		rank := v.page[orderedHashLeafLabRankStart+slot]
		if control == 0 {
			if rank != orderedHashLeafLabEmptyRank {
				return fmt.Errorf("%w: empty control rank", ErrOrderedHashLeafLabCorrupt)
			}
			continue
		}
		if control&orderedHashLeafLabControlLive == 0 ||
			int(rank) >= int(v.count) {
			return fmt.Errorf("%w: live control/rank", ErrOrderedHashLeafLabCorrupt)
		}
		bit := uint64(1) << uint(rank&63)
		if seenRanks[rank>>6]&bit != 0 {
			return fmt.Errorf("%w: duplicate lexical rank", ErrOrderedHashLeafLabCorrupt)
		}
		seenRanks[rank>>6] |= bit
		start, end, ok := v.recordBounds(int(rank))
		if !ok {
			return fmt.Errorf("%w: record bounds", ErrOrderedHashLeafLabCorrupt)
		}
		keyLength := orderedHashLeafLabKeyLength(v.page, v.layout, int(rank))
		if keyLength == 0 || start+keyLength >= end {
			return fmt.Errorf("%w: key/value interval", ErrOrderedHashLeafLabCorrupt)
		}
		key := v.page[start : start+keyLength]
		hash := orderedHashLeafLabKeyHash(v.seed, key)
		if control&orderedHashLeafLabControlTag != byte(hash>>57) ||
			!orderedHashLeafLabSlotIsCandidate(hash, uint8(slot)) {
			return fmt.Errorf("%w: tag or candidate slot", ErrOrderedHashLeafLabCorrupt)
		}
		if slot >= OrderedHashLeafLabNormalSlotCount {
			stash++
		}
		live++
	}
	if live != int(v.count) || stash != int(v.stashCount) {
		return fmt.Errorf("%w: live/stash count", ErrOrderedHashLeafLabCorrupt)
	}
	for rank := 0; rank < int(v.count); rank++ {
		if seenRanks[rank>>6]&(uint64(1)<<uint(rank&63)) == 0 {
			return fmt.Errorf("%w: missing lexical rank", ErrOrderedHashLeafLabCorrupt)
		}
		start, end, ok := v.recordBounds(rank)
		if !ok {
			return fmt.Errorf("%w: lexical bounds", ErrOrderedHashLeafLabCorrupt)
		}
		keyLength := orderedHashLeafLabKeyLength(v.page, v.layout, rank)
		overflow := v.rankOverflow(rank)
		if keyLength == 0 || start+keyLength >= end ||
			overflow && end-start-keyLength != OrderedHashLeafLabOverflowRefSize {
			return fmt.Errorf("%w: lexical record", ErrOrderedHashLeafLabCorrupt)
		}
		if rank != 0 {
			previousStart, _, _ := v.recordBounds(rank - 1)
			previousLength := orderedHashLeafLabKeyLength(v.page, v.layout, rank-1)
			if bytes.Compare(
				v.page[previousStart:previousStart+previousLength],
				v.page[start:start+keyLength],
			) >= 0 {
				return fmt.Errorf("%w: key order", ErrOrderedHashLeafLabCorrupt)
			}
		}
	}
	first, ok := v.boundary(0)
	if !ok || int(first) != v.layout.heapStart {
		return fmt.Errorf("%w: first boundary", ErrOrderedHashLeafLabCorrupt)
	}
	last, ok := v.boundary(boundaries - 1)
	if !ok || last != v.heapEnd ||
		!allZero(v.page[int(v.heapEnd):orderedHashLeafLabTrailerStart(v.page)]) {
		return fmt.Errorf("%w: last boundary or padding", ErrOrderedHashLeafLabCorrupt)
	}
	return nil
}

func orderedHashLeafLabUnusedBitsZero(encoded []byte, usedBits int) bool {
	if len(encoded) == 0 {
		return usedBits == 0
	}
	usedInLast := usedBits & 7
	if usedInLast == 0 {
		return true
	}
	return encoded[len(encoded)-1]&^byte((1<<uint(usedInLast))-1) == 0
}

// Header returns the stable leaf identity.
func (v OrderedHashLeafLabView) Header() OrderedHashLeafLabHeader { return v.header }

// Len returns live rows.
func (v OrderedHashLeafLabView) Len() int { return int(v.count) }

// StashLen returns rows in the bounded exceptional-placement area.
func (v OrderedHashLeafLabView) StashLen() int { return int(v.stashCount) }

// Lookup performs bounded tag probing and exact same-page key confirmation.
func (v *OrderedHashLeafLabView) Lookup(key []byte) (
	slot uint8, value []byte, overflow, ok bool,
) {
	if len(key) == 0 || len(key) > OrderedHashLeafLabMaxKeyLength || v.count == 0 {
		return 0, nil, false, false
	}
	hash := orderedHashLeafLabKeyHash(v.seed, key)
	wantControl := orderedHashLeafLabControlLive | byte(hash>>57)
	first, second := orderedHashLeafLabGroups(hash)
	firstHome := uint8(hash>>16) & 0x0f
	secondHome := uint8(hash>>20) & 0x0f
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group, home := first, firstHome
		if groupIndex != 0 {
			group, home = second, secondHome
		}
		base := group * orderedHashLeafLabGroupSize
		for ordinal := uint8(0); ordinal < orderedHashLeafLabGroupSize; ordinal++ {
			candidate := base + (home+ordinal)&(orderedHashLeafLabGroupSize-1)
			if v.page[orderedHashLeafLabControlStart+int(candidate)] != wantControl {
				continue
			}
			if value, overflow, ok := v.lookupCandidate(candidate, key); ok {
				return candidate, value, overflow, true
			}
		}
	}
	for candidate := OrderedHashLeafLabNormalSlotCount; candidate < OrderedHashLeafLabSlotCount && v.stashCount != 0; candidate++ {
		if v.page[orderedHashLeafLabControlStart+candidate] != wantControl {
			continue
		}
		if value, overflow, ok := v.lookupCandidate(uint8(candidate), key); ok {
			return uint8(candidate), value, overflow, true
		}
	}
	return 0, nil, false, false
}

// LookupHashed reuses a hash computed by the tablet anchor router.
func (v *OrderedHashLeafLabView) LookupHashed(hash uint64, key []byte) (
	slot uint8, value []byte, overflow, ok bool,
) {
	if len(key) == 0 || len(key) > OrderedHashLeafLabMaxKeyLength || v.count == 0 {
		return 0, nil, false, false
	}
	wantControl := orderedHashLeafLabControlLive | byte(hash>>57)
	first, second := orderedHashLeafLabGroups(hash)
	firstHome := uint8(hash>>16) & 0x0f
	secondHome := uint8(hash>>20) & 0x0f
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group, home := first, firstHome
		if groupIndex != 0 {
			group, home = second, secondHome
		}
		base := group * orderedHashLeafLabGroupSize
		for ordinal := uint8(0); ordinal < orderedHashLeafLabGroupSize; ordinal++ {
			candidate := base + (home+ordinal)&(orderedHashLeafLabGroupSize-1)
			if v.page[orderedHashLeafLabControlStart+int(candidate)] != wantControl {
				continue
			}
			if value, overflow, ok := v.lookupCandidate(candidate, key); ok {
				return candidate, value, overflow, true
			}
		}
	}
	for candidate := OrderedHashLeafLabNormalSlotCount; candidate < OrderedHashLeafLabSlotCount && v.stashCount != 0; candidate++ {
		if v.page[orderedHashLeafLabControlStart+candidate] != wantControl {
			continue
		}
		if value, overflow, ok := v.lookupCandidate(uint8(candidate), key); ok {
			return uint8(candidate), value, overflow, true
		}
	}
	return 0, nil, false, false
}

func (v *OrderedHashLeafLabView) lookupCandidate(
	slot uint8, key []byte,
) ([]byte, bool, bool) {
	rank := int(v.page[orderedHashLeafLabRankStart+int(slot)])
	if rank >= int(v.count) ||
		orderedHashLeafLabKeyLength(v.page, v.layout, rank) != len(key) {
		return nil, false, false
	}
	start, end, ok := v.recordBounds(rank)
	if !ok || !bytes.Equal(v.page[start:start+len(key)], key) {
		return nil, false, false
	}
	return v.page[start+len(key) : end : end], v.rankOverflow(rank), true
}

// LookupSlot verifies a compiled stable-slot hint without hashing.
func (v *OrderedHashLeafLabView) LookupSlot(slot uint8, key []byte) (
	value []byte, overflow, ok bool,
) {
	if len(key) == 0 ||
		v.page[orderedHashLeafLabControlStart+int(slot)]&orderedHashLeafLabControlLive == 0 {
		return nil, false, false
	}
	return v.lookupCandidate(slot, key)
}

func (v *OrderedHashLeafLabView) rankOverflow(rank int) bool {
	return v.page[v.layout.overflowStart+rank/8]&(byte(1)<<uint(rank&7)) != 0
}

func (v *OrderedHashLeafLabView) boundary(index int) (uint16, bool) {
	if index < 0 || index > int(v.count) {
		return 0, false
	}
	checkpoint := index / v.layout.checkpointStride
	baseIndex := checkpoint * v.layout.checkpointStride
	position := int(binary.LittleEndian.Uint16(
		v.page[v.layout.checkpointStart+checkpoint*2:],
	))
	if position < 0 || position >= v.layout.highBytes*8 ||
		!orderedHashLeafLabHighBit(v.page, v.layout, position) {
		return 0, false
	}
	for current := baseIndex; current < index; current++ {
		next, ok := orderedHashLeafLabNextHighBit(v.page, v.layout, position+1)
		if !ok {
			return 0, false
		}
		position = next
	}
	high := position - index
	if high < 0 || high >= orderedHashLeafLabEFLowerUniverse {
		return 0, false
	}
	return uint16(high<<orderedHashLeafLabEFLowerBits) |
		uint16(v.page[v.layout.lowStart+index]), true
}

func (v *OrderedHashLeafLabView) recordBounds(rank int) (int, int, bool) {
	if rank < 0 || rank >= int(v.count) {
		return 0, 0, false
	}
	position, ok := v.selectBitPosition(rank)
	if !ok {
		return 0, 0, false
	}
	nextPosition, ok := orderedHashLeafLabNextHighBit(
		v.page, v.layout, position+1,
	)
	if !ok {
		return 0, 0, false
	}
	start := uint16((position-rank)<<orderedHashLeafLabEFLowerBits) |
		uint16(v.page[v.layout.lowStart+rank])
	nextRank := rank + 1
	end := uint16((nextPosition-nextRank)<<orderedHashLeafLabEFLowerBits) |
		uint16(v.page[v.layout.lowStart+nextRank])
	if end <= start {
		return 0, 0, false
	}
	return int(start), int(end), true
}

func orderedHashLeafLabHighBit(
	page []byte, layout orderedHashLeafLabLayout, position int,
) bool {
	return position >= 0 && position < layout.highBytes*8 &&
		page[layout.highStart+position/8]&(byte(1)<<uint(position&7)) != 0
}

func orderedHashLeafLabNextHighBit(
	page []byte, layout orderedHashLeafLabLayout, position int,
) (int, bool) {
	if position < 0 || position >= layout.highBytes*8 {
		return 0, false
	}
	byteIndex := position / 8
	value := page[layout.highStart+byteIndex] &^
		(byte(1)<<uint(position&7) - 1)
	for {
		if value != 0 {
			return byteIndex*8 + bits.TrailingZeros8(value), true
		}
		byteIndex++
		if byteIndex >= layout.highBytes {
			return 0, false
		}
		value = page[layout.highStart+byteIndex]
	}
}

// LowerBound returns the first lexical rank whose key is >= key.
func (v *OrderedHashLeafLabView) LowerBound(key []byte) int {
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		start, _, _ := v.recordBounds(middle)
		length := orderedHashLeafLabKeyLength(v.page, v.layout, middle)
		if bytes.Compare(v.page[start:start+length], key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// AllRows returns a zero-allocation lexical iterator.
func (v *OrderedHashLeafLabView) AllRows() OrderedHashLeafLabIterator {
	return v.iteratorAt(0, nil, nil)
}

// Range returns rows in [lower, upper). A nil/empty upper is unbounded.
func (v *OrderedHashLeafLabView) Range(lower, upper []byte) OrderedHashLeafLabIterator {
	return v.iteratorAt(v.LowerBound(lower), upper, nil)
}

// Prefix returns rows beginning with prefix in lexical order.
func (v *OrderedHashLeafLabView) Prefix(prefix []byte) OrderedHashLeafLabIterator {
	return v.iteratorAt(v.LowerBound(prefix), nil, prefix)
}

func (v *OrderedHashLeafLabView) iteratorAt(
	rank int, upper, prefix []byte,
) OrderedHashLeafLabIterator {
	iterator := OrderedHashLeafLabIterator{
		page: v.page, layout: v.layout, count: v.count,
		rank: uint16(rank), upper: upper, prefix: prefix,
	}
	if rank >= int(v.count) {
		iterator.finished = true
		return iterator
	}
	position, ok := v.selectBitPosition(rank)
	if !ok {
		iterator.finished = true
		return iterator
	}
	iterator.bitPos = uint16(position)
	iterator.offset = uint16((position-rank)<<orderedHashLeafLabEFLowerBits) |
		uint16(v.page[v.layout.lowStart+rank])
	return iterator
}

func (v *OrderedHashLeafLabView) selectBitPosition(index int) (int, bool) {
	offset, ok := v.boundary(index)
	if !ok {
		return 0, false
	}
	// Elias-Fano select identity: position = high(offset) + index.
	return int(offset>>orderedHashLeafLabEFLowerBits) + index, true
}

// Next returns one borrowed lexical row.
func (it *OrderedHashLeafLabIterator) Next() (OrderedHashLeafLabRow, bool) {
	key, value, overflow, ok := it.NextBorrowed()
	return OrderedHashLeafLabRow{Key: key, Value: value, Overflow: overflow}, ok
}

// NextBorrowed is the low-overhead lexical scan API.
func (it *OrderedHashLeafLabIterator) NextBorrowed() (
	key, value []byte, overflow, ok bool,
) {
	if it == nil || it.finished || it.rank >= it.count {
		return nil, nil, false, false
	}
	rank := int(it.rank)
	nextPosition, found := orderedHashLeafLabNextHighBit(
		it.page, it.layout, int(it.bitPos)+1,
	)
	if !found {
		it.finished = true
		return nil, nil, false, false
	}
	nextRank := rank + 1
	end := uint16((nextPosition-nextRank)<<orderedHashLeafLabEFLowerBits) |
		uint16(it.page[it.layout.lowStart+nextRank])
	keyLength := orderedHashLeafLabKeyLength(it.page, it.layout, rank)
	start := int(it.offset)
	key = it.page[start : start+keyLength : start+keyLength]
	if len(it.upper) != 0 && bytes.Compare(key, it.upper) >= 0 ||
		len(it.prefix) != 0 && !bytes.HasPrefix(key, it.prefix) {
		it.finished = true
		return nil, nil, false, false
	}
	value = it.page[start+keyLength : int(end) : int(end)]
	overflow = it.page[it.layout.overflowStart+rank/8]&
		(byte(1)<<uint(rank&7)) != 0
	it.rank++
	it.bitPos = uint16(nextPosition)
	it.offset = end
	if it.rank >= it.count {
		it.finished = true
	}
	return key, value, overflow, true
}

// DeleteTo writes the canonical tombstone-free after-image into dst while
// preserving every surviving stable slot.
func (v *OrderedHashLeafLabView) DeleteTo(
	dst []byte, generation uint64, slot uint8, key []byte,
) ([]byte, error) {
	if generation == 0 || bytesOverlapOrderedHashLeafLab(dst, v.page) {
		return nil, fmt.Errorf("%w: ordered-hash leaf delete destination", ErrInvalidWrite)
	}
	if _, _, ok := v.LookupSlot(slot, key); !ok {
		return nil, ErrOrderedHashLeafLabNotFound
	}
	var records [OrderedHashLeafLabSlotCount]OrderedHashLeafLabRecord
	rankSlots, ok := v.rankSlots()
	if !ok {
		return nil, ErrOrderedHashLeafLabCorrupt
	}
	count := 0
	for rank := 0; rank < int(v.count); rank++ {
		recordSlot := rankSlots[rank]
		start, end, _ := v.recordBounds(rank)
		keyLength := orderedHashLeafLabKeyLength(v.page, v.layout, rank)
		if recordSlot == slot {
			continue
		}
		records[count] = OrderedHashLeafLabRecord{
			Slot:     recordSlot,
			Key:      v.page[start : start+keyLength : start+keyLength],
			Value:    v.page[start+keyLength : end : end],
			Overflow: v.rankOverflow(rank),
		}
		count++
	}
	return EncodeOrderedHashLeafLab(
		dst, OrderedHashLeafLabHeader{BucketID: v.header.BucketID, Generation: generation},
		v.seed, records[:count],
	)
}

// UpdateTo writes one canonical after-image while preserving key, slot, and
// lexical rank.
func (v *OrderedHashLeafLabView) UpdateTo(
	dst []byte, generation uint64, slot uint8, key, value []byte, overflow bool,
) ([]byte, error) {
	if generation == 0 || len(value) == 0 ||
		overflow && len(value) != OrderedHashLeafLabOverflowRefSize ||
		bytesOverlapOrderedHashLeafLab(dst, v.page) {
		return nil, fmt.Errorf("%w: ordered-hash leaf update", ErrInvalidWrite)
	}
	if _, _, ok := v.LookupSlot(slot, key); !ok {
		return nil, ErrOrderedHashLeafLabNotFound
	}
	rank := int(v.page[orderedHashLeafLabRankStart+int(slot)])
	start, end, ok := v.recordBounds(rank)
	if !ok {
		return nil, ErrOrderedHashLeafLabCorrupt
	}
	keyLength := orderedHashLeafLabKeyLength(v.page, v.layout, rank)
	oldValueLength := end - start - keyLength
	delta := len(value) - oldValueLength
	oldHeapEnd := int(v.heapEnd)
	newHeapEnd := oldHeapEnd + delta
	newExtent := orderedHashLeafLabExtentForHeapEnd(newHeapEnd)
	if newExtent > OrderedHashLeafLabPageSize {
		return nil, ErrOrderedHashLeafLabSplit
	}
	if len(dst) < newExtent {
		return nil, fmt.Errorf("%w: short ordered-hash leaf update destination", ErrInvalidWrite)
	}

	page := dst[:newExtent]
	clear(page)
	copyEnd := min(oldHeapEnd, len(page))
	copy(page[:copyEnd], v.page[:copyEnd])
	if delta != 0 {
		var boundaries [OrderedHashLeafLabSlotCount + 1]uint16
		for index := 0; index <= int(v.count); index++ {
			boundary, found := v.boundary(index)
			if !found {
				return nil, ErrOrderedHashLeafLabCorrupt
			}
			if index > rank {
				boundary = uint16(int(boundary) + delta)
			}
			boundaries[index] = boundary
		}
		copy(page[end+delta:newHeapEnd], v.page[end:oldHeapEnd])
		if delta < 0 {
			clear(page[newHeapEnd:oldHeapEnd])
		}
		clear(page[v.layout.lowStart : v.layout.checkpointStart+v.layout.checkpointCount*2])
		for index := 0; index <= int(v.count); index++ {
			putOrderedHashLeafLabBoundary(page, v.layout, index, boundaries[index])
		}
		buildOrderedHashLeafLabCheckpoints(page, v.layout, int(v.count)+1)
	}
	copy(page[start+keyLength:start+keyLength+len(value)], value)
	flag := byte(1) << uint(rank&7)
	if overflow {
		page[v.layout.overflowStart+rank/8] |= flag
	} else {
		page[v.layout.overflowStart+rank/8] &^= flag
	}
	binary.LittleEndian.PutUint64(page[28:36], generation)
	binary.LittleEndian.PutUint16(page[38:40], uint16(newHeapEnd))
	sealOrderedHashLeafLab(page)
	return page, nil
}

// MaterializeOwnedUpdate updates the admitted page itself. The caller must
// prove that the page is exclusively owned by the writer and unreachable from
// every pinned snapshot. The immutable UpdateTo path is the unconditional COW
// fallback. It preserves BucketID, Generation, and the physical extent because
// all three are part of the published PageRef identity. Same-length replacement
// touches only the value and checksum; a length change additionally shifts the
// lexical tail and rebuilds the bounded succinct boundaries.
func (v *OrderedHashLeafLabView) MaterializeOwnedUpdate(
	slot uint8, key, value []byte, overflow bool,
) error {
	if len(value) == 0 ||
		overflow && len(value) != OrderedHashLeafLabOverflowRefSize {
		return fmt.Errorf("%w: ordered-hash leaf materialized update", ErrInvalidWrite)
	}
	if _, _, ok := v.LookupSlot(slot, key); !ok {
		return ErrOrderedHashLeafLabNotFound
	}
	rank := int(v.page[orderedHashLeafLabRankStart+int(slot)])
	start, end, ok := v.recordBounds(rank)
	if !ok {
		return ErrOrderedHashLeafLabCorrupt
	}
	keyLength := orderedHashLeafLabKeyLength(v.page, v.layout, rank)
	delta := len(value) - (end - start - keyLength)
	oldHeapEnd := int(v.heapEnd)
	newHeapEnd := oldHeapEnd + delta
	newExtent := orderedHashLeafLabExtentForHeapEnd(newHeapEnd)
	if newExtent > OrderedHashLeafLabPageSize {
		return ErrOrderedHashLeafLabSplit
	}
	if newExtent > len(v.page) {
		return ErrOrderedHashLeafLabResize
	}
	changedStart := start + keyLength
	if delta != 0 {
		changedStart = min(changedStart, 38)
		var boundaries [OrderedHashLeafLabSlotCount + 1]uint16
		for index := 0; index <= int(v.count); index++ {
			boundary, found := v.boundary(index)
			if !found {
				return ErrOrderedHashLeafLabCorrupt
			}
			if index > rank {
				boundary = uint16(int(boundary) + delta)
			}
			boundaries[index] = boundary
		}
		copy(v.page[end+delta:newHeapEnd], v.page[end:oldHeapEnd])
		if delta < 0 {
			clear(v.page[newHeapEnd:oldHeapEnd])
		}
		clear(v.page[v.layout.lowStart : v.layout.checkpointStart+
			v.layout.checkpointCount*2])
		for index := 0; index <= int(v.count); index++ {
			putOrderedHashLeafLabBoundary(v.page, v.layout, index, boundaries[index])
		}
		buildOrderedHashLeafLabCheckpoints(v.page, v.layout, int(v.count)+1)
	}
	copy(v.page[start+keyLength:start+keyLength+len(value)], value)
	flag := byte(1) << uint(rank&7)
	oldOverflow := v.page[v.layout.overflowStart+rank/8]&flag != 0
	if overflow {
		v.page[v.layout.overflowStart+rank/8] |= flag
	} else {
		v.page[v.layout.overflowStart+rank/8] &^= flag
	}
	if oldOverflow != overflow {
		changedStart = min(changedStart, v.layout.overflowStart+rank/8)
	}
	binary.LittleEndian.PutUint16(v.page[38:40], uint16(newHeapEnd))
	clear(v.page[newHeapEnd:orderedHashLeafLabTrailerStart(v.page)])
	resealOrderedHashLeafLabOwned(
		v.page, changedStart, max(oldHeapEnd, newHeapEnd),
	)
	v.heapEnd = uint16(newHeapEnd)
	return nil
}

// MaterializeOwnedDelete removes one row from the exclusively owned admitted
// page. It clears the stable slot without a tombstone, shifts only the two
// lexical heap regions around the deleted row, rewrites bounded rank metadata,
// and retains PageRef identity and physical extent. Call DeleteTo when snapshot
// ownership is not proven or a minimally shrunk extent should be published.
func (v *OrderedHashLeafLabView) MaterializeOwnedDelete(
	slot uint8, key []byte,
) error {
	if _, _, ok := v.LookupSlot(slot, key); !ok {
		return ErrOrderedHashLeafLabNotFound
	}
	deleteRank := int(v.page[orderedHashLeafLabRankStart+int(slot)])
	deleteStart, deleteEnd, ok := v.recordBounds(deleteRank)
	if !ok {
		return ErrOrderedHashLeafLabCorrupt
	}
	oldCount := int(v.count)
	oldLayout := v.layout
	newCount := oldCount - 1
	newLayout := orderedHashLeafLabMakeLayout(newCount)
	oldHeapStart := oldLayout.heapStart
	oldHeapEnd := int(v.heapEnd)
	beforeBytes := deleteStart - oldHeapStart
	afterBytes := oldHeapEnd - deleteEnd
	newHeapEnd := newLayout.heapStart + beforeBytes + afterBytes

	var keyLengths [OrderedHashLeafLabSlotCount]uint8
	var overflows [OrderedHashLeafLabSlotCount]bool
	var recordBytes [OrderedHashLeafLabSlotCount]int
	for rank := 0; rank < oldCount; rank++ {
		start, end, found := v.recordBounds(rank)
		if !found {
			return ErrOrderedHashLeafLabCorrupt
		}
		keyLengths[rank] = uint8(orderedHashLeafLabKeyLength(v.page, oldLayout, rank))
		overflows[rank] = v.rankOverflow(rank)
		recordBytes[rank] = end - start
	}

	copy(
		v.page[newLayout.heapStart:newLayout.heapStart+beforeBytes],
		v.page[oldHeapStart:deleteStart],
	)
	copy(
		v.page[newLayout.heapStart+beforeBytes:newHeapEnd],
		v.page[deleteEnd:oldHeapEnd],
	)
	clear(v.page[orderedHashLeafLabVariableStart:newLayout.heapStart])
	if newHeapEnd < oldHeapEnd {
		clear(v.page[newHeapEnd:oldHeapEnd])
	}

	v.page[orderedHashLeafLabControlStart+int(slot)] = 0
	v.page[orderedHashLeafLabRankStart+int(slot)] = orderedHashLeafLabEmptyRank
	for candidate := 0; candidate < OrderedHashLeafLabSlotCount; candidate++ {
		rank := v.page[orderedHashLeafLabRankStart+candidate]
		if rank != orderedHashLeafLabEmptyRank && int(rank) > deleteRank {
			v.page[orderedHashLeafLabRankStart+candidate] = rank - 1
		}
	}

	cursor := newLayout.heapStart
	outRank := 0
	for rank := 0; rank < oldCount; rank++ {
		if rank == deleteRank {
			continue
		}
		putOrderedHashLeafLabKeyLength(
			v.page, newLayout, outRank, keyLengths[rank],
		)
		if overflows[rank] {
			v.page[newLayout.overflowStart+outRank/8] |=
				byte(1) << uint(outRank&7)
		}
		putOrderedHashLeafLabBoundary(v.page, newLayout, outRank, uint16(cursor))
		cursor += recordBytes[rank]
		outRank++
	}
	putOrderedHashLeafLabBoundary(v.page, newLayout, newCount, uint16(cursor))
	buildOrderedHashLeafLabCheckpoints(v.page, newLayout, newCount+1)

	stashCount := int(v.stashCount)
	if slot >= OrderedHashLeafLabNormalSlotCount {
		stashCount--
	}
	binary.LittleEndian.PutUint16(v.page[16:18], uint16(newCount))
	binary.LittleEndian.PutUint16(v.page[18:20], uint16(stashCount))
	binary.LittleEndian.PutUint16(v.page[36:38], uint16(newLayout.heapStart))
	binary.LittleEndian.PutUint16(v.page[38:40], uint16(newHeapEnd))
	clear(v.page[newHeapEnd:orderedHashLeafLabTrailerStart(v.page)])
	resealOrderedHashLeafLabOwned(v.page, 16, max(oldHeapEnd, newHeapEnd))
	v.count = uint16(newCount)
	v.stashCount = uint16(stashCount)
	v.heapEnd = uint16(newHeapEnd)
	v.layout = newLayout
	return nil
}

// MaterializeOwnedInsert inserts into the exclusively owned admitted page
// without relocating any existing stable slot. It returns the new slot or the
// bounded split/resize signal while preserving PageRef identity and extent.
// InsertTo is the immutable COW fallback.
func (v *OrderedHashLeafLabView) MaterializeOwnedInsert(
	key, value []byte, overflow bool,
) (uint8, error) {
	if len(key) == 0 ||
		len(key) > OrderedHashLeafLabMaxKeyLength || len(value) == 0 ||
		overflow && len(value) != OrderedHashLeafLabOverflowRefSize {
		return 0, fmt.Errorf("%w: ordered-hash leaf materialized insert", ErrInvalidWrite)
	}
	if _, _, _, ok := v.Lookup(key); ok {
		return 0, fmt.Errorf("%w: duplicate ordered-hash leaf key", ErrInvalidWrite)
	}
	if v.count == OrderedHashLeafLabSlotCount {
		return 0, ErrOrderedHashLeafLabSplit
	}
	hash := orderedHashLeafLabKeyHash(v.seed, key)
	slot, ok := v.emptyInsertionSlot(hash)
	if !ok {
		return 0, ErrOrderedHashLeafLabSplit
	}
	insertRank := v.LowerBound(key)
	oldCount := int(v.count)
	oldLayout := v.layout
	newCount := oldCount + 1
	newLayout := orderedHashLeafLabMakeLayout(newCount)
	oldHeapStart := oldLayout.heapStart
	oldHeapEnd := int(v.heapEnd)
	insertOffset := oldHeapEnd
	if insertRank != oldCount {
		boundary, found := v.boundary(insertRank)
		if !found {
			return 0, ErrOrderedHashLeafLabCorrupt
		}
		insertOffset = int(boundary)
	}
	beforeBytes := insertOffset - oldHeapStart
	afterBytes := oldHeapEnd - insertOffset
	insertBytes := len(key) + len(value)
	newHeapEnd := newLayout.heapStart + beforeBytes + insertBytes + afterBytes
	newExtent := orderedHashLeafLabExtentForHeapEnd(newHeapEnd)
	if newExtent > OrderedHashLeafLabPageSize {
		return 0, ErrOrderedHashLeafLabSplit
	}
	if newExtent > len(v.page) {
		return 0, ErrOrderedHashLeafLabResize
	}

	var keyLengths [OrderedHashLeafLabSlotCount]uint8
	var overflows [OrderedHashLeafLabSlotCount]bool
	var recordBytes [OrderedHashLeafLabSlotCount]int
	for rank := 0; rank < oldCount; rank++ {
		start, end, found := v.recordBounds(rank)
		if !found {
			return 0, ErrOrderedHashLeafLabCorrupt
		}
		keyLengths[rank] = uint8(orderedHashLeafLabKeyLength(v.page, oldLayout, rank))
		overflows[rank] = v.rankOverflow(rank)
		recordBytes[rank] = end - start
	}

	afterStart := newLayout.heapStart + beforeBytes + insertBytes
	copy(v.page[afterStart:newHeapEnd], v.page[insertOffset:oldHeapEnd])
	copy(
		v.page[newLayout.heapStart:newLayout.heapStart+beforeBytes],
		v.page[oldHeapStart:insertOffset],
	)
	copy(v.page[newLayout.heapStart+beforeBytes:], key)
	copy(v.page[newLayout.heapStart+beforeBytes+len(key):], value)
	clear(v.page[orderedHashLeafLabVariableStart:newLayout.heapStart])

	for candidate := 0; candidate < OrderedHashLeafLabSlotCount; candidate++ {
		rank := v.page[orderedHashLeafLabRankStart+candidate]
		if rank != orderedHashLeafLabEmptyRank && int(rank) >= insertRank {
			v.page[orderedHashLeafLabRankStart+candidate] = rank + 1
		}
	}
	v.page[orderedHashLeafLabControlStart+int(slot)] =
		orderedHashLeafLabControlLive | byte(hash>>57)
	v.page[orderedHashLeafLabRankStart+int(slot)] = byte(insertRank)

	cursor := newLayout.heapStart
	oldRank := 0
	for rank := 0; rank < newCount; rank++ {
		length := uint8(len(key))
		isOverflow := overflow
		bytesInRecord := insertBytes
		if rank != insertRank {
			length = keyLengths[oldRank]
			isOverflow = overflows[oldRank]
			bytesInRecord = recordBytes[oldRank]
			oldRank++
		}
		putOrderedHashLeafLabKeyLength(v.page, newLayout, rank, length)
		if isOverflow {
			v.page[newLayout.overflowStart+rank/8] |=
				byte(1) << uint(rank&7)
		}
		putOrderedHashLeafLabBoundary(v.page, newLayout, rank, uint16(cursor))
		cursor += bytesInRecord
	}
	putOrderedHashLeafLabBoundary(v.page, newLayout, newCount, uint16(cursor))
	buildOrderedHashLeafLabCheckpoints(v.page, newLayout, newCount+1)

	stashCount := int(v.stashCount)
	if slot >= OrderedHashLeafLabNormalSlotCount {
		stashCount++
	}
	binary.LittleEndian.PutUint16(v.page[16:18], uint16(newCount))
	binary.LittleEndian.PutUint16(v.page[18:20], uint16(stashCount))
	binary.LittleEndian.PutUint16(v.page[36:38], uint16(newLayout.heapStart))
	binary.LittleEndian.PutUint16(v.page[38:40], uint16(newHeapEnd))
	clear(v.page[newHeapEnd:orderedHashLeafLabTrailerStart(v.page)])
	resealOrderedHashLeafLabOwned(v.page, 16, max(oldHeapEnd, newHeapEnd))
	v.count = uint16(newCount)
	v.stashCount = uint16(stashCount)
	v.heapEnd = uint16(newHeapEnd)
	v.layout = newLayout
	return slot, nil
}

// InsertTo writes one canonical after-image. Existing slots never relocate.
// A full candidate set, exhausted stash, row limit, or byte limit reports the
// explicit split signal.
func (v *OrderedHashLeafLabView) InsertTo(
	dst []byte, generation uint64, key, value []byte, overflow bool,
) ([]byte, uint8, error) {
	if generation == 0 || len(key) == 0 || len(key) > OrderedHashLeafLabMaxKeyLength ||
		len(value) == 0 || overflow && len(value) != OrderedHashLeafLabOverflowRefSize ||
		bytesOverlapOrderedHashLeafLab(dst, v.page) {
		return nil, 0, fmt.Errorf("%w: ordered-hash leaf insert", ErrInvalidWrite)
	}
	if _, _, _, ok := v.Lookup(key); ok {
		return nil, 0, fmt.Errorf("%w: duplicate ordered-hash leaf key", ErrInvalidWrite)
	}
	if v.count == OrderedHashLeafLabSlotCount {
		return nil, 0, ErrOrderedHashLeafLabSplit
	}
	hash := orderedHashLeafLabKeyHash(v.seed, key)
	slot, ok := v.emptyInsertionSlot(hash)
	if !ok {
		return nil, 0, ErrOrderedHashLeafLabSplit
	}
	insertRank := v.LowerBound(key)
	var records [OrderedHashLeafLabSlotCount]OrderedHashLeafLabRecord
	rankSlots, valid := v.rankSlots()
	if !valid {
		return nil, 0, ErrOrderedHashLeafLabCorrupt
	}
	out := 0
	for rank := 0; rank <= int(v.count); rank++ {
		if rank == insertRank {
			records[out] = OrderedHashLeafLabRecord{
				Slot: slot, Key: key, Value: value, Overflow: overflow,
			}
			out++
		}
		if rank == int(v.count) {
			break
		}
		recordSlot := rankSlots[rank]
		start, end, _ := v.recordBounds(rank)
		keyLength := orderedHashLeafLabKeyLength(v.page, v.layout, rank)
		records[out] = OrderedHashLeafLabRecord{
			Slot:     recordSlot,
			Key:      v.page[start : start+keyLength : start+keyLength],
			Value:    v.page[start+keyLength : end : end],
			Overflow: v.rankOverflow(rank),
		}
		out++
	}
	page, err := EncodeOrderedHashLeafLab(
		dst, OrderedHashLeafLabHeader{BucketID: v.header.BucketID, Generation: generation},
		v.seed, records[:out],
	)
	if errors.Is(err, ErrOrderedHashLeafLabSplit) {
		return nil, 0, ErrOrderedHashLeafLabSplit
	}
	return page, slot, err
}

func (v *OrderedHashLeafLabView) emptyInsertionSlot(hash uint64) (uint8, bool) {
	first, second := orderedHashLeafLabGroups(hash)
	firstHome := uint8(hash>>16) & 0x0f
	secondHome := uint8(hash>>20) & 0x0f
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group, home := first, firstHome
		if groupIndex != 0 {
			group, home = second, secondHome
		}
		base := group * orderedHashLeafLabGroupSize
		for ordinal := uint8(0); ordinal < orderedHashLeafLabGroupSize; ordinal++ {
			slot := base + (home+ordinal)&(orderedHashLeafLabGroupSize-1)
			if v.page[orderedHashLeafLabControlStart+int(slot)] == 0 {
				return slot, true
			}
		}
	}
	for slot := OrderedHashLeafLabNormalSlotCount; slot < OrderedHashLeafLabSlotCount; slot++ {
		if v.page[orderedHashLeafLabControlStart+slot] == 0 {
			return uint8(slot), true
		}
	}
	return 0, false
}

func (v *OrderedHashLeafLabView) rankSlots() (
	[OrderedHashLeafLabSlotCount]uint8, bool,
) {
	var slots [OrderedHashLeafLabSlotCount]uint8
	var seen [4]uint64
	for slot := 0; slot < OrderedHashLeafLabSlotCount; slot++ {
		if v.page[orderedHashLeafLabControlStart+slot]&
			orderedHashLeafLabControlLive == 0 {
			continue
		}
		rank := v.page[orderedHashLeafLabRankStart+slot]
		if int(rank) >= int(v.count) {
			return slots, false
		}
		bit := uint64(1) << uint(rank&63)
		if seen[rank>>6]&bit != 0 {
			return slots, false
		}
		seen[rank>>6] |= bit
		slots[rank] = uint8(slot)
	}
	for rank := 0; rank < int(v.count); rank++ {
		if seen[rank>>6]&(uint64(1)<<uint(rank&63)) == 0 {
			return slots, false
		}
	}
	return slots, true
}

func bytesOverlapOrderedHashLeafLab(dst, src []byte) bool {
	if len(dst) == 0 || len(src) == 0 {
		return false
	}
	dstStart := uintptr(unsafe.Pointer(&dst[0]))
	dstEnd := dstStart + uintptr(len(dst))
	srcStart := uintptr(unsafe.Pointer(&src[0]))
	srcEnd := srcStart + uintptr(len(src))
	return dstStart < srcEnd && srcStart < dstEnd
}
