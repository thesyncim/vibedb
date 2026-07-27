package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"unsafe"
)

// The adaptive ordered-leaf lab tests a two-class primary leaf whose stable
// slot identity is also the secondary-index posting coordinate. Narrow and
// Wide share slots 0..216 exactly; promotion only adds slots 217..255.
//
// Normal slots use two bounded 16-slot keyed-hash groups. Exceptional
// placements use salted rejection tags over stable stash slots. Admission
// chooses the first canonical salt whose collision group is bounded, so an
// exact miss never degenerates into a whole-leaf key scan. The canonical
// record heap remains lexical, so scans never follow a hash table or a delta
// overlay. Deletes rebuild a tombstone-free image.
const (
	AdaptiveOrderedLeafLabNormalSlots = 192
	AdaptiveOrderedLeafLabNarrowSlots = 217
	AdaptiveOrderedLeafLabWideSlots   = 256
	AdaptiveOrderedLeafLabNarrowStash = 25
	AdaptiveOrderedLeafLabWideStash   = 64

	AdaptiveOrderedLeafLabNarrowLiveTarget = 195
	AdaptiveOrderedLeafLabNarrowBytes      = 4 << 10
	AdaptiveOrderedLeafLabWideBytes        = 8 << 10
	AdaptiveOrderedLeafLabHeaderBytes      = 48
	AdaptiveOrderedLeafLabTrailerBytes     = 8
	AdaptiveOrderedLeafLabMaxKeyBytes      = 127
	AdaptiveOrderedLeafLabOverflowRefBytes = 16
	// The encoded salt is accepted only when every possible stash tag selects
	// at most this many exact key confirmations. Narrow normally needs one or
	// two; the bound also covers a 64-row Wide stash.
	AdaptiveOrderedLeafLabMaxStashExactChecks = 8

	adaptiveOrderedLeafLabCodecVersion = 1
	adaptiveOrderedLeafLabGroupSize    = 16
	adaptiveOrderedLeafLabGroupCount   = AdaptiveOrderedLeafLabNormalSlots /
		adaptiveOrderedLeafLabGroupSize
	adaptiveOrderedLeafLabSelectStride    = 16
	adaptiveOrderedLeafLabLowerBits       = 8
	adaptiveOrderedLeafLabControlLive     = byte(0x80)
	adaptiveOrderedLeafLabControlTag      = byte(0x7f)
	adaptiveOrderedLeafLabControlStart    = AdaptiveOrderedLeafLabHeaderBytes
	adaptiveOrderedLeafLabNormalRankStart = adaptiveOrderedLeafLabControlStart +
		AdaptiveOrderedLeafLabNormalSlots
	adaptiveOrderedLeafLabMagic = "AOLAB001"
)

var (
	ErrAdaptiveOrderedLeafLabCorrupt = errors.New(
		"vibejson: corrupt adaptive ordered-leaf lab",
	)
	ErrAdaptiveOrderedLeafLabNotFound = errors.New(
		"vibejson: adaptive ordered-leaf lab key not found",
	)
	ErrAdaptiveOrderedLeafLabFull = errors.New(
		"vibejson: adaptive ordered-leaf lab class is full",
	)
	ErrAdaptiveOrderedLeafLabNeedsWide = errors.New(
		"vibejson: adaptive ordered-leaf lab needs wide class",
	)
	ErrAdaptiveOrderedLeafLabNeedsRewrite = errors.New(
		"vibejson: adaptive ordered-leaf lab update needs COW rewrite",
	)
	adaptiveOrderedLeafLabGroupForByte = makeAdaptiveOrderedLeafLabGroupTable()
)

func makeAdaptiveOrderedLeafLabGroupTable() [256]uint8 {
	var table [256]uint8
	for index := range table {
		table[index] = uint8(index % adaptiveOrderedLeafLabGroupCount)
	}
	return table
}

// AdaptiveOrderedLeafLabClass is a durable physical leaf class.
type AdaptiveOrderedLeafLabClass uint8

const (
	AdaptiveOrderedLeafLabNarrow AdaptiveOrderedLeafLabClass = 1
	AdaptiveOrderedLeafLabWide   AdaptiveOrderedLeafLabClass = 2
)

func (class AdaptiveOrderedLeafLabClass) extentBytes() int {
	switch class {
	case AdaptiveOrderedLeafLabNarrow:
		return AdaptiveOrderedLeafLabNarrowBytes
	case AdaptiveOrderedLeafLabWide:
		return AdaptiveOrderedLeafLabWideBytes
	default:
		return 0
	}
}

func (class AdaptiveOrderedLeafLabClass) slotCount() int {
	switch class {
	case AdaptiveOrderedLeafLabNarrow:
		return AdaptiveOrderedLeafLabNarrowSlots
	case AdaptiveOrderedLeafLabWide:
		return AdaptiveOrderedLeafLabWideSlots
	default:
		return 0
	}
}

func (class AdaptiveOrderedLeafLabClass) stashSlots() int {
	slots := class.slotCount()
	if slots == 0 {
		return 0
	}
	return slots - AdaptiveOrderedLeafLabNormalSlots
}

// AdaptiveOrderedLeafLabHeader is the PageRef-stable leaf identity.
type AdaptiveOrderedLeafLabHeader struct {
	BucketID   uint64
	Generation uint64
}

// AdaptiveOrderedLeafLabRecord is canonical encoder input. Records must be
// strictly bytewise lexical and have unique stable slots valid for the class.
type AdaptiveOrderedLeafLabRecord struct {
	Slot     uint8
	Key      []byte
	Value    []byte
	Overflow bool
}

// AdaptiveOrderedLeafLabPlan is a zero-allocation preflight result. It fixes
// the exact durable slab before a caller reserves memory and can therefore
// encode directly into a right-sized 4 KiB or 8 KiB buffer.
type AdaptiveOrderedLeafLabPlan struct {
	class         AdaptiveOrderedLeafLabClass
	live          uint16
	stashCount    uint16
	payloadBytes  uint16
	metadataBytes uint16
	extentBytes   uint16
	stashSalt     byte
}

func (p AdaptiveOrderedLeafLabPlan) Class() AdaptiveOrderedLeafLabClass {
	return p.class
}

func (p AdaptiveOrderedLeafLabPlan) Live() int {
	return int(p.live)
}

func (p AdaptiveOrderedLeafLabPlan) StashLive() int {
	return int(p.stashCount)
}

func (p AdaptiveOrderedLeafLabPlan) PayloadBytes() int {
	return int(p.payloadBytes)
}

// MetadataBytes includes the trailer and all persistent structure, but
// excludes key/value payload and physical tail slack.
func (p AdaptiveOrderedLeafLabPlan) MetadataBytes() int {
	return int(p.metadataBytes)
}

func (p AdaptiveOrderedLeafLabPlan) ExtentBytes() int {
	return int(p.extentBytes)
}

// SlabClass is the exact allocator class. It deliberately equals ExtentBytes
// today; naming the allocation contract separately lets a future allocator
// map multiple extents to one resident class without changing the codec API.
func (p AdaptiveOrderedLeafLabPlan) SlabClass() int {
	return int(p.extentBytes)
}

// AdaptiveOrderedLeafLabRow borrows key/value memory from an admitted page.
type AdaptiveOrderedLeafLabRow struct {
	Key      []byte
	Value    []byte
	Overflow bool
}

type adaptiveOrderedLeafLabLayout struct {
	// Point lookup and lexical iteration keep their metadata in the leading
	// cache lines. The placement/stash fields below are admission and mutation
	// cold except when the normal path misses.
	keyLengthStart   int
	overflowStart    int
	lowStart         int
	highStart        int
	highBytes        int
	checkpointStart  int
	checkpointCount  int
	checkpointStride int
	heapStart        int
	controlStart     int
	normalRankStart  int
	stashBitmap      int
	stashBitmapLen   int
	stashRankStart   int
	stashRankLen     int
	denseStashRanks  bool
	stashTagStart    int
	stashTagLen      int
	sortedStashTags  bool
	stashHashStart   int
	stashHashLen     int
	filterStart      int
	filterLen        int
	sparseWide       bool
}

// AdaptiveOrderedLeafLabView is a fail-closed, borrowed leaf view. Admission
// verifies checksums, lexical order, slot/rank bijection, hash placement,
// rejection-filter exactness, succinct boundaries, and canonical padding.
type AdaptiveOrderedLeafLabView struct {
	header     AdaptiveOrderedLeafLabHeader
	seed       [16]byte
	page       []byte
	count      uint16
	stashCount uint16
	heapEnd    uint16
	class      AdaptiveOrderedLeafLabClass
	layout     adaptiveOrderedLeafLabLayout
}

// AdaptiveOrderedLeafLabIterator streams a lexical interval without
// allocating. Returned slices borrow the admitted page.
type AdaptiveOrderedLeafLabIterator struct {
	page           []byte
	upper          []byte
	prefix         []byte
	keyLengthStart int
	overflowStart  int
	lowStart       int
	highStart      int
	highBytes      int
	count          uint16
	rank           uint16
	bitPos         uint16
	offset         uint16
	finished       bool
}

type adaptiveOrderedLeafLabPlacer struct {
	records  []AdaptiveOrderedLeafLabRecord
	seed     [16]byte
	owner    [AdaptiveOrderedLeafLabNormalSlots]int16
	assigned [AdaptiveOrderedLeafLabWideSlots]uint8
	visited  [AdaptiveOrderedLeafLabNormalSlots]uint16
	epoch    uint16
}

func adaptiveOrderedLeafLabMakeLayout(
	class AdaptiveOrderedLeafLabClass, live int,
) adaptiveOrderedLeafLabLayout {
	extent := AdaptiveOrderedLeafLabNarrowBytes
	stashCount := max(0, live-AdaptiveOrderedLeafLabNormalSlots)
	return adaptiveOrderedLeafLabMakeLayoutForExtent(
		class, live, extent, stashCount, false,
	)
}

func adaptiveOrderedLeafLabMakeLayoutForExtent(
	class AdaptiveOrderedLeafLabClass, live, extent, stashCount int,
	indexedSparse bool,
) adaptiveOrderedLeafLabLayout {
	stashSlots := class.stashSlots()
	if stashSlots == 0 || live < 0 || live > class.slotCount() ||
		stashCount < 0 || stashCount > stashSlots ||
		indexedSparse ||
		(extent != AdaptiveOrderedLeafLabNarrowBytes &&
			extent != AdaptiveOrderedLeafLabWideBytes) {
		return adaptiveOrderedLeafLabLayout{}
	}
	boundaries := live + 1
	keyLengthBytes := (live*7 + 7) / 8
	overflowBytes := (live + 7) / 8
	highBytes := (boundaries + extent/(1<<adaptiveOrderedLeafLabLowerBits) + 7) / 8
	checkpointStride := adaptiveOrderedLeafLabSelectStride
	if class == AdaptiveOrderedLeafLabWide {
		checkpointStride = adaptiveOrderedLeafLabSelectStride / 2
	}
	sparseWide := class == AdaptiveOrderedLeafLabWide &&
		live < AdaptiveOrderedLeafLabWideSlots
	if sparseWide {
		// Forty-eight rows plus their typical small-record high-bit growth fit
		// the 64-bit select window, while two fewer checkpoints keep churn
		// images under five structural bytes/live.
		checkpointStride = 56
	} else if class == AdaptiveOrderedLeafLabWide {
		checkpointStride = 12
	}
	checkpointCount := (boundaries + checkpointStride - 1) / checkpointStride
	layout := adaptiveOrderedLeafLabLayout{
		controlStart:     adaptiveOrderedLeafLabControlStart,
		stashTagLen:      stashSlots,
		highBytes:        highBytes,
		checkpointCount:  checkpointCount,
		checkpointStride: checkpointStride,
	}
	bitmapBytes := (stashSlots + 7) / 8
	layout.stashRankLen = stashSlots
	// Sparse images use a bitmap plus live-only lexical ranks when it saves
	// bytes. At high stash occupancy, a 0xff-sentinel rank vector is smaller.
	// Full Wide must use a bitmap because lexical rank 255 is valid.
	if stashCount+bitmapBytes < stashSlots ||
		class == AdaptiveOrderedLeafLabWide && !sparseWide {
		layout.stashBitmapLen = bitmapBytes
		layout.stashRankLen = stashCount
		layout.denseStashRanks = true
	}
	if class == AdaptiveOrderedLeafLabWide {
		layout.sparseWide = sparseWide
		layout.stashTagLen = (stashSlots * 4) / 8
		if stashCount > 16 {
			layout.stashTagLen = stashSlots
		}
		if stashCount > 16 && (stashCount <= 24 || stashCount > 48) {
			// A sorted 12-bit tag directory plus packed six-bit stable slots
			// makes an absent lookup logarithmic without spending a byte per
			// possible slot. Four-bit planes remain cheaper below 17 rows.
			layout.sortedStashTags = true
			layout.stashTagLen = adaptiveOrderedLeafLabPackedBytes(
				stashCount, 12,
			) + adaptiveOrderedLeafLabPackedBytes(stashCount, 6)
			layout.filterLen = 16
			if stashCount > 48 {
				layout.filterLen = 32
			}
		}
	}
	layout.normalRankStart = adaptiveOrderedLeafLabNormalRankStart
	layout.stashBitmap = layout.normalRankStart + AdaptiveOrderedLeafLabNormalSlots
	layout.stashRankStart = layout.stashBitmap + layout.stashBitmapLen
	layout.stashTagStart = layout.stashRankStart + layout.stashRankLen
	layout.stashHashStart = layout.stashTagStart + layout.stashTagLen
	layout.filterStart = layout.stashHashStart + layout.stashHashLen
	layout.keyLengthStart = layout.filterStart + layout.filterLen
	layout.overflowStart = layout.keyLengthStart + keyLengthBytes
	layout.lowStart = layout.overflowStart + overflowBytes
	layout.highStart = layout.lowStart + boundaries
	layout.checkpointStart = layout.highStart + highBytes
	layout.heapStart = layout.checkpointStart + checkpointCount*2
	return layout
}

// AdaptiveOrderedLeafLabMetadataBytes reports the canonical dense-placement
// structure for a live count. A pre-existing stable-slot population can have
// more exceptional rows; PlanAdaptiveOrderedLeafLab is the exact API for an
// actual record set and physical extent.
func AdaptiveOrderedLeafLabMetadataBytes(
	class AdaptiveOrderedLeafLabClass, live int,
) int {
	layout := adaptiveOrderedLeafLabMakeLayout(class, live)
	if layout.heapStart == 0 {
		return 0
	}
	return layout.heapStart + AdaptiveOrderedLeafLabTrailerBytes
}

func AdaptiveOrderedLeafLabMetadataBytesPerLive(
	class AdaptiveOrderedLeafLabClass, live int,
) float64 {
	if live <= 0 {
		return 0
	}
	return float64(AdaptiveOrderedLeafLabMetadataBytes(class, live)) /
		float64(live)
}

// PlaceAdaptiveOrderedLeafLabRecords assigns provisional stable slots before
// first publication. The augmenting matcher fills the shared normal region;
// bounded failures receive monotonically increasing stash slots.
func PlaceAdaptiveOrderedLeafLabRecords(
	class AdaptiveOrderedLeafLabClass,
	seed [16]byte,
	records []AdaptiveOrderedLeafLabRecord,
) error {
	if class.slotCount() == 0 || seed == ([16]byte{}) ||
		len(records) > class.slotCount() {
		return fmt.Errorf("%w: adaptive leaf placement class/count", ErrInvalidWrite)
	}
	placer := adaptiveOrderedLeafLabPlacer{records: records, seed: seed}
	for index := range placer.owner {
		placer.owner[index] = -1
	}
	nextStash := AdaptiveOrderedLeafLabNormalSlots
	for index := range records {
		if len(records[index].Key) == 0 {
			return fmt.Errorf("%w: empty adaptive leaf key", ErrInvalidWrite)
		}
		placer.epoch++
		if placer.epoch == 0 {
			clear(placer.visited[:])
			placer.epoch = 1
		}
		if placer.place(index) {
			continue
		}
		if nextStash >= class.slotCount() {
			if class == AdaptiveOrderedLeafLabNarrow {
				return ErrAdaptiveOrderedLeafLabNeedsWide
			}
			return ErrAdaptiveOrderedLeafLabFull
		}
		placer.assigned[index] = uint8(nextStash)
		nextStash++
	}
	for index := range records {
		records[index].Slot = placer.assigned[index]
	}
	return nil
}

func (p *adaptiveOrderedLeafLabPlacer) place(index int) bool {
	hash := adaptiveOrderedLeafLabKeyHash(p.seed, p.records[index].Key)
	for ordinal := 0; ordinal < adaptiveOrderedLeafLabGroupSize*2; ordinal++ {
		slot := adaptiveOrderedLeafLabCandidate(hash, ordinal)
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

func adaptiveOrderedLeafLabGroups(hash uint64) (uint8, uint8) {
	first := adaptiveOrderedLeafLabGroupForByte[uint8(hash)]
	second := adaptiveOrderedLeafLabGroupForByte[uint8(hash>>8)]
	if second == first {
		second++
		if second == adaptiveOrderedLeafLabGroupCount {
			second = 0
		}
	}
	return first, second
}

func adaptiveOrderedLeafLabCandidate(hash uint64, ordinal int) uint8 {
	first, second := adaptiveOrderedLeafLabGroups(hash)
	group := first
	home := uint8(hash>>16) & (adaptiveOrderedLeafLabGroupSize - 1)
	if ordinal >= adaptiveOrderedLeafLabGroupSize {
		group = second
		home = uint8(hash>>20) & (adaptiveOrderedLeafLabGroupSize - 1)
		ordinal -= adaptiveOrderedLeafLabGroupSize
	}
	return group*adaptiveOrderedLeafLabGroupSize +
		(home+uint8(ordinal))&(adaptiveOrderedLeafLabGroupSize-1)
}

func adaptiveOrderedLeafLabNormalCandidate(hash uint64, slot uint8) bool {
	if int(slot) >= AdaptiveOrderedLeafLabNormalSlots {
		return false
	}
	first, second := adaptiveOrderedLeafLabGroups(hash)
	group := slot / adaptiveOrderedLeafLabGroupSize
	return group == first || group == second
}

func adaptiveOrderedLeafLabKeyHash(seed [16]byte, key []byte) uint64 {
	// Keep the same keyed primitive and eight-byte fast path as the current
	// ordered leaf so the adaptive comparison measures layout, not hashing.
	return orderedHashLeafLabKeyHash(seed, key)
}

func adaptiveOrderedLeafLabBloomAdd(filter []byte, hash uint64) {
	for shift := uint(0); shift < 64; shift += 16 {
		bit := int(byte(hash >> shift))
		if len(filter) != 32 {
			bit %= len(filter) * 8
		}
		filter[bit>>3] |= byte(1) << uint(bit&7)
	}
}

func adaptiveOrderedLeafLabBloomMayContain(filter []byte, hash uint64) bool {
	for shift := uint(0); shift < 64; shift += 16 {
		bit := int(byte(hash >> shift))
		if len(filter) != 32 {
			bit %= len(filter) * 8
		}
		if filter[bit>>3]&(byte(1)<<uint(bit&7)) == 0 {
			return false
		}
	}
	return true
}

func adaptiveOrderedLeafLabStashTag(hash uint64) byte {
	return byte(hash >> 48)
}

func adaptiveOrderedLeafLabSaltedStashTag(
	hash uint64, salt byte, tagBits uint,
) byte {
	// The keyed hash remains the security boundary. Salt only selects a
	// canonical independent projection so admission can cap collision groups
	// without persisting another hash or consulting another page.
	mixed := hash ^ (uint64(salt)+1)*0x9e3779b97f4a7c15
	mixed ^= mixed >> 29
	mixed *= 0xbf58476d1ce4e5b9
	mixed ^= mixed >> 32
	return byte(mixed >> (64 - tagBits))
}

func adaptiveOrderedLeafLabSaltedStashTag16(
	hash uint64, salt byte,
) uint16 {
	mixed := hash ^ (uint64(salt)+1)*0x9e3779b97f4a7c15
	mixed ^= mixed >> 29
	mixed *= 0xbf58476d1ce4e5b9
	mixed ^= mixed >> 32
	return uint16(mixed >> 48)
}

func adaptiveOrderedLeafLabSaltedStashTag12(
	hash uint64, salt byte,
) uint16 {
	return adaptiveOrderedLeafLabSaltedStashTag16(hash, salt) >> 4
}

func adaptiveOrderedLeafLabPackedBytes(count, width int) int {
	return (count*width + 7) / 8
}

func adaptiveOrderedLeafLabPutPacked(
	dst []byte, index, width int, value uint16,
) {
	bit := index * width
	for remaining := width; remaining != 0; {
		at := bit >> 3
		shift := uint(bit & 7)
		take := min(remaining, 8-int(shift))
		mask := uint16(1<<uint(take)) - 1
		dst[at] |= byte(value&mask) << shift
		value >>= uint(take)
		bit += take
		remaining -= take
	}
}

func adaptiveOrderedLeafLabGetPacked(
	src []byte, index, width int,
) uint16 {
	if width == 12 {
		at := (index >> 1) * 3
		if index&1 == 0 {
			return uint16(src[at]) | uint16(src[at+1]&0x0f)<<8
		}
		return uint16(src[at+1]>>4) | uint16(src[at+2])<<4
	}
	if width == 6 {
		bit := index * 6
		at := bit >> 3
		word := uint16(src[at])
		if at+1 < len(src) {
			word |= uint16(src[at+1]) << 8
		}
		return word >> uint(bit&7) & 0x3f
	}
	bit := index * width
	var value uint16
	outShift := uint(0)
	for remaining := width; remaining != 0; {
		at := bit >> 3
		shift := uint(bit & 7)
		take := min(remaining, 8-int(shift))
		mask := byte(1<<uint(take)) - 1
		value |= uint16(src[at]>>shift&mask) << outShift
		outShift += uint(take)
		bit += take
		remaining -= take
	}
	return value
}

func adaptiveOrderedLeafLabBuildSortedStashTags(
	dst []byte,
	hashes *[AdaptiveOrderedLeafLabWideStash]uint64,
	mask uint64,
	count int,
	salt byte,
) {
	clear(dst)
	var tags [AdaptiveOrderedLeafLabWideStash]uint16
	var slots [AdaptiveOrderedLeafLabWideStash]byte
	live := 0
	for mask != 0 {
		slot := bits.TrailingZeros64(mask)
		tag := adaptiveOrderedLeafLabSaltedStashTag12(hashes[slot], salt)
		position := live
		for position != 0 &&
			(tags[position-1] > tag ||
				tags[position-1] == tag && slots[position-1] > byte(slot)) {
			tags[position] = tags[position-1]
			slots[position] = slots[position-1]
			position--
		}
		tags[position] = tag
		slots[position] = byte(slot)
		live++
		mask &= mask - 1
	}
	if live != count {
		panic("adaptive ordered-leaf sorted stash count mismatch")
	}
	tagBytes := adaptiveOrderedLeafLabPackedBytes(count, 12)
	for index := 0; index < count; index++ {
		adaptiveOrderedLeafLabPutPacked(dst[:tagBytes], index, 12, tags[index])
		adaptiveOrderedLeafLabPutPacked(dst[tagBytes:], index, 6, uint16(slots[index]))
	}
}

func adaptiveOrderedLeafLabFilterBits(
	tag uint16, filterBytes int,
) (byte, byte) {
	mask := byte(filterBytes*8 - 1)
	first := byte(tag) & mask
	second := (byte(tag>>4) ^ byte(tag)*0x5d) & mask
	if second == first {
		second = (second + 1) & mask
	}
	return first, second
}

func adaptiveOrderedLeafLabChooseStashSalt(
	hashes *[AdaptiveOrderedLeafLabWideStash]uint64,
	count int,
	tagBits uint,
) (byte, bool) {
	if count == 0 {
		return 0, true
	}
	if tagBits == 16 || tagBits == 12 {
		var tags [AdaptiveOrderedLeafLabWideStash]uint16
		for candidate := 0; candidate < 256; candidate++ {
			valid := true
			for index := 0; index < count; index++ {
				if tagBits == 12 {
					tags[index] = adaptiveOrderedLeafLabSaltedStashTag12(
						hashes[index], byte(candidate),
					)
				} else {
					tags[index] = adaptiveOrderedLeafLabSaltedStashTag16(
						hashes[index], byte(candidate),
					)
				}
				collisions := 1
				for previous := 0; previous < index; previous++ {
					if tags[previous] == tags[index] {
						collisions++
					}
				}
				if collisions > AdaptiveOrderedLeafLabMaxStashExactChecks {
					valid = false
					break
				}
			}
			if valid {
				return byte(candidate), true
			}
		}
		return 0, false
	}
	var buckets [256]uint8
	bucketCount := 1 << tagBits
	for candidate := 0; candidate < 256; candidate++ {
		clear(buckets[:bucketCount])
		valid := true
		for index := 0; index < count; index++ {
			tag := adaptiveOrderedLeafLabSaltedStashTag(
				hashes[index], byte(candidate), tagBits,
			)
			buckets[tag]++
			if buckets[tag] > AdaptiveOrderedLeafLabMaxStashExactChecks {
				valid = false
				break
			}
		}
		if valid {
			return byte(candidate), true
		}
	}
	return 0, false
}

func adaptiveOrderedLeafLabPutStashTag(tags []byte, index int, tag byte) {
	if len(tags) == (AdaptiveOrderedLeafLabWideStash*3+7)/8 {
		bit := uint(index * 3)
		at := bit / 8
		shift := bit & 7
		word := uint16(tags[at]) | uint16(tag&7)<<shift
		tags[at] = byte(word)
		if shift > 5 {
			tags[at+1] |= byte(word >> 8)
		}
		return
	}
	if len(tags) == (AdaptiveOrderedLeafLabWideStash*4+7)/8 {
		bit := uint(index * 4)
		at := bit / 8
		shift := bit & 7
		tags[at] |= (tag & 15) << shift
		return
	}
	tags[index] = tag
}

func adaptiveOrderedLeafLabGetStashTag(tags []byte, index int) byte {
	if len(tags) == (AdaptiveOrderedLeafLabWideStash*3+7)/8 {
		bit := uint(index * 3)
		at := bit / 8
		shift := bit & 7
		word := uint16(tags[at])
		if shift > 5 {
			word |= uint16(tags[at+1]) << 8
		}
		return byte(word>>shift) & 7
	}
	if len(tags) == (AdaptiveOrderedLeafLabWideStash*4+7)/8 {
		bit := uint(index * 4)
		return tags[bit/8] >> (bit & 7) & 15
	}
	return tags[index]
}

func adaptiveOrderedLeafLabWantStashTag(tags []byte, hash uint64) byte {
	tag := adaptiveOrderedLeafLabStashTag(hash)
	if len(tags) == (AdaptiveOrderedLeafLabWideStash*3+7)/8 {
		return tag & 7
	}
	if len(tags) == (AdaptiveOrderedLeafLabWideStash*4+7)/8 {
		return tag & 15
	}
	return tag
}

func adaptiveOrderedLeafLabSparseWideTag(hash uint64) byte {
	return adaptiveOrderedLeafLabSaltedStashTag(hash, 0, 4)
}

func adaptiveOrderedLeafLabPutSparseWideTag(
	tags []byte, index int, hash uint64, salt ...byte,
) {
	var selected byte
	if len(salt) != 0 {
		selected = salt[0]
	}
	tag := adaptiveOrderedLeafLabSaltedStashTag(hash, selected, 4)
	for bit := 0; bit < 4; bit++ {
		if tag&(1<<uint(bit)) != 0 {
			tags[bit*8+index/8] |= byte(1) << uint(index&7)
		}
	}
}

func adaptiveOrderedLeafLabGetSparseWideTag(tags []byte, index int) byte {
	var tag byte
	for bit := 0; bit < 4; bit++ {
		tag |= (tags[bit*8+index/8] >> uint(index&7) & 1) << uint(bit)
	}
	return tag
}

func adaptiveOrderedLeafLabSparseWideTagMask(
	tags []byte, hash uint64, salt ...byte,
) uint64 {
	var selected byte
	if len(salt) != 0 {
		selected = salt[0]
	}
	tag := adaptiveOrderedLeafLabSaltedStashTag(hash, selected, 4)
	mask := ^uint64(0)
	for bit := 0; bit < 4; bit++ {
		plane := binary.LittleEndian.Uint64(tags[bit*8:])
		if tag&(1<<uint(bit)) != 0 {
			mask &= plane
		} else {
			mask &^= plane
		}
	}
	return mask
}

func adaptiveOrderedLeafLabStashTagBits(tags []byte) int {
	if len(tags) == (AdaptiveOrderedLeafLabWideStash*3+7)/8 {
		return 3
	}
	if len(tags) == (AdaptiveOrderedLeafLabWideStash*4+7)/8 {
		return 4
	}
	return 8
}

const (
	adaptiveOrderedLeafLabByteOnes = uint64(0x0101010101010101)
	adaptiveOrderedLeafLabByteHigh = uint64(0x8080808080808080)
)

func adaptiveOrderedLeafLabEqualByteHighBits(word uint64, want byte) uint64 {
	x := word ^ uint64(want)*adaptiveOrderedLeafLabByteOnes
	return (x - adaptiveOrderedLeafLabByteOnes) &^ x &
		adaptiveOrderedLeafLabByteHigh
}

func adaptiveOrderedLeafLabTagMask(
	tags []byte, want byte,
) uint64 {
	var mask uint64
	index := 0
	for ; index+8 <= len(tags); index += 8 {
		matches := adaptiveOrderedLeafLabEqualByteHighBits(
			binary.LittleEndian.Uint64(tags[index:]), want,
		)
		for matches != 0 {
			at := bits.TrailingZeros64(matches) >> 3
			// The subtract-and-mask idiom can mark the byte following a real
			// zero because of a cross-byte borrow. This one-byte confirmation
			// is metadata-only and keeps the exact-key bound unchanged.
			if tags[index+at] == want {
				mask |= uint64(1) << uint(index+at)
			}
			matches &= matches - 1
		}
	}
	for ; index < len(tags); index++ {
		if tags[index] == want {
			mask |= uint64(1) << uint(index)
		}
	}
	return mask
}

// PlanAdaptiveOrderedLeafLab validates canonical input and returns its exact
// metadata, payload, and physical slab without allocating.
func PlanAdaptiveOrderedLeafLab(
	class AdaptiveOrderedLeafLabClass,
	seed [16]byte,
	records []AdaptiveOrderedLeafLabRecord,
) (AdaptiveOrderedLeafLabPlan, error) {
	slots := class.slotCount()
	if slots == 0 || seed == ([16]byte{}) || len(records) > slots {
		return AdaptiveOrderedLeafLabPlan{}, fmt.Errorf(
			"%w: adaptive leaf plan class/count/seed", ErrInvalidWrite,
		)
	}
	payloadBytes := 0
	inputStashCount := 0
	var occupied [4]uint64
	var stashHashes [AdaptiveOrderedLeafLabWideStash]uint64
	for rank := range records {
		record := &records[rank]
		if len(record.Key) == 0 || len(record.Key) > AdaptiveOrderedLeafLabMaxKeyBytes {
			return AdaptiveOrderedLeafLabPlan{}, fmt.Errorf(
				"%w: adaptive leaf key length", ErrInvalidWrite,
			)
		}
		if len(record.Value) == 0 ||
			record.Overflow && len(record.Value) != AdaptiveOrderedLeafLabOverflowRefBytes {
			return AdaptiveOrderedLeafLabPlan{}, fmt.Errorf(
				"%w: adaptive leaf value", ErrInvalidWrite,
			)
		}
		if rank != 0 && bytes.Compare(records[rank-1].Key, record.Key) >= 0 {
			return AdaptiveOrderedLeafLabPlan{}, fmt.Errorf(
				"%w: adaptive leaf keys not lexical", ErrInvalidWrite,
			)
		}
		slot := int(record.Slot)
		if slot >= slots {
			return AdaptiveOrderedLeafLabPlan{}, fmt.Errorf(
				"%w: adaptive leaf slot outside class", ErrInvalidWrite,
			)
		}
		bit := uint64(1) << uint(slot&63)
		if occupied[slot>>6]&bit != 0 {
			return AdaptiveOrderedLeafLabPlan{}, fmt.Errorf(
				"%w: duplicate adaptive leaf slot", ErrInvalidWrite,
			)
		}
		occupied[slot>>6] |= bit
		hash := adaptiveOrderedLeafLabKeyHash(seed, record.Key)
		if slot < AdaptiveOrderedLeafLabNormalSlots &&
			!adaptiveOrderedLeafLabNormalCandidate(hash, record.Slot) {
			return AdaptiveOrderedLeafLabPlan{}, fmt.Errorf(
				"%w: adaptive leaf slot outside candidate groups", ErrInvalidWrite,
			)
		}
		if slot >= AdaptiveOrderedLeafLabNormalSlots {
			stashHashes[inputStashCount] = hash
			inputStashCount++
		}
		if payloadBytes > AdaptiveOrderedLeafLabWideBytes-
			len(record.Key)-len(record.Value) {
			if class == AdaptiveOrderedLeafLabNarrow {
				return AdaptiveOrderedLeafLabPlan{}, ErrAdaptiveOrderedLeafLabNeedsWide
			}
			return AdaptiveOrderedLeafLabPlan{}, ErrAdaptiveOrderedLeafLabFull
		}
		payloadBytes += len(record.Key) + len(record.Value)
	}
	tagBits := uint(8)
	if class == AdaptiveOrderedLeafLabWide && inputStashCount <= 16 {
		tagBits = 4
	} else if class == AdaptiveOrderedLeafLabWide &&
		(inputStashCount <= 24 || inputStashCount > 48) {
		tagBits = 12
	}
	stashSalt, ok := adaptiveOrderedLeafLabChooseStashSalt(
		&stashHashes, inputStashCount, tagBits,
	)
	if !ok {
		// A secret SipHash makes this unreachable for non-malicious inputs.
		// Failing the preflight preserves the hard exact-check bound instead
		// of silently degrading to a whole-stash fallback.
		if class == AdaptiveOrderedLeafLabNarrow {
			return AdaptiveOrderedLeafLabPlan{}, ErrAdaptiveOrderedLeafLabNeedsWide
		}
		return AdaptiveOrderedLeafLabPlan{}, ErrAdaptiveOrderedLeafLabFull
	}
	extent := AdaptiveOrderedLeafLabNarrowBytes
	layout := adaptiveOrderedLeafLabMakeLayoutForExtent(
		class, len(records), extent, inputStashCount, false,
	)
	heapEnd := layout.heapStart + payloadBytes
	if class == AdaptiveOrderedLeafLabWide &&
		heapEnd > extent-AdaptiveOrderedLeafLabTrailerBytes {
		extent = AdaptiveOrderedLeafLabWideBytes
		layout = adaptiveOrderedLeafLabMakeLayoutForExtent(
			class, len(records), extent, inputStashCount, false,
		)
		heapEnd = layout.heapStart + payloadBytes
	}
	trailerStart := extent - AdaptiveOrderedLeafLabTrailerBytes
	if heapEnd > trailerStart {
		if class == AdaptiveOrderedLeafLabNarrow {
			return AdaptiveOrderedLeafLabPlan{}, ErrAdaptiveOrderedLeafLabNeedsWide
		}
		return AdaptiveOrderedLeafLabPlan{}, ErrAdaptiveOrderedLeafLabFull
	}
	return AdaptiveOrderedLeafLabPlan{
		class:         class,
		live:          uint16(len(records)),
		stashCount:    uint16(inputStashCount),
		payloadBytes:  uint16(payloadBytes),
		metadataBytes: uint16(layout.heapStart + AdaptiveOrderedLeafLabTrailerBytes),
		extentBytes:   uint16(extent),
		stashSalt:     stashSalt,
	}, nil
}

// Encode writes this preflight plan into the selected extent. The returned
// full slice hides any unused caller capacity from the admitted view; actual
// residency and allocator savings require the caller to allocate exactly
// plan.SlabClass() bytes, as the allocation contract above specifies.
func (plan AdaptiveOrderedLeafLabPlan) Encode(
	dst []byte,
	header AdaptiveOrderedLeafLabHeader,
	seed [16]byte,
	records []AdaptiveOrderedLeafLabRecord,
) ([]byte, error) {
	current, err := PlanAdaptiveOrderedLeafLab(plan.class, seed, records)
	if err != nil {
		return nil, err
	}
	if current != plan || header.Generation == 0 ||
		len(dst) < plan.ExtentBytes() {
		return nil, fmt.Errorf(
			"%w: stale plan, identity, or destination slab", ErrInvalidWrite,
		)
	}
	for index := range records {
		if adaptiveOrderedLeafLabOverlaps(dst[:plan.ExtentBytes()], records[index].Key) ||
			adaptiveOrderedLeafLabOverlaps(dst[:plan.ExtentBytes()], records[index].Value) {
			return nil, fmt.Errorf(
				"%w: adaptive leaf destination aliases input", ErrInvalidWrite,
			)
		}
	}
	return encodeAdaptiveOrderedLeafLabPlan(dst, header, seed, records, plan)
}

// EncodeAdaptiveOrderedLeafLab is the compatibility entry point. Allocation
// sensitive callers should preflight, allocate plan.SlabClass(), then invoke
// plan.Encode.
func EncodeAdaptiveOrderedLeafLab(
	dst []byte,
	class AdaptiveOrderedLeafLabClass,
	header AdaptiveOrderedLeafLabHeader,
	seed [16]byte,
	records []AdaptiveOrderedLeafLabRecord,
) ([]byte, error) {
	plan, err := PlanAdaptiveOrderedLeafLab(class, seed, records)
	if err != nil {
		return nil, err
	}
	if header.Generation == 0 || len(dst) < plan.ExtentBytes() {
		return nil, fmt.Errorf(
			"%w: adaptive leaf identity or destination slab",
			ErrInvalidWrite,
		)
	}
	for index := range records {
		if adaptiveOrderedLeafLabOverlaps(dst[:plan.ExtentBytes()], records[index].Key) ||
			adaptiveOrderedLeafLabOverlaps(dst[:plan.ExtentBytes()], records[index].Value) {
			return nil, fmt.Errorf(
				"%w: adaptive leaf destination aliases input", ErrInvalidWrite,
			)
		}
	}
	return encodeAdaptiveOrderedLeafLabPlan(
		dst, header, seed, records, plan,
	)
}

func encodeAdaptiveOrderedLeafLabPlan(
	dst []byte,
	header AdaptiveOrderedLeafLabHeader,
	seed [16]byte,
	records []AdaptiveOrderedLeafLabRecord,
	plan AdaptiveOrderedLeafLabPlan,
) ([]byte, error) {
	class := plan.class
	slots := class.slotCount()
	extent := plan.ExtentBytes()
	inputStashCount := plan.StashLive()
	layout := adaptiveOrderedLeafLabMakeLayoutForExtent(
		class, len(records), extent, inputStashCount, false,
	)
	heapEnd := layout.heapStart + plan.PayloadBytes()

	page := dst[:extent:extent]
	clear(page)
	if layout.stashBitmapLen == 0 {
		for index := 0; index < class.stashSlots(); index++ {
			page[layout.stashRankStart+index] = 0xff
		}
	}
	copy(page[:8], adaptiveOrderedLeafLabMagic)
	binary.LittleEndian.PutUint32(page[8:12], DevelopmentFormatVersion)
	page[12] = adaptiveOrderedLeafLabCodecVersion
	page[13] = byte(class)
	binary.LittleEndian.PutUint16(page[14:16], AdaptiveOrderedLeafLabHeaderBytes)
	binary.LittleEndian.PutUint16(page[16:18], uint16(slots))
	binary.LittleEndian.PutUint16(page[18:20], uint16(len(records)))
	page[21] = plan.stashSalt
	binary.LittleEndian.PutUint16(page[22:24], uint16(layout.heapStart))
	binary.LittleEndian.PutUint16(page[24:26], uint16(heapEnd))
	binary.LittleEndian.PutUint64(page[28:36], header.BucketID)
	binary.LittleEndian.PutUint64(page[36:44], header.Generation)

	cursor := layout.heapStart
	stashCount := 0
	var occupied [4]uint64
	var stashRanks [AdaptiveOrderedLeafLabWideStash]byte
	var stashHashesBySlot [AdaptiveOrderedLeafLabWideStash]uint64
	for rank := range records {
		record := &records[rank]
		slot := int(record.Slot)
		if slot >= slots {
			return nil, fmt.Errorf("%w: adaptive leaf slot outside class", ErrInvalidWrite)
		}
		bit := uint64(1) << uint(slot&63)
		if occupied[slot>>6]&bit != 0 {
			return nil, fmt.Errorf("%w: duplicate adaptive leaf slot", ErrInvalidWrite)
		}
		occupied[slot>>6] |= bit
		hash := adaptiveOrderedLeafLabKeyHash(seed, record.Key)
		if slot < AdaptiveOrderedLeafLabNormalSlots {
			if !adaptiveOrderedLeafLabNormalCandidate(hash, record.Slot) {
				return nil, fmt.Errorf("%w: adaptive leaf slot outside candidate groups", ErrInvalidWrite)
			}
			page[layout.controlStart+slot] =
				adaptiveOrderedLeafLabControlLive | byte(hash>>57)
			page[layout.normalRankStart+slot] = byte(rank)
		} else {
			stash := slot - AdaptiveOrderedLeafLabNormalSlots
			if layout.stashBitmapLen != 0 {
				page[layout.stashBitmap+stash/8] |= byte(1) << uint(stash&7)
			}
			if layout.denseStashRanks {
				stashRanks[stash] = byte(rank)
			} else {
				page[layout.stashRankStart+stash] = byte(rank)
			}
			if layout.stashTagLen != 0 {
				tags := page[layout.stashTagStart : layout.stashTagStart+layout.stashTagLen]
				if layout.sortedStashTags {
					stashHashesBySlot[stash] = hash
				} else if class == AdaptiveOrderedLeafLabWide &&
					layout.stashTagLen != class.stashSlots() {
					adaptiveOrderedLeafLabPutSparseWideTag(
						tags, stash, hash, plan.stashSalt,
					)
				} else {
					tagBits := uint(8)
					adaptiveOrderedLeafLabPutStashTag(
						tags, stash,
						adaptiveOrderedLeafLabSaltedStashTag(
							hash, plan.stashSalt, tagBits,
						),
					)
				}
			}
			stashCount++
		}
		adaptiveOrderedLeafLabPutKeyLength(page, &layout, rank, uint8(len(record.Key)))
		if record.Overflow {
			page[layout.overflowStart+rank/8] |= byte(1) << uint(rank&7)
		}
		adaptiveOrderedLeafLabPutBoundary(page, &layout, rank, uint16(cursor))
		copy(page[cursor:], record.Key)
		cursor += len(record.Key)
		copy(page[cursor:], record.Value)
		cursor += len(record.Value)
	}
	if layout.denseStashRanks {
		out := 0
		for index := 0; index < class.stashSlots(); index++ {
			if page[layout.stashBitmap+index/8]&
				(byte(1)<<uint(index&7)) == 0 {
				continue
			}
			page[layout.stashRankStart+out] = stashRanks[index]
			out++
		}
		if out != stashCount {
			panic("adaptive ordered-leaf dense stash rank mismatch")
		}
	}
	if layout.sortedStashTags {
		stashMask := func() uint64 {
			if layout.stashBitmapLen == 4 {
				return uint64(binary.LittleEndian.Uint32(
					page[layout.stashBitmap:],
				))
			}
			if layout.stashBitmapLen == 8 {
				return binary.LittleEndian.Uint64(page[layout.stashBitmap:])
			}
			var mask uint64
			for index := 0; index < class.stashSlots(); index++ {
				if page[layout.stashRankStart+index] != 0xff {
					mask |= uint64(1) << uint(index)
				}
			}
			return mask
		}()
		adaptiveOrderedLeafLabBuildSortedStashTags(
			page[layout.stashTagStart:layout.stashTagStart+layout.stashTagLen],
			&stashHashesBySlot,
			stashMask,
			stashCount,
			plan.stashSalt,
		)
		filter := page[layout.filterStart : layout.filterStart+layout.filterLen]
		for mask := stashMask; mask != 0; mask &= mask - 1 {
			slot := bits.TrailingZeros64(mask)
			tag := adaptiveOrderedLeafLabSaltedStashTag12(
				stashHashesBySlot[slot], plan.stashSalt,
			)
			first, second := adaptiveOrderedLeafLabFilterBits(tag, len(filter))
			filter[first>>3] |= byte(1) << uint(first&7)
			filter[second>>3] |= byte(1) << uint(second&7)
		}
	}
	adaptiveOrderedLeafLabPutBoundary(page, &layout, len(records), uint16(cursor))
	adaptiveOrderedLeafLabBuildCheckpoints(page, &layout, len(records)+1)
	page[20] = byte(stashCount)
	adaptiveOrderedLeafLabSeal(page)
	return page, nil
}

func adaptiveOrderedLeafLabInsertWideHash(
	page []byte, layout adaptiveOrderedLeafLabLayout, hash uint64, stash int,
) {
	home := int(hash>>32) % layout.stashHashLen
	for probe := 0; probe < layout.stashHashLen; probe++ {
		at := layout.stashHashStart + (home+probe)%layout.stashHashLen
		if page[at] == 0 {
			entry := byte(stash + 1)
			if layout.sparseWide {
				entry |= byte(hash>>61&7) << 5
			}
			page[at] = entry
			return
		}
	}
	panic("adaptive ordered-leaf Wide stash directory unexpectedly full")
}

func adaptiveOrderedLeafLabPutKeyLength(
	page []byte, layout *adaptiveOrderedLeafLabLayout, rank int, length uint8,
) {
	bit := rank * 7
	at := layout.keyLengthStart + bit/8
	shift := uint(bit & 7)
	word := uint16(page[at]) | uint16(length)<<shift
	if at+1 < layout.overflowStart {
		word |= uint16(page[at+1]) << 8
	}
	page[at] = byte(word)
	if at+1 < layout.overflowStart {
		page[at+1] = byte(word >> 8)
	}
}

func adaptiveOrderedLeafLabKeyLength(
	page []byte, layout *adaptiveOrderedLeafLabLayout, rank int,
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

func adaptiveOrderedLeafLabPutBoundary(
	page []byte, layout *adaptiveOrderedLeafLabLayout, index int, offset uint16,
) {
	page[layout.lowStart+index] = byte(offset)
	position := int(offset>>adaptiveOrderedLeafLabLowerBits) + index
	page[layout.highStart+position/8] |= byte(1) << uint(position&7)
}

func adaptiveOrderedLeafLabBuildCheckpoints(
	page []byte, layout *adaptiveOrderedLeafLabLayout, boundaries int,
) {
	for index := 0; index < boundaries; index += layout.checkpointStride {
		position, ok := adaptiveOrderedLeafLabSelectFromStart(page, layout, index)
		if !ok {
			panic("adaptive ordered-leaf encoder produced invalid boundary bitmap")
		}
		binary.LittleEndian.PutUint16(
			page[layout.checkpointStart+(index/layout.checkpointStride)*2:],
			uint16(position),
		)
	}
}

func adaptiveOrderedLeafLabSelectFromStart(
	page []byte, layout *adaptiveOrderedLeafLabLayout, target int,
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

func adaptiveOrderedLeafLabSeal(page []byte) {
	trailer := len(page) - AdaptiveOrderedLeafLabTrailerBytes
	checksum := PageChecksum(page[:trailer])
	binary.LittleEndian.PutUint32(page[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(page[trailer+4:], ^checksum)
}

// OpenAdaptiveOrderedLeafLab admits only exact canonical class images.
func OpenAdaptiveOrderedLeafLab(
	src []byte, seed [16]byte,
) (AdaptiveOrderedLeafLabView, error) {
	if (len(src) != AdaptiveOrderedLeafLabNarrowBytes &&
		len(src) != AdaptiveOrderedLeafLabWideBytes) ||
		seed == ([16]byte{}) {
		return AdaptiveOrderedLeafLabView{}, fmt.Errorf("%w: image length/seed", ErrAdaptiveOrderedLeafLabCorrupt)
	}
	trailer := len(src) - AdaptiveOrderedLeafLabTrailerBytes
	checksum := binary.LittleEndian.Uint32(src[trailer : trailer+4])
	if binary.LittleEndian.Uint32(src[trailer+4:]) != ^checksum ||
		PageChecksum(src[:trailer]) != checksum {
		return AdaptiveOrderedLeafLabView{}, fmt.Errorf("%w: checksum", ErrAdaptiveOrderedLeafLabCorrupt)
	}
	class := AdaptiveOrderedLeafLabClass(src[13])
	count := int(binary.LittleEndian.Uint16(src[18:20]))
	stashCount := int(src[20])
	layout := adaptiveOrderedLeafLabMakeLayoutForExtent(
		class, count, len(src), stashCount, false,
	)
	heapEnd := int(binary.LittleEndian.Uint16(src[24:26]))
	validExtent := class == AdaptiveOrderedLeafLabNarrow &&
		len(src) == AdaptiveOrderedLeafLabNarrowBytes ||
		class == AdaptiveOrderedLeafLabWide &&
			(len(src) == AdaptiveOrderedLeafLabNarrowBytes ||
				len(src) == AdaptiveOrderedLeafLabWideBytes)
	if !validExtent || layout.heapStart == 0 ||
		string(src[:8]) != adaptiveOrderedLeafLabMagic ||
		binary.LittleEndian.Uint32(src[8:12]) != DevelopmentFormatVersion ||
		src[12] != adaptiveOrderedLeafLabCodecVersion ||
		binary.LittleEndian.Uint16(src[14:16]) != AdaptiveOrderedLeafLabHeaderBytes ||
		int(binary.LittleEndian.Uint16(src[16:18])) != class.slotCount() ||
		count > class.slotCount() || stashCount > class.stashSlots() ||
		int(binary.LittleEndian.Uint16(src[22:24])) != layout.heapStart ||
		heapEnd < layout.heapStart || heapEnd > trailer ||
		binary.LittleEndian.Uint64(src[36:44]) == 0 ||
		!allZero(src[26:28]) || !allZero(src[44:48]) {
		return AdaptiveOrderedLeafLabView{}, fmt.Errorf("%w: header", ErrAdaptiveOrderedLeafLabCorrupt)
	}
	if class == AdaptiveOrderedLeafLabWide &&
		len(src) == AdaptiveOrderedLeafLabWideBytes {
		compact := adaptiveOrderedLeafLabMakeLayoutForExtent(
			class, count, AdaptiveOrderedLeafLabNarrowBytes,
			stashCount, false,
		)
		payloadBytes := heapEnd - layout.heapStart
		if compact.heapStart+payloadBytes <=
			AdaptiveOrderedLeafLabNarrowBytes-AdaptiveOrderedLeafLabTrailerBytes {
			return AdaptiveOrderedLeafLabView{}, fmt.Errorf(
				"%w: non-minimal Wide extent", ErrAdaptiveOrderedLeafLabCorrupt,
			)
		}
	}
	view := AdaptiveOrderedLeafLabView{
		header: AdaptiveOrderedLeafLabHeader{
			BucketID:   binary.LittleEndian.Uint64(src[28:36]),
			Generation: binary.LittleEndian.Uint64(src[36:44]),
		},
		class: class, seed: seed, page: src, count: uint16(count),
		stashCount: uint16(stashCount), heapEnd: uint16(heapEnd), layout: layout,
	}
	if err := view.validate(); err != nil {
		return AdaptiveOrderedLeafLabView{}, err
	}
	return view, nil
}

func (v AdaptiveOrderedLeafLabView) validate() error {
	boundaries := int(v.count) + 1
	stashBitmapBits := v.class.stashSlots()
	if v.layout.stashBitmapLen == 0 {
		stashBitmapBits = 0
	}
	stashTagBits := v.class.stashSlots() * adaptiveOrderedLeafLabStashTagBits(
		v.page[v.layout.stashTagStart:v.layout.stashTagStart+v.layout.stashTagLen],
	)
	if v.layout.stashTagLen == 0 {
		stashTagBits = 0
	}
	highCount := 0
	for index := 0; index < v.layout.highBytes; index++ {
		highCount += bits.OnesCount8(v.page[v.layout.highStart+index])
	}
	if highCount != boundaries ||
		!adaptiveOrderedLeafLabUnusedBitsZero(
			v.page[v.layout.keyLengthStart:v.layout.overflowStart],
			int(v.count)*7,
		) ||
		!adaptiveOrderedLeafLabUnusedBitsZero(
			v.page[v.layout.overflowStart:v.layout.lowStart],
			int(v.count),
		) ||
		!adaptiveOrderedLeafLabUnusedBitsZero(
			v.page[v.layout.stashBitmap:v.layout.stashBitmap+v.layout.stashBitmapLen],
			stashBitmapBits,
		) ||
		!adaptiveOrderedLeafLabUnusedBitsZero(
			v.page[v.layout.stashTagStart:v.layout.stashTagStart+v.layout.stashTagLen],
			stashTagBits,
		) {
		return fmt.Errorf("%w: compact metadata", ErrAdaptiveOrderedLeafLabCorrupt)
	}
	for index := 0; index < boundaries; index += v.layout.checkpointStride {
		want, ok := adaptiveOrderedLeafLabSelectFromStart(v.page, &v.layout, index)
		got := int(binary.LittleEndian.Uint16(
			v.page[v.layout.checkpointStart+(index/v.layout.checkpointStride)*2:],
		))
		if !ok || got != want {
			return fmt.Errorf("%w: select checkpoint", ErrAdaptiveOrderedLeafLabCorrupt)
		}
	}

	var seenRanks [4]uint64
	var stashHashes [AdaptiveOrderedLeafLabWideStash]uint64
	stashSalt := v.page[21]
	live := 0
	for slot := 0; slot < AdaptiveOrderedLeafLabNormalSlots; slot++ {
		control := v.page[v.layout.controlStart+slot]
		rank := v.page[v.layout.normalRankStart+slot]
		if control == 0 {
			if rank != 0 {
				return fmt.Errorf("%w: empty normal rank", ErrAdaptiveOrderedLeafLabCorrupt)
			}
			continue
		}
		if control&adaptiveOrderedLeafLabControlLive == 0 ||
			int(rank) >= int(v.count) ||
			!v.validateSlotRank(uint8(slot), int(rank), control, &seenRanks) {
			return fmt.Errorf("%w: normal slot", ErrAdaptiveOrderedLeafLabCorrupt)
		}
		live++
	}
	stash := 0
	for index := 0; index < v.class.stashSlots(); index++ {
		rank, isLive := v.stashRankAt(index)
		tag := byte(0)
		if v.layout.stashTagLen != 0 && !v.layout.sortedStashTags {
			tags := v.page[v.layout.stashTagStart : v.layout.stashTagStart+v.layout.stashTagLen]
			if v.class == AdaptiveOrderedLeafLabWide &&
				v.layout.stashTagLen != v.class.stashSlots() {
				tag = adaptiveOrderedLeafLabGetSparseWideTag(tags, index)
			} else {
				tag = adaptiveOrderedLeafLabGetStashTag(tags, index)
			}
		}
		if !isLive {
			emptyRank := rank == 0
			if v.layout.stashBitmapLen == 0 {
				emptyRank = rank == 0xff
			}
			if !emptyRank || tag != 0 {
				return fmt.Errorf("%w: empty stash rank/tag", ErrAdaptiveOrderedLeafLabCorrupt)
			}
			continue
		}
		if int(rank) >= int(v.count) ||
			!v.validateSlotRank(
				uint8(AdaptiveOrderedLeafLabNormalSlots+index),
				int(rank), 0, &seenRanks,
			) {
			return fmt.Errorf("%w: stash slot", ErrAdaptiveOrderedLeafLabCorrupt)
		}
		start, _, _ := v.recordBounds(int(rank))
		keyLength := adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, int(rank))
		hash := adaptiveOrderedLeafLabKeyHash(v.seed, v.page[start:start+keyLength])
		if v.layout.stashTagLen != 0 && !v.layout.sortedStashTags {
			wantTag := adaptiveOrderedLeafLabSaltedStashTag(
				hash, stashSalt, 8,
			)
			if v.class == AdaptiveOrderedLeafLabWide &&
				v.layout.stashTagLen != v.class.stashSlots() {
				wantTag = adaptiveOrderedLeafLabSaltedStashTag(
					hash, stashSalt, 4,
				)
			}
			if tag != wantTag {
				return fmt.Errorf("%w: stash tag slot=%d got=%d want=%d",
					ErrAdaptiveOrderedLeafLabCorrupt, index, tag, wantTag)
			}
		}
		stashHashes[stash] = hash
		stash++
		live++
	}
	tagBits := uint(8)
	if v.layout.sortedStashTags {
		tagBits = 12
	} else if v.class == AdaptiveOrderedLeafLabWide &&
		v.layout.stashTagLen != v.class.stashSlots() {
		tagBits = 4
	}
	canonicalSalt, saltOK := adaptiveOrderedLeafLabChooseStashSalt(
		&stashHashes, stash, tagBits,
	)
	if live != int(v.count) || stash != int(v.stashCount) ||
		!saltOK || stashSalt != canonicalSalt {
		return fmt.Errorf("%w: live count or rejection filter", ErrAdaptiveOrderedLeafLabCorrupt)
	}
	if v.layout.sortedStashTags {
		var hashesBySlot [AdaptiveOrderedLeafLabWideStash]uint64
		hashIndex := 0
		mask := v.stashMask()
		for scan := mask; scan != 0; scan &= scan - 1 {
			slot := bits.TrailingZeros64(scan)
			hashesBySlot[slot] = stashHashes[hashIndex]
			hashIndex++
		}
		var want [AdaptiveOrderedLeafLabWideStash * 3]byte
		adaptiveOrderedLeafLabBuildSortedStashTags(
			want[:v.layout.stashTagLen],
			&hashesBySlot, mask, stash, stashSalt,
		)
		var wantFilter [32]byte
		for scan := mask; scan != 0; scan &= scan - 1 {
			slot := bits.TrailingZeros64(scan)
			tag := adaptiveOrderedLeafLabSaltedStashTag12(
				hashesBySlot[slot], stashSalt,
			)
			first, second := adaptiveOrderedLeafLabFilterBits(
				tag, v.layout.filterLen,
			)
			wantFilter[first>>3] |= byte(1) << uint(first&7)
			wantFilter[second>>3] |= byte(1) << uint(second&7)
		}
		if !bytes.Equal(
			want[:v.layout.stashTagLen],
			v.page[v.layout.stashTagStart:v.layout.stashTagStart+v.layout.stashTagLen],
		) || !bytes.Equal(
			wantFilter[:v.layout.filterLen],
			v.page[v.layout.filterStart:v.layout.filterStart+v.layout.filterLen],
		) {
			return fmt.Errorf(
				"%w: sorted stash tag directory",
				ErrAdaptiveOrderedLeafLabCorrupt,
			)
		}
	}
	for rank := 0; rank < int(v.count); rank++ {
		if seenRanks[rank>>6]&(uint64(1)<<uint(rank&63)) == 0 {
			return fmt.Errorf("%w: missing lexical rank", ErrAdaptiveOrderedLeafLabCorrupt)
		}
		start, end, ok := v.recordBounds(rank)
		keyLength := adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, rank)
		if !ok || keyLength == 0 || start+keyLength >= end ||
			v.rankOverflow(rank) &&
				end-start-keyLength != AdaptiveOrderedLeafLabOverflowRefBytes {
			return fmt.Errorf("%w: lexical record", ErrAdaptiveOrderedLeafLabCorrupt)
		}
		if rank != 0 {
			previousStart, _, _ := v.recordBounds(rank - 1)
			previousLength := adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, rank-1)
			if bytes.Compare(
				v.page[previousStart:previousStart+previousLength],
				v.page[start:start+keyLength],
			) >= 0 {
				return fmt.Errorf("%w: lexical order", ErrAdaptiveOrderedLeafLabCorrupt)
			}
		}
	}
	first, ok := v.boundary(0)
	last, lastOK := v.boundary(int(v.count))
	if !ok || int(first) != v.layout.heapStart || !lastOK ||
		last != v.heapEnd ||
		!allZero(v.page[int(v.heapEnd):len(v.page)-AdaptiveOrderedLeafLabTrailerBytes]) {
		return fmt.Errorf("%w: boundaries or padding", ErrAdaptiveOrderedLeafLabCorrupt)
	}
	return nil
}

func (v AdaptiveOrderedLeafLabView) validateSlotRank(
	slot uint8, rank int, control byte, seen *[4]uint64,
) bool {
	bit := uint64(1) << uint(rank&63)
	if seen[rank>>6]&bit != 0 {
		return false
	}
	start, end, ok := v.recordBounds(rank)
	if !ok {
		return false
	}
	keyLength := adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, rank)
	if keyLength == 0 || start+keyLength >= end {
		return false
	}
	if int(slot) < AdaptiveOrderedLeafLabNormalSlots {
		hash := adaptiveOrderedLeafLabKeyHash(v.seed, v.page[start:start+keyLength])
		if control&adaptiveOrderedLeafLabControlTag != byte(hash>>57) ||
			!adaptiveOrderedLeafLabNormalCandidate(hash, slot) {
			return false
		}
	}
	seen[rank>>6] |= bit
	return true
}

func adaptiveOrderedLeafLabUnusedBitsZero(encoded []byte, usedBits int) bool {
	if len(encoded) == 0 {
		return usedBits == 0
	}
	if usedBits&7 == 0 {
		return true
	}
	return encoded[len(encoded)-1]&^byte((1<<uint(usedBits&7))-1) == 0
}

func (v *AdaptiveOrderedLeafLabView) Header() AdaptiveOrderedLeafLabHeader {
	if v == nil {
		return AdaptiveOrderedLeafLabHeader{}
	}
	return v.header
}

func (v *AdaptiveOrderedLeafLabView) Class() AdaptiveOrderedLeafLabClass {
	if v == nil {
		return 0
	}
	return v.class
}

func (v *AdaptiveOrderedLeafLabView) Len() int {
	if v == nil {
		return 0
	}
	return int(v.count)
}

func (v *AdaptiveOrderedLeafLabView) StashLen() int {
	if v == nil {
		return 0
	}
	return int(v.stashCount)
}

func (v *AdaptiveOrderedLeafLabView) PersistentBytes() []byte {
	if v == nil {
		return nil
	}
	return v.page
}

func (v *AdaptiveOrderedLeafLabView) PhysicalSlackBytes() int {
	if v == nil {
		return 0
	}
	return len(v.page) - AdaptiveOrderedLeafLabTrailerBytes - int(v.heapEnd)
}

// Lookup uses bounded normal probes, then keyed stash rejection metadata and
// exact same-page confirmation.
func (v *AdaptiveOrderedLeafLabView) Lookup(key []byte) (
	slot uint8, value []byte, overflow, ok bool,
) {
	if v == nil || len(key) == 0 || len(key) > AdaptiveOrderedLeafLabMaxKeyBytes {
		return 0, nil, false, false
	}
	return v.LookupHashed(adaptiveOrderedLeafLabKeyHash(v.seed, key), key)
}

// LookupHashed reuses the keyed hash computed by the tablet router.
func (v *AdaptiveOrderedLeafLabView) LookupHashed(
	hash uint64, key []byte,
) (slot uint8, value []byte, overflow, ok bool) {
	if v == nil || v.count == 0 || len(key) == 0 ||
		len(key) > AdaptiveOrderedLeafLabMaxKeyBytes {
		return 0, nil, false, false
	}
	want := adaptiveOrderedLeafLabControlLive | byte(hash>>57)
	first, second := adaptiveOrderedLeafLabGroups(hash)
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group := first
		if groupIndex != 0 {
			group = second
		}
		base := int(group) * adaptiveOrderedLeafLabGroupSize
		controls := v.page[adaptiveOrderedLeafLabControlStart+base : adaptiveOrderedLeafLabControlStart+base+
			adaptiveOrderedLeafLabGroupSize]
		for wordIndex := 0; wordIndex < 2; wordIndex++ {
			matches := adaptiveOrderedLeafLabEqualByteHighBits(
				binary.LittleEndian.Uint64(controls[wordIndex*8:]), want,
			)
			for matches != 0 {
				inWord := bits.TrailingZeros64(matches) >> 3
				if controls[wordIndex*8+inWord] == want {
					candidate := uint8(base + wordIndex*8 + inWord)
					if value, overflow, ok := v.lookupNormalCandidate(
						candidate, key,
					); ok {
						return candidate, value, overflow, true
					}
				}
				matches &= matches - 1
			}
		}
	}
	return v.lookupStashHashed(hash, key)
}

func (v *AdaptiveOrderedLeafLabView) lookupStashHashed(
	hash uint64, key []byte,
) (slot uint8, value []byte, overflow, ok bool) {
	if v.stashCount == 0 {
		return 0, nil, false, false
	}
	tags := v.page[v.layout.stashTagStart : v.layout.stashTagStart+v.layout.stashTagLen]
	salt := v.page[21]
	if v.layout.sortedStashTags {
		count := int(v.stashCount)
		tagBytes := adaptiveOrderedLeafLabPackedBytes(count, 12)
		want := adaptiveOrderedLeafLabSaltedStashTag12(hash, salt)
		first, second := adaptiveOrderedLeafLabFilterBits(
			want, v.layout.filterLen,
		)
		if v.page[v.layout.filterStart+int(first>>3)]&
			(byte(1)<<uint(first&7)) == 0 ||
			v.page[v.layout.filterStart+int(second>>3)]&
				(byte(1)<<uint(second&7)) == 0 {
			return 0, nil, false, false
		}
		low, high := 0, count
		for low < high {
			middle := int(uint(low+high) >> 1)
			if adaptiveOrderedLeafLabGetPacked(
				tags[:tagBytes], middle, 12,
			) < want {
				low = middle + 1
			} else {
				high = middle
			}
		}
		for index := low; index < count &&
			adaptiveOrderedLeafLabGetPacked(
				tags[:tagBytes], index, 12,
			) == want; index++ {
			stash := int(adaptiveOrderedLeafLabGetPacked(
				tags[tagBytes:], index, 6,
			))
			candidate := uint8(AdaptiveOrderedLeafLabNormalSlots + stash)
			if value, overflow, ok := v.lookupStash(stash, key); ok {
				return candidate, value, overflow, true
			}
		}
		return 0, nil, false, false
	}
	var mask uint64
	if v.class == AdaptiveOrderedLeafLabWide &&
		v.layout.stashTagLen != v.class.stashSlots() {
		mask = adaptiveOrderedLeafLabSparseWideTagMask(tags, hash, salt)
	} else {
		want := adaptiveOrderedLeafLabSaltedStashTag(hash, salt, 8)
		mask = adaptiveOrderedLeafLabTagMask(tags, want)
	}
	mask &= v.stashMask()
	for mask != 0 {
		index := bits.TrailingZeros64(mask)
		candidate := uint8(AdaptiveOrderedLeafLabNormalSlots + index)
		if value, overflow, ok := v.lookupStash(index, key); ok {
			return candidate, value, overflow, true
		}
		mask &= mask - 1
	}
	return 0, nil, false, false
}

func (v *AdaptiveOrderedLeafLabView) LookupSlot(
	slot uint8, key []byte,
) (value []byte, overflow, ok bool) {
	if v == nil || len(key) == 0 || int(slot) >= v.class.slotCount() {
		return nil, false, false
	}
	if int(slot) < AdaptiveOrderedLeafLabNormalSlots {
		if v.page[adaptiveOrderedLeafLabControlStart+int(slot)] == 0 {
			return nil, false, false
		}
		return v.lookupNormalCandidate(slot, key)
	}
	index := int(slot) - AdaptiveOrderedLeafLabNormalSlots
	_, isLive := v.stashRankAt(index)
	if !isLive {
		return nil, false, false
	}
	return v.lookupStash(index, key)
}

func (v *AdaptiveOrderedLeafLabView) lookupNormalCandidate(
	slot uint8, key []byte,
) ([]byte, bool, bool) {
	rank := int(v.page[adaptiveOrderedLeafLabNormalRankStart+int(slot)])
	if rank >= int(v.count) ||
		adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, rank) != len(key) {
		return nil, false, false
	}
	start, end, ok := v.recordBounds(rank)
	if !ok || !bytes.Equal(v.page[start:start+len(key)], key) {
		return nil, false, false
	}
	return v.page[start+len(key) : end : end], v.rankOverflow(rank), true
}

func (v *AdaptiveOrderedLeafLabView) lookupStash(
	stash int, key []byte,
) ([]byte, bool, bool) {
	rankByte, live := v.stashRankAt(stash)
	if !live {
		return nil, false, false
	}
	rank := int(rankByte)
	return v.lookupRank(rank, key)
}

func (v *AdaptiveOrderedLeafLabView) lookupRank(
	rank int, key []byte,
) ([]byte, bool, bool) {
	if rank >= int(v.count) ||
		adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, rank) != len(key) {
		return nil, false, false
	}
	start, end, ok := v.recordBounds(rank)
	if !ok || !bytes.Equal(v.page[start:start+len(key)], key) {
		return nil, false, false
	}
	return v.page[start+len(key) : end : end], v.rankOverflow(rank), true
}

func (v *AdaptiveOrderedLeafLabView) stashRankAt(index int) (byte, bool) {
	if v == nil || index < 0 || index >= v.class.stashSlots() {
		return 0, false
	}
	if v.layout.stashBitmapLen == 0 {
		rank := v.page[v.layout.stashRankStart+index]
		return rank, rank != 0xff
	}
	mask := v.stashMask()
	bit := uint64(1) << uint(index)
	if mask&bit == 0 {
		return 0, false
	}
	dense := bits.OnesCount64(mask & (bit - 1))
	if dense >= v.layout.stashRankLen {
		// Preserve raw bitmap liveness so admission detects a bitmap claiming
		// more dense ranks than the canonical stash count.
		return 0, true
	}
	return v.page[v.layout.stashRankStart+dense], true
}

func (v *AdaptiveOrderedLeafLabView) stashMask() uint64 {
	if v.layout.stashBitmapLen == 0 {
		var mask uint64
		for index := 0; index < v.class.stashSlots(); index++ {
			if v.page[v.layout.stashRankStart+index] != 0xff {
				mask |= uint64(1) << uint(index)
			}
		}
		return mask
	}
	if v.layout.stashBitmapLen == 4 {
		return uint64(binary.LittleEndian.Uint32(v.page[v.layout.stashBitmap:]))
	}
	return binary.LittleEndian.Uint64(v.page[v.layout.stashBitmap:])
}

func (v *AdaptiveOrderedLeafLabView) rankOverflow(rank int) bool {
	return v.page[v.layout.overflowStart+rank/8]&(byte(1)<<uint(rank&7)) != 0
}

func (v *AdaptiveOrderedLeafLabView) boundary(index int) (uint16, bool) {
	if index < 0 || index > int(v.count) {
		return 0, false
	}
	checkpoint := index / v.layout.checkpointStride
	baseIndex := checkpoint * v.layout.checkpointStride
	position := int(binary.LittleEndian.Uint16(
		v.page[v.layout.checkpointStart+checkpoint*2:],
	))
	if !adaptiveOrderedLeafLabHighBit(v.page, &v.layout, position) {
		return 0, false
	}
	if advance := index - baseIndex; advance != 0 {
		next, ok := adaptiveOrderedLeafLabAdvanceHighBits(
			v.page, &v.layout, position, advance,
		)
		if !ok {
			return 0, false
		}
		position = next
	}
	high := position - index
	if high < 0 || high >= len(v.page)>>adaptiveOrderedLeafLabLowerBits {
		return 0, false
	}
	return uint16(high<<adaptiveOrderedLeafLabLowerBits) |
		uint16(v.page[v.layout.lowStart+index]), true
}

func adaptiveOrderedLeafLabAdvanceHighBits(
	page []byte, layout *adaptiveOrderedLeafLabLayout, position, advance int,
) (int, bool) {
	if position < 0 || advance < 0 || position >= layout.highBytes*8 {
		return 0, false
	}
	byteIndex := position / 8
	bitIndex := uint(position & 7)
	// A canonical small-record leaf keeps the next checkpoint interval inside
	// one 64-bit window. Load it in one operation and select by repeatedly
	// clearing the lowest set bit. The cold fallback preserves correctness for
	// unusually large records whose high-bit span crosses the window.
	word := binary.LittleEndian.Uint64(page[layout.highStart+byteIndex:]) >> bitIndex
	available := layout.highBytes*8 - position
	if available < 64 {
		word &= uint64(1)<<uint(available) - 1
	}
	if word&1 == 0 {
		return 0, false
	}
	for remaining := advance; remaining != 0; remaining-- {
		word &= word - 1
		if word == 0 {
			next := position
			for skipped := 0; skipped < advance; skipped++ {
				var ok bool
				next, ok = adaptiveOrderedLeafLabNextHighBit(
					page, layout, next+1,
				)
				if !ok {
					return 0, false
				}
			}
			return next, true
		}
	}
	return position + bits.TrailingZeros64(word), true
}

func (v *AdaptiveOrderedLeafLabView) recordBounds(rank int) (int, int, bool) {
	if rank < 0 || rank >= int(v.count) {
		return 0, 0, false
	}
	position, ok := v.selectBitPosition(rank)
	if !ok {
		return 0, 0, false
	}
	nextPosition, ok := adaptiveOrderedLeafLabNextHighBit(
		v.page, &v.layout, position+1,
	)
	if !ok {
		return 0, 0, false
	}
	start := uint16((position-rank)<<adaptiveOrderedLeafLabLowerBits) |
		uint16(v.page[v.layout.lowStart+rank])
	nextRank := rank + 1
	end := uint16((nextPosition-nextRank)<<adaptiveOrderedLeafLabLowerBits) |
		uint16(v.page[v.layout.lowStart+nextRank])
	if end <= start {
		return 0, 0, false
	}
	return int(start), int(end), true
}

func adaptiveOrderedLeafLabHighBit(
	page []byte, layout *adaptiveOrderedLeafLabLayout, position int,
) bool {
	return position >= 0 && position < layout.highBytes*8 &&
		page[layout.highStart+position/8]&(byte(1)<<uint(position&7)) != 0
}

func adaptiveOrderedLeafLabNextHighBit(
	page []byte, layout *adaptiveOrderedLeafLabLayout, position int,
) (int, bool) {
	return adaptiveOrderedLeafLabNextHighBitAt(
		page, layout.highStart, layout.highBytes, position,
	)
}

func adaptiveOrderedLeafLabNextHighBitAt(
	page []byte, highStart, highBytes, position int,
) (int, bool) {
	if position < 0 || position >= highBytes*8 {
		return 0, false
	}
	byteIndex := position / 8
	value := page[highStart+byteIndex] &^
		(byte(1)<<uint(position&7) - 1)
	for {
		if value != 0 {
			return byteIndex*8 + bits.TrailingZeros8(value), true
		}
		byteIndex++
		if byteIndex >= highBytes {
			return 0, false
		}
		value = page[highStart+byteIndex]
	}
}

func (v *AdaptiveOrderedLeafLabView) selectBitPosition(index int) (int, bool) {
	offset, ok := v.boundary(index)
	if !ok {
		return 0, false
	}
	return int(offset>>adaptiveOrderedLeafLabLowerBits) + index, true
}

// LowerBound returns the first lexical rank whose key is >= key.
func (v *AdaptiveOrderedLeafLabView) LowerBound(key []byte) int {
	if v == nil {
		return 0
	}
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		start, _, _ := v.recordBounds(middle)
		length := adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, middle)
		if bytes.Compare(v.page[start:start+length], key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func (v *AdaptiveOrderedLeafLabView) AllRows() AdaptiveOrderedLeafLabIterator {
	return v.iteratorAt(0, nil, nil)
}

func (v *AdaptiveOrderedLeafLabView) Range(
	lower, upper []byte,
) AdaptiveOrderedLeafLabIterator {
	return v.iteratorAt(v.LowerBound(lower), upper, nil)
}

func (v *AdaptiveOrderedLeafLabView) Prefix(
	prefix []byte,
) AdaptiveOrderedLeafLabIterator {
	return v.iteratorAt(v.LowerBound(prefix), nil, prefix)
}

func (v *AdaptiveOrderedLeafLabView) iteratorAt(
	rank int, upper, prefix []byte,
) AdaptiveOrderedLeafLabIterator {
	iterator := AdaptiveOrderedLeafLabIterator{
		page: v.page, upper: upper, prefix: prefix,
		keyLengthStart: v.layout.keyLengthStart,
		overflowStart:  v.layout.overflowStart,
		lowStart:       v.layout.lowStart,
		highStart:      v.layout.highStart,
		highBytes:      v.layout.highBytes,
		count:          v.count,
		rank:           uint16(rank),
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
	iterator.offset = uint16((position-rank)<<adaptiveOrderedLeafLabLowerBits) |
		uint16(v.page[iterator.lowStart+rank])
	return iterator
}

func (it *AdaptiveOrderedLeafLabIterator) Next() (
	AdaptiveOrderedLeafLabRow, bool,
) {
	key, value, overflow, ok := it.NextBorrowed()
	return AdaptiveOrderedLeafLabRow{
		Key: key, Value: value, Overflow: overflow,
	}, ok
}

func (it *AdaptiveOrderedLeafLabIterator) NextBorrowed() (
	key, value []byte, overflow, ok bool,
) {
	if it == nil || it.finished || it.rank >= it.count {
		return nil, nil, false, false
	}
	rank := int(it.rank)
	nextPosition, found := adaptiveOrderedLeafLabNextHighBitAt(
		it.page, it.highStart, it.highBytes, int(it.bitPos)+1,
	)
	if !found {
		it.finished = true
		return nil, nil, false, false
	}
	nextRank := rank + 1
	end := uint16((nextPosition-nextRank)<<adaptiveOrderedLeafLabLowerBits) |
		uint16(it.page[it.lowStart+nextRank])
	bit := rank * 7
	at := it.keyLengthStart + bit/8
	shift := uint(bit & 7)
	word := uint16(it.page[at])
	if at+1 < it.overflowStart {
		word |= uint16(it.page[at+1]) << 8
	}
	keyLength := int(word>>shift) & 0x7f
	start := int(it.offset)
	key = it.page[start : start+keyLength : start+keyLength]
	if len(it.upper) != 0 && bytes.Compare(key, it.upper) >= 0 ||
		len(it.prefix) != 0 && !bytes.HasPrefix(key, it.prefix) {
		it.finished = true
		return nil, nil, false, false
	}
	value = it.page[start+keyLength : int(end) : int(end)]
	overflow = it.page[it.overflowStart+rank/8]&
		(byte(1)<<uint(rank&7)) != 0
	it.rank++
	it.bitPos = uint16(nextPosition)
	it.offset = end
	if it.rank >= it.count {
		it.finished = true
	}
	return key, value, overflow, true
}

// MaterializeOwnedUpdate changes only an equal-length value in an exclusively
// owned image. A size change is explicit COW work.
func (v *AdaptiveOrderedLeafLabView) MaterializeOwnedUpdate(
	slot uint8, key, value []byte, overflow bool,
) error {
	if v == nil || len(value) == 0 ||
		overflow && len(value) != AdaptiveOrderedLeafLabOverflowRefBytes {
		return fmt.Errorf("%w: adaptive owned update", ErrInvalidWrite)
	}
	old, _, ok := v.LookupSlot(slot, key)
	if !ok {
		return ErrAdaptiveOrderedLeafLabNotFound
	}
	if len(old) != len(value) {
		return ErrAdaptiveOrderedLeafLabNeedsRewrite
	}
	rank, ok := v.slotRank(slot)
	if !ok {
		return ErrAdaptiveOrderedLeafLabCorrupt
	}
	start, _, ok := v.recordBounds(rank)
	if !ok {
		return ErrAdaptiveOrderedLeafLabCorrupt
	}
	keyLength := adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, rank)
	copy(v.page[start+keyLength:start+keyLength+len(value)], value)
	flag := byte(1) << uint(rank&7)
	if overflow {
		v.page[v.layout.overflowStart+rank/8] |= flag
	} else {
		v.page[v.layout.overflowStart+rank/8] &^= flag
	}
	adaptiveOrderedLeafLabSeal(v.page)
	return nil
}

// UpdateTo writes a canonical COW after-image and preserves key, rank, slot,
// and every other record.
func (v *AdaptiveOrderedLeafLabView) UpdateTo(
	dst []byte, generation uint64, slot uint8, key, value []byte, overflow bool,
) ([]byte, error) {
	if generation == 0 || len(value) == 0 ||
		overflow && len(value) != AdaptiveOrderedLeafLabOverflowRefBytes ||
		adaptiveOrderedLeafLabOverlaps(dst, v.page) {
		return nil, fmt.Errorf("%w: adaptive COW update", ErrInvalidWrite)
	}
	if _, _, ok := v.LookupSlot(slot, key); !ok {
		return nil, ErrAdaptiveOrderedLeafLabNotFound
	}
	var records [AdaptiveOrderedLeafLabWideSlots]AdaptiveOrderedLeafLabRecord
	count, ok := v.copyRecords(records[:], -1)
	if !ok {
		return nil, ErrAdaptiveOrderedLeafLabCorrupt
	}
	rank, _ := v.slotRank(slot)
	records[rank].Value = value
	records[rank].Overflow = overflow
	return EncodeAdaptiveOrderedLeafLab(
		dst, v.class,
		AdaptiveOrderedLeafLabHeader{BucketID: v.header.BucketID, Generation: generation},
		v.seed, records[:count],
	)
}

// DeleteTo emits a tombstone-free COW image preserving every surviving slot.
func (v *AdaptiveOrderedLeafLabView) DeleteTo(
	dst []byte, generation uint64, slot uint8, key []byte,
) ([]byte, error) {
	if generation == 0 || adaptiveOrderedLeafLabOverlaps(dst, v.page) {
		return nil, fmt.Errorf("%w: adaptive delete", ErrInvalidWrite)
	}
	rank, ok := v.slotRank(slot)
	if !ok {
		return nil, ErrAdaptiveOrderedLeafLabNotFound
	}
	if _, _, ok := v.LookupSlot(slot, key); !ok {
		return nil, ErrAdaptiveOrderedLeafLabNotFound
	}
	var records [AdaptiveOrderedLeafLabWideSlots]AdaptiveOrderedLeafLabRecord
	count, valid := v.copyRecords(records[:], rank)
	if !valid {
		return nil, ErrAdaptiveOrderedLeafLabCorrupt
	}
	return EncodeAdaptiveOrderedLeafLab(
		dst, v.class,
		AdaptiveOrderedLeafLabHeader{BucketID: v.header.BucketID, Generation: generation},
		v.seed, records[:count],
	)
}

// RestoreTo inserts a deleted key at its previous stable slot. This is the
// secondary-posting-safe delete+restore path.
func (v *AdaptiveOrderedLeafLabView) RestoreTo(
	dst []byte,
	generation uint64,
	slot uint8,
	key, value []byte,
	overflow bool,
) ([]byte, error) {
	return v.insertAtTo(dst, generation, slot, key, value, overflow)
}

// InsertTo assigns a currently empty candidate or stash slot without moving
// any published stable slot.
func (v *AdaptiveOrderedLeafLabView) InsertTo(
	dst []byte,
	generation uint64,
	key, value []byte,
	overflow bool,
) ([]byte, uint8, error) {
	if _, _, _, ok := v.Lookup(key); ok {
		return nil, 0, fmt.Errorf("%w: duplicate adaptive leaf key", ErrInvalidWrite)
	}
	if int(v.count) >= v.class.slotCount() {
		if v.class == AdaptiveOrderedLeafLabNarrow {
			return nil, 0, ErrAdaptiveOrderedLeafLabNeedsWide
		}
		return nil, 0, ErrAdaptiveOrderedLeafLabFull
	}
	hash := adaptiveOrderedLeafLabKeyHash(v.seed, key)
	slot, ok := v.emptySlot(hash)
	if !ok {
		if v.class == AdaptiveOrderedLeafLabNarrow {
			return nil, 0, ErrAdaptiveOrderedLeafLabNeedsWide
		}
		return nil, 0, ErrAdaptiveOrderedLeafLabFull
	}
	page, err := v.insertAtTo(dst, generation, slot, key, value, overflow)
	return page, slot, err
}

func (v *AdaptiveOrderedLeafLabView) insertAtTo(
	dst []byte,
	generation uint64,
	slot uint8,
	key, value []byte,
	overflow bool,
) ([]byte, error) {
	if v == nil || generation == 0 || len(key) == 0 ||
		len(key) > AdaptiveOrderedLeafLabMaxKeyBytes || len(value) == 0 ||
		overflow && len(value) != AdaptiveOrderedLeafLabOverflowRefBytes ||
		int(slot) >= v.class.slotCount() ||
		adaptiveOrderedLeafLabOverlaps(dst, v.page) {
		return nil, fmt.Errorf("%w: adaptive insert", ErrInvalidWrite)
	}
	if _, _, _, ok := v.Lookup(key); ok {
		return nil, fmt.Errorf("%w: duplicate adaptive leaf key", ErrInvalidWrite)
	}
	if _, occupied := v.slotRank(slot); occupied {
		return nil, fmt.Errorf("%w: occupied adaptive leaf slot", ErrInvalidWrite)
	}
	hash := adaptiveOrderedLeafLabKeyHash(v.seed, key)
	if int(slot) < AdaptiveOrderedLeafLabNormalSlots &&
		!adaptiveOrderedLeafLabNormalCandidate(hash, slot) {
		return nil, fmt.Errorf("%w: restored key incompatible with normal slot", ErrInvalidWrite)
	}
	insertRank := v.LowerBound(key)
	var records [AdaptiveOrderedLeafLabWideSlots]AdaptiveOrderedLeafLabRecord
	count, valid := v.copyRecords(records[:], -1)
	if !valid {
		return nil, ErrAdaptiveOrderedLeafLabCorrupt
	}
	copy(records[insertRank+1:count+1], records[insertRank:count])
	records[insertRank] = AdaptiveOrderedLeafLabRecord{
		Slot: slot, Key: key, Value: value, Overflow: overflow,
	}
	return EncodeAdaptiveOrderedLeafLab(
		dst, v.class,
		AdaptiveOrderedLeafLabHeader{BucketID: v.header.BucketID, Generation: generation},
		v.seed, records[:count+1],
	)
}

// PromoteAdaptiveOrderedLeafLab re-encodes Narrow as Wide while preserving
// every existing stable slot exactly.
func PromoteAdaptiveOrderedLeafLab(
	dst []byte,
	generation uint64,
	narrow *AdaptiveOrderedLeafLabView,
) ([]byte, error) {
	if narrow == nil || narrow.class != AdaptiveOrderedLeafLabNarrow ||
		generation == 0 || adaptiveOrderedLeafLabOverlaps(dst, narrow.page) {
		return nil, fmt.Errorf("%w: adaptive promotion", ErrInvalidWrite)
	}
	var records [AdaptiveOrderedLeafLabWideSlots]AdaptiveOrderedLeafLabRecord
	count, ok := narrow.copyRecords(records[:], -1)
	if !ok {
		return nil, ErrAdaptiveOrderedLeafLabCorrupt
	}
	return EncodeAdaptiveOrderedLeafLab(
		dst, AdaptiveOrderedLeafLabWide,
		AdaptiveOrderedLeafLabHeader{
			BucketID: narrow.header.BucketID, Generation: generation,
		},
		narrow.seed, records[:count],
	)
}

func (v *AdaptiveOrderedLeafLabView) copyRecords(
	dst []AdaptiveOrderedLeafLabRecord, skipRank int,
) (int, bool) {
	slots, ok := v.rankSlots()
	if !ok || len(dst) < int(v.count) {
		return 0, false
	}
	out := 0
	for rank := 0; rank < int(v.count); rank++ {
		if rank == skipRank {
			continue
		}
		start, end, valid := v.recordBounds(rank)
		if !valid {
			return 0, false
		}
		keyLength := adaptiveOrderedLeafLabKeyLength(v.page, &v.layout, rank)
		dst[out] = AdaptiveOrderedLeafLabRecord{
			Slot:     slots[rank],
			Key:      v.page[start : start+keyLength : start+keyLength],
			Value:    v.page[start+keyLength : end : end],
			Overflow: v.rankOverflow(rank),
		}
		out++
	}
	return out, true
}

func (v *AdaptiveOrderedLeafLabView) rankSlots() (
	[AdaptiveOrderedLeafLabWideSlots]uint8, bool,
) {
	var slots [AdaptiveOrderedLeafLabWideSlots]uint8
	var seen [4]uint64
	for slot := 0; slot < AdaptiveOrderedLeafLabNormalSlots; slot++ {
		if v.page[v.layout.controlStart+slot] == 0 {
			continue
		}
		rank := v.page[v.layout.normalRankStart+slot]
		bit := uint64(1) << uint(rank&63)
		if int(rank) >= int(v.count) || seen[rank>>6]&bit != 0 {
			return slots, false
		}
		seen[rank>>6] |= bit
		slots[rank] = uint8(slot)
	}
	mask := v.stashMask()
	for mask != 0 {
		index := bits.TrailingZeros64(mask)
		rank, live := v.stashRankAt(index)
		if !live {
			return slots, false
		}
		bit := uint64(1) << uint(rank&63)
		if int(rank) >= int(v.count) || seen[rank>>6]&bit != 0 {
			return slots, false
		}
		seen[rank>>6] |= bit
		slots[rank] = uint8(AdaptiveOrderedLeafLabNormalSlots + index)
		mask &= mask - 1
	}
	for rank := 0; rank < int(v.count); rank++ {
		if seen[rank>>6]&(uint64(1)<<uint(rank&63)) == 0 {
			return slots, false
		}
	}
	return slots, true
}

func (v *AdaptiveOrderedLeafLabView) slotRank(slot uint8) (int, bool) {
	if v == nil || int(slot) >= v.class.slotCount() {
		return 0, false
	}
	if int(slot) < AdaptiveOrderedLeafLabNormalSlots {
		if v.page[v.layout.controlStart+int(slot)] == 0 {
			return 0, false
		}
		return int(v.page[v.layout.normalRankStart+int(slot)]), true
	}
	index := int(slot) - AdaptiveOrderedLeafLabNormalSlots
	rank, isLive := v.stashRankAt(index)
	if !isLive {
		return 0, false
	}
	return int(rank), true
}

func (v *AdaptiveOrderedLeafLabView) emptySlot(hash uint64) (uint8, bool) {
	first, second := adaptiveOrderedLeafLabGroups(hash)
	firstHome := uint8(hash>>16) & (adaptiveOrderedLeafLabGroupSize - 1)
	secondHome := uint8(hash>>20) & (adaptiveOrderedLeafLabGroupSize - 1)
	for groupIndex := 0; groupIndex < 2; groupIndex++ {
		group, home := first, firstHome
		if groupIndex != 0 {
			group, home = second, secondHome
		}
		base := group * adaptiveOrderedLeafLabGroupSize
		for ordinal := uint8(0); ordinal < adaptiveOrderedLeafLabGroupSize; ordinal++ {
			slot := base + (home+ordinal)&(adaptiveOrderedLeafLabGroupSize-1)
			if v.page[v.layout.controlStart+int(slot)] == 0 {
				return slot, true
			}
		}
	}
	mask := v.stashMask()
	for index := 0; index < v.class.stashSlots(); index++ {
		if mask&(uint64(1)<<uint(index)) == 0 {
			return uint8(AdaptiveOrderedLeafLabNormalSlots + index), true
		}
	}
	return 0, false
}

func adaptiveOrderedLeafLabOverlaps(dst, src []byte) bool {
	if len(dst) == 0 || len(src) == 0 {
		return false
	}
	dstStart := uintptr(unsafe.Pointer(&dst[0]))
	dstEnd := dstStart + uintptr(len(dst))
	srcStart := uintptr(unsafe.Pointer(&src[0]))
	srcEnd := srcStart + uintptr(len(src))
	return dstStart < srcEnd && srcStart < dstEnd
}
