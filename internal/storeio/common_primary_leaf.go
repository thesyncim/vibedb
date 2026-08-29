package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"unsafe"
)

// This is the common-page form of the adaptive ordered primary leaf.
// PagePrimaryLeaf is its only durable discriminator. The payload does not carry
// a second magic, version, identity, generation, or class: common framing and
// the selecting PageRef already bind them.
//
// Stable hash slots and lexical ranks are independent. Normal slots use two
// bounded keyed-hash groups. Narrow stash misses are rejected by a compact
// filter; Wide uses a 50%-loaded exact-confirmation directory. Record
// boundaries are succinct and the record heap is physically lexical.
const (
	CommonPrimaryLeafNarrowBytes = 4 << 10
	CommonPrimaryLeafWideBytes   = 8 << 10
	// CommonPrimaryLeafMaxExtentBytes is the largest physical extent a common
	// primary leaf or its owned raw mutation workspace may occupy.
	CommonPrimaryLeafMaxExtentBytes = 64 << 10

	CommonPrimaryLeafNormalSlots = 192
	CommonPrimaryLeafNarrowSlots = 217
	CommonPrimaryLeafWideSlots   = 256
	CommonPrimaryLeafNarrowLive  = 195
	CommonPrimaryLeafMaxKeyBytes = 256

	// Overflow values carry the complete durable PageRef used by the existing
	// overflow chain. The first overflow page carries Total; inventing a
	// shorter, lossy leaf descriptor would make recovery or validation depend
	// on unchecked ambient state.
	CommonPrimaryLeafOverflowBytes = PageRefSize

	CommonPrimaryLeafLeafLogicalIDBase     = PrimaryLeafLogicalIDBase
	CommonPrimaryLeafLeafLogicalIDLimit    = PrimaryLeafLogicalIDLimit
	CommonPrimaryLeafFirstDynamicLogicalID = PrimaryFirstDynamicLogicalID

	commonPrimaryLeafPayloadHeader = 3
	commonPrimaryLeafGroupSize     = 16
	commonPrimaryLeafGroupCount    = CommonPrimaryLeafNormalSlots /
		commonPrimaryLeafGroupSize
	commonPrimaryLeafLowerBits    = 8
	commonPrimaryLeafControlLive  = byte(0x80)
	commonPrimaryLeafControlTag   = byte(0x7f)
	commonPrimaryLeafNarrowFilter = 32
	commonPrimaryLeafWideHash     = 128
	commonPrimaryLeafEscapeLength = 127
	commonPrimaryLeafPageKind     = PagePrimaryLeaf
)

var (
	// ErrCommonPrimaryLeafCorrupt reports that a leaf image failed checksum or
	// structural admission and must not be read.
	ErrCommonPrimaryLeafCorrupt = errors.New(
		"vibedb: corrupt common primary leaf",
	)
	// ErrCommonPrimaryLeafNotFound reports that a key is absent from the leaf.
	ErrCommonPrimaryLeafNotFound = errors.New(
		"vibedb: common primary leaf key not found",
	)
	// ErrCommonPrimaryLeafFull reports that the current slot class has no room
	// for another record; the caller must split.
	ErrCommonPrimaryLeafFull = errors.New(
		"vibedb: common primary leaf class is full",
	)
	// ErrCommonPrimaryLeafNeedsWide reports that the rows exceed the narrow
	// budget and must be re-encoded in the wide class.
	ErrCommonPrimaryLeafNeedsWide = errors.New(
		"vibedb: common primary leaf needs wide class",
	)
)

// CommonPrimaryLeafClass is the encoded class byte. Production durable primary
// leaves use CommonPrimaryLeafCompact. The other classes remain decodable
// development or internal raw-envelope geometries used by placement and
// structural mutation.
type CommonPrimaryLeafClass uint8

const (
	// CommonPrimaryLeafNarrow is the 4 KiB-oriented succinct class with the
	// smaller stable-slot count.
	CommonPrimaryLeafNarrow CommonPrimaryLeafClass = 1
	// CommonPrimaryLeafWide is the larger succinct class used when the same rows
	// exceed the narrow budget.
	CommonPrimaryLeafWide CommonPrimaryLeafClass = 2
)

func (class CommonPrimaryLeafClass) slots() int {
	switch class {
	case CommonPrimaryLeafNarrow:
		return CommonPrimaryLeafNarrowSlots
	case CommonPrimaryLeafWide:
		return CommonPrimaryLeafWideSlots
	default:
		return 0
	}
}

func (class CommonPrimaryLeafClass) stashSlots() int {
	if class.slots() == 0 {
		return 0
	}
	return class.slots() - CommonPrimaryLeafNormalSlots
}

// CommonPrimaryLeafBounds are the selecting state-root bounds needed
// to admit physical PageRefs embedded in overflow records.
type CommonPrimaryLeafBounds struct {
	FileEnd           uint64
	NextLogicalID     uint64
	AllocationQuantum uint32
}

// CommonPrimaryLeafHeader is a leaf's stable identity: the store it belongs to,
// the generation that sealed it, and the BucketID it holds. PageSize is carried
// here too because a leaf's physical extent is independent of its slot class.
type CommonPrimaryLeafHeader struct {
	StoreID    [16]byte
	Generation uint64
	Bucket     BucketID
	// PageSize is independent of the stable-slot class. Tiny values can use a
	// 4 KiB extent while representative inline JSON can use 32 or 64 KiB
	// without changing BucketID, slots, postings, or lookup geometry.
	PageSize uint32
}

// CommonPrimaryLeafValue has exactly one representation. Inline
// borrows caller/page memory. Overflow is the complete first-page reference.
type CommonPrimaryLeafValue struct {
	Inline   []byte
	Overflow PageRef
}

// IsOverflow reports whether the value is stored out of line, so the caller
// must follow Overflow rather than read Inline.
func (v CommonPrimaryLeafValue) IsOverflow() bool {
	return len(v.Inline) == 0 && v.Overflow.LogicalID != 0
}

// DecodePrimaryOverflowRef decodes the borrowed 32-byte out-of-line value
// descriptor a raw or posting-row scan yields for an overflow record into its
// chain-head PageRef. It returns the zero PageRef for a wrong-length input; the
// selecting snapshot bounds still gate the reference when the chain is opened.
func DecodePrimaryOverflowRef(raw []byte) PageRef {
	if len(raw) != PageRefSize {
		return PageRef{}
	}
	return decodePageRef(raw)
}

// CommonPrimaryLeafRecord is one key/value together with its stable slot. It is
// the build-and-mutate spelling: the slot is authoritative so an update or
// delete lands on the same physical slot the key already occupies. Key and
// Value.Inline are borrowed until the encoder returns.
type CommonPrimaryLeafRecord struct {
	Slot  uint8
	Key   []byte
	Value CommonPrimaryLeafValue
}

// CommonPrimaryLeafRow is one key/value pair yielded by iteration, without the
// stable slot readers do not need. Both fields borrow the page image.
type CommonPrimaryLeafRow struct {
	Key   []byte
	Value CommonPrimaryLeafValue
}

type commonPrimaryLeafLayout struct {
	controlStart     int
	normalRankStart  int
	stashBitmap      int
	stashBitmapLen   int
	stashRankStart   int
	stashTagStart    int
	stashTagLen      int
	stashHashStart   int
	stashHashLen     int
	filterStart      int
	filterLen        int
	keyLengthStart   int
	overflowStart    int
	lowStart         int
	highStart        int
	highBytes        int
	checkpointStart  int
	checkpointCount  int
	checkpointStride int
	checkpointWidth  int
	heapStart        int
}

func commonPrimaryLeafLayoutFor(
	class CommonPrimaryLeafClass, live, extent int,
) commonPrimaryLeafLayout {
	if class.slots() == 0 || live < 0 || live > class.slots() ||
		extent < CommonPrimaryLeafNarrowBytes ||
		extent > 64<<10 || extent&(extent-1) != 0 {
		return commonPrimaryLeafLayout{}
	}
	stashSlots := class.stashSlots()
	boundaries := live + 1
	layout := commonPrimaryLeafLayout{
		controlStart:     commonPrimaryLeafPayloadHeader,
		keyLengthStart:   0,
		overflowStart:    0,
		lowStart:         0,
		highBytes:        (boundaries + extent/(1<<commonPrimaryLeafLowerBits) + 7) / 8,
		checkpointStride: 16,
		checkpointWidth:  2,
	}
	if extent == CommonPrimaryLeafNarrowBytes {
		// 4 KiB offsets have at most four high bits. Adding a maximum lexical
		// rank of 217 still fits one byte, halving the select table.
		layout.checkpointWidth = 1
	}
	if class == CommonPrimaryLeafNarrow {
		layout.stashTagLen = stashSlots
		layout.filterLen = commonPrimaryLeafNarrowFilter
	} else {
		layout.stashBitmapLen = (stashSlots + 7) / 8
		layout.stashHashLen = commonPrimaryLeafWideHash
		layout.checkpointStride = 8
	}
	layout.checkpointCount = (boundaries + layout.checkpointStride - 1) /
		layout.checkpointStride
	layout.normalRankStart = layout.controlStart + CommonPrimaryLeafNormalSlots
	layout.stashBitmap = layout.normalRankStart + CommonPrimaryLeafNormalSlots
	layout.stashRankStart = layout.stashBitmap + layout.stashBitmapLen
	layout.stashTagStart = layout.stashRankStart + stashSlots
	layout.stashHashStart = layout.stashTagStart + layout.stashTagLen
	layout.filterStart = layout.stashHashStart + layout.stashHashLen
	layout.keyLengthStart = layout.filterStart + layout.filterLen
	layout.overflowStart = layout.keyLengthStart + (live*7+7)/8
	layout.lowStart = layout.overflowStart + (live+7)/8
	layout.highStart = layout.lowStart + boundaries
	layout.checkpointStart = layout.highStart + layout.highBytes
	layout.heapStart = layout.checkpointStart +
		layout.checkpointCount*layout.checkpointWidth
	return layout
}

// CommonPrimaryLeafStructuralBytes includes common framing and exact
// persistent leaf metadata, but excludes live keys, values, and physical slack.
func CommonPrimaryLeafStructuralBytes(
	class CommonPrimaryLeafClass, live, extent int,
) int {
	extra := 0
	if class == CommonPrimaryLeafCompact {
		return PageHeaderSize + compactPrimaryHeaderBytes + PageTrailerSize
	}
	if class == CommonPrimaryLeafUnified {
		// Class 5 deliberately reuses the wide slot envelope. The unified
		// section header is fixed metadata; template and dictionary bytes are
		// compressed document content and therefore are not structural bytes.
		class = CommonPrimaryLeafWide
		extra = commonPrimaryUnifiedHeaderBytes
	}
	layout := commonPrimaryLeafLayoutFor(class, live, extent)
	if layout.heapStart == 0 {
		return 0
	}
	return PageHeaderSize + layout.heapStart + extra + PageTrailerSize
}

// CommonPrimaryLeafStructuralBytesPerKey reports the leaf's fixed structural
// overhead (everything but key and value bytes) amortized over live records, a
// space-accounting probe used to compare classes and extents.
func CommonPrimaryLeafStructuralBytesPerKey(
	class CommonPrimaryLeafClass, live, extent int,
) float64 {
	if live <= 0 {
		return 0
	}
	return float64(CommonPrimaryLeafStructuralBytes(class, live, extent)) /
		float64(live)
}

// CommonPrimaryLeafLogicalID derives the collision-free leaf identity
// shared by the tablet router and secondary posting coordinates.
func CommonPrimaryLeafLogicalID(bucket BucketID) (uint64, bool) {
	if uint32(bucket) >= PrimaryBucketIDLimit {
		return 0, false
	}
	return CommonPrimaryLeafLeafLogicalIDBase + uint64(bucket), true
}

type commonPrimaryLeafPlacer struct {
	records  []CommonPrimaryLeafRecord
	seed     [16]byte
	owner    [CommonPrimaryLeafNormalSlots]int16
	assigned [CommonPrimaryLeafWideSlots]uint8
	visited  [CommonPrimaryLeafNormalSlots]uint16
	epoch    uint16
}

// PlaceCommonPrimaryLeafRecords assigns each record its stable slot for the
// given class and seed, writing the slot back into records in place. It is the
// deterministic placement step a build runs before EncodeCommonPrimaryLeaf, and
// it fails closed when the records cannot be placed within the class.
func PlaceCommonPrimaryLeafRecords(
	class CommonPrimaryLeafClass,
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
) error {
	if class.slots() == 0 || seed == ([16]byte{}) ||
		len(records) > class.slots() {
		return fmt.Errorf("%w: placement class/count", ErrInvalidWrite)
	}
	p := commonPrimaryLeafPlacer{records: records, seed: seed}
	for index := range p.owner {
		p.owner[index] = -1
	}
	nextStash := CommonPrimaryLeafNormalSlots
	for index := range records {
		if len(records[index].Key) == 0 ||
			len(records[index].Key) > CommonPrimaryLeafMaxKeyBytes {
			return fmt.Errorf("%w: placement key", ErrInvalidWrite)
		}
		p.epoch++
		if p.epoch == 0 {
			clear(p.visited[:])
			p.epoch = 1
		}
		if p.place(index) {
			continue
		}
		if nextStash >= class.slots() {
			if class == CommonPrimaryLeafNarrow {
				return ErrCommonPrimaryLeafNeedsWide
			}
			return ErrCommonPrimaryLeafFull
		}
		p.assigned[index] = uint8(nextStash)
		nextStash++
	}
	for index := range records {
		records[index].Slot = p.assigned[index]
	}
	return nil
}

func (p *commonPrimaryLeafPlacer) place(index int) bool {
	hash := commonPrimaryLeafHash(p.seed, p.records[index].Key)
	for ordinal := range commonPrimaryLeafGroupSize * 2 {
		slot := commonPrimaryLeafCandidate(hash, ordinal)
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

func commonPrimaryLeafGroups(hash uint64) (uint8, uint8) {
	first := uint8(hash) % commonPrimaryLeafGroupCount
	second := uint8(hash>>8) % commonPrimaryLeafGroupCount
	if second == first {
		second = (second + 1) % commonPrimaryLeafGroupCount
	}
	return first, second
}

func commonPrimaryLeafCandidate(hash uint64, ordinal int) uint8 {
	first, second := commonPrimaryLeafGroups(hash)
	group := first
	home := uint8(hash>>16) & (commonPrimaryLeafGroupSize - 1)
	if ordinal >= commonPrimaryLeafGroupSize {
		group = second
		home = uint8(hash>>20) & (commonPrimaryLeafGroupSize - 1)
		ordinal -= commonPrimaryLeafGroupSize
	}
	return group*commonPrimaryLeafGroupSize +
		(home+uint8(ordinal))&(commonPrimaryLeafGroupSize-1)
}

func commonPrimaryLeafNormalCandidate(hash uint64, slot uint8) bool {
	if int(slot) >= CommonPrimaryLeafNormalSlots {
		return false
	}
	first, second := commonPrimaryLeafGroups(hash)
	group := slot / commonPrimaryLeafGroupSize
	return group == first || group == second
}

func commonPrimaryLeafHash(seed [16]byte, key []byte) uint64 {
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
	commonPrimaryLeafSipFront(&v0, &v1, &v2, &v3)
	commonPrimaryLeafSipBack(&v0, &v1, &v2, &v3)
	v0 ^= message
	last := uint64(8) << 56
	v3 ^= last
	commonPrimaryLeafSipFront(&v0, &v1, &v2, &v3)
	commonPrimaryLeafSipBack(&v0, &v1, &v2, &v3)
	v0 ^= last
	v2 ^= 0xff
	for range 3 {
		commonPrimaryLeafSipFront(&v0, &v1, &v2, &v3)
		commonPrimaryLeafSipBack(&v0, &v1, &v2, &v3)
	}
	return v0 ^ v1 ^ v2 ^ v3
}

func commonPrimaryLeafSipFront(v0, v1, v2, v3 *uint64) {
	*v0 += *v1
	*v1 = bits.RotateLeft64(*v1, 13)
	*v1 ^= *v0
	*v0 = bits.RotateLeft64(*v0, 32)
	*v2 += *v3
	*v3 = bits.RotateLeft64(*v3, 16)
	*v3 ^= *v2
}

func commonPrimaryLeafSipBack(v0, v1, v2, v3 *uint64) {
	*v0 += *v3
	*v3 = bits.RotateLeft64(*v3, 21)
	*v3 ^= *v0
	*v2 += *v1
	*v1 = bits.RotateLeft64(*v1, 17)
	*v1 ^= *v2
	*v2 = bits.RotateLeft64(*v2, 32)
}

func commonPrimaryLeafFilterAdd(filter []byte, hash uint64) {
	for shift := uint(0); shift < 64; shift += 16 {
		bit := byte(hash >> shift)
		filter[int(bit)>>3] |= byte(1) << uint(bit&7)
	}
}

func commonPrimaryLeafFilterMayContain(filter []byte, hash uint64) bool {
	for shift := uint(0); shift < 64; shift += 16 {
		bit := byte(hash >> shift)
		if filter[int(bit)>>3]&(byte(1)<<uint(bit&7)) == 0 {
			return false
		}
	}
	return true
}

func commonPrimaryLeafValueBytes(value CommonPrimaryLeafValue) int {
	if len(value.Inline) != 0 {
		if value.Overflow.LogicalID != 0 {
			return 0
		}
		return len(value.Inline)
	}
	if value.Overflow.LogicalID != 0 {
		return PageRefSize
	}
	return 0
}

func commonPrimaryLeafValidateBounds(
	bounds CommonPrimaryLeafBounds,
) bool {
	if !validPhysicalPageSize(bounds.AllocationQuantum) ||
		bounds.NextLogicalID < CommonPrimaryLeafFirstDynamicLogicalID {
		return false
	}
	layout, err := MutableStoreLayout(bounds.AllocationQuantum)
	return err == nil && bounds.FileEnd >= layout.DataStart &&
		bounds.FileEnd <= maxSuperblockFileOffset &&
		bounds.FileEnd%uint64(bounds.AllocationQuantum) == 0
}

func commonPrimaryLeafValidateOverflow(
	ref PageRef,
	leafLogicalID, leafGeneration uint64,
	bounds CommonPrimaryLeafBounds,
) bool {
	if !commonPrimaryLeafValidateBounds(bounds) {
		return false
	}
	layout, _ := MutableStoreLayout(bounds.AllocationQuantum)
	length := uint64(ref.Length)
	return ref.Offset >= layout.DataStart &&
		ref.Offset%uint64(bounds.AllocationQuantum) == 0 &&
		ref.Offset <= maxSuperblockFileOffset &&
		length <= bounds.FileEnd && ref.Offset <= bounds.FileEnd-length &&
		ref.LogicalID >= CommonPrimaryLeafFirstDynamicLogicalID &&
		ref.LogicalID < bounds.NextLogicalID && ref.LogicalID != leafLogicalID &&
		ref.Generation != 0 && ref.Generation < uint64(1)<<48 &&
		ref.Generation <= leafGeneration && ref.Kind == PageOverflow &&
		validPageExtentSize(PageOverflow, ref.Length) &&
		ref.Length >= bounds.AllocationQuantum &&
		ref.Length%bounds.AllocationQuantum == 0
}

func commonPrimaryLeafValidateExpectedRef(
	ref PageRef,
	bucket BucketID,
	selectingGeneration uint64,
	bounds CommonPrimaryLeafBounds,
) bool {
	logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
	if !ok || selectingGeneration == 0 ||
		selectingGeneration >= uint64(1)<<48 ||
		!commonPrimaryLeafValidateBounds(bounds) {
		return false
	}
	return ref.Offset != 0 &&
		ref.Offset%uint64(bounds.AllocationQuantum) == 0 &&
		ref.Offset <= maxSuperblockFileOffset &&
		uint64(ref.Length) <= bounds.FileEnd &&
		ref.Offset <= bounds.FileEnd-uint64(ref.Length) &&
		ref.Length >= CommonPrimaryLeafNarrowBytes &&
		ref.Length <= CommonPrimaryLeafMaxExtentBytes &&
		validPageExtentSize(PagePrimaryLeaf, ref.Length) &&
		ref.LogicalID == logicalID &&
		ref.Generation != 0 && ref.Generation < uint64(1)<<48 &&
		ref.Generation <= selectingGeneration &&
		ref.Kind == commonPrimaryLeafPageKind
}

// EncodeCommonPrimaryLeaf writes one immutable leaf image into dst from records
// whose slots PlaceCommonPrimaryLeafRecords already assigned. It validates the
// class, geometry, and every overflow PageRef against bounds, seals the image,
// and returns the slice aliasing dst. It allocates nothing.
func EncodeCommonPrimaryLeaf(
	dst []byte,
	class CommonPrimaryLeafClass,
	header CommonPrimaryLeafHeader,
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
	bounds CommonPrimaryLeafBounds,
) ([]byte, error) {
	extent := int(header.PageSize)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(header.Bucket)
	if class.slots() == 0 || extent < CommonPrimaryLeafNarrowBytes ||
		extent > 64<<10 || !validPhysicalPageSize(header.PageSize) ||
		len(dst) < extent || header.StoreID == ([16]byte{}) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		seed == ([16]byte{}) || len(records) > class.slots() || !logicalOK ||
		!commonPrimaryLeafValidateBounds(bounds) {
		return nil, fmt.Errorf("%w: primary leaf identity/class/bounds", ErrInvalidWrite)
	}
	layout := commonPrimaryLeafLayoutFor(class, len(records), extent)
	heapEnd := layout.heapStart
	for rank := range records {
		record := &records[rank]
		valueBytes := commonPrimaryLeafValueBytes(record.Value)
		if len(record.Key) == 0 ||
			len(record.Key) > CommonPrimaryLeafMaxKeyBytes ||
			valueBytes == 0 {
			return nil, fmt.Errorf("%w: primary leaf key/value", ErrInvalidWrite)
		}
		if rank != 0 && bytes.Compare(records[rank-1].Key, record.Key) >= 0 {
			return nil, fmt.Errorf("%w: primary leaf keys not lexical", ErrInvalidWrite)
		}
		if record.Value.IsOverflow() &&
			!commonPrimaryLeafValidateOverflow(
				record.Value.Overflow, logicalID, header.Generation, bounds,
			) {
			return nil, fmt.Errorf(
				"%w: primary leaf overflow ref row=%d ref=%+v leaf_generation=%d bounds=%+v",
				ErrInvalidWrite, rank, record.Value.Overflow, header.Generation, bounds,
			)
		}
		heapEnd += len(record.Key) + valueBytes
		if len(record.Key) >= commonPrimaryLeafEscapeLength {
			heapEnd++
		}
	}
	payloadCapacity := extent - PageHeaderSize - PageTrailerSize
	if heapEnd > payloadCapacity {
		if class == CommonPrimaryLeafNarrow {
			return nil, ErrCommonPrimaryLeafNeedsWide
		}
		return nil, ErrCommonPrimaryLeafFull
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: logicalID, PageSize: uint32(extent),
		PayloadLength: uint32(heapEnd), Kind: commonPrimaryLeafPageKind,
	})
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		payload[2] = byte(class) | 0x80
	} else {
		payload[0] = byte(len(records) - 1)
		payload[2] = byte(class)
	}
	cursor := layout.heapStart
	stashCount := 0
	var occupied [4]uint64
	var wideHashes [CommonPrimaryLeafWideSlots - CommonPrimaryLeafNormalSlots]uint64
	for rank := range records {
		record := &records[rank]
		slot := int(record.Slot)
		if slot >= class.slots() {
			return nil, fmt.Errorf("%w: primary leaf slot", ErrInvalidWrite)
		}
		bit := uint64(1) << uint(slot&63)
		if occupied[slot>>6]&bit != 0 {
			return nil, fmt.Errorf("%w: duplicate primary leaf slot", ErrInvalidWrite)
		}
		occupied[slot>>6] |= bit
		hash := commonPrimaryLeafHash(seed, record.Key)
		if slot < CommonPrimaryLeafNormalSlots {
			if !commonPrimaryLeafNormalCandidate(hash, record.Slot) {
				return nil, fmt.Errorf("%w: primary leaf candidate slot", ErrInvalidWrite)
			}
			payload[layout.controlStart+slot] =
				commonPrimaryLeafControlLive | byte(hash>>57)
			payload[layout.normalRankStart+slot] = byte(rank)
		} else {
			stash := slot - CommonPrimaryLeafNormalSlots
			if class == CommonPrimaryLeafNarrow {
				payload[layout.stashRankStart+stash] = byte(rank + 1)
				payload[layout.stashTagStart+stash] = byte(hash >> 56)
				commonPrimaryLeafFilterAdd(
					payload[layout.filterStart:layout.filterStart+layout.filterLen],
					hash,
				)
			} else {
				payload[layout.stashBitmap+stash/8] |= byte(1) << uint(stash&7)
				payload[layout.stashRankStart+stash] = byte(rank)
				wideHashes[stash] = hash
			}
			stashCount++
		}
		commonPrimaryLeafPutKeyLength(
			payload, &layout, rank, len(record.Key),
		)
		if record.Value.IsOverflow() {
			payload[layout.overflowStart+rank/8] |= byte(1) << uint(rank&7)
		}
		commonPrimaryLeafPutBoundary(payload, &layout, rank, uint16(cursor))
		if len(record.Key) >= commonPrimaryLeafEscapeLength {
			payload[cursor] = byte(len(record.Key) - 1)
			cursor++
		}
		copy(payload[cursor:], record.Key)
		cursor += len(record.Key)
		if record.Value.IsOverflow() {
			encodePageRef(payload[cursor:cursor+PageRefSize], record.Value.Overflow)
			cursor += PageRefSize
		} else {
			copy(payload[cursor:], record.Value.Inline)
			cursor += len(record.Value.Inline)
		}
	}
	if class == CommonPrimaryLeafWide {
		for stash, hash := range wideHashes {
			if payload[layout.stashBitmap+stash/8]&
				(byte(1)<<uint(stash&7)) == 0 {
				continue
			}
			commonPrimaryLeafInsertWideHash(payload, layout, hash, stash)
		}
	}
	commonPrimaryLeafPutBoundary(
		payload, &layout, len(records), uint16(cursor),
	)
	commonPrimaryLeafBuildCheckpoints(payload, &layout, len(records)+1)
	payload[1] = byte(stashCount)
	page := dst[:extent]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func commonPrimaryLeafInsertWideHash(
	payload []byte, layout commonPrimaryLeafLayout, hash uint64, stash int,
) {
	home := int(hash>>32) & (commonPrimaryLeafWideHash - 1)
	for probe := range commonPrimaryLeafWideHash {
		at := layout.stashHashStart +
			(home+probe)&(commonPrimaryLeafWideHash-1)
		if payload[at] == 0 {
			payload[at] = byte(stash + 1)
			return
		}
	}
	panic("common primary leaf wide directory full")
}

func commonPrimaryLeafPutKeyLength(
	payload []byte, layout *commonPrimaryLeafLayout, rank, length int,
) {
	code := min(length, commonPrimaryLeafEscapeLength)
	bit := rank * 7
	at := layout.keyLengthStart + bit/8
	shift := uint(bit & 7)
	word := uint16(payload[at]) | uint16(code)<<shift
	if at+1 < layout.overflowStart {
		word |= uint16(payload[at+1]) << 8
	}
	payload[at] = byte(word)
	if at+1 < layout.overflowStart {
		payload[at+1] = byte(word >> 8)
	}
}

func commonPrimaryLeafKeyCode(
	payload []byte, layout *commonPrimaryLeafLayout, rank int,
) int {
	bit := rank * 7
	at := layout.keyLengthStart + bit/8
	shift := uint(bit & 7)
	word := uint16(payload[at])
	if at+1 < layout.overflowStart {
		word |= uint16(payload[at+1]) << 8
	}
	return int(word>>shift) & 0x7f
}

func commonPrimaryLeafPutBoundary(
	payload []byte, layout *commonPrimaryLeafLayout,
	index int, offset uint16,
) {
	payload[layout.lowStart+index] = byte(offset)
	position := int(offset>>commonPrimaryLeafLowerBits) + index
	payload[layout.highStart+position/8] |= byte(1) << uint(position&7)
}

func commonPrimaryLeafBuildCheckpoints(
	payload []byte, layout *commonPrimaryLeafLayout, boundaries int,
) {
	for index := 0; index < boundaries; index += layout.checkpointStride {
		position, ok := commonPrimaryLeafSelectFromStart(
			payload, layout, index,
		)
		if !ok {
			panic("common primary leaf invalid boundary bitmap")
		}
		at := layout.checkpointStart +
			(index/layout.checkpointStride)*layout.checkpointWidth
		if layout.checkpointWidth == 1 {
			if position > 255 {
				panic("common primary leaf one-byte checkpoint overflow")
			}
			payload[at] = byte(position)
		} else {
			binary.LittleEndian.PutUint16(payload[at:], uint16(position))
		}
	}
}

func commonPrimaryLeafSelectFromStart(
	payload []byte, layout *commonPrimaryLeafLayout, target int,
) (int, bool) {
	remaining := target
	for byteIndex := 0; byteIndex < layout.highBytes; byteIndex++ {
		value := payload[layout.highStart+byteIndex]
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

// CommonPrimaryLeafView is a borrowed, allocation-free read view over one
// admitted leaf image. It is produced by OpenCommonPrimaryLeaf after full
// structural validation, so every read method may trust the layout; the view
// aliases the page bytes and is valid only while that page lease is held.
type CommonPrimaryLeafView struct {
	header     CommonPrimaryLeafHeader
	class      CommonPrimaryLeafClass
	seed       [16]byte
	page       []byte
	payload    []byte
	count      uint16
	stashCount uint8
	layout     commonPrimaryLeafLayout
	bounds     CommonPrimaryLeafBounds
}

// OpenCommonPrimaryLeaf validates common framing, the selecting
// PageRef, all succinct/hash structures, lexical order, and overflow PageRefs.
// The returned view borrows src and allocates nothing on success.
func OpenCommonPrimaryLeaf(
	src []byte,
	seed [16]byte,
	bucket BucketID,
	expected PageRef,
	selectingGeneration uint64,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryLeafView, error) {
	if seed == ([16]byte{}) ||
		!commonPrimaryLeafValidateExpectedRef(
			expected, bucket, selectingGeneration, bounds,
		) {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: selector", ErrCommonPrimaryLeafCorrupt,
		)
	}
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: %w", ErrCommonPrimaryLeafCorrupt, err,
		)
	}
	if len(payload) < commonPrimaryLeafPayloadHeader {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: short payload", ErrCommonPrimaryLeafCorrupt,
		)
	}
	class := CommonPrimaryLeafClass(payload[2] & 0x7f)
	logicalID, _ := CommonPrimaryLeafLogicalID(bucket)
	if class.slots() == 0 || len(src) < int(pageHeader.PageSize) ||
		pageHeader.LogicalID != logicalID ||
		pageHeader.LogicalID != expected.LogicalID ||
		pageHeader.Generation != expected.Generation ||
		pageHeader.PageSize != expected.Length ||
		pageHeader.Kind != expected.Kind {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: common identity or payload header",
			ErrCommonPrimaryLeafCorrupt,
		)
	}
	count := 0
	if payload[2]&0x80 == 0 {
		count = int(payload[0]) + 1
	} else if payload[0] != 0 {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: empty count encoding", ErrCommonPrimaryLeafCorrupt,
		)
	}
	stashCount := int(payload[1])
	layout := commonPrimaryLeafLayoutFor(
		class, count, int(pageHeader.PageSize),
	)
	if layout.heapStart == 0 || count > class.slots() ||
		stashCount > class.stashSlots() || len(payload) < layout.heapStart {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: layout", ErrCommonPrimaryLeafCorrupt,
		)
	}
	view := CommonPrimaryLeafView{
		header: CommonPrimaryLeafHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		class: class, seed: seed,
		page:    src[:int(pageHeader.PageSize):int(pageHeader.PageSize)],
		payload: payload, count: uint16(count), stashCount: uint8(stashCount),
		layout: layout, bounds: bounds,
	}
	if err := view.validate(logicalID); err != nil {
		return CommonPrimaryLeafView{}, err
	}
	return view, nil
}

// AdmittedCommonPrimaryLeaf reconstructs a leaf whose common framing, exact
// selector, succinct metadata, hash tables, lexical rows, and overflow
// references were already fully validated by PageCache admission. Calling it
// on arbitrary bytes is invalid.
func AdmittedCommonPrimaryLeaf(
	src []byte,
	seed [16]byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
) CommonPrimaryLeafView {
	pageHeader, _ := decodePageHeader(src)
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	class := CommonPrimaryLeafClass(payload[2] & 0x7f)
	count := 0
	if payload[2]&0x80 == 0 {
		count = int(payload[0]) + 1
	}
	layout := commonPrimaryLeafLayoutFor(
		class, count, int(pageHeader.PageSize),
	)
	return CommonPrimaryLeafView{
		header: CommonPrimaryLeafHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		class: class, seed: seed,
		page:    src[:int(pageHeader.PageSize):int(pageHeader.PageSize)],
		payload: payload, count: uint16(count), stashCount: payload[1],
		layout: layout, bounds: bounds,
	}
}

func (v *CommonPrimaryLeafView) validate(logicalID uint64) error {
	boundaries := int(v.count) + 1
	stashBitmapBits := v.class.stashSlots()
	if v.class == CommonPrimaryLeafNarrow {
		stashBitmapBits = 0
	}
	ones := 0
	for index := 0; index < v.layout.highBytes; index++ {
		ones += bits.OnesCount8(v.payload[v.layout.highStart+index])
	}
	if ones != boundaries ||
		!commonPrimaryLeafUnusedBitsZero(
			v.payload[v.layout.keyLengthStart:v.layout.overflowStart],
			int(v.count)*7,
		) ||
		!commonPrimaryLeafUnusedBitsZero(
			v.payload[v.layout.overflowStart:v.layout.lowStart],
			int(v.count),
		) ||
		!commonPrimaryLeafUnusedBitsZero(
			v.payload[v.layout.stashBitmap:v.layout.stashBitmap+v.layout.stashBitmapLen],
			stashBitmapBits,
		) {
		return fmt.Errorf("%w: compact metadata", ErrCommonPrimaryLeafCorrupt)
	}
	for index := 0; index < boundaries; index += v.layout.checkpointStride {
		want, ok := commonPrimaryLeafSelectFromStart(
			v.payload, &v.layout, index,
		)
		at := v.layout.checkpointStart +
			(index/v.layout.checkpointStride)*v.layout.checkpointWidth
		got := int(v.payload[at])
		if v.layout.checkpointWidth == 2 {
			got = int(binary.LittleEndian.Uint16(v.payload[at:]))
		}
		if !ok || got != want {
			return fmt.Errorf("%w: checkpoint", ErrCommonPrimaryLeafCorrupt)
		}
	}
	var seen [4]uint64
	var wantFilter [commonPrimaryLeafNarrowFilter]byte
	var wantWide [commonPrimaryLeafWideHash]byte
	live, stash := 0, 0
	for slot := range CommonPrimaryLeafNormalSlots {
		control := v.payload[v.layout.controlStart+slot]
		rank := v.payload[v.layout.normalRankStart+slot]
		if control == 0 {
			if rank != 0 {
				return fmt.Errorf("%w: empty normal rank", ErrCommonPrimaryLeafCorrupt)
			}
			continue
		}
		if control&commonPrimaryLeafControlLive == 0 ||
			int(rank) >= int(v.count) ||
			!v.validateSlotRank(uint8(slot), int(rank), control, &seen) {
			return fmt.Errorf("%w: normal slot", ErrCommonPrimaryLeafCorrupt)
		}
		live++
	}
	for index := 0; index < v.class.stashSlots(); index++ {
		rankCode := v.payload[v.layout.stashRankStart+index]
		isLive := rankCode != 0
		rank := int(rankCode) - 1
		if v.class == CommonPrimaryLeafWide {
			isLive = v.payload[v.layout.stashBitmap+index/8]&
				(byte(1)<<uint(index&7)) != 0
			rank = int(rankCode)
		}
		if !isLive {
			if rankCode != 0 ||
				v.class == CommonPrimaryLeafNarrow &&
					v.payload[v.layout.stashTagStart+index] != 0 {
				return fmt.Errorf("%w: empty stash", ErrCommonPrimaryLeafCorrupt)
			}
			continue
		}
		if rank >= int(v.count) ||
			!v.validateSlotRank(
				uint8(CommonPrimaryLeafNormalSlots+index),
				rank, 0, &seen,
			) {
			return fmt.Errorf("%w: stash slot", ErrCommonPrimaryLeafCorrupt)
		}
		key, _, ok := v.keyAt(rank)
		if !ok {
			return fmt.Errorf("%w: stash key", ErrCommonPrimaryLeafCorrupt)
		}
		hash := commonPrimaryLeafHash(v.seed, key)
		if v.class == CommonPrimaryLeafNarrow {
			if v.payload[v.layout.stashTagStart+index] != byte(hash>>56) {
				return fmt.Errorf("%w: stash tag", ErrCommonPrimaryLeafCorrupt)
			}
			commonPrimaryLeafFilterAdd(wantFilter[:], hash)
		} else {
			home := int(hash>>32) & (commonPrimaryLeafWideHash - 1)
			for probe := range commonPrimaryLeafWideHash {
				at := (home + probe) & (commonPrimaryLeafWideHash - 1)
				if wantWide[at] == 0 {
					wantWide[at] = byte(index + 1)
					break
				}
			}
		}
		stash++
		live++
	}
	if live != int(v.count) || stash != int(v.stashCount) {
		return fmt.Errorf("%w: live counts", ErrCommonPrimaryLeafCorrupt)
	}
	if v.class == CommonPrimaryLeafNarrow &&
		!bytes.Equal(
			wantFilter[:],
			v.payload[v.layout.filterStart:v.layout.filterStart+v.layout.filterLen],
		) {
		return fmt.Errorf("%w: stash filter", ErrCommonPrimaryLeafCorrupt)
	}
	if v.class == CommonPrimaryLeafWide &&
		!bytes.Equal(
			wantWide[:],
			v.payload[v.layout.stashHashStart:v.layout.stashHashStart+v.layout.stashHashLen],
		) {
		return fmt.Errorf("%w: wide stash directory", ErrCommonPrimaryLeafCorrupt)
	}
	var previousKey []byte
	for rank := 0; rank < int(v.count); rank++ {
		if seen[rank>>6]&(uint64(1)<<uint(rank&63)) == 0 {
			return fmt.Errorf("%w: missing rank", ErrCommonPrimaryLeafCorrupt)
		}
		key, valueStart, end, ok := v.keyBounds(rank)
		if !ok || valueStart >= end {
			return fmt.Errorf("%w: lexical record", ErrCommonPrimaryLeafCorrupt)
		}
		if rank != 0 && bytes.Compare(previousKey, key) >= 0 {
			return fmt.Errorf("%w: lexical order", ErrCommonPrimaryLeafCorrupt)
		}
		previousKey = key
		if v.rankOverflow(rank) {
			if end-valueStart != PageRefSize ||
				!pageRefReservedZero(v.payload[valueStart:end]) ||
				!commonPrimaryLeafValidateOverflow(
					decodePageRef(v.payload[valueStart:end]),
					logicalID, v.header.Generation, v.bounds,
				) {
				return fmt.Errorf("%w: overflow descriptor", ErrCommonPrimaryLeafCorrupt)
			}
		}
	}
	first, firstOK := v.boundary(0)
	last, lastOK := v.boundary(int(v.count))
	if !firstOK || int(first) != v.layout.heapStart ||
		!lastOK || int(last) != len(v.payload) {
		return fmt.Errorf("%w: terminal boundaries", ErrCommonPrimaryLeafCorrupt)
	}
	return nil
}

func commonPrimaryLeafUnusedBitsZero(encoded []byte, usedBits int) bool {
	if len(encoded) == 0 {
		return usedBits == 0
	}
	if usedBits&7 == 0 {
		return true
	}
	return encoded[len(encoded)-1]&^byte((1<<uint(usedBits&7))-1) == 0
}

func commonPrimaryLeafPackedByteAt(src []byte, bit int) byte {
	at := bit >> 3
	if at >= len(src) {
		return 0
	}
	value := uint16(src[at])
	if at+1 < len(src) {
		value |= uint16(src[at+1]) << 8
	}
	return byte(value >> uint(bit&7))
}

func commonPrimaryLeafDeletePackedField(
	dst, src []byte, oldBits, start, width int,
) {
	newBits := oldBits - width
	whole := start >> 3
	copy(dst[:whole], src[:whole])
	out := whole
	if keep := start & 7; keep != 0 {
		mask := byte(1<<uint(keep)) - 1
		dst[out] = src[out]&mask |
			commonPrimaryLeafPackedByteAt(src, start+width)<<uint(keep)
		out++
	}
	for ; out < len(dst); out++ {
		dst[out] = commonPrimaryLeafPackedByteAt(
			src, out*8+width,
		)
	}
	if used := newBits & 7; used != 0 {
		dst[len(dst)-1] &= byte(1<<uint(used)) - 1
	}
}

func commonPrimaryLeafInsertPackedField(
	dst, src []byte, oldBits, start, width int, field uint16,
) {
	newBits := oldBits + width
	whole := start >> 3
	copy(dst[:whole], src[:whole])
	if keep := start & 7; keep != 0 {
		dst[whole] = src[whole] & (byte(1<<uint(keep)) - 1)
	}
	at := start >> 3
	shift := uint(start & 7)
	word := uint16(dst[at]) | field<<shift
	dst[at] = byte(word)
	if at+1 < len(dst) {
		dst[at+1] = byte(word >> 8)
	}
	suffix := start + width
	out := suffix >> 3
	if keep := suffix & 7; keep != 0 {
		mask := byte(1<<uint(keep)) - 1
		dst[out] = dst[out]&mask |
			commonPrimaryLeafPackedByteAt(src, start)<<uint(keep)
		out++
	}
	for ; out < len(dst); out++ {
		dst[out] = commonPrimaryLeafPackedByteAt(
			src, out*8-width,
		)
	}
	if used := newBits & 7; used != 0 {
		dst[len(dst)-1] &= byte(1<<uint(used)) - 1
	}
}

func (v *CommonPrimaryLeafView) validateSlotRank(
	slot uint8, rank int, control byte, seen *[4]uint64,
) bool {
	bit := uint64(1) << uint(rank&63)
	if seen[rank>>6]&bit != 0 {
		return false
	}
	key, _, ok := v.keyAt(rank)
	if !ok {
		return false
	}
	if int(slot) < CommonPrimaryLeafNormalSlots {
		hash := commonPrimaryLeafHash(v.seed, key)
		if control&commonPrimaryLeafControlTag != byte(hash>>57) ||
			!commonPrimaryLeafNormalCandidate(hash, slot) {
			return false
		}
	}
	seen[rank>>6] |= bit
	return true
}

func commonPrimaryLeafNextHighBitAt(
	payload []byte, highStart, highBytes, position int,
) (int, bool) {
	if position < 0 || position >= highBytes*8 {
		return 0, false
	}
	byteIndex := position / 8
	value := payload[highStart+byteIndex] &^
		(byte(1)<<uint(position&7) - 1)
	for {
		if value != 0 {
			return byteIndex*8 + bits.TrailingZeros8(value), true
		}
		byteIndex++
		if byteIndex >= highBytes {
			return 0, false
		}
		value = payload[highStart+byteIndex]
	}
}

func (v *CommonPrimaryLeafView) boundary(index int) (uint16, bool) {
	if index < 0 || index > int(v.count) {
		return 0, false
	}
	checkpoint := index / v.layout.checkpointStride
	baseIndex := checkpoint * v.layout.checkpointStride
	at := v.layout.checkpointStart + checkpoint*v.layout.checkpointWidth
	position := int(v.payload[at])
	if v.layout.checkpointWidth == 2 {
		position = int(binary.LittleEndian.Uint16(v.payload[at:]))
	}
	if position < 0 || position >= v.layout.highBytes*8 ||
		v.payload[v.layout.highStart+position/8]&
			(byte(1)<<uint(position&7)) == 0 {
		return 0, false
	}
	for current := baseIndex; current < index; current++ {
		next, ok := commonPrimaryLeafNextHighBitAt(
			v.payload, v.layout.highStart, v.layout.highBytes, position+1,
		)
		if !ok {
			return 0, false
		}
		position = next
	}
	high := position - index
	if high < 0 ||
		high >= len(v.page)>>commonPrimaryLeafLowerBits {
		return 0, false
	}
	return uint16(high<<commonPrimaryLeafLowerBits) |
		uint16(v.payload[v.layout.lowStart+index]), true
}

func (v *CommonPrimaryLeafView) recordBounds(
	rank int,
) (int, int, bool) {
	if rank < 0 || rank >= int(v.count) {
		return 0, 0, false
	}
	start, ok := v.boundary(rank)
	if !ok {
		return 0, 0, false
	}
	position := int(start>>commonPrimaryLeafLowerBits) + rank
	nextPosition, ok := commonPrimaryLeafNextHighBitAt(
		v.payload, v.layout.highStart, v.layout.highBytes, position+1,
	)
	if !ok {
		return 0, 0, false
	}
	nextRank := rank + 1
	end := uint16(
		(nextPosition-nextRank)<<commonPrimaryLeafLowerBits,
	) | uint16(v.payload[v.layout.lowStart+nextRank])
	return int(start), int(end), end > start
}

func (v *CommonPrimaryLeafView) keyBounds(
	rank int,
) (key []byte, valueStart, end int, ok bool) {
	start, end, ok := v.recordBounds(rank)
	if !ok {
		return nil, 0, 0, false
	}
	length := commonPrimaryLeafKeyCode(v.payload, &v.layout, rank)
	if length == 0 {
		return nil, 0, 0, false
	}
	if length == commonPrimaryLeafEscapeLength {
		if start >= end {
			return nil, 0, 0, false
		}
		length = int(v.payload[start]) + 1
		if length < commonPrimaryLeafEscapeLength ||
			length > CommonPrimaryLeafMaxKeyBytes {
			return nil, 0, 0, false
		}
		start++
	}
	if start+length >= end {
		return nil, 0, 0, false
	}
	return v.payload[start : start+length : start+length],
		start + length, end, true
}

func (v *CommonPrimaryLeafView) keyAt(
	rank int,
) (key []byte, valueStart int, ok bool) {
	key, valueStart, _, ok = v.keyBounds(rank)
	return key, valueStart, ok
}

func (v *CommonPrimaryLeafView) rankOverflow(rank int) bool {
	return v.payload[v.layout.overflowStart+rank/8]&
		(byte(1)<<uint(rank&7)) != 0
}

// HasOverflowRows reports whether any live row stores its value out of line. It
// reads only the overflow bitmap, so the checkpoint fast path can skip the
// per-row rewrite scan for the common all-inline leaf without opening a value.
func (v *CommonPrimaryLeafView) HasOverflowRows() bool {
	if v == nil || v.count == 0 {
		return false
	}
	for _, b := range v.payload[v.layout.overflowStart:v.layout.lowStart] {
		if b != 0 {
			return true
		}
	}
	return false
}

// RewriteOverflowRefs rewrites, in place, every out-of-line value descriptor in
// payload whose head ref resolve maps to a different PageRef. payload must be the
// payload region of a byte-for-byte copy of this view's raw-class leaf page: the
// checkpoint materialize copies the volatile leaf verbatim into a durable page,
// then calls this to swap each volatile chain head for the durable head it minted
// in the checkpoint transaction. Row geometry is taken from the view (identical
// to payload at call time); resolve receives the current head and returns the ref
// to store, returning the same ref to leave a row untouched so a durable head
// carried forward from a prior checkpoint costs only the resolve call. The caller
// reseals the page. It allocates nothing.
func (v *CommonPrimaryLeafView) RewriteOverflowRefs(
	payload []byte,
	resolve func(head PageRef) (PageRef, error),
) error {
	if v == nil {
		return nil
	}
	if len(payload) != len(v.payload) {
		return fmt.Errorf(
			"%w: overflow rewrite payload width", ErrCommonPrimaryLeafCorrupt,
		)
	}
	for rank := 0; rank < int(v.count); rank++ {
		if !v.rankOverflow(rank) {
			continue
		}
		_, valueStart, end, ok := v.keyBounds(rank)
		if !ok || end-valueStart != PageRefSize {
			return fmt.Errorf(
				"%w: overflow descriptor", ErrCommonPrimaryLeafCorrupt,
			)
		}
		head := decodePageRef(payload[valueStart:end])
		next, err := resolve(head)
		if err != nil {
			return err
		}
		if next != head {
			encodePageRef(payload[valueStart:end], next)
		}
	}
	return nil
}

// Header returns the leaf's stable identity; the zero value for a nil view.
func (v *CommonPrimaryLeafView) Header() CommonPrimaryLeafHeader {
	if v == nil {
		return CommonPrimaryLeafHeader{}
	}
	return v.header
}

// Len returns the number of live records in the leaf.
func (v *CommonPrimaryLeafView) Len() int {
	if v == nil {
		return 0
	}
	return int(v.count)
}

// PersistentBytes returns the borrowed on-disk image. It aliases the admitted
// page and must not be mutated or retained past the page's lease.
func (v *CommonPrimaryLeafView) PersistentBytes() []byte {
	if v == nil {
		return nil
	}
	return v.page
}

// Lookup returns the slot and value for key, hashing the key once. The returned
// value borrows the page image and is valid only while the lease is held.
func (v *CommonPrimaryLeafView) Lookup(
	key []byte,
) (uint8, CommonPrimaryLeafValue, bool) {
	if v == nil || len(key) == 0 ||
		len(key) > CommonPrimaryLeafMaxKeyBytes {
		return 0, CommonPrimaryLeafValue{}, false
	}
	return v.LookupHashed(commonPrimaryLeafHash(v.seed, key), key)
}

// LookupRaw is Lookup that returns borrowed bytes rather than a decoded value:
// raw is inline bytes or the encoded overflow descriptor, and overflow says
// which. It hashes the key once and lets the caller avoid decoding on a miss.
func (v *CommonPrimaryLeafView) LookupRaw(
	key []byte,
) (slot uint8, raw []byte, overflow, ok bool) {
	if v == nil || len(key) == 0 ||
		len(key) > CommonPrimaryLeafMaxKeyBytes {
		return 0, nil, false, false
	}
	return v.LookupRawHashed(commonPrimaryLeafHash(v.seed, key), key)
}

// LookupHashed is Lookup for a caller that already holds the key's hash and so
// avoids rehashing it.
func (v *CommonPrimaryLeafView) LookupHashed(
	hash uint64, key []byte,
) (uint8, CommonPrimaryLeafValue, bool) {
	slot, raw, overflow, ok := v.LookupRawHashed(hash, key)
	if !ok {
		return 0, CommonPrimaryLeafValue{}, false
	}
	if overflow {
		return slot, CommonPrimaryLeafValue{
			Overflow: decodePageRef(raw),
		}, true
	}
	return slot, CommonPrimaryLeafValue{Inline: raw}, true
}

// LookupRawHashed is the store hot path. Inline bytes and the encoded 32-byte
// overflow descriptor both borrow the admitted page; overflow tells the caller
// whether decoding the descriptor is necessary.
func (v *CommonPrimaryLeafView) LookupRawHashed(
	hash uint64, key []byte,
) (slot uint8, raw []byte, overflow, ok bool) {
	if v == nil || v.count == 0 || len(key) == 0 ||
		len(key) > CommonPrimaryLeafMaxKeyBytes {
		return 0, nil, false, false
	}
	want := commonPrimaryLeafControlLive | byte(hash>>57)
	first, second := commonPrimaryLeafGroups(hash)
	firstHome := uint8(hash>>16) & (commonPrimaryLeafGroupSize - 1)
	secondHome := uint8(hash>>20) & (commonPrimaryLeafGroupSize - 1)
	for groupIndex := range 2 {
		group, home := first, firstHome
		if groupIndex != 0 {
			group, home = second, secondHome
		}
		base := group * commonPrimaryLeafGroupSize
		for ordinal := range uint8(commonPrimaryLeafGroupSize) {
			slot := base + (home+ordinal)&
				(commonPrimaryLeafGroupSize-1)
			if v.payload[v.layout.controlStart+int(slot)] != want {
				continue
			}
			rank := int(v.payload[v.layout.normalRankStart+int(slot)])
			if raw, overflow, ok := v.lookupRankRaw(rank, key); ok {
				return slot, raw, overflow, true
			}
		}
	}
	if v.class == CommonPrimaryLeafWide {
		home := int(hash>>32) & (commonPrimaryLeafWideHash - 1)
		for probe := range commonPrimaryLeafWideHash {
			entry := v.payload[v.layout.stashHashStart+
				(home+probe)&(commonPrimaryLeafWideHash-1)]
			if entry == 0 {
				break
			}
			stash := int(entry) - 1
			rank := int(v.payload[v.layout.stashRankStart+stash])
			if raw, overflow, ok := v.lookupRankRaw(rank, key); ok {
				return uint8(CommonPrimaryLeafNormalSlots + stash),
					raw, overflow, true
			}
		}
		return 0, nil, false, false
	}
	if v.stashCount == 0 ||
		!commonPrimaryLeafFilterMayContain(
			v.payload[v.layout.filterStart:v.layout.filterStart+v.layout.filterLen],
			hash,
		) {
		return 0, nil, false, false
	}
	mask := v.stashMask()
	tag := byte(hash >> 56)
	for mask != 0 {
		stash := bits.TrailingZeros64(mask)
		if v.payload[v.layout.stashTagStart+stash] == tag {
			rank := int(v.payload[v.layout.stashRankStart+stash]) - 1
			if raw, overflow, ok := v.lookupRankRaw(rank, key); ok {
				return uint8(CommonPrimaryLeafNormalSlots + stash),
					raw, overflow, true
			}
		}
		mask &= mask - 1
	}
	return 0, nil, false, false
}

func (v *CommonPrimaryLeafView) lookupRank(
	rank int, key []byte,
) (CommonPrimaryLeafValue, bool) {
	raw, overflow, ok := v.lookupRankRaw(rank, key)
	if !ok {
		return CommonPrimaryLeafValue{}, false
	}
	if overflow {
		return CommonPrimaryLeafValue{Overflow: decodePageRef(raw)}, true
	}
	return CommonPrimaryLeafValue{Inline: raw}, true
}

func (v *CommonPrimaryLeafView) lookupRankRaw(
	rank int, key []byte,
) ([]byte, bool, bool) {
	got, valueStart, end, ok := v.keyBounds(rank)
	if !ok || !bytes.Equal(got, key) {
		return nil, false, false
	}
	return v.payload[valueStart:end:end], v.rankOverflow(rank), true
}

func (v *CommonPrimaryLeafView) stashMask() uint64 {
	if v.class == CommonPrimaryLeafNarrow {
		var mask uint64
		for stash := 0; stash < v.class.stashSlots(); stash++ {
			if v.payload[v.layout.stashRankStart+stash] != 0 {
				mask |= uint64(1) << uint(stash)
			}
		}
		return mask
	}
	if v.layout.stashBitmapLen == 4 {
		return uint64(binary.LittleEndian.Uint32(
			v.payload[v.layout.stashBitmap:],
		))
	}
	return binary.LittleEndian.Uint64(v.payload[v.layout.stashBitmap:])
}

func (v *CommonPrimaryLeafView) slotRank(slot uint8) (int, bool) {
	if v == nil || int(slot) >= v.class.slots() {
		return 0, false
	}
	if int(slot) < CommonPrimaryLeafNormalSlots {
		if v.payload[v.layout.controlStart+int(slot)] == 0 {
			return 0, false
		}
		return int(v.payload[v.layout.normalRankStart+int(slot)]), true
	}
	stash := int(slot) - CommonPrimaryLeafNormalSlots
	if v.class == CommonPrimaryLeafNarrow {
		rank := v.payload[v.layout.stashRankStart+stash]
		if rank == 0 {
			return 0, false
		}
		return int(rank) - 1, true
	}
	if v.payload[v.layout.stashBitmap+stash/8]&
		(byte(1)<<uint(stash&7)) == 0 {
		return 0, false
	}
	return int(v.payload[v.layout.stashRankStart+stash]), true
}

// LookupSlot returns the value at a known stable slot, verifying key still
// occupies it. It is the slot-addressed read used once a caller already holds
// the slot from a prior lookup or a locator.
func (v *CommonPrimaryLeafView) LookupSlot(
	slot uint8, key []byte,
) (CommonPrimaryLeafValue, bool) {
	rank, ok := v.slotRank(slot)
	if !ok {
		return CommonPrimaryLeafValue{}, false
	}
	return v.lookupRank(rank, key)
}

// LowerBound returns the lexical rank of the first key greater than or equal to
// key, in [0, Len]. It is the entry point for ordered range and prefix scans.
func (v *CommonPrimaryLeafView) LowerBound(key []byte) int {
	if v == nil {
		return 0
	}
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		got, _, _ := v.keyAt(middle)
		if bytes.Compare(got, key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// CommonPrimaryLeafIterator is an allocation-free cursor over a leaf's rows in
// ascending key order. It decodes each key and value lazily against the borrowed
// page image; results are valid only until the next advance or page release.
type CommonPrimaryLeafIterator struct {
	payload  []byte
	layout   commonPrimaryLeafLayout
	upper    []byte
	prefix   []byte
	count    uint16
	rank     uint16
	bitPos   uint16
	offset   uint16
	finished bool
}

// AllRows returns an iterator over every row in ascending key order.
func (v *CommonPrimaryLeafView) AllRows() CommonPrimaryLeafIterator {
	return v.iteratorAt(0, nil, nil)
}

// Range returns an iterator over rows with key in [lower, upper). A nil upper
// runs to the end of the leaf.
func (v *CommonPrimaryLeafView) Range(
	lower, upper []byte,
) CommonPrimaryLeafIterator {
	return v.iteratorAt(v.LowerBound(lower), upper, nil)
}

func (v *CommonPrimaryLeafView) iteratorAt(
	rank int, upper, prefix []byte,
) CommonPrimaryLeafIterator {
	it := CommonPrimaryLeafIterator{
		upper: upper, prefix: prefix, rank: uint16(rank),
	}
	if v == nil || rank >= int(v.count) {
		it.finished = true
		return it
	}
	offset, ok := v.boundary(rank)
	if !ok {
		it.finished = true
		return it
	}
	it.payload = v.payload
	it.layout = v.layout
	it.count = v.count
	it.offset = offset
	it.bitPos = uint16(int(offset>>commonPrimaryLeafLowerBits) + rank)
	return it
}

// Next returns the next row, or false when the iterator is exhausted. The row's
// Key and inline bytes borrow the page image and are valid only until the next
// call or page release.
func (it *CommonPrimaryLeafIterator) Next() (
	CommonPrimaryLeafRow, bool,
) {
	key, inline, overflow, ok := it.NextBorrowed()
	return CommonPrimaryLeafRow{
		Key: key,
		Value: CommonPrimaryLeafValue{
			Inline: inline, Overflow: overflow,
		},
	}, ok
}

// NextBorrowed is the scan hot path. It avoids constructing the larger tagged
// value wrapper when callers consume inline JSON directly.
func (it *CommonPrimaryLeafIterator) NextBorrowed() (
	key, inline []byte, overflow PageRef, ok bool,
) {
	key, raw, isOverflow, ok := it.NextRawBorrowed()
	if !ok {
		return nil, nil, PageRef{}, false
	}
	if isOverflow {
		return key, nil, decodePageRef(raw), true
	}
	return key, raw, PageRef{}, true
}

// NextRawBorrowed is the lowest-level scan step: it yields the next key and its
// raw bytes (inline JSON or the encoded overflow descriptor, distinguished by
// overflow) without decoding, so a scan that only needs inline values never
// constructs a value wrapper. All slices borrow the page image.
func (it *CommonPrimaryLeafIterator) NextRawBorrowed() (
	key, raw []byte, overflow, ok bool,
) {
	if it == nil || it.finished || it.rank >= it.count {
		return nil, nil, false, false
	}
	rank := int(it.rank)
	nextPosition, ok := commonPrimaryLeafNextHighBitAt(
		it.payload, it.layout.highStart, it.layout.highBytes,
		int(it.bitPos)+1,
	)
	if !ok {
		it.finished = true
		return nil, nil, false, false
	}
	nextRank := rank + 1
	end := uint16(
		(nextPosition-nextRank)<<commonPrimaryLeafLowerBits,
	) | uint16(it.payload[it.layout.lowStart+nextRank])
	keyLength := commonPrimaryLeafKeyCode(
		it.payload, &it.layout, rank,
	)
	start := int(it.offset)
	if keyLength == commonPrimaryLeafEscapeLength {
		keyLength = int(it.payload[start]) + 1
		start++
	}
	key = it.payload[start : start+keyLength : start+keyLength]
	if len(it.upper) != 0 && bytes.Compare(key, it.upper) >= 0 ||
		len(it.prefix) != 0 && !bytes.HasPrefix(key, it.prefix) {
		it.finished = true
		return nil, nil, false, false
	}
	valueStart := start + keyLength
	if it.payload[it.layout.overflowStart+rank/8]&
		(byte(1)<<uint(rank&7)) != 0 {
		overflow = true
	}
	raw = it.payload[valueStart:int(end):int(end)]
	it.rank++
	it.bitPos = uint16(nextPosition)
	it.offset = end
	if it.rank >= it.count {
		it.finished = true
	}
	return key, raw, overflow, true
}

func (v *CommonPrimaryLeafView) rankSlots() (
	[CommonPrimaryLeafWideSlots]uint8, bool,
) {
	var slots [CommonPrimaryLeafWideSlots]uint8
	var seen [4]uint64
	for slot := range CommonPrimaryLeafNormalSlots {
		if v.payload[v.layout.controlStart+slot] == 0 {
			continue
		}
		rank := v.payload[v.layout.normalRankStart+slot]
		bit := uint64(1) << uint(rank&63)
		if int(rank) >= int(v.count) || seen[rank>>6]&bit != 0 {
			return slots, false
		}
		seen[rank>>6] |= bit
		slots[rank] = uint8(slot)
	}
	mask := v.stashMask()
	for mask != 0 {
		stash := bits.TrailingZeros64(mask)
		rank := v.payload[v.layout.stashRankStart+stash]
		if v.class == CommonPrimaryLeafNarrow {
			rank--
		}
		bit := uint64(1) << uint(rank&63)
		if int(rank) >= int(v.count) || seen[rank>>6]&bit != 0 {
			return slots, false
		}
		seen[rank>>6] |= bit
		slots[rank] = uint8(CommonPrimaryLeafNormalSlots + stash)
		mask &= mask - 1
	}
	for rank := 0; rank < int(v.count); rank++ {
		if seen[rank>>6]&(uint64(1)<<uint(rank&63)) == 0 {
			return slots, false
		}
	}
	return slots, true
}

func (v *CommonPrimaryLeafView) copyRecords(
	dst []CommonPrimaryLeafRecord, skipRank int,
) (int, bool) {
	slots, ok := v.rankSlots()
	if !ok || len(dst) < int(v.count) {
		return 0, false
	}
	it := v.AllRows()
	out := 0
	for rank := 0; rank < int(v.count); rank++ {
		key, raw, overflow, valid := it.NextRawBorrowed()
		if !valid {
			return 0, false
		}
		if rank == skipRank {
			continue
		}
		value := CommonPrimaryLeafValue{}
		if overflow {
			value.Overflow = decodePageRef(raw)
		} else {
			value.Inline = raw
		}
		dst[out] = CommonPrimaryLeafRecord{
			Slot: slots[rank], Key: key, Value: value,
		}
		out++
	}
	return out, true
}

func commonPrimaryLeafOverlaps(dst, src []byte) bool {
	if len(dst) == 0 || len(src) == 0 {
		return false
	}
	dstStart := uintptr(unsafe.Pointer(&dst[0]))
	dstEnd := dstStart + uintptr(len(dst))
	srcStart := uintptr(unsafe.Pointer(&src[0]))
	srcEnd := srcStart + uintptr(len(src))
	return dstStart < srcEnd && srcStart < dstEnd
}

// UpdateTo writes a new copy-on-write leaf image into dst that replaces the
// value at (slot, key) with value under a strictly newer generation. It never
// mutates the source page and rejects a dst that aliases or is smaller than it,
// so a concurrent reader of the old generation is never disturbed.
func (v *CommonPrimaryLeafView) UpdateTo(
	dst []byte,
	generation uint64,
	slot uint8,
	key []byte,
	value CommonPrimaryLeafValue,
) ([]byte, error) {
	if v == nil || generation <= v.header.Generation ||
		generation >= uint64(1)<<48 ||
		commonPrimaryLeafValueBytes(value) == 0 ||
		len(dst) < len(v.page) ||
		commonPrimaryLeafOverlaps(dst, v.page) {
		return nil, fmt.Errorf("%w: primary leaf update", ErrInvalidWrite)
	}
	rank, ok := v.slotRank(slot)
	if !ok {
		return nil, ErrCommonPrimaryLeafNotFound
	}
	old, ok := v.LookupSlot(slot, key)
	if !ok {
		return nil, ErrCommonPrimaryLeafNotFound
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(v.header.Bucket)
	if value.IsOverflow() &&
		!commonPrimaryLeafValidateOverflow(
			value.Overflow, logicalID, generation, v.bounds,
		) {
		return nil, fmt.Errorf("%w: primary leaf update overflow", ErrInvalidWrite)
	}
	if commonPrimaryLeafValueBytes(old) ==
		commonPrimaryLeafValueBytes(value) {
		page := dst[:len(v.page)]
		copy(page, v.page)
		binary.LittleEndian.PutUint64(page[24:32], generation)
		payload := page[PageHeaderSize : PageHeaderSize+len(v.payload)]
		_, valueStart, _ := v.keyAt(rank)
		_, end, _ := v.recordBounds(rank)
		if value.IsOverflow() {
			encodePageRef(payload[valueStart:end], value.Overflow)
			payload[v.layout.overflowStart+rank/8] |= byte(1) << uint(rank&7)
		} else {
			copy(payload[valueStart:end], value.Inline)
			payload[v.layout.overflowStart+rank/8] &^= byte(1) << uint(rank&7)
		}
		if _, err := sealPage(page, false); err != nil {
			return nil, err
		}
		return page, nil
	}
	var records [CommonPrimaryLeafWideSlots]CommonPrimaryLeafRecord
	count, valid := v.copyRecords(records[:], -1)
	if !valid {
		return nil, ErrCommonPrimaryLeafCorrupt
	}
	records[rank].Value = value
	return EncodeCommonPrimaryLeaf(
		dst, v.class,
		CommonPrimaryLeafHeader{
			StoreID: v.header.StoreID, Generation: generation,
			Bucket: v.header.Bucket, PageSize: uint32(len(v.page)),
		},
		v.seed, records[:count], v.bounds,
	)
}

// DeleteTo writes a new copy-on-write leaf image into dst with (slot, key)
// removed under a strictly newer generation, leaving no tombstone. Like UpdateTo
// it never touches the source page and rejects an aliasing or too-small dst.
func (v *CommonPrimaryLeafView) DeleteTo(
	dst []byte, generation uint64, slot uint8, key []byte,
) ([]byte, error) {
	if v == nil || generation <= v.header.Generation ||
		generation >= uint64(1)<<48 || len(dst) < len(v.page) ||
		commonPrimaryLeafOverlaps(dst, v.page) {
		return nil, fmt.Errorf("%w: primary leaf delete", ErrInvalidWrite)
	}
	rank, ok := v.slotRank(slot)
	if !ok {
		return nil, ErrCommonPrimaryLeafNotFound
	}
	recordStart, recordEnd, ok := v.recordBounds(rank)
	if !ok {
		return nil, ErrCommonPrimaryLeafCorrupt
	}
	keyStart := recordStart
	keyLength := commonPrimaryLeafKeyCode(
		v.payload, &v.layout, rank,
	)
	if keyLength == commonPrimaryLeafEscapeLength {
		if keyStart >= recordEnd {
			return nil, ErrCommonPrimaryLeafCorrupt
		}
		keyLength = int(v.payload[keyStart]) + 1
		keyStart++
	}
	if keyLength != len(key) || keyStart+keyLength >= recordEnd ||
		!bytes.Equal(v.payload[keyStart:keyStart+keyLength], key) {
		return nil, ErrCommonPrimaryLeafNotFound
	}
	count := int(v.count) - 1
	layout := commonPrimaryLeafLayoutFor(
		v.class, count, len(v.page),
	)
	removed := recordEnd - recordStart
	heapBytes := len(v.payload) - v.layout.heapStart - removed
	payloadLength := layout.heapStart + heapBytes
	logicalID, _ := CommonPrimaryLeafLogicalID(v.header.Bucket)
	payload, err := InitPage(dst, PageHeader{
		StoreID: v.header.StoreID, Generation: generation,
		LogicalID: logicalID, PageSize: uint32(len(v.page)),
		PayloadLength: uint32(payloadLength),
		Kind:          commonPrimaryLeafPageKind,
	})
	if err != nil {
		return nil, err
	}
	copy(
		payload[:layout.keyLengthStart],
		v.payload[:v.layout.keyLengthStart],
	)
	if count == 0 {
		payload[0] = 0
		payload[2] = byte(v.class) | 0x80
	} else {
		payload[0] = byte(count - 1)
		payload[2] = byte(v.class)
	}

	for normal := range CommonPrimaryLeafNormalSlots {
		if v.payload[v.layout.controlStart+normal] == 0 {
			continue
		}
		oldRank := int(v.payload[v.layout.normalRankStart+normal])
		if normal == int(slot) {
			payload[layout.controlStart+normal] = 0
			payload[layout.normalRankStart+normal] = 0
		} else if oldRank > rank {
			payload[layout.normalRankStart+normal] = byte(oldRank - 1)
		}
	}
	for stash := 0; stash < v.class.stashSlots(); stash++ {
		oldRank, live := v.stashRankAt(stash)
		if !live {
			continue
		}
		stableSlot := CommonPrimaryLeafNormalSlots + stash
		if stableSlot == int(slot) {
			if v.class == CommonPrimaryLeafNarrow {
				payload[layout.stashRankStart+stash] = 0
				payload[layout.stashTagStart+stash] = 0
			} else {
				payload[layout.stashBitmap+stash/8] &^=
					byte(1) << uint(stash&7)
				payload[layout.stashRankStart+stash] = 0
			}
			payload[1]--
		} else if oldRank > rank {
			if v.class == CommonPrimaryLeafNarrow {
				payload[layout.stashRankStart+stash]--
			} else {
				payload[layout.stashRankStart+stash]--
			}
		}
	}
	if int(slot) >= CommonPrimaryLeafNormalSlots {
		if err := v.rebuildMutationStash(payload, &layout, int(slot), 0, nil); err != nil {
			return nil, err
		}
	}

	commonPrimaryLeafDeletePackedField(
		payload[layout.keyLengthStart:layout.overflowStart],
		v.payload[v.layout.keyLengthStart:v.layout.overflowStart],
		int(v.count)*7, rank*7, 7,
	)
	commonPrimaryLeafDeletePackedField(
		payload[layout.overflowStart:layout.lowStart],
		v.payload[v.layout.overflowStart:v.layout.lowStart],
		int(v.count), rank, 1,
	)

	copy(
		payload[layout.heapStart:],
		v.payload[v.layout.heapStart:recordStart],
	)
	prefixBytes := recordStart - v.layout.heapStart
	copy(
		payload[layout.heapStart+prefixBytes:],
		v.payload[recordEnd:],
	)

	position := v.layout.heapStart >> commonPrimaryLeafLowerBits
	for oldIndex := 0; oldIndex <= int(v.count); oldIndex++ {
		offset := uint16(
			(position-oldIndex)<<commonPrimaryLeafLowerBits,
		) | uint16(v.payload[v.layout.lowStart+oldIndex])
		if oldIndex != rank+1 {
			newIndex := oldIndex
			newOffset := int(offset) - v.layout.heapStart + layout.heapStart
			if oldIndex > rank+1 {
				newIndex--
				newOffset -= removed
			}
			payload[layout.lowStart+newIndex] = byte(newOffset)
			newPosition := (newOffset >>
				commonPrimaryLeafLowerBits) + newIndex
			payload[layout.highStart+newPosition/8] |=
				byte(1) << uint(newPosition&7)
			if newIndex%layout.checkpointStride == 0 {
				at := layout.checkpointStart +
					(newIndex/layout.checkpointStride)*layout.checkpointWidth
				if layout.checkpointWidth == 1 {
					payload[at] = byte(newPosition)
				} else {
					binary.LittleEndian.PutUint16(
						payload[at:], uint16(newPosition),
					)
				}
			}
		}
		if oldIndex != int(v.count) {
			position, _ = commonPrimaryLeafNextHighBitAt(
				v.payload, v.layout.highStart, v.layout.highBytes, position+1,
			)
		}
	}
	page := dst[:len(v.page)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func (v *CommonPrimaryLeafView) emptySlot(hash uint64) (uint8, bool) {
	first, second := commonPrimaryLeafGroups(hash)
	groups := [2]uint8{first, second}
	homes := [2]uint8{
		uint8(hash>>16) & (commonPrimaryLeafGroupSize - 1),
		uint8(hash>>20) & (commonPrimaryLeafGroupSize - 1),
	}
	for groupIndex := range 2 {
		base := groups[groupIndex] * commonPrimaryLeafGroupSize
		for ordinal := range uint8(commonPrimaryLeafGroupSize) {
			slot := base + (homes[groupIndex]+ordinal)&
				(commonPrimaryLeafGroupSize-1)
			if v.payload[v.layout.controlStart+int(slot)] == 0 {
				return slot, true
			}
		}
	}
	mask := v.stashMask()
	for stash := 0; stash < v.class.stashSlots(); stash++ {
		if mask&(uint64(1)<<uint(stash)) == 0 {
			return uint8(CommonPrimaryLeafNormalSlots + stash), true
		}
	}
	return 0, false
}

// InsertTo writes a new copy-on-write leaf image into dst adding (key, value)
// under a strictly newer generation and returns the slot it assigned. It fails
// with ErrCommonPrimaryLeafFull or ErrCommonPrimaryLeafNeedsWide when the class
// cannot hold the row, signaling the caller to split or promote.
func (v *CommonPrimaryLeafView) InsertTo(
	dst []byte,
	generation uint64,
	key []byte,
	value CommonPrimaryLeafValue,
) ([]byte, uint8, error) {
	if v == nil || generation <= v.header.Generation ||
		generation >= uint64(1)<<48 || len(key) == 0 ||
		len(key) > CommonPrimaryLeafMaxKeyBytes ||
		commonPrimaryLeafValueBytes(value) == 0 ||
		len(dst) < len(v.page) ||
		commonPrimaryLeafOverlaps(dst, v.page) {
		return nil, 0, fmt.Errorf("%w: primary leaf insert", ErrInvalidWrite)
	}
	hash := commonPrimaryLeafHash(v.seed, key)
	if _, _, _, ok := v.LookupRawHashed(hash, key); ok {
		return nil, 0, fmt.Errorf("%w: duplicate primary leaf key", ErrInvalidWrite)
	}
	if int(v.count) >= v.class.slots() {
		if v.class == CommonPrimaryLeafNarrow {
			return nil, 0, ErrCommonPrimaryLeafNeedsWide
		}
		return nil, 0, ErrCommonPrimaryLeafFull
	}
	slot, ok := v.emptySlot(hash)
	if !ok {
		if v.class == CommonPrimaryLeafNarrow {
			return nil, 0, ErrCommonPrimaryLeafNeedsWide
		}
		return nil, 0, ErrCommonPrimaryLeafFull
	}
	page, err := v.insertSlotTo(dst, generation, slot, hash, key, value)
	return page, slot, err
}

// InsertSlotTo restores a key into a specific vacant stable slot. Delete/restore
// workflows use this form to preserve secondary-index coordinates without
// paying for global placement or moving any surviving key.
func (v *CommonPrimaryLeafView) InsertSlotTo(
	dst []byte,
	generation uint64,
	slot uint8,
	key []byte,
	value CommonPrimaryLeafValue,
) ([]byte, error) {
	if v == nil || generation <= v.header.Generation ||
		generation >= uint64(1)<<48 || len(key) == 0 ||
		len(key) > CommonPrimaryLeafMaxKeyBytes ||
		commonPrimaryLeafValueBytes(value) == 0 ||
		len(dst) < len(v.page) ||
		commonPrimaryLeafOverlaps(dst, v.page) {
		return nil, fmt.Errorf("%w: primary leaf stable insert", ErrInvalidWrite)
	}
	hash := commonPrimaryLeafHash(v.seed, key)
	if _, _, _, ok := v.LookupRawHashed(hash, key); ok {
		return nil, fmt.Errorf("%w: duplicate primary leaf key", ErrInvalidWrite)
	}
	if int(v.count) >= v.class.slots() {
		if v.class == CommonPrimaryLeafNarrow {
			return nil, ErrCommonPrimaryLeafNeedsWide
		}
		return nil, ErrCommonPrimaryLeafFull
	}
	if int(slot) >= v.class.slots() {
		return nil, fmt.Errorf("%w: primary leaf stable slot", ErrInvalidWrite)
	}
	if _, live := v.slotRank(slot); live ||
		int(slot) < CommonPrimaryLeafNormalSlots &&
			!commonPrimaryLeafNormalCandidate(hash, slot) {
		return nil, fmt.Errorf("%w: primary leaf stable slot", ErrInvalidWrite)
	}
	return v.insertSlotTo(dst, generation, slot, hash, key, value)
}

func (v *CommonPrimaryLeafView) insertSlotTo(
	dst []byte,
	generation uint64,
	slot uint8,
	hash uint64,
	key []byte,
	value CommonPrimaryLeafValue,
) ([]byte, error) {
	logicalID, _ := CommonPrimaryLeafLogicalID(v.header.Bucket)
	if value.IsOverflow() &&
		!commonPrimaryLeafValidateOverflow(
			value.Overflow, logicalID, generation, v.bounds,
		) {
		return nil, fmt.Errorf("%w: primary leaf insert overflow", ErrInvalidWrite)
	}
	insertRank := v.LowerBound(key)
	count := int(v.count) + 1
	layout := commonPrimaryLeafLayoutFor(
		v.class, count, len(v.page),
	)
	valueBytes := commonPrimaryLeafValueBytes(value)
	recordBytes := len(key) + valueBytes
	if len(key) >= commonPrimaryLeafEscapeLength {
		recordBytes++
	}
	heapBytes := len(v.payload) - v.layout.heapStart + recordBytes
	payloadLength := layout.heapStart + heapBytes
	payloadCapacity := len(v.page) - PageHeaderSize - PageTrailerSize
	if payloadLength > payloadCapacity {
		if v.class == CommonPrimaryLeafNarrow {
			return nil, ErrCommonPrimaryLeafNeedsWide
		}
		return nil, ErrCommonPrimaryLeafFull
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: v.header.StoreID, Generation: generation,
		LogicalID: logicalID, PageSize: uint32(len(v.page)),
		PayloadLength: uint32(payloadLength),
		Kind:          commonPrimaryLeafPageKind,
	})
	if err != nil {
		return nil, err
	}
	copy(
		payload[:layout.keyLengthStart],
		v.payload[:v.layout.keyLengthStart],
	)
	payload[0] = byte(count - 1)
	payload[2] = byte(v.class)

	for normal := range CommonPrimaryLeafNormalSlots {
		if v.payload[v.layout.controlStart+normal] != 0 &&
			int(v.payload[v.layout.normalRankStart+normal]) >= insertRank {
			payload[layout.normalRankStart+normal]++
		}
	}
	for stash := 0; stash < v.class.stashSlots(); stash++ {
		oldRank, live := v.stashRankAt(stash)
		if live && oldRank >= insertRank {
			payload[layout.stashRankStart+stash]++
		}
	}
	if int(slot) < CommonPrimaryLeafNormalSlots {
		payload[layout.controlStart+int(slot)] =
			commonPrimaryLeafControlLive | byte(hash>>57)
		payload[layout.normalRankStart+int(slot)] = byte(insertRank)
	} else {
		stash := int(slot) - CommonPrimaryLeafNormalSlots
		if v.class == CommonPrimaryLeafNarrow {
			payload[layout.stashRankStart+stash] = byte(insertRank + 1)
			payload[layout.stashTagStart+stash] = byte(hash >> 56)
			commonPrimaryLeafFilterAdd(
				payload[layout.filterStart:layout.filterStart+layout.filterLen],
				hash,
			)
		} else {
			payload[layout.stashBitmap+stash/8] |= byte(1) << uint(stash&7)
			payload[layout.stashRankStart+stash] = byte(insertRank)
		}
		payload[1]++
	}
	if v.class == CommonPrimaryLeafWide &&
		int(slot) >= CommonPrimaryLeafNormalSlots {
		if err := v.rebuildMutationStash(payload, &layout, -1, int(slot), key); err != nil {
			return nil, err
		}
	}

	keyCode := min(len(key), commonPrimaryLeafEscapeLength)
	commonPrimaryLeafInsertPackedField(
		payload[layout.keyLengthStart:layout.overflowStart],
		v.payload[v.layout.keyLengthStart:v.layout.overflowStart],
		int(v.count)*7, insertRank*7, 7, uint16(keyCode),
	)
	overflowCode := uint16(0)
	if value.IsOverflow() {
		overflowCode = 1
	}
	commonPrimaryLeafInsertPackedField(
		payload[layout.overflowStart:layout.lowStart],
		v.payload[v.layout.overflowStart:v.layout.lowStart],
		int(v.count), insertRank, 1, overflowCode,
	)

	insertOffset, ok := v.boundary(insertRank)
	if !ok {
		return nil, ErrCommonPrimaryLeafCorrupt
	}
	prefixBytes := int(insertOffset) - v.layout.heapStart
	copy(
		payload[layout.heapStart:],
		v.payload[v.layout.heapStart:int(insertOffset)],
	)
	cursor := layout.heapStart + prefixBytes
	if len(key) >= commonPrimaryLeafEscapeLength {
		payload[cursor] = byte(len(key) - 1)
		cursor++
	}
	copy(payload[cursor:], key)
	cursor += len(key)
	if value.IsOverflow() {
		encodePageRef(payload[cursor:cursor+PageRefSize], value.Overflow)
		cursor += PageRefSize
	} else {
		copy(payload[cursor:], value.Inline)
		cursor += len(value.Inline)
	}
	copy(payload[cursor:], v.payload[int(insertOffset):])

	position := v.layout.heapStart >> commonPrimaryLeafLowerBits
	for oldIndex := 0; oldIndex <= int(v.count); oldIndex++ {
		offset := uint16(
			(position-oldIndex)<<commonPrimaryLeafLowerBits,
		) | uint16(v.payload[v.layout.lowStart+oldIndex])
		newOffset := int(offset) - v.layout.heapStart + layout.heapStart
		if oldIndex == insertRank {
			for ordinal, offset := range [...]int{
				newOffset, newOffset + recordBytes,
			} {
				newIndex := insertRank + ordinal
				payload[layout.lowStart+newIndex] = byte(offset)
				newPosition := (offset >>
					commonPrimaryLeafLowerBits) + newIndex
				payload[layout.highStart+newPosition/8] |=
					byte(1) << uint(newPosition&7)
				if newIndex%layout.checkpointStride == 0 {
					at := layout.checkpointStart +
						(newIndex/layout.checkpointStride)*layout.checkpointWidth
					if layout.checkpointWidth == 1 {
						payload[at] = byte(newPosition)
					} else {
						binary.LittleEndian.PutUint16(
							payload[at:], uint16(newPosition),
						)
					}
				}
			}
		} else {
			newIndex := oldIndex
			if oldIndex > insertRank {
				newIndex++
				newOffset += recordBytes
			}
			payload[layout.lowStart+newIndex] = byte(newOffset)
			newPosition := (newOffset >>
				commonPrimaryLeafLowerBits) + newIndex
			payload[layout.highStart+newPosition/8] |=
				byte(1) << uint(newPosition&7)
			if newIndex%layout.checkpointStride == 0 {
				at := layout.checkpointStart +
					(newIndex/layout.checkpointStride)*layout.checkpointWidth
				if layout.checkpointWidth == 1 {
					payload[at] = byte(newPosition)
				} else {
					binary.LittleEndian.PutUint16(
						payload[at:], uint16(newPosition),
					)
				}
			}
		}
		if oldIndex != int(v.count) {
			position, _ = commonPrimaryLeafNextHighBitAt(
				v.payload, v.layout.highStart, v.layout.highBytes, position+1,
			)
		}
	}
	page := dst[:len(v.page)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func (v *CommonPrimaryLeafView) stashRankAt(
	stash int,
) (int, bool) {
	code := v.payload[v.layout.stashRankStart+stash]
	if v.class == CommonPrimaryLeafNarrow {
		if code == 0 {
			return 0, false
		}
		return int(code) - 1, true
	}
	if v.payload[v.layout.stashBitmap+stash/8]&
		(byte(1)<<uint(stash&7)) == 0 {
		return 0, false
	}
	return int(code), true
}

func (v *CommonPrimaryLeafView) rebuildMutationStash(
	payload []byte,
	layout *commonPrimaryLeafLayout,
	deletedSlot int,
	insertedSlot int,
	insertedKey []byte,
) error {
	if v.class == CommonPrimaryLeafNarrow {
		clear(payload[layout.filterStart : layout.filterStart+layout.filterLen])
	} else {
		clear(payload[layout.stashHashStart : layout.stashHashStart+layout.stashHashLen])
	}
	for stash := 0; stash < v.class.stashSlots(); stash++ {
		slot := CommonPrimaryLeafNormalSlots + stash
		if slot == deletedSlot {
			continue
		}
		var hash uint64
		if slot == insertedSlot {
			hash = commonPrimaryLeafHash(v.seed, insertedKey)
		} else {
			rank, live := v.stashRankAt(stash)
			if !live {
				continue
			}
			key, _, ok := v.keyAt(rank)
			if !ok {
				return ErrCommonPrimaryLeafCorrupt
			}
			hash = commonPrimaryLeafHash(v.seed, key)
		}
		if v.class == CommonPrimaryLeafNarrow {
			commonPrimaryLeafFilterAdd(
				payload[layout.filterStart:layout.filterStart+layout.filterLen],
				hash,
			)
		} else {
			commonPrimaryLeafInsertWideHash(
				payload, *layout, hash, stash,
			)
		}
	}
	return nil
}
