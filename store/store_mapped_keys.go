package store

import (
	"encoding/binary"
	"math"
	"math/bits"
	"runtime"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storemem"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// storeMappedKeyRef is the compact, pointer-free authority for one key in a
// collection image. off and length address the already-validated manifest; loc is
// the stable collection row. The open-addressed table stores a one-based index into
// refs, keeping an empty bucket representable without reserving a hash value.
type storeMappedKeyRef struct {
	off    uint64
	length uint32
	loc    uint32
}

// storeCompactKeyRef is the Builder layout while the owned key arena is
// below 4 GiB. Open keeps storeMappedKeyRef because offsets address an
// arbitrary caller-owned image. Both layouts keep the location inline, so a
// successful lookup reads one descriptor cache line.
type storeCompactKeyRef struct {
	off    uint32
	length uint32
	loc    uint32
}

// storeDenseKeyRef is the eight-byte bulk-build layout. Builder emits
// contiguous full chunks and assigns slots in ordinal order, so ref itself
// determines chunk and slot when ChunkDocuments is a power of two. Persisted
// images may contain sparse chunk IDs and therefore retain an explicit loc.
type storeDenseKeyRef struct {
	off    uint32
	length uint32
}

const (
	storeMappedLocationSlotBits = 6
	storeMappedLocationMaxChunk = 1 << (32 - storeMappedLocationSlotBits)
	storeMappedKeyRefBytes      = 16
	storeCompactKeyRefBytes     = 12
	storeDenseKeyRefBytes       = 8
)

var _ [storeMappedKeyRefBytes - unsafe.Sizeof(storeMappedKeyRef{})]byte
var _ [unsafe.Sizeof(storeMappedKeyRef{}) - storeMappedKeyRefBytes]byte
var _ [storeCompactKeyRefBytes - unsafe.Sizeof(storeCompactKeyRef{})]byte
var _ [unsafe.Sizeof(storeCompactKeyRef{}) - storeCompactKeyRefBytes]byte
var _ [storeDenseKeyRefBytes - unsafe.Sizeof(storeDenseKeyRef{})]byte
var _ [unsafe.Sizeof(storeDenseKeyRef{}) - storeDenseKeyRefBytes]byte

// storeMappedKeys replaces the pointer-heavy key HAMT for an immutable mapped
// base. refs and buckets share one anonymous pointer-free block outside the Go
// heap on common Unix systems. Later mutations live in the ordinary persistent
// HAMT overlay; a lookup checks that overlay first, then this base, and always
// verifies the complete current key spelling at the returned stable slot.
//
// No view into block escapes a method. The finalizer is therefore a resource
// backstop, not a borrowed-value lifetime mechanism: every collection state and
// mapped chunk keeps this object reachable, and runtime.KeepAlive covers the
// last native load. Returned key strings borrow source, the caller-owned collection
// image, whose existing lifetime contract is unchanged.
type storeMappedKeys struct {
	source     []byte
	refs       []storeMappedKeyRef
	compact    []storeCompactKeyRef
	dense      []storeDenseKeyRef
	wideLocs   []Location
	controls   []byte
	slots32    []uint32
	slots64    []uint64
	mask       uint64
	count      int
	denseShift uint8
	// flexible is selected once for compact, ordinal-derived, wide-location,
	// or 64-bit-slot layouts. The zero value keeps Open's common lookup
	// guard to one byte load without preloading cold slice headers.
	flexible bool
	block    *storemem.Block
	// sourceBlock is non-nil when Builder owns packed key spellings.
	// Open instead borrows its caller-owned image through source.
	sourceBlock *storemem.Block
}

func newStoreMappedKeys(source []byte, count int, wideLocations bool) (*storeMappedKeys, error) {
	return newStoreMappedKeysLayout(source, count, wideLocations, 0)
}

// refWidth is 0 for the mapped 16-byte layout, 12 for compact owned refs, and
// 8 for dense owned refs. Keeping the choice numeric makes invalid internal
// combinations fail closed in one constructor.
func newStoreMappedKeysLayout(source []byte, count int, wideLocations bool, refWidth int) (*storeMappedKeys, error) {
	refSize := int(unsafe.Sizeof(storeMappedKeyRef{}))
	switch refWidth {
	case storeCompactKeyRefBytes:
		refSize = int(unsafe.Sizeof(storeCompactKeyRef{}))
	case storeDenseKeyRefBytes:
		refSize = int(unsafe.Sizeof(storeDenseKeyRef{}))
	case 0:
	default:
		return nil, ErrCheckpointTooLarge
	}
	if count < 0 || count > math.MaxInt/refSize {
		return nil, ErrCheckpointTooLarge
	}
	capacity := 1
	for capacity < 8 || count > capacity-capacity/4 {
		if capacity > math.MaxInt/2 {
			return nil, ErrCheckpointTooLarge
		}
		capacity *= 2
	}
	locationBytes := 0
	if wideLocations {
		locationSize := int(unsafe.Sizeof(Location{}))
		if count > math.MaxInt/locationSize {
			return nil, ErrCheckpointTooLarge
		}
		locationBytes = count * locationSize
	}
	// controls repeats its first seven bytes after the logical table so every
	// wrapping eight-byte probe is one bounds-checked native load. slots starts
	// at the next eight-byte boundary.
	controlBytes := capacity + storeMappedKeyGroup - 1
	slotOffset := (controlBytes + 7) &^ 7
	slotSize := 4
	if uint64(count) >= uint64(^uint32(0)) {
		slotSize = 8
	}
	if capacity > math.MaxInt/slotSize || slotOffset > math.MaxInt-capacity*slotSize {
		return nil, ErrCheckpointTooLarge
	}
	refBytes := uint64(count * refSize)
	tableBytes := uint64(slotOffset + capacity*slotSize)
	if refBytes > uint64(math.MaxInt)-uint64(locationBytes) ||
		refBytes+uint64(locationBytes) > uint64(math.MaxInt)-tableBytes {
		return nil, ErrCheckpointTooLarge
	}
	block, err := storemem.Allocate(int(refBytes + uint64(locationBytes) + tableBytes))
	if err != nil {
		return nil, err
	}
	data := block.Bytes()
	var refs []storeMappedKeyRef
	var compactRefs []storeCompactKeyRef
	var denseRefs []storeDenseKeyRef
	if count != 0 && refWidth == storeDenseKeyRefBytes {
		denseRefs = unsafe.Slice((*storeDenseKeyRef)(unsafe.Pointer(unsafe.SliceData(data))), count)
	} else if count != 0 && refWidth == storeCompactKeyRefBytes {
		compactRefs = unsafe.Slice((*storeCompactKeyRef)(unsafe.Pointer(unsafe.SliceData(data))), count)
	} else if count != 0 {
		refs = unsafe.Slice((*storeMappedKeyRef)(unsafe.Pointer(unsafe.SliceData(data))), count)
	}
	position := int(refBytes)
	var wideLocs []Location
	if wideLocations && count != 0 {
		wideLocs = unsafe.Slice((*Location)(unsafe.Pointer(&data[position])), count)
		position += locationBytes
	}
	tableData := data[position:]
	controls := tableData[:controlBytes]
	slotData := tableData[slotOffset:]
	m := &storeMappedKeys{
		source: source, refs: refs, compact: compactRefs, dense: denseRefs,
		wideLocs: wideLocs, controls: controls,
		mask: uint64(capacity - 1), flexible: refWidth != 0 || wideLocations || slotSize != 4,
		block: block,
	}
	if slotSize == 4 {
		m.slots32 = unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(slotData))), capacity)
	} else {
		m.slots64 = unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(slotData))), capacity)
	}
	runtime.SetFinalizer(m, (*storeMappedKeys).release)
	return m, nil
}

func newStoreOwnedKeys(count, sourceBytes int, wideLocations bool, chunkDocuments int) (*storeMappedKeys, error) {
	if sourceBytes < 0 {
		return nil, ErrCheckpointTooLarge
	}
	sourceBlock, err := storemem.Allocate(sourceBytes)
	if err != nil {
		return nil, err
	}
	refWidth := 0
	if uint64(sourceBytes) <= uint64(^uint32(0)) {
		refWidth = storeCompactKeyRefBytes
		if !wideLocations && chunkDocuments > 0 && chunkDocuments <= 64 && chunkDocuments&(chunkDocuments-1) == 0 {
			refWidth = storeDenseKeyRefBytes
		}
	}
	m, err := newStoreMappedKeysLayout(sourceBlock.Bytes(), count, wideLocations, refWidth)
	if err != nil {
		_ = sourceBlock.Close()
		return nil, err
	}
	m.sourceBlock = sourceBlock
	if refWidth == storeDenseKeyRefBytes {
		m.denseShift = uint8(bits.TrailingZeros(uint(chunkDocuments)))
	}
	return m, nil
}

func (m *storeMappedKeys) release() {
	if m == nil || m.block == nil {
		return
	}
	_ = m.block.Close()
	if m.sourceBlock != nil {
		_ = m.sourceBlock.Close()
		m.sourceBlock = nil
	}
	m.block = nil
	m.source = nil
	m.refs = nil
	m.compact = nil
	m.dense = nil
	m.wideLocs = nil
	m.controls = nil
	m.slots32 = nil
	m.slots64 = nil
}

func (m *storeMappedKeys) externalBytes() uint64 {
	if m == nil {
		return 0
	}
	var bytes uint64
	if m.block != nil && m.block.OutsideHeap() {
		bytes += uint64(m.block.Len())
	}
	if m.sourceBlock != nil && m.sourceBlock.OutsideHeap() {
		bytes += uint64(m.sourceBlock.Len())
	}
	return bytes
}

func (m *storeMappedKeys) key(ref uint64) string {
	off, length := m.keySpan(ref)
	end := off + uint64(length)
	key := byteview.String(m.source[off:end])
	runtime.KeepAlive(m)
	return key
}

func (m *storeMappedKeys) keySpan(ref uint64) (uint64, uint32) {
	if m.dense != nil {
		r := m.dense[ref]
		return uint64(r.off), r.length
	}
	if m.compact != nil {
		r := m.compact[ref]
		return uint64(r.off), r.length
	}
	r := m.refs[ref]
	return r.off, r.length
}

func (m *storeMappedKeys) setKeySpan(ref, off uint64, length uint32) {
	if m.dense != nil {
		m.dense[ref].off = uint32(off)
		m.dense[ref].length = length
		return
	}
	if m.compact != nil {
		m.compact[ref].off = uint32(off)
		m.compact[ref].length = length
		return
	}
	m.refs[ref].off = off
	m.refs[ref].length = length
}

func (m *storeMappedKeys) keyRefCount() int {
	if m.dense != nil {
		return len(m.dense)
	}
	if m.compact != nil {
		return len(m.compact)
	}
	return len(m.refs)
}

func (m *storeMappedKeys) slotCount() int {
	if m.slots32 != nil {
		return len(m.slots32)
	}
	return len(m.slots64)
}

func (m *storeMappedKeys) slotRef(slot uint64) uint64 {
	if m.slots32 != nil {
		return uint64(m.slots32[slot])
	}
	return m.slots64[slot]
}

func (m *storeMappedKeys) setSlotRef(slot, ref uint64) {
	if m.slots32 != nil {
		m.slots32[slot] = uint32(ref)
		return
	}
	m.slots64[slot] = ref
}

func (m *storeMappedKeys) setLocation(ref uint64, loc Location) {
	if m.dense != nil {
		want := Location{Chunk: uint32(ref >> m.denseShift), Slot: uint8(ref & uint64((1<<m.denseShift)-1))}
		if loc != want {
			panic("vibejson: dense collection key location invariant")
		}
		return
	}
	if m.wideLocs != nil {
		m.wideLocs[ref] = loc
		return
	}
	packed := loc.Chunk<<storeMappedLocationSlotBits | uint32(loc.Slot)
	if m.compact != nil {
		m.compact[ref].loc = packed
		return
	}
	m.refs[ref].loc = packed
}

const (
	storeMappedKeyGroup = 8
	storeMappedKeyLo    = uint64(0x0101010101010101)
	storeMappedKeyHi    = uint64(0x8080808080808080)
)

func storeMappedKeyFingerprint(hash uint64) byte { return byte(hash>>57) + 1 }

// storeMappedKeyMatches returns a high bit for every byte equal to value. The
// subtraction idiom may mark an adjacent byte after a match because of borrow;
// that is harmless because callers recheck the control byte and exact key.
// It never misses a matching byte.
func storeMappedKeyMatches(word uint64, value byte) uint64 {
	x := word ^ uint64(value)*storeMappedKeyLo
	return (x - storeMappedKeyLo) &^ x & storeMappedKeyHi
}

func storeMappedKeyHasEmpty(word uint64) bool {
	return (word-storeMappedKeyLo)&^word&storeMappedKeyHi != 0
}

// insert probes eight one-byte fingerprints per native word. Open is single-
// threaded and the table is immutable after publication. false reports a
// complete-key duplicate; hashes and seven-bit fingerprints are routing only.
func (m *storeMappedKeys) insert(hash, ref uint64) bool {
	want := m.key(ref)
	fingerprint := storeMappedKeyFingerprint(hash)
	index := hash & m.mask
	for {
		word := binary.LittleEndian.Uint64(m.controls[index : index+storeMappedKeyGroup])
		matches := storeMappedKeyMatches(word, fingerprint)
		for matches != 0 {
			lane := uint(bits.TrailingZeros64(matches)) >> 3
			slot := (index + uint64(lane)) & m.mask
			if m.controls[index+uint64(lane)] == fingerprint && m.key(m.slotRef(slot)-1) == want {
				runtime.KeepAlive(m)
				return false
			}
			matches &= matches - 1
		}
		if storeMappedKeyHasEmpty(word) {
			for lane := range uint64(storeMappedKeyGroup) {
				controlIndex := index + lane
				if m.controls[controlIndex] != 0 {
					continue
				}
				slot := controlIndex & m.mask
				m.controls[slot] = fingerprint
				if slot < storeMappedKeyGroup-1 {
					m.controls[uint64(m.slotCount())+slot] = fingerprint
				}
				m.setSlotRef(slot, ref+1)
				m.count++
				runtime.KeepAlive(m)
				return true
			}
		}
		index = (index + storeMappedKeyGroup) & m.mask
	}
}

func (m *storeMappedKeys) lookup(hash uint64, key string) (Location, bool) {
	if m == nil || m.count == 0 {
		return Location{}, false
	}
	// OpenStore's common layout has explicit 16-byte refs, packed locations,
	// and 32-bit table ordinals. Keep that established read path monomorphic;
	// the compact Builder layouts enter the equally exact flexible path.
	if m.flexible {
		return m.lookupFlexible(hash, key)
	}
	fingerprint := storeMappedKeyFingerprint(hash)
	index := hash & m.mask
	for {
		word := binary.LittleEndian.Uint64(m.controls[index : index+storeMappedKeyGroup])
		matches := storeMappedKeyMatches(word, fingerprint)
		for matches != 0 {
			lane := uint(bits.TrailingZeros64(matches)) >> 3
			controlIndex := index + uint64(lane)
			slot := controlIndex & m.mask
			if m.controls[controlIndex] == fingerprint {
				ref := uint64(m.slots32[slot]) - 1
				r := m.refs[ref]
				end := r.off + uint64(r.length)
				if uint32(len(key)) == r.length && vibejson.BytesEqualString(m.source[r.off:end], key) {
					loc := Location{
						Chunk: r.loc >> storeMappedLocationSlotBits,
						Slot:  uint8(r.loc & (1<<storeMappedLocationSlotBits - 1)),
					}
					runtime.KeepAlive(m)
					return loc, true
				}
			}
			matches &= matches - 1
		}
		if storeMappedKeyHasEmpty(word) {
			runtime.KeepAlive(m)
			return Location{}, false
		}
		index = (index + storeMappedKeyGroup) & m.mask
	}
}

// lookupFlexible serves ordinal-derived, compact, wide-location, and 64-bit
// slot layouts. Layout selection is exact metadata routing; every candidate
// still receives a full key-byte comparison before its location is returned.
func (m *storeMappedKeys) lookupFlexible(hash uint64, key string) (Location, bool) {
	fingerprint := storeMappedKeyFingerprint(hash)
	index := hash & m.mask
	for {
		word := binary.LittleEndian.Uint64(m.controls[index : index+storeMappedKeyGroup])
		matches := storeMappedKeyMatches(word, fingerprint)
		for matches != 0 {
			lane := uint(bits.TrailingZeros64(matches)) >> 3
			controlIndex := index + uint64(lane)
			slot := controlIndex & m.mask
			if m.controls[controlIndex] == fingerprint {
				ref := m.slotRef(slot) - 1
				var off uint64
				var length uint32
				var loc Location
				switch {
				case m.refs != nil:
					r := m.refs[ref]
					off, length = r.off, r.length
					if m.wideLocs != nil {
						loc = m.wideLocs[ref]
					} else {
						loc = Location{Chunk: r.loc >> storeMappedLocationSlotBits, Slot: uint8(r.loc & (1<<storeMappedLocationSlotBits - 1))}
					}
				case m.dense != nil:
					r := m.dense[ref]
					off, length = uint64(r.off), r.length
					loc = Location{Chunk: uint32(ref >> m.denseShift), Slot: uint8(ref & uint64((1<<m.denseShift)-1))}
				default:
					r := m.compact[ref]
					off, length = uint64(r.off), r.length
					if m.wideLocs != nil {
						loc = m.wideLocs[ref]
					} else {
						loc = Location{Chunk: r.loc >> storeMappedLocationSlotBits, Slot: uint8(r.loc & (1<<storeMappedLocationSlotBits - 1))}
					}
				}
				end := off + uint64(length)
				if uint32(len(key)) == length && vibejson.BytesEqualString(m.source[off:end], key) {
					runtime.KeepAlive(m)
					return loc, true
				}
			}
			matches &= matches - 1
		}
		if storeMappedKeyHasEmpty(word) {
			runtime.KeepAlive(m)
			return Location{}, false
		}
		index = (index + storeMappedKeyGroup) & m.mask
	}
}

func (m *storeMappedKeys) keyAt(base uint64, ordinal uint8) string {
	return m.key(base + uint64(ordinal))
}

func storeStateKeyLookup(state *State, hash uint64, key string) (Location, bool) {
	_, loc, ok := storeStateKeyLookupChunk(state, hash, key)
	return loc, ok
}

// storeStateKeyLookupChunk returns the already-resolved chunk with the stable
// location. Hot callers therefore walk the persistent chunk vector once. The
// mapped base's table already performed exact-key comparison; only a page that
// has since been rebuilt needs a second comparison against its current slot.
func storeStateKeyLookupChunk(state *State, hash uint64, key string) (*Chunk, Location, bool) {
	if state == nil {
		return nil, Location{}, false
	}
	if loc, ok := storeKeyLookup(state.keys, hash, key); ok {
		chunk := state.Chunks.Get(loc.Chunk)
		if chunk != nil && chunk.Live&(uint64(1)<<loc.Slot) != 0 {
			return chunk, loc, true
		}
	}
	if loc, ok := state.baseKeys.lookup(hash, key); ok {
		chunk := state.Chunks.Get(loc.Chunk)
		if chunk != nil && chunk.Live&(uint64(1)<<loc.Slot) != 0 &&
			(chunk.mappedKeys == state.baseKeys || chunk.Key(int(loc.Slot)) == key) {
			return chunk, loc, true
		}
	}
	return nil, Location{}, false
}
