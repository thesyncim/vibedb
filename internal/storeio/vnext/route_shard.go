package vnext

import (
	"encoding/binary"
	"hash/crc32"
	"math/bits"

	"github.com/thesyncim/vibedb/internal/storemem"
)

// RouteShard is an immutable, compact, open-addressed route table. It maps a
// caller-supplied complete keyed hash to stable uint32 locations. The hash and
// its sixteen-bit tag only prune candidates: Lookup delegates identity to the
// caller's exact verifier before returning a location.
//
// The table owns one pointer-free [storemem.Block] when constructed by
// BuildRouteShard. OpenRouteShard instead borrows an already immutable image.
// Both forms permit concurrent Lookup calls only while the bytes are immutable
// and Close is not concurrent with use. This type provides no publication or
// mutation synchronization; a future owner must publish a complete immutable
// shard with its own generation/root protocol.
//
// Every slot is bit-packed little-endian as [state:2 | location:32 | tag:16],
// for exactly fifty bits, or 6.25 bytes before the final-byte rounding. States
// are empty, live, and tombstone; the final state spelling is rejected. The
// current builder emits live and empty states only. Tombstones are accepted on
// open so a future immutable image format can preserve probe chains, but are
// deliberately not mutated in place here.
type RouteShard struct {
	block    *storemem.Block
	data     []byte
	slots    []byte
	capacity uint32
	count    uint32
}

// RouteShardEntry is one builder input. Hash must be the complete keyed hash
// chosen by the owning collection; Location is a stable, opaque 32-bit route.
// Equal hashes and equal tags are legal because the reader verifies the full
// key through Lookup's verifier.
type RouteShardEntry struct {
	Hash     uint64
	Location uint32
}

const (
	routeShardMagic       = uint32(0x52534a56) // "VJSR" little-endian
	routeShardVersion     = uint16(1)
	routeShardHeaderBytes = 32
	routeShardSlotBits    = uint64(50)
	routeShardTagBits     = uint64(16)

	routeShardEmpty     = uint64(0)
	routeShardLive      = uint64(1)
	routeShardTombstone = uint64(2)
)

// BuildRouteShard builds one immutable shard at at most 15/16 occupancy. The
// returned block is outside the Go heap on common Unix platforms. Entries are
// copied into that block; no input bytes or slices are retained.
func BuildRouteShard(entries []RouteShardEntry) (*RouteShard, error) {
	capacity, ok := routeShardCapacity(len(entries))
	if !ok {
		return nil, ErrInvalidFrame
	}
	slotBytes, ok := routeShardSlotBytes(capacity)
	if !ok || slotBytes > int(^uint(0)>>1)-routeShardHeaderBytes {
		return nil, ErrInvalidFrame
	}
	block, err := storemem.Allocate(routeShardHeaderBytes + slotBytes)
	if err != nil {
		return nil, err
	}
	data := block.Bytes()
	binary.LittleEndian.PutUint32(data[0:4], routeShardMagic)
	binary.LittleEndian.PutUint16(data[4:6], routeShardVersion)
	binary.LittleEndian.PutUint16(data[6:8], routeShardHeaderBytes)
	binary.LittleEndian.PutUint32(data[8:12], capacity)
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(entries)))
	binary.LittleEndian.PutUint32(data[16:20], uint32(slotBytes))
	slots := data[routeShardHeaderBytes:]
	mask := uint64(capacity - 1)
	for _, entry := range entries {
		tag := routeShardTag(entry.Hash)
		for probe := uint64(0); probe < uint64(capacity); probe++ {
			index := (entry.Hash + probe) & mask
			offset := index * routeShardSlotBits
			if routeShardBits(slots, offset, routeShardSlotBits)&3 != routeShardEmpty {
				continue
			}
			routeShardPutBits(slots, offset, routeShardSlotBits,
				routeShardLive|uint64(entry.Location)<<2|uint64(tag)<<34)
			break
		}
	}
	binary.LittleEndian.PutUint32(data[20:24], routeShardChecksum(data))
	shard, err := openRouteShard(data)
	if err != nil {
		_ = block.Close()
		return nil, err
	}
	shard.block = block
	return shard, nil
}

// OpenRouteShard verifies and borrows one immutable shard image. The caller
// retains ownership of src and must not modify or release it while using the
// returned shard. The returned value has no block to Close.
func OpenRouteShard(src []byte) (*RouteShard, error) { return openRouteShard(src) }

func openRouteShard(src []byte) (*RouteShard, error) {
	if len(src) < routeShardHeaderBytes ||
		binary.LittleEndian.Uint32(src[0:4]) != routeShardMagic ||
		binary.LittleEndian.Uint16(src[4:6]) != routeShardVersion ||
		binary.LittleEndian.Uint16(src[6:8]) != routeShardHeaderBytes ||
		!allZero(src[24:routeShardHeaderBytes]) {
		return nil, corrupt("route shard header")
	}
	capacity := binary.LittleEndian.Uint32(src[8:12])
	count := binary.LittleEndian.Uint32(src[12:16])
	slotBytes := binary.LittleEndian.Uint32(src[16:20])
	expectedBytes, ok := routeShardSlotBytes(capacity)
	if !ok || uint64(slotBytes) != uint64(expectedBytes) ||
		uint64(routeShardHeaderBytes)+uint64(slotBytes) != uint64(len(src)) ||
		count > capacity || uint64(count)*16 > uint64(capacity)*15 ||
		binary.LittleEndian.Uint32(src[20:24]) != routeShardChecksum(src) {
		return nil, corrupt("route shard bounds")
	}
	slots := src[routeShardHeaderBytes:]
	live := uint32(0)
	for index := uint64(0); index < uint64(capacity); index++ {
		word := routeShardBits(slots, index*routeShardSlotBits, routeShardSlotBits)
		state := word & 3
		switch state {
		case routeShardEmpty, routeShardTombstone:
			if word != state {
				return nil, corrupt("route shard empty or tombstone payload")
			}
		case routeShardLive:
			live++
		default:
			return nil, corrupt("route shard state")
		}
	}
	usedBits := uint64(capacity) * routeShardSlotBits
	if excess := uint64(slotBytes)*8 - usedBits; excess != 0 &&
		slots[len(slots)-1]&byte(0xff<<(8-excess)) != 0 {
		return nil, corrupt("route shard tail")
	}
	if live != count {
		return nil, corrupt("route shard live count")
	}
	return &RouteShard{data: src, slots: slots, capacity: capacity, count: count}, nil
}

// Lookup probes one route shard linearly from the full hash's home bucket. A
// non-nil verifier must compare the authoritative complete key for candidate
// locations; nil therefore reports a miss rather than trusting a fingerprint.
// It allocates nothing and never exposes an unverified location.
func (s *RouteShard) Lookup(hash uint64, verify func(location uint32) bool) (uint32, bool) {
	if s == nil || s.data == nil || verify == nil {
		return 0, false
	}
	tag := routeShardTag(hash)
	mask := uint64(s.capacity - 1)
	for probe := uint64(0); probe < uint64(s.capacity); probe++ {
		word := routeShardBits(s.slots, ((hash+probe)&mask)*routeShardSlotBits, routeShardSlotBits)
		switch word & 3 {
		case routeShardEmpty:
			return 0, false
		case routeShardLive:
			if uint16(word>>34) == tag && verify(uint32(word>>2)) {
				return uint32(word >> 2), true
			}
		}
	}
	return 0, false
}

// Len reports the number of live routes.
func (s *RouteShard) Len() int {
	if s == nil {
		return 0
	}
	return int(s.count)
}

// Capacity reports the immutable probe-table capacity.
func (s *RouteShard) Capacity() int {
	if s == nil {
		return 0
	}
	return int(s.capacity)
}

// Bytes borrows the complete immutable image. It becomes invalid after Close
// for an owned shard and must never be modified while the shard is in use.
func (s *RouteShard) Bytes() []byte {
	if s == nil {
		return nil
	}
	return s.data
}

// OutsideHeap reports whether an owned shard uses external anonymous memory.
// A borrowed shard returns false because ownership belongs to its caller.
func (s *RouteShard) OutsideHeap() bool { return s != nil && s.block != nil && s.block.OutsideHeap() }

// Close releases an owned builder block. It is not concurrent with Lookup.
// Borrowed shards have nothing to release.
func (s *RouteShard) Close() error {
	if s == nil || s.block == nil {
		return nil
	}
	block := s.block
	s.block = nil
	s.data = nil
	s.slots = nil
	s.capacity = 0
	s.count = 0
	if err := block.Close(); err != nil {
		return err
	}
	return nil
}

func routeShardCapacity(count int) (uint32, bool) {
	if count < 0 || uint64(count) > uint64(^uint32(0))*15/16 {
		return 0, false
	}
	need := uint64(2)
	for need*15 < uint64(count)*16 {
		if need > uint64(^uint32(0))/2 {
			return 0, false
		}
		need <<= 1
	}
	return uint32(need), true
}

func routeShardSlotBytes(capacity uint32) (int, bool) {
	if capacity < 2 || capacity&(capacity-1) != 0 {
		return 0, false
	}
	bytes := (uint64(capacity)*routeShardSlotBits + 7) / 8
	if bytes > uint64(int(^uint(0)>>1)) || bytes > uint64(^uint32(0)) {
		return 0, false
	}
	return int(bytes), true
}

func routeShardTag(hash uint64) uint16 {
	// The home bucket uses low hash bits. Derive the tag from a separately mixed
	// view so the two selector fields remain independent for a keyed hash.
	mixed := bits.RotateLeft64(hash, 23) ^ hash>>17 ^ hash<<11
	return uint16(mixed & ((1 << routeShardTagBits) - 1))
}

func routeShardBits(src []byte, offset, width uint64) uint64 {
	var value uint64
	for shift := uint64(0); shift < width; {
		at := offset + shift
		available := 8 - at&7
		take := min(width-shift, available)
		value |= uint64((src[at>>3]>>uint(at&7))&byte((1<<take)-1)) << shift
		shift += take
	}
	return value
}

func routeShardPutBits(dst []byte, offset, width, value uint64) {
	for shift := uint64(0); shift < width; {
		at := offset + shift
		available := 8 - at&7
		take := min(width-shift, available)
		mask := byte((1 << take) - 1)
		index := at >> 3
		dst[index] = dst[index]&^(mask<<uint(at&7)) | byte(value&uint64(mask))<<uint(at&7)
		value >>= take
		shift += take
	}
}

func routeShardChecksum(data []byte) uint32 {
	checksum := crc32.Update(0, frameCRC, data[:20])
	var zero [4]byte
	checksum = crc32.Update(checksum, frameCRC, zero[:])
	return crc32.Update(checksum, frameCRC, data[24:])
}
