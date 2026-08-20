package distribution

import "encoding/binary"

// DefaultVirtualBucketBits fixes the current logical bucket space at 2^20.
// A distribution may state the same value explicitly in catalog metadata; a
// zero metadata value selects this default so in-memory callers do not need a
// second configuration path.
const DefaultVirtualBucketBits uint8 = 20

const (
	MinVirtualBucketBits uint8 = 8
	MaxVirtualBucketBits uint8 = 24
)

// VirtualBucket is a logical placement slot. Physical shards own contiguous
// runs of buckets; tenant identity never implies one physical owner.
type VirtualBucket uint32

// ValidVirtualBucketBits reports whether bits is an admitted bucket width.
// The upper bound keeps bucket identifiers in 24 bits and catalog/range work
// bounded while leaving sixteen million independently movable slots.
func ValidVirtualBucketBits(bits uint8) bool {
	return bits >= MinVirtualBucketBits && bits <= MaxVirtualBucketBits
}

// VirtualBucketCount returns the number of buckets for bits, or zero for an
// unsupported width.
func VirtualBucketCount(bits uint8) uint32 {
	if !ValidVirtualBucketBits(bits) {
		return 0
	}
	return uint32(1) << bits
}

// VirtualBucketForHash selects the high bits of one stable tuple hash. The
// returned keyspace point is the canonical start of that bucket, so every row
// in a bucket resolves identically even if a malformed manifest boundary cuts
// through unused interior keyspace.
func VirtualBucketForHash(hash uint64, bits uint8) (VirtualBucket, KeyspacePoint, bool) {
	if !ValidVirtualBucketBits(bits) {
		return 0, KeyspacePoint{}, false
	}
	bucket := VirtualBucket(hash >> (64 - bits))
	return bucket, virtualBucketStart(bucket, bits), true
}

// VirtualBucketForPoint recovers the logical bucket containing p.
func VirtualBucketForPoint(p KeyspacePoint, bits uint8) (VirtualBucket, bool) {
	if !ValidVirtualBucketBits(bits) {
		return 0, false
	}
	return VirtualBucket(binary.BigEndian.Uint64(p[:]) >> (64 - bits)), true
}

// VirtualBucketRange returns the exact half-open keyspace interval represented
// by bucket. The last bucket ends at Max because 2^64 has no KeyspacePoint.
func VirtualBucketRange(bucket VirtualBucket, bits uint8) (KeyRange, bool) {
	count := VirtualBucketCount(bits)
	if count == 0 || uint32(bucket) >= count {
		return KeyRange{}, false
	}
	start := virtualBucketStart(bucket, bits)
	if uint32(bucket)+1 == count {
		return KeyRange{Start: start, End: KeyspaceEnd{Max: true}}, true
	}
	return KeyRange{
		Start: start,
		End:   KeyspaceEnd{Point: virtualBucketStart(bucket+1, bits)},
	}, true
}

// VirtualBucketBoundary reports whether p is the canonical start of a bucket.
func VirtualBucketBoundary(p KeyspacePoint, bits uint8) bool {
	if !ValidVirtualBucketBits(bits) {
		return false
	}
	value := binary.BigEndian.Uint64(p[:])
	return value&((uint64(1)<<(64-bits))-1) == 0
}

func virtualBucketStart(bucket VirtualBucket, bits uint8) KeyspacePoint {
	var point KeyspacePoint
	binary.BigEndian.PutUint64(point[:], uint64(bucket)<<(64-bits))
	return point
}
