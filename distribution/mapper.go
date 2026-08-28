package distribution

import "github.com/cespare/xxhash/v2"

// MapperVersion identifies a mapper's frozen mapping revision. It is part of
// placement identity.
type MapperVersion uint32

// PrefixSet is the set of leading prefix lengths a mapper supports, encoded as
// a bitset over 1..63. Length 0 is never a member.
type PrefixSet uint64

// NewPrefixSet returns the set of the given prefix lengths. Lengths outside
// 1..63 are ignored.
func NewPrefixSet(lengths ...int) PrefixSet {
	var s PrefixSet
	for _, l := range lengths {
		if l >= 1 && l <= 63 {
			s |= 1 << uint(l)
		}
	}
	return s
}

// Contains reports whether prefixLen is a supported prefix length.
func (s PrefixSet) Contains(prefixLen int) bool {
	if prefixLen < 1 || prefixLen > 63 {
		return false
	}
	return s&(1<<uint(prefixLen)) != 0
}

// LongestAtMost returns the longest supported prefix length not exceeding max,
// or 0 when none is supported. It is how the router picks the longest bound
// leading prefix a mapper can map.
func (s PrefixSet) LongestAtMost(max int) int {
	if max > 63 {
		max = 63
	}
	for l := max; l >= 1; l-- {
		if s&(1<<uint(l)) != 0 {
			return l
		}
	}
	return 0
}

// Mapper maps an ordered leading prefix of typed shard-key values to explicit
// keyspace points and ranges. A supported leading prefix may map to a wider
// range; a missing non-leading component never permits skipping ahead, so the
// router only ever supplies a contiguous bound leading prefix.
type Mapper interface {
	// Arity reports the number of shard-key columns of a full key.
	Arity() int
	// SupportedPrefixes reports which leading prefix lengths MapPrefix accepts.
	SupportedPrefixes() PrefixSet
	// Version reports the frozen mapping revision.
	Version() MapperVersion
	// Admits reports whether the mapper accepts values as a prefix of the given
	// length, returning a typed error otherwise.
	Admits(prefixLen int, values []Scalar) error
	// MapPrefix maps a supported leading prefix to its destinations. A full key
	// commonly yields one point; a shorter leading prefix yields a range.
	MapPrefix(values []Scalar) (DestinationSet, error)
}

// MapperInto is the optional allocation-free mapper extension used by Router
// when available. Implementations overwrite the logical contents of the caller
// scratch (retaining its capacity) and may allocate only when that capacity is
// insufficient for their result. MapPrefix retains the ordinary independently
// owned-result contract for callers that need to keep destinations.
type MapperInto interface {
	Mapper
	MapPrefixInto(
		values []Scalar,
		pointScratch []KeyspacePoint,
		rangeScratch []KeyRange,
	) (DestinationSet, error)
}

// NativeMapperVersion is the frozen version of the native reference mapper.
const NativeMapperVersion MapperVersion = 1

// NativeMapper hashes the complete canonical placement tuple and maps its high
// bits to one virtual bucket in the fixed 8-byte keyspace. Hashing the complete
// tuple is the property that lets (tenant, locality-key) placements spread one
// tenant across many buckets and physical shards. A shorter leading prefix
// cannot predict the remaining tuple hash and therefore maps honestly to the
// full keyspace; a tenant-only query scatters unless an index narrows it.
//
// xxHash64 is used only as a deterministic, high-throughput distribution hash.
// Scalar equality and tuple framing remain defined exclusively by the current
// canonical tuple codec.
type NativeMapper struct {
	arity      int
	prefixes   PrefixSet
	bucketBits uint8
}

// NewNativeMapper returns the one current native mapper over arity components
// using DefaultVirtualBucketBits.
func NewNativeMapper(arity int) *NativeMapper {
	return NewNativeMapperWithBucketBits(arity, DefaultVirtualBucketBits)
}

// NewNativeMapperWithBucketBits returns the current native mapper with an
// explicit virtual-bucket width. It panics for invalid static metadata; runtime
// catalog construction validates the same bounds before calling it.
func NewNativeMapperWithBucketBits(arity int, bucketBits uint8) *NativeMapper {
	if arity < 1 || arity > KeyspaceWidth {
		panic("distribution: NativeMapper arity must be in 1..8")
	}
	if !ValidVirtualBucketBits(bucketBits) {
		panic("distribution: NativeMapper virtual bucket bits must be in 8..24")
	}
	m := &NativeMapper{arity: arity, bucketBits: bucketBits}
	for l := 1; l <= arity; l++ {
		m.prefixes |= 1 << uint(l)
	}
	return m
}

// Arity reports the component count of a full key.
func (m *NativeMapper) Arity() int { return m.arity }

// SupportedPrefixes reports the supported leading prefix lengths, 1..Arity.
func (m *NativeMapper) SupportedPrefixes() PrefixSet { return m.prefixes }

// Version reports the frozen native mapping revision.
func (m *NativeMapper) Version() MapperVersion { return NativeMapperVersion }

// VirtualBucketBits reports the mapper's immutable bucket-space width.
func (m *NativeMapper) VirtualBucketBits() uint8 { return m.bucketBits }

// Admits reports whether values form an acceptable prefix of length prefixLen:
// the length must match and be supported, and every value must be an encodable
// placement scalar. It returns ErrIncompleteShardKey on a length mismatch, a
// *MapperError (matching ErrUnsupportedMapper) for an unsupported length, and a
// *ShardValueError (matching ErrInvalidShardValue) for a non-encodable value.
func (m *NativeMapper) Admits(prefixLen int, values []Scalar) error {
	if len(values) != prefixLen {
		return ErrIncompleteShardKey
	}
	if !m.prefixes.Contains(prefixLen) {
		return &MapperError{Reason: "unsupported prefix length"}
	}
	var buf [256]byte
	if _, err := appendTuple(buf[:0], values); err != nil {
		return &ShardValueError{Reason: "value is not an encodable placement scalar"}
	}
	return nil
}

// MapPrefix maps a supported leading prefix to its destinations. It validates
// the prefix itself, so it is safe to call without a preceding Admits.
func (m *NativeMapper) MapPrefix(values []Scalar) (DestinationSet, error) {
	return m.MapPrefixInto(values, nil, nil)
}

// MapPrefixInto maps values like MapPrefix while reusing caller-owned result
// storage. Native mapping emits exactly one point for a full key or one range
// for a shorter prefix, so one-element scratch makes this path allocation-free.
func (m *NativeMapper) MapPrefixInto(
	values []Scalar,
	pointScratch []KeyspacePoint,
	rangeScratch []KeyRange,
) (DestinationSet, error) {
	if !m.prefixes.Contains(len(values)) {
		if len(values) < 1 || len(values) > m.arity {
			return DestinationSet{}, ErrIncompleteShardKey
		}
		return DestinationSet{}, &MapperError{Reason: "unsupported prefix length"}
	}
	if len(values) == m.arity {
		p, err := m.mapFullKey(values)
		if err != nil {
			return DestinationSet{}, err
		}
		pointScratch = append(pointScratch[:0], p)
		return DestinationSet{Points: pointScratch}, nil
	}
	if err := m.Admits(len(values), values); err != nil {
		return DestinationSet{}, err
	}
	rangeScratch = append(rangeScratch[:0], KeyRange{
		Start: KeyspacePoint{}, End: KeyspaceEnd{Max: true},
	})
	return DestinationSet{Ranges: rangeScratch}, nil
}

// PointFor returns the full-key point that MapPrefix maps values to. len(values)
// must equal Arity; otherwise it reports ErrIncompleteShardKey. It lets callers
// predict the native mapping without decoding a DestinationSet.
func (m *NativeMapper) PointFor(values []Scalar) (KeyspacePoint, error) {
	if len(values) != m.arity {
		return KeyspacePoint{}, ErrIncompleteShardKey
	}
	return m.mapFullKey(values)
}

// NativePointForEncodedTuplePrefix maps the first arity canonical tuple
// frames in raw without materializing Scalars. consumed is the exact prefix
// length, allowing callers to validate an appended locator tuple separately.
// The operation allocates nothing and fails closed on malformed tuple bytes or
// an unsupported bucket width.
func NativePointForEncodedTuplePrefix(
	raw []byte,
	arity int,
	bucketBits uint8,
) (point KeyspacePoint, consumed int, ok bool) {
	if !ValidVirtualBucketBits(bucketBits) {
		return KeyspacePoint{}, 0, false
	}
	consumed, ok = CanonicalTuplePrefixLen(raw, arity)
	if !ok {
		return KeyspacePoint{}, 0, false
	}
	_, point, ok = VirtualBucketForHash(xxhash.Sum64(raw[:consumed]), bucketBits)
	return point, consumed, ok
}

// PrefixRangeFor returns the keyspace range a supported shorter leading prefix
// (1..Arity-1) maps to. It reports a *MapperError for a full-length or
// unsupported prefix.
func (m *NativeMapper) PrefixRangeFor(values []Scalar) (KeyRange, error) {
	l := len(values)
	if l < 1 || l >= m.arity {
		return KeyRange{}, &MapperError{Reason: "not a shorter leading prefix"}
	}
	if !m.prefixes.Contains(l) {
		return KeyRange{}, &MapperError{Reason: "unsupported prefix length"}
	}
	if err := m.Admits(l, values); err != nil {
		return KeyRange{}, err
	}
	return KeyRange{Start: KeyspacePoint{}, End: KeyspaceEnd{Max: true}}, nil
}

// mapFullKey hashes one canonical tuple into its bucket-start point. The common
// path stays on a 256-byte stack buffer; only unusually large placement keys
// allocate while preserving exactly the same bytes and result.
func (m *NativeMapper) mapFullKey(values []Scalar) (KeyspacePoint, error) {
	if len(values) != m.arity {
		return KeyspacePoint{}, ErrIncompleteShardKey
	}
	var storage [256]byte
	encoded, err := appendTuple(storage[:0], values)
	if err != nil {
		return KeyspacePoint{}, &ShardValueError{Reason: "value is not an encodable placement scalar"}
	}
	_, point, _ := VirtualBucketForHash(xxhash.Sum64(encoded), m.bucketBits)
	return point, nil
}
