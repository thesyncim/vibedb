package distribution

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

// NativeMapper is a dependency-free reference mapper over the fixed 8-byte
// keyspace. Each shard-key component owns a contiguous byte segment of the
// keyspace; a component's canonical scalar bytes are folded with FNV-1a into
// its segment. A full key of Arity components fills every segment and maps to
// one point; a supported shorter leading prefix fills only its leading
// segments and maps to the keyspace range that every completion of that prefix
// falls within, so a full-key point always lies inside its own leading-prefix
// range. The mapping is deterministic and is not a Vitess mapping. It exists so
// routing can be exercised end-to-end without any external dependency.
type NativeMapper struct {
	arity    int
	prefixes PrefixSet
	bounds   [KeyspaceWidth + 1]int // component i owns bytes [bounds[i], bounds[i+1])
}

// NewNativeMapper returns a native mapper over arity components (1..8),
// supporting every leading prefix length 1..arity. It panics for an arity
// outside 1..8, since a component needs at least one of the eight keyspace
// bytes.
func NewNativeMapper(arity int) *NativeMapper {
	if arity < 1 || arity > KeyspaceWidth {
		panic("distribution: NativeMapper arity must be in 1..8")
	}
	m := &NativeMapper{arity: arity}
	for l := 1; l <= arity; l++ {
		m.prefixes |= 1 << uint(l)
	}
	base := KeyspaceWidth / arity
	rem := KeyspaceWidth % arity
	off := 0
	for i := 0; i < arity; i++ {
		w := base
		if i < rem {
			w++
		}
		off += w
		m.bounds[i+1] = off
	}
	return m
}

// Arity reports the component count of a full key.
func (m *NativeMapper) Arity() int { return m.arity }

// SupportedPrefixes reports the supported leading prefix lengths, 1..Arity.
func (m *NativeMapper) SupportedPrefixes() PrefixSet { return m.prefixes }

// Version reports the frozen native mapping revision.
func (m *NativeMapper) Version() MapperVersion { return NativeMapperVersion }

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
	var buf [64]byte
	for i := range values {
		if _, err := appendScalarV1(buf[:0], values[i]); err != nil {
			return &ShardValueError{Reason: "value is not an encodable placement scalar"}
		}
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
	p, full, err := m.mapToPoint(values)
	if err != nil {
		return DestinationSet{}, err
	}
	if full {
		pointScratch = append(pointScratch[:0], p)
		return DestinationSet{Points: pointScratch}, nil
	}
	rangeScratch = append(rangeScratch[:0], KeyRange{
		Start: p, End: successorEnd(p, m.bounds[len(values)]),
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
	p, _, err := m.mapToPoint(values)
	return p, err
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
	p, _, err := m.mapToPoint(values)
	if err != nil {
		return KeyRange{}, err
	}
	return KeyRange{Start: p, End: successorEnd(p, m.bounds[l])}, nil
}

// mapToPoint fills the leading segments of a point from values' canonical bytes
// and reports whether values are a full key. Trailing segments stay zero, which
// makes the returned point double as a prefix range start.
func (m *NativeMapper) mapToPoint(values []Scalar) (KeyspacePoint, bool, error) {
	var p KeyspacePoint
	var buf [64]byte
	for i := range values {
		b, err := appendScalarV1(buf[:0], values[i])
		if err != nil {
			return KeyspacePoint{}, false, &ShardValueError{Reason: "value is not an encodable placement scalar"}
		}
		h := fnv1a64(b)
		lo, hi := m.bounds[i], m.bounds[i+1]
		w := hi - lo
		for j := 0; j < w; j++ {
			p[lo+j] = byte(h >> uint(8*(w-1-j)))
		}
	}
	return p, len(values) == m.arity, nil
}

// successorEnd returns the exclusive end just past every point sharing the
// width-byte leading region of p (whose trailing bytes are zero): the region
// incremented by one, or Max when the region is all ones.
func successorEnd(p KeyspacePoint, width int) KeyspaceEnd {
	var e KeyspacePoint
	copy(e[:width], p[:width])
	for i := width - 1; i >= 0; i-- {
		e[i]++
		if e[i] != 0 {
			return KeyspaceEnd{Point: e}
		}
	}
	return KeyspaceEnd{Max: true}
}

// fnv1a64 is the 64-bit FNV-1a hash, inlined so component folding stays
// dependency-free and allocation-free.
func fnv1a64(b []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}
