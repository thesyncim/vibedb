package vitessroute

import "github.com/thesyncim/vibedb/distribution"

// Frozen mapping revisions for this compatibility profile. A mapper version is
// placement identity; it must not change without a regenerated differential
// proof against a newly pinned upstream release.
const (
	// XXHashMapperVersion is the frozen revision of the xxhash mapping.
	XXHashMapperVersion distribution.MapperVersion = 1
	// MultiColMapperVersion is the frozen revision of the multicol mapping.
	MultiColMapperVersion distribution.MapperVersion = 1
)

// XXHashMapper is a distribution.Mapper reproducing the Vitess xxhash Vindex
// over the fixed 8-byte keyspace. It has arity 1 and supports only the full
// single-column key, mapping it to one keyspace point.
type XXHashMapper struct{}

// Compile-time proof that XXHashMapper satisfies the distribution contract.
var _ distribution.Mapper = (*XXHashMapper)(nil)

// NewXXHashMapper returns an xxhash mapper.
func NewXXHashMapper() *XXHashMapper { return &XXHashMapper{} }

// Arity reports the single shard-key column of a full xxhash key.
func (*XXHashMapper) Arity() int { return 1 }

// SupportedPrefixes reports that only the full single-column key is mappable.
func (*XXHashMapper) SupportedPrefixes() distribution.PrefixSet {
	return distribution.NewPrefixSet(1)
}

// Version reports the frozen xxhash mapping revision.
func (*XXHashMapper) Version() distribution.MapperVersion { return XXHashMapperVersion }

// Admits reports whether values form an acceptable prefix of length prefixLen.
// It returns ErrIncompleteShardKey on a length mismatch, a *MapperError
// (matching ErrUnsupportedMapper) for any length other than one, and a
// *ShardValueError (matching ErrInvalidShardValue) for a value the upstream
// input serialization rejects.
func (*XXHashMapper) Admits(prefixLen int, values []distribution.Scalar) error {
	if len(values) != prefixLen {
		return distribution.ErrIncompleteShardKey
	}
	if prefixLen != 1 {
		return &distribution.MapperError{Reason: "xxhash mapper supports only a single-column full key"}
	}
	_, err := vindexBytes(values[0])
	return err
}

// MapPrefix maps the full single-column key to its keyspace point. It validates
// the prefix itself, so it is safe to call without a preceding Admits.
func (*XXHashMapper) MapPrefix(values []distribution.Scalar) (distribution.DestinationSet, error) {
	if len(values) != 1 {
		if len(values) < 1 {
			return distribution.DestinationSet{}, distribution.ErrIncompleteShardKey
		}
		return distribution.DestinationSet{}, &distribution.MapperError{Reason: "xxhash mapper supports only a single-column full key"}
	}
	p, err := xxhashPoint(values[0])
	if err != nil {
		return distribution.DestinationSet{}, err
	}
	return distribution.DestinationSet{Points: []distribution.KeyspacePoint{p}}, nil
}

// MultiColMapper is a distribution.Mapper reproducing the Vitess multicol Vindex
// over the fixed 8-byte keyspace with xxhash sub-vindexes. Its arity is the
// configured column count; it supports every leading prefix length 1..arity. A
// full key maps to one point; a shorter leading prefix maps to a keyspace range.
type MultiColMapper struct {
	columnBytes []int
	prefixes    distribution.PrefixSet
}

// Compile-time proof that MultiColMapper satisfies the distribution contract.
var _ distribution.Mapper = (*MultiColMapper)(nil)

// NewMultiColMapper builds a multicol mapper over the given per-column byte
// widths, one xxhash sub-vindex per column. The widths must number between one
// and eight and total exactly the 8-byte keyspace width with every column
// between one and eight bytes; otherwise it returns a *ConfigError (matching
// ErrUnsupportedVSchema). The slice is copied and never retained.
func NewMultiColMapper(columnBytes []int) (*MultiColMapper, error) {
	if len(columnBytes) < 1 || len(columnBytes) > distribution.KeyspaceWidth {
		return nil, &ConfigError{Reason: "multicol requires between 1 and 8 columns"}
	}
	widths := make([]int, len(columnBytes))
	copy(widths, columnBytes)
	total := 0
	for _, w := range widths {
		if w < 1 || w > distribution.KeyspaceWidth {
			return nil, &ConfigError{Reason: "each multicol column must be between 1 and 8 bytes"}
		}
		total += w
	}
	if total != distribution.KeyspaceWidth {
		return nil, &ConfigError{Reason: "multicol column_bytes must total exactly 8 bytes"}
	}

	lengths := make([]int, len(widths))
	for i := range widths {
		lengths[i] = i + 1
	}
	return &MultiColMapper{columnBytes: widths, prefixes: distribution.NewPrefixSet(lengths...)}, nil
}

// Arity reports the configured multicol column count.
func (m *MultiColMapper) Arity() int { return len(m.columnBytes) }

// SupportedPrefixes reports the supported leading prefix lengths, 1..Arity.
func (m *MultiColMapper) SupportedPrefixes() distribution.PrefixSet { return m.prefixes }

// Version reports the frozen multicol mapping revision.
func (*MultiColMapper) Version() distribution.MapperVersion { return MultiColMapperVersion }

// Admits reports whether values form an acceptable leading prefix of length
// prefixLen. It returns ErrIncompleteShardKey on a length mismatch, a
// *MapperError (matching ErrUnsupportedMapper) for an unsupported length, and a
// *ShardValueError (matching ErrInvalidShardValue) for a rejected value.
func (m *MultiColMapper) Admits(prefixLen int, values []distribution.Scalar) error {
	if len(values) != prefixLen {
		return distribution.ErrIncompleteShardKey
	}
	if !m.prefixes.Contains(prefixLen) {
		return &distribution.MapperError{Reason: "unsupported prefix length"}
	}
	for i := range values {
		if _, err := vindexBytes(values[i]); err != nil {
			return err
		}
	}
	return nil
}

// MapPrefix maps a supported leading prefix to its destinations: a full key to
// one point, a shorter leading prefix to a range. It validates the prefix
// itself, so it is safe to call without a preceding Admits.
func (m *MultiColMapper) MapPrefix(values []distribution.Scalar) (distribution.DestinationSet, error) {
	if len(values) < 1 {
		return distribution.DestinationSet{}, distribution.ErrIncompleteShardKey
	}
	if !m.prefixes.Contains(len(values)) {
		return distribution.DestinationSet{}, &distribution.MapperError{Reason: "unsupported prefix length"}
	}
	return mapMultiCol(m.columnBytes, values)
}
