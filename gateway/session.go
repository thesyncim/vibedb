package gateway

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
)

// Position is the shard wire's logical applied-position identity. It is an
// alias so a gateway session vector can be passed to the shard codec without a
// second representation or conversion.
type Position = shardservice.Position

// MaxSessionVectorEntries is the hard cardinality bound of a SessionVector.
const MaxSessionVectorEntries = 64

var (
	// ErrSessionVectorOverflow reports a vector whose distinct source count
	// cannot fit within MaxSessionVectorEntries.
	ErrSessionVectorOverflow = errors.New("gateway: session vector entry limit exceeded")
	// ErrPositionLineageRequired reports two positions for the same named shard
	// but different log identities. Their indexes are incomparable without a
	// durable lineage or transition proof.
	ErrPositionLineageRequired = errors.New("gateway: position lineage proof required")
)

// SessionVectorLimitError reports the bounded cardinality a construction or
// merge would exceed.
type SessionVectorLimitError struct {
	Limit int
	Count int
}

func (e *SessionVectorLimitError) Error() string {
	return fmt.Sprintf("gateway: session vector entry limit exceeded: %d entries exceed %d", e.Count, e.Limit)
}

func (e *SessionVectorLimitError) Unwrap() error { return ErrSessionVectorOverflow }

// PositionLineageError identifies the source whose log identity changed.
// Numerically comparing LeftLogID and RightLogID indexes would be unsafe.
type PositionLineageError struct {
	Distribution distribution.DistributionName
	Shard        distribution.ShardID
	LeftLogID    [16]byte
	RightLogID   [16]byte
}

func (e *PositionLineageError) Error() string {
	return fmt.Sprintf(
		"gateway: position lineage proof required for distribution %q shard %q: log %x cannot be compared with %x",
		e.Distribution, e.Shard, e.LeftLogID, e.RightLogID)
}

func (e *PositionLineageError) Unwrap() error { return ErrPositionLineageRequired }

// SessionVector is a bounded immutable set of per-source minimum positions.
// Entries are sorted by (Distribution, Shard), and at most one entry exists for
// each such source. Repeated positions from the same LogID collapse to the
// greatest index. Reusing a source name with another LogID fails closed because
// indexes from different logs are unrelated.
//
// SessionVector deliberately carries no routing-layout provenance and is not
// attached to Query or Result yet. A split or merge must supply a certified
// lineage translation plus a layout fence before a position may move to a new
// shard identity; this standalone type never claims that continuity.
type SessionVector struct {
	positions []Position
}

// NewSessionVector validates, sorts, deduplicates, and defensively copies
// positions. The input count itself is bounded so construction work cannot be
// amplified by a large list of duplicates.
func NewSessionVector(positions ...Position) (SessionVector, error) {
	if len(positions) > MaxSessionVectorEntries {
		return SessionVector{}, sessionVectorOverflow(len(positions))
	}
	if len(positions) == 0 {
		return SessionVector{}, nil
	}

	ordered := slices.Clone(positions)
	for i := range ordered {
		if err := ordered[i].Validate(); err != nil {
			return SessionVector{}, fmt.Errorf("gateway: session position %d: %w", i, err)
		}
	}
	slices.SortFunc(ordered, comparePositionSource)

	out := make([]Position, 0, len(ordered))
	for _, p := range ordered {
		if len(out) == 0 || !out[len(out)-1].SameSource(p) {
			out = append(out, p)
			continue
		}
		last := &out[len(out)-1]
		if last.LogID != p.LogID {
			return SessionVector{}, positionLineageError(*last, p)
		}
		if p.Index > last.Index {
			last.Index = p.Index
		}
	}
	return SessionVector{positions: out}, nil
}

// Len reports the number of distinct source positions.
func (v SessionVector) Len() int { return len(v.positions) }

// Positions returns a sorted defensive copy of the vector entries.
func (v SessionVector) Positions() []Position { return slices.Clone(v.positions) }

// PositionFor returns the minimum for one exact named source, if present. The
// returned LogID remains part of its identity and must not be discarded.
func (v SessionVector) PositionFor(distributionName distribution.DistributionName, shard distribution.ShardID) (Position, bool) {
	i, ok := sort.Find(len(v.positions), func(i int) int {
		p := v.positions[i]
		switch {
		case p.Distribution < distributionName:
			return 1
		case p.Distribution > distributionName:
			return -1
		case p.Shard < shard:
			return 1
		case p.Shard > shard:
			return -1
		default:
			return 0
		}
	})
	if !ok {
		return Position{}, false
	}
	return v.positions[i], true
}

// With returns a new vector containing p. The receiver is unchanged.
func (v SessionVector) With(p Position) (SessionVector, error) {
	one, err := NewSessionVector(p)
	if err != nil {
		return SessionVector{}, err
	}
	return v.Merge(one)
}

// Merge returns the elementwise maximum of two vectors for identical log
// identities. It is linear in their bounded cardinalities and never mutates
// either input. A changed LogID or an output above the hard cap fails closed.
func (v SessionVector) Merge(other SessionVector) (SessionVector, error) {
	if len(v.positions) > MaxSessionVectorEntries {
		return SessionVector{}, sessionVectorOverflow(len(v.positions))
	}
	if len(other.positions) > MaxSessionVectorEntries {
		return SessionVector{}, sessionVectorOverflow(len(other.positions))
	}

	out := make([]Position, 0, min(MaxSessionVectorEntries, len(v.positions)+len(other.positions)))
	i, j := 0, 0
	for i < len(v.positions) || j < len(other.positions) {
		if i == len(v.positions) {
			if len(out)+len(other.positions)-j > MaxSessionVectorEntries {
				return SessionVector{}, sessionVectorOverflow(len(out) + len(other.positions) - j)
			}
			out = append(out, other.positions[j:]...)
			break
		}
		if j == len(other.positions) {
			if len(out)+len(v.positions)-i > MaxSessionVectorEntries {
				return SessionVector{}, sessionVectorOverflow(len(out) + len(v.positions) - i)
			}
			out = append(out, v.positions[i:]...)
			break
		}

		left, right := v.positions[i], other.positions[j]
		switch comparePositionSource(left, right) {
		case -1:
			if len(out) == MaxSessionVectorEntries {
				return SessionVector{}, sessionVectorOverflow(len(out) + 1)
			}
			out = append(out, left)
			i++
		case 1:
			if len(out) == MaxSessionVectorEntries {
				return SessionVector{}, sessionVectorOverflow(len(out) + 1)
			}
			out = append(out, right)
			j++
		default:
			if left.LogID != right.LogID {
				return SessionVector{}, positionLineageError(left, right)
			}
			if right.Index > left.Index {
				left.Index = right.Index
			}
			if len(out) == MaxSessionVectorEntries {
				return SessionVector{}, sessionVectorOverflow(len(out) + 1)
			}
			out = append(out, left)
			i++
			j++
		}
	}
	return SessionVector{positions: out}, nil
}

func comparePositionSource(left, right Position) int {
	switch {
	case left.Distribution < right.Distribution:
		return -1
	case left.Distribution > right.Distribution:
		return 1
	case left.Shard < right.Shard:
		return -1
	case left.Shard > right.Shard:
		return 1
	default:
		return 0
	}
}

func positionLineageError(left, right Position) error {
	return &PositionLineageError{
		Distribution: left.Distribution,
		Shard:        left.Shard,
		LeftLogID:    left.LogID,
		RightLogID:   right.LogID,
	}
}

func sessionVectorOverflow(count int) error {
	return &SessionVectorLimitError{Limit: MaxSessionVectorEntries, Count: count}
}
