package distribution

import (
	"bytes"
	"slices"
)

// DomainKind classifies a shard-key ordinal's bound value domain.
type DomainKind uint8

const (
	// DomainUnknown means the ordinal is not narrowed; it must never be used to
	// narrow routing and forces a scatter subject to admission.
	DomainUnknown DomainKind = iota
	// DomainEmpty means no value satisfies the ordinal; it yields RouteEmpty
	// with no mapper or network work.
	DomainEmpty
	// DomainFinite means the ordinal is bound to a finite, canonical-byte
	// deduplicated set of values.
	DomainFinite
)

// ValueDomain is one shard-key ordinal's bound domain. For DomainFinite, Values
// holds the canonical-byte deduplicated satisfying values ordered by those
// bytes; the other kinds carry no values.
type ValueDomain struct {
	Kind   DomainKind
	Values []Scalar
}

// BoundConstraints indexes one ValueDomain per shard-key ordinal:
// BoundConstraints[i] constrains placement column i.
type BoundConstraints []ValueDomain

// UnknownDomain returns the DomainUnknown domain: routing may not be narrowed.
func UnknownDomain() ValueDomain { return ValueDomain{Kind: DomainUnknown} }

// EmptyDomain returns the DomainEmpty domain: no value satisfies the ordinal.
func EmptyDomain() ValueDomain { return ValueDomain{Kind: DomainEmpty} }

// FiniteDomain returns a DomainFinite domain over values, deduplicated by their
// frozen canonical scalar bytes and ordered by those bytes. No values yields
// EmptyDomain, matching an empty membership set. It reports a *ShardValueError
// (matching ErrInvalidShardValue) for a value that is not an encodable
// placement scalar.
func FiniteDomain(values ...Scalar) (ValueDomain, error) {
	var b ConstraintBuilder
	b.codec = V1
	if err := b.AddMembership(values); err != nil {
		return ValueDomain{}, err
	}
	return b.Domain(), nil
}

// ConstraintBuilder intersects the equality, membership, and unbindable
// predicates over one shard-key ordinal into a canonical ValueDomain,
// comparing and deduplicating by frozen canonical scalar bytes. It reuses
// internal buffers across ordinals via Reset and holds no maps; it is not safe
// for concurrent use. Its result is independent of predicate order: a proven
// contradiction yields DomainEmpty, an unbindable value preserves
// DomainUnknown and never narrows routing, and finiteness only comes from
// bindable predicates.
type ConstraintBuilder struct {
	codec      TupleCodec
	running    canonSet // deduped intersection of the bindable predicates seen
	incoming   canonSet // scratch for the predicate being intersected
	haveFinite bool     // at least one bindable predicate has been intersected
	empty      bool     // bindable predicates already contradict
	unbindable bool     // a value could not be bound safely
}

// NewConstraintBuilder returns a builder that encodes canonical bytes with the
// frozen version 1 tuple codec.
func NewConstraintBuilder() *ConstraintBuilder {
	return &ConstraintBuilder{codec: V1}
}

// Reset clears accumulated predicates, reusing buffers for the next ordinal.
func (b *ConstraintBuilder) Reset() {
	if b.codec == nil {
		b.codec = V1
	}
	b.running.reset()
	b.incoming.reset()
	b.haveFinite = false
	b.empty = false
	b.unbindable = false
}

// AddEquality intersects the equality predicate ordinal == v. It reports a
// *ShardValueError for a value that is not an encodable placement scalar.
func (b *ConstraintBuilder) AddEquality(v Scalar) error {
	b.incoming.reset()
	if err := b.incoming.add(b.codec, v); err != nil {
		return err
	}
	b.incoming.sortDedup()
	b.mergePredicate()
	return nil
}

// AddMembership intersects the membership predicate ordinal IN values. An empty
// values slice is an unsatisfiable predicate and forces DomainEmpty. It reports
// a *ShardValueError for any value that is not an encodable placement scalar.
func (b *ConstraintBuilder) AddMembership(values []Scalar) error {
	b.incoming.reset()
	for i := range values {
		if err := b.incoming.add(b.codec, values[i]); err != nil {
			return err
		}
	}
	b.incoming.sortDedup()
	b.mergePredicate()
	return nil
}

// AddUnbindable records that a predicate references a value that cannot be
// bound safely (for example a not-yet-available join-fed set). It never
// narrows routing: unless a bindable contradiction is proven, the ordinal
// stays DomainUnknown.
func (b *ConstraintBuilder) AddUnbindable() { b.unbindable = true }

// mergePredicate folds b.incoming (already sorted and deduplicated) into the
// running bindable intersection.
func (b *ConstraintBuilder) mergePredicate() {
	if b.empty {
		return
	}
	if !b.haveFinite {
		b.running.copyFrom(&b.incoming)
		b.haveFinite = true
	} else {
		b.running.intersect(&b.incoming)
	}
	if len(b.running.items) == 0 {
		b.empty = true
	}
}

// Domain reduces the accumulated predicates to a canonical ValueDomain. A
// proven bindable contradiction wins over an unbindable value, so a provably
// empty ordinal still short-circuits to RouteEmpty.
func (b *ConstraintBuilder) Domain() ValueDomain {
	if b.empty {
		return EmptyDomain()
	}
	if b.unbindable || !b.haveFinite {
		return UnknownDomain()
	}
	values := make([]Scalar, len(b.running.items))
	for i := range b.running.items {
		values[i] = b.running.items[i].scalar
	}
	return ValueDomain{Kind: DomainFinite, Values: values}
}

// canonItem indexes one value's canonical scalar bytes inside a canonSet arena.
// Offsets, not pointers, survive arena reallocation.
type canonItem struct {
	scalar     Scalar
	start, end int
}

// canonSet is a set of scalars keyed by frozen canonical scalar bytes, backed
// by a reusable byte arena and index slice so deduplication and intersection
// avoid maps and per-call allocation. After sortDedup its items are sorted by
// canonical bytes with no duplicates, which is what makes both intersection and
// finite-domain output independent of input order and of equal-valued
// spellings.
type canonSet struct {
	arena []byte
	items []canonItem
}

func (s *canonSet) reset() {
	s.arena = s.arena[:0]
	s.items = s.items[:0]
}

// bytesAt returns the canonical bytes of item i.
func (s *canonSet) bytesAt(i int) []byte {
	return s.arena[s.items[i].start:s.items[i].end]
}

// add appends value, encoding its canonical scalar bytes into the arena via the
// frozen codec. It reports a *ShardValueError (matching ErrInvalidShardValue)
// for a value outside the closed placement scalar set.
func (s *canonSet) add(codec TupleCodec, value Scalar) error {
	start := len(s.arena)
	b, err := codec.AppendScalar(s.arena, value)
	if err != nil {
		s.arena = s.arena[:start]
		return &ShardValueError{Reason: "value is not an encodable placement scalar"}
	}
	s.arena = b
	s.items = append(s.items, canonItem{scalar: value, start: start, end: len(s.arena)})
	return nil
}

// sortDedup orders items by canonical bytes and drops adjacent duplicates. A
// set of one or zero items is already canonical and skips the sort, which keeps
// the single-value route free of the sort closure allocation.
func (s *canonSet) sortDedup() {
	if len(s.items) <= 1 {
		return
	}
	slices.SortFunc(s.items, func(a, b canonItem) int {
		return bytes.Compare(s.arena[a.start:a.end], s.arena[b.start:b.end])
	})
	w := 1
	for i := 1; i < len(s.items); i++ {
		if !bytes.Equal(s.bytesAt(i), s.bytesAt(w-1)) {
			s.items[w] = s.items[i]
			w++
		}
	}
	s.items = s.items[:w]
}

// copyFrom replaces s with a byte-identical copy of other, reusing s's buffers.
// Because the arena is copied verbatim, other's offsets stay valid against s.
func (s *canonSet) copyFrom(other *canonSet) {
	s.arena = append(s.arena[:0], other.arena...)
	s.items = append(s.items[:0], other.items...)
}

// intersect keeps only the items whose canonical bytes also appear in other.
// Both sets must be sorted and deduplicated; s keeps its own arena and order.
func (s *canonSet) intersect(other *canonSet) {
	w, i, j := 0, 0, 0
	for i < len(s.items) && j < len(other.items) {
		switch bytes.Compare(s.bytesAt(i), other.bytesAt(j)) {
		case -1:
			i++
		case 1:
			j++
		default:
			s.items[w] = s.items[i]
			w++
			i++
			j++
		}
	}
	s.items = s.items[:w]
}
