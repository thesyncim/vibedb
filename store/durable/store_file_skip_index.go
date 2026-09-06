package durable

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

// DataSkippingOp is one scalar comparison eligible for compact stripe
// min/max pruning. It is intentionally independent of SQL/query packages.
type DataSkippingOp uint8

const (
	DataSkippingEqual DataSkippingOp = iota
	DataSkippingLess
	DataSkippingLessEqual
	DataSkippingGreater
	DataSkippingGreaterEqual
)

// DataSkippingFilterMemoryBytes is the complete retained byte high-water for
// eight lower/upper extrema plus one fixed ordered-key probe. The probe has
// four extrema capacities so bounded source spellings cannot trigger a
// transient growth allocation before an oversized term is declined.
const DataSkippingFilterMemoryBytes int64 = (2*storeio.PageCatalogMaxSkipIndexes + 4) * storeio.CompactPrimarySummaryMaxKeyBytes

// DataSkippingPredicate is one planning-time path/literal comparison. Path is
// used only while matching the immutable collection catalog; the scan filter
// stores a byte-sized summary ordinal and canonical ordered scalar bytes.
type DataSkippingPredicate struct {
	Path  string
	Op    DataSkippingOp
	Value vibejson.Index
}

type dataSkippingConstraint struct {
	ordinal        uint8
	hasLower       bool
	hasUpper       bool
	lowerInclusive bool
	upperInclusive bool
	lower          []byte
	upper          []byte
}

// DataSkippingFilter is a reusable, single-scan compact-stripe filter. Its
// zero value is ready for CompileDataSkippingFilter. All scan-time decisions
// use ordinals and bytewise canonical terms; strings never enter the hot path.
type DataSkippingFilter struct {
	constraints    [storeio.PageCatalogMaxSkipIndexes]dataSkippingConstraint
	count          int
	summaryCount   int
	arena          []byte
	probe          []byte
	skippedRows    uint64
	skippedStripes uint64
	alwaysFalse    bool
}

// hasDataSkippingPath reports whether path is one of the collection's
// persisted skip-index paths. The catalog is normalized and sorted when the
// collection opens, so this is a bounded, allocation-free admission check
// used before a native integer scan. A nil or closed snapshot simply has no
// matching path.
func (s *Snapshot) hasDataSkippingPath(path string) bool {
	if s == nil || s.collection == nil || s.state == nil || path == "" {
		return false
	}
	_, found := slices.BinarySearch(s.collection.options.SkipIndexes, path)
	return found
}

// DataSkippingMetrics is the detached physical work avoided by one completed
// scan.
type DataSkippingMetrics struct {
	Rows    uint64
	Stripes uint64
}

// Metrics returns the last scan's detached skip counters.
func (f *DataSkippingFilter) Metrics() DataSkippingMetrics {
	if f == nil {
		return DataSkippingMetrics{}
	}
	return DataSkippingMetrics{Rows: f.skippedRows, Stripes: f.skippedStripes}
}

// CompileDataSkippingFilter matches planning predicates to the persisted skip
// catalog and canonicalizes their scalar bounds. false is a cost-free decline:
// no declared path was usable. dst retains its small extrema buffers for warm
// executions.
func (s *Snapshot) CompileDataSkippingFilter(
	dst *DataSkippingFilter,
	predicates []DataSkippingPredicate,
) (bool, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return false, ErrClosed
	}
	if dst == nil || len(s.collection.options.SkipIndexes) == 0 {
		return false, nil
	}
	for i := 0; i < dst.count; i++ {
		dst.constraints[i].hasLower = false
		dst.constraints[i].hasUpper = false
	}
	dst.count = 0
	dst.summaryCount = len(s.collection.options.SkipIndexes)
	dst.skippedRows = 0
	dst.skippedStripes = 0
	dst.alwaysFalse = false

	for _, predicate := range predicates {
		if predicate.Op > DataSkippingGreaterEqual {
			continue
		}
		ordinal, found := slices.BinarySearch(
			s.collection.options.SkipIndexes, predicate.Path,
		)
		if !found || ordinal > int(^uint8(0)) {
			continue
		}
		component, scalar := primaryExactComponent(predicate.Value.Root().Raw())
		if !scalar || len(component.JSON) >
			2*storeio.CompactPrimarySummaryMaxKeyBytes {
			continue
		}
		var components [1]storeio.IndexTermComponent
		components[0] = component
		termBytes, sized := storeio.IndexTermKeyEncodedSize(components[:])
		if !sized || termBytes > storeio.CompactPrimarySummaryMaxKeyBytes {
			continue
		}
		dst.ensureArena()
		term, ok := storeio.AppendIndexTermKey(dst.probe[:0], components[:])
		if !ok || len(term) != termBytes {
			continue
		}
		dst.probe = term
		constraint := dst.constraint(uint8(ordinal))
		if constraint == nil {
			continue
		}
		switch predicate.Op {
		case DataSkippingEqual:
			mergeDataSkippingLower(constraint, term, true)
			mergeDataSkippingUpper(constraint, term, true)
		case DataSkippingLess:
			mergeDataSkippingUpper(constraint, term, false)
		case DataSkippingLessEqual:
			mergeDataSkippingUpper(constraint, term, true)
		case DataSkippingGreater:
			mergeDataSkippingLower(constraint, term, false)
		case DataSkippingGreaterEqual:
			mergeDataSkippingLower(constraint, term, true)
		default:
			continue
		}
	}
	for i := 0; i < dst.count; i++ {
		constraint := &dst.constraints[i]
		if !constraint.hasLower || !constraint.hasUpper {
			continue
		}
		order := bytes.Compare(constraint.lower, constraint.upper)
		if order > 0 || order == 0 &&
			(!constraint.lowerInclusive || !constraint.upperInclusive) {
			dst.alwaysFalse = true
		}
	}
	return dst.count != 0, nil
}

func (f *DataSkippingFilter) ensureArena() {
	if f == nil || cap(f.arena) == int(DataSkippingFilterMemoryBytes) &&
		cap(f.probe) == 4*storeio.CompactPrimarySummaryMaxKeyBytes {
		return
	}
	f.arena = make([]byte, int(DataSkippingFilterMemoryBytes))
	for i := range f.constraints {
		start := 2 * i * storeio.CompactPrimarySummaryMaxKeyBytes
		middle := start + storeio.CompactPrimarySummaryMaxKeyBytes
		end := middle + storeio.CompactPrimarySummaryMaxKeyBytes
		f.constraints[i].lower = f.arena[start:start:middle]
		f.constraints[i].upper = f.arena[middle:middle:end]
	}
	probe := 2 * storeio.PageCatalogMaxSkipIndexes *
		storeio.CompactPrimarySummaryMaxKeyBytes
	f.probe = f.arena[probe:probe:cap(f.arena)]
}

func (f *DataSkippingFilter) constraint(ordinal uint8) *dataSkippingConstraint {
	for i := 0; i < f.count; i++ {
		if f.constraints[i].ordinal == ordinal {
			return &f.constraints[i]
		}
	}
	if f.count == len(f.constraints) {
		return nil
	}
	constraint := &f.constraints[f.count]
	if cap(constraint.lower) != storeio.CompactPrimarySummaryMaxKeyBytes ||
		cap(constraint.upper) != storeio.CompactPrimarySummaryMaxKeyBytes {
		return nil
	}
	constraint.ordinal = ordinal
	constraint.hasLower = false
	constraint.hasUpper = false
	f.count++
	return constraint
}

func mergeDataSkippingLower(
	constraint *dataSkippingConstraint,
	term []byte,
	inclusive bool,
) {
	if !constraint.hasLower {
		constraint.lower = append(constraint.lower[:0], term...)
		constraint.hasLower = true
		constraint.lowerInclusive = inclusive
		return
	}
	order := bytes.Compare(term, constraint.lower)
	if order > 0 {
		constraint.lower = append(constraint.lower[:0], term...)
		constraint.lowerInclusive = inclusive
	} else if order == 0 {
		constraint.lowerInclusive = constraint.lowerInclusive && inclusive
	}
}

func mergeDataSkippingUpper(
	constraint *dataSkippingConstraint,
	term []byte,
	inclusive bool,
) {
	if !constraint.hasUpper {
		constraint.upper = append(constraint.upper[:0], term...)
		constraint.hasUpper = true
		constraint.upperInclusive = inclusive
		return
	}
	order := bytes.Compare(term, constraint.upper)
	if order < 0 {
		constraint.upper = append(constraint.upper[:0], term...)
		constraint.upperInclusive = inclusive
	} else if order == 0 {
		constraint.upperInclusive = constraint.upperInclusive && inclusive
	}
}

func (f *DataSkippingFilter) mayContain(
	stripe *storeio.CompactPrimaryStripeView,
) (bool, error) {
	if f == nil || stripe == nil {
		return true, nil
	}
	if stripe.SummaryCount() != f.summaryCount {
		return false, fmt.Errorf(
			"%w: compact summary catalog count=%d want=%d",
			storeio.ErrCommonPrimaryLeafCorrupt,
			stripe.SummaryCount(), f.summaryCount,
		)
	}
	if f.alwaysFalse {
		return false, nil
	}
	for i := 0; i < f.count; i++ {
		constraint := &f.constraints[i]
		minimum, maximum, valid := stripe.Summary(int(constraint.ordinal))
		if !valid {
			continue
		}
		if constraint.hasLower {
			order := bytes.Compare(maximum, constraint.lower)
			if order < 0 || order == 0 && !constraint.lowerInclusive {
				return false, nil
			}
		}
		if constraint.hasUpper {
			order := bytes.Compare(minimum, constraint.upper)
			if order > 0 || order == 0 && !constraint.upperInclusive {
				return false, nil
			}
		}
	}
	return true, nil
}
