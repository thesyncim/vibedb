package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// The token filter and field probe consume compact VCS1 rows through
// the token view without rendering non-target holes; both degrade per row —
// never per store — to the render path for trivial rows, container targets,
// overflow chains, and non-unified leaves.

// EqFilter is a reusable equality predicate for Snapshot.FilterEqCount. Its
// constructor selects canonical-spelling or exact scalar-value semantics. It
// is single-consumer; reuse across scans and snapshots of the same collection
// amortizes all resolution scratch.
type EqFilter struct {
	inner *storeio.UnifiedEqFilter
}

// IntegerOrder is the durable API spelling of the exact signed integer
// ordering accepted by Snapshot.FilterIntegerOrderCount.
type IntegerOrder = storeio.UnifiedIntegerOrder

// IntegerInterval is the normalized half-open signed interval consumed by
// Snapshot.FilterIntegerIntervalCount. Lower is inclusive and Upper is
// exclusive unless UpperUnbounded is true.
type IntegerInterval = storeio.UnifiedIntegerInterval

const (
	IntegerLess         = storeio.UnifiedIntegerLess
	IntegerLessEqual    = storeio.UnifiedIntegerLessEqual
	IntegerGreater      = storeio.UnifiedIntegerGreater
	IntegerGreaterEqual = storeio.UnifiedIntegerGreaterEqual
)

// IntegerOrderFilter is reusable state for one strict FOR ordered COUNT.
// Unsupported leaves decline the whole scan so callers can use the generic
// executor without exposing a partial result.
type IntegerOrderFilter struct {
	inner *storeio.UnifiedIntegerOrderFilter
}

// IntegerIntervalFilter is reusable state for one strict FOR interval COUNT.
// Unsupported leaves decline the whole scan so callers can use the generic
// executor without exposing a partial result.
type IntegerIntervalFilter struct {
	inner *storeio.UnifiedIntegerIntervalFilter
}

// IntegerExtremaFilter is reusable state for one strict FOR integer MIN/MAX
// scan. Unsupported leaves decline the complete scan atomically.
type IntegerExtremaFilter struct {
	inner *storeio.UnifiedIntegerExtremaFilter
}

// NewIntegerOrderFilter builds an exact integer ordering over a unified field
// path. The query layer is responsible for restricting its literal to int64.
func NewIntegerOrderFilter(
	path string, needle int64, op IntegerOrder,
) (*IntegerOrderFilter, error) {
	inner, err := storeio.NewUnifiedIntegerOrderFilter([]byte(path), needle, op)
	if err != nil {
		return nil, err
	}
	return &IntegerOrderFilter{inner: inner}, nil
}

// NewIntegerIntervalFilter builds an exact normalized signed interval over a
// unified field path. The query layer performs endpoint normalization before
// calling this constructor.
func NewIntegerIntervalFilter(
	path string, interval IntegerInterval,
) (*IntegerIntervalFilter, error) {
	inner, err := storeio.NewUnifiedIntegerIntervalFilter([]byte(path), interval)
	if err != nil {
		return nil, err
	}
	return &IntegerIntervalFilter{inner: inner}, nil
}

// NewIntegerExtremaFilter builds an exact integer MIN/MAX scan over a unified
// field path. The query layer restricts admission to one named numeric path.
func NewIntegerExtremaFilter(path string) (*IntegerExtremaFilter, error) {
	inner, err := storeio.NewUnifiedIntegerExtremaFilter([]byte(path))
	if err != nil {
		return nil, err
	}
	return &IntegerExtremaFilter{inner: inner}, nil
}

// NewEqFilter builds an equality filter over a "/a/b" field path and the
// JSON spelling of one comparand value (canonicalized internally).
func NewEqFilter(path string, needleJSON []byte) (*EqFilter, error) {
	inner, err := storeio.NewUnifiedEqFilter([]byte(path), needleJSON)
	if err != nil {
		return nil, err
	}
	return &EqFilter{inner: inner}, nil
}

// NewScalarEqFilter builds an equality filter with exact JSON scalar value
// semantics. It differs from NewEqFilter only for numbers: equivalent decimal
// spellings such as 1, 1.0, and 1e0 compare equal without binary rounding.
func NewScalarEqFilter(path string, needleJSON []byte) (*EqFilter, error) {
	inner, err := storeio.NewUnifiedScalarEqFilter([]byte(path), needleJSON)
	if err != nil {
		return nil, err
	}
	return &EqFilter{inner: inner}, nil
}

// FilterEqResult reports one filtered scan: rows matched, rows that took the
// render-then-filter fallback lane (reported, not hidden), and rows
// scanned in total.
type FilterEqResult struct {
	Matched  int
	Fallback int
	Scanned  int
}

// FilterIntegerOrderResult reports a strict ordered scan. Supported is false
// when any present target stream cannot answer exactly from compact FOR data;
// Matched and Scanned are then deliberately zero and must be discarded.
type FilterIntegerOrderResult struct {
	Matched   int
	Scanned   int
	Supported bool
}

// FilterIntegerIntervalResult reports one strict interval scan. Supported is
// false when any present target stream cannot answer exactly from compact FOR
// data; Matched and Scanned are then deliberately zero and must be discarded.
type FilterIntegerIntervalResult struct {
	Matched   int
	Scanned   int
	Supported bool
}

// FilterIntegerExtremaResult reports one strict integer MIN/MAX scan. Found
// is false when every resolved target is absent, which maps to SQL NULL.
// Min and Max are zero when Supported is false and must be discarded.
type FilterIntegerExtremaResult struct {
	Min, Max  int64
	Scanned   int
	Found     bool
	Supported bool
}

// FilterEqCount scans every live document and counts those whose value at the
// filter's path equals its needle under the filter constructor's semantics.
// Unified leaves evaluate rows from tokens; every row the
// token lane cannot decide — and every row of a non-unified leaf — renders
// into reused scratch and evaluates there. Overflow documents reassemble
// through the snapshot's reused chain buffer. A warmed filter scan allocates
// nothing.
func (s *Snapshot) FilterEqCount(f *EqFilter) (FilterEqResult, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return FilterEqResult{}, ErrClosed
	}
	if f == nil || f.inner == nil {
		return FilterEqResult{}, fmt.Errorf("vibedb: nil unified filter")
	}
	state := s.state
	catalogBounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.fileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	var cursor storeio.PrimaryGraphCursor
	if err := storeio.InitPrimaryGraphCursor(
		&cursor, s.collection.cache, state.root.PrimaryRoot,
		catalogBounds, leafBounds, nil, nil,
	); err != nil {
		return FilterEqResult{}, err
	}
	cursor.AdoptSpliceScratch(s.scanSpliceScratch)
	defer func() {
		s.scanSpliceScratch = cursor.ReleaseSpliceScratch()
		cursor.Close()
	}()
	f.inner.Reset()
	var progress storeio.UnifiedFilterProgress
	for {
		_, ref, err := cursor.FilterCountEq(f.inner, &progress)
		if err != nil {
			return FilterEqResult{}, err
		}
		if ref == (storeio.PageRef{}) {
			return FilterEqResult{
				Matched:  progress.Matched,
				Fallback: progress.Fallback,
				Scanned:  progress.Scanned,
			}, nil
		}
		// Overflow chains carry canonical bytes: reassemble into the
		// snapshot's reused buffer and evaluate through the render path.
		s.overflowScanValue, err = s.collection.appendPrimaryOverflowValue(
			s.overflowScanValue[:0], ref, leafBounds,
		)
		if err != nil {
			return FilterEqResult{}, err
		}
		matched, err := f.inner.EvalRendered(s.overflowScanValue)
		if err != nil {
			return FilterEqResult{}, err
		}
		if matched {
			progress.Matched++
		}
	}
}

// FilterIntegerOrderCount scans a snapshot using the strict FOR ordering lane.
// It never renders or falls back per row: unsupported compact leaves decline
// atomically, allowing the query executor to run the original predicate.
func (s *Snapshot) FilterIntegerOrderCount(
	f *IntegerOrderFilter,
) (FilterIntegerOrderResult, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return FilterIntegerOrderResult{}, ErrClosed
	}
	if f == nil || f.inner == nil {
		return FilterIntegerOrderResult{}, fmt.Errorf("vibedb: nil unified integer filter")
	}
	state := s.state
	catalogBounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.fileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	var cursor storeio.PrimaryGraphCursor
	if err := storeio.InitPrimaryGraphCursor(
		&cursor, s.collection.cache, state.root.PrimaryRoot,
		catalogBounds, leafBounds, nil, nil,
	); err != nil {
		return FilterIntegerOrderResult{}, err
	}
	defer cursor.Close()
	var progress storeio.UnifiedFilterProgress
	for {
		supported, err := cursor.FilterCountIntegerOrdered(f.inner, &progress)
		if err != nil {
			return FilterIntegerOrderResult{}, err
		}
		if !supported {
			return FilterIntegerOrderResult{}, nil
		}
		return FilterIntegerOrderResult{
			Matched: progress.Matched, Scanned: progress.Scanned, Supported: true,
		}, nil
	}
}

// FilterIntegerIntervalCount scans a snapshot using the strict FOR interval
// lane. It never renders or falls back per row: unsupported compact leaves
// decline atomically, allowing the query executor to run the original
// predicate.
func (s *Snapshot) FilterIntegerIntervalCount(
	f *IntegerIntervalFilter,
) (FilterIntegerIntervalResult, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return FilterIntegerIntervalResult{}, ErrClosed
	}
	if f == nil || f.inner == nil {
		return FilterIntegerIntervalResult{}, fmt.Errorf("vibedb: nil unified integer interval filter")
	}
	state := s.state
	catalogBounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.fileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	var cursor storeio.PrimaryGraphCursor
	if err := storeio.InitPrimaryGraphCursor(
		&cursor, s.collection.cache, state.root.PrimaryRoot,
		catalogBounds, leafBounds, nil, nil,
	); err != nil {
		return FilterIntegerIntervalResult{}, err
	}
	defer cursor.Close()
	var progress storeio.UnifiedFilterProgress
	for {
		supported, err := cursor.FilterCountIntegerInterval(f.inner, &progress)
		if err != nil {
			return FilterIntegerIntervalResult{}, err
		}
		if !supported {
			return FilterIntegerIntervalResult{}, nil
		}
		return FilterIntegerIntervalResult{
			Matched: progress.Matched, Scanned: progress.Scanned, Supported: true,
		}, nil
	}
}

// FilterIntegerExtrema scans a snapshot using the strict FOR integer extrema
// lane. It never renders or falls back per row: unsupported compact leaves
// decline atomically, allowing the query executor to run the generic path.
func (s *Snapshot) FilterIntegerExtrema(
	f *IntegerExtremaFilter,
) (FilterIntegerExtremaResult, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return FilterIntegerExtremaResult{}, ErrClosed
	}
	if f == nil || f.inner == nil {
		return FilterIntegerExtremaResult{}, fmt.Errorf("vibedb: nil unified integer extrema filter")
	}
	state := s.state
	catalogBounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.fileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	var cursor storeio.PrimaryGraphCursor
	if err := storeio.InitPrimaryGraphCursor(
		&cursor, s.collection.cache, state.root.PrimaryRoot,
		catalogBounds, leafBounds, nil, nil,
	); err != nil {
		return FilterIntegerExtremaResult{}, err
	}
	defer cursor.Close()
	var progress storeio.UnifiedIntegerExtremaProgress
	supported, err := cursor.FilterIntegerExtrema(f.inner, &progress)
	if err != nil {
		return FilterIntegerExtremaResult{}, err
	}
	if !supported {
		return FilterIntegerExtremaResult{}, nil
	}
	return FilterIntegerExtremaResult{
		Min: progress.Min, Max: progress.Max, Found: progress.Found,
		Scanned: progress.Scanned, Supported: true,
	}, nil
}

// fieldProbeKey identifies one (leaf epoch, template) resolution. Pages are
// immutable per (LogicalID, Generation) — the page cache's own identity — so
// a cached hole ordinal can never go stale within a collection; a rewritten
// leaf arrives under a new generation and misses.
type fieldProbeKey struct {
	generation uint64
	logicalID  uint64
	template   uint8
}

// FieldProbe reads one field's canonical spelling from documents by key
// without copying or parsing the rest of the document. A probe is
// single-consumer, bound to one
// collection, and reusable across snapshots; per-(leaf, template) hole
// resolutions are cached, so a warmed probe allocates nothing.
type FieldProbe struct {
	resolver storeio.UnifiedHoleResolver
	cache    map[fieldProbeKey]int16
	doc      []byte
}

// NewFieldProbe builds a probe for a "/a/b" field path.
func NewFieldProbe(path string) (*FieldProbe, error) {
	p := &FieldProbe{cache: make(map[fieldProbeKey]int16, 64)}
	if err := p.resolver.SetPath([]byte(path)); err != nil {
		return nil, err
	}
	return p, nil
}

// appendPathOf resolves the probe's path within one rendered document and
// appends the value's canonical spelling — the shared render-path tail for
// every row the token view cannot serve directly.
func (p *FieldProbe) appendPathOf(dst, doc []byte) ([]byte, bool, error) {
	start, end, found, err := p.resolver.PathSpanOf(doc)
	if err != nil || !found {
		return dst, false, err
	}
	return append(dst, doc[start:end]...), true, nil
}

// AppendField appends the canonical spelling of the probed field of the
// document at key. found is false when the key is absent or the document has
// no value at the path. The fast path serves templated rows of unified
// leaves from tokens; trivial rows, container targets, and overflow rows
// render only the one row; non-unified leaves and router-superseded reads
// degrade to whole-document AppendRaw plus a path walk (today's pattern).
func (s *Snapshot) AppendField(
	dst []byte, p *FieldProbe, key []byte,
) ([]byte, bool, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return dst, false, ErrClosed
	}
	if p == nil {
		return dst, false, fmt.Errorf("vibedb: nil field probe")
	}
	state := s.state
	if state.root.PrimaryRoot == (storeio.PageRef{}) ||
		len(key) == 0 || len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return dst, false, nil
	}
	// The probe accelerates only reads the resident router can serve at the
	// snapshot's exact generation; every other alignment (older materialized
	// snapshots, mid-swap windows) is correct through the whole-document
	// fallback, mirroring the resolvePrimaryGraph decision structure without
	// duplicating its page-walk oracle.
	router := s.collection.primaryRouter.Load()
	if router == nil || router.Generation() != state.root.Generation {
		return s.appendFieldFallback(dst, p, key)
	}
	route, ok := router.Route(key)
	if !ok {
		return dst, false, fmt.Errorf(
			"%w: resident primary route", storeio.ErrSegmentedTabletRouterCorrupt,
		)
	}
	if router.Generation() != state.root.Generation {
		return s.appendFieldFallback(dst, p, key)
	}
	leafLease, err := router.AcquireLeaf(s.collection.cache, route)
	if err != nil {
		return dst, false, err
	}
	defer leafLease.Release()
	page := leafLease.Page()
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
		return s.appendFieldFallback(dst, p, key)
	}
	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		page, state.root.StoreID, route.Bucket,
	)
	if !ok {
		return dst, false, fmt.Errorf(
			"%w: unified primary leaf", storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	rank, found := stripe.FindKey(key)
	if !found {
		return dst, false, nil
	}
	out, found, supported := stripe.AppendResolvedHole(dst, rank, &p.resolver)
	if supported {
		return out, found, nil
	}
	return s.appendFieldFallback(dst, p, key)
}

// appendFieldFallback is the whole-document path: AppendRaw into the probe's
// reused scratch, then the path walk — exactly the copy-then-parse pattern
// the token view exists to delete, kept as the correctness net for every
// alignment the fast path does not claim.
func (s *Snapshot) appendFieldFallback(
	dst []byte, p *FieldProbe, key []byte,
) ([]byte, bool, error) {
	doc, found, err := s.AppendRaw(p.doc[:0], key)
	p.doc = doc
	if err != nil || !found {
		return dst, false, err
	}
	return p.appendPathOf(dst, p.doc)
}
