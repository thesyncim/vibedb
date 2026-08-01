package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// The token filter and field probe consume class-5 rows through
// the token view without rendering non-target holes; both degrade per row —
// never per store — to the render path for trivial rows, container targets,
// overflow chains, and non-unified leaves.

// EqFilter is a reusable canonical-spelling equality predicate for
// Snapshot.FilterEqCount. It is single-consumer; reuse across scans and
// snapshots of the same collection amortizes all resolution scratch.
type EqFilter struct {
	inner *storeio.UnifiedEqFilter
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

// FilterEqResult reports one filtered scan: rows matched, rows that took the
// render-then-filter fallback lane (reported, not hidden), and rows
// scanned in total.
type FilterEqResult struct {
	Matched  int
	Fallback int
	Scanned  int
}

// FilterEqCount scans every live document and counts those whose value at
// the filter's path equals the filter's needle by canonical spelling.
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
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		return s.appendFieldFallback(dst, p, key)
	}
	bounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.fileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	uv, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, state.root.StoreID, route.Bucket, bounds,
	)
	if !ok {
		return dst, false, fmt.Errorf(
			"%w: unified primary leaf", storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	body, overflow, found := uv.LookupBodyHashed(route.Hash, key)
	if !found {
		return dst, false, nil
	}
	if overflow {
		p.doc, err = s.collection.appendPrimaryOverflowValue(
			p.doc[:0], storeio.DecodePrimaryOverflowRef(body), bounds,
		)
		if err != nil {
			return dst, false, err
		}
		return p.appendPathOf(dst, p.doc)
	}
	template, ok := uv.RowTemplate(body)
	if !ok {
		return dst, false, fmt.Errorf(
			"%w: unified row body", storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	if template < 0 {
		// Trivial row: the body after the tag is the canonical document.
		return p.appendPathOf(dst, body[1:])
	}
	// The routed PageRef already names the leaf's immutable identity
	// (LogicalID, Generation) — no page re-decode needed for the cache key.
	cacheKey := fieldProbeKey{
		generation: route.Ref.Generation,
		logicalID:  route.Ref.LogicalID,
		template:   uint8(template),
	}
	hole, cached := p.cache[cacheKey]
	if !cached {
		hole = int16(p.resolver.Resolve(&uv, template))
		p.cache[cacheKey] = hole
	}
	if hole == storeio.UnifiedHoleAbsent {
		return dst, false, nil
	}
	if hole < 0 {
		// Container target: render this one row and walk it.
		p.doc = uv.AppendAdmittedRowBody(p.doc[:0], body)
		return p.appendPathOf(dst, p.doc)
	}
	tok, ok := uv.RowToken(body, int(hole))
	if !ok {
		return dst, false, fmt.Errorf(
			"%w: unified row token", storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	return storeio.AppendUnifiedRowToken(dst, tok), true, nil
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
