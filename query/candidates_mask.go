package query

import (
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

// Declared index binding is deliberately late. A Query is immutable and may
// outlive online index creation, backfill, or drop; each snapshot carries
// the exact catalog generation against which this execution chooses a plan.
//
// This file is the mask-based candidate planner shared by every backend
// whose snapshot type satisfies store.IndexSource — today store.Snapshot
// (directly) and store/durable's reusable IndexSession.
// It replaces what were previously two hand-duplicated planners
// (store_candidates.go and file_candidates.go's AND/OR/Cmp/Contains/Not
// dispatch was ~60% structurally identical between them). The remaining
// per-backend files keep only their genuinely distinct entry points and
// capabilities: store_candidates.go's storeCandidateMasks* wrap
// store.Snapshot directly, file_candidates.go's file* wrap durable's
// private-state reusable index session.
//
// Needle-scratch discipline: every AppendIndexMasks/AppendIndexCandidateMasks
// call below passes w.needleScratch (a Workspace-owned, reused array),
// never a freshly-built local array or a bare scalar spread with `...`.
// This is not a style preference — verified empirically during this
// redesign, Go's generic dictionary dispatch defeats escape analysis for a
// variadic slice built inside a function generic over an interface type
// parameter, even though the concrete (non-generic) equivalent stack-
// allocates the same construction for free. Passing an already-existing
// slice through the generic call, instead of constructing one inside it,
// keeps these calls at zero allocations.

// snapshotCandidateMasks is the shared entry point: plan p's predicate
// against snapshot's declared index catalog and return the resulting
// stable-slot masks, or a nil, unbounded result when the catalog can't
// answer it. requireExact selects between candidate masks (may still need a
// document recheck) and exact masks (already collision-verified).
// A sourceCaps is the optional [store.IndexSource] capability a backend has: a
// materializable live-row universe for complementing a NOT. It is genuinely
// optional — the heap Snapshot has it, durable does not — so the planner has to
// know which it is dealing with.
//
// Whatever a caller puts in this field is converted to an interface, so a
// backend should state the narrowest concrete type that implements the
// capability. One pointer converts in place; a wider struct is copied to the
// heap to be boxed, which is the cost this type exists to remove.
//
// It knows because the caller says so, not because the planner asks. Asking
// meant an `any(snapshot)` type assertion inside the generic dispatch, and a
// type parameter wider than one word must be copied to the heap to be boxed.
// Both the heap Snapshot and durable IndexSession pointer are one word and
// convert in place, but capability selection remains a backend property fixed
// at compile time, so each concrete entry point states its own and the compiler
// checks the claim.
type sourceCaps struct {
	live store.LiveMaskSource
}

func snapshotCandidateMasks[S store.IndexSource](p *plan, snapshot S, caps sourceCaps, w *Workspace, requireExact bool) ([]store.Mask, bool, error) {
	if p.where == nil {
		return nil, false, nil
	}
	w.storeMaskUsed = 0
	w.storeIndexProbes = 0
	w.storeIndexes = snapshot.AppendIndexes(w.storeIndexes[:0])
	masks, bounded, exact, err := candidatesFor(p.where, snapshot, caps, p.valuePaths, w.storeIndexes, w, requireExact)
	if err != nil {
		return nil, false, err
	}
	if !bounded {
		return nil, false, nil
	}
	if masks == nil {
		return w.emptyStoreMask[:0], exact, nil
	}
	return masks, exact, nil
}

// candidatesFor is the generic Cmp/Contains/And/Or/Not dispatch shared by
// every store.IndexSource-satisfying backend.
func candidatesFor[S store.IndexSource](p *compiledPredicate, snapshot S, caps sourceCaps, paths []compiledPath, indexes []store.IndexInfo, w *Workspace, requireExact bool) ([]store.Mask, bool, bool, error) {
	switch p.kind {
	case predCmp:
		if index, ok := singleColumnIndex(p.indexPath(paths), indexes); p.op == Eq && ok {
			out := w.nextStoreMasks()
			w.needleScratch[0] = p.needle
			var err error
			if requireExact {
				out, err = snapshot.AppendIndexMasks(out, index.Name, w.needleScratch[:1]...)
			} else {
				out, err = snapshot.AppendIndexCandidateMasks(out, index.Name, w.needleScratch[:1]...)
			}
			if err != nil {
				return nil, false, false, err
			}
			w.storeIndexProbes++
			w.keepStoreMasks(out)
			return out, true, requireExact, nil
		}
		// No exact index answers this leaf, so the catalog cannot bound it: it
		// falls through to a full scan.
		return nil, false, false, nil
	case predIn, predInBound:
		// The index probes one exact value at a time (its variadic values are
		// a compound key's columns, not alternatives), so a membership costs
		// one probe per alternative, unioned. That is the same probe count a
		// disjunction of equalities would pay; what In saves is on the other
		// side of the probe, where every returned candidate is rechecked by
		// binary search instead of by a walk of the whole set.
		//
		// A late-bound membership reaches this identically once its slot is
		// filled. A binding that chose the lookup strategy has no set at all
		// and no index can bound it, so it declines here and the join is
		// answered by the per-row probe during the recheck instead.
		lits, needles, bindable := p.membership(w)
		if !bindable {
			return nil, false, false, nil
		}
		if len(needles) == 0 {
			// An empty membership matches nothing, which is an exact answer on
			// its own. lits is resolved rather than p.lits because a late-bound
			// join membership carries its values in the Workspace slot. A
			// non-empty membership without scalar needles is unindexable.
			if len(lits) == 0 {
				return nil, true, true, nil
			}
			return nil, false, false, nil
		}
		index, ok := singleColumnIndex(p.indexPath(paths), indexes)
		if !ok {
			return nil, false, false, nil
		}
		var acc []store.Mask
		for i := range needles {
			out := w.nextStoreMasks()
			w.needleScratch[0] = needles[i]
			var err error
			if requireExact {
				out, err = snapshot.AppendIndexMasks(out, index.Name, w.needleScratch[:1]...)
			} else {
				out, err = snapshot.AppendIndexCandidateMasks(out, index.Name, w.needleScratch[:1]...)
			}
			if err != nil {
				return nil, false, false, err
			}
			w.storeIndexProbes++
			w.keepStoreMasks(out)
			if i == 0 {
				acc = out
				continue
			}
			acc = unionStoreMasks(w.nextStoreMasks(), acc, out)
			w.keepStoreMasks(acc)
		}
		return acc, true, requireExact, nil
	case predAntiBound:
		// Existential negation is a two-valued complement, not SQL NOT over a
		// nullable equality. A heap snapshot can complement an exact positive
		// membership against its metadata-only live universe; that retains
		// NULL, missing, and scalar/container values outside the membership.
		// Durable deliberately offers no live universe because obtaining one
		// would require a full document-page scan, so it declines here before
		// paying for any positive probes.
		if caps.live == nil {
			return nil, false, false, nil
		}
		positive := *p
		positive.kind = predInBound
		inner, bounded, exact, err := candidatesFor(
			&positive, snapshot, caps, paths, indexes, w, true,
		)
		if err != nil {
			return nil, false, false, err
		}
		if !bounded || !exact {
			return nil, false, false, nil
		}
		liveMasks := caps.live.AppendLiveMasks(w.nextStoreMasks())
		w.keepStoreMasks(liveMasks)
		out := andNotStoreMasks(w.nextStoreMasks(), liveMasks, inner)
		w.keepStoreMasks(out)
		return out, true, true, nil
	case predExists, predIsNull:
		// An exact index stores values, not the fact that a path is missing or
		// explicitly null, so nothing in the catalog bounds these: they fall
		// through to a full scan.
		return nil, false, false, nil
	case predContains:
		if p.containPlan == nil {
			return nil, false, false, nil
		}
		return candidatesFor(p.containPlan, snapshot, caps, paths, indexes, w, requireExact)
	case predAnd:
		return andCandidatesFor(p, snapshot, caps, paths, indexes, w, requireExact)
	case predOr:
		for _, kid := range p.kids {
			if !kid.canBound(paths, indexes, w) {
				return nil, false, false, nil
			}
		}
		return orCandidatesFor(p, snapshot, caps, paths, indexes, w, requireExact)
	case predNot:
		if len(p.kids) != 1 {
			return nil, false, false, nil
		}
		// Complementing against the live universe is a metadata-only operation
		// on a heap Snapshot (LiveMaskSource) but would need real page I/O on
		// durable, so NOT stays unbounded for any backend that can't provide
		// it — matching durable's behavior of declining NOT
		// entirely, now expressed as an optional capability instead of a
		// hard-coded backend split. Checked before recursing into the child: a
		// backend that can never complete a NOT should decline it at zero cost,
		// not after paying for a child probe it can't use.
		live := caps.live
		if live == nil {
			return nil, false, false, nil
		}
		// Complementing a candidate superset is unsafe: a hash collision in the
		// child could remove a real NOT match. Force exact leaf rechecks before
		// subtracting from the live universe.
		inner, bounded, exact, err := candidatesFor(p.kids[0], snapshot, caps, paths, indexes, w, true)
		if err != nil {
			return nil, false, false, err
		}
		if !bounded || !exact {
			return nil, false, false, nil
		}
		liveMasks := live.AppendLiveMasks(w.nextStoreMasks())
		w.keepStoreMasks(liveMasks)
		out := andNotStoreMasks(w.nextStoreMasks(), liveMasks, inner)
		w.keepStoreMasks(out)
		return out, true, true, nil
	default:
		return nil, false, false, nil
	}
}

func andCandidatesFor[S store.IndexSource](p *compiledPredicate, snapshot S, caps sourceCaps, paths []compiledPath, indexes []store.IndexInfo, w *Workspace, requireExact bool) ([]store.Mask, bool, bool, error) {
	var acc []store.Mask
	have := false
	allExact := true
	var compound store.IndexInfo
	if index, count, ok := p.bestCompoundIndexInto(paths, indexes, &w.needleScratch); ok {
		compound = index
		acc = w.nextStoreMasks()
		var err error
		if requireExact {
			acc, err = snapshot.AppendIndexMasks(acc, index.Name, w.needleScratch[:count]...)
		} else {
			acc, err = snapshot.AppendIndexCandidateMasks(acc, index.Name, w.needleScratch[:count]...)
		}
		if err != nil {
			return nil, false, false, err
		}
		w.storeIndexProbes++
		w.keepStoreMasks(acc)
		have = true
		allExact = requireExact
	}
	for _, kid := range p.kids {
		if kid.coveredEquality(paths, compound) {
			continue
		}
		rows, bounded, exact, err := candidatesFor(kid, snapshot, caps, paths, indexes, w, requireExact)
		if err != nil {
			return nil, false, false, err
		}
		if !bounded {
			allExact = false
			continue
		}
		allExact = allExact && exact
		if !have {
			acc, have = rows, true
			continue
		}
		acc = intersectStoreMasks(w.nextStoreMasks(), acc, rows)
		w.keepStoreMasks(acc)
	}
	if !have {
		return nil, false, false, nil
	}
	return acc, true, allExact, nil
}

func orCandidatesFor[S store.IndexSource](p *compiledPredicate, snapshot S, caps sourceCaps, paths []compiledPath, indexes []store.IndexInfo, w *Workspace, requireExact bool) ([]store.Mask, bool, bool, error) {
	var acc []store.Mask
	allExact := true
	for i, kid := range p.kids {
		rows, bounded, exact, err := candidatesFor(kid, snapshot, caps, paths, indexes, w, requireExact)
		if err != nil {
			return nil, false, false, err
		}
		if !bounded {
			return nil, false, false, nil
		}
		allExact = allExact && exact
		if i == 0 {
			acc = rows
			continue
		}
		acc = unionStoreMasks(w.nextStoreMasks(), acc, rows)
		w.keepStoreMasks(acc)
	}
	return acc, true, allExact, nil
}

// singleColumnIndex finds a ready exact index whose only column is path. It is
// the one lookup shared by candidate generation and by both no-I/O planner
// passes, so a leaf's "is this indexable" answer cannot drift from what
// candidate generation would actually probe.
func singleColumnIndex(path string, indexes []store.IndexInfo) (store.IndexInfo, bool) {
	for _, index := range indexes {
		if index.Kind == store.IndexExact && index.State == store.IndexReady &&
			index.ColumnCount == 1 && index.Columns[0] == path {
			return index, true
		}
	}
	return store.IndexInfo{}, false
}

// membership returns the alternatives and needles a membership leaf tests
// against, resolving a late-bound one through its slot in the executing
// Workspace. bindable is false for a binding that chose the lookup strategy:
// it has no set, so no index probe can bound it and every planner pass must
// treat it as unbounded rather than as an empty — and therefore
// nothing-matching — membership.
//
// The compile-time leaf answers from its own fields with one predictable
// branch, which is what keeps the existing In path exactly as fast as it was
// before late binding existed.
func (p *compiledPredicate) membership(w *Workspace) (lits []scalar, needles []vibejson.Index, bindable bool) {
	if p.kind != predInBound {
		return p.lits, p.needles, true
	}
	if p.slot >= len(w.joins) {
		return nil, nil, false
	}
	b := &w.joins[p.slot]
	if b.mode != joinBindSet {
		return nil, nil, false
	}
	return b.lits, b.needles, true
}

// membershipBounded reports whether an In leaf can be answered from the index
// catalog. An empty membership is bounded without any index — it matches no
// row, and an empty candidate set proves that exactly. Otherwise every
// alternative must carry a scalar needle (compilation drops all of them if any
// one does not, so a partial, unsound bound cannot arise) and the path must
// have a ready single-column exact index.
func (p *compiledPredicate) membershipBounded(paths []compiledPath, indexes []store.IndexInfo, w *Workspace) bool {
	lits, needles, bindable := p.membership(w)
	if !bindable {
		return false
	}
	if len(lits) == 0 {
		return true
	}
	if len(needles) != len(lits) {
		return false
	}
	_, ok := singleColumnIndex(p.indexPath(paths), indexes)
	return ok
}

// canBound is the no-I/O planner pass: does the declared index catalog
// alone (no snapshot access) let this predicate return a bounded candidate
// set? OR requires every branch to prove usable before any backend attempts
// real work on the first one — a probe that turns out unbounded after a
// sibling already paid its cost (page I/O on durable) is wasted work.
//
// w is read only to resolve a late-bound membership's slot, which is already
// filled by the time any planner pass runs; the catalog itself still comes from
// indexes and no snapshot is touched.
func (p *compiledPredicate) canBound(
	paths []compiledPath, indexes []store.IndexInfo, w *Workspace,
) bool {
	switch p.kind {
	case predCmp:
		if p.op != Eq {
			return false
		}
		_, ok := singleColumnIndex(p.indexPath(paths), indexes)
		return ok
	case predIn, predInBound:
		return p.membershipBounded(paths, indexes, w)
	case predContains:
		return p.containPlan != nil && p.containPlan.canBound(paths, indexes, w)
	case predAnd:
		if _, _, ok := p.bestCompoundIndex(paths, indexes); ok {
			return true
		}
		for _, kid := range p.kids {
			if kid.canBound(paths, indexes, w) {
				return true
			}
		}
		return false
	case predOr:
		if len(p.kids) == 0 {
			return false
		}
		for _, kid := range p.kids {
			if !kid.canBound(paths, indexes, w) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// canAnswerExactly is the no-I/O proof for a direct indexed-answer lane
// (e.g. an indexed count that never touches JSON): every predicate leaf must
// have a persistent exact probe, with no unbounded residual left for the
// general row evaluator.
func (p *compiledPredicate) canAnswerExactly(paths []compiledPath, indexes []store.IndexInfo, w *Workspace) bool {
	switch p.kind {
	case predCmp:
		if p.op != Eq {
			return false
		}
		_, ok := singleColumnIndex(p.indexPath(paths), indexes)
		return ok
	case predIn, predInBound:
		return p.membershipBounded(paths, indexes, w)
	case predContains:
		return p.containPlan != nil && p.containPlan.canAnswerExactly(paths, indexes, w)
	case predAnd:
		if len(p.kids) == 0 {
			return false
		}
		compound, _, _ := p.bestCompoundIndex(paths, indexes)
		for _, kid := range p.kids {
			if kid.coveredEquality(paths, compound) {
				continue
			}
			if !kid.canAnswerExactly(paths, indexes, w) {
				return false
			}
		}
		return compound.ColumnCount != 0 || len(p.kids) != 0
	case predOr:
		if len(p.kids) == 0 {
			return false
		}
		for _, kid := range p.kids {
			if !kid.canAnswerExactly(paths, indexes, w) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// maxExactProbeColumns reports the widest exact-index probe selected by the
// same catalog rules as canAnswerExactly/candidatesFor. The caller invokes it
// only after canAnswerExactly succeeds. Width matters to durable admission:
// a single-column collision recheck can seek one raw scalar, while a compound
// recheck may need a complete document tape.
func (p *compiledPredicate) maxExactProbeColumns(
	paths []compiledPath,
	indexes []store.IndexInfo,
	w *Workspace,
) int {
	switch p.kind {
	case predCmp:
		return 1
	case predIn, predInBound:
		lits, _, bindable := p.membership(w)
		if !bindable || len(lits) == 0 {
			return 0
		}
		return 1
	case predContains:
		if p.containPlan == nil {
			return 0
		}
		return p.containPlan.maxExactProbeColumns(paths, indexes, w)
	case predAnd:
		compound, _, _ := p.bestCompoundIndex(paths, indexes)
		width := int(compound.ColumnCount)
		for _, kid := range p.kids {
			if kid.coveredEquality(paths, compound) {
				continue
			}
			width = max(
				width,
				kid.maxExactProbeColumns(paths, indexes, w),
			)
		}
		return width
	case predOr:
		width := 0
		for _, kid := range p.kids {
			width = max(
				width,
				kid.maxExactProbeColumns(paths, indexes, w),
			)
		}
		return width
	default:
		return 0
	}
}

func (p *compiledPredicate) coveredEquality(paths []compiledPath, compound store.IndexInfo) bool {
	if compound.ColumnCount < 2 || p.kind != predCmp || p.op != Eq {
		return false
	}
	path := p.indexPath(paths)
	for i := 0; i < int(compound.ColumnCount); i++ {
		if compound.Columns[i] == path {
			return true
		}
	}
	return false
}

// bestCompoundIndex is the value-returning form used by canBound/
// canAnswerExactly, which only need the chosen index (or just whether one
// exists) and never spread the values through a generic variadic call, so
// the local array it builds is never at risk of the escape this file's
// header documents.
func (p *compiledPredicate) bestCompoundIndex(paths []compiledPath, indexes []store.IndexInfo) (store.IndexInfo, [store.MaxIndexColumns]vibejson.Index, bool) {
	var best store.IndexInfo
	var bestValues [store.MaxIndexColumns]vibejson.Index
	for _, index := range indexes {
		if index.Kind != store.IndexExact || index.State != store.IndexReady || index.ColumnCount < 2 || index.ColumnCount <= best.ColumnCount {
			continue
		}
		var values [store.MaxIndexColumns]vibejson.Index
		matched := true
		for i := 0; i < int(index.ColumnCount); i++ {
			value, ok := p.findEquality(index.Columns[i], paths)
			if !ok {
				matched = false
				break
			}
			values[i] = value
		}
		if matched {
			best, bestValues = index, values
		}
	}
	return best, bestValues, best.ColumnCount != 0
}

// bestCompoundIndexInto is bestCompoundIndex writing into a caller-owned
// buffer (a Workspace's needleScratch) instead of returning a fresh array —
// the form the generic andCandidatesFor uses, so its later
// `dst[:count]...` spread passes an already-existing slice into the
// store.IndexSource call.
func (p *compiledPredicate) bestCompoundIndexInto(paths []compiledPath, indexes []store.IndexInfo, dst *[store.MaxIndexColumns]vibejson.Index) (store.IndexInfo, int, bool) {
	best, bestValues, ok := p.bestCompoundIndex(paths, indexes)
	if !ok {
		return best, 0, false
	}
	*dst = bestValues
	return best, int(best.ColumnCount), true
}

func (p *compiledPredicate) findEquality(path string, paths []compiledPath) (vibejson.Index, bool) {
	if p.kind == predCmp && p.op == Eq && p.indexPath(paths) == path {
		return p.needle, true
	}
	if p.kind == predAnd {
		for _, kid := range p.kids {
			if value, ok := kid.findEquality(path, paths); ok {
				return value, true
			}
		}
	}
	return vibejson.Index{}, false
}

func intersectStoreMasks(dst, a, b []store.Mask) []store.Mask {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].Chunk < b[j].Chunk:
			i = advanceStoreMasksUntil(a, i, b[j].Chunk)
		case a[i].Chunk > b[j].Chunk:
			j = advanceStoreMasksUntil(b, j, a[i].Chunk)
		default:
			if bits := a[i].Bits & b[j].Bits; bits != 0 {
				dst = append(dst, store.Mask{Chunk: a[i].Chunk, Bits: bits})
			}
			i++
			j++
		}
	}
	return dst
}

func unionStoreMasks(dst, a, b []store.Mask) []store.Mask {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].Chunk < b[j].Chunk:
			dst = append(dst, a[i])
			i++
		case a[i].Chunk > b[j].Chunk:
			dst = append(dst, b[j])
			j++
		default:
			dst = append(dst, store.Mask{Chunk: a[i].Chunk, Bits: a[i].Bits | b[j].Bits})
			i++
			j++
		}
	}
	dst = append(dst, a[i:]...)
	return append(dst, b[j:]...)
}

func andNotStoreMasks(dst, a, b []store.Mask) []store.Mask {
	j := 0
	for _, left := range a {
		if j < len(b) && b[j].Chunk < left.Chunk {
			j = advanceStoreMasksUntil(b, j, left.Chunk)
		}
		bits := left.Bits
		if j < len(b) && b[j].Chunk == left.Chunk {
			bits &^= b[j].Bits
		}
		if bits != 0 {
			dst = append(dst, store.Mask{Chunk: left.Chunk, Bits: bits})
		}
	}
	return dst
}

// advanceStoreMasksUntil returns the first position after pos whose chunk is
// at least target. The immediate next word is checked first; when postings are
// highly skewed an exponential probe brackets the target before binary search.
// This is Roaring's advanceUntil strategy applied to stable chunk words.
// Provenance: ALGO-ROARING-001.
func advanceStoreMasksUntil(masks []store.Mask, pos int, target uint32) int {
	lower := pos + 1
	if lower >= len(masks) || masks[lower].Chunk >= target {
		return lower
	}
	remaining := len(masks) - lower
	// Dense neighbours favour the branch-predictable linear walk. Galloping
	// pays for itself when both the positional and chunk-key distances are
	// large; this is the same adaptive distinction Roaring makes between
	// locally dense and strongly skewed container streams.
	if remaining <= 16 || uint64(target)-uint64(masks[lower].Chunk) <= 8 {
		for lower < len(masks) && masks[lower].Chunk < target {
			lower++
		}
		return lower
	}

	span := 1
	previous := 0
	for span < remaining && masks[lower+span].Chunk < target {
		previous = span
		// Clamp before doubling so an adversarially large slice cannot wrap
		// int and turn a bounds-safe search into an invalid address.
		if span > (remaining-1)/2 {
			span = remaining
			break
		}
		span *= 2
	}
	upper := len(masks) - 1
	if span < remaining {
		upper = lower + span
	}
	if masks[upper].Chunk < target {
		return len(masks)
	}
	if masks[upper].Chunk == target {
		return upper
	}
	lower += previous
	if lower == upper {
		return upper
	}
	for lower+1 < upper {
		middle := lower + (upper-lower)/2
		if masks[middle].Chunk < target {
			lower = middle
		} else {
			upper = middle
		}
	}
	return upper
}
