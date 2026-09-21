package query

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Full-text (==>) binding for durable snapshots.
//
// The heap binder resolves every ==> node against a store.Snapshot's tin
// catalog; the durable twin resolves against durable snapshots instead.
// Parsing still happens per execution against the executing generation's own
// index — wildcard and fuzzy expansions pin to the dictionary being scanned —
// and per-row evaluation stays tin.MatchSingle over the row's text, so the
// bound queries flow into the same worker evaluators through bindMatches.
// A missing tin index and an invalid query are both statement errors naming
// the path, exactly like the heap path.

// bindFilePredMatches resolves every ==> node under pd through resolve,
// which parses against the owning durable snapshot's generation-pinned tin
// index. Snapshots for plans without ==> nodes are never touched, so a nil
// inner snapshot (a cataloged collection with no backing file) only errors
// when its plan actually declares full text.
func bindFilePredMatches(
	pd *compiledPredicate,
	owner *plan,
	resolve func(path string) (*tin.Index, error),
	queries []tin.Query,
) error {
	if pd == nil {
		return nil
	}
	if pd.kind == predMatch {
		if pd.col < 0 || pd.col >= len(owner.valuePaths) || pd.slot < 0 || pd.slot >= len(queries) {
			return fmt.Errorf("query: ==> node names an uncompiled path or slot")
		}
		path := owner.valuePaths[pd.col].indexPath()
		ix, err := resolve(path)
		if err != nil {
			if errors.Is(err, store.ErrIndexNotFound) {
				return fmt.Errorf(
					"query: ==> over %s requires a tin index over that path (CREATE INDEX ... USING tin)",
					path,
				)
			}
			return err
		}
		q, err := ix.ParseTINQL(pd.pattern)
		if err != nil {
			return fmt.Errorf("query: ==> over %s: invalid TINQL query: %v", path, err)
		}
		queries[pd.slot] = q
	}
	for _, kid := range pd.kids {
		if err := bindFilePredMatches(kid, owner, resolve, queries); err != nil {
			return err
		}
	}
	return nil
}

// bindFilePlanMatches binds one plan level against its own durable snapshot,
// recursing into join and mark inner plans with theirs. Inner snapshots come
// from the database cut, the same source the join and mark binders pin.
func bindFilePlanMatches(
	p *plan,
	snapshot *durable.Snapshot,
	catalog durable.DatabaseSnapshot,
	queries []tin.Query,
) error {
	if err := bindFilePredMatches(p.where, p, fileTinResolver(snapshot), queries); err != nil {
		return err
	}
	for i := range p.joins {
		inner, ok := catalog.Collection(p.joins[i].collection)
		if !ok {
			return fmt.Errorf(
				"query: join: collection %q is not in the database snapshot",
				p.joins[i].collection,
			)
		}
		if err := bindFilePlanMatches(
			p.joins[i].inner, inner, catalog, queries,
		); err != nil {
			return err
		}
	}
	for i := range p.marks {
		inner, ok := catalog.Collection(p.marks[i].collection)
		if !ok {
			return fmt.Errorf(
				"query: correlated subquery: collection %q is not in the database snapshot",
				p.marks[i].collection,
			)
		}
		if err := bindFilePlanMatches(
			p.marks[i].inner, inner, catalog, queries,
		); err != nil {
			return err
		}
	}
	return nil
}

// fileTinResolver parses against one durable snapshot. A nil snapshot — a
// cataloged collection with no backing file — has no generation to parse
// against, so any ==> over it is a statement error, never a silent
// non-match.
func fileTinResolver(
	snapshot *durable.Snapshot,
) func(path string) (*tin.Index, error) {
	return func(path string) (*tin.Index, error) {
		if snapshot == nil {
			return nil, fmt.Errorf(
				"query: ==> over %s names a collection with no durable snapshot to parse against",
				path,
			)
		}
		return snapshot.TinIndexForPath(path)
	}
}

// bindFileMatches resolves and parses every ==> slot in p against the
// driving durable snapshot and the database cut before any scan starts. It
// mirrors plan.bindMatches: shared parsed queries, no-op reset when the
// plan carries no ==> node, and the same Workspace binding every worker
// evaluator aliases.
func (p *plan) bindFileMatches(
	w *Workspace,
	snapshot *durable.Snapshot,
	catalog durable.DatabaseSnapshot,
) error {
	if p.matchCount == 0 {
		w.matchQueries = w.matchQueries[:0]
		w.eval.bindMatches(nil)
		return nil
	}
	if snapshot == nil {
		return fmt.Errorf("query: ==> full-text match needs a durable snapshot")
	}
	for len(w.matchQueries) < p.matchCount {
		w.matchQueries = append(w.matchQueries, tin.Query{})
	}
	w.matchQueries = w.matchQueries[:p.matchCount]
	queries := w.matchQueries
	if err := bindFilePlanMatches(p, snapshot, catalog, queries); err != nil {
		return err
	}
	w.eval.bindMatches(queries)
	return nil
}

// bindFileOverlayMatches binds ==> over the driving snapshot only. The
// overlay path carries no database cut and rejects joins, so join and mark
// inner plans with ==> stay statement errors here. A non-empty overlay
// would hide staged terms from parse-time wildcard and fuzzy expansions —
// a recall gap, not a slowdown — so it refuses loudly instead.
func (p *plan) bindFileOverlayMatches(
	w *Workspace,
	snapshot *durable.Snapshot,
	overlay FileOverlay,
) error {
	if p.matchCount == 0 {
		w.matchQueries = w.matchQueries[:0]
		w.eval.bindMatches(nil)
		return nil
	}
	if snapshot == nil {
		return fmt.Errorf("query: ==> full-text match needs a durable snapshot")
	}
	if overlay != nil && fileOverlayHasContent(overlay) {
		return fmt.Errorf(
			"query: ==> full-text match is not supported over a staged-write overlay with pending writes; flush it first",
		)
	}
	for i := range p.joins {
		if planHasMatch(p.joins[i].inner) {
			return fmt.Errorf(
				"query: ==> full-text match is not supported over join inner plans on the overlay path",
			)
		}
	}
	for i := range p.marks {
		if planHasMatch(p.marks[i].inner) {
			return fmt.Errorf(
				"query: ==> full-text match is not supported over correlated subqueries on the overlay path",
			)
		}
	}
	for len(w.matchQueries) < p.matchCount {
		w.matchQueries = append(w.matchQueries, tin.Query{})
	}
	w.matchQueries = w.matchQueries[:p.matchCount]
	queries := w.matchQueries
	if err := bindFilePredMatches(
		p.where, p, fileTinResolver(snapshot), queries,
	); err != nil {
		return err
	}
	w.eval.bindMatches(queries)
	return nil
}

// planHasMatch reports whether any ==> node sits under pl.
func planHasMatch(pl *plan) bool {
	if pl == nil {
		return false
	}
	found := false
	var walkPred func(pd *compiledPredicate)
	walkPred = func(pd *compiledPredicate) {
		if pd == nil || found {
			return
		}
		if pd.kind == predMatch {
			found = true
			return
		}
		for _, kid := range pd.kids {
			walkPred(kid)
		}
	}
	walkPred(pl.where)
	if found {
		return true
	}
	for i := range pl.joins {
		if planHasMatch(pl.joins[i].inner) {
			return true
		}
	}
	for i := range pl.marks {
		if planHasMatch(pl.marks[i].inner) {
			return true
		}
	}
	return false
}

// fileOverlayHasContent probes the staged-write overlay for any pending
// row. It runs only for plans declaring ==>, never on the hot path.
func fileOverlayHasContent(overlay FileOverlay) bool {
	found := false
	probe := func([]byte) error {
		found = true
		return nil
	}
	_ = overlay.RangeInserts(probe)
	if found {
		return true
	}
	_ = overlay.RangePresent(probe)
	return found
}
