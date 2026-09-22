package query

import (
	"fmt"
	"runtime"

	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibedb/store"
)

// tinSegmentGateDocs is the snapshot size at which the heap ==> path fans
// out to segment indexes: below it the single index wins outright (fan-out
// overhead exceeds the search). Crossover-tuned: at 32k docs segmented
// top-K already wins 2.5x at tin level (205us to 83us, -cpu=8), 3.5x at
// 64k, 4.7x at 128k; at 8k the 1.4x is not worth the build duplication.
// The 8k-doc pinned benches stay single. A variable so tests can force
// the segmented path on small corpora.
var tinSegmentGateDocs = 32768

// tinSegmentShards sizes the fan-out: one segment per worker up to eight.
// A single worker never segments.
func tinSegmentShards() int {
	if n := runtime.GOMAXPROCS(0); n < 8 {
		return n
	}
	return 8
}

// bindTinSegments resolves the segmented build for path when the snapshot
// repays fan-out: enough documents and more than one segment. It reports
// false for small snapshots, single-worker runtimes, and missing
// definitions, leaving the single-index path to serve (and to report the
// missing-index statement error there).
func bindTinSegments(snapshot store.Snapshot, path string) ([]*tin.Index, bool) {
	if snapshot.Len() < tinSegmentGateDocs || tinSegmentShards() < 2 {
		return nil, false
	}
	segs, err := snapshot.TinSegmentsForPath(path, tinSegmentShards())
	if err != nil || len(segs) < 2 {
		return nil, false
	}
	return segs, true
}

// Full-text (==>) execution binding.
//
// A predMatch node carries its TINQL text (compiledPredicate.pattern) and a
// slot. Per execution, bindMatches resolves every slot: the node's canonical
// index path against the owning collection's tin catalog, then ParseTINQL
// against that snapshot-pinned index. Parsing cannot move to compile time
// because wildcard and fuzzy expansions resolve against the executing
// snapshot's own dictionary; parsing per execution keeps expansions pinned to
// the state being scanned even when a prepared statement runs against many
// snapshots. Per-row evaluation is then tin.MatchSingle over the row's text,
// with one TextScratch per slot per evaluator, so concurrent executions and
// filter-phase workers share the parsed queries read-only while each keeps
// its own transient scratch.
//
// Slots are statement-global: assignMatchSlots numbers every ==> node in the
// finished plan — driving WHERE plus join and mark inner plans — so one
// Workspace binding serves nested scans too. Inner plans resolve against
// their own collections through the catalog, exactly like join and mark
// bindings do.

// evalMatch reports whether the row's text satisfies the node's bound query.
// A non-string, null, or absent cell matches nothing, the same rule evalCmp
// applies; an unbound slot (a source that never binds) is false rather than
// a panic, and every such source rejects ==> plans before reaching here.
func evalMatch(s *evalScratch, p *compiledPredicate, cols [][]scalar, row int) bool {
	if p.slot < 0 || p.slot >= len(s.matchQueries) {
		return false
	}
	cell := cols[p.col][row]
	if cell.kind != kindString {
		return false
	}
	return tin.MatchSingle(cell.sval, s.matchQueries[p.slot], &s.matchScratch[p.slot])
}

// bindMatches points s at queries for one execution and gives it one match
// scratch per slot. It mirrors bindMarks: shared parsed queries, per-worker
// scratch, no-op reset when the plan carries no ==> node.
func (s *evalScratch) bindMatches(queries []tin.Query) {
	s.matchQueries = queries
	if len(queries) == 0 {
		s.matchScratch = s.matchScratch[:0]
		return
	}
	s.matchScratch = resize(s.matchScratch, len(queries))
}

// assignMatchSlots numbers every ==> node in the finished plan in one
// deterministic walk, returning the slot count. It runs at the end of
// compilePlan so recompilation renumbers from scratch; the compile case
// leaves slot at -1 so a node this pass never visits can never alias slot 0.
func assignMatchSlots(p *plan) int {
	n := 0
	var walkPred func(pd *compiledPredicate)
	walkPred = func(pd *compiledPredicate) {
		if pd == nil {
			return
		}
		if pd.kind == predMatch {
			pd.slot = n
			n++
		}
		for _, kid := range pd.kids {
			walkPred(kid)
		}
	}
	var walkPlan func(pl *plan)
	walkPlan = func(pl *plan) {
		if pl == nil {
			return
		}
		walkPred(pl.where)
		for i := range pl.joins {
			walkPlan(pl.joins[i].inner)
		}
		for i := range pl.marks {
			walkPlan(pl.marks[i].inner)
		}
	}
	walkPlan(p)
	return n
}

// bindMatches resolves and parses every ==> slot in p before the driving
// scan starts. It runs before bindJoins and bindMarks so nested scans, which
// reuse the calling evaluator, already see the queries when they collect.
func (p *plan) bindMatches(w *Workspace, snapshot store.Snapshot, catalog store.DatabaseSnapshot) error {
	if p.matchCount == 0 {
		w.matchQueries = w.matchQueries[:0]
		w.matchIndexes = w.matchIndexes[:0]
		w.matchShards = w.matchShards[:0]
		w.matchPatterns = w.matchPatterns[:0]
		w.matchTinBuilds = w.matchTinBuilds[:0]
		w.matchTinRouter = nil
		w.eval.bindMatches(nil)
		return nil
	}
	for len(w.matchQueries) < p.matchCount {
		w.matchQueries = append(w.matchQueries, tin.Query{})
	}
	for len(w.matchIndexes) < p.matchCount {
		w.matchIndexes = append(w.matchIndexes, nil)
	}
	for len(w.matchShards) < p.matchCount {
		w.matchShards = append(w.matchShards, nil)
	}
	for len(w.matchPatterns) < p.matchCount {
		w.matchPatterns = append(w.matchPatterns, "")
	}
	// Reslice to the exact count so workers aliasing w.matchQueries see the
	// same binding the calling evaluator gets; retained capacity past the
	// count stays warm for the next execution.
	w.matchQueries = w.matchQueries[:p.matchCount]
	w.matchIndexes = w.matchIndexes[:p.matchCount]
	w.matchShards = w.matchShards[:p.matchCount]
	w.matchPatterns = w.matchPatterns[:p.matchCount]
	// A Workspace reused across backends must not retain the other one's
	// builds: a stale durable build would mask a heap scan with foreign
	// addresses.
	w.matchTinBuilds = w.matchTinBuilds[:0]
	w.matchTinRouter = nil
	queries := w.matchQueries
	if err := bindPlanMatches(p, snapshot, catalog, queries, w.matchIndexes, w.matchShards, w.matchPatterns); err != nil {
		return err
	}
	w.eval.bindMatches(queries)
	return nil
}

// bindPlanMatches binds one plan level against its own collection snapshot,
// recursing into join and mark inner plans with theirs. indexes parallels
// queries: the generation-pinned build each slot parsed against, retained
// for index-pruned candidate masks.
func bindPlanMatches(p *plan, snapshot store.Snapshot, catalog store.DatabaseSnapshot, queries []tin.Query, indexes []*tin.Index, shards [][]tin.Shard, patterns []string) error {
	if err := bindPredMatches(p.where, p, snapshot, queries, indexes, shards, patterns); err != nil {
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
		if err := bindPlanMatches(p.joins[i].inner, inner, catalog, queries, indexes, shards, patterns); err != nil {
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
		if err := bindPlanMatches(p.marks[i].inner, inner, catalog, queries, indexes, shards, patterns); err != nil {
			return err
		}
	}
	return nil
}

// bindPredMatches resolves every ==> node under pd: the canonical index path
// against the owning plan's collection, then one ParseTINQL against that
// snapshot's index. A missing tin index and an invalid query are both
// statement errors naming the path.
func bindPredMatches(pd *compiledPredicate, owner *plan, snapshot store.Snapshot, queries []tin.Query, indexes []*tin.Index, shards [][]tin.Shard, patterns []string) error {
	if pd == nil {
		return nil
	}
	if pd.kind == predMatch {
		if pd.col < 0 || pd.col >= len(owner.valuePaths) || pd.slot < 0 || pd.slot >= len(queries) ||
			pd.slot >= len(indexes) || pd.slot >= len(shards) || pd.slot >= len(patterns) {
			return fmt.Errorf("query: ==> node names an uncompiled path or slot")
		}
		path := owner.valuePaths[pd.col].indexPath()
		patterns[pd.slot] = pd.pattern
		if segs, ok := bindTinSegments(snapshot, path); ok {
			// Segmented heap path: the single index stays unbuilt.
			// Shard buffers persist across executions; only the
			// index pointers rebind to this snapshot's segments.
			sh := shards[pd.slot]
			if len(sh) != len(segs) {
				sh = make([]tin.Shard, len(segs))
			}
			for i := range segs {
				sh[i].Ix = segs[i]
			}
			shards[pd.slot] = sh
			// The shared parse runs against the first segment.
			// Masks re-parse per shard when it reports Expanded,
			// and top-K serves only unexpanded shapes from it.
			q, err := segs[0].ParseTINQL(pd.pattern)
			if err != nil {
				return fmt.Errorf("query: ==> over %s: invalid TINQL query: %v", path, err)
			}
			queries[pd.slot] = q
			indexes[pd.slot] = nil
		} else {
			shards[pd.slot] = nil
			ix, err := snapshot.TinIndexForPath(path)
			if err != nil {
				return fmt.Errorf(
					"query: ==> over %s requires a tin index over that path (CREATE INDEX ... USING tin)",
					path,
				)
			}
			q, err := ix.ParseTINQL(pd.pattern)
			if err != nil {
				return fmt.Errorf("query: ==> over %s: invalid TINQL query: %v", path, err)
			}
			queries[pd.slot] = q
			indexes[pd.slot] = ix
		}
	}
	for _, kid := range pd.kids {
		if err := bindPredMatches(kid, owner, snapshot, queries, indexes, shards, patterns); err != nil {
			return err
		}
	}
	return nil
}

// rejectTinMatch guards execution paths with no index catalog to bind
// against: segments, raw sources, overlays, and durable snapshots. ==> over
// those is a clear statement error, never a silent non-match.
func rejectTinMatch(p *plan, source string) error {
	if p.matchCount == 0 {
		return nil
	}
	return fmt.Errorf(
		"query: ==> full-text match is not supported over %s; query an in-memory collection snapshot",
		source,
	)
}
