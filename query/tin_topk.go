package query

import (
	"sort"

	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibedb/store"
)

// Index-driven top-K restriction for ORDER BY SCORE() ... LIMIT.
//
// A full-text ORDER BY SCORE() LIMIT query used to scan every row, retokenize
// every matched body twice (once to recheck the ==> predicate, once to score
// it), materialize every survivor, and sort them all for a LIMIT that keeps
// a handful. The tin index already ranks the same documents with
// bit-identical BM25 (the Score/ScoreSingle mirror rule) in (score, DocID)
// order, and DocIDs pack the scan's own (chunk, slot) addresses in scan
// order, so the scan can read exactly the ranked survivors instead.
//
// The restriction only narrows the scanned row set: every downstream stage —
// the per-row ==> recheck, SCORE() evaluation, the stable sort, OFFSET/LIMIT
// slicing — runs unchanged over the survivors. Output is therefore identical
// to the full pipeline by construction, as long as the index ranking agrees
// with the full sort's prefix (scores bit-identical, ties in scan order).
// TestTinTopKMatchesFullSortPrefix proves that agreement over ties,
// multi-chunk corpora, deletes, both directions, offsets, and worker counts.
// Any shape outside the proven contract declines to the ordinary scan; an
// index is an optimization, never a semantic.

// tinTopKSpec records one proven pushdown shape on the plan: the ==> slot
// whose index ranks the scan, the sort direction, and the caller-visible
// window. The zero value disables the restriction. The spec names shapes,
// never snapshot data, so a cached lowering stays valid across snapshots;
// enforcement re-reads the execution's bound query and index.
type tinTopKSpec struct {
	set    bool
	slot   int
	desc   bool
	offset int
	limit  int
}

// decideTinTopK recognizes WHERE <== single query> with a single ORDER BY
// SCORE() key and a LIMIT. Every other shape — joins, marks, grouping,
// aggregates, DISTINCT, HAVING, plan-level ordering, multi-key orders,
// unordered or unlimited sorts, compound predicates — returns the zero spec
// and keeps the ordinary scan. The caller runs inside bindScorePlan, where
// the finished plan, the scalar program, and the numbered ==> slot are all
// visible; match is the bound slot's predicate.
func decideTinTopK(s *Statement, p *plan, r *statementScalar, match *compiledPredicate) tinTopKSpec {
	if r == nil || r.ordered == nil || r.ordered.having != nil || len(r.ordered.order) != 1 {
		return tinTopKSpec{}
	}
	o := &r.ordered.order[0]
	if o.root < 0 || int(o.root) >= len(r.nodes) || r.nodes[o.root].kind != statementScalarScore {
		return tinTopKSpec{}
	}
	// Without a LIMIT the sort still needs every row's key, so there is no
	// window to restrict to; the ordinary scan serves it.
	if !s.hasLimit {
		return tinTopKSpec{}
	}
	if s.tree == nil || s.tree.Distinct || s.tree.Having != nil || len(s.tree.GroupBy) != 0 {
		return tinTopKSpec{}
	}
	if r.hasAggregate || p.grouped || p.hasAggregate || p.singleRow || p.runtimeSQLPaths {
		return tinTopKSpec{}
	}
	if len(p.joins) != 0 || len(p.marks) != 0 || p.fanOutJoin >= 0 || len(p.order) != 0 {
		return tinTopKSpec{}
	}
	// The WHERE must be exactly the bound ==> node: a bare predMatch with
	// no kids. Anything else (conjunctions, disjunctions, NOT forms) admits
	// rows the index ranking cannot place, so only the ranking itself may
	// decide survivors.
	if p.where == nil || p.where != match || p.where.kind != predMatch || len(p.where.kids) != 0 {
		return tinTopKSpec{}
	}
	if match.slot < 0 {
		return tinTopKSpec{}
	}
	return tinTopKSpec{set: true, slot: match.slot, desc: o.desc, offset: s.offset, limit: s.limit}
}

// applyTinTopK fills the workspace's compact row list with the index's ranked
// survivors for spec, in rank order, and reports true. On any miss — no bound
// index for the slot, an unrepresentable slot address — it reports false and
// leaves the ordinary mask scan to run; callers must reset w.storeRows before
// reusing it on that path. The tin build is pinned to the executing
// snapshot's own state, so the ranking describes exactly the rows the scan
// would visit; the downstream ==> recheck still verdicts every survivor.
func applyTinTopK(w *Workspace, spec tinTopKSpec) bool {
	if spec.slot < 0 || spec.slot >= len(w.matchQueries) {
		return false
	}
	q := w.matchQueries[spec.slot]
	var ix *tin.Index
	if spec.slot < len(w.matchIndexes) && w.matchIndexes[spec.slot] != nil {
		ix = w.matchIndexes[spec.slot]
	} else if spec.slot < len(w.matchTinBuilds) && w.matchTinBuilds[spec.slot] != nil &&
		w.matchTinBuilds[spec.slot].Index() != nil {
		ix = w.matchTinBuilds[spec.slot].Index()
	}
	var survivors []tin.Scored
	haveSeg := !q.Expanded && spec.slot < len(w.matchShards) && len(w.matchShards[spec.slot]) > 0
	if spec.desc {
		if spec.limit == 0 {
			if ix == nil && !haveSeg {
				return false
			}
			w.storeRows = w.storeRows[:0]
			w.tinTopKUsed = true
			return true
		}
		need := spec.offset + spec.limit
		if need < spec.offset {
			// offset+limit overflowed past any reachable rank: the full
			// ranking covers every rank, sliced below.
			need = 0
		}
		// Segmented heap path first: one shared statistics view makes
		// the gathered top-K exactly the single index's, with global
		// document identities the row mapping below already speaks.
		// Expanding queries lower per shard, so only unexpanded
		// shapes serve from the shared parse; anything else declines
		// to the single index.
		segServed := false
		if haveSeg {
			if hits, ok := tin.ScoreSegmented(w.matchShards[spec.slot], q, need, w.matchScored[:0]); ok {
				w.matchScored = hits
				if need > 0 && len(hits) > need {
					hits = hits[:need]
				}
				survivors = hits
				segServed = true
			}
		}
		if !segServed {
			if ix == nil {
				return false
			}
			hits := ix.Score(q, need, w.matchScored[:0])
			w.matchScored = hits
			if need > 0 && len(hits) > need {
				hits = hits[:need]
			}
			survivors = hits
		}
	} else {
		// Ascending keeps scan order inside ties, which is the reverse of
		// nothing the descending ranking offers directly: re-sort the full
		// ranking by score alone, stably, so equal scores retain the
		// ranking's DocID order — the scan order the full sort breaks ties
		// with. Scoring the full match set still skips every per-row
		// retokenization, which is where the time used to go. Segmented
		// snapshots serve lone terms, whose shared-view full ranking
		// merges bit-identically (see tin.ScoreSegmentedFull); anything
		// else declines to the single index, and to the ordinary scan
		// when it is unbuilt.
		segServed := false
		if haveSeg && q.Op == tin.OpTerm {
			if hits, ok := tin.ScoreSegmentedFull(w.matchShards[spec.slot], q, w.matchScored[:0]); ok {
				w.matchScored = hits
				segServed = true
			}
		}
		var hits []tin.Scored
		if segServed {
			hits = w.matchScored
		} else {
			if ix == nil {
				return false
			}
			hits = ix.Score(q, 0, w.matchScored[:0])
			w.matchScored = hits
		}
		sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score < hits[j].Score })
		// Keep the offset+limit prefix, exactly like the plan-level bound:
		// the cursor still skips the offset itself downstream.
		hi := spec.offset + spec.limit
		if hi < spec.offset || hi > len(hits) {
			hi = len(hits)
		}
		survivors = hits[:hi]
	}
	rows := w.storeRows[:0]
	for _, hit := range survivors {
		chunk := uint32(hit.Doc >> 32)
		slot := uint64(hit.Doc & 0xffffffff)
		if slot > 255 {
			// Outside the row-address universe the compact scan speaks:
			// decline rather than emit a truncated set.
			return false
		}
		rows = append(rows, store.Location{Chunk: chunk, Slot: uint8(slot)})
	}
	w.storeRows = rows
	w.tinTopKUsed = true
	return true
}
