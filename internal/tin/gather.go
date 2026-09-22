package tin

import (
	"sync"
)

// Distributed gather: parallel search over shippable segments.
//
// A sharded deployment splits documents across segment indexes (see
// segment.go) and serves a query by searching every segment, then merging.
// Each segment scores with its own document frequencies — Lucene-segment
// semantics, documented on Segment — and the gatherer merges by (score,
// DocID). Per-segment top-K suffices for an exact global top-K: a document
// the global top-K keeps has fewer than topK better-or-equal documents
// globally, hence fewer inside its own segment, so its segment's top-K
// contains it; the merge then applies the same (score desc, DocID asc)
// order the single-index heap uses, ties included.
//
// DocIDs are expected disjoint across segments (disjoint snapshot slices).
// A DocID present in several segments keeps its first merge-order
// occurrence — the higher score, or the smaller DocID on ties —
// deterministically; overlapping slices are a degenerate input, not an
// error.
//
// Concurrency uses one goroutine per segment; the caller sizes the shard
// count. Score and Match hold each index's own lock, so distinct segments
// search fully in parallel. Per-segment result buffers are caller-owned
// (Shard.Out/DocOut) and warm across calls; the merge heap is the only
// per-query allocation, bounded by the shard count, never by documents.

// Shard is one searchable segment plus its reusable result buffers. Out
// and DocOut persist across calls so steady-state searches reuse their
// capacity; their contents are valid only for the call that filled them.
type Shard struct {
	Ix     *Index
	Out    []Scored
	DocOut []DocID
}

// ScoreGathered scores q on every shard in parallel and merges the exact
// global top-K into out. Each shard contributes its own top-K (sufficient
// by the argument above); topK <= 0 merges full per-shard rankings. A nil
// shard index contributes nothing. Out may alias nothing the caller still
// needs; shard buffers must not alias out or each other.
func ScoreGathered(shards []Shard, q Query, topK int, out []Scored) []Scored {
	runs := gatherScores(shards, q, topK)
	return mergeScored(runs, topK, out)
}

// SegGlobals is one corpus-wide statistics view every segment ranks
// under: total documents, average length, and each scoring term's global
// document frequency. Segments searched under one view produce the same
// scores the single index over their union would, so the merge is exact
// where ScoreGathered's per-segment idfs are only Lucene-approximate.
type SegGlobals struct {
	nDocs int
	avg   float64
	df    map[uint64]int
}

// gatherSegGlobals sums corpus statistics over shards: documents, tokens,
// and per-term document frequencies from postings counts, never matches,
// so gathering costs O(shards × terms).
func gatherSegGlobals(shards []Shard, terms []uint64) SegGlobals {
	var g SegGlobals
	var tokens uint64
	g.df = make(map[uint64]int, len(terms))
	for i := range shards {
		ix := shards[i].Ix
		if ix == nil {
			continue
		}
		ix.mu.Lock()
		g.nDocs += ix.nDocs
		tokens += ix.tokens
		for _, t := range terms {
			if p := ix.post[t]; p != nil {
				g.df[t] += p.docCount()
			}
		}
		ix.mu.Unlock()
	}
	if g.nDocs > 0 {
		g.avg = float64(tokens) / float64(g.nDocs)
	}
	return g
}

// ScorePinned scores like Score's top-K fast path but under gs, reporting
// false when q leaves the pinned shapes (anything but a term or an
// all-term AND with topK > 0). Shard shortfalls still report true —
// missing lists and empty intersections contribute nothing — so only
// shape and fast-gate declines veto the merge.
func (ix *Index) ScorePinned(q Query, topK int, out []Scored, gs *SegGlobals) ([]Scored, bool) {
	if gs == nil || topK <= 0 {
		return out, false
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()
	switch q.Op {
	case OpTerm:
		// gs covers q's own terms, so a missing key means the term is
		// absent everywhere: df 0, and the nil list below contributes
		// nothing.
		return ix.scoreSingleTopKCore(ix.post[q.Term], gs.nDocs, gs.avg,
			gs.df[q.Term], q.boostOf(), topK, out)
	case OpAnd:
		kids := q.Kids
		if len(kids) == 0 || !allTerms(kids) {
			return out, false
		}
		return ix.scoreAndTopKCore(kids, q.boostOf(), topK, out, gs.nDocs,
			gs.avg, func(term uint64) int { return gs.df[term] })
	}
	return out, false
}

// ScorePinnedFull scores q's whole matching set under gs, reporting
// false outside OpTerm: a lone term sums once per document, so the
// shared-view scores match the single index bit for bit and the merge
// order equals its ranking. AND sums in shard/task-dependent orders
// that agree only to 1 ulp (see score_and.go), which can reorder
// near-ties, so conjunctions stay on the top-K path whose single-index
// twin uses the same core. A missing list contributes nothing.
func (ix *Index) ScorePinnedFull(q Query, out []Scored, gs *SegGlobals) ([]Scored, bool) {
	if gs == nil || q.Op != OpTerm {
		return out, false
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()
	p := ix.post[q.Term]
	if p == nil || gs.nDocs == 0 {
		return out, true
	}
	n := p.docCount()
	if n == 0 {
		return out, true
	}
	// topK covers the shard's whole list, so the heap never fills and
	// no sealed block is ever skipped: the run is the full ranking.
	return ix.scoreSingleTopKCore(p, gs.nDocs, gs.avg, gs.df[q.Term], q.boostOf(), n, out)
}

// ScoreSegmentedFull scores q on every shard in parallel under one shared
// statistics view and merges the exact full ranking. See ScorePinnedFull
// for the OpTerm-only contract; anything else reports false and the
// caller serves it another way. Nil shards contribute nothing. Buffer
// rules match ScoreGathered.
func ScoreSegmentedFull(shards []Shard, q Query, out []Scored) ([]Scored, bool) {
	if q.Op != OpTerm {
		return out, false
	}
	gs := gatherSegGlobals(shards, []uint64{q.Term})
	if gs.nDocs == 0 {
		return out, true
	}
	runs := make([][]Scored, len(shards))
	oks := make([]bool, len(shards))
	var wg sync.WaitGroup
	for i := range shards {
		if shards[i].Ix == nil {
			oks[i] = true
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, ok := shards[i].Ix.ScorePinnedFull(q, shards[i].Out[:0], &gs)
			shards[i].Out = s
			runs[i] = s
			oks[i] = ok
		}(i)
	}
	wg.Wait()
	for _, ok := range oks {
		if !ok {
			return out, false
		}
	}
	return mergeScored(runs, 0, out), true
}

// ScoreSegmented scores q on every shard in parallel under one shared
// statistics view and merges the exact global top-K. It reports false
// when q leaves the pinned shapes or any shard declines its fast gate;
// the caller serves those shapes another way (single index, ordinary
// scan). Nil shards contribute nothing. Buffer rules match ScoreGathered.
func ScoreSegmented(shards []Shard, q Query, topK int, out []Scored) ([]Scored, bool) {
	if topK <= 0 {
		return out, false
	}
	var terms []uint64
	switch q.Op {
	case OpTerm:
		terms = []uint64{q.Term}
	case OpAnd:
		if len(q.Kids) == 0 || !allTerms(q.Kids) {
			return out, false
		}
		terms = make([]uint64, len(q.Kids))
		for i, k := range q.Kids {
			terms[i] = k.Term
		}
	default:
		return out, false
	}
	gs := gatherSegGlobals(shards, terms)
	if gs.nDocs == 0 {
		return out, true
	}
	runs := make([][]Scored, len(shards))
	oks := make([]bool, len(shards))
	var wg sync.WaitGroup
	for i := range shards {
		if shards[i].Ix == nil {
			oks[i] = true
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, ok := shards[i].Ix.ScorePinned(q, topK, shards[i].Out[:0], &gs)
			shards[i].Out = s
			runs[i] = s
			oks[i] = ok
		}(i)
	}
	wg.Wait()
	for _, ok := range oks {
		if !ok {
			return out, false
		}
	}
	return mergeScored(runs, topK, out), true
}

// MatchGathered matches q on every shard in parallel and merges the union
// of matching DocIDs, deduplicated, into out. Each per-shard run arrives
// DocID-sorted, so the union merge is linear. A nil shard index
// contributes nothing.
func MatchGathered(shards []Shard, q Query, out []DocID) []DocID {
	runs := gatherMatches(shards, q)
	return mergeDocIDs(runs, out)
}

// gatherScores runs one Score per shard, each on its own goroutine, and
// collects the per-shard runs in shard order. A shard that already holds
// enough warmed capacity allocates nothing.
func gatherScores(shards []Shard, q Query, topK int) [][]Scored {
	runs := make([][]Scored, len(shards))
	var wg sync.WaitGroup
	for i := range shards {
		if shards[i].Ix == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shards[i].Out = shards[i].Ix.Score(q, topK, shards[i].Out[:0])
			runs[i] = shards[i].Out
		}(i)
	}
	wg.Wait()
	return runs
}

// MatchGatheredQueries matches one query per shard in parallel and merges
// the deduplicated union. It serves patterns whose dictionary expansion
// differs per shard: each shard matches the pattern parsed against its
// own vocabulary, so the union covers every shard's expansions exactly.
// queries parallels shards; a nil index's query is ignored. Buffer rules
// match MatchGathered.
func MatchGatheredQueries(shards []Shard, queries []Query, out []DocID) []DocID {
	runs := make([][]DocID, len(shards))
	var wg sync.WaitGroup
	for i := range shards {
		if shards[i].Ix == nil || i >= len(queries) {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shards[i].DocOut = shards[i].Ix.Match(queries[i], shards[i].DocOut[:0])
			runs[i] = shards[i].DocOut
		}(i)
	}
	wg.Wait()
	return mergeDocIDs(runs, out)
}

// gatherMatches runs one Match per shard, each on its own goroutine.
func gatherMatches(shards []Shard, q Query) [][]DocID {
	runs := make([][]DocID, len(shards))
	var wg sync.WaitGroup
	for i := range shards {
		if shards[i].Ix == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shards[i].DocOut = shards[i].Ix.Match(q, shards[i].DocOut[:0])
			runs[i] = shards[i].DocOut
		}(i)
	}
	wg.Wait()
	return runs
}

// mergeScored k-way merges per-shard (score desc, DocID asc) runs into the
// global order and truncates to topK (topK <= 0 keeps everything). Equal
// DocIDs dedupe to their first — hence highest-scoring — occurrence.
func mergeScored(runs [][]Scored, topK int, out []Scored) []Scored {
	var heap mergeHeap
	for _, run := range runs {
		if len(run) > 0 {
			heap = append(heap, mergeHead{run: run})
		}
	}
	// heap.Init inlines as a down-heap pass; the heap package would do
	// the same with an interface tax per comparison.
	for i := len(heap)/2 - 1; i >= 0; i-- {
		heap.down(i)
	}
	out = out[:0]
	var last DocID
	haveLast := false
	for len(heap) > 0 {
		if topK > 0 && len(out) >= topK {
			break
		}
		best := heap[0]
		cur := best.run[0]
		if len(best.run) == 1 {
			heap.pop()
		} else {
			best.run = best.run[1:]
			heap[0] = best
			heap.down(0)
		}
		if haveLast && cur.Doc == last {
			continue
		}
		last, haveLast = cur.Doc, true
		out = append(out, cur)
	}
	return out
}

// mergeHead is one run's unread head inside the merge heap.
type mergeHead struct {
	run []Scored
}

// mergeHeap is a binary heap of run heads ordered by (score desc, DocID
// asc), matching the single-index ranking exactly.
type mergeHeap []mergeHead

func (h mergeHeap) less(i, j int) bool {
	a, b := h[i].run[0], h[j].run[0]
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	return a.Doc < b.Doc
}

func (h mergeHeap) down(i int) {
	for {
		left := 2*i + 1
		if left >= len(h) {
			return
		}
		best := left
		if right := left + 1; right < len(h) && h.less(right, left) {
			best = right
		}
		if !h.less(best, i) {
			return
		}
		h[i], h[best] = h[best], h[i]
		i = best
	}
}

func (h *mergeHeap) pop() {
	n := len(*h) - 1
	(*h)[0] = (*h)[n]
	*h = (*h)[:n]
	if n > 0 {
		h.down(0)
	}
}

// mergeDocIDs k-way unions DocID-sorted runs, deduplicated, into out.
// Linear in the union size; every per-shard run arrives DocID-sorted.
func mergeDocIDs(runs [][]DocID, out []DocID) []DocID {
	var heap docHeap
	for _, run := range runs {
		if len(run) > 0 {
			heap = append(heap, docHead{run: run})
		}
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		heap.down(i)
	}
	dst := out[:0]
	var last DocID
	haveLast := false
	for len(heap) > 0 {
		cur := heap[0].run[0]
		if len(heap[0].run) == 1 {
			heap.pop()
		} else {
			heap[0].run = heap[0].run[1:]
			heap.down(0)
		}
		if haveLast && cur == last {
			continue
		}
		last, haveLast = cur, true
		dst = append(dst, cur)
	}
	return dst
}

// docHead is one DocID run's unread head.
type docHead struct {
	run []DocID
}

// docHeap is a binary heap of DocID run heads, smallest first.
type docHeap []docHead

func (h docHeap) down(i int) {
	for {
		left := 2*i + 1
		if left >= len(h) {
			return
		}
		best := left
		if right := left + 1; right < len(h) && h[right].run[0] < h[left].run[0] {
			best = right
		}
		if h[best].run[0] >= h[i].run[0] {
			return
		}
		h[i], h[best] = h[best], h[i]
		i = best
	}
}

func (h *docHeap) pop() {
	n := len(*h) - 1
	(*h)[0] = (*h)[n]
	*h = (*h)[:n]
	if n > 0 {
		h.down(0)
	}
}
