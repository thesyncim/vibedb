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

// ScorePinnedFull scores q's whole matching set under gs: OpTerm, plus
// all-term ANDs through the gateless full core. A lone term sums once
// per document, so the shared-view scores match the single index bit
// for bit. Conjunctions accumulate shortest-first by the shared df on
// every shard, the same order the top-K core uses everywhere —
// deterministic, unlike the single index's mergeScores order, so the
// two agree to 1 ulp and order identically outside pathological
// near-ties (see score_and.go). Anything else reports false; a missing
// list contributes nothing.
func (ix *Index) ScorePinnedFull(q Query, out []Scored, gs *SegGlobals) ([]Scored, bool) {
	if gs == nil {
		return out, false
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()
	switch q.Op {
	case OpTerm:
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
	case OpAnd:
		kids := q.Kids
		if len(kids) == 0 || !allTerms(kids) {
			return out, false
		}
		return ix.scoreAndFull(kids, q.boostOf(), out, gs.nDocs, gs.avg,
			func(term uint64) int { return gs.df[term] })
	}
	return out, false
}

// ScoreSegmentedFull scores q on every shard in parallel under one shared
// statistics view and merges the exact full ranking. See ScorePinnedFull
// for the OpTerm plus all-term AND contract; anything else reports false
// and the caller serves it another way. Nil shards contribute nothing.
// Buffer rules match ScoreGathered.
func ScoreSegmentedFull(shards []Shard, q Query, out []Scored) ([]Scored, bool) {
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

// segmentedTerms extracts the bound term set for the pinned paths: lone
// terms and all-term conjunctions, the shapes whose shared-view sums the
// merge proves exact. Anything else declines.
func segmentedTerms(q Query) ([]uint64, bool) {
	switch q.Op {
	case OpTerm:
		return []uint64{q.Term}, true
	case OpAnd:
		if len(q.Kids) == 0 || !allTerms(q.Kids) {
			return nil, false
		}
		terms := make([]uint64, len(q.Kids))
		for i, k := range q.Kids {
			terms[i] = k.Term
		}
		return terms, true
	}
	return nil, false
}

// ScoreSegmentedPool scores through p instead of spawning per call:
// the same shape gate, shared view, per-shard ScorePinned calls, and
// merge as ScoreSegmented, verdict-identical, with the pool's warmed
// allocation budget. See GatherPool for lifecycle.
func ScoreSegmentedPool(p *GatherPool, shards []Shard, q Query, topK int, out []Scored) ([]Scored, bool) {
	if topK <= 0 {
		return out, false
	}
	terms, ok := segmentedTerms(q)
	if !ok {
		return out, false
	}
	gs := gatherSegGlobals(shards, terms)
	if gs.nDocs == 0 {
		return out, true
	}
	return p.ScorePinned(shards, q, topK, &gs, out)
}

// ScoreSegmentedFullPool merges the exact full ranking through p:
// the ScoreSegmentedFull contract on the pool's budget.
func ScoreSegmentedFullPool(p *GatherPool, shards []Shard, q Query, out []Scored) ([]Scored, bool) {
	terms, ok := segmentedTerms(q)
	if !ok {
		return out, false
	}
	gs := gatherSegGlobals(shards, terms)
	if gs.nDocs == 0 {
		return out, true
	}
	return p.ScorePinnedFull(shards, q, &gs, out)
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

// GatherPool is a persistent worker set for distributed search. Spawning
// one goroutine per shard per query costs a stack plus scheduling per
// shard on every call (~2 allocs each, measured); parked workers pay
// that once and then serve every query. Size the pool to the search
// threads (usually GOMAXPROCS), not the shard count: workers stride
// over shards, so any shard count runs on any pool size, and shards
// beyond the thread count still queue in waves — oversubscription is a
// deployment property no gather API can remove. Close the pool when the
// deployment stops serving.
type GatherPool struct {
	work []chan gatherJob
	done chan struct{}
	wg   sync.WaitGroup
}

// gatherOp selects the per-shard call a pooled worker runs: plain Score,
// pinned top-K, or pinned full ranking.
type gatherOp uint8

const (
	gatherOpScore gatherOp = iota
	gatherOpPinned
	gatherOpPinnedFull
)

// gatherJob is one worker's share of one query: every worker receives the
// same job value and serves the shards its stride owns. All fields are
// read-only except the worker's own runs/oks slots and Out buffers, which
// are disjoint by stride, and the shared WaitGroup.
type gatherJob struct {
	shards []Shard
	runs   [][]Scored
	oks    []bool
	gs     *SegGlobals
	q      Query
	topK   int
	worker int
	of     int
	op     gatherOp
	wg     *sync.WaitGroup
}

// NewGatherPool starts n parked search workers.
func NewGatherPool(n int) *GatherPool {
	if n < 1 {
		n = 1
	}
	p := &GatherPool{work: make([]chan gatherJob, n), done: make(chan struct{})}
	for w := range p.work {
		p.work[w] = make(chan gatherJob, 1)
		p.wg.Add(1)
		go p.serve(w)
	}
	return p
}

func (p *GatherPool) serve(w int) {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			return
		case job := <-p.work[w]:
			switch job.op {
			case gatherOpPinned:
				for i := job.worker; i < len(job.shards); i += job.of {
					if job.shards[i].Ix == nil {
						job.oks[i] = true
						continue
					}
					s, ok := job.shards[i].Ix.ScorePinned(job.q, job.topK, job.shards[i].Out[:0], job.gs)
					job.shards[i].Out = s
					job.runs[i] = s
					job.oks[i] = ok
				}
			case gatherOpPinnedFull:
				for i := job.worker; i < len(job.shards); i += job.of {
					if job.shards[i].Ix == nil {
						job.oks[i] = true
						continue
					}
					s, ok := job.shards[i].Ix.ScorePinnedFull(job.q, job.shards[i].Out[:0], job.gs)
					job.shards[i].Out = s
					job.runs[i] = s
					job.oks[i] = ok
				}
			default:
				for i := job.worker; i < len(job.shards); i += job.of {
					if job.shards[i].Ix == nil {
						continue
					}
					job.shards[i].Out = job.shards[i].Ix.Score(job.q, job.topK, job.shards[i].Out[:0])
					job.runs[i] = job.shards[i].Out
				}
			}
			job.wg.Done()
		}
	}
}

// Score searches every shard through the pool and merges the exact global
// top-K, verdict-identical to ScoreGathered: the same per-shard Score
// calls over the same buffers into the same merge. A warmed call spends
// exactly three allocations — the runs slice, the pre-sized merge heap,
// and the shared WaitGroup — independent of shard count. Shard buffers
// must not alias out or each
// other, as with ScoreGathered, and concurrent Score calls must use
// disjoint shard sets: sharing one Shard's Out across callers races,
// exactly like sharing it across ScoreGathered calls.
func (p *GatherPool) Score(shards []Shard, q Query, topK int, out []Scored) []Scored {
	runs := make([][]Scored, len(shards))
	p.dispatch(gatherJob{shards: shards, q: q, topK: topK, runs: runs})
	return mergeScored(runs, topK, out)
}

// ScorePinned runs ScoreSegmented's per-shard work through the pool — the
// same ScorePinned calls over the same buffers — and reports the merge
// only when every shard serves (no declines). Verdict-identical to
// ScoreSegmented; a warmed call adds one allocation for the oks slice.
func (p *GatherPool) ScorePinned(shards []Shard, q Query, topK int, gs *SegGlobals, out []Scored) ([]Scored, bool) {
	runs := make([][]Scored, len(shards))
	oks := make([]bool, len(shards))
	p.dispatch(gatherJob{shards: shards, q: q, topK: topK, runs: runs, oks: oks, gs: gs, op: gatherOpPinned})
	for _, ok := range oks {
		if !ok {
			return out, false
		}
	}
	return mergeScored(runs, topK, out), true
}

// ScorePinnedFull runs ScoreSegmentedFull's per-shard work through the
// pool and merges the exact full ranking, verdict-identical to
// ScoreSegmentedFull. Warmed allocations match ScorePinned.
func (p *GatherPool) ScorePinnedFull(shards []Shard, q Query, gs *SegGlobals, out []Scored) ([]Scored, bool) {
	runs := make([][]Scored, len(shards))
	oks := make([]bool, len(shards))
	p.dispatch(gatherJob{shards: shards, q: q, runs: runs, oks: oks, gs: gs, op: gatherOpPinnedFull})
	for _, ok := range oks {
		if !ok {
			return out, false
		}
	}
	return mergeScored(runs, 0, out), true
}

// dispatch fans one job value out to the workers covering shards: at most
// one worker per shard, idle workers never woken.
func (p *GatherPool) dispatch(job gatherJob) {
	var wg sync.WaitGroup
	n := len(p.work)
	if len(job.shards) < n {
		n = len(job.shards)
	}
	wg.Add(n)
	job.of = n
	job.wg = &wg
	for w := 0; w < n; w++ {
		job.worker = w
		p.work[w] <- job
	}
	wg.Wait()
}

// Close parks the workers. Call it only once no Score call is in flight:
// a worker may observe done with a submitted job still queued, hanging
// that call's wait. The deployment owns the lifecycle — construct with
// the server, close on shutdown.
func (p *GatherPool) Close() {
	close(p.done)
	p.wg.Wait()
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
	// Pre-sized: one allocation covers every run's head instead of
	// regrowing through the capacity ladder per call.
	heap := make(mergeHeap, 0, len(runs))
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
