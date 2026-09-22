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
