package tin

import "math"

// BlockMax WAND top-K for all-term ORs.
//
// Streaming OR scores every matching document; WAND skips documents no
// heap candidate can beat. Each kid carries a cursor over its postings
// plus the current 128-row block's upper bound — peak frequency over
// minimum length through blockBound, the same bound the top-K skips use
// — and the pivot loop advances losers past the first document whose
// cumulative bound clears the heap threshold. Pivot candidates score
// exactly (bm25One per kid, the kernel tail itself) and enter the same
// worst-first heap, so ranks match streaming with scores agreeing to
// 1e-12: WAND sums kids in fixed order while streaming sums in
// mergeScores order (see the AND contract in score_and.go).
//
// Block statistics are single-visit by construction: cursors advance
// monotonically, so each block's peak and minimum length compute once,
// on entry, into cursor fields — never into cross-call caches, so
// mutations cannot stale them. Open blocks scan offset deltas and probe
// lengths; sealed blocks read the count width free and probe lengths
// from the just-decoded ids. Sealed cache arrays obey the no-hold rule:
// every sealedBlock result is consumed before the next sealed call.
//
// The pre-gate mirrors the AND path: an unselective disjunction never
// pays cursor overhead on top of near-total scoring.

// wandGatePre bounds WAND's work change like andGatePre bounds the AND
// path: the smallest kid against the total without matching.
const wandGatePre = 2

// wandGateMinDocs floors the pre-gate: below this union size the
// streaming-vs-WAND delta is sub-microsecond noise either way, so WAND
// always proceeds — which also keeps small unions exercising the pivot
// path in tests instead of declining out of coverage.
const wandGateMinDocs = 1024

// wandExhausted marks a cursor past its list's end; it sorts after every
// real document. Pivot arithmetic never touches it: candidates always
// come from a live head, so pivot+1 cannot wrap.
const wandExhausted = DocID(math.MaxUint64)

// wandCursor is one OR kid's traversal state: a monotonic position, the
// current block number (open and sealed alike, in 128-row units), and
// that block's upper bound with the kid's boost folded in.
type wandCursor struct {
	p     *postings
	idfV  float64
	avg   float64
	boost float64
	doc   DocID
	ub    float64
	idx   int // open row
	blk   int // current 128-row block, open or sealed
	wrow  int // sealed row within blk
}

// scoreOrTopK routes an all-term OR through WAND under the index's own
// corpus statistics, mirroring scoreAndTopK.
func (ix *Index) scoreOrTopK(q Query, topK int, out []Scored) ([]Scored, bool) {
	kids := q.Kids
	if topK <= 0 || len(kids) == 0 || !allTerms(kids) {
		return out, false
	}
	return ix.scoreOrWandTopK(kids, q.boostOf(), topK, out, ix.nDocs,
		float64(ix.tokens)/float64(max(ix.nDocs, 1)), func(term uint64) int {
			if p := ix.post[term]; p != nil {
				return p.docCount()
			}
			return 0
		})
}

// scoreOrWandTopK appends an all-term OR's best topK hits in rank order,
// reporting false when the shape or selectivity favors streaming:
// unexpanded term kids only, topK > 0, and the pre-gate must pass. df is
// the shared view's document frequency (global for one index).
func (ix *Index) scoreOrWandTopK(kids []Query, boost float64, topK int, out []Scored,
	nDocs int, avg float64, df func(term uint64) int) ([]Scored, bool) {
	if topK <= 0 || len(kids) == 0 || !allTerms(kids) {
		return out, false
	}
	total, smallest, present := 0, 0, 0
	for _, k := range kids {
		p := ix.post[k.Term]
		if p == nil || p.docCount() == 0 {
			// A missing kid contributes nothing to the union.
			continue
		}
		if n := p.docCount(); smallest == 0 || n < smallest {
			smallest = n
		}
		total += p.docCount()
		present++
	}
	if total == 0 || nDocs == 0 {
		return out, true
	}
	if total > wandGateMinDocs && smallest*present > total/wandGatePre {
		return out, false
	}
	curs := ix.wandCurs[:0]
	for _, k := range kids {
		p := ix.post[k.Term]
		if p == nil || p.docCount() == 0 {
			continue
		}
		c := wandCursor{p: p, idfV: idf(nDocs, df(k.Term)), avg: avg, boost: k.boostOf()}
		ix.wandEnter(&c)
		curs = append(curs, c)
	}
	ix.wandCurs = curs[:0]
	h := ix.topHeap[:0]
	for {
		// Cursors stay sorted by document; the exhausted tail sorts
		// last, so the loop ends when the head is exhausted. k is
		// tiny (query kids), so insertion sort beats heap bookkeeping.
		// Strict less keeps equal docs stable and the walk
		// deterministic.
		for i := 1; i < len(curs); i++ {
			for j := i; j > 0 && curs[j].doc < curs[j-1].doc; j-- {
				curs[j], curs[j-1] = curs[j-1], curs[j]
			}
		}
		if curs[0].doc == wandExhausted {
			break
		}
		var pivot DocID
		if len(h) < topK {
			pivot = curs[0].doc
		} else {
			thresh := h[0].Score
			limit := thresh
			if boost != 0 {
				limit = thresh / boost
			}
			// Margin matches blockSkippable: advance while the
			// cumulative bound clears the threshold by less than
			// the rounding margin.
			bar := limit * (1 - 1e-9)
			cum := 0.0
			pivot = wandExhausted
			for i := range curs {
				if curs[i].doc == wandExhausted {
					break
				}
				cum += curs[i].ub
				if cum > bar {
					pivot = curs[i].doc
					break
				}
			}
			if pivot == wandExhausted {
				// No block left can beat the threshold.
				break
			}
		}
		if curs[0].doc == pivot {
			// Candidate: every cursor at the pivot contributes its
			// exact kid score; the document length probes once.
			dl := float64(ix.docLength(pivot))
			sum := 0.0
			i := 0
			for i < len(curs) && curs[i].doc == pivot {
				sum += bm25One(curs[i].idfV, avg, curs[i].boost, ix.wandFreq(&curs[i]), dl)
				i++
			}
			c := Scored{Doc: pivot, Score: sum * boost}
			if len(h) < topK || worseScored(h[0], c) {
				h = heapPush(h, topK, c)
			}
			for j := 0; j < i; j++ {
				ix.wandAdvance(&curs[j], pivot+1)
			}
		} else {
			for i := range curs {
				if curs[i].doc >= pivot {
					break
				}
				ix.wandAdvance(&curs[i], pivot)
			}
		}
	}
	ix.topHeap = h[:0]
	sortScored(h)
	return append(out, h...), true
}

// wandEnter positions a cursor at its first row and bounds that block.
func (ix *Index) wandEnter(c *wandCursor) {
	p := c.p
	if p.sealed == nil {
		c.idx, c.blk = 0, 0
		if len(p.ids) == 0 {
			c.doc = wandExhausted
			c.ub = 0
			return
		}
		c.doc = p.ids[0]
		c.ub = ix.wandOpenBound(c)
		return
	}
	s := p.sealed
	c.blk, c.wrow = 0, 0
	for c.blk < len(s.blk) {
		ids, _, _ := ix.sealedBlock(s, c.blk)
		if len(ids) > 0 {
			c.doc = ids[0]
			c.ub = ix.wandSealedBound(c, ids)
			return
		}
		c.blk++
	}
	c.doc = wandExhausted
	c.ub = 0
}

// wandOpenBound bounds the cursor's current 128-row block: peak offset
// delta over minimum probed length, through the shared blockBound.
func (ix *Index) wandOpenBound(c *wandCursor) float64 {
	p := c.p
	b0 := c.blk * sealedBlockRows
	b1 := b0 + sealedBlockRows
	if b1 > len(p.ids) {
		b1 = len(p.ids)
	}
	maxTF, minLen := uint32(0), uint32(0)
	for i := b0; i < b1; i++ {
		if f := p.off[i+1] - p.off[i]; f > maxTF {
			maxTF = f
		}
		if d := ix.docLength(p.ids[i]); minLen == 0 || d < minLen {
			minLen = d
		}
	}
	return blockBound(c.idfV, c.avg, c.boost, float64(maxTF), minLen)
}

// wandSealedBound bounds the cursor's current sealed block over its
// decoded ids: the count width caps every frequency for free, lengths
// probe per row. ids is this block's just-decoded array, consumed
// immediately under the no-hold rule.
func (ix *Index) wandSealedBound(c *wandCursor, ids []DocID) float64 {
	s := c.p.sealed
	maxTF := float64(uint64(1)<<s.blk[c.blk].cntW - 1)
	minLen := uint32(0)
	for _, id := range ids {
		if d := ix.docLength(id); minLen == 0 || d < minLen {
			minLen = d
		}
	}
	return blockBound(c.idfV, c.avg, c.boost, maxTF, minLen)
}

// wandAdvance moves a cursor to the first document at or past target,
// rebounding on block changes. Forward-only, like the traversal.
func (ix *Index) wandAdvance(c *wandCursor, target DocID) {
	p := c.p
	if p.sealed == nil {
		lo, hi := c.idx, len(p.ids)
		for lo < hi {
			m := lo + (hi-lo)/2
			if p.ids[m] < target {
				lo = m + 1
			} else {
				hi = m
			}
		}
		c.idx = lo
		if lo >= len(p.ids) {
			c.doc = wandExhausted
			c.ub = 0
			return
		}
		c.doc = p.ids[lo]
		if nb := lo / sealedBlockRows; nb != c.blk {
			c.blk = nb
			c.ub = ix.wandOpenBound(c)
		}
		return
	}
	s := p.sealed
	for {
		ids, _, _ := ix.sealedBlock(s, c.blk)
		lo, hi := c.wrow, len(ids)
		for lo < hi {
			m := lo + (hi-lo)/2
			if ids[m] < target {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo < len(ids) {
			c.wrow = lo
			c.doc = ids[lo]
			return
		}
		c.blk++
		c.wrow = 0
		if c.blk >= len(s.blk) {
			c.doc = wandExhausted
			c.ub = 0
			return
		}
		ids, _, _ = ix.sealedBlock(s, c.blk)
		if len(ids) == 0 {
			continue
		}
		c.doc = ids[0]
		c.ub = ix.wandSealedBound(c, ids)
	}
}

// wandFreq reads a cursor's frequency at its current document: the
// offset delta on open lists, the decoded count on sealed ones.
func (ix *Index) wandFreq(c *wandCursor) float64 {
	p := c.p
	if p.sealed == nil {
		return float64(p.off[c.idx+1] - p.off[c.idx])
	}
	_, cnt, _ := ix.sealedBlock(p.sealed, c.blk)
	return float64(cnt[c.wrow])
}
