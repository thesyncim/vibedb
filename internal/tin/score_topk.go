package tin

// Single-term top-K scoring with block-max skipping.
//
// Score's old pipeline stages every hit (scoreTerm gathers tf/dl arrays,
// runs the kernel, appends one Scored per document) and then selects the
// top-K with quickselect — two full passes over n plus the gather traffic.
// A single-term top-K query needs neither: this path scores straight into
// a bounded worst-first heap, and skips whole 128-row blocks whose BM25
// upper bound cannot beat the heap threshold (BlockMax WAND over one
// term). The bound comes from data the seal already stores — the block's
// count width caps its max frequency, the index minimum length floors its
// lengths — so skipping costs zero space: no per-block maxima are stored.
// Open lists chunk the same way, tracking the chunk maximum while filling
// frequencies.
//
// Bit-identity with scoreTerm+topKScored is structural, not statistical:
// every kept row runs through the same active kernel (wide or scalar) over
// the same values in document order, and 128-row boundaries preserve the
// wide kernel's global pair grid (128 is even), so rounding matches the
// whole-list call exactly; the heap keeps the identical total order
// (score descending, DocID ascending) that sortScored establishes.
//
// Skipping engages only when the heap is full, the threshold is positive,
// and the boost is nonnegative. idf needs no gate: this BM25 variant is
// Log1p of a positive quantity, always positive, so with boost >= 0 every
// score is nonnegative and the formula rises in tf and falls in dl — the
// bound binds. Negative boosts take the heap path without skipping, which
// stays exact. A 1e-9 relative margin under the threshold keeps float
// rounding from ever skipping a boundary tie.

// scoreSingleTopK appends the term's best topK hits in rank order,
// reporting false when the query falls outside the fast path (missing
// term, empty index, or topK outside 1..nHits-1, which the old pipeline
// serves directly).
func (ix *Index) scoreSingleTopK(term uint64, boost float64, topK int, out []Scored) ([]Scored, bool) {
	p := ix.post[term]
	if p == nil || ix.nDocs == 0 {
		return out, false
	}
	n := p.docCount()
	if topK <= 0 || topK >= n {
		return out, false
	}
	return ix.scoreSingleTopKCore(p, ix.nDocs, float64(ix.tokens)/float64(ix.nDocs), n,
		boost, topK, out)
}

// scoreSingleTopKCore scores one term's list into a sorted top-K run with
// caller-supplied corpus statistics, so indexes sharing one stats view
// rank identically. A missing list contributes nothing; unlike the
// routed wrapper it never declines, since a shard's shortfall still
// merges exactly. Callers guarantee topK > 0: the heap root read below
// needs a nonempty heap once full.
func (ix *Index) scoreSingleTopKCore(p *postings, nDocs int, avg float64, df int,
	boost float64, topK int, out []Scored) ([]Scored, bool) {
	if p == nil || nDocs == 0 {
		return out, true
	}
	idfV := idf(nDocs, df)
	h := ix.topHeap[:0]
	if p.sealed == nil {
		h = ix.scoreOpenTopK(p, idfV, avg, boost, topK, h)
	} else {
		h = ix.scoreSealedTopK(p.sealed, idfV, avg, boost, topK, h)
	}
	ix.topHeap = h[:0]
	sortScored(h)
	return append(out, h...), true
}

// blockBound upper-bounds every BM25 score in a block whose frequencies
// peak at maxTF and whose lengths clear minLen, for nonnegative boost
// (idf is always positive here). The formula's tf quotient rises in tf
// and its denominator rises in dl.
func blockBound(idfV, avg, boost, maxTF float64, minLen uint32) float64 {
	den := maxTF + bm25K1*(1-bm25B+bm25B*float64(minLen)/avg)
	return boost * idfV * maxTF * (bm25K1 + 1) / den
}

// blockSkippable reports whether a block peaking at maxTF can be dropped:
// the heap is full past zero and the block's upper bound clears the
// threshold by less than the rounding margin.
func blockSkippable(idfV, avg, boost, maxTF float64, minLen uint32, thresh float64) bool {
	if thresh <= 0 {
		return false
	}
	return blockBound(idfV, avg, boost, maxTF, minLen) <= thresh*(1-1e-9)
}

// worseScored orders hits worst-first for the heap: lower score loses,
// and tied scores lose by larger DocID — the exact inverse of the final
// rank order, so the root is always the current threshold.
func worseScored(a, b Scored) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	return a.Doc > b.Doc
}

// heapPush inserts c into the worst-first heap h bounded at k hits. Equal
// (score, doc) pairs cannot occur — documents are unique per term — so a
// strict improvement test never drops a hit the old pipeline would keep.
func heapPush(h []Scored, k int, c Scored) []Scored {
	if len(h) < k {
		h = append(h, c)
		up := len(h) - 1
		for up > 0 {
			parent := (up - 1) / 2
			if !worseScored(h[up], h[parent]) {
				break
			}
			h[up], h[parent] = h[parent], h[up]
			up = parent
		}
		return h
	}
	if worseScored(h[0], c) {
		h[0] = c
		down := 0
		for {
			left := 2*down + 1
			if left >= len(h) {
				break
			}
			small := left
			if right := left + 1; right < len(h) && worseScored(h[right], h[left]) {
				small = right
			}
			if !worseScored(h[small], h[down]) {
				break
			}
			h[down], h[small] = h[small], h[down]
			down = small
		}
	}
	return h
}

// scoreSealedTopK scores one sealed term into the heap, skipping blocks
// whose count width already proves them threshold-cold.
func (ix *Index) scoreSealedTopK(s *sealedPostings, idfV, avg, boost float64, topK int, h []Scored) []Scored {
	skip := boost >= 0
	// Chunk staging rides the index-owned kernel scratch (heap buffers
	// shared with scoreTerm): stack arrays would escape through the
	// indirect kernel call and allocate per block.
	tf := ix.scoreTF[:0]
	dl := ix.scoreDL[:0]
	sc := ix.scoreOut[:0]
	ids := ix.decIDs[:0]
	// Hoisted length-cache probe, mirroring scoreOpenTopK: decoded ids
	// walk document order, so rows share one dense length array; the
	// slow path is docLength itself.
	var larr []uint32
	var lbase uint64
	var lkey uint32
	lok := false
	for b := range s.blk {
		bl := &s.blk[b]
		rows := int(bl.rows)
		// cntW is the width of the block's peak frequency, so
		// (1<<cntW)-1 caps every frequency in the block for free.
		if skip && len(h) == topK &&
			blockSkippable(idfV, avg, boost, float64(uint64(1)<<bl.cntW-1), ix.minDocLen, h[0].Score) {
			continue
		}
		idR := packReaderAt(s.ids, bl.idOff)
		cntR := packReaderAt(s.cnts, bl.cntOff)
		id := DocID(s.first[b])
		base := len(ids)
		// Stores by index into one bulk extension per block: the
		// decode loop carries no append bounds checks.
		ids = extendN(ids, rows)
		tf = extendN(tf[:0], rows)
		dl = extendN(dl[:0], rows)
		for k := 0; k < rows; k++ {
			if k > 0 {
				id += DocID(idR.next(bl.idW))
			}
			c := cntR.next(bl.cntW)
			ids[base+k] = id
			tf[k] = float64(c)
			key := uint32(uint64(id) >> 32)
			slot := uint64(id) & 0xffffffff
			if lok && key == lkey {
				if d := slot - lbase; d < uint64(len(larr)) {
					dl[k] = float64(larr[d])
					continue
				}
			}
			dl[k] = float64(ix.docLength(id))
			lok = ix.lenCacheOK
			lkey = ix.lenCacheKey
			larr = ix.lenCacheArr
			lbase = ix.lenCacheBase
		}
		sc = bm25Scores(idfV, avg, boost, tf, dl, sc[:0])
		for i, id := range ids[base:] {
			// heapPush re-checks this first; skipping the call for
			// rejected rows keeps the hot loop call-free.
			c := Scored{Doc: id, Score: sc[i]}
			if len(h) < topK || worseScored(h[0], c) {
				h = heapPush(h, topK, c)
			}
		}
	}
	ix.decIDs = ids
	ix.scoreTF, ix.scoreDL, ix.scoreOut = tf[:0], dl[:0], sc[:0]
	return h
}

// scoreOpenTopK scores one open term into the heap, tracking each 128-row
// chunk's peak frequency while filling and skipping threshold-cold chunks
// before touching lengths or the kernel.
func (ix *Index) scoreOpenTopK(p *postings, idfV, avg, boost float64, topK int, h []Scored) []Scored {
	skip := boost >= 0
	tf := ix.scoreTF[:0]
	dl := ix.scoreDL[:0]
	sc := ix.scoreOut[:0]
	var ids [sealedBlockRows]DocID
	// Hoisted length-cache probe: scoring walks DocID order, so rows of
	// a chunk share one dense length array; the slow path is docLength
	// itself, which keeps the shared cache fields evolving identically.
	var larr []uint32
	var lbase uint64
	var lkey uint32
	lok := false
	n := len(p.ids)
	for base := 0; base < n; base += sealedBlockRows {
		end := base + sealedBlockRows
		if end > n {
			end = n
		}
		rows := end - base
		maxF := uint32(0)
		// Stores by index into one bulk extension per chunk: the
		// fill loop carries no append bounds checks.
		tf = extendN(tf[:0], rows)
		dl = extendN(dl[:0], rows)
		for i := 0; i < rows; i++ {
			id := p.ids[base+i]
			f := p.off[base+i+1] - p.off[base+i]
			tf[i] = float64(f)
			if f > maxF {
				maxF = f
			}
			ids[i] = id
			key := uint32(uint64(id) >> 32)
			slot := uint64(id) & 0xffffffff
			if lok && key == lkey {
				if d := slot - lbase; d < uint64(len(larr)) {
					dl[i] = float64(larr[d])
					continue
				}
			}
			dl[i] = float64(ix.docLength(id))
			lok = ix.lenCacheOK
			lkey = ix.lenCacheKey
			larr = ix.lenCacheArr
			lbase = ix.lenCacheBase
		}
		if skip && len(h) == topK &&
			blockSkippable(idfV, avg, boost, float64(maxF), ix.minDocLen, h[0].Score) {
			continue
		}
		sc = bm25Scores(idfV, avg, boost, tf, dl, sc[:0])
		for i := 0; i < rows; i++ {
			// heapPush re-checks this first; skipping the call for
			// rejected rows keeps the hot loop call-free.
			c := Scored{Doc: ids[i], Score: sc[i]}
			if len(h) < topK || worseScored(h[0], c) {
				h = heapPush(h, topK, c)
			}
		}
	}
	ix.scoreTF, ix.scoreDL, ix.scoreOut = tf[:0], dl[:0], sc[:0]
	return h
}
