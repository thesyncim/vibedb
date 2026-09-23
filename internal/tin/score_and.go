package tin

// Selective-AND top-K scoring.
//
// Score's old AND pipeline scores every kid's whole list, merges, then
// filters to the intersection: a `rare AND common` query scores tens of
// thousands of documents to keep a dozen. This path intersects first and
// scores only matched documents. The intersect tracks every keep doc's
// list position per kid — driver positions ride the filter, siblings
// resolve one position per driver doc — so scoring seeks frequencies
// directly and never re-probes: one membership probe per (kid, doc),
// period. Per-(term, doc) BM25 runs through the active kernel with
// pair-slot-matched rounding: positions below the odd tail run paired
// (SIMD lanes are independent, so any pairing rounds exactly like the
// whole-list call), and a matched odd tail runs scalar exactly like the
// kernel tail. Kids accumulate shortest-first in a deterministic order
// with the AND boost scaling after, mirroring mergeScores/scaleScores.
//
// One deliberate deviation: mergeScores sums in the order an unstable
// doc-sort leaves equal keys, which depends on the full (unmatched)
// input — irreproducible without scoring everything. Shortest-first
// summation is deterministic and differs by at most last-ulp rounding,
// so the AND differential asserts exact ranks with 1e-12 score agreement
// (the same standard as the wide-vs-scalar kernel differential), while
// single-term paths stay bit-identical.

// andGatePre bounds the restricted path's work change: it compares the
// smallest kid against the total without matching, so a clearly
// unselective conjunction never pays a second match. The post-gate runs
// after the intersect, when the keep set is known, and compares keep work
// against streaming work directly: the intersect already tracked every
// keep doc's per-list position, so scoring the keep is direct reads plus
// kernel rows with no further probes, while streaming would score every
// list whole plus merge, filter, and select. The keep path proceeds
// whenever its document work does not exceed the total streaming work.
const andGatePre = 2

// scoreAndTopK appends an all-term AND's best topK hits in rank order,
// reporting false when the query falls outside the fast path (non-term
// kids, no top-K bound, or an unselective intersection the streaming
// pipeline serves better).
func (ix *Index) scoreAndTopK(q Query, topK int, out []Scored) ([]Scored, bool) {
	kids := q.Kids
	if topK <= 0 || len(kids) == 0 || !allTerms(kids) {
		return out, false
	}
	return ix.scoreAndTopKCore(kids, q.boostOf(), topK, out, ix.nDocs,
		float64(ix.tokens)/float64(max(ix.nDocs, 1)), func(term uint64) int {
			if p := ix.post[term]; p != nil {
				return p.docCount()
			}
			return 0
		})
}

// scoreAndTopKCore is scoreAndTopK with caller-supplied corpus statistics:
// nDocs and avg anchor the shared view every shard ranks under, while df
// resolves each kid's global document frequency. List handling — the
// missing-kid empty, shortest-first reorder, selectivity gates — stays
// local: it gates the shard's own work. A shard missing a kid, or holding
// no documents, contributes nothing and still reports true, so the merge
// stays exact.
func (ix *Index) scoreAndTopKCore(kids []Query, boost float64, topK int, out []Scored,
	nDocs int, avg float64, df func(term uint64) int) ([]Scored, bool) {
	// Pre-gate on bare document frequencies, before paying a match.
	lists := ix.termLists[:0]
	total := 0
	for _, k := range kids {
		p := ix.post[k.Term]
		if p == nil {
			// A missing kid empties the intersection, mirroring
			// matchTermsInto; nothing scores, nothing appends.
			ix.termLists = lists
			return out, true
		}
		lists = append(lists, p)
		total += p.docCount()
	}
	ix.termLists = lists
	// Shortest-first by the shared df view, not local counts: every
	// shard then accumulates kid scores in the single index's own kid
	// order, keeping the merged sums bit-identical. Locally df is the
	// list count, so the routed path orders exactly as before.
	short := 0
	for i := range lists {
		if df(kids[i].Term) < df(kids[short].Term) {
			short = i
		}
	}
	lists[0], lists[short] = lists[short], lists[0]
	if short != 0 {
		kids = append(ix.andKids[:0], kids...)
		kids[0], kids[short] = kids[short], kids[0]
		ix.andKids = kids
	}
	if lists[0].docCount()*len(lists) > total/andGatePre {
		return out, false
	}
	keep, poss := ix.andIntersect(lists)
	if int64(len(keep))*int64(len(lists)) > int64(total) {
		return out, false
	}
	if len(keep) == 0 || nDocs == 0 {
		return out, true
	}
	// Bulk-extend the staging lanes once per query so the fill loops
	// below store by index with no per-row bounds checks; sums are
	// re-zeroed because the scratch persists across calls.
	dl := extendN(ix.andDL[:0], len(keep))
	for i, d := range keep {
		dl[i] = float64(ix.docLength(d))
	}
	sums := extendN(ix.andSums[:0], len(keep))
	clear(sums)
	for ki, k := range kids {
		p := lists[ki]
		ix.andKidScores(p, idf(nDocs, df(k.Term)), avg, k.boostOf(), poss[ki], dl, sums)
	}
	h := ix.topHeap[:0]
	for i, d := range keep {
		// heapPush re-checks this first; skipping the call for
		// rejected rows keeps the hot loop call-free, mirroring the
		// single-term path.
		c := Scored{Doc: d, Score: sums[i] * boost}
		if len(h) < topK || worseScored(h[0], c) {
			h = heapPush(h, topK, c)
		}
	}
	ix.topHeap = h[:0]
	ix.andDL, ix.andSums = dl[:0], sums[:0]
	sortScored(h)
	return append(out, h...), true
}

// scoreAndFull appends the full all-term AND ranking in rank order: the
// top-K core's intersect, deterministic shortest-first accumulation, and
// kernel lanes, but with no gates and no heap — every keep document
// scores, so the full path never declines. df is the shared view's
// document frequency (global for one index, pinned for segments), which
// keeps kid order identical wherever the same corpus is scored: merged
// per-shard full rankings then carry the same sums the single index
// would, up to the last-ulp ordering the AND contract already allows.
// Lanes ride the same index-owned staging as the top-K core.
func (ix *Index) scoreAndFull(kids []Query, boost float64, out []Scored,
	nDocs int, avg float64, df func(term uint64) int) ([]Scored, bool) {
	lists := ix.termLists[:0]
	for _, k := range kids {
		p := ix.post[k.Term]
		if p == nil {
			ix.termLists = lists
			return out, true
		}
		lists = append(lists, p)
	}
	ix.termLists = lists
	// Shortest-first by the shared df view, exactly like the top-K core:
	// the same corpus accumulates kids in the same order on one index
	// and on every segment.
	short := 0
	for i := range lists {
		if df(kids[i].Term) < df(kids[short].Term) {
			short = i
		}
	}
	lists[0], lists[short] = lists[short], lists[0]
	if short != 0 {
		kids = append(ix.andKids[:0], kids...)
		kids[0], kids[short] = kids[short], kids[0]
		ix.andKids = kids
	}
	keep, poss := ix.andIntersect(lists)
	if len(keep) == 0 || nDocs == 0 {
		return out, true
	}
	dl := extendN(ix.andDL[:0], len(keep))
	for i, d := range keep {
		dl[i] = float64(ix.docLength(d))
	}
	sums := extendN(ix.andSums[:0], len(keep))
	clear(sums)
	for ki, k := range kids {
		p := lists[ki]
		ix.andKidScores(p, idf(nDocs, df(k.Term)), avg, k.boostOf(), poss[ki], dl, sums)
	}
	base := len(out)
	for i, d := range keep {
		out = append(out, Scored{Doc: d, Score: sums[i] * boost})
	}
	ix.andDL, ix.andSums = dl[:0], sums[:0]
	sortScored(out[base:])
	return out, true
}

// andIntersect intersects shortest-first lists, tracking every keep
// doc's position per list. It returns the keep set with one position
// lane per list; lanes share andPos in stride of the driver length.
// The keep SET matches matchTermsInto exactly (same driver, same gates,
// same order); only the tracked positions are new.
func (ix *Index) andIntersect(lists []*postings) ([]DocID, [][]int) {
	driver := ix.appendList(ix.andKeep[:0], lists[0])
	ix.andKeep = driver
	stride := len(driver)
	// One bulk extension for all lanes: every slot is overwritten
	// before any read in the loops below, so no per-slot append
	// bounds checks and no zeroing.
	pos := extendN(ix.andPos[:0], len(lists)*stride)
	ix.andPos = pos
	lane := func(k int) []int { return pos[k*stride : (k+1)*stride] }
	for i := range driver {
		lane(0)[i] = i
	}
	for o := 1; o < len(lists); o++ {
		l := lists[o]
		if l.docCount() < gallopRatioGate*len(driver) {
			// The sibling side is read-only in the merge, so an
			// open list walks in place: only sealed lists pay the
			// andOther decode.
			other := l.ids
			if l.sealed != nil {
				other = ix.appendList(ix.andOther[:0], l)
				ix.andOther = other
			}
			w, i, j := 0, 0, 0
			for i < len(driver) && j < len(other) {
				switch {
				case driver[i] < other[j]:
					i++
				case driver[i] > other[j]:
					j++
				default:
					driver[w] = driver[i]
					for k := 0; k < o; k++ {
						lane(k)[w] = lane(k)[i]
					}
					lane(o)[w] = j
					w++
					i++
					j++
				}
			}
			driver = driver[:w]
			continue
		}
		w := 0
		for i, d := range driver {
			p, ok := ix.listPos(l, d)
			if !ok {
				continue
			}
			driver[w] = d
			for k := 0; k < o; k++ {
				lane(k)[w] = lane(k)[i]
			}
			lane(o)[w] = p
			w++
		}
		driver = driver[:w]
		if len(driver) == 0 {
			break
		}
	}
	ix.andKeep = driver
	poss := ix.andPoss[:0]
	for k := range lists {
		poss = append(poss, lane(k)[:len(driver)])
	}
	ix.andPoss = poss
	return driver, poss
}

// listPos resolves one document's position in either layout: binary
// search over the open array, block index plus cached search sealed.
func (ix *Index) listPos(p *postings, doc DocID) (int, bool) {
	if p.sealed == nil {
		lo, hi := 0, len(p.ids)
		for lo < hi {
			m := lo + (hi-lo)/2
			if p.ids[m] < doc {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo < len(p.ids) && p.ids[lo] == doc {
			return lo, true
		}
		return 0, false
	}
	return ix.sealedFindRow(p.sealed, doc)
}

// andKidScores accumulates one term kid's boost-scaled BM25 over keep
// into sums, reading frequencies by position. Positions below the list's
// odd tail stage for the kernel; a matched tail runs scalar, exactly
// like the kernel's own tail.
// extendN grows s by n elements reusing capacity, allocating only on
// growth; the caller overwrites every new element before reading, so
// fill loops store by index with no per-row append bounds checks.
func extendN[S ~[]E, E any](s S, n int) S {
	if n > cap(s)-len(s) {
		ns := make(S, len(s), len(s)+n)
		copy(ns, s)
		s = ns
	}
	return s[:len(s)+n]
}

func (ix *Index) andKidScores(p *postings, idfV, avg, kidBoost float64, poss []int, dl, sums []float64) {
	// Staging stores by index into one bulk extension: no per-row
	// append bounds checks. The fill splits by layout so the hot
	// loops stay call-free (andFreqAt does not inline); both stage
	// the identical values in keep order.
	ptf := extendN(ix.andPTF[:0], len(poss))
	pdl := extendN(ix.andPDL[:0], len(poss))
	if p.sealed == nil {
		for i, pos := range poss {
			ptf[i] = float64(p.off[pos+1] - p.off[pos])
			pdl[i] = dl[i]
		}
	} else {
		s := p.sealed
		for i, pos := range poss {
			bl := &s.blk[pos/sealedBlockRows]
			wrow := uint32(pos % sealedBlockRows)
			r := packReaderSeek(s.cnts, bl.cntOff, wrow*uint32(bl.cntW))
			ptf[i] = float64(r.next(bl.cntW))
			pdl[i] = dl[i]
		}
	}
	// Every keep document rides the kernel lanes (odd counts pad
	// inside bm25Scores): lane assignment never depends on list
	// length, so sharded and single indexes score each document
	// identically. Routing tails through the scalar spelling instead
	// measured 1-ulp flips that broke rank identity.
	if len(ptf) > 0 {
		sc := bm25Scores(idfV, avg, kidBoost, ptf, pdl, ix.scoreOut[:0])
		for j := range sc {
			sums[j] += sc[j]
		}
		ix.scoreOut = sc[:0]
	}
	ix.andPTF, ix.andPDL = ptf[:0], pdl[:0]
}
