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

// andGatePre / andGatePost bound the restricted path's work change. The
// pre-gate compares the smallest kid against the total without matching
// (a clearly unselective conjunction never pays a second match); the
// post-gate compares the actual intersection (a large keep falls back to
// streaming, which wins per-document at scale).
const (
	andGatePre  = 2
	andGatePost = 8
)

// scoreAndTopK appends an all-term AND's best topK hits in rank order,
// reporting false when the query falls outside the fast path (non-term
// kids, no top-K bound, or an unselective intersection the streaming
// pipeline serves better).
func (ix *Index) scoreAndTopK(q Query, topK int, out []Scored) ([]Scored, bool) {
	kids := q.Kids
	if topK <= 0 || len(kids) == 0 || !allTerms(kids) {
		return out, false
	}
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
	short := 0
	for i := range lists {
		if lists[i].docCount() < lists[short].docCount() {
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
	if int64(len(keep))*int64(len(lists)) > int64(total)/andGatePost {
		return out, false
	}
	if len(keep) == 0 || ix.nDocs == 0 {
		return out, true
	}
	avg := float64(ix.tokens) / float64(ix.nDocs)
	dl := ix.andDL[:0]
	for _, d := range keep {
		dl = append(dl, float64(ix.docLength(d)))
	}
	sums := ix.andSums[:0]
	for range keep {
		sums = append(sums, 0)
	}
	for ki, k := range kids {
		p := lists[ki]
		n := p.docCount()
		ix.andKidScores(p, n, idf(ix.nDocs, n), avg, k.boostOf(), poss[ki], dl, sums)
	}
	boost := q.boostOf()
	h := ix.topHeap[:0]
	for i, d := range keep {
		h = heapPush(h, topK, Scored{Doc: d, Score: sums[i] * boost})
	}
	ix.topHeap = h[:0]
	ix.andDL, ix.andSums = dl[:0], sums[:0]
	sortScored(h)
	return append(out, h...), true
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
	pos := ix.andPos[:0]
	for range lists {
		for range stride {
			pos = append(pos, 0)
		}
	}
	ix.andPos = pos
	lane := func(k int) []int { return pos[k*stride : (k+1)*stride] }
	for i := range driver {
		lane(0)[i] = i
	}
	for o := 1; o < len(lists); o++ {
		l := lists[o]
		if l.docCount() < gallopRatioGate*len(driver) {
			other := ix.appendList(ix.andOther[:0], l)
			ix.andOther = other
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

// andFreqAt seeks one term frequency by list position: arithmetic
// fixed-width seek sealed, offset difference open. No search involved.
func (ix *Index) andFreqAt(p *postings, pos int) uint32 {
	if p.sealed != nil {
		s := p.sealed
		bl := &s.blk[pos/sealedBlockRows]
		wrow := uint32(pos % sealedBlockRows)
		r := packReaderSeek(s.cnts, bl.cntOff, wrow*uint32(bl.cntW))
		return r.next(bl.cntW)
	}
	return p.off[pos+1] - p.off[pos]
}

// andKidScores accumulates one term kid's boost-scaled BM25 over keep
// into sums, reading frequencies by position. Positions below the list's
// odd tail stage for the kernel; a matched tail runs scalar, exactly
// like the kernel's own tail.
func (ix *Index) andKidScores(p *postings, n int, idfV, avg, kidBoost float64, poss []int, dl, sums []float64) {
	ptf := ix.andPTF[:0]
	pdl := ix.andPDL[:0]
	pidx := ix.andPIdx[:0]
	paired := n &^ 1
	for i, pos := range poss {
		f := ix.andFreqAt(p, pos)
		if pos < paired {
			ptf = append(ptf, float64(f))
			pdl = append(pdl, dl[i])
			pidx = append(pidx, i)
			continue
		}
		sums[i] += bm25One(idfV, avg, kidBoost, float64(f), dl[i])
	}
	if len(ptf)%2 == 1 {
		// Lanes compute independently: padding with any value rounds
		// every real element exactly as a paired lane.
		ptf = append(ptf, ptf[len(ptf)-1])
		pdl = append(pdl, pdl[len(pdl)-1])
	}
	if len(ptf) > 0 {
		sc := bm25Scores(idfV, avg, kidBoost, ptf, pdl, ix.scoreOut[:0])
		for j, i := range pidx {
			sums[i] += sc[j]
		}
		ix.scoreOut = sc[:0]
	}
	ix.andPTF, ix.andPDL, ix.andPIdx = ptf[:0], pdl[:0], pidx[:0]
}
