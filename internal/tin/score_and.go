package tin

import "sort"

// Selective-AND top-K scoring.
//
// Score's old AND pipeline scores every kid's whole list, merges, then
// filters to the intersection: a `rare AND common` query scores tens of
// thousands of documents to keep a dozen. This path intersects first —
// matchTermsInto, the same keep the old pipeline filters by — and scores
// only matched documents into a bounded heap. Per-(term, doc) BM25 runs
// through the active kernel with pair-slot-matched rounding: list
// positions below the odd tail run paired (SIMD lanes are independent, so
// any pairing rounds exactly like the whole-list call), and a matched odd
// tail runs scalar exactly like the kernel tail. Kid contributions sum in
// kid order and the AND boost scales after, mirroring mergeScores and
// scaleScores.
//
// One deliberate deviation: mergeScores sums in the order an unstable
// doc-sort leaves equal keys, which depends on the full (unmatched)
// input — irreproducible without scoring everything. Kid-order summation
// is deterministic and differs by at most last-ulp rounding, so the AND
// differential asserts exact ranks with 1e-12 score agreement (the same
// standard as the wide-vs-scalar kernel differential), while single-term
// paths stay bit-identical.

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
	if topK <= 0 || !allTerms(kids) {
		return out, false
	}
	// Pre-gate on bare document frequencies, before paying a match.
	total := 0
	minDF := 0
	for i, k := range kids {
		df := 0
		if p := ix.post[k.Term]; p != nil {
			df = p.docCount()
		}
		total += df
		if i == 0 || df < minDF {
			minDF = df
		}
	}
	if len(kids) == 0 || int64(minDF)*int64(len(kids)) > int64(total)/andGatePre {
		return out, false
	}
	keep := ix.matchTermsInto(kids, ix.andKeep[:0])
	ix.andKeep = keep
	if int64(len(keep))*int64(len(kids)) > int64(total)/andGatePost {
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
	for _, k := range kids {
		p := ix.post[k.Term]
		if p == nil {
			// Unreachable with a nonempty keep (matchTermsInto
			// empties the intersection for missing kids); a missing
			// kid contributes nothing, mirroring scoreTerm.
			continue
		}
		n := p.docCount()
		idfV := idf(ix.nDocs, n)
		ix.andKidScores(p, n, idfV, avg, k.boostOf(), keep, dl, sums)
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

// andFreq resolves one term's frequency and list position for doc.
func (ix *Index) andFreq(p *postings, doc DocID) (uint32, int, bool) {
	if p.sealed != nil {
		s := p.sealed
		pos, ok := ix.sealedFindRow(s, doc)
		if !ok {
			return 0, 0, false
		}
		bl := &s.blk[pos/sealedBlockRows]
		wrow := uint32(pos % sealedBlockRows)
		r := packReaderSeek(s.cnts, bl.cntOff, wrow*uint32(bl.cntW))
		return r.next(bl.cntW), pos, true
	}
	pos := sort.Search(len(p.ids), func(i int) bool { return p.ids[i] >= doc })
	if pos >= len(p.ids) || p.ids[pos] != doc {
		return 0, 0, false
	}
	return p.off[pos+1] - p.off[pos], pos, true
}

// andKidScores accumulates one term kid's boost-scaled BM25 over keep into
// sums. Positions below the list's odd tail stage for the kernel;
// a matched tail runs scalar, exactly like the kernel's own tail.
func (ix *Index) andKidScores(p *postings, n int, idfV, avg, kidBoost float64, keep []DocID, dl, sums []float64) {
	ptf := ix.andPTF[:0]
	pdl := ix.andPDL[:0]
	pidx := ix.andPIdx[:0]
	paired := n &^ 1
	for i, d := range keep {
		f, pos, ok := ix.andFreq(p, d)
		if !ok {
			continue
		}
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
