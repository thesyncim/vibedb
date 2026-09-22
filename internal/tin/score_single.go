package tin

// Transient single-document BM25 scoring. ScoreSingle answers what
// ix.Score attributes to one document, without touching the index: the
// caller pins collection statistics once per execution (ScoreStats) and
// scores rows lock-free against them. SQL SCORE() runs on this.
//
// The mirror rule: every arm below must produce bit-identical output to
// the corresponding scoreInto arm for the same document, given the same
// statistics. Term frequencies come from the row's own spans (positions
// for terms, span counts otherwise — exactly what scoreInto feeds the
// kernel), document frequency from the pinned per-node idf, length from
// the row's token count. TestScoreSingleAgreesWithScore proves the mirror
// over the whole operator corpus; any divergence there is a bug here, not
// drift, so the two switches must stay structurally identical — including
// the explicit operator lists, never `default`, so adding an operator to
// one forces a compile-time-adjacent decision in the other.

// ScoreStats pins a query's collection statistics for transient scoring.
// Refresh it per execution: dictionaries move between generations, and so
// do frequencies. Buffers persist across refreshes, so warm executions
// allocate nothing.
type ScoreStats struct {
	avg   float64
	nDocs int
	idfs  []float64
	buf   []DocID
}

// RefreshScoreStats recomputes st for q: average length plus one idf per
// idf-bearing node in preorder. Nodes that score by summing children
// (And, Or, AndNot, AtLeast) and OpAll carry no idf. The preorder must
// match scoreSingleInto's consumption order exactly.
func (ix *Index) RefreshScoreStats(q Query, st *ScoreStats) *ScoreStats {
	if st == nil {
		st = &ScoreStats{}
	}
	ix.mu.Lock()
	ix.ensureSorted()
	nDocs, tokens := ix.nDocs, ix.tokens
	ix.mu.Unlock()
	st.nDocs = nDocs
	if nDocs == 0 {
		st.avg = 0
	} else {
		st.avg = float64(tokens) / float64(nDocs)
	}
	st.idfs = st.idfs[:0]
	var walk func(q Query)
	walk = func(q Query) {
		switch q.Op {
		case OpTerm, OpPhrase,
			OpThen, OpNear, OpWithin,
			OpEncloses, OpEnclosedBy, OpOverlapping, OpBefore, OpAfter,
			OpFilter:
			st.buf = ix.Match(q, st.buf[:0])
			st.idfs = append(st.idfs, idf(nDocs, len(st.buf)))
		}
		for _, k := range q.Kids {
			walk(k)
		}
	}
	walk(q)
	return st
}

// ScoreSingle reports text's BM25 score for q under the pinned statistics,
// with the same value ix.Score attributes to the document. A row that does
// not match scores 0 (like an unscored document, which simply has no
// entry). Warm scratch makes the call allocation-free: spans reuse the
// scratch arena, and the BM25 math is scalar.
func ScoreSingle(text string, q Query, st *ScoreStats, scratch *TextScratch) float64 {
	if st == nil {
		return 0
	}
	pairs := scanPairs(text, scratch.pairs[:0])
	if !needsPositions(q) {
		view := docView{pairs: pairs, length: uint32(len(pairs)), scratch: scratch}
		cur := scoreCursor{st: st}
		score, _ := view.scoreSingleUnsorted(q, &cur)
		scratch.pairs = pairs[:0]
		return score
	}
	sortTokPos(pairs)
	view := docView{pairs: pairs, length: uint32(len(pairs)), scratch: scratch}
	scratch.arena = scratch.arena[:0]
	cur := scoreCursor{st: st}
	score, _ := view.scoreSingleInto(q, &cur)
	scratch.pairs = pairs[:0]
	return score
}

// scoreSingleUnsorted mirrors scoreSingleInto over unsorted pairs for
// queries that never observe positions: term frequencies are linear
// counts, and the tree walk consumes pinned idfs in the same preorder,
// so values are bit-identical to the sorted path. Positional operators
// cannot reach here (needsPositions gates); the default arm still
// consumes idfs exactly like RefreshScoreStats' walk, so the cursor
// stays in sync even if the two ever disagree.
func (v docView) scoreSingleUnsorted(q Query, cur *scoreCursor) (float64, bool) {
	st := cur.st
	dl := float64(len(v.pairs))
	count := func(term uint64) float64 {
		n := 0
		for _, tp := range v.pairs {
			if tp.hash == term {
				n++
			}
		}
		return float64(n)
	}
	switch q.Op {
	case OpTerm:
		idf := cur.nextIdf()
		if st.nDocs == 0 {
			return 0, false
		}
		tf := count(q.Term)
		if tf == 0 {
			return 0, false
		}
		return bm25One(idf, st.avg, q.boostOf(), tf, dl), true
	case OpAnd:
		if len(q.Kids) == 0 {
			return 0, false
		}
		var sum float64
		matched := true
		for _, k := range q.Kids {
			s, ok := v.scoreSingleUnsorted(k, cur)
			if !ok {
				matched = false
				continue
			}
			sum += s
		}
		if !matched {
			return 0, false
		}
		return sum * q.boostOf(), true
	case OpOr:
		var sum float64
		matched := false
		for _, k := range q.Kids {
			s, ok := v.scoreSingleUnsorted(k, cur)
			if ok {
				matched = true
				sum += s
			}
		}
		if !matched {
			return 0, false
		}
		return sum * q.boostOf(), true
	case OpAndNot:
		if len(q.Kids) == 0 {
			return 0, false
		}
		s, ok := v.scoreSingleUnsorted(q.Kids[0], cur)
		if !ok {
			return 0, false
		}
		for _, k := range q.Kids[1:] {
			_, banned := v.scoreSingleUnsorted(k, cur)
			if banned {
				return 0, false
			}
		}
		return s * q.boostOf(), true
	case OpAll:
		return q.boostOf(), true
	case OpAtLeast:
		var sum float64
		matched := 0
		for _, k := range q.Kids {
			s, ok := v.scoreSingleUnsorted(k, cur)
			if ok {
				matched++
				sum += s
			}
		}
		if matched < q.Threshold {
			return 0, false
		}
		return sum * q.boostOf(), true
	default:
		switch q.Op {
		case OpTerm, OpPhrase,
			OpThen, OpNear, OpWithin,
			OpEncloses, OpEnclosedBy, OpOverlapping, OpBefore, OpAfter,
			OpFilter:
			cur.nextIdf()
		}
		for _, k := range q.Kids {
			v.scoreSingleUnsorted(k, cur)
		}
		return 0, false
	}
}

// scoreCursor walks the pinned idfs in preorder, mirroring
// RefreshScoreStats' walk.
type scoreCursor struct {
	st *ScoreStats
	at int
}

// nextIdf consumes the next pinned idf.
func (c *scoreCursor) nextIdf() float64 {
	idf := c.st.idfs[c.at]
	c.at++
	return idf
}

// scoreSingleInto mirrors scoreInto for one document: it returns the row's
// score contribution and whether the row matches. Term frequencies are the
// row's own span counts; membership is non-empty spans, the same test
// MatchSingle uses.
func (v docView) scoreSingleInto(q Query, cur *scoreCursor) (float64, bool) {
	st := cur.st
	dl := float64(len(v.pairs))
	switch q.Op {
	case OpTerm:
		idf := cur.nextIdf()
		if st.nDocs == 0 {
			return 0, false
		}
		tf := float64(len(v.positions(q.Term)))
		if tf == 0 {
			return 0, false
		}
		return bm25One(idf, st.avg, q.boostOf(), tf, dl), true
	case OpPhrase:
		idf := cur.nextIdf()
		if st.nDocs == 0 {
			return 0, false
		}
		s := v.scratch
		spans := v.phraseSpans(q.Phrase, q.Slop, s.arena[len(s.arena):len(s.arena)])
		s.arena = spans
		tf := float64(len(spans))
		if tf == 0 {
			return 0, false
		}
		return bm25One(idf, st.avg, q.boostOf(), tf, dl), true
	case OpAnd:
		if len(q.Kids) == 0 {
			return 0, false
		}
		// No short-circuit: every kid consumes its pinned idfs in
		// preorder, matched or not, so the cursor stays in sync.
		var sum float64
		matched := true
		for _, k := range q.Kids {
			s, ok := v.scoreSingleInto(k, cur)
			if !ok {
				matched = false
				continue
			}
			sum += s
		}
		if !matched {
			return 0, false
		}
		return sum * q.boostOf(), true
	case OpOr:
		var sum float64
		matched := false
		for _, k := range q.Kids {
			s, ok := v.scoreSingleInto(k, cur)
			if ok {
				matched = true
				sum += s
			}
		}
		if !matched {
			return 0, false
		}
		return sum * q.boostOf(), true
	case OpAndNot:
		if len(q.Kids) == 0 {
			return 0, false
		}
		s, ok := v.scoreSingleInto(q.Kids[0], cur)
		if !ok {
			return 0, false
		}
		for _, k := range q.Kids[1:] {
			_, banned := v.scoreSingleInto(k, cur)
			if banned {
				return 0, false
			}
		}
		return s * q.boostOf(), true
	case OpAll:
		return q.boostOf(), true
	case OpAtLeast:
		var sum float64
		matched := 0
		for _, k := range q.Kids {
			s, ok := v.scoreSingleInto(k, cur)
			if ok {
				matched++
				sum += s
			}
		}
		if matched < q.Threshold {
			return 0, false
		}
		return sum * q.boostOf(), true
	case OpThen, OpNear, OpWithin,
		OpEncloses, OpEnclosedBy, OpOverlapping, OpBefore, OpAfter,
		OpFilter:
		idf := cur.nextIdf()
		if st.nDocs == 0 {
			return 0, false
		}
		s := v.scratch
		spans := v.evalInto(q, s.arena[len(s.arena):len(s.arena)])
		s.arena = spans
		tf := float64(len(spans))
		if tf == 0 {
			return 0, false
		}
		return bm25One(idf, st.avg, q.boostOf(), tf, dl), true
	default:
		// Unknown operator: consume the subtree's idfs in preorder so
		// the cursor stays in sync, then report no match.
		for _, k := range q.Kids {
			v.scoreSingleInto(k, cur)
		}
		return 0, false
	}
}
