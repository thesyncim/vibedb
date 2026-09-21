package tin

import (
	"cmp"
	"math"
	"slices"
)

// Query evaluation: terms, phrases with slop, boolean operators, match-all,
// and BM25 top-K. Queries are plain values over term hashes — the TINQL
// parser (a later slice) compiles surface syntax down to this AST, and the
// SQL ==> operator will lower to it too.

// Op tags a Query node.
type Op uint8

const (
	// OpTerm matches documents containing Term.
	OpTerm Op = iota
	// OpPhrase matches documents where Terms occur in order with at most
	// Slop extra words between consecutive terms (Slop 0 is adjacency).
	OpPhrase
	// OpAnd matches documents matching every kid.
	OpAnd
	// OpOr matches documents matching any kid.
	OpOr
	// OpAndNot matches documents matching Kids[0] and no later kid.
	OpAndNot
	// OpAll matches every document (TIN's standalone `*`).
	OpAll
)

// Query is one evaluator node. Only the fields its Op reads are meaningful:
// Term for OpTerm; Terms and Slop for OpPhrase; Kids for the boolean ops;
// Boost scales the node's BM25 contribution (1 when unset... see Score).
type Query struct {
	Op    Op
	Term  uint64
	Terms []uint64
	Slop  int
	Kids  []Query
	Boost float32
}

// boostOf normalizes the unset/zero boost to 1.
func (q Query) boostOf() float64 {
	if q.Boost == 0 {
		return 1
	}
	return float64(q.Boost)
}

// Match appends every document matching q, ascending, and returns out.
// Single-level queries (terms, phrases, match-all) append straight into out
// without scratch; each boolean level reuses its accumulator in place, so
// combination costs one allocation per level, never per document.
func (ix *Index) Match(q Query, out []DocID) []DocID {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()
	return ix.matchInto(q, out)
}

func (ix *Index) matchInto(q Query, out []DocID) []DocID {
	switch q.Op {
	case OpAll:
		base := len(out)
		for id := range ix.docs {
			out = append(out, id)
		}
		head := out[base:]
		sortDocIDs(head)
		return out
	case OpTerm:
		if p := ix.post[q.Term]; p != nil {
			out = append(out, p.ids...)
		}
		return out
	case OpPhrase:
		return ix.matchPhrase(q.Terms, q.Slop, out)
	case OpAnd:
		if len(q.Kids) == 0 {
			return out
		}
		acc := ix.matchInto(q.Kids[0], nil)
		for _, k := range q.Kids[1:] {
			other := ix.matchInto(k, nil)
			// In-place: the write head never outruns the read head.
			acc = intersectInto(acc, other, acc[:0])
		}
		out = append(out, acc...)
		return out
	case OpOr:
		var res []DocID
		for _, k := range q.Kids {
			res = ix.matchInto(k, res)
		}
		sortDocIDs(res)
		res = dedupeInto(res)
		out = append(out, res...)
		return out
	case OpAndNot:
		if len(q.Kids) == 0 {
			return out
		}
		acc := ix.matchInto(q.Kids[0], nil)
		for _, k := range q.Kids[1:] {
			other := ix.matchInto(k, nil)
			// In-place: survivors only move down.
			acc = differenceInto(acc, other, acc[:0])
		}
		out = append(out, acc...)
		return out
	default:
		return out
	}
}

// matchPhrase matches an ordered term sequence with slop tolerance: between
// consecutive terms it allows 1..1+Slop position steps.
func (ix *Index) matchPhrase(terms []uint64, slop int, out []DocID) []DocID {
	if len(terms) == 0 {
		return out
	}
	first := ix.post[terms[0]]
	if first == nil {
		return out
	}
	lists := make([]*postings, 0, len(terms))
	lists = append(lists, first)
	for _, h := range terms[1:] {
		p := ix.post[h]
		if p == nil {
			return out
		}
		lists = append(lists, p)
	}
	rows := make([]int, len(lists))
	for i := range first.ids {
		if phraseAt(lists, rows, i, slop) {
			out = append(out, first.ids[i])
		}
	}
	return out
}

// phraseAt reports whether the terms co-occur in order at posting row i of
// the first term, within slop tolerance. Other terms' rows are found by
// binary search on the same document; rows is caller scratch (one slot per
// term) so the per-candidate check allocates nothing.
func phraseAt(lists []*postings, rows []int, row int, slop int) bool {
	doc := lists[0].ids[row]
	rows[0] = row
	for t := 1; t < len(lists); t++ {
		p := lists[t]
		lo, hi := 0, len(p.ids)
		for lo < hi {
			m := lo + (hi-lo)/2
			if p.ids[m] < doc {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo >= len(p.ids) || p.ids[lo] != doc {
			return false
		}
		rows[t] = lo
	}
	return spansMatch(lists, rows, slop)
}

// spansMatch checks the positional chain greedily from each start position
// of term 0: every later term must offer a position within 1..1+slop steps
// after the previous term's position. Position lists ascend, so the first
// slot in range is the earliest reachable one, and greedy choice is complete:
// any later slot only shrinks the window left for the remaining terms.
func spansMatch(lists []*postings, rows []int, slop int) bool {
	first := positionsOf(lists[0], rows[0])
	for _, start := range first {
		cur := start
		ok := true
		for t := 1; t < len(lists); t++ {
			next, found := nextInRange(positionsOf(lists[t], rows[t]), cur, slop)
			if !found {
				ok = false
				break
			}
			cur = next
		}
		if ok {
			return true
		}
	}
	return false
}

// positionsOf returns the position slice for one posting row.
func positionsOf(p *postings, row int) []uint32 {
	return p.pos[p.off[row]:p.off[row+1]]
}

// nextInRange returns the earliest slot strictly after cur and at most
// 1+slop steps beyond it.
func nextInRange(slots []uint32, cur uint32, slop int) (uint32, bool) {
	lo, hi := 0, len(slots)
	for lo < hi {
		m := lo + (hi-lo)/2
		if slots[m] <= cur {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < len(slots) && slots[lo] <= cur+1+uint32(slop) {
		return slots[lo], true
	}
	return 0, false
}

// Score appends BM25-scored hits for q, descending by score (ties by DocID),
// truncated to topK when topK > 0. It reuses index-owned scratch; a warmed
// call allocates only when out must grow.
func (ix *Index) Score(q Query, topK int, out []Scored) []Scored {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()
	ix.scratchS = ix.scratchS[:0]
	ix.scoreInto(q, ix.docs, &ix.scratchS)
	s := ix.scratchS
	sortScored(s)
	if topK > 0 && len(s) > topK {
		s = s[:topK]
	}
	out = append(out, s...)
	ix.scratchS = ix.scratchS[:0]
	return out
}

// scoreInto accumulates q's BM25 over its matching documents.
func (ix *Index) scoreInto(q Query, docs map[DocID]*docMeta, acc *[]Scored) {
	switch q.Op {
	case OpTerm:
		ix.scoreTerm(q.Term, q.boostOf(), docs, acc)
	case OpPhrase:
		ix.scorePhrase(q.Terms, q.Slop, q.boostOf(), docs, acc)
	case OpAnd, OpOr:
		for _, k := range q.Kids {
			ix.scoreInto(k, docs, acc)
		}
		mergeScores(acc)
	case OpAndNot:
		if len(q.Kids) == 0 {
			return
		}
		var pos []Scored
		ix.scoreInto(q.Kids[0], docs, &pos)
		var banned []DocID
		for _, k := range q.Kids[1:] {
			banned = ix.matchInto(k, banned)
		}
		sortDocIDs(banned)
		banned = dedupeInto(banned)
		sortScoredByDoc(pos)
		i, j := 0, 0
		for i < len(pos) && j < len(banned) {
			switch {
			case pos[i].Doc < banned[j]:
				*acc = append(*acc, pos[i])
				i++
			case pos[i].Doc > banned[j]:
				j++
			default:
				i++
				j++
			}
		}
		*acc = append(*acc, pos[i:]...)
	case OpAll:
		for id := range docs {
			*acc = append(*acc, Scored{Doc: id, Score: q.boostOf()})
		}
	}
}

// scoreTerm adds one term's BM25 contribution over its posting list.
func (ix *Index) scoreTerm(term uint64, boost float64, docs map[DocID]*docMeta, acc *[]Scored) {
	p := ix.post[term]
	if p == nil || ix.nDocs == 0 {
		return
	}
	idf := idf(ix.nDocs, len(p.ids))
	avg := float64(ix.tokens) / float64(ix.nDocs)
	for i, id := range p.ids {
		dl := float64(docs[id].length)
		f := float64(p.freq[i])
		den := f + bm25K1*(1-bm25B+bm25B*dl/avg)
		*acc = append(*acc, Scored{Doc: id, Score: boost * idf * f * (bm25K1 + 1) / den})
	}
}

// scorePhrase scores phrase occurrences like a term whose frequency is the
// occurrence count.
func (ix *Index) scorePhrase(terms []uint64, slop int, boost float64, docs map[DocID]*docMeta, acc *[]Scored) {
	if len(terms) == 0 || ix.nDocs == 0 {
		return
	}
	matched := ix.matchPhrase(terms, slop, nil)
	if len(matched) == 0 {
		return
	}
	idf := idf(ix.nDocs, len(matched))
	avg := float64(ix.tokens) / float64(ix.nDocs)
	for _, id := range matched {
		f := float64(countPhrase(terms, slop, id, ix.post))
		dl := float64(docs[id].length)
		den := f + bm25K1*(1-bm25B+bm25B*dl/avg)
		*acc = append(*acc, Scored{Doc: id, Score: boost * idf * f * (bm25K1 + 1) / den})
	}
}

// countPhrase counts ordered occurrences of terms in doc.
func countPhrase(terms []uint64, slop int, doc DocID, post map[uint64]*postings) int {
	lists := make([]*postings, 0, len(terms))
	rows := make([]int, 0, len(terms))
	for _, h := range terms {
		p := post[h]
		if p == nil {
			return 0
		}
		lo, hi := 0, len(p.ids)
		for lo < hi {
			m := lo + (hi-lo)/2
			if p.ids[m] < doc {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo >= len(p.ids) || p.ids[lo] != doc {
			return 0
		}
		lists = append(lists, p)
		rows = append(rows, lo)
	}
	n := 0
	prev := lists[0].pos[lists[0].off[rows[0]]:lists[0].off[rows[0]+1]]
	for _, start := range prev {
		cur := start
		ok := true
		for t := 1; t < len(lists); t++ {
			slots := lists[t].pos[lists[t].off[rows[t]]:lists[t].off[rows[t]+1]]
			found := false
			for _, s := range slots {
				if s > cur && s <= cur+1+uint32(slop) {
					cur = s
					found = true
					break
				}
				if s > cur+1+uint32(slop) {
					break
				}
			}
			if !found {
				ok = false
				break
			}
		}
		if ok {
			n++
		}
	}
	return n
}

// idf is BM25's smoothed inverse document frequency.
func idf(nDocs, df int) float64 {
	return math.Log1p(float64(nDocs-df) + 0.5/(float64(df)+0.5))
}

// mergeScores collapses duplicate docs (from OR/AND children) by summation,
// keeping the slice sorted for the caller. Sum-then-sort keeps multi-term
// scoring additive, as BM25 requires.
func mergeScores(acc *[]Scored) {
	s := *acc
	if len(s) < 2 {
		return
	}
	sortScoredByDoc(s)
	w := 0
	for r := 1; r < len(s); r++ {
		if s[r].Doc == s[w].Doc {
			s[w].Score += s[r].Score
		} else {
			w++
			s[w] = s[r]
		}
	}
	*acc = s[:w+1]
}

// The sorts below use slices.SortFunc with package-level comparators: unlike
// sort.Slice, nothing boxes through any or reflect, so sorting allocates
// nothing and stays off the hot path's budget.
func sortScored(s []Scored) {
	slices.SortFunc(s, cmpScoredDesc)
}

func cmpScoredDesc(a, b Scored) int {
	if a.Score != b.Score {
		if a.Score > b.Score {
			return -1
		}
		return 1
	}
	return cmp.Compare(a.Doc, b.Doc)
}

func sortScoredByDoc(s []Scored) {
	slices.SortFunc(s, cmpScoredDoc)
}

func cmpScoredDoc(a, b Scored) int {
	return cmp.Compare(a.Doc, b.Doc)
}

func sortDocIDs(s []DocID) {
	slices.Sort(s)
}

func dedupeInto(s []DocID) []DocID {
	if len(s) < 2 {
		return s
	}
	w := 0
	for r := 1; r < len(s); r++ {
		if s[r] != s[w] {
			w++
			s[w] = s[r]
		}
	}
	return s[:w+1]
}

// intersectInto intersects two sorted lists into out.
func intersectInto(a, b []DocID, out []DocID) []DocID {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

// differenceInto keeps elements of sorted a absent from sorted b.
func differenceInto(a, b []DocID, out []DocID) []DocID {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			j++
		default:
			i++
			j++
		}
	}
	return append(out, a[i:]...)
}
