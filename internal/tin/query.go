package tin

import (
	"cmp"
	"math"
	"slices"
)

// Query evaluation over span sets. Every TINQL expression denotes a set of
// spans (position ranges); boolean and span-relation operators combine them,
// and Match projects the final set to documents while Score weights it with
// BM25. Queries are plain values over term hashes — ParseTINQL compiles
// surface syntax down to this AST, and the SQL ==> operator will lower to it
// too.
//
// Layout note for distribution and persistence: evaluation only reads sorted
// posting lists, the term dictionary, and document lengths — all plain
// slices and maps with deterministic order — so an index shard encodes,
// ships, and merges without pointer fixups (a later slice adds Encode/Merge).

// Op tags a Query node.
type Op uint8

const (
	// OpTerm matches documents containing Term; each occurrence is a span.
	OpTerm Op = iota
	// OpPhrase matches the ordered Phrase slots; see PhrasePos. Slop allows
	// up to Slop extra words between consecutive slots.
	OpPhrase
	// OpAnd keeps documents matching every kid; spans union per document.
	OpAnd
	// OpOr keeps documents matching any kid; spans concatenate.
	OpOr
	// OpAndNot keeps documents matching Kids[0] and no later kid.
	OpAndNot
	// OpAll matches every document with its whole extent as one span (TIN's
	// standalone `*`).
	OpAll
	// OpThen is ordered proximity: pairs of Kids[0]/Kids[1] spans with
	// 0..Dist extra words between, emitted as covering spans.
	OpThen
	// OpNear is OpThen in either order.
	OpNear
	// OpWithin keeps Kids[0] spans of width at most Dist words.
	OpWithin
	// OpEncloses keeps Kids[0] spans containing a Kids[1] span (or
	// containing none, when Neg holds).
	OpEncloses
	// OpEnclosedBy keeps Kids[0] spans inside a Kids[1] span (or inside
	// none, when Neg holds).
	OpEnclosedBy
	// OpOverlapping keeps Kids[0] spans sharing a position with a Kids[1]
	// span (or sharing none, when Neg holds).
	OpOverlapping
	// OpBefore keeps Kids[0] spans starting before a Kids[1] span's start.
	OpBefore
	// OpAfter keeps Kids[0] spans starting after a Kids[1] span's start.
	OpAfter
	// OpFilter keeps Kids[0] spans fully inside Filter's window.
	OpFilter
	// OpAtLeast keeps documents matching at least Threshold kids, with the
	// contributing spans concatenated.
	OpAtLeast
)

// PhrasePos is one phrase slot: one of Alts must occur there, or (when Any)
// any single word. An empty Alts without Any never matches.
type PhrasePos struct {
	Alts []uint64
	Any  bool
}

// FilterKind tags a positional window; positions are 0-based.
type FilterKind uint8

const (
	// FilterFirstWords keeps spans in the first N tokens.
	FilterFirstWords FilterKind = iota
	// FilterFirstPct keeps spans in the first N percent of the document.
	FilterFirstPct
	// FilterLastWords keeps spans in the last N tokens.
	FilterLastWords
	// FilterLastPct keeps spans in the last N percent of the document.
	FilterLastPct
	// FilterMiddlePct keeps spans in the middle N percent of the document.
	FilterMiddlePct
	// FilterWords keeps spans in the inclusive range [Lo, Hi].
	FilterWords
)

// FilterSpec is one IN-filter window.
type FilterSpec struct {
	Kind FilterKind
	N    int
	Lo   int
	Hi   int
}

// Query is one evaluator node. Only the fields its Op reads are meaningful.
type Query struct {
	Op     Op
	Term   uint64
	Phrase []PhrasePos
	Slop   int
	Kids   []Query
	Boost  float32
	// Dist bounds OpThen/OpNear gaps and OpWithin widths, in words.
	Dist int
	// Neg selects the NOT form of the span relations.
	Neg bool
	// Filter windows OpFilter.
	Filter FilterSpec
	// Threshold counts OpAtLeast kids (percentages resolved at parse).
	Threshold int
	// Expanded reports dictionary expansion shaped the query: a
	// wildcard, fuzzy, MATCHES, or range operator generated alternatives
	// from the parsed index's vocabulary, so the same pattern may lower
	// differently on another index. Segmented top-K serves only
	// unexpanded queries from one parse.
	Expanded bool
}

// boostOf normalizes the unset/zero boost to 1.
func (q Query) boostOf() float64 {
	if q.Boost == 0 {
		return 1
	}
	return float64(q.Boost)
}

// spanHit is one [start, end) span in doc; end is exclusive so a single
// occurrence at p is {p, p+1} and a whole document of L words is {0, L}.
type spanHit struct {
	doc        DocID
	start, end uint32
}

// Match appends every document matching q, ascending, and returns out.
// Term, match-all, and boolean queries run on document fast paths; phrases,
// proximity, relations, filters, and thresholds evaluate span sets and
// project. Per-level combination allocates bounded by the query shape, never
// by the data size.
func (ix *Index) Match(q Query, out []DocID) []DocID {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()
	switch q.Op {
	case OpAll:
		base := len(out)
		for id := range ix.docs {
			out = append(out, id)
		}
		sortDocIDs(out[base:])
		return out
	case OpTerm:
		if p := ix.post[q.Term]; p != nil {
			if p.sealed == nil {
				out = append(out, p.ids...)
			} else {
				out = p.sealed.sealedAppendIDs(out)
			}
		}
		return out
	case OpAnd:
		if len(q.Kids) == 0 {
			return out
		}
		if allTerms(q.Kids) {
			// The keep set stages straight into caller-owned out:
			// no intermediate array to copy and drop.
			return ix.matchTermsInto(q.Kids, out)
		}
		acc := ix.matchInto(q.Kids[0], nil)
		for _, k := range q.Kids[1:] {
			other := ix.matchInto(k, nil)
			// In-place: the write head never outruns the read head.
			acc = intersectInto(acc, other, acc[:0])
		}
		return append(out, acc...)
	case OpOr:
		var res []DocID
		for _, k := range q.Kids {
			res = ix.matchInto(k, res)
		}
		sortDocIDs(res)
		return append(out, dedupeInto(res)...)
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
		return append(out, acc...)
	default:
		if q.Op == OpPhrase {
			// Phrases match at document level without span
			// materialization: same verdicts, O(width) staging.
			return ix.matchPhraseDocs(q.Phrase, q.Slop, out)
		}
		spans := ix.evalInto(q, nil)
		return append(out, projectDocs(spans)...)
	}
}

// gallopRatioGate selects probing over decoding: a list this many times
// longer than the current driver is probed per driver element (block index
// plus cached binary search, or plain binary search when open) instead of
// decoded and merged. A probe costs ~100ns; a merge step ~3.5ns, so the
// gate pays off whenever it fires — and equal-size conjunctions never
// reach it, keeping the streaming merge.
const gallopRatioGate = 64

// allTerms reports whether every kid is a plain term.
func allTerms(kids []Query) bool {
	for _, k := range kids {
		if k.Op != OpTerm {
			return false
		}
	}
	return true
}

// matchTermsInto intersects all-term kids layout-aware. The shortest list
// drives: siblings near its size decode and merge (streaming, as before),
// while hugely longer siblings are probed per driver element, so a rare
// term never pays the common term's full decode. The driver appends into
// out and filters in place; termLists stages the resolved postings in
// index-owned scratch.
func (ix *Index) matchTermsInto(kids []Query, out []DocID) []DocID {
	if len(kids) == 0 {
		return out
	}
	lists := ix.termLists[:0]
	for _, k := range kids {
		p := ix.post[k.Term]
		if p == nil {
			ix.termLists = lists
			return out
		}
		lists = append(lists, p)
	}
	ix.termLists = lists
	short := 0
	for i := range lists {
		if lists[i].docCount() < lists[short].docCount() {
			short = i
		}
	}
	lists[0], lists[short] = lists[short], lists[0]
	base := len(out)
	out = ix.appendList(out, lists[0])
	driver := out[base:]
	for _, l := range lists[1:] {
		if len(driver) == 0 {
			break
		}
		if l.docCount() >= gallopRatioGate*len(driver) {
			w := 0
			for _, d := range driver {
				if ix.listContains(l, d) {
					driver[w] = d
					w++
				}
			}
			driver = driver[:w]
			continue
		}
		// Sibling decodes ride index-owned scratch, warm across calls:
		// a rare term must never pay the common term's allocation.
		other := ix.appendList(ix.matchOther[:0], l)
		driver = intersectScalar(driver, other, driver[:0])
		ix.matchOther = other
	}
	return out[:base+len(driver)]
}

// appendList appends one posting list's ids in either layout.
func (ix *Index) appendList(out []DocID, p *postings) []DocID {
	if p.sealed == nil {
		return append(out, p.ids...)
	}
	return p.sealed.sealedAppendIDs(out)
}

// listContains probes one document's membership in either layout: binary
// search over the open array, block index plus cached binary search sealed.
func (ix *Index) listContains(p *postings, doc DocID) bool {
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
		return lo < len(p.ids) && p.ids[lo] == doc
	}
	_, ok := ix.sealedFindRow(p.sealed, doc)
	return ok
}

// matchInto is the document fast path shared by the boolean combinators.
func (ix *Index) matchInto(q Query, out []DocID) []DocID {
	switch q.Op {
	case OpTerm:
		// Terms stay on the document fast path even under booleans: span
		// expansion would decode positions no boolean needs.
		if p := ix.post[q.Term]; p != nil {
			out = ix.appendList(out, p)
		}
		return out
	case OpAnd:
		if len(q.Kids) == 0 {
			return out
		}
		if allTerms(q.Kids) {
			// The keep set stages straight into caller-owned out:
			// no intermediate array to copy and drop.
			return ix.matchTermsInto(q.Kids, out)
		}
		acc := ix.matchInto(q.Kids[0], nil)
		for _, k := range q.Kids[1:] {
			other := ix.matchInto(k, nil)
			acc = intersectInto(acc, other, acc[:0])
		}
		return append(out, acc...)
	case OpOr:
		var res []DocID
		for _, k := range q.Kids {
			res = ix.matchInto(k, res)
		}
		sortDocIDs(res)
		return append(out, dedupeInto(res)...)
	case OpAndNot:
		if len(q.Kids) == 0 {
			return out
		}
		acc := ix.matchInto(q.Kids[0], nil)
		for _, k := range q.Kids[1:] {
			other := ix.matchInto(k, nil)
			acc = differenceInto(acc, other, acc[:0])
		}
		return append(out, acc...)
	default:
		if q.Op == OpPhrase {
			// Phrases match at document level without span
			// materialization: same verdicts, O(width) staging.
			return ix.matchPhraseDocs(q.Phrase, q.Slop, out)
		}
		spans := ix.evalInto(q, nil)
		return append(out, projectDocs(spans)...)
	}
}

// projectDocs extracts ascending distinct documents from (doc, start)
// ordered spans.
func projectDocs(spans []spanHit) []DocID {
	var out []DocID
	for _, s := range spans {
		if n := len(out); n == 0 || out[n-1] != s.doc {
			out = append(out, s.doc)
		}
	}
	return out
}

// evalInto appends q's span hits sorted by (doc, start). Intermediate levels
// allocate bounded by the query shape; leaves append into out directly.
func (ix *Index) evalInto(q Query, out []spanHit) []spanHit {
	switch q.Op {
	case OpTerm:
		if p := ix.post[q.Term]; p != nil {
			if p.sealed == nil {
				for i, id := range p.ids {
					for _, pos := range positionsOf(p, i) {
						out = append(out, spanHit{doc: id, start: pos, end: pos + 1})
					}
				}
			} else {
				out = ix.sealedTermSpans(p.sealed, out, ix.decPos)
			}
		}
		return out
	case OpAll:
		for id, meta := range ix.docs {
			out = append(out, spanHit{doc: id, start: 0, end: meta.length})
		}
		sortSpanHits(out)
		return out
	case OpPhrase:
		return ix.phraseSpans(q.Phrase, q.Slop, out)
	case OpAnd:
		if len(q.Kids) == 0 {
			return out
		}
		acc := ix.evalInto(q.Kids[0], nil)
		for _, k := range q.Kids[1:] {
			other := ix.evalInto(k, nil)
			acc = joinDocs(acc, other)
		}
		return append(out, acc...)
	case OpOr:
		var res []spanHit
		for _, k := range q.Kids {
			res = ix.evalInto(k, res)
		}
		sortSpanHits(res)
		return append(out, res...)
	case OpAndNot:
		if len(q.Kids) == 0 {
			return out
		}
		acc := ix.evalInto(q.Kids[0], nil)
		for _, k := range q.Kids[1:] {
			other := ix.evalInto(k, nil)
			acc = subtractDocs(acc, projectDocSet(other))
		}
		return append(out, acc...)
	case OpThen:
		if len(q.Kids) != 2 {
			return out
		}
		return ix.proximitySpans(q.Kids[0], q.Kids[1], q.Dist, out)
	case OpNear:
		if len(q.Kids) != 2 {
			return out
		}
		out = ix.proximitySpans(q.Kids[0], q.Kids[1], q.Dist, out)
		out = ix.proximitySpans(q.Kids[1], q.Kids[0], q.Dist, out)
		sortSpanHits(out)
		return out
	case OpWithin:
		if len(q.Kids) != 1 {
			return out
		}
		for _, s := range ix.evalInto(q.Kids[0], nil) {
			if int(s.end-s.start) <= q.Dist {
				out = append(out, s)
			}
		}
		return out
	case OpEncloses, OpEnclosedBy, OpOverlapping, OpBefore, OpAfter:
		if len(q.Kids) != 2 {
			return out
		}
		return relateSpans(q.Op, q.Neg, ix.evalInto(q.Kids[0], nil), ix.evalInto(q.Kids[1], nil), out)
	case OpFilter:
		if len(q.Kids) != 1 {
			return out
		}
		for _, s := range ix.evalInto(q.Kids[0], nil) {
			if window, ok := ix.filterWindow(q.Filter, s.doc); ok && s.start >= window[0] && s.end <= window[1] {
				out = append(out, s)
			}
		}
		return out
	case OpAtLeast:
		return ix.atLeastSpans(q, out)
	default:
		return out
	}
}

// joinDocs keeps spans of a whose document also occurs in b, concatenating
// both sides' spans per document (the union a boolean AND denotes over span
// sets). Inputs must be (doc, start) ordered; output is too.
func joinDocs(a, b []spanHit) []spanHit {
	// Fresh output: interleaving both sides' spans per document pushes the
	// write head past the read head, so aliasing a clobbers unread spans.
	out := make([]spanHit, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].doc < b[j].doc:
			i = skipDoc(a, i)
		case a[i].doc > b[j].doc:
			j = skipDoc(b, j)
		default:
			d := a[i].doc
			for i < len(a) && a[i].doc == d {
				out = append(out, a[i])
				i++
			}
			for j < len(b) && b[j].doc == d {
				out = append(out, b[j])
				j++
			}
		}
	}
	// Restore start order inside documents concatenated from both sides.
	tidyDocSpans(out)
	return out
}

// skipDoc advances past one document's run.
func skipDoc(s []spanHit, i int) int {
	d := s[i].doc
	for i < len(s) && s[i].doc == d {
		i++
	}
	return i
}

// tidyDocSpans sorts each document's run by start (runs arrive ordered from
// each side but interleaved across sides).
func tidyDocSpans(s []spanHit) {
	start := 0
	for start < len(s) {
		end := skipDoc(s, start)
		slices.SortFunc(s[start:end], cmpSpanStart)
		start = end
	}
}

func cmpSpanStart(a, b spanHit) int {
	if a.start != b.start {
		return cmp.Compare(a.start, b.start)
	}
	return cmp.Compare(a.end, b.end)
}

func sortSpanHits(s []spanHit) {
	slices.SortFunc(s, func(a, b spanHit) int {
		if a.doc != b.doc {
			return cmp.Compare(a.doc, b.doc)
		}
		return cmpSpanStart(a, b)
	})
}

// subtractDocs drops whole documents listed in banned (ascending).
func subtractDocs(spans []spanHit, banned []DocID) []spanHit {
	out := spans[:0]
	j := 0
	for _, s := range spans {
		for j < len(banned) && banned[j] < s.doc {
			j++
		}
		if j < len(banned) && banned[j] == s.doc {
			continue
		}
		out = append(out, s)
	}
	return out
}

// projectDocSet extracts ascending distinct documents.
func projectDocSet(spans []spanHit) []DocID {
	var out []DocID
	for _, s := range spans {
		if n := len(out); n == 0 || out[n-1] != s.doc {
			out = append(out, s.doc)
		}
	}
	return out
}

// pairSpans pairs a-spans with the earliest b-span starting at most dist
// words after the a-span ends (gap = b.start - a.end, so adjacency is gap
// 0), emitting covering spans. Both lists address one document and ascend by
// start. Each a-span searches from the list head (binary search): ends need
// not ascend with starts, so a shared cursor would miss pairs.
func pairSpans(aSpans, bSpans []spanHit, dist int, out []spanHit) []spanHit {
	for _, a := range aSpans {
		lo, hi := 0, len(bSpans)
		for lo < hi {
			m := lo + (hi-lo)/2
			if int64(bSpans[m].start) < int64(a.end) {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo < len(bSpans) && int64(bSpans[lo].start)-int64(a.end) <= int64(dist) {
			out = append(out, spanHit{doc: a.doc, start: a.start, end: bSpans[lo].end})
		}
	}
	return out
}

// proximitySpans pairs per-document span groups with pairSpans.
func (ix *Index) proximitySpans(aq, bq Query, dist int, out []spanHit) []spanHit {
	a := ix.evalInto(aq, nil)
	b := ix.evalInto(bq, nil)
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].doc < b[j].doc:
			i = skipDoc(a, i)
		case a[i].doc > b[j].doc:
			j = skipDoc(b, j)
		default:
			d := a[i].doc
			ai := i
			for ai < len(a) && a[ai].doc == d {
				ai++
			}
			jj := j
			for j < len(b) && b[j].doc == d {
				j++
			}
			out = pairSpans(a[i:ai], b[jj:j], dist, out)
			i = ai
		}
	}
	return out
}

// relateSpans filters left spans by their relation to right spans of the
// same document. BEFORE/AFTER compare starts existentially: an a-span is
// kept when some b-span starts later/earlier.
func relateSpans(op Op, neg bool, left, right []spanHit, out []spanHit) []spanHit {
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		switch {
		case left[i].doc < right[j].doc:
			if neg {
				out = append(out, left[i])
			}
			i++
		case left[i].doc > right[j].doc:
			j = skipDoc(right, j)
		default:
			d := left[i].doc
			jj := j
			for j < len(right) && right[j].doc == d {
				j++
			}
			b := right[jj:j]
			for i < len(left) && left[i].doc == d {
				if relateOne(op, left[i], b) != neg {
					out = append(out, left[i])
				}
				i++
			}
		}
	}
	if neg {
		for ; i < len(left); i++ {
			out = append(out, left[i])
		}
	}
	return out
}

// relateOne tests one left span against its document's right spans.
func relateOne(op Op, a spanHit, b []spanHit) bool {
	switch op {
	case OpEncloses:
		for _, s := range b {
			if s.start >= a.start && s.end <= a.end {
				return true
			}
		}
		return false
	case OpEnclosedBy:
		for _, s := range b {
			if a.start >= s.start && a.end <= s.end {
				return true
			}
		}
		return false
	case OpOverlapping:
		for _, s := range b {
			if a.start < s.end && s.start < a.end {
				return true
			}
		}
		return false
	case OpBefore:
		for _, s := range b {
			if a.start < s.start {
				return true
			}
		}
		return false
	case OpAfter:
		for _, s := range b {
			if a.start > s.start {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// filterWindow resolves a FilterSpec to the inclusive-exclusive [lo, hi)
// token window for doc. ok is false for an unknown document.
func (ix *Index) filterWindow(f FilterSpec, doc DocID) (window [2]uint32, ok bool) {
	meta, ok := ix.docs[doc]
	if !ok {
		return window, false
	}
	return filterWindowLen(f, meta.length), true
}

// filterWindowLen resolves a FilterSpec against a known document length in
// words. It is the sharable core of filterWindow for cursors (like transient
// matching) that know lengths without the document table.
func filterWindowLen(f FilterSpec, length uint32) [2]uint32 {
	l := int(length)
	switch f.Kind {
	case FilterFirstWords:
		return [2]uint32{0, uint32(min(f.N, l))}
	case FilterFirstPct:
		return [2]uint32{0, uint32(pctOf(l, f.N))}
	case FilterLastWords:
		return [2]uint32{uint32(max(l-f.N, 0)), uint32(l)}
	case FilterLastPct:
		k := pctOf(l, f.N)
		return [2]uint32{uint32(max(l-k, 0)), uint32(l)}
	case FilterMiddlePct:
		k := pctOf(l, f.N)
		s := (l - k) / 2
		return [2]uint32{uint32(s), uint32(s + k)}
	case FilterWords:
		lo := min(max(f.Lo, 0), l)
		hi := min(max(f.Hi+1, 0), l)
		if hi < lo {
			hi = lo
		}
		return [2]uint32{uint32(lo), uint32(hi)}
	default:
		return [2]uint32{}
	}
}

// pctOf rounds a percentage of l up: a 25% window of a 3-word document still
// covers its first word.
func pctOf(l, pct int) int {
	if pct <= 0 || l <= 0 {
		return 0
	}
	return (l*pct + 99) / 100
}

// atLeastDocs returns ascending documents occurring in at least threshold
// of the per-kid ascending doc lists. Counting sorted runs replaces a hash
// map: one slice allocation, no buckets, no pointer chasing.
func atLeastDocs(lists [][]DocID, threshold int) []DocID {
	var all []DocID
	for _, l := range lists {
		all = append(all, l...)
	}
	sortDocIDs(all)
	// Compact qualifying run heads in place; the write head trails the read
	// head, so this is safe.
	w, i := 0, 0
	for i < len(all) {
		j := i + 1
		for j < len(all) && all[j] == all[i] {
			j++
		}
		if j-i >= threshold {
			all[w] = all[i]
			w++
		}
		i = j
	}
	return all[:w]
}

// atLeastSpans keeps documents matching at least Threshold kids,
// concatenating the contributing spans.
func (ix *Index) atLeastSpans(q Query, out []spanHit) []spanHit {
	kids := make([][]spanHit, 0, len(q.Kids))
	lists := make([][]DocID, 0, len(q.Kids))
	for _, k := range q.Kids {
		spans := ix.evalInto(k, nil)
		kids = append(kids, spans)
		lists = append(lists, projectDocSet(spans))
	}
	qual := atLeastDocs(lists, q.Threshold)
	for _, spans := range kids {
		i, j := 0, 0
		for i < len(spans) && j < len(qual) {
			switch {
			case spans[i].doc < qual[j]:
				i = skipDoc(spans, i)
			case spans[i].doc > qual[j]:
				j++
			default:
				out = append(out, spans[i])
				i++
			}
		}
	}
	sortSpanHits(out)
	return out
}

// phraseSpans matches an ordered slot chain with slop tolerance. Any slots
// advance exactly one word and must land inside the document; after them a
// constrained slot may sit 1..1+slop steps beyond, so explicit `_` gaps and
// slop compose instead of conflicting.
func (ix *Index) phraseSpans(ph []PhrasePos, slop int, out []spanHit) []spanHit {
	if len(ph) == 0 {
		return out
	}
	anchor := -1
	for i, slot := range ph {
		if !slot.Any && len(slot.Alts) > 0 {
			anchor = i
			break
		}
	}
	if anchor == -1 {
		// All-Any: every window of len(ph) words in every document.
		for id, meta := range ix.docs {
			for s := uint32(0); s+uint32(len(ph)) <= meta.length && meta.length > 0; s++ {
				out = append(out, spanHit{doc: id, start: s, end: s + uint32(len(ph))})
			}
		}
		sortSpanHits(out)
		return out
	}
	// Candidate documents hold the anchor slot: union its alternatives.
	altKids := make([]Query, 0, len(ph[anchor].Alts))
	for _, h := range ph[anchor].Alts {
		altKids = append(altKids, Query{Op: OpTerm, Term: h})
	}
	cands := ix.matchInto(Query{Op: OpOr, Kids: altKids}, nil)
	slots := make([]uint32, len(ph))
	// posStage carries every slot's positions for one candidate document:
	// tails append sequentially and stay valid through the chain check,
	// then reset for the next document. One buffer per phrase query.
	var posStage []uint32
	for _, doc := range cands {
		meta, ok := ix.docs[doc]
		if !ok {
			continue
		}
		lists := make([][]uint32, len(ph))
		stage := posStage[:0]
		complete := true
		for t, slot := range ph {
			if slot.Any {
				continue
			}
			var tail []uint32
			tail, stage = ix.slotPositions(slot.Alts, doc, stage)
			lists[t] = tail
			if len(lists[t]) == 0 {
				complete = false
				break
			}
		}
		posStage = stage
		if !complete {
			continue
		}
		for _, a := range lists[anchor] {
			if s0, s1, ok := chainFrom(ph, lists, meta.length, anchor, a, slop, slots); ok {
				out = append(out, spanHit{doc: doc, start: s0, end: s1 + 1})
			}
		}
	}
	return out
}

// slotPositions returns the ascending union of alts' positions in doc, or
// nil when none occur there, plus the extended stage. Open single-term
// lookups alias the posting list (no copy, as before); every other shape
// decodes into the caller's stage tail, which stays valid until the caller
// resets the stage.
func (ix *Index) slotPositions(alts []uint64, doc DocID, stage []uint32) ([]uint32, []uint32) {
	if len(alts) == 1 {
		if p := ix.post[alts[0]]; p != nil {
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
					return positionsOf(p, lo), stage
				}
				return nil, stage
			}
			if tail, stage2, ok := ix.sealedRowPositions(p.sealed, doc, stage); ok {
				return tail, stage2
			}
		}
		return nil, stage
	}
	base := len(stage)
	for _, h := range alts {
		p := ix.post[h]
		if p == nil {
			continue
		}
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
				stage = append(stage, positionsOf(p, lo)...)
			}
			continue
		}
		if _, stage2, ok := ix.sealedRowPositions(p.sealed, doc, stage); ok {
			stage = stage2
		}
	}
	tail := stage[base:]
	slices.Sort(tail)
	return tail, stage
}

// matchPhraseDocs appends the documents matching a phrase query without
// materializing per-occurrence span hits. Candidates come from the anchor
// slot's term intersection (the same anchor phraseSpans uses); each
// candidate proves out through one chainFrom walk that exits on the first
// chained anchor position, so staging stays O(phrase width) per document
// instead of O(hits). The verdict set is identical to
// evalInto+projectDocs: same anchor, same slot positions (slotPositions),
// same chain check over the same document lengths, same ascending order.
// Match-only: scoring and span relations keep the span machinery, which
// counts occurrences. All staging lanes are index-owned and written back
// for the next call; the keep set itself rides caller-owned out.
func (ix *Index) matchPhraseDocs(ph []PhrasePos, slop int, out []DocID) []DocID {
	if len(ph) == 0 {
		return out
	}
	anchor := -1
	for i, slot := range ph {
		if !slot.Any && len(slot.Alts) > 0 {
			anchor = i
			break
		}
	}
	base := len(out)
	if anchor == -1 {
		// All-Any: every document with room for the window, ascending.
		for id, meta := range ix.docs {
			if uint32(len(ph)) <= meta.length {
				out = append(out, id)
			}
		}
		sortDocIDs(out[base:])
		return out
	}
	// The hot exact shape — slop-0, two slots, one alternative each —
	// skips the union/sort/dedupe/chain lanes for the pair merge.
	if slop == 0 && len(ph) == 2 && !ph[0].Any && !ph[1].Any &&
		len(ph[0].Alts) == 1 && len(ph[1].Alts) == 1 {
		return ix.matchExactPairDocs(ph[0].Alts[0], ph[1].Alts[0], out)
	}
	// Anchor alternatives union (a document holds the slot when it
	// holds any alternative): the same union matchInto serves the span
	// path, staged into the candidate lane.
	cands := extendN(ix.matchCands[:0], 0)
	for _, h := range ph[anchor].Alts {
		if p := ix.post[h]; p != nil {
			cands = ix.appendList(cands, p)
		}
	}
	sortDocIDs(cands)
	cands = dedupeInto(cands)
	lists := extendN(ix.matchSlotLists[:0], len(ph))
	slots := extendN(ix.matchChainSlots[:0], len(ph))
	stage := extendN(ix.matchPosStage[:0], 0)
	for _, doc := range cands {
		meta, ok := ix.docs[doc]
		if !ok {
			continue
		}
		stage = stage[:0]
		complete := true
		for t, slot := range ph {
			if slot.Any {
				lists[t] = nil
				continue
			}
			var tail []uint32
			tail, stage = ix.slotPositions(slot.Alts, doc, stage)
			lists[t] = tail
			if len(lists[t]) == 0 {
				complete = false
				break
			}
		}
		if !complete {
			continue
		}
		for _, a := range lists[anchor] {
			if _, _, ok := chainFrom(ph, lists, meta.length, anchor, a, slop, slots); ok {
				out = append(out, doc)
				break
			}
		}
	}
	ix.matchCands = cands[:0]
	ix.matchSlotLists = lists
	ix.matchChainSlots = slots
	ix.matchPosStage = stage
	return out
}

// chainFrom verifies the slot chain around one anchor occurrence. Forward
// slots take the earliest reachable position and backward slots the latest
// reachable one; both greeds are complete because position lists ascend, so
// the extremal reachable choice always leaves maximal room for the rest.
// slots is caller scratch with one entry per slot.
func chainFrom(ph []PhrasePos, lists [][]uint32, docLen uint32, anchor int, anchorPos uint32, slop int, slots []uint32) (uint32, uint32, bool) {
	slots[anchor] = anchorPos
	cur := anchorPos
	for t := anchor + 1; t < len(ph); t++ {
		if ph[t].Any {
			cur++
			if cur >= docLen {
				return 0, 0, false
			}
			slots[t] = cur
			continue
		}
		next, ok := earliestAfter(lists[t], cur, slop)
		if !ok {
			return 0, 0, false
		}
		cur = next
		slots[t] = cur
	}
	cur = anchorPos
	for t := anchor - 1; t >= 0; t-- {
		if ph[t].Any {
			if cur == 0 {
				return 0, 0, false
			}
			cur--
			slots[t] = cur
			continue
		}
		prev, ok := latestBefore(lists[t], cur, slop)
		if !ok {
			return 0, 0, false
		}
		cur = prev
		slots[t] = cur
	}
	return slots[0], slots[len(ph)-1], true
}

// matchExactPairDocs appends the documents where term hb occurs exactly one
// position after ha: the slop-0, two-slot, single-alternative phrase, which
// is the hot exact-phrase shape (a quoted two-word query). It skips the
// generic lane's candidate union, sort, dedupe, per-slot union sorts, and
// chain generality: a doc-level merge when both sides are open, otherwise a
// walk of the smaller side in ascending order probing the other, then one
// two-pointer adjacency walk per shared document. Verdicts equal
// matchPhraseDocs on this shape: same postings, same slot positions
// (positionsOf / sealedRowPositions), same ascending order, one hit per
// document on the first adjacency. Open/open stages nothing and allocates
// nothing; sealed sides decode into the match position stage. The keep set
// rides caller-owned out, as in the generic lane.
func (ix *Index) matchExactPairDocs(ha, hb uint64, out []DocID) []DocID {
	pa := ix.post[ha]
	pb := ix.post[hb]
	if pa == nil || pb == nil {
		return out
	}
	if pa == pb {
		return ix.matchSelfPairDocs(pa, out)
	}
	if pa.sealed == nil && pb.sealed == nil {
		i, j := 0, 0
		ai, bj := pa.ids, pb.ids
		for i < len(ai) && j < len(bj) {
			a, b := ai[i], bj[j]
			if a != b {
				if a < b {
					i++
				} else {
					j++
				}
				continue
			}
			if _, ok := ix.docs[a]; ok && adjacentPair(positionsOf(pa, i), positionsOf(pb, j)) {
				out = append(out, a)
			}
			i++
			j++
		}
		return out
	}
	drive, probe := pa, pb
	driveSealed, probeSealed := pa.sealed != nil, pb.sealed != nil
	if pairDocCount(pb) < pairDocCount(pa) {
		drive, probe = pb, pa
		driveSealed, probeSealed = probeSealed, driveSealed
	}
	stage := extendN(ix.matchPosStage[:0], 0)
	if !driveSealed {
		for i, doc := range drive.ids {
			var other []uint32
			if probeSealed {
				var st2 []uint32
				var ok bool
				other, st2, ok = ix.sealedRowPositions(probe.sealed, doc, stage)
				if !ok {
					stage = st2
					continue
				}
				stage = st2
			} else {
				lo, hi := 0, len(probe.ids)
				for lo < hi {
					m := lo + (hi-lo)/2
					if probe.ids[m] < doc {
						lo = m + 1
					} else {
						hi = m
					}
				}
				if lo >= len(probe.ids) || probe.ids[lo] != doc {
					continue
				}
				other = positionsOf(probe, lo)
			}
			// Order follows the query, not the walk: when the rarer side
			// won the drive, its positions sit in other.
			mine := positionsOf(drive, i)
			if _, ok := ix.docs[doc]; ok {
				if isHa := drive == pa; (isHa && adjacentPair(mine, other)) ||
					(!isHa && adjacentPair(other, mine)) {
					out = append(out, doc)
				}
			}
		}
		ix.matchPosStage = stage
		return out
	}
	// Drive block rows by index, refetching the block's ids per document:
	// a sealedBlock result aliases the shared 4-entry decoded-block cache
	// and must never be held across the sealedRowAt/sealedRowPositions
	// calls below, which evict it. The refetch is a tag hit.
	for b := range drive.sealed.blk {
		rows := drive.sealed.blk[b].rows
		for k := uint32(0); k < rows; k++ {
			ids, _, _ := ix.sealedBlock(drive.sealed, b)
			doc := ids[k]
			base := len(stage)
			stage = ix.sealedRowAt(drive.sealed, b, int(k), stage)
			mine := stage[base:]
			var other []uint32
			if probeSealed {
				var st2 []uint32
				var ok bool
				other, st2, ok = ix.sealedRowPositions(probe.sealed, doc, stage)
				if !ok {
					stage = st2
					continue
				}
				stage = st2
			} else {
				lo, hi := 0, len(probe.ids)
				for lo < hi {
					m := lo + (hi-lo)/2
					if probe.ids[m] < doc {
						lo = m + 1
					} else {
						hi = m
					}
				}
				if lo >= len(probe.ids) || probe.ids[lo] != doc {
					continue
				}
				other = positionsOf(probe, lo)
			}
			if _, ok := ix.docs[doc]; ok {
				if isHa := drive == pa; (isHa && adjacentPair(mine, other)) ||
					(!isHa && adjacentPair(other, mine)) {
					out = append(out, doc)
				}
			}
		}
	}
	ix.matchPosStage = stage
	return out
}

// pairDocCount estimates a postings list's document count for drive-side
// choice: open length or sealed row total.
func pairDocCount(p *postings) int {
	if p.sealed == nil {
		return len(p.ids)
	}
	return int(p.sealed.n)
}

// matchSelfPairDocs appends the documents where one term occurs at two
// adjacent positions: the "x x" phrase, where both slots share a postings
// list. One ordered walk, one two-pointer self-adjacency check per row.
func (ix *Index) matchSelfPairDocs(p *postings, out []DocID) []DocID {
	if p.sealed == nil {
		for i, doc := range p.ids {
			pos := positionsOf(p, i)
			if _, ok := ix.docs[doc]; ok && adjacentPair(pos, pos) {
				out = append(out, doc)
			}
		}
		return out
	}
	stage := extendN(ix.matchPosStage[:0], 0)
	// Same no-hold rule as the pair drive above: refetch per document.
	for b := range p.sealed.blk {
		rows := p.sealed.blk[b].rows
		for k := uint32(0); k < rows; k++ {
			ids, _, _ := ix.sealedBlock(p.sealed, b)
			doc := ids[k]
			base := len(stage)
			stage = ix.sealedRowAt(p.sealed, b, int(k), stage)
			if _, ok := ix.docs[doc]; ok && adjacentPair(stage[base:], stage[base:]) {
				out = append(out, doc)
			}
		}
	}
	ix.matchPosStage = stage
	return out
}

// adjacentPair reports whether b holds a+1 for some a in a: a single
// two-pointer walk over ascending position lists, exiting on the first
// adjacency. The MaxUint32 guard keeps the +1 exact; no document reaches
// a 4-billion-word length, so it never fires.
func adjacentPair(a, b []uint32) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == ^uint32(0) {
			i++
			continue
		}
		want := a[i] + 1
		if bj := b[j]; bj < want {
			j++
		} else if bj > want {
			i++
		} else {
			return true
		}
	}
	return false
}

// earliestAfter returns the first slot position in (cur, cur+1+slop].
func earliestAfter(slots []uint32, cur uint32, slop int) (uint32, bool) {
	lo, hi := 0, len(slots)
	for lo < hi {
		m := lo + (hi-lo)/2
		if slots[m] <= cur {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < len(slots) && int64(slots[lo])-int64(cur) <= int64(slop)+1 {
		return slots[lo], true
	}
	return 0, false
}

// latestBefore returns the last slot position in [cur-1-slop, cur-1].
func latestBefore(slots []uint32, cur uint32, slop int) (uint32, bool) {
	if cur == 0 {
		return 0, false
	}
	lo, hi := 0, len(slots)
	for lo < hi {
		m := lo + (hi-lo)/2
		if slots[m] < cur {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == 0 {
		return 0, false
	}
	if s := slots[lo-1]; int64(cur)-int64(s) <= int64(slop)+1 {
		return s, true
	}
	return 0, false
}

// positionsOf returns the position slice for one posting row.
func positionsOf(p *postings, row int) []uint32 {
	return p.pos[p.off[row]:p.off[row+1]]
}

// Score appends BM25-scored hits for q, descending by score (ties by DocID),
// truncated to topK when topK > 0. It reuses index-owned scratch; a warmed
// call allocates only when out must grow.
func (ix *Index) Score(q Query, topK int, out []Scored) []Scored {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()
	if topK > 0 {
		switch q.Op {
		case OpTerm:
			if s, ok := ix.scoreSingleTopK(q.Term, q.boostOf(), topK, out); ok {
				return s
			}
		case OpAnd:
			if s, ok := ix.scoreAndTopK(q, topK, out); ok {
				return s
			}
		}
	}
	ix.scratchS = ix.scratchS[:0]
	ix.scoreInto(q, &ix.scratchS)
	s := ix.scratchS
	s = topKScored(s, topK)
	out = append(out, s...)
	ix.scratchS = ix.scratchS[:0]
	return out
}

// scoreInto accumulates q's BM25 over its matching documents. Terms and
// phrases score from posting frequencies; proximity, relations, filters, and
// thresholds score their resulting span counts as occurrence frequencies;
// booleans sum their children. Every level scales by its boost.
func (ix *Index) scoreInto(q Query, acc *[]Scored) {
	switch q.Op {
	case OpTerm:
		ix.scoreTerm(q.Term, q.boostOf(), acc)
	case OpPhrase:
		ix.scorePhrase(q.Phrase, q.Slop, q.boostOf(), acc)
	case OpAnd:
		if len(q.Kids) == 0 {
			return
		}
		for _, k := range q.Kids {
			ix.scoreInto(k, acc)
		}
		mergeScores(acc)
		// Summation ranges over the union; AND ranks only the intersection.
		var keep []DocID
		if allTerms(q.Kids) {
			keep = ix.matchTermsInto(q.Kids, ix.matchKeep[:0])
		} else {
			keep = ix.matchInto(q.Kids[0], nil)
			for _, k := range q.Kids[1:] {
				other := ix.matchInto(k, nil)
				keep = intersectInto(keep, other, keep[:0])
			}
		}
		filterScored(acc, keep)
		ix.matchKeep = keep[:0]
		scaleScores(acc, q.boostOf())
	case OpOr:
		for _, k := range q.Kids {
			ix.scoreInto(k, acc)
		}
		mergeScores(acc)
		scaleScores(acc, q.boostOf())
	case OpAndNot:
		if len(q.Kids) == 0 {
			return
		}
		var pos []Scored
		ix.scoreInto(q.Kids[0], &pos)
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
		scaleScores(acc, q.boostOf())
	case OpAll:
		for id := range ix.docs {
			*acc = append(*acc, Scored{Doc: id, Score: q.boostOf()})
		}
	case OpAtLeast:
		for _, k := range q.Kids {
			ix.scoreInto(k, acc)
		}
		// Keep only documents matching enough kids, then merge.
		lists := make([][]DocID, 0, len(q.Kids))
		for _, k := range q.Kids {
			lists = append(lists, ix.matchInto(k, nil))
		}
		keep := atLeastDocs(lists, q.Threshold)
		sortScoredByDoc(*acc)
		w, j := 0, 0
		for _, s := range *acc {
			for j < len(keep) && keep[j] < s.Doc {
				j++
			}
			if j < len(keep) && keep[j] == s.Doc {
				(*acc)[w] = s
				w++
			}
		}
		*acc = (*acc)[:w]
		mergeScores(acc)
		scaleScores(acc, q.boostOf())
	default:
		ix.scoreSpans(ix.evalInto(q, nil), q.boostOf(), acc)
	}
}

// filterScored keeps accumulated hits whose document is in keep. acc must
// be document-ordered (as mergeScores leaves it) and keep ascending.
func filterScored(acc *[]Scored, keep []DocID) {
	s := *acc
	w, j := 0, 0
	for _, h := range s {
		for j < len(keep) && keep[j] < h.Doc {
			j++
		}
		if j < len(keep) && keep[j] == h.Doc {
			s[w] = h
			w++
		}
	}
	*acc = s[:w]
}

// scaleScores multiplies accumulated hits by a group boost.
func scaleScores(acc *[]Scored, boost float64) {
	if boost == 1 {
		return
	}
	for i := range *acc {
		(*acc)[i].Score *= boost
	}
}

// scoreTerm adds one term's BM25 contribution over its posting list. It
// gathers frequencies and lengths into reused scratch, then runs the (wide
// or scalar) kernel over the flat arrays.
func (ix *Index) scoreTerm(term uint64, boost float64, acc *[]Scored) {
	p := ix.post[term]
	if p == nil || ix.nDocs == 0 {
		return
	}
	idf := idf(ix.nDocs, p.docCount())
	avg := float64(ix.tokens) / float64(ix.nDocs)
	tf, dl, sc := ix.scoreTF[:0], ix.scoreDL[:0], ix.scoreOut[:0]
	var ids []DocID
	if p.sealed == nil {
		ids = p.ids
		for i, id := range p.ids {
			tf = append(tf, float64(p.off[i+1]-p.off[i]))
			dl = append(dl, float64(ix.docLength(id)))
		}
	} else {
		ids, tf, dl = ix.sealedScoreGather(p.sealed, tf, dl)
	}
	sc = bm25Scores(idf, avg, boost, tf, dl, sc)
	for i, id := range ids {
		*acc = append(*acc, Scored{Doc: id, Score: sc[i]})
	}
	ix.scoreTF, ix.scoreDL, ix.scoreOut = tf[:0], dl[:0], sc[:0]
}

// scorePhrase scores phrase occurrences like a term whose frequency is the
// occurrence count.
func (ix *Index) scorePhrase(ph []PhrasePos, slop int, boost float64, acc *[]Scored) {
	if len(ph) == 0 || ix.nDocs == 0 {
		return
	}
	spans := ix.phraseSpans(ph, slop, nil)
	ix.scoreSpans(spans, boost, acc)
}

// scoreSpans scores span hits with BM25 over occurrence counts: tf is the
// document's span count, df the number of spanned documents.
func (ix *Index) scoreSpans(spans []spanHit, boost float64, acc *[]Scored) {
	if len(spans) == 0 || ix.nDocs == 0 {
		return
	}
	matched := projectDocSet(spans)
	idf := idf(ix.nDocs, len(matched))
	avg := float64(ix.tokens) / float64(ix.nDocs)
	tf, dl, sc := ix.scoreTF[:0], ix.scoreDL[:0], ix.scoreOut[:0]
	i := 0
	for i < len(spans) {
		d := spans[i].doc
		j := i
		for j < len(spans) && spans[j].doc == d {
			j++
		}
		tf = append(tf, float64(j-i))
		dl = append(dl, float64(ix.docLength(d)))
		i = j
	}
	sc = bm25Scores(idf, avg, boost, tf, dl, sc)
	for k, d := range matched {
		*acc = append(*acc, Scored{Doc: d, Score: sc[k]})
	}
	ix.scoreTF, ix.scoreDL, ix.scoreOut = tf[:0], dl[:0], sc[:0]
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

// topKScored returns s's best topK hits in rank order: quickselect
// partition by the same total order sortScored uses, then sort the prefix.
// Bit-identical to sorting all and truncating (the comparator breaks every
// tie by DocID, so the top-K set and its order are unique), at O(n)
// average instead of O(n log n). topK <= 0 keeps every hit, sorted.
func topKScored(s []Scored, topK int) []Scored {
	if topK <= 0 || topK >= len(s) {
		sortScored(s)
		return s
	}
	quickselectScored(s, topK)
	prefix := s[:topK]
	sortScored(prefix)
	return prefix
}

// quickselectScored rearranges s so its first k elements are its k best
// (unordered). Deterministic middle pivot, three-way partition, in place.
// The equal band matters: under this comparator equal entries are
// identical (Doc, Score) pairs, hence interchangeable, and a zero-boost
// query scores everything equal — two-way schemes degrade to quadratic
// there while this one returns in one pass.
func quickselectScored(s []Scored, k int) {
	lo, hi := 0, len(s)
	for hi-lo > 1 {
		p := s[lo+(hi-lo)/2]
		lt, i, gt := lo, lo, hi-1
		for i <= gt {
			switch c := cmpScoredDesc(s[i], p); {
			case c < 0:
				s[lt], s[i] = s[i], s[lt]
				lt++
				i++
			case c > 0:
				s[i], s[gt] = s[gt], s[i]
				gt--
			default:
				i++
			}
		}
		// [lo,lt) outranks everything else in range; [lt,gt] ties the
		// pivot (identical pairs); (gt,hi) ranks below.
		switch {
		case k <= lt-lo:
			hi = lt
		case k <= gt-lo+1:
			return
		default:
			k -= gt - lo + 1
			lo = gt + 1
		}
	}
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
