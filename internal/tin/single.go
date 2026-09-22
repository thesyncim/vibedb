package tin

import (
	"slices"
)

// Transient single-document matching. MatchSingle answers whether one text
// matches a query without an index: it tokenizes into caller scratch, sorts
// (hash, position) runs, and runs the same span algebra the index path uses
// (chainFrom, relateOne, pairSpans, filter windows, boolean combinators).
// The query engine uses it as the exact row recheck behind ==> candidates,
// and as the whole answer when matching unindexed text with an already
// parsed (index-expanded) query.
//
// The zero-alloc contract holds for warmed scratch: token pairs, run
// scratch, span scratch, and slot scratch are all reused across calls.

// tokPos is one token occurrence: its folded hash and 0-based position.
type tokPos struct {
	hash uint64
	pos  uint32
}

// TextScratch stages transient matching. Reuse one across rows; it retains
// no references between calls.
//
// The zero-alloc contract: on warm scratch (buffers sized to the documents
// being matched) MatchSingle allocates nothing for ANY operator. Token
// pairs thread by value (scanPairs), positions and phrase unions reuse
// merge/pool, final spans reuse spans, and every intermediate span list the
// boolean combinators build lives in the arena bump buffer, which one call
// resets and reuses throughout. Growth allocates only while sizing up.
type TextScratch struct {
	pairs []tokPos
	merge []uint32
	pool  []uint32
	spans []spanHit
	slots []uint32
	lists [][]uint32
	arena []spanHit
}

// MatchSingle reports whether text matches q. q must be parsed (expansions
// resolved); expansion operators never appear because ParseTINQL desugars
// them, and a zero OpOr (empty expansion) matches nothing.
func MatchSingle(text string, q Query, scratch *TextScratch) bool {
	// scanPairs, not scanString: the emit closure escapes to the heap on
	// every call, while the slice-threading scanner allocates only on
	// growth, so warm scratch makes this call free.
	pairs := scanPairs(text, scratch.pairs[:0])
	slices.SortFunc(pairs, func(a, b tokPos) int {
		if a.hash != b.hash {
			if a.hash < b.hash {
				return -1
			}
			return 1
		}
		if a.pos < b.pos {
			return -1
		}
		if a.pos > b.pos {
			return 1
		}
		return 0
	})
	scratch.pairs = pairs
	length := uint32(0)
	for _, tp := range pairs {
		if tp.pos+1 > length {
			length = tp.pos + 1
		}
	}
	view := docView{pairs: pairs, length: length, scratch: scratch}
	scratch.arena = scratch.arena[:0]
	spans := scratch.spans[:0]
	spans = view.evalInto(q, spans)
	scratch.spans = spans[:0]
	return len(spans) > 0
}

// evalTemp evaluates kid into a fresh tail of the scratch arena, so sibling
// combinators can hold several tails alive at once. Tails that outgrow the
// tip fork to a new array; evalTemp copies a forked tail back into the arena
// (reclaiming the dead inner region first) instead of adopting the fork, so
// the arena keeps one array whose capacity converges on the live set and
// warm callers never fork. A tail that fits only widens the anchor.
func (v docView) evalTemp(q Query) []spanHit {
	s := v.scratch
	start := len(s.arena)
	tmp := v.evalInto(q, s.arena[start:start])
	if len(tmp) > cap(s.arena)-start {
		s.arena = append(s.arena[:start], tmp...)
		return s.arena[start:]
	}
	s.arena = s.arena[:start+len(tmp)]
	return s.arena[start:]
}

// docView is one document's sorted token runs plus shared scratch.
type docView struct {
	pairs   []tokPos
	length  uint32
	scratch *TextScratch
}

// positions returns the ascending positions of hash, or nil. The slice
// aliases the view's runs.
func (v docView) positions(hash uint64) []uint32 {
	lo, hi := 0, len(v.pairs)
	for lo < hi {
		m := lo + (hi-lo)/2
		if v.pairs[m].hash < hash {
			lo = m + 1
		} else {
			hi = m
		}
	}
	start := lo
	for lo < len(v.pairs) && v.pairs[lo].hash == hash {
		lo++
	}
	if lo == start {
		return nil
	}
	out := v.scratch.merge[:0]
	for _, tp := range v.pairs[start:lo] {
		out = append(out, tp.pos)
	}
	v.scratch.merge = out
	return out
}

// union returns the ascending union of several hashes' positions, appended
// to the pool. Every slot gets its own tail, because per-slot lists must
// all stay valid while the chain verifies (a shared buffer would clobber).
func (v docView) union(alts []uint64) []uint32 {
	base := len(v.scratch.pool)
	out := v.scratch.pool
	for _, h := range alts {
		lo, hi := 0, len(v.pairs)
		for lo < hi {
			m := lo + (hi-lo)/2
			if v.pairs[m].hash < h {
				lo = m + 1
			} else {
				hi = m
			}
		}
		for lo < len(v.pairs) && v.pairs[lo].hash == h {
			out = append(out, v.pairs[lo].pos)
			lo++
		}
	}
	tail := out[base:]
	slices.Sort(tail)
	tail = dedupeU32(tail)
	v.scratch.pool = out[:base+len(tail)]
	return v.scratch.pool[base:]
}

func dedupeU32(s []uint32) []uint32 {
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

// evalInto appends the document's span hits for q (doc field unset; single
// document).
func (v docView) evalInto(q Query, out []spanHit) []spanHit {
	switch q.Op {
	case OpTerm:
		for _, pos := range v.positions(q.Term) {
			out = append(out, spanHit{start: pos, end: pos + 1})
		}
		return out
	case OpAll:
		return append(out, spanHit{start: 0, end: v.length})
	case OpPhrase:
		return v.phraseSpans(q.Phrase, q.Slop, out)
	case OpAnd:
		if len(q.Kids) == 0 {
			return out
		}
		acc := v.evalTemp(q.Kids[0])
		if len(acc) == 0 {
			return out
		}
		for _, k := range q.Kids[1:] {
			other := v.evalTemp(k)
			if len(other) == 0 {
				return out
			}
			acc = append(acc, other...)
		}
		// Re-anchor past the merge: whoever extends the live set owns the
		// anchor, so the arena converges instead of re-forking every call.
		v.scratch.arena = acc
		return append(out, acc...)
	case OpOr:
		for _, k := range q.Kids {
			out = v.evalInto(k, out)
		}
		return out
	case OpAndNot:
		if len(q.Kids) == 0 {
			return out
		}
		acc := v.evalTemp(q.Kids[0])
		if len(acc) == 0 {
			return out
		}
		for _, k := range q.Kids[1:] {
			if len(v.evalTemp(k)) > 0 {
				return out
			}
		}
		return append(out, acc...)
	case OpThen:
		if len(q.Kids) != 2 {
			return out
		}
		return pairSpans(v.evalTemp(q.Kids[0]), v.evalTemp(q.Kids[1]), q.Dist, out)
	case OpNear:
		if len(q.Kids) != 2 {
			return out
		}
		left, right := v.evalTemp(q.Kids[0]), v.evalTemp(q.Kids[1])
		out = pairSpans(left, right, q.Dist, out)
		return pairSpans(right, left, q.Dist, out)
	case OpWithin:
		if len(q.Kids) != 1 {
			return out
		}
		for _, s := range v.evalTemp(q.Kids[0]) {
			if int(s.end-s.start) <= q.Dist {
				out = append(out, s)
			}
		}
		return out
	case OpEncloses, OpEnclosedBy, OpOverlapping, OpBefore, OpAfter:
		if len(q.Kids) != 2 {
			return out
		}
		return relateSingle(q.Op, q.Neg, v.evalTemp(q.Kids[0]), v.evalTemp(q.Kids[1]), out)
	case OpFilter:
		if len(q.Kids) != 1 {
			return out
		}
		window := filterWindowLen(q.Filter, v.length)
		for _, s := range v.evalTemp(q.Kids[0]) {
			if s.start >= window[0] && s.end <= window[1] {
				out = append(out, s)
			}
		}
		return out
	case OpAtLeast:
		matched := 0
		acc := v.scratch.arena[len(v.scratch.arena):len(v.scratch.arena)]
		for _, k := range q.Kids {
			kid := v.evalTemp(k)
			if len(kid) > 0 {
				matched++
				acc = append(acc, kid...)
			}
		}
		v.scratch.arena = acc
		if matched >= q.Threshold {
			return append(out, acc...)
		}
		return out
	default:
		return out
	}
}

// phraseSpans matches the slot chain for the one document.
func (v docView) phraseSpans(ph []PhrasePos, slop int, out []spanHit) []spanHit {
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
		for s := uint32(0); s+uint32(len(ph)) <= v.length && v.length > 0; s++ {
			out = append(out, spanHit{start: s, end: s + uint32(len(ph))})
		}
		return out
	}
	v.scratch.pool = v.scratch.pool[:0]
	lists := v.scratch.lists
	if cap(lists) < len(ph) {
		lists = make([][]uint32, len(ph))
	} else {
		lists = lists[:len(ph)]
	}
	v.scratch.lists = lists
	for t, slot := range ph {
		if slot.Any {
			continue
		}
		lists[t] = v.union(slot.Alts)
		if len(lists[t]) == 0 {
			return out
		}
	}
	slots := v.scratch.slots
	if cap(slots) < len(ph) {
		slots = make([]uint32, len(ph))
	} else {
		slots = slots[:len(ph)]
	}
	v.scratch.slots = slots
	for _, a := range lists[anchor] {
		if s0, s1, ok := chainFrom(ph, lists, v.length, anchor, a, slop, slots); ok {
			out = append(out, spanHit{start: s0, end: s1 + 1})
		}
	}
	return out
}

// relateSingle filters left spans by a span relation within one document.
func relateSingle(op Op, neg bool, left, right []spanHit, out []spanHit) []spanHit {
	for _, a := range left {
		if relateOne(op, a, right) != neg {
			out = append(out, a)
		}
	}
	return out
}
