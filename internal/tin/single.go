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
type TextScratch struct {
	pairs []tokPos
	merge []uint32
	pool  []uint32
	spans []spanHit
	slots []uint32
}

// MatchSingle reports whether text matches q. q must be parsed (expansions
// resolved); expansion operators never appear because ParseTINQL desugars
// them, and a zero OpOr (empty expansion) matches nothing.
func MatchSingle(text string, q Query, scratch *TextScratch) bool {
	pairs := scratch.pairs[:0]
	scanString(text, func(h uint64, p uint32) {
		pairs = append(pairs, tokPos{hash: h, pos: p})
	})
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
	spans := scratch.spans[:0]
	spans = view.evalInto(q, spans)
	scratch.spans = spans[:0]
	return len(spans) > 0
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
		acc := v.evalInto(q.Kids[0], nil)
		if len(acc) == 0 {
			return out
		}
		for _, k := range q.Kids[1:] {
			other := v.evalInto(k, nil)
			if len(other) == 0 {
				return out
			}
			acc = append(acc, other...)
		}
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
		acc := v.evalInto(q.Kids[0], nil)
		if len(acc) == 0 {
			return out
		}
		for _, k := range q.Kids[1:] {
			if len(v.evalInto(k, nil)) > 0 {
				return out
			}
		}
		return append(out, acc...)
	case OpThen:
		if len(q.Kids) != 2 {
			return out
		}
		return pairSpans(v.evalInto(q.Kids[0], nil), v.evalInto(q.Kids[1], nil), q.Dist, out)
	case OpNear:
		if len(q.Kids) != 2 {
			return out
		}
		out = pairSpans(v.evalInto(q.Kids[0], nil), v.evalInto(q.Kids[1], nil), q.Dist, out)
		return pairSpans(v.evalInto(q.Kids[1], nil), v.evalInto(q.Kids[0], nil), q.Dist, out)
	case OpWithin:
		if len(q.Kids) != 1 {
			return out
		}
		for _, s := range v.evalInto(q.Kids[0], nil) {
			if int(s.end-s.start) <= q.Dist {
				out = append(out, s)
			}
		}
		return out
	case OpEncloses, OpEnclosedBy, OpOverlapping, OpBefore, OpAfter:
		if len(q.Kids) != 2 {
			return out
		}
		return relateSingle(q.Op, q.Neg, v.evalInto(q.Kids[0], nil), v.evalInto(q.Kids[1], nil), out)
	case OpFilter:
		if len(q.Kids) != 1 {
			return out
		}
		window := filterWindowLen(q.Filter, v.length)
		for _, s := range v.evalInto(q.Kids[0], nil) {
			if s.start >= window[0] && s.end <= window[1] {
				out = append(out, s)
			}
		}
		return out
	case OpAtLeast:
		matched := 0
		var acc []spanHit
		for _, k := range q.Kids {
			kid := v.evalInto(k, nil)
			if len(kid) > 0 {
				matched++
				acc = append(acc, kid...)
			}
		}
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
	lists := make([][]uint32, len(ph))
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
