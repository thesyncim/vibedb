package tin

import (
	"slices"
	"unicode/utf8"
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
	// comb stages one combinator's output (Then/Near/relations). Kid
	// spans live in arena tails and out may alias those tails, so a
	// combinator must not write results directly into out while it is
	// still reading its inputs: the writes would clobber not-yet-read
	// spans. Staging into comb (disjoint from the arena) and then
	// appending to out is correct under any aliasing — append/copy is
	// memmove — and stays zero-alloc: comb capacity converges on the
	// largest combinator result. Each combinator fully consumes its
	// staging before returning, so nested combinators safely reuse it.
	comb []spanHit
}

// needsPositions reports whether q can observe token positions or order.
// Term/boolean structure only tests membership per term, so unsorted
// pairs answer it with linear scans; phrases, proximity, relations, and
// positional filters read positions and force the sorted path. Unknown
// operators conservatively need positions.
func needsPositions(q Query) bool {
	switch q.Op {
	case OpPhrase, OpThen, OpNear, OpWithin,
		OpEncloses, OpEnclosedBy, OpOverlapping, OpBefore, OpAfter,
		OpFilter:
		return true
	case OpTerm, OpAnd, OpOr, OpAndNot, OpAll, OpAtLeast:
		for _, k := range q.Kids {
			if needsPositions(k) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// matchUnsorted answers boolean structure over unsorted pairs: no term
// observes positions, so membership is a linear scan with early exit and
// the combinators fold booleans instead of span lists. Emptiness matches
// evalInto exactly (an AtLeast with nothing matched is empty even when
// its threshold is zero).
func matchUnsorted(pairs []tokPos, q Query) bool {
	switch q.Op {
	case OpTerm:
		for _, tp := range pairs {
			if tp.hash == q.Term {
				return true
			}
		}
		return false
	case OpAll:
		return true
	case OpAnd:
		if len(q.Kids) == 0 {
			return false
		}
		for _, k := range q.Kids {
			if !matchUnsorted(pairs, k) {
				return false
			}
		}
		return true
	case OpOr:
		for _, k := range q.Kids {
			if matchUnsorted(pairs, k) {
				return true
			}
		}
		return false
	case OpAndNot:
		if len(q.Kids) == 0 || !matchUnsorted(pairs, q.Kids[0]) {
			return false
		}
		for _, k := range q.Kids[1:] {
			if matchUnsorted(pairs, k) {
				return false
			}
		}
		return true
	case OpAtLeast:
		matched := 0
		for _, k := range q.Kids {
			if matchUnsorted(pairs, k) {
				matched++
			}
		}
		return matched > 0 && matched >= q.Threshold
	default:
		return false
	}
}

// MatchSingle reports whether text matches q. q must be parsed (expansions
// resolved); expansion operators never appear because ParseTINQL desugars
// them, and a zero OpOr (empty expansion) matches nothing.
// stopCap bounds the stop set: queries with more distinct terms fall back
// to the full scan, which stays exact. Sixteen covers every realistic
// recheck while keeping the tracking on the stack, so the hot path grows
// no struct and allocates nothing.
const stopCap = 16

// decideUnsorted evaluates boolean structure over the distinct terms
// found so far, without assuming the scan finished: done reports the
// verdict is final (a later token cannot change it), verdict its value.
// At end of text the verdict equals matchUnsorted exactly — duplicates
// never affect membership, and every arm mirrors evalInto's emptiness,
// including the zero-match AtLeast edge.
func decideUnsorted(q Query, found []tokPos, eof bool) (done, verdict bool) {
	has := func(term uint64) bool {
		for _, tp := range found {
			if tp.hash == term {
				return true
			}
		}
		return false
	}
	switch q.Op {
	case OpTerm:
		if has(q.Term) {
			return true, true
		}
		if eof {
			return true, false
		}
		return false, false
	case OpAll:
		return true, true
	case OpAnd:
		if len(q.Kids) == 0 {
			return true, false
		}
		allTrue := true
		for i := range q.Kids {
			d, v := decideUnsorted(q.Kids[i], found, eof)
			if d && !v {
				return true, false
			}
			if !d || !v {
				allTrue = false
			}
		}
		if allTrue {
			return true, true
		}
		if eof {
			return true, false
		}
		return false, false
	case OpOr:
		if len(q.Kids) == 0 {
			return true, false
		}
		allFalse := true
		for i := range q.Kids {
			d, v := decideUnsorted(q.Kids[i], found, eof)
			if d && v {
				return true, true
			}
			if !d || v {
				allFalse = false
			}
		}
		if allFalse || eof {
			return true, false
		}
		return false, false
	case OpAndNot:
		if len(q.Kids) == 0 {
			return true, false
		}
		d0, v0 := decideUnsorted(q.Kids[0], found, eof)
		if d0 && !v0 {
			return true, false
		}
		allBannedFalse := true
		for _, k := range q.Kids[1:] {
			d, v := decideUnsorted(k, found, eof)
			if d && v {
				return true, false
			}
			if !d || v {
				allBannedFalse = false
			}
		}
		if d0 && v0 && allBannedFalse {
			return true, true
		}
		if eof {
			return true, false
		}
		return false, false
	case OpAtLeast:
		matched := 0
		for i := range q.Kids {
			if d, v := decideUnsorted(q.Kids[i], found, eof); d && v {
				matched++
			}
		}
		if matched > 0 && matched >= q.Threshold {
			return true, true
		}
		if eof {
			return true, false
		}
		return false, false
	default:
		return false, false
	}
}

// collectTerms gathers the query's distinct term hashes for the stop
// set; structural operators contribute none. It reports false when the
// set would overflow the caller's bounded buffer, in which case the
// caller scans fully.
func collectTerms(q *Query, out []uint64) ([]uint64, bool) {
	if q.Op == OpTerm {
		for _, h := range out {
			if h == q.Term {
				return out, true
			}
		}
		if len(out) == cap(out) {
			return nil, false
		}
		return append(out, q.Term), true
	}
	for i := range q.Kids {
		var ok bool
		if out, ok = collectTerms(&q.Kids[i], out); !ok {
			return nil, false
		}
	}
	return out, true
}

// scanPairsUntil tokenizes like scanPairs but returns once the distinct
// terms found decide the verdict. Each outer iteration completes at most
// one token (a separator or rune flushes at most once; word runs never
// flush), so one top-of-loop hook observes every completion: membership
// probe unrolled for the dominant single-term shape, dedupe, and the
// decide walk on growth only — no calls on the hot path, which is what
// keeps full scans near the untracked cost. Flush stays lean and
// inlineable. Overshoot is bounded by one trailing word; its pairs are
// ordinary scratch. Everything is values over caller-stack buffers, so
// the scan allocates nothing by construction.
func scanPairsUntil(text string, out []tokPos, q Query, needed []uint64, found []tokPos) ([]tokPos, []tokPos, bool, bool) {
	s := pairScanner{out: out, h: fnvOffset}
	i, n, base := 0, len(text), len(out)
	for i < n {
		if len(s.out) > base {
			base = len(s.out)
			h := s.out[base-1].hash
			member := len(needed) == 1 && needed[0] == h
			if !member {
				for _, t := range needed {
					if t == h {
						member = true
						break
					}
				}
			}
			if member {
				dup := false
				for _, tp := range found {
					if tp.hash == h {
						dup = true
						break
					}
				}
				if !dup {
					found = append(found, tokPos{hash: h})
					if d, v := decideUnsorted(q, found, false); d {
						return s.out, found, true, v
					}
				}
			}
		}
		c := text[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(text[i:])
			s.feed(r, size)
			i += size
			continue
		}
		if b := scanTab[c]; b == 0 {
			s.flush()
			i++
			continue
		}
		if !s.inWord {
			s.h = fnvOffset
			s.inWord = true
		}
		h := s.h
		for i < n {
			c := text[i]
			if c >= utf8.RuneSelf {
				break
			}
			b := scanTab[c]
			if b == 0 {
				break
			}
			h = mix(h, b)
			i++
		}
		s.h = h
	}
	s.flush()
	if len(s.out) > base {
		h := s.out[len(s.out)-1].hash
		member := len(needed) == 1 && needed[0] == h
		if !member {
			for _, t := range needed {
				if t == h {
					member = true
					break
				}
			}
		}
		if member {
			dup := false
			for _, tp := range found {
				if tp.hash == h {
					dup = true
					break
				}
			}
			if !dup {
				found = append(found, tokPos{hash: h})
				if d, v := decideUnsorted(q, found, false); d {
					return s.out, found, true, v
				}
			}
		}
	}
	return s.out, found, false, false
}

// scanPairsUntilBytes is scanPairsUntil over a caller-owned buffer: the
// same fused machine and early-exit hooks, DecodeRune past multibyte
// sequences, byte indexing elsewhere. Agreement with the string lane is
// proved by TestScanPairsUntilAgrees; the two must stay structurally
// identical for the same reason the score mirrors must.
func scanPairsUntilBytes(buf []byte, out []tokPos, q Query, needed []uint64, found []tokPos) ([]tokPos, []tokPos, bool, bool) {
	s := pairScanner{out: out, h: fnvOffset}
	i, n, base := 0, len(buf), len(out)
	for i < n {
		if len(s.out) > base {
			base = len(s.out)
			h := s.out[base-1].hash
			member := len(needed) == 1 && needed[0] == h
			if !member {
				for _, t := range needed {
					if t == h {
						member = true
						break
					}
				}
			}
			if member {
				dup := false
				for _, tp := range found {
					if tp.hash == h {
						dup = true
						break
					}
				}
				if !dup {
					found = append(found, tokPos{hash: h})
					if d, v := decideUnsorted(q, found, false); d {
						return s.out, found, true, v
					}
				}
			}
		}
		c := buf[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRune(buf[i:])
			s.feed(r, size)
			i += size
			continue
		}
		if b := scanTab[c]; b == 0 {
			s.flush()
			i++
			continue
		}
		if !s.inWord {
			s.h = fnvOffset
			s.inWord = true
		}
		h := s.h
		for i < n {
			c := buf[i]
			if c >= utf8.RuneSelf {
				break
			}
			b := scanTab[c]
			if b == 0 {
				break
			}
			h = mix(h, b)
			i++
		}
		s.h = h
	}
	s.flush()
	if len(s.out) > base {
		h := s.out[len(s.out)-1].hash
		member := len(needed) == 1 && needed[0] == h
		if !member {
			for _, t := range needed {
				if t == h {
					member = true
					break
				}
			}
		}
		if member {
			dup := false
			for _, tp := range found {
				if tp.hash == h {
					dup = true
					break
				}
			}
			if !dup {
				found = append(found, tokPos{hash: h})
				if d, v := decideUnsorted(q, found, false); d {
					return s.out, found, true, v
				}
			}
		}
	}
	return s.out, found, false, false
}

// matchStopScan answers term-boolean structure while scanning, stopping
// once the distinct terms found decide the verdict: recheck candidates
// are index-positive, so their terms are present and the exit fires on
// nearly every row. Stale or over-accepted rows fall through to the full
// scan, which matchUnsorted answers exactly. Tracking rides bounded
// caller-stack buffers (found never outgrows needed, both capped), so
// oversized stop sets and degenerate term-free structure fall back to
// the exact full scan.
func matchStopScan(text string, q Query, scratch *TextScratch) bool {
	var neededBuf [stopCap]uint64
	needed, ok := collectTerms(&q, neededBuf[:0])
	if !ok || len(needed) == 0 {
		pairs := scanPairs(text, scratch.pairs[:0])
		scratch.pairs = pairs
		return matchUnsorted(pairs, q)
	}
	var foundBuf [stopCap]tokPos
	out, found, stopped, verdict := scanPairsUntil(text, scratch.pairs[:0], q, needed, foundBuf[:0])
	scratch.pairs = out
	if stopped {
		return verdict
	}
	return matchUnsorted(found, q)
}

// matchStopScanBytes is matchStopScan over a caller-owned buffer.
func matchStopScanBytes(buf []byte, q Query, scratch *TextScratch) bool {
	var neededBuf [stopCap]uint64
	needed, ok := collectTerms(&q, neededBuf[:0])
	if !ok || len(needed) == 0 {
		pairs := scanPairsBytes(buf, scratch.pairs[:0])
		scratch.pairs = pairs
		return matchUnsorted(pairs, q)
	}
	var foundBuf [stopCap]tokPos
	out, found, stopped, verdict := scanPairsUntilBytes(buf, scratch.pairs[:0], q, needed, foundBuf[:0])
	scratch.pairs = out
	if stopped {
		return verdict
	}
	return matchUnsorted(found, q)
}

// MatchSingleBytes is MatchSingle over a caller-owned buffer: same lanes,
// same verdicts, no string conversion. The query executor holds row text
// as arena-backed views (byteview), so this is the lane it drives;
// MatchSingle stays for string owners.
func MatchSingleBytes(buf []byte, q Query, scratch *TextScratch) bool {
	if !needsPositions(q) {
		switch q.Op {
		case OpTerm, OpOr, OpAtLeast, OpAll:
			if q.Op == OpAll {
				scratch.pairs = scratch.pairs[:0]
				return true
			}
			return matchStopScanBytes(buf, q, scratch)
		}
		pairs := scanPairsBytes(buf, scratch.pairs[:0])
		scratch.pairs = pairs
		return matchUnsorted(pairs, q)
	}
	pairs := scanPairsBytes(buf, scratch.pairs[:0])
	sortTokPos(pairs)
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

func MatchSingle(text string, q Query, scratch *TextScratch) bool {
	// scanPairs, not scanString: the emit closure escapes to the heap on
	// every call, while the slice-threading scanner allocates only on
	// growth, so warm scratch makes this call free.
	if !needsPositions(q) {
		// Early exit pays exactly when the verdict can precede end of
		// text: single terms, alternatives, and thresholds decide on
		// presence, while conjunctions need the last term and banned
		// terms need the whole text — tracking would tax every token
		// for an exit that rarely fires, so those scan fully.
		switch q.Op {
		case OpTerm, OpOr, OpAtLeast, OpAll:
			if q.Op == OpAll {
				scratch.pairs = scratch.pairs[:0]
				return true
			}
			return matchStopScan(text, q, scratch)
		}
		pairs := scanPairs(text, scratch.pairs[:0])
		scratch.pairs = pairs
		return matchUnsorted(pairs, q)
	}
	pairs := scanPairs(text, scratch.pairs[:0])
	sortTokPos(pairs)
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
	if len(tmp) == 0 {
		return s.arena[start:start]
	}
	// Anchor only when tmp really is the current arena tail at start.
	// A capacity heuristic is not enough: inner evals may reallocate
	// the arena mid-call, so the tail slice captured above can have
	// zero capacity while the arena now reports spare room; the final
	// append then forks to a foreign array and anchoring would adopt
	// the stale region instead of tmp's contents. Pointer-compare and
	// copy a forked tail back, converging the arena on one array.
	if start < len(s.arena) && &tmp[0] == &s.arena[start] && len(tmp) <= cap(s.arena)-start {
		s.arena = s.arena[:start+len(tmp)]
		return s.arena[start:]
	}
	s.arena = append(s.arena[:start], tmp...)
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
		staged := pairSpans(v.evalTemp(q.Kids[0]), v.evalTemp(q.Kids[1]), q.Dist, v.scratch.comb[:0])
		v.scratch.comb = staged
		return append(out, staged...)
	case OpNear:
		if len(q.Kids) != 2 {
			return out
		}
		left, right := v.evalTemp(q.Kids[0]), v.evalTemp(q.Kids[1])
		staged := pairSpans(left, right, q.Dist, v.scratch.comb[:0])
		v.scratch.comb = staged
		out = append(out, staged...)
		staged = pairSpans(right, left, q.Dist, v.scratch.comb[:0])
		v.scratch.comb = staged
		return append(out, staged...)
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
		staged := relateSingle(q.Op, q.Neg, v.evalTemp(q.Kids[0]), v.evalTemp(q.Kids[1]), v.scratch.comb[:0])
		v.scratch.comb = staged
		return append(out, staged...)
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
