package query

import (
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/store"
)

// Postings-accelerated candidate selection.
//
// candidateRows (execute.go) is the seam selectRows consults to decide which
// rows the compiled WHERE predicate is tested against. Returning nil means
// "every row" — the honest full columnar scan. This file fills the seam: when
// the Segment opted into the inverted posting layer (Segment.Postings) and the
// predicate has a leaf the postings can answer, it returns a narrowed candidate
// slice instead, and selectRows verifies each candidate with the same per-row
// eval. The accepted-rows contract is therefore unchanged; the postings only
// prune candidates the compiled predicate still confirms, so an accelerated Run
// and a full-scan Run return byte-identical results.
//
// A leaf is POSTABLE when it maps onto an execution primitive of the posting
// layer over a single top-level field:
//
//   - EXISTS(path)        -> Segment.AppendWhereExists
//   - path @> scalarJSON  -> Segment.AppendWhereContainsIndex (scalar needle)
//   - path = scalarLit    -> Segment.AppendWhereContainsIndex (equality)
//
// Equality rides the containment primitive because a scalar value contains an
// equal scalar, so the containment posting is a superset of rows whose value
// at path equals v (it also admits arrays that hold v and, by hash coarseness,
// collisions) — every one of which the per-row evalCmp re-checks and keeps only
// on an exact, in-kind, exact-decimal match. IS NULL, inequalities other than
// =, containment against a structured needle (the buckets index scalars only),
// negation, and any nested or pointer path are NOT postable and fall through to
// the full scan. The posting primitives are themselves exact — equal to their
// own full scan — so a postable leaf's candidate set is a sound superset
// (in fact the exact set) of the rows it accepts, the property the re-check and
// the set combinators below rely on.
//
// A predicate combines its postable leaves structurally, always yielding a
// superset of its accepted rows so the re-check stays exact:
//
//   - AND intersects the candidate sets of its postable conjuncts; an unpostable
//     conjunct contributes "every row" and simply does not narrow, so one
//     postable conjunct is enough to prune.
//   - OR unions its disjuncts, but only when every disjunct is postable — an
//     unpostable disjunct could accept any row, so the union would have to admit
//     every row and the whole disjunction falls back to the full scan.
//   - NOT is never postable (the complement of a selective set is not selective).
//
// The set merges are linear O(a+b) passes over the ascending, deduplicated
// ordinal slices the posting primitives return. Every leaf and merge writes
// into a distinct reusable Workspace buffer, so the whole tree can be warmed
// to zero allocations without aliasing an input with its output.

// postKind classifies a leaf predicate's posting probe, or postNone when the
// leaf cannot be answered from the postings.
type postKind uint8

const (
	postNone postKind = iota
	postExists
	postContains
	postEq
	// postIn is a membership: the leaf's needles are each probed through the
	// containment primitive and unioned. The probe carries only the path;
	// the needle set lives on the predicate.
	postIn
)

// A postProbe is a leaf predicate's compiled posting probe: which primitive
// answers it, the top-level field it reads, and the containment needle for the
// containment and equality forms. It is set during predicate compilation and is
// the zero value (postNone) for every unpostable leaf.
type postProbe struct {
	kind   postKind
	path   string
	needle vibejson.Index
}

// candidates returns a superset of the rows this predicate accepts, and ok
// reporting whether that superset is a real bound (a narrowed candidate set) as
// opposed to "every row". A nil slice with ok true means a postable predicate
// that matches no row — an empty candidate set, distinct from the unbounded
// ok-false case. The caller (candidateRows) normalizes the nil-but-ok slice so
// selectRows never mistakes an empty candidate set for a full scan.
func (p *compiledPredicate) candidates(s *store.Segment, w *Workspace) (rows []int, ok bool) {
	switch p.kind {
	case predCmp, predContains, predExists:
		if p.probe.kind == postNone {
			return nil, false
		}
		rows, ok := p.probe.run(s, w.nextCandidates())
		w.keepCandidates(rows)
		return rows, ok
	case predIn:
		// An empty membership accepts no row, which is an exact bound the
		// postings need not be consulted for.
		if len(p.lits) == 0 {
			return nil, true
		}
		if p.probe.kind != postIn {
			return nil, false
		}
		var acc []int
		for i := range p.needles {
			rows := s.AppendWhereContainsIndex(w.nextCandidates(), p.probe.path, p.needles[i])
			w.keepCandidates(rows)
			if i == 0 {
				acc = rows
				continue
			}
			acc = unionSortedInto(w.nextCandidates(), acc, rows)
			w.keepCandidates(acc)
		}
		return acc, true
	case predAnd:
		return andCandidates(p.kids, s, w)
	case predOr:
		return orCandidates(p.kids, s, w)
	default:
		// predIsNull and predNot are not postable. predInBound cannot reach a
		// DocSet at all — a join needs a database snapshot to resolve its inner
		// collection from, and RunInto rejects a join plan for every Source that
		// names one collection — so it falls here as the unpostable leaf it
		// would be anyway rather than as a case that could ever run.
		return nil, false
	}
}

// run executes a leaf probe, returning the primitive's ascending ordinal set.
// postNone reports "not postable" so the caller keeps the full scan.
func (pp postProbe) run(s *store.Segment, dst []int) ([]int, bool) {
	switch pp.kind {
	case postExists:
		return s.AppendWhereExists(dst, pp.path), true
	case postContains, postEq:
		return s.AppendWhereContainsIndex(dst, pp.path, pp.needle), true
	default:
		return nil, false
	}
}

// andCandidates intersects the candidate sets of the postable conjuncts. An
// unpostable conjunct is "every row" and is skipped; with no postable conjunct
// the conjunction cannot be bounded and reports ok false (full scan).
func andCandidates(kids []*compiledPredicate, s *store.Segment, w *Workspace) ([]int, bool) {
	var acc []int
	have := false
	for _, kid := range kids {
		rows, ok := kid.candidates(s, w)
		if !ok {
			continue
		}
		if !have {
			acc, have = rows, true
			continue
		}
		acc = intersectSortedInto(w.nextCandidates(), acc, rows)
		w.keepCandidates(acc)
	}
	if !have {
		return nil, false
	}
	return acc, true
}

// orCandidates unions the candidate sets of the disjuncts. Every disjunct must
// be postable; one unpostable disjunct forces the whole disjunction to the full
// scan, since it could otherwise accept a row no union would cover.
func orCandidates(kids []*compiledPredicate, s *store.Segment, w *Workspace) ([]int, bool) {
	var acc []int
	for i, kid := range kids {
		rows, ok := kid.candidates(s, w)
		if !ok {
			return nil, false
		}
		if i == 0 {
			acc = rows
			continue
		}
		acc = unionSortedInto(w.nextCandidates(), acc, rows)
		w.keepCandidates(acc)
	}
	return acc, true
}

// intersectSorted returns the sorted intersection of two ascending,
// deduplicated ordinal slices in one linear pass.
func intersectSortedInto(out, a, b []int) []int {
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

// unionSorted returns the sorted union of two ascending, deduplicated ordinal
// slices in one linear pass.
func unionSortedInto(out, a, b []int) []int {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// eqNeedle renders an equality literal as the JSON containment needle its
// equality maps onto: a number keeps its canonical spelling, a bool its keyword,
// and a string its JSON encoding. It reports false for a kind that has no scalar
// needle (never reached for a Cmp literal, which is always bool, number, or
// string).
// The needle bytes are carved from c's text arena, and the index built over
// them borrows them, so they live exactly as long as the plan does. The two
// keyword needles are package constants instead: they never vary, so spending
// arena bytes on a fresh copy of "true" per equality would be pure waste.
func (c *Compiler) eqNeedle(lit scalar) ([]byte, bool) {
	switch lit.kind {
	case kindBool:
		if lit.bval {
			return trueNeedle, true
		}
		return falseNeedle, true
	case kindNumber:
		return c.bytes(lit.num), true
	case kindString:
		buf := appendJSONString(c.tmp[:0], lit.sval)
		c.tmp = buf
		return c.bytes(buf), true
	default:
		return nil, false
	}
}

// The boolean equality needles. They are read-only for the whole process, so
// one copy each is enough.
var (
	trueNeedle  = []byte("true")
	falseNeedle = []byte("false")
)

// appendJSONString appends s as a JSON string literal (quotes, the two-character
// escapes, and \u00XX for the remaining control characters), so the needle
// decodes back to exactly s and BuildIndex accepts it. Bytes at or above 0x20
// other than " and \ are valid JSON content and pass through unaltered, which
// keeps already-valid UTF-8 intact.
func appendJSONString(dst []byte, s string) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			dst = append(dst, '\\', '"')
		case c == '\\':
			dst = append(dst, '\\', '\\')
		case c == '\n':
			dst = append(dst, '\\', 'n')
		case c == '\r':
			dst = append(dst, '\\', 'r')
		case c == '\t':
			dst = append(dst, '\\', 't')
		case c < 0x20:
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		default:
			dst = append(dst, c)
		}
	}
	return append(dst, '"')
}
