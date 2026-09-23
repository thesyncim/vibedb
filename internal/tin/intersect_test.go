package tin

import (
	"fmt"
	"math/rand"
	"testing"
)

// The dispatch path must equal the scalar oracle at every size, on every
// build: without SIMD it is the scalar spelling; with SIMD it is the
// vector kernel above the threshold and the scalar spelling below it.
func TestIntersectIntoMatchesScalar(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for _, n := range []int{0, 1, 2, 3, 31, 32, 33, 63, 64, 65, 100, 129, 200, 300} {
		for _, density := range []float64{0, 0.1, 0.5, 0.9, 1} {
			a := sortedRun(rng, n, density)
			b := sortedRun(rng, n, density)
			want := intersectScalar(a, b, nil)
			if got := intersectInto(a, b, nil); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("n=%d density=%v fresh: got %v want %v", n, density, got, want)
			}
			// In-place with out aliasing a, as every call site does.
			acc := append([]DocID(nil), a...)
			if got := intersectInto(acc, b, acc[:0]); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("n=%d density=%v alias: got %v want %v", n, density, got, want)
			}
		}
	}
	// Structured edges: identical, disjoint, subset, interleaved, and
	// single-element runs around the threshold.
	a := make([]DocID, 0, 200)
	for i := 0; i < 200; i++ {
		a = append(a, DocID(i))
	}
	edges := [][2][]DocID{
		{a, append([]DocID(nil), a...)},
		{a, nil},
		{nil, a},
		{a, []DocID{1000, 1001}},
		{a[:100], a},
		{a, a[100:]},
		{[]DocID{5}, []DocID{5}},
		{[]DocID{5}, []DocID{6}},
	}
	for i, e := range edges {
		want := intersectScalar(e[0], e[1], nil)
		if got := intersectInto(e[0], e[1], nil); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("edge %d: got %v want %v", i, got, want)
		}
		acc := append([]DocID(nil), e[0]...)
		if got := intersectInto(acc, e[1], acc[:0]); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("edge %d alias: got %v want %v", i, got, want)
		}
	}
}

// referenceMerge intersects term kids the old way: decode every list fully,
// then merge pairwise. matchTermsInto must equal it exactly while skipping
// the long lists' decodes.
func referenceMerge(ix *Index, kids []Query) []DocID {
	var acc []DocID
	for i, k := range kids {
		p := ix.post[k.Term]
		if p == nil {
			return nil
		}
		var cur []DocID
		if p.sealed == nil {
			cur = append(cur, p.ids...)
		} else {
			cur = p.sealed.sealedAppendIDs(cur)
		}
		if i == 0 {
			acc = cur
			continue
		}
		acc = intersectScalar(acc, cur, acc[:0])
	}
	return acc
}

// TestMatchTermsIntoAgrees proves the layout-aware conjunction equals both
// the decode-and-merge reference and the independent span route, over even
// and skewed corpora, open and sealed, including missing terms.
func TestMatchTermsIntoAgrees(t *testing.T) {
	skewed := map[DocID]string{}
	for i := 0; i < 600; i++ {
		text := "common filler words here"
		if i%200 == 0 {
			text += " needle"
		}
		if i%100 == 0 {
			text += " pin"
		}
		skewed[DocID(i+1)] = text
	}
	corpora := map[string]map[DocID]string{
		"mixed":  sealedCorpusDocs(),
		"skewed": skewed,
	}
	queries := [][]string{
		{"common"},
		{"common", "filler"},
		{"common", "selective"},
		{"needle", "common"},
		{"needle", "pin", "common"},
		{"needle", "absent"},
		{"absent"},
		{"filler", "words", "here", "common"},
	}
	for name, docs := range corpora {
		for _, layout := range []string{"open", "sealed"} {
			ix := NewIndex()
			for id, text := range docs {
				ix.Add(id, text)
			}
			if layout == "sealed" {
				ix.Seal()
			}
			// The reference reads posting rows directly, so sort first:
			// entry points (Match/Score) always sort before evaluating.
			ix.ensureSorted()
			for _, terms := range queries {
				var kids []Query
				for _, term := range terms {
					kids = append(kids, Query{Op: OpTerm, Term: mustHash(t, term)})
				}
				q := Query{Op: OpAnd, Kids: kids}
				want := referenceMerge(ix, kids)
				if got := ix.matchTermsInto(kids, nil); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("%s/%s %q: termsInto %v, want %v", name, layout, terms, got, want)
				}
				if got := ix.Match(q, nil); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("%s/%s %q: Match %v, want %v", name, layout, terms, got, want)
				}
				spans := projectDocs(ix.evalInto(q, nil))
				if fmt.Sprint(spans) != fmt.Sprint(want) {
					t.Fatalf("%s/%s %q: spans %v, want %v", name, layout, terms, spans, want)
				}
			}
		}
	}
}

// sortedRun builds an ascending DocID run of length n with roughly density
// overlap against a shared base sequence.
func sortedRun(rng *rand.Rand, n int, density float64) []DocID {
	out := make([]DocID, 0, n)
	id := DocID(rng.Intn(3))
	for len(out) < n {
		id += DocID(1 + rng.Intn(3))
		if density == 1 || (density > 0 && rng.Float64() < density) || (density == 0 && len(out) == 0) {
			out = append(out, id)
		} else if density == 0 {
			out = append(out, id+1000000)
			id += 1000000
		}
	}
	return out
}
