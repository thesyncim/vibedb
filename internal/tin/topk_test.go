package tin

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestDocLengthDense pins the dense length array: 0-based and 1-based
// sequential adds stay dense with exact lengths, replace overwrites in
// place, remove zeroes the slot, and the first sparse jump (or out-of-order
// arrival) drops the array with the map staying source of truth. Score
// agreement elsewhere proves the values.
func TestDocLengthDense(t *testing.T) {
	zero := NewIndex()
	for i := 0; i < 300; i++ {
		zero.Add(DocID(i), "common filler")
	}
	if zero.docLens == nil || len(zero.docLens) != 300 {
		t.Fatalf("0-based adds: dense array len %d, want 300", len(zero.docLens))
	}
	for i := 0; i < 300; i++ {
		if got := zero.docLength(DocID(i)); got != 2 {
			t.Fatalf("doc %d length = %d, want 2", i, got)
		}
	}
	one := NewIndex()
	for i := 1; i <= 300; i++ {
		one.Add(DocID(i), "common filler extra")
	}
	if one.docLens == nil || len(one.docLens) != 300 {
		t.Fatalf("1-based adds: dense array len %d, want 300", len(one.docLens))
	}
	if got := one.docLength(42); got != 3 {
		t.Fatalf("doc 42 length = %d, want 3", got)
	}
	// Replace overwrites; remove zeroes; both keep the array dense.
	one.Add(42, "changed")
	if got := one.docLength(42); got != 1 {
		t.Fatalf("replaced doc 42 length = %d, want 1", got)
	}
	if !one.Remove(43) {
		t.Fatal("remove reported absent")
	}
	if got := one.docLength(43); got != 0 {
		t.Fatalf("removed doc 43 length = %d, want 0", got)
	}
	if one.docLens == nil {
		t.Fatal("replace/remove dropped a dense array")
	}
	// Sparse jump and out-of-order arrival fall back to the map.
	sparse := NewIndex()
	sparse.Add(1, "a")
	sparse.Add(DocID(1)<<40, "b")
	if sparse.docLens != nil {
		t.Fatal("wide gap kept a dense array")
	}
	if got := sparse.docLength(1); got != 1 {
		t.Fatalf("sparse doc 1 length = %d, want 1", got)
	}
	ooo := NewIndex()
	ooo.Add(5, "a b")
	ooo.Add(3, "c")
	if ooo.docLens != nil {
		t.Fatal("out-of-order arrival kept a dense array")
	}
	if got := ooo.docLength(3); got != 1 {
		t.Fatalf("ooo doc 3 length = %d, want 1", got)
	}
}

// TestTopKScoredPartition directly pins the partition logic: all-equal
// scores (the quadratic trap for two-way schemes), reversed and shuffled
// input orders, random scores fuzzed against full sort, every K edge.
func TestTopKScoredPartition(t *testing.T) {
	orders := map[string]func(n int) []Scored{
		"ascending": func(n int) []Scored {
			s := make([]Scored, n)
			for i := range s {
				s[i] = Scored{Doc: DocID(i + 1), Score: 1.5}
			}
			return s
		},
		"descending": func(n int) []Scored {
			s := make([]Scored, n)
			for i := range s {
				s[i] = Scored{Doc: DocID(n - i), Score: 1.5}
			}
			return s
		},
		"shuffled": func(n int) []Scored {
			s := make([]Scored, n)
			for i := range s {
				s[i] = Scored{Doc: DocID((i*2654435761)%(n+1) + 1), Score: 1.5}
			}
			return s
		},
	}
	for name, build := range orders {
		for _, n := range []int{0, 1, 2, 3, 10, 100, 1000} {
			full := append([]Scored(nil), build(n)...)
			sortScored(full)
			for _, k := range []int{0, 1, 2, 3, n / 2, n - 1, n, n + 1} {
				if k < 0 {
					continue
				}
				got := topKScored(append([]Scored(nil), build(n)...), k)
				want := full
				if k > 0 && len(want) > k {
					want = want[:k]
				}
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("%s n=%d k=%d:\n got  %v\n want %v", name, n, k, got, want)
				}
			}
		}
	}
	// Random scores fuzzed against full sort across sizes and K.
	rng := rand.New(rand.NewSource(9))
	for _, n := range []int{0, 1, 2, 5, 63, 64, 65, 200, 1000} {
		base := make([]Scored, n)
		for i := range base {
			base[i] = Scored{Doc: DocID(i + 1), Score: rng.NormFloat64()}
		}
		full := append([]Scored(nil), base...)
		sortScored(full)
		for _, k := range []int{0, 1, 2, n / 3, n - 1, n, n + 5} {
			got := topKScored(append([]Scored(nil), base...), k)
			want := full
			if k > 0 && len(want) > k {
				want = want[:k]
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("random n=%d k=%d mismatch", n, k)
			}
		}
	}
}

// TestTopKScoredMatchesFullSort proves top-K selection bit-identical to
// sorting everything and truncating: same total order, unique ranks, same
// output — over both layouts, the whole operator corpus, skewed shapes,
// and every K edge. (Equal-score inputs live in TestTopKScoredPartition:
// zero boost normalizes to 1, so no query spelling produces them.)
func TestTopKScoredMatchesFullSort(t *testing.T) {
	skewed := map[DocID]string{}
	for i := 0; i < 600; i++ {
		text := "common filler words here"
		if i%200 == 0 {
			text += " needle"
		}
		if i%7 == 0 {
			text += " selective selective selective"
		}
		skewed[DocID(i+1)] = text
	}
	tiny := map[DocID]string{1: "a b", 2: "b c", 3: "a c"}
	corpora := map[string]map[DocID]string{
		"mixed":  sealedCorpusDocs(),
		"skewed": skewed,
		"tiny":   tiny,
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
			var queries []Query
			for _, input := range scoreCorpusInputs {
				q, err := ix.ParseTINQL(input)
				if err != nil {
					t.Fatalf("ParseTINQL(%q): %v", input, err)
				}
				queries = append(queries, q)
			}
			for _, q := range queries {
				full := ix.Score(q, 0, nil)
				for _, topK := range []int{0, 1, 2, 3, 5, 7, 10, 100, len(full) - 1, len(full), len(full) + 1} {
					if topK < 0 {
						continue
					}
					got := ix.Score(q, topK, nil)
					want := full
					if topK > 0 && len(want) > topK {
						want = want[:topK]
					}
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("%s/%s topK=%d:\n got  %v\n want %v", name, layout, topK, got, want)
					}
				}
			}
		}
	}
}
