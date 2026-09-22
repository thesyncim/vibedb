package tin

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestDocLengthDense pins the dense length runs: 0-based and 1-based
// sequential adds stay dense with exact lengths, replace overwrites in
// place, remove zeroes the slot, and packed (chunk, slot) ids stay dense
// per chunk instead of dying at the first chunk boundary. Sparse jumps and
// out-of-order arrivals stay map-served without poisoning their chunk's
// later contiguous runs, and no sparse id may grow a giant array. Score
// agreement elsewhere proves the values; every value assertion below is
// unchanged from the flat-array era.
func TestDocLengthDense(t *testing.T) {
	zero := NewIndex()
	for i := 0; i < 300; i++ {
		zero.Add(DocID(i), "common filler")
	}
	if c := zero.docLens[0]; c == nil || len(c.arr) != 300 {
		t.Fatalf("0-based adds: dense array len %d, want 300", len(c.arr))
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
	if c := one.docLens[0]; c == nil || len(c.arr) != 300 {
		t.Fatalf("1-based adds: dense array len %d, want 300", len(c.arr))
	}
	if got := one.docLength(42); got != 3 {
		t.Fatalf("doc 42 length = %d, want 3", got)
	}
	// Replace overwrites; remove zeroes; both keep the run dense.
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
	if one.docLens[0] == nil {
		t.Fatal("replace/remove dropped a dense run")
	}
	// Packed ids stay dense per chunk across chunk boundaries; removals
	// zero their slots in place and keep the run.
	packed := NewIndex()
	for chunk := uint32(0); chunk < 3; chunk++ {
		for slot := 0; slot < 64; slot++ {
			packed.Add(DocID(uint64(chunk)<<32|uint64(slot)), "common filler")
		}
		for slot := 1; slot < 64; slot += 2 {
			packed.Remove(DocID(uint64(chunk)<<32 | uint64(slot)))
		}
	}
	for chunk := uint32(0); chunk < 3; chunk++ {
		if c := packed.docLens[chunk]; c == nil || len(c.arr) != 64 {
			t.Fatalf("chunk %d: dense run len %d, want 64", chunk, len(c.arr))
		}
		for slot := 0; slot < 64; slot++ {
			want := uint32(2)
			if slot%2 == 1 {
				want = 0
			}
			if got := packed.docLength(DocID(uint64(chunk)<<32 | uint64(slot))); got != want {
				t.Fatalf("chunk %d slot %d length = %d, want %d", chunk, slot, got, want)
			}
		}
	}
	// Sparse jump and out-of-order arrival stay map-served with exact
	// values and bounded arrays: no sparse id grows a giant run.
	sparse := NewIndex()
	sparse.Add(1, "a")
	sparse.Add(DocID(1)<<40, "b")
	for _, c := range sparse.docLens {
		if len(c.arr) > 2 {
			t.Fatalf("wide gap grew a dense run of len %d", len(c.arr))
		}
	}
	if got := sparse.docLength(1); got != 1 {
		t.Fatalf("sparse doc 1 length = %d, want 1", got)
	}
	if got := sparse.docLength(DocID(1) << 40); got != 1 {
		t.Fatalf("sparse far doc length = %d, want 1", got)
	}
	ooo := NewIndex()
	ooo.Add(5, "a b")
	ooo.Add(3, "c")
	if got := ooo.docLength(3); got != 1 {
		t.Fatalf("ooo doc 3 length = %d, want 1", got)
	}
	if got := ooo.docLength(5); got != 2 {
		t.Fatalf("ooo doc 5 length = %d, want 2", got)
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
