package tin

import (
	"strings"
	"testing"
)

// wandCorpus builds a skewed single index: ubiquitous, mid, rare, and
// planted identical triples for exact ties. The caller seals the twin.
func wandCorpus() *Index {
	ix := NewIndex()
	for i := 0; i < 600; i++ {
		var sb strings.Builder
		sb.WriteString("common")
		if i%3 == 0 {
			sb.WriteString(" mid balance")
		}
		if i%17 == 0 {
			sb.WriteString(" rare select")
		}
		if i%10 == 0 {
			sb.WriteString(" tiea tieb")
		}
		if i%25 == 0 {
			sb.WriteString(" tiea tiea tieb tieb")
		}
		ix.Add(DocID(i+1), sb.String())
	}
	return ix
}

// streamOrTopK is the pre-WAND oracle: Score's own fallback (full
// scoreInto plus topKScored), replicated exactly.
func streamOrTopK(ix *Index, q Query, topK int) []Scored {
	ix.scratchS = ix.scratchS[:0]
	ix.scoreInto(q, &ix.scratchS)
	s := ix.scratchS
	s = topKScored(s, topK)
	out := append([]Scored(nil), s...)
	ix.scratchS = ix.scratchS[:0]
	return out
}

// TestScoreOrWandTopKExact proves WAND ranks like streaming: identical
// Doc order with scores agreeing to 1e-12 (fixed kid-order sums versus
// mergeScores order, the AND contract), over open and sealed twins,
// boosts, missing kids, ties, and every topK. Ubiquitous disjunctions
// must decline through the pre-gate instead of paying cursor overhead.
func TestScoreOrWandTopKExact(t *testing.T) {
	patterns := []string{
		`mid OR rare`, `rare OR select`, `tiea OR tieb`,
		`mid OR rare OR select`, `common OR rare`,
		`mid^2 OR rare`, `mid OR rare^0.5`, `missing OR rare`,
		`missing OR absent`, `mid* OR rare`,
	}
	for _, sealed := range []bool{false, true} {
		ix := wandCorpus()
		if sealed {
			ix.Seal()
		}
		for _, pattern := range patterns {
			q, err := ix.ParseTINQL(pattern)
			if err != nil {
				t.Fatalf("sealed=%v ParseTINQL(%q): %v", sealed, pattern, err)
			}
			for _, topK := range []int{1, 5, 50} {
				got, ok := ix.scoreOrTopK(q, topK, nil)
				want := streamOrTopK(ix, q, topK)
				if pattern == "missing OR absent" {
					if !ok || len(got) != 0 {
						t.Fatalf("sealed=%v %s: ok=%v hits=%v, want empty serve", sealed, pattern, ok, got)
					}
					continue
				}
				if !ok {
					t.Fatalf("sealed=%v %s topK=%d declined, want WAND", sealed, pattern, topK)
				}
				if len(got) != len(want) {
					t.Fatalf("sealed=%v %s topK=%d: %d hits, want %d", sealed, pattern, topK, len(got), len(want))
				}
				for i := range got {
					if got[i].Doc != want[i].Doc {
						t.Fatalf("sealed=%v %s topK=%d hit %d doc = %v, want %v",
							sealed, pattern, topK, i, got[i].Doc, want[i].Doc)
					}
					if !scoresClose(got[i].Score, want[i].Score) {
						t.Fatalf("sealed=%v %s topK=%d hit %d score = %v, want %v",
							sealed, pattern, topK, i, got[i].Score, want[i].Score)
					}
				}
			}
		}
		// Ubiquitous disjunctions decline: near-total scoring must not
		// pay cursor overhead. The floor keeps small unions serving,
		// so the decline case needs an above-floor corpus.
		big := NewIndex()
		for i := 0; i < 3000; i++ {
			big.Add(DocID(i+1), "ubiq widespread")
		}
		if sealed {
			big.Seal()
		}
		for _, pattern := range []string{`ubiq OR widespread`} {
			q, err := big.ParseTINQL(pattern)
			if err != nil {
				t.Fatalf("sealed=%v ParseTINQL(%q): %v", sealed, pattern, err)
			}
			if _, ok := big.scoreOrTopK(q, 10, nil); ok {
				t.Fatalf("sealed=%v %s served, want pre-gate decline", sealed, pattern)
			}
		}
		// Non-term members decline.
		for _, pattern := range []string{`"tiea tieb" OR mid`, `mid OR (rare AND select)`} {
			q, err := ix.ParseTINQL(pattern)
			if err != nil {
				continue
			}
			if _, ok := ix.scoreOrTopK(q, 10, nil); ok {
				t.Fatalf("sealed=%v %s served, want shape decline", sealed, pattern)
			}
		}
	}
}

// TestScoreOrWandTopKMatchesScore proves the Score entry serves OR
// top-K through WAND end to end: Score's verdicts equal the streaming
// oracle wherever WAND engages.
func TestScoreOrWandTopKMatchesScore(t *testing.T) {
	ix := wandCorpus()
	for _, pattern := range []string{
		`mid OR rare`, `tiea OR tieb`, `mid OR rare OR select`, `common OR rare`,
	} {
		q, err := ix.ParseTINQL(pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, topK := range []int{1, 10, 100} {
			got := ix.Score(q, topK, nil)
			want := streamOrTopK(ix, q, topK)
			if len(got) != len(want) {
				t.Fatalf("%s topK=%d: %d hits, want %d", pattern, topK, len(got), len(want))
			}
			for i := range got {
				if got[i].Doc != want[i].Doc {
					t.Fatalf("%s topK=%d hit %d doc = %v, want %v", pattern, topK, i, got[i].Doc, want[i].Doc)
				}
				if !scoresClose(got[i].Score, want[i].Score) {
					t.Fatalf("%s topK=%d hit %d score = %v, want %v", pattern, topK, i, got[i].Score, want[i].Score)
				}
			}
		}
	}
}
