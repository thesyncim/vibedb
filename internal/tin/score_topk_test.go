package tin

import (
	"reflect"
	"strings"
	"testing"
)

// topKExactIndex builds a corpus with heavy-tail frequencies (Zipf-like TF
// via multiplicative hashing), varied lengths, one ubiquitous term (tiny
// idf), and one rare term — the shapes that exercise block skipping,
// threshold margins, and tie orders.
func topKExactIndex(t *testing.T, seal bool, n int) *Index {
	t.Helper()
	ix := NewIndex()
	for i := 0; i < n; i++ {
		var sb strings.Builder
		sb.WriteString("common filler")
		if i%5 != 0 {
			tf := 1 + (i*2654435761)%512
			for k := 0; k < tf; k++ {
				sb.WriteString(" zipf")
			}
		}
		for k := 0; k < (i*97)%240; k++ {
			sb.WriteString(" pad")
		}
		if i < 3 {
			sb.WriteString(" rare")
		}
		ix.Add(DocID(i+1), sb.String())
	}
	if seal {
		ix.Seal()
	}
	return ix
}

// oldTopK runs the pre-fast-path pipeline: gather everything, then select.
func oldTopK(ix *Index, q Query, topK int) []Scored {
	var acc []Scored
	ix.scoreInto(q, &acc)
	return topKScored(acc, topK)
}

func TestScoreSingleTopKExact(t *testing.T) {
	for _, n := range []int{10, 130, 300, 2000} {
		for _, seal := range []bool{false, true} {
			ix := topKExactIndex(t, seal, n)
			terms := []string{"zipf", "common", "rare", "missing"}
			boosts := []float32{0, 1, 2.5, -1.5}
			topKs := []int{1, 2, 10, n - 1, n, n + 5, 0}
			for _, term := range terms {
				for _, boost := range boosts {
					q := Query{Op: OpTerm, Term: mustHash(t, term), Boost: boost}
					for _, topK := range topKs {
						got := ix.Score(q, topK, nil)
						want := oldTopK(ix, q, topK)
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("n=%d seal=%v term=%q boost=%v topK=%d:\n got=%v\nwant=%v",
								n, seal, term, boost, topK, got, want)
						}
					}
				}
			}
			// Removals leave minDocLen stale-low; bounds stay valid and
			// results must remain exact, shortest docs included.
			for i := 1; i <= n/10; i++ {
				ix.Remove(DocID(i))
			}
			for _, term := range []string{"zipf", "common"} {
				q := Query{Op: OpTerm, Term: mustHash(t, term)}
				for _, topK := range []int{1, 10, n / 2} {
					if !reflect.DeepEqual(ix.Score(q, topK, nil), oldTopK(ix, q, topK)) {
						t.Fatalf("post-remove n=%d seal=%v term=%q topK=%d mismatch",
							n, seal, term, topK)
					}
				}
			}
		}
	}
}

func TestScoreSingleTopKZeroAlloc(t *testing.T) {
	for _, seal := range []bool{false, true} {
		ix := topKExactIndex(t, seal, 2000)
		q := Query{Op: OpTerm, Term: mustHash(t, "zipf")}
		out := ix.Score(q, 10, nil)
		if n := testing.AllocsPerRun(20, func() {
			out = ix.Score(q, 10, out[:0])
		}); n != 0 {
			t.Fatalf("seal=%v: warmed top-10 allocates %.1f per run", seal, n)
		}
	}
}

// skewTopKIndex is the skipping benchmark corpus: 8k docs, "zipf" in 80%
// with heavy-tail TF so most blocks run threshold-cold after the heap
// fills, plus a ubiquitous "fill" term for the no-skip parity case.
func skewTopKIndex(t testing.TB, seal bool) *Index {
	t.Helper()
	ix := NewIndex()
	for i := 0; i < 8192; i++ {
		var sb strings.Builder
		sb.WriteString("fill")
		if i%5 != 0 {
			tf := 1 + (i*2654435761)%1024
			for k := 0; k < tf; k++ {
				sb.WriteString(" zipf")
			}
		}
		for k := 0; k < 20+(i*61)%400; k++ {
			sb.WriteString(" pad")
		}
		ix.Add(DocID(i+1), sb.String())
	}
	if seal {
		ix.Seal()
	}
	return ix
}

func benchmarkScoreTopKFast(b *testing.B, ix *Index, term string, topK int) {
	b.Helper()
	q := Query{Op: OpTerm, Term: mustHash(b, term)}
	out := ix.Score(q, topK, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Score(q, topK, out[:0])
	}
	_ = out
}

// benchmarkScoreTopKOld pins the old gather-then-quickselect pipeline on
// the same corpus so the fast path's speedup is measured, not asserted.
func benchmarkScoreTopKOld(b *testing.B, ix *Index, term string, topK int) {
	b.Helper()
	q := Query{Op: OpTerm, Term: mustHash(b, term)}
	var acc []Scored
	var got []Scored
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc = acc[:0]
		ix.scoreInto(q, &acc)
		got = topKScored(acc, topK)
	}
	_ = got
}

func BenchmarkScoreTopKSkewedSealed(b *testing.B) {
	benchmarkScoreTopKFast(b, skewTopKIndex(b, true), "zipf", 10)
}
func BenchmarkScoreTopKSkewedOpen(b *testing.B) {
	benchmarkScoreTopKFast(b, skewTopKIndex(b, false), "zipf", 10)
}
func BenchmarkScoreTopKSkewedOldSealed(b *testing.B) {
	benchmarkScoreTopKOld(b, skewTopKIndex(b, true), "zipf", 10)
}
func BenchmarkScoreTopKSkewedOldOpen(b *testing.B) {
	benchmarkScoreTopKOld(b, skewTopKIndex(b, false), "zipf", 10)
}
func BenchmarkScoreTopKUniformSealed(b *testing.B) {
	benchmarkScoreTopKFast(b, skewTopKIndex(b, true), "fill", 10)
}
func BenchmarkScoreTopKUniformOldSealed(b *testing.B) {
	benchmarkScoreTopKOld(b, skewTopKIndex(b, true), "fill", 10)
}
