package tin

import (
	"math"
	"testing"
)

// scoredRankEqual asserts identical ranking with 1e-12 score agreement:
// the AND fast path sums kids in deterministic kid order while the old
// pipeline sums in unstable-sort order, so last-ulp rounding may differ
// without ever flipping a rank (the kernel differential's own standard).
func scoredRankEqual(a, b []Scored) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Doc != b[i].Doc {
			return false
		}
		rel := math.Abs(a[i].Score-b[i].Score) / (1 + math.Abs(b[i].Score))
		if rel > 1e-12 {
			return false
		}
	}
	return true
}

func TestScoreAndTopK(t *testing.T) {
	terms := []string{"zipf", "common", "rare", "missing"}
	kidSets := [][]int{{0, 1}, {1, 0}, {0, 2}, {0, 3}, {3, 0}, {1, 2}, {0, 1, 2}, {2, 2}}
	boostSets := [][]float32{{0, 0}, {1, 1}, {2, -1}, {1, 1, 0}, {-2, 3}}
	for _, n := range []int{50, 500, 3000} {
		for _, seal := range []bool{false, true} {
			ix := topKExactIndex(t, seal, n)
			for _, ks := range kidSets {
				for _, bs := range boostSets {
					if len(bs) < len(ks) {
						continue
					}
					q := Query{Op: OpAnd, Boost: 1}
					for i, k := range ks {
						q.Kids = append(q.Kids, Query{Op: OpTerm, Term: mustHash(t, terms[k]), Boost: bs[i]})
					}
					for _, topK := range []int{1, 5, 50, 0} {
						got := ix.Score(q, topK, nil)
						want := oldTopK(ix, q, topK)
						if !scoredRankEqual(got, want) {
							t.Fatalf("n=%d seal=%v kids=%v boosts=%v topK=%d:\n got=%v\nwant=%v",
								n, seal, ks, bs, topK, got, want)
						}
					}
				}
			}
		}
	}
}

// andBenchIndex: 20k docs where "needle" is rare (12 docs) and "common"
// is everywhere — the selective conjunction shape.
func andBenchIndex(t testing.TB, seal bool) *Index {
	t.Helper()
	ix := NewIndex()
	for i := 0; i < 20000; i++ {
		text := "common filler words here"
		if i%1700 == 0 {
			text += " needle"
		}
		ix.Add(DocID(i+1), text)
	}
	if seal {
		ix.Seal()
	}
	return ix
}

func andQuery(t testing.TB, terms ...string) Query {
	t.Helper()
	q := Query{Op: OpAnd}
	for _, term := range terms {
		q.Kids = append(q.Kids, Query{Op: OpTerm, Term: mustHash(t, term)})
	}
	return q
}

func benchmarkScoreAndFast(b *testing.B, ix *Index, q Query, topK int) {
	b.Helper()
	out := ix.Score(q, topK, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Score(q, topK, out[:0])
	}
	_ = out
}

func benchmarkScoreAndOld(b *testing.B, ix *Index, q Query, topK int) {
	b.Helper()
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

func BenchmarkScoreAndSelectiveSealed(b *testing.B) {
	ix := andBenchIndex(b, true)
	benchmarkScoreAndFast(b, ix, andQuery(b, "needle", "common"), 10)
}
func BenchmarkScoreAndSelectiveOpen(b *testing.B) {
	ix := andBenchIndex(b, false)
	benchmarkScoreAndFast(b, ix, andQuery(b, "needle", "common"), 10)
}
func BenchmarkScoreAndSelectiveOldSealed(b *testing.B) {
	ix := andBenchIndex(b, true)
	benchmarkScoreAndOld(b, ix, andQuery(b, "needle", "common"), 10)
}
func BenchmarkScoreAndSelectiveOldOpen(b *testing.B) {
	ix := andBenchIndex(b, false)
	benchmarkScoreAndOld(b, ix, andQuery(b, "needle", "common"), 10)
}
func BenchmarkScoreAndUnselectiveSealed(b *testing.B) {
	ix := andBenchIndex(b, true)
	benchmarkScoreAndFast(b, ix, andQuery(b, "common", "filler"), 10)
}
func BenchmarkScoreAndUnselectiveOldSealed(b *testing.B) {
	ix := andBenchIndex(b, true)
	benchmarkScoreAndOld(b, ix, andQuery(b, "common", "filler"), 10)
}
