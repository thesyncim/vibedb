//go:build go1.27 && !go1.28 && goexperiment.simd && (amd64 || arm64)

package tin

import "simd/archsimd"

// bm25Wide evaluates two BM25 scores per iteration with 128-bit float
// vectors (native on both NEON and AVX2). The op sequence per lane matches
// bm25Scalar exactly — separate Mul then Add, never MulAdd — so results are
// bit-identical while instruction-level parallelism doubles.
func bm25Wide(idf, avg, boost float64, tf, dl, out []float64) []float64 {
	n := len(tf) &^ 1
	if n > 0 {
		k1 := archsimd.BroadcastFloat64x2(bm25K1)
		base := archsimd.BroadcastFloat64x2(1 - bm25B)
		slope := archsimd.BroadcastFloat64x2(bm25B)
		avgv := archsimd.BroadcastFloat64x2(avg)
		idfv := archsimd.BroadcastFloat64x2(idf)
		boostv := archsimd.BroadcastFloat64x2(boost)
		k1p1 := archsimd.BroadcastFloat64x2(bm25K1 + 1)
		var pair [2]float64
		i := 0
		for ; i < n; i += 2 {
			fv := archsimd.LoadFloat64x2(tf[i : i+2])
			dv := archsimd.LoadFloat64x2(dl[i : i+2])
			// den = f + k1*(base + slope*(dl/avg)); same order as scalar.
			norm := base.Add(slope.Mul(dv.Div(avgv)))
			den := fv.Add(k1.Mul(norm))
			sc := boostv.Mul(idfv).Mul(fv).Mul(k1p1).Div(den)
			sc.Store(pair[:])
			out = append(out, pair[0], pair[1])
		}
	}
	for i := n; i < len(tf); i++ {
		out = append(out, bm25One(idf, avg, boost, tf[i], dl[i]))
	}
	return out
}
