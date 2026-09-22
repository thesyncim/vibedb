package tin

// BM25 arithmetic dispatch: bm25Scores evaluates the Okapi formula over
// parallel tf/dl slices. The vector kernel (bm25_wide.go) handles pairs with
// Float64x2; everywhere else bm25Scalar runs. Both spell the same formula,
// but bit-identity between them is unachievable: the compiler fuses an FMA
// into the scalar denominator on some targets while the vector kernel keeps
// separate roundings. The differential test therefore demands 1e-12 relative
// agreement, which never flips a ranking — ties break by DocID.

// bm25Impl is the scoring kernel in force, selected by per-arch enable
// files exactly like the fold kernel.
var bm25Impl = bm25Scalar

// bm25Scores appends boost-scaled BM25 for len(tf) documents. tf holds term
// frequencies, dl document lengths; out receives the scores (appended, so it
// may be a reused scratch buffer).
func bm25Scores(idf, avg, boost float64, tf, dl, out []float64) []float64 {
	return bm25Impl(idf, avg, boost, tf, dl, out)
}

// bm25Scalar is the portable spelling and the differential oracle.
func bm25Scalar(idf, avg, boost float64, tf, dl, out []float64) []float64 {
	for i := range tf {
		f := tf[i]
		den := f + bm25K1*(1-bm25B+bm25B*dl[i]/avg)
		out = append(out, boost*idf*f*(bm25K1+1)/den)
	}
	return out
}

// bm25One evaluates a single document, shared by tails.
func bm25One(idf, avg, boost, f, dl float64) float64 {
	den := f + bm25K1*(1-bm25B+bm25B*dl/avg)
	return boost * idf * f * (bm25K1 + 1) / den
}
