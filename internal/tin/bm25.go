package tin

// BM25 arithmetic dispatch: bm25Scores evaluates the Okapi formula over
// parallel tf/dl slices. The vector kernel (bm25_wide.go) handles pairs with
// Float64x2; everywhere else bm25Scalar runs. Every spelling performs the
// same IEEE operations in the same order, one rounding each:
//
//	norm = (1-b) + b*(dl/avg)
//	den  = f + k1*norm
//	s    = boost*idf*f*(k1+1) / den
//
// The scalar forms wrap each product that feeds an addition in an explicit
// float64 conversion: Go may otherwise fuse x*y+z into one FMA (it does on
// arm64), which rounds once where the vector lanes round twice. With that,
// the vector kernel, bm25Scalar, and bm25One agree bit for bit, so a score
// never depends on which path (full sort, top-K, WAND, sharded) computed it.
// The vector kernel still pads odd tails through its own lanes.

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
		out = append(out, bm25One(idf, avg, boost, tf[i], dl[i]))
	}
	return out
}

// bm25One evaluates a single document, shared by tails.
func bm25One(idf, avg, boost, f, dl float64) float64 {
	norm := (1 - bm25B) + float64(bm25B*(dl/avg))
	den := f + float64(bm25K1*norm)
	return boost * idf * f * (bm25K1 + 1) / den
}
