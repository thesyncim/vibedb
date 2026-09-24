//go:build go1.27 && !go1.28 && goexperiment.simd && (amd64 || arm64)

package tin

import (
	"math"
	"math/rand"
	"testing"
)

// TestBM25WideMatchesScalar demands the wide kernel and the scalar spelling
// agree bit for bit across ordinary, extreme, and degenerate inputs. The
// scalar form rounds every product explicitly (no FMA fusion), matching the
// vector lanes' separate Mul/Add roundings, so any path that scores a
// document yields the identical float and ranking never depends on the path.
func TestBM25WideMatchesScalar(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	edge := []float64{0, 1, 1e-300, 5e-324, 1e300, math.MaxFloat64 / 2, 0.75, 1.2}
	var tf, dl []float64
	for i := 0; i < 513; i++ {
		switch i % 4 {
		case 0:
			tf = append(tf, float64(rng.Intn(1000)))
			dl = append(dl, float64(1+rng.Intn(100000)))
		case 1:
			tf = append(tf, edge[rng.Intn(len(edge))])
			dl = append(dl, edge[rng.Intn(len(edge))])
		case 2:
			tf = append(tf, 1)
			dl = append(dl, 1)
		default:
			tf = append(tf, math.SmallestNonzeroFloat64)
			dl = append(dl, 1e308)
		}
	}
	for _, tc := range [][3]float64{{1.5, 100, 1}, {0, 100, 1}, {3.2, 1, 2.5}, {0.7, 1e6, 0}} {
		idf, avg, boost := tc[0], tc[1], tc[2]
		wide := bm25Wide(idf, avg, boost, tf, dl, nil)
		scalar := bm25Scalar(idf, avg, boost, tf, dl, nil)
		if len(wide) != len(scalar) {
			t.Fatalf("length %d vs %d", len(wide), len(scalar))
		}
		for i := range wide {
			if math.Float64bits(wide[i]) != math.Float64bits(scalar[i]) &&
				!(math.IsNaN(wide[i]) && math.IsNaN(scalar[i])) {
				t.Fatalf("idf=%v avg=%v boost=%v tf=%v dl=%v: wide=%v scalar=%v",
					idf, avg, boost, tf[i], dl[i], wide[i], scalar[i])
			}
		}
	}
}
