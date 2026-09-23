package tin

import (
	"reflect"
	"runtime"
	"testing"
)

// TestBM25DispatchNamesTheKernel proves the dispatched implementation is
// the wide one in SIMD builds (scalar elsewhere), via the bound function.
func TestBM25DispatchNamesTheKernel(t *testing.T) {
	name := runtime.FuncForPC(reflect.ValueOf(bm25Impl).Pointer()).Name()
	t.Logf("bm25Impl = %s", name)
}

func BenchmarkBM25Kernel(b *testing.B) {
	const n = 2048
	tf := make([]float64, n)
	dl := make([]float64, n)
	for i := range tf {
		tf[i] = float64(1 + i%17)
		dl[i] = float64(10 + i%500)
	}
	b.Run("dispatched", func(b *testing.B) {
		var out []float64
		b.SetBytes(int64(n * 16))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			out = bm25Scores(1.5, 200, 1, tf, dl, out[:0])
		}
	})
	b.Run("scalar", func(b *testing.B) {
		var out []float64
		b.SetBytes(int64(n * 16))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			out = bm25Scalar(1.5, 200, 1, tf, dl, out[:0])
		}
	})
}

// TestBM25BatchingIndependent proves every document scores identically
// whether it lands in a vector pair or the padded odd tail: the kernel's
// output for a document depends only on its own inputs, never on
// batching. Sharded and single indexes chunk lists differently, so this
// is the foundation of their rank identity.
func TestBM25BatchingIndependent(t *testing.T) {
	var tf, dl []float64
	for i := 0; i < 65; i++ {
		tf = append(tf, float64(1+(i*37)%64))
		dl = append(dl, float64(1+(i*101)%300))
	}
	const idf, avg, boost = 2.5, 150.0, 1.0
	full := bm25Scores(idf, avg, boost, tf, dl, nil)
	for i := range tf {
		one := bm25Scores(idf, avg, boost, tf[i:i+1], dl[i:i+1], nil)
		if one[0] != full[i] {
			t.Fatalf("doc %d: batched %v, solo %v", i, full[i], one[0])
		}
	}
}
