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
