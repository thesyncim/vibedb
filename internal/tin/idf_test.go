package tin

import (
	"math"
	"testing"
)

// The IDF must be the smoothed BM25 form ln(1+(N-df+0.5)/(df+0.5)): a
// precedence slip once computed ln(1+(N-df)+0.5/(df+0.5)), which barely
// varies with df and so ranked common terms almost like rare ones.
func TestIDFIsSmoothedBM25(t *testing.T) {
	for _, tc := range []struct{ n, df int }{{1, 1}, {10, 1}, {10, 5}, {10, 10}, {1000, 3}} {
		want := math.Log(1 + (float64(tc.n-tc.df)+0.5)/(float64(tc.df)+0.5))
		if got := idf(tc.n, tc.df); math.Abs(got-want) > 1e-12 || got <= 0 {
			t.Fatalf("idf(%d,%d)=%v want %v", tc.n, tc.df, got, want)
		}
	}
	for df := 1; df < 100; df++ {
		if idf(100, df+1) >= idf(100, df) {
			t.Fatalf("idf not decreasing in df at %d", df)
		}
	}
	// A term in 1 of 100 documents must weigh far more than one in 50 of 100.
	if ratio := idf(100, 1) / idf(100, 50); ratio < 4 {
		t.Fatalf("rare/common idf ratio=%v", ratio)
	}
}
