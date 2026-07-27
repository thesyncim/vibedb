package durable

import (
	"math"
	"testing"
)

func TestFileStoreFloat64EncodingPreservesSignedZero(t *testing.T) {
	if got := fileStoreFloat64Encoding(math.Copysign(0, -1)); got != 3 {
		t.Fatalf("signed-zero encoding rank = %d, want general float64", got)
	}
	if got := fileStoreFloat64Encoding(0); got != 0 {
		t.Fatalf("positive-zero encoding rank = %d, want uint8", got)
	}
}
