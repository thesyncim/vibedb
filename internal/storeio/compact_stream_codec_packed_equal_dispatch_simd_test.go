//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package storeio

import (
	"strings"
	"testing"
)

func TestCountCompactPackedEqualDispatchNEON(t *testing.T) {
	for _, name := range []string{
		compactPackedEqualDispatchName(countCompactPacked7EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked8EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked10EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked16EqualImpl),
	} {
		if !strings.HasSuffix(name, "EqualNEON") {
			t.Fatalf("SIMD arm64 dispatch=%q, want NEON", name)
		}
	}
}
