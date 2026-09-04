//go:build !go1.27 || go1.28 || !goexperiment.simd || (!arm64 && !amd64)

package storeio

import (
	"strings"
	"testing"
)

func TestCountCompactPackedEqualDispatchScalar(t *testing.T) {
	for _, name := range []string{
		compactPackedEqualDispatchName(countCompactPacked7EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked8EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked10EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked16EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked7LessImpl),
		compactPackedEqualDispatchName(countCompactPacked8LessImpl),
		compactPackedEqualDispatchName(countCompactPacked10LessImpl),
		compactPackedEqualDispatchName(countCompactPacked16LessImpl),
	} {
		if !strings.HasSuffix(name, "EqualScalar") && !strings.HasSuffix(name, "LessScalar") {
			t.Fatalf("portable dispatch=%q, want scalar", name)
		}
	}
}
