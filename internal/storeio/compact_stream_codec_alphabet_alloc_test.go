//go:build !race

package storeio

import (
	"math/bits"
	"testing"
)

func TestCompactAlphabetReservoirWarmZeroAllocations(t *testing.T) {
	const slot = 2
	var scratch compactStreamScratch
	for _, cardinality := range []int{1, 2, 3, 5, 9, 17, 33, 64} {
		values := compactAlphabetReservoirTestValues(cardinality)
		plan, ok := scratch.measureAlphabet(slot, values, 0)
		wantWidth := bits.Len(uint(cardinality - 1))
		if !ok || plan.width != wantWidth {
			t.Fatalf("cardinality=%d alphabet plan=%+v ok=%v, want width %d",
				cardinality, plan, ok, wantWidth)
		}
		if warm := scratch.finishAlphabet(slot, values, plan); len(warm.data) != plan.totalBytes {
			t.Fatalf("cardinality=%d warm output bytes=%d, want %d",
				cardinality, len(warm.data), plan.totalBytes)
		}
		var outputBytes int
		if allocs := testing.AllocsPerRun(100, func() {
			encoded := scratch.finishAlphabet(slot, values, plan)
			outputBytes += len(encoded.data)
		}); allocs != 0 {
			t.Fatalf("cardinality=%d width=%d warm finish allocations=%v, want 0",
				cardinality, wantWidth, allocs)
		}
		if outputBytes == 0 {
			t.Fatalf("cardinality=%d optimized output was not observed", cardinality)
		}
	}
}
