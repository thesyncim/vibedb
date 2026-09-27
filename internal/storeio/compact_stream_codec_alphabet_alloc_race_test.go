//go:build race

package storeio

import (
	"testing"
)

func TestCompactAlphabetRaceAllocationReferenceControl(t *testing.T) {
	for _, cardinality := range []int{1, 64} {
		values := compactAlphabetReservoirTestValues(cardinality)
		var optimized, reference compactStreamScratch
		const slot = 2
		plan, ok := optimized.measureAlphabet(slot, values, 0)
		if !ok {
			t.Fatalf("cardinality=%d alphabet rejected", cardinality)
		}
		reference.alphabet[slot] = append(reference.alphabet[slot][:0], optimized.alphabet[slot]...)
		optimized.finishAlphabet(slot, values, plan)
		finishCompactAlphabetScalarReference(&reference, slot, values, plan)
		var optimizedBytes, referenceBytes int
		optimizedAllocs := testing.AllocsPerRun(100, func() {
			optimizedBytes += len(optimized.finishAlphabet(slot, values, plan).data)
		})
		referenceAllocs := testing.AllocsPerRun(100, func() {
			referenceBytes += len(finishCompactAlphabetScalarReference(
				&reference, slot, values, plan,
			).data)
		})
		t.Logf("race allocation control cardinality=%d width=%d: reservoir=%v scalar=%v allocs/run",
			cardinality, plan.width, optimizedAllocs, referenceAllocs)
		if optimizedAllocs != referenceAllocs || optimizedBytes == 0 || referenceBytes == 0 {
			t.Fatalf("cardinality=%d race allocations differ: reservoir=%v scalar=%v",
				cardinality, optimizedAllocs, referenceAllocs)
		}
	}
}
