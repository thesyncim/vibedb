//go:build !race

package driver

import "testing"

func TestCatalogVibeEncoderPreallocatedAllocations(t *testing.T) {
	catalog := representativeLargeCatalog()
	bound, err := catalogSizeUpperBound(catalog)
	if err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		raw, encodeErr := appendCatalogJSON(make([]byte, 0, bound), catalog)
		if encodeErr != nil || len(raw) == 0 || cap(raw) != bound {
			panic("catalog encoder did not retain caller capacity")
		}
	})
	// Output, sorted table names, and encoder scratch are the complete ordinary
	// allocation budget. This gate intentionally does not run under -race,
	// whose instrumentation adds variable bookkeeping allocations.
	if allocations > 3 {
		t.Fatalf("preallocated catalog encode allocations = %.1f, maximum 3", allocations)
	}
}
