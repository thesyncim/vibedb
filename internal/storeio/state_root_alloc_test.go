package storeio

import "testing"

func TestStateRootCodecSteadyAllocation(t *testing.T) {
	root, fileEnd := testStateRoot(9)
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeStateRootPage(page, root, fileEnd); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := EncodeStateRootPage(page, root, fileEnd); err != nil {
			panic(err)
		}
		if _, err := DecodeStateRootPage(page, fileEnd); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("state-root codec allocations = %g, want 0", allocs)
	}
}
