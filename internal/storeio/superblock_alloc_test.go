package storeio

import "testing"

func TestSuperblockCodecSteadyAllocation(t *testing.T) {
	state := []byte("allocation-free state root")
	root := testSuperblock(1, 4*uint64(testSuperblockPageSize), state)
	var first, second [SuperblockSize]byte
	if _, err := EncodeSuperblock(first[:], root); err != nil {
		t.Fatal(err)
	}
	root.Generation = 2
	root.StateOffset += uint64(root.PageSize)
	root.FileEnd += uint64(root.PageSize)
	if _, err := EncodeSuperblock(second[:], root); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := EncodeSuperblock(second[:], root); err != nil {
			panic(err)
		}
		if _, err := DecodeSuperblock(second[:]); err != nil {
			panic(err)
		}
		if _, _, err := SelectSuperblock(first[:], second[:]); err != nil {
			panic(err)
		}
		_ = PageChecksum(state)
	}); allocs != 0 {
		t.Fatalf("superblock codec allocations = %g, want 0", allocs)
	}
}
