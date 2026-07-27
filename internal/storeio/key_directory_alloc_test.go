package storeio

import "testing"

var keyLocationSink KeyLocation
var keyRefSink PageRef

func TestKeyDirectorySteadyAllocation(t *testing.T) {
	leafHeader := testKeyDirectoryHeader(0, 9)
	entries := []KeyDirectoryEntry{
		{Key: []byte("alpha"), Location: KeyLocation{Chunk: 1, Slot: 2}},
		{Key: []byte("omega"), Location: KeyLocation{Chunk: 2, Slot: 3}},
	}
	branchHeader := testKeyDirectoryHeader(1, 10)
	children := []KeyDirectoryChild{
		{Lower: []byte(""), Ref: testKeyDirectoryRef(2, 4, 10)},
		{Lower: []byte("m"), Ref: testKeyDirectoryRef(3, 5, 11)},
	}
	leafPage := make([]byte, testSuperblockPageSize)
	branchPage := make([]byte, testSuperblockPageSize)
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := EncodeKeyDirectoryLeaf(leafPage, leafHeader, entries, testKeyDirectoryNextLogicalID, 3, 64); err != nil {
			panic(err)
		}
		leaf, err := OpenKeyDirectoryPage(leafPage, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 3, 64)
		if err != nil {
			panic(err)
		}
		keyLocationSink, _ = leaf.Lookup([]byte("omega"))
		if _, err := EncodeKeyDirectoryBranch(branchPage, branchHeader, children, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID); err != nil {
			panic(err)
		}
		branch, err := OpenKeyDirectoryPage(branchPage, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 3, 64)
		if err != nil {
			panic(err)
		}
		keyRefSink, _ = branch.Child([]byte("omega"))
	}); allocs != 0 {
		t.Fatalf("key-directory codec and lookup allocations = %g, want 0", allocs)
	}
}
