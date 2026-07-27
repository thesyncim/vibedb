package storeio

import "testing"

func TestChunkDirectorySteadyAllocation(t *testing.T) {
	header := testChunkDirectoryHeader(0, 0, ^uint64(0))
	refs := testChunkDirectoryRefs(header)
	fileEnd := testChunkDirectoryFileEnd(refs)
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeChunkDirectoryPage(page, header, refs, fileEnd, testChunkDirectoryNextLogicalID); err != nil {
		t.Fatal(err)
	}
	view, err := OpenChunkDirectoryPage(page, fileEnd, testChunkDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := EncodeChunkDirectoryPage(page, header, refs, fileEnd, testChunkDirectoryNextLogicalID); err != nil {
			panic(err)
		}
		opened, err := OpenChunkDirectoryPage(page, fileEnd, testChunkDirectoryNextLogicalID)
		if err != nil {
			panic(err)
		}
		if _, ok := opened.Lookup(63); !ok {
			panic("directory lookup miss")
		}
		if _, ok := view.RefAt(63); !ok {
			panic("directory rank miss")
		}
	}); allocs != 0 {
		t.Fatalf("chunk-directory codec and lookup allocations = %g, want 0", allocs)
	}
}
