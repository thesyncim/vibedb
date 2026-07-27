package storeio

import "testing"

func testPostingEntries() []PostingEntry {
	entries := make([]PostingEntry, 1900)
	for i := range entries {
		entries[i] = PostingEntry{
			Chunk: uint32(10 + i*2),
			Bits:  uint64(1) << uint((i*17)&63),
		}
	}
	return entries
}

func TestPostingPageSteadyAllocation(t *testing.T) {
	entries := testPostingEntries()
	header := testPostingHeader()
	segments := []PostingSegment{{StreamID: 7, TupleHash: 99, Entries: entries}}
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodePostingPage(page, header, segments, testPostingNextLogicalID, testPostingIndexHighWater); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := EncodePostingPage(page, header, segments, testPostingNextLogicalID, testPostingIndexHighWater); err != nil {
			panic(err)
		}
		view, err := OpenPostingPage(page, testPostingNextLogicalID, testPostingIndexHighWater)
		if err != nil {
			panic(err)
		}
		segment, ok := view.Lookup(7)
		if !ok {
			panic("posting lookup missed")
		}
		it := segment.Iterator()
		for range len(entries) {
			if _, ok := it.Next(); !ok {
				panic("posting iterator ended early")
			}
		}
	}); allocs != 0 {
		t.Fatalf("posting-page codec, lookup, and iteration allocations = %g, want 0", allocs)
	}
}
