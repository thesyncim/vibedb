package storeio

import "testing"

func TestCommitterSteadyAllocation(t *testing.T) {
	committer, _, pageSize := newPortableCommitter(t, 4, 1)
	defer committer.Close()
	var generation uint64
	storeID := [16]byte{1, 3, 5, 7, 9, 11, 13, 15, 2, 4, 6, 8, 10, 12, 14, 16}
	if allocs := testing.AllocsPerRun(20, func() {
		generation++
		batch, err := committer.Begin(1)
		if err != nil {
			panic(err)
		}
		page, err := batch.PageBuffer(0)
		if err != nil {
			panic(err)
		}
		clear(page)
		copy(page, "page")
		pageOffset := testMutableStoreDataStart(uint32(pageSize)) +
			uint64(pageSize)*(generation-1)
		if err := batch.SetPage(0, int64(pageOffset), pageSize); err != nil {
			panic(err)
		}
		root := testInlineSuperblock(generation)
		root.StoreID = storeID
		root.State.StoreID = storeID
		root.PageSize = uint32(pageSize)
		root.State.PageSize = uint32(pageSize)
		root.FileEnd = pageOffset + uint64(pageSize)
		if err := batch.SetInlineSuperblock(root); err != nil {
			panic(err)
		}
		if err := batch.Publish(generation); err != nil {
			panic(err)
		}
		if err := committer.Wait(generation); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("Begin/fill/Publish/Wait allocations = %g, want 0", allocs)
	}
}
