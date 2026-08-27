package storeio

import (
	"errors"
	"testing"
)

func TestGenerationMigrationStagingChainCanonicalAndAuthenticated(t *testing.T) {
	const pageSize = uint32(4096)
	migrationID := [16]byte{9}
	extents := []GenerationMigrationStagingExtent{
		{Offset: 64 << 10, Length: 1 << 20, FirstLogicalID: 100, LogicalIDCount: 257, DataBytes: 512 << 10},
		{Offset: (64 << 10) + (1 << 20), Length: 2 << 20, FirstLogicalID: 357, LogicalIDCount: 513, DataBytes: 1 << 20},
	}
	ref := PageRef{Offset: extents[0].Offset, LogicalID: 100, Generation: 8, Length: pageSize, Kind: PageMigrationStagingChain}
	page, err := EncodeGenerationMigrationStagingChainPage(
		make([]byte, pageSize), testStoreID, migrationID, 8, ref.LogicalID, 1,
		PageRef{}, 2, 3<<20, (512<<10)+(1<<20)+2*uint64(pageSize), extents,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenGenerationMigrationStagingChainPage(page, ref, testStoreID, migrationID, 8)
	if err != nil || view.Sequence() != 1 || view.Previous() != (PageRef{}) ||
		view.CumulativeExtentCount() != 2 || view.CumulativeAllocatedBytes() != 3<<20 {
		t.Fatalf("view = %+v err=%v", view, err)
	}
	it := view.Iterator()
	for index := range extents {
		got, ok := it.Next()
		if !ok || got != extents[index] {
			t.Fatalf("extent %d = %+v,%v want %+v", index, got, ok, extents[index])
		}
	}
	if _, ok := it.Next(); ok {
		t.Fatal("iterator accepted trailing extent")
	}
	corrupt := append([]byte(nil), page...)
	corrupt[PageHeaderSize+16] ^= 1
	if _, err := OpenGenerationMigrationStagingChainPage(corrupt, ref, testStoreID, migrationID, 8); !errors.Is(err, ErrGenerationMigrationManifestCorrupt) {
		t.Fatalf("corrupt page error = %v", err)
	}
}

func TestGenerationMigrationStagingChainRejectsNoncanonicalExtents(t *testing.T) {
	bad := []GenerationMigrationStagingExtent{
		{Offset: 128 << 10, Length: 4096, FirstLogicalID: 4, LogicalIDCount: 1},
		{Offset: 64 << 10, Length: 4096, FirstLogicalID: 5, LogicalIDCount: 1},
	}
	if _, err := EncodeGenerationMigrationStagingChainPage(
		make([]byte, 4096), testStoreID, [16]byte{4}, 3, 4, 1,
		PageRef{}, 2, 8192, 8192, bad,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("reordered extent error = %v", err)
	}
}
