package storeio

import (
	"os"
	"testing"
)

func TestGenerationMigrationRetirementDriverPersistsCatalogCursor(t *testing.T) {
	manifestFile, err := os.CreateTemp(t.TempDir(), "retirement-manifest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer manifestFile.Close()
	manifestStore, err := OpenGenerationMigrationManifestStore(manifestFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	m := GenerationMigrationManifest{
		StoreID: testStoreID, MigrationID: [16]byte{8}, Phase: GenerationMigrationPublished,
		RetirementPhase:  GenerationMigrationRetireCatalog,
		SourceGeneration: 7, TargetGeneration: 8, AppliedSequence: 3, CapturedSequence: 3,
		SourceFileEnd: 1 << 20, TargetFileEnd: 2 << 20,
		ReservedOffset: 2 << 20, ReservedBytes: 4 << 20, FirstLogicalID: 100, LogicalIDCount: 100,
		SourcePrimaryRoot:  PageRef{Offset: 128 << 10, LogicalID: GlobalTabletCatalogRootLogicalID, Generation: 7, Length: GlobalTabletCatalogRootBytes, Kind: PagePrimaryCatalog},
		TargetPrimaryRoot:  PageRef{Offset: 2 << 20, LogicalID: GlobalTabletCatalogRootLogicalID, Generation: 8, Length: GlobalTabletCatalogRootBytes, Kind: PagePrimaryCatalog},
		SourceCatalogHead:  PageRef{Offset: 64 << 10, LogicalID: 4, Generation: 7, Length: 4096, Kind: PageCatalogSegment},
		SourceCatalogBytes: PageCatalogCanonicalHeaderSize,
	}
	if err := manifestStore.Create(m); err != nil {
		t.Fatal(err)
	}
	dataFile, err := os.CreateTemp(t.TempDir(), "retirement-data-*")
	if err != nil {
		t.Fatal(err)
	}
	defer dataFile.Close()
	cache, err := NewPageCache(dataFile, PageCacheOptions{PageSize: 4096, ResidentBytes: 8192, StoreID: testStoreID, ReadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	var retired []FreeExtent
	driver := GenerationMigrationRetirementDriver{Manifest: manifestStore, Cache: cache, PageSize: 4096, MaxPageSize: 4096, BatchExtents: 4, RetireDurably: func(extents []FreeExtent) error {
		retired = append(retired, extents...)
		return manifestFile.Sync()
	}}
	done, err := driver.Step()
	if err != nil || !done {
		t.Fatalf("step done=%v err=%v", done, err)
	}
	if len(retired) != 1 || retired[0].Offset != m.SourceCatalogHead.Offset || retired[0].Length != 4096 {
		t.Fatalf("retired = %+v", retired)
	}
	loaded, err := manifestStore.Load()
	if err != nil || loaded.RetirementPhase != GenerationMigrationRetireDone || loaded.RetirementOrdinal != 0 {
		t.Fatalf("loaded = %+v err=%v", loaded, err)
	}
}
