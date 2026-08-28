package storeio

import (
	"errors"
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
		SourcePrimaryRoot:   PageRef{Offset: 128 << 10, LogicalID: GlobalTabletCatalogRootLogicalID, Generation: 7, Length: GlobalTabletCatalogRootBytes, Kind: PagePrimaryCatalog},
		TargetPrimaryRoot:   PageRef{Offset: 2 << 20, LogicalID: GlobalTabletCatalogRootLogicalID, Generation: 8, Length: GlobalTabletCatalogRootBytes, Kind: PagePrimaryCatalog},
		SourceCatalogHead:   PageRef{Offset: 64 << 10, LogicalID: 4, Generation: 7, Length: 4096, Kind: PageCatalogSegment},
		SourceCatalogBytes:  PageCatalogCanonicalHeaderSize,
		TargetScratchOffset: 3 << 20, TargetScratchBytes: 4096,
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
	if err != nil || done {
		t.Fatalf("step done=%v err=%v", done, err)
	}
	done, err = driver.Step()
	if err != nil || !done {
		t.Fatalf("scratch step done=%v err=%v", done, err)
	}
	if len(retired) != 2 || retired[0].Offset != m.SourceCatalogHead.Offset || retired[0].Length != 4096 || retired[1].Offset != m.TargetScratchOffset || retired[1].Length != m.TargetScratchBytes || retired[1].RetiredGeneration != m.TargetGeneration {
		t.Fatalf("retired = %+v", retired)
	}
	loaded, err := manifestStore.Load()
	if err != nil || loaded.RetirementPhase != GenerationMigrationRetireDone || loaded.RetirementOrdinal != 0 {
		t.Fatalf("loaded = %+v err=%v", loaded, err)
	}
}

func TestGenerationMigrationRetirementDriverCrashResumeIsBoundedAndComplete(t *testing.T) {
	image, records := buildPrimaryGraphTestImage(t, 1000)
	plan, err := PlanPrimaryGraph(image.root.StoreID, records, false)
	if err != nil {
		t.Fatal(err)
	}
	manifestFile, err := os.CreateTemp(t.TempDir(), "retirement-resume-*")
	if err != nil {
		t.Fatal(err)
	}
	defer manifestFile.Close()
	manifestStore, err := OpenGenerationMigrationManifestStore(manifestFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	m := GenerationMigrationManifest{
		StoreID: image.root.StoreID, MigrationID: [16]byte{7}, Phase: GenerationMigrationPublished,
		RetirementPhase:  GenerationMigrationRetirePrimary,
		SourceGeneration: 1, TargetGeneration: 2,
		SourceFileEnd: image.bounds.FileEnd, TargetFileEnd: image.bounds.FileEnd + 8<<20,
		ReservedOffset: image.bounds.FileEnd + 8<<20, ReservedBytes: 8 << 20,
		FirstLogicalID: image.root.NextLogicalID, LogicalIDCount: 1000,
		SourcePrimaryRoot: image.root.PrimaryRoot,
		TargetPrimaryRoot: PageRef{Offset: image.bounds.FileEnd + 8<<20, LogicalID: GlobalTabletCatalogRootLogicalID, Generation: 2, Length: GlobalTabletCatalogRootBytes, Kind: PagePrimaryCatalog},
	}
	if err := manifestStore.Create(m); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(image.file, PageCacheOptions{PageSize: int(format0PageSize), MaxPageSize: GlobalTabletCatalogRootBytes, ResidentBytes: 4 << 20, StoreID: image.root.StoreID, ReadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	crash := errors.New("simulated crash after durable retire")
	crashOnce := true
	seen := make(map[FreeExtent]struct{}, plan.PageCount())
	driver := GenerationMigrationRetirementDriver{Manifest: manifestStore, Cache: cache, PageSize: format0PageSize, MaxPageSize: GlobalTabletCatalogRootBytes, BatchExtents: 3, RetireDurably: func(extents []FreeExtent) error {
		if len(extents) > 3 {
			t.Fatalf("retirement batch=%d, want <=3", len(extents))
		}
		for _, extent := range extents {
			seen[extent] = struct{}{}
		}
		if crashOnce {
			crashOnce = false
			return crash
		}
		return nil
	}}
	if done, err := driver.Step(); done || !errors.Is(err, crash) {
		t.Fatalf("crash cut done=%v err=%v", done, err)
	}
	afterCrash, err := manifestStore.Load()
	if err != nil || afterCrash.RetirementOrdinal != 0 {
		t.Fatalf("cursor advanced across crash: %+v err=%v", afterCrash, err)
	}
	for steps := 0; ; steps++ {
		if steps > plan.PageCount()+8 {
			t.Fatal("retirement failed to converge")
		}
		done, err := driver.Step()
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if len(seen) != plan.PageCount() {
		t.Fatalf("unique retired extents=%d want=%d", len(seen), plan.PageCount())
	}
}
