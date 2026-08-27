package storeio

import (
	"errors"
	"os"
	"testing"
)

func TestGenerationMigrationManifestStoreFallsBackAcrossTornAdvance(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "migration-manifest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	store, err := OpenGenerationMigrationManifestStore(file, 0)
	if err != nil {
		t.Fatal(err)
	}
	m := GenerationMigrationManifest{
		StoreID: testStoreID, MigrationID: [16]byte{2}, Phase: GenerationMigrationCopying,
		SourceGeneration: 3, TargetGeneration: 4, ReservedOffset: 1 << 20, ReservedBytes: 8 << 20,
		FirstLogicalID: 10, LogicalIDCount: 100, SourceFileEnd: 1 << 20,
		SourcePrimaryRoot: PageRef{Offset: 64 << 10, LogicalID: GlobalTabletCatalogRootLogicalID, Generation: 3, Length: GlobalTabletCatalogRootBytes, Kind: PagePrimaryCatalog},
		Cursor:            []byte("a"),
	}
	if err := store.Create(m); err != nil {
		t.Fatal(err)
	}
	first, err := store.Load()
	if err != nil || first.ManifestSequence != 1 {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	next := first
	next.Cursor = []byte("b")
	next.TargetFileEnd = 4096
	if err := store.Advance(next); err != nil {
		t.Fatal(err)
	}
	latest, err := store.Load()
	if err != nil || latest.ManifestSequence != 2 || string(latest.Cursor) != "b" {
		t.Fatalf("latest = %+v err=%v", latest, err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	fallback, err := store.Load()
	if err != nil || fallback.ManifestSequence != 1 || string(fallback.Cursor) != "a" {
		t.Fatalf("fallback = %+v err=%v", fallback, err)
	}
	if err := file.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrGenerationMigrationManifestNotFound) {
		t.Fatalf("empty load error = %v", err)
	}
}
