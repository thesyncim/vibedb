package storeio

import (
	"errors"
	"os"
	"testing"
)

func TestGenerationMigrationStagingAllocatorCrashOrderedChain(t *testing.T) {
	const pageSize = uint32(4096)
	file, err := os.CreateTemp(t.TempDir(), "migration-staging-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	manifestOffset := layout.DataStart
	initialFileEnd := manifestOffset + 2*GenerationMigrationManifestBytes
	if err := file.Truncate(int64(initialFileEnd)); err != nil {
		t.Fatal(err)
	}
	manifestStore, err := OpenGenerationMigrationManifestStore(
		file, int64(manifestOffset),
	)
	if err != nil {
		t.Fatal(err)
	}
	manifest := GenerationMigrationManifest{
		StoreID: testStoreID, MigrationID: [16]byte{7},
		Phase: GenerationMigrationCopying, SourceGeneration: 4, TargetGeneration: 5,
		SourceFileEnd:     initialFileEnd,
		SourcePrimaryRoot: PageRef{Offset: 64 << 10, LogicalID: GlobalTabletCatalogRootLogicalID, Generation: 4, Length: pageSize, Kind: PagePrimaryCatalog},
	}
	if err := manifestStore.Create(manifest); err != nil {
		t.Fatal(err)
	}
	const dataBytes = uint64(256 << 10)
	const dataLogicalIDs = uint64(64)
	data, linked, err := AppendGenerationMigrationStagingExtent(
		file, manifestStore, initialFileEnd, 1000, pageSize,
		dataBytes, dataLogicalIDs,
		func(bytes, logicalIDs uint64) (UnrootedGenerationReservation, uint64, error) {
			reservation := UnrootedGenerationReservation{
				Offset: initialFileEnd, Length: bytes,
				FirstLogicalID: 1000, LogicalIDCount: logicalIDs,
			}
			if err := file.Truncate(int64(reservation.Offset + reservation.Length)); err != nil {
				return UnrootedGenerationReservation{}, 0, err
			}
			if err := file.Sync(); err != nil {
				return UnrootedGenerationReservation{}, 0, err
			}
			return reservation, reservation.Offset + reservation.Length, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if data.Offset != initialFileEnd+uint64(pageSize) || data.Length != dataBytes ||
		data.FirstLogicalID != 1001 || data.LogicalIDCount != dataLogicalIDs ||
		linked.PendingExtentBytes != 0 || linked.StagingExtentCount != 1 ||
		linked.StagingAllocatedBytes != dataBytes+uint64(pageSize) ||
		linked.TargetFileEnd != initialFileEnd+dataBytes+uint64(pageSize) {
		t.Fatalf("data=%+v linked=%+v", data, linked)
	}
	page := make([]byte, pageSize)
	if _, err := file.ReadAt(page, int64(linked.StagingChainTail.Offset)); err != nil {
		t.Fatal(err)
	}
	view, err := OpenGenerationMigrationStagingChainPage(
		page, linked.StagingChainTail, testStoreID, manifest.MigrationID, manifest.TargetGeneration,
	)
	if err != nil || view.Sequence() != 1 || view.CumulativeExtentCount() != 1 {
		t.Fatalf("chain = %+v err=%v", view, err)
	}
}

func TestInspectGenerationMigrationPendingExtentRejectsAmbiguousCuts(t *testing.T) {
	m := GenerationMigrationManifest{
		StoreID: testStoreID, MigrationID: [16]byte{4},
		Phase: GenerationMigrationCopying, SourceGeneration: 2, TargetGeneration: 3,
		SourcePrimaryRoot:   PageRef{Offset: 64 << 10, LogicalID: GlobalTabletCatalogRootLogicalID, Generation: 2, Length: 4096, Kind: PagePrimaryCatalog},
		PendingExtentOffset: 1 << 20, PendingExtentBytes: 64 << 10,
		PendingFirstLogicalID: 100, PendingLogicalIDCount: 16,
	}
	reservation, state, err := InspectGenerationMigrationPendingExtent(m, 1<<20, 100)
	if err != nil || state != GenerationMigrationPendingIntent || reservation.Length != 64<<10 {
		t.Fatalf("intent = %+v,%d,%v", reservation, state, err)
	}
	reservation, state, err = InspectGenerationMigrationPendingExtent(m, (1<<20)+(64<<10), 116)
	if err != nil || state != GenerationMigrationPendingPublished {
		t.Fatalf("published = %+v,%d,%v", reservation, state, err)
	}
	if _, _, err := InspectGenerationMigrationPendingExtent(m, (1<<20)+(32<<10), 108); !errors.Is(err, ErrGenerationMigrationManifestCorrupt) {
		t.Fatalf("ambiguous error = %v", err)
	}
}
