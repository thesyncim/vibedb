package storeio

import (
	"errors"
	"testing"
)

func TestMutableStoreLayoutGeometry(t *testing.T) {
	tests := []struct {
		pageSize  uint32
		roots     [2]uint64
		journals  [2]uint64
		manifest  uint64
		dataStart uint64
	}{
		{
			pageSize: 4096, roots: [2]uint64{0, 4096},
			journals: [2]uint64{8192, 12288}, manifest: 16384, dataStart: 24576,
		},
		{
			pageSize: 8192, roots: [2]uint64{0, 8192},
			journals: [2]uint64{16384, 20480}, manifest: 24576, dataStart: 32768,
		},
		{
			pageSize: 65536, roots: [2]uint64{0, 65536},
			journals: [2]uint64{131072, 135168}, manifest: 139264, dataStart: 196608,
		},
	}
	for _, tc := range tests {
		layout, err := MutableStoreLayout(tc.pageSize)
		if err != nil {
			t.Fatalf("MutableStoreLayout(%d): %v", tc.pageSize, err)
		}
		if layout.RootOffsets != tc.roots ||
			layout.MaterializationJournalOffsets != tc.journals ||
			layout.GenerationMigrationManifestOffset != tc.manifest ||
			layout.DataStart != tc.dataStart {
			t.Fatalf(
				"MutableStoreLayout(%d) = %#v, want roots=%v journals=%v data=%d",
				tc.pageSize, layout, tc.roots, tc.journals, tc.dataStart,
			)
		}
		if layout.DataStart%uint64(tc.pageSize) != 0 {
			t.Fatalf("MutableStoreLayout(%d) data start is not page aligned", tc.pageSize)
		}
		for _, offset := range layout.MaterializationJournalOffsets {
			if offset%MaterializationJournalSize != 0 ||
				offset+MaterializationJournalSize > layout.DataStart {
				t.Fatalf(
					"MutableStoreLayout(%d) journal offset %d escapes reserved prefix",
					tc.pageSize, offset,
				)
			}
		}
		manifestEnd := layout.GenerationMigrationManifestOffset +
			generationMigrationManifestCopies*GenerationMigrationManifestBytes
		if layout.GenerationMigrationManifestOffset%GenerationMigrationManifestBytes != 0 ||
			manifestEnd > layout.DataStart {
			t.Fatalf("MutableStoreLayout(%d) migration manifest escapes reserved prefix", tc.pageSize)
		}
	}
}

func TestMutableStoreLayoutRejectsInvalidPageSize(t *testing.T) {
	for _, pageSize := range []uint32{0, 2048, 6144} {
		if _, err := MutableStoreLayout(pageSize); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("MutableStoreLayout(%d) error = %v, want %v", pageSize, err, ErrInvalidWrite)
		}
	}
}

func TestMutableStoreLayoutReservedPrefixIsNeverAllocatable(t *testing.T) {
	storeID := [16]byte{1}
	for _, pageSize := range []uint32{4096, 8192, 65536} {
		layout, err := MutableStoreLayout(pageSize)
		if err != nil {
			t.Fatalf("MutableStoreLayout(%d): %v", pageSize, err)
		}
		fileEnd := layout.DataStart + uint64(pageSize)
		reservedOffset := layout.MaterializationJournalOffsets[0]
		ref := PageRef{
			Offset: reservedOffset, LogicalID: 2, Generation: 1,
			Length: pageSize, Kind: PageOverflow,
		}
		root := StateRoot{
			StoreID: storeID, Generation: 1, PageSize: pageSize,
			MaxPageSize: pageSize, NextLogicalID: 3,
		}
		if err := validateStatePageRef(
			ref, PageOverflow, true, root, fileEnd,
		); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf(
				"page size %d reserved PageRef error = %v, want %v",
				pageSize, err, ErrInvalidWrite,
			)
		}
		if err := validateFreeExtent(FreeExtent{
			Offset: reservedOffset, Length: uint64(pageSize), RetiredGeneration: 1,
		}, pageSize, fileEnd); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf(
				"page size %d reserved free extent error = %v, want %v",
				pageSize, err, ErrInvalidWrite,
			)
		}
		targetRef := ref
		targetRef.Kind = PageOverflow
		if err := validateMaterializationTargetRef(MaterializationJournalHeader{
			StoreID: storeID, Sequence: 1, TargetGeneration: 2,
			PageSize: pageSize, SectorSize: MaterializationJournalMinSectorSize,
		}, targetRef); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf(
				"page size %d reserved journal target error = %v, want %v",
				pageSize, err, ErrInvalidWrite,
			)
		}
		tx := WriteTransaction{
			options: WriteTransactionOptions{
				Generation: 2, PageSize: pageSize, FileEnd: fileEnd,
			},
			reuseEdits: make([]ReuseEdit, 0, 1),
		}
		if _, _, err := tx.allocateFromReusable(0, FreeExtent{
			Offset: reservedOffset, Length: uint64(pageSize), RetiredGeneration: 1,
		}, uint64(pageSize)); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf(
				"page size %d reserved reusable extent error = %v, want %v",
				pageSize, err, ErrInvalidWrite,
			)
		}
	}
}
