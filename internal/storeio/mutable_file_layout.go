package storeio

import "fmt"

const (
	materializationJournalCopies      = 2
	generationMigrationManifestCopies = 2
)

// MutableStoreFileLayout is the allocator-excluded prefix of a mutable Store
// page file. The two root copies each own a complete Store page. The two
// materialization undo capsules and generation-migration manifests follow them
// as packed 4-KiB slots, and ordinary allocations start at the next Store-page
// boundary.
//
// Keeping the capsule slots outside the allocator is what lets recovery find
// them before decoding any mutable state. Packing both slots into one page when
// PageSize is larger than 4 KiB avoids paying two full Store pages for fixed
// 4-KiB recovery records.
type MutableStoreFileLayout struct {
	RootOffsets                       [superblockCopies]uint64
	MaterializationJournalOffsets     [materializationJournalCopies]uint64
	GenerationMigrationManifestOffset uint64
	DataStart                         uint64
}

// MutableStoreLayout returns the authoritative fixed layout for a mutable
// Store page file. No PageRef or free extent may start below DataStart.
func MutableStoreLayout(pageSize uint32) (MutableStoreFileLayout, error) {
	if !validPhysicalPageSize(pageSize) {
		return MutableStoreFileLayout{}, fmt.Errorf(
			"%w: mutable Store page size=%d", ErrInvalidWrite, pageSize,
		)
	}
	quantum := uint64(pageSize)
	journalStart := uint64(superblockCopies) * quantum
	journalEnd := journalStart +
		uint64(materializationJournalCopies*MaterializationJournalSize)
	manifestStart := journalEnd
	manifestEnd := manifestStart +
		uint64(generationMigrationManifestCopies*GenerationMigrationManifestBytes)
	dataStart := (manifestEnd + quantum - 1) &^ (quantum - 1)
	return MutableStoreFileLayout{
		RootOffsets: [superblockCopies]uint64{0, quantum},
		MaterializationJournalOffsets: [materializationJournalCopies]uint64{
			journalStart,
			journalStart + MaterializationJournalSize,
		},
		GenerationMigrationManifestOffset: manifestStart,
		DataStart:                         dataStart,
	}, nil
}
