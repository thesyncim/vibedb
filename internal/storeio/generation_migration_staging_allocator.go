package storeio

import (
	"fmt"
	"os"
)

// GenerationMigrationExtentReserveFunc publishes one exact collision-proof
// physical/logical high-water reservation and waits for it to become durable.
// Its caller holds the Store writer exclusion from before the manifest intent
// is synced until the returned chain link is synced and installed.
type GenerationMigrationExtentReserveFunc func(
	bytes, logicalIDs uint64,
) (reservation UnrootedGenerationReservation, publishedFileEnd uint64, err error)

// AppendGenerationMigrationStagingExtent performs the four crash-ordered cuts
// for one incremental same-file reservation: durable intent, durable allocator
// high waters, authenticated chain page, then durable tail installation. The
// returned reservation excludes the chain page and is ready for staged data.
func AppendGenerationMigrationStagingExtent(
	file *os.File,
	manifestStore *GenerationMigrationManifestStore,
	expectedFileEnd, expectedNextLogicalID uint64,
	pageSize uint32,
	dataBytes, dataLogicalIDs uint64,
	reserve GenerationMigrationExtentReserveFunc,
) (UnrootedGenerationReservation, GenerationMigrationManifest, error) {
	var data UnrootedGenerationReservation
	if file == nil || manifestStore == nil || reserve == nil ||
		!validPhysicalPageSize(pageSize) || dataBytes == 0 ||
		dataBytes%uint64(pageSize) != 0 || dataLogicalIDs == 0 ||
		expectedFileEnd == 0 || expectedFileEnd%uint64(pageSize) != 0 ||
		expectedNextLogicalID == 0 || dataBytes > ^uint64(0)-uint64(pageSize) ||
		dataLogicalIDs == ^uint64(0) {
		return data, GenerationMigrationManifest{}, fmt.Errorf("%w: migration staging append", ErrInvalidWrite)
	}
	manifest, err := manifestStore.Load()
	if err != nil || manifest.Phase == GenerationMigrationPublished || manifest.PendingExtentBytes != 0 {
		if err != nil {
			return data, GenerationMigrationManifest{}, err
		}
		return data, GenerationMigrationManifest{}, fmt.Errorf("%w: migration staging state", ErrInvalidWrite)
	}
	totalBytes := uint64(pageSize) + dataBytes
	totalLogicalIDs := dataLogicalIDs + 1
	pending := manifest
	pending.PendingExtentOffset = expectedFileEnd
	pending.PendingExtentBytes = totalBytes
	pending.PendingFirstLogicalID = expectedNextLogicalID
	pending.PendingLogicalIDCount = totalLogicalIDs
	if err := manifestStore.Advance(pending); err != nil {
		return data, GenerationMigrationManifest{}, err
	}
	pending, err = manifestStore.Load()
	if err != nil {
		return data, GenerationMigrationManifest{}, err
	}
	reservation, publishedFileEnd, err := reserve(totalBytes, totalLogicalIDs)
	if err != nil {
		return data, pending, err
	}
	if reservation.Offset != expectedFileEnd || reservation.Length != totalBytes ||
		reservation.FirstLogicalID != expectedNextLogicalID ||
		reservation.LogicalIDCount != totalLogicalIDs ||
		publishedFileEnd != expectedFileEnd+totalBytes {
		return data, pending, fmt.Errorf("%w: migration reservation witness", ErrInvalidWrite)
	}
	extent := GenerationMigrationStagingExtent{
		Offset: reservation.Offset, Length: reservation.Length,
		FirstLogicalID: reservation.FirstLogicalID,
		LogicalIDCount: reservation.LogicalIDCount, DataBytes: dataBytes,
	}
	chainRef := PageRef{
		Offset: reservation.Offset, LogicalID: reservation.FirstLogicalID,
		Generation: manifest.TargetGeneration, Length: pageSize,
		Kind: PageMigrationStagingChain,
	}
	image := make([]byte, pageSize)
	_, err = EncodeGenerationMigrationStagingChainPage(
		image, manifest.StoreID, manifest.MigrationID, manifest.TargetGeneration,
		chainRef.LogicalID, manifest.StagingChainSequence+1,
		manifest.StagingChainTail, manifest.StagingExtentCount+1,
		manifest.StagingAllocatedBytes+totalBytes,
		manifest.StagingUsedBytes+totalBytes,
		[]GenerationMigrationStagingExtent{extent},
	)
	if err != nil {
		return data, pending, err
	}
	if _, err := file.WriteAt(image, int64(chainRef.Offset)); err != nil {
		return data, pending, err
	}
	if err := file.Sync(); err != nil {
		return data, pending, err
	}
	linked := pending
	linked.PendingExtentOffset, linked.PendingExtentBytes = 0, 0
	linked.PendingFirstLogicalID, linked.PendingLogicalIDCount = 0, 0
	linked.StagingChainTail = chainRef
	linked.StagingChainSequence++
	linked.StagingExtentCount++
	linked.StagingAllocatedBytes += totalBytes
	linked.StagingUsedBytes += totalBytes
	linked.TargetFileEnd = publishedFileEnd
	if err := manifestStore.Advance(linked); err != nil {
		return data, pending, err
	}
	linked, err = manifestStore.Load()
	if err != nil {
		return data, GenerationMigrationManifest{}, err
	}
	data = UnrootedGenerationReservation{
		Offset: reservation.Offset + uint64(pageSize), Length: dataBytes,
		FirstLogicalID: reservation.FirstLogicalID + 1,
		LogicalIDCount: dataLogicalIDs,
	}
	return data, linked, nil
}

type GenerationMigrationPendingExtentState uint8

const (
	GenerationMigrationPendingIntent GenerationMigrationPendingExtentState = iota + 1
	GenerationMigrationPendingPublished
)

// InspectGenerationMigrationPendingExtent distinguishes a synced intent from
// a published-but-unlinked reservation using only authenticated manifest and
// state-root high waters. Recovery never guesses from trailing file bytes.
func InspectGenerationMigrationPendingExtent(
	manifest GenerationMigrationManifest, fileEnd, nextLogicalID uint64,
) (UnrootedGenerationReservation, GenerationMigrationPendingExtentState, error) {
	var reservation UnrootedGenerationReservation
	if manifest.PendingExtentBytes == 0 || !validGenerationMigrationStagingState(manifest) {
		return reservation, 0, fmt.Errorf("%w: no pending migration extent", ErrInvalidWrite)
	}
	reservation = UnrootedGenerationReservation{
		Offset: manifest.PendingExtentOffset, Length: manifest.PendingExtentBytes,
		FirstLogicalID: manifest.PendingFirstLogicalID,
		LogicalIDCount: manifest.PendingLogicalIDCount,
	}
	if fileEnd == reservation.Offset && nextLogicalID == reservation.FirstLogicalID {
		return reservation, GenerationMigrationPendingIntent, nil
	}
	if fileEnd == reservation.Offset+reservation.Length &&
		nextLogicalID == reservation.FirstLogicalID+reservation.LogicalIDCount {
		return reservation, GenerationMigrationPendingPublished, nil
	}
	return UnrootedGenerationReservation{}, 0,
		fmt.Errorf("%w: ambiguous migration extent high waters", ErrGenerationMigrationManifestCorrupt)
}
