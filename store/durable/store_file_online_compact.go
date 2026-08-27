package durable

import (
	"crypto/rand"

	"github.com/thesyncim/vibedb/internal/storeio"
)

const onlineCompactionStagingChunkBytes = 4 << 20

func emptyMigrationPublicationDescriptor() ([]byte, error) {
	return storeio.EncodePublicationDescriptor(make([]byte, 4096), nil)
}

// beginOnlineGenerationMigrationLocked installs a crash-discoverable locator
// for the two authenticated manifest slots and creates the initial source cut.
// The caller owns writer exclusively.
func (c *Collection) beginOnlineGenerationMigrationLocked() (*storeio.GenerationMigrationManifestStore, storeio.GenerationMigrationManifest, error) {
	if c == nil || c.closed || c.file == nil {
		return nil, storeio.GenerationMigrationManifest{}, ErrClosed
	}
	if err := c.rejectCheckpointGroupOwner(); err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	if err := c.flushPendingForStructural(); err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return nil, storeio.GenerationMigrationManifest{}, storeio.ErrInvalidWrite
	}
	if state.root.MigrationManifestOffset != 0 {
		manifestStore, err := storeio.OpenGenerationMigrationManifestStore(
			c.file, int64(state.root.MigrationManifestOffset),
		)
		if err != nil {
			return nil, storeio.GenerationMigrationManifest{}, err
		}
		manifest, err := manifestStore.Load()
		return manifestStore, manifest, err
	}
	generation := state.root.Generation + 1
	if generation == 0 || generation >= uint64(1)<<48 {
		return nil, storeio.GenerationMigrationManifest{}, storeio.ErrGenerationOrder
	}
	tx, err := c.beginWriteTransaction(1, storeio.WriteTransactionOptions{
		StoreID: c.storeID, Generation: generation,
		PageSize: uint32(c.options.PageSize), FileEnd: state.fileEnd,
		NextLogicalID: state.root.NextLogicalID,
	})
	if err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	abort := true
	defer func() {
		if abort {
			_ = tx.Abort()
		}
	}()
	pageSize := uint64(c.options.PageSize)
	manifestBytes := uint64(2 * storeio.GenerationMigrationManifestBytes)
	manifestBytes = (manifestBytes + pageSize - 1) &^ (pageSize - 1)
	reservation, err := tx.ReserveUnrootedGeneration(manifestBytes, 1)
	if err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	descriptor, err := emptyMigrationPublicationDescriptor()
	if err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	if err := tx.SetPublicationDescriptor(descriptor); err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	nextState := &fileStoreState{root: state.root, fileEnd: tx.FileEnd(), freeHead: state.freeHead}
	nextState.root.Generation = generation
	nextState.root.NextLogicalID = tx.NextLogicalID()
	nextState.root.MigrationManifestOffset = reservation.Offset
	c.snapshotGate.Lock()
	c.beginReaderFence()
	if err := tx.PublishInline(nextState.root, c.inlineFree); err != nil {
		c.endReaderFence()
		c.snapshotGate.Unlock()
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	abort = false
	c.primaryRouter.Load().AdvanceGeneration(generation)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.endReaderFence()
	c.snapshotGate.Unlock()
	if c.deferredCanonicalLane() {
		err = c.flushPublishedPhysicalLocked()
	} else {
		err = c.waitPublished(generation)
	}
	if err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	manifestStore, err := storeio.OpenGenerationMigrationManifestStore(c.file, int64(reservation.Offset))
	if err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	var migrationID [16]byte
	for migrationID == ([16]byte{}) {
		if _, err := rand.Read(migrationID[:]); err != nil {
			return nil, storeio.GenerationMigrationManifest{}, err
		}
	}
	manifest := storeio.GenerationMigrationManifest{
		StoreID: c.storeID, MigrationID: migrationID,
		Phase:                storeio.GenerationMigrationCopying,
		SourceGeneration:     nextState.root.Generation,
		TargetGeneration:     nextState.root.Generation + 1,
		SourceFileEnd:        nextState.fileEnd,
		SourcePrimaryRoot:    nextState.root.PrimaryRoot,
		SourceExactIndexRoot: nextState.root.ExactIndexRoot,
		SourceCatalogHead:    nextState.root.PageCatalogHead,
		SourceCatalogBytes:   nextState.root.PageCatalogBytes,
		SourceIndexCount:     nextState.root.IndexCount,
	}
	if err := manifestStore.Create(manifest); err != nil {
		return nil, storeio.GenerationMigrationManifest{}, err
	}
	manifest, err = manifestStore.Load()
	return manifestStore, manifest, err
}

// publishOnlineMigrationReservationLocked advances only allocator high waters.
// The manifest intent is already durable and writer exclusion prevents an
// unrelated publication from entering between that intent and this root.
func (c *Collection) publishOnlineMigrationReservationLocked(
	bytes, logicalIDs uint64,
) (storeio.UnrootedGenerationReservation, uint64, error) {
	var reservation storeio.UnrootedGenerationReservation
	state := c.state.Load()
	if state == nil || state.root.MigrationManifestOffset == 0 {
		return reservation, 0, storeio.ErrInvalidWrite
	}
	generation := state.root.Generation + 1
	if generation == 0 || generation >= uint64(1)<<48 {
		return reservation, 0, storeio.ErrGenerationOrder
	}
	tx, err := c.beginWriteTransaction(1, storeio.WriteTransactionOptions{
		StoreID: c.storeID, Generation: generation,
		PageSize: uint32(c.options.PageSize), FileEnd: state.fileEnd,
		NextLogicalID: state.root.NextLogicalID,
	})
	if err != nil {
		return reservation, 0, err
	}
	abort := true
	defer func() {
		if abort {
			_ = tx.Abort()
		}
	}()
	reservation, err = tx.ReserveUnrootedGeneration(bytes, logicalIDs)
	if err != nil {
		return reservation, 0, err
	}
	descriptor, err := emptyMigrationPublicationDescriptor()
	if err != nil {
		return reservation, 0, err
	}
	if err := tx.SetPublicationDescriptor(descriptor); err != nil {
		return reservation, 0, err
	}
	nextState := &fileStoreState{root: state.root, fileEnd: tx.FileEnd(), freeHead: state.freeHead}
	nextState.root.Generation = generation
	nextState.root.NextLogicalID = tx.NextLogicalID()
	c.snapshotGate.Lock()
	c.beginReaderFence()
	if err := tx.PublishInline(nextState.root, c.inlineFree); err != nil {
		c.endReaderFence()
		c.snapshotGate.Unlock()
		return reservation, 0, err
	}
	abort = false
	c.primaryRouter.Load().AdvanceGeneration(generation)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.endReaderFence()
	c.snapshotGate.Unlock()
	if c.deferredCanonicalLane() {
		err = c.flushPublishedPhysicalLocked()
	} else {
		err = c.waitPublished(generation)
	}
	return reservation, nextState.fileEnd, err
}

func (c *Collection) growOnlineMigrationStaging(
	manifestStore *storeio.GenerationMigrationManifestStore,
	minimumDataBytes, minimumLogicalIDs uint64,
) (storeio.UnrootedGenerationReservation, storeio.GenerationMigrationManifest, error) {
	var reservation storeio.UnrootedGenerationReservation
	c.writer.Lock()
	defer c.writer.Unlock()
	if c.closed {
		return reservation, storeio.GenerationMigrationManifest{}, ErrClosed
	}
	state := c.state.Load()
	if state == nil {
		return reservation, storeio.GenerationMigrationManifest{}, storeio.ErrInvalidWrite
	}
	pageSize := uint64(c.options.PageSize)
	dataBytes := max(minimumDataBytes, uint64(onlineCompactionStagingChunkBytes))
	dataBytes = (dataBytes + pageSize - 1) &^ (pageSize - 1)
	return storeio.AppendGenerationMigrationStagingExtent(
		c.file, manifestStore, state.fileEnd, state.root.NextLogicalID,
		uint32(c.options.PageSize), dataBytes, minimumLogicalIDs,
		c.publishOnlineMigrationReservationLocked,
	)
}
