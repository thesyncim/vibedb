package durable

import (
	"crypto/rand"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
)

type OnlineCompactionReport struct {
	Documents, Attempts                     uint64
	SourceFileEnd, InstalledFileEnd         uint64
	StagingAllocatedBytes, StagingUsedBytes uint64
}

type onlineCompactionBuild struct {
	primary   storeio.PageRef
	exact     storeio.PageRef
	catalog   storeio.StagedPageCatalog
	router    *storeio.ResidentPrimaryRouter
	epoch     *primaryExactEpoch
	documents uint64
}

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
		SourceNextLogicalID:  nextState.root.NextLogicalID,
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
	// Recovery rejects a root whose allocator high-water exceeds the apparent
	// file size. Materialize the sparse high-water before publishing it; data
	// pages remain unreachable until the final conditional install.
	if err := c.file.Truncate(int64(tx.FileEnd())); err != nil {
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
	if c.onlineMigrationObserver != nil {
		if err := c.committer.SetPublicationObserver(nil); err != nil {
			return reservation, storeio.GenerationMigrationManifest{}, err
		}
		defer func() {
			_ = c.committer.SetPublicationObserver(c.onlineMigrationObserver)
		}()
	}
	pageSize := uint64(c.options.PageSize)
	dataBytes := max(minimumDataBytes, uint64(c.options.MaxPageSize))
	dataBytes = (dataBytes + pageSize - 1) &^ (pageSize - 1)
	return storeio.AppendGenerationMigrationStagingExtent(
		c.file, manifestStore, state.fileEnd, state.root.NextLogicalID,
		uint32(c.options.PageSize), dataBytes, minimumLogicalIDs,
		c.publishOnlineMigrationReservationLocked,
	)
}

// CompactOnline rewrites the live primary graph into bounded same-file staging
// extents while reads continue. Publications are observed from before the
// snapshot cut. A bounded retry loop converges after ordinary foreground bursts
// while preserving typed starvation/backpressure under a continuously dirty
// collection.
func (c *Collection) CompactOnline() (OnlineCompactionReport, error) {
	var total OnlineCompactionReport
	for attempt := uint64(1); attempt <= 4; attempt++ {
		report, err := c.compactOnlineAttempt()
		if total.SourceFileEnd == 0 {
			total.SourceFileEnd = report.SourceFileEnd
		}
		total.Attempts = attempt
		if err == nil {
			report.Attempts = attempt
			if total.SourceFileEnd != 0 {
				report.SourceFileEnd = total.SourceFileEnd
			}
			return report, nil
		}
		if !errors.Is(err, storeio.ErrGenerationMigrationStarved) {
			return report, err
		}
	}
	return total, storeio.ErrGenerationMigrationStarved
}

func (c *Collection) compactOnlineAttempt() (OnlineCompactionReport, error) {
	var report OnlineCompactionReport
	if c == nil {
		return report, ErrClosed
	}
	c.writer.Lock()
	if c.onlineMigrationDirty != nil {
		c.writer.Unlock()
		return report, storeio.ErrQueueFull
	}
	manifestStore, manifest, err := c.beginOnlineGenerationMigrationLocked()
	if err != nil {
		c.writer.Unlock()
		return report, err
	}
	report.SourceFileEnd = manifest.SourceFileEnd
	dirty, err := storeio.NewGenerationMigrationDirtySet(4096)
	if err != nil {
		c.writer.Unlock()
		return report, err
	}
	keyObserver := dirty.ObservePublication(func(key []byte) (uint64, bool) {
		router := c.primaryRouter.Load()
		if router == nil {
			return 0, false
		}
		route, ok := router.Route(key)
		return route.Ref.LogicalID, ok
	})
	observer := func(generation uint64, descriptor []byte) error {
		if len(descriptor) == 0 {
			return dirty.MarkTopology(generation)
		}
		return keyObserver(generation, descriptor)
	}
	c.onlineMigrationDirty, c.onlineMigrationObserver = dirty, observer
	if err := c.committer.SetPublicationObserver(observer); err != nil {
		c.onlineMigrationDirty, c.onlineMigrationObserver = nil, nil
		c.writer.Unlock()
		return report, err
	}
	snapshot, err := c.pinSnapshotLocked()
	c.writer.Unlock()
	if err != nil {
		c.clearOnlineMigrationObserver()
		return report, err
	}
	build, err := c.buildOnlineCompactionGeneration(snapshot, manifestStore, manifest)
	closeErr := snapshot.Close()
	if err != nil || closeErr != nil {
		c.clearOnlineMigrationObserver()
		return report, fmt.Errorf("online compaction build: %w", errors.Join(err, closeErr))
	}
	report.Documents, report.Attempts = build.documents, 1

	c.writer.Lock()
	defer c.writer.Unlock()
	if err := c.committer.SetPublicationObserver(nil); err != nil {
		return report, err
	}
	c.onlineMigrationDirty, c.onlineMigrationObserver = nil, nil
	ids, _, topology := dirty.Drain(make([]uint64, 0, dirty.Capacity()))
	if len(ids) != 0 || topology {
		return report, storeio.ErrGenerationMigrationStarved
	}
	current := c.state.Load()
	if current == nil {
		return report, ErrClosed
	}
	generation := current.root.Generation + 1
	tx, err := c.beginWriteTransaction(1, storeio.WriteTransactionOptions{
		StoreID: c.storeID, Generation: generation,
		PageSize: uint32(c.options.PageSize), FileEnd: current.fileEnd,
		NextLogicalID: current.root.NextLogicalID,
	})
	if err != nil {
		return report, err
	}
	abort := true
	defer func() {
		if abort {
			_ = tx.Abort()
		}
	}()
	target := current.root
	target.Generation = generation
	target.PrimaryRoot = build.primary
	target.ExactIndexRoot = build.exact
	if build.catalog.Head != (storeio.PageRef{}) {
		target.PageCatalogHead = build.catalog.Head
		target.PageCatalogDigest = build.catalog.Digest
		target.PageCatalogBytes = build.catalog.Bytes
	}
	descriptor, err := emptyMigrationPublicationDescriptor()
	if err != nil {
		return report, err
	}
	ready, err := manifestStore.Load()
	if err != nil {
		return report, err
	}
	ready.Phase = storeio.GenerationMigrationReady
	ready.CapturedSequence = current.root.Generation
	ready.AppliedSequence = ready.CapturedSequence
	ready.TargetPrimaryRoot = build.primary
	ready.TargetExactIndexRoot = build.exact
	ready.TargetCatalogHead = build.catalog.Head
	if err := manifestStore.Advance(ready); err != nil {
		return report, err
	}

	nextState := &fileStoreState{root: target, fileEnd: current.fileEnd, freeHead: current.freeHead}
	if build.epoch != nil {
		c.primaryEpochRetired = slices.Grow(c.primaryEpochRetired, 1)
	}
	c.snapshotGate.Lock()
	c.beginReaderFence()
	if err := storeio.PublishStagedStateConditional(tx, current.root, target, c.inlineFree, descriptor); err != nil {
		c.endReaderFence()
		c.snapshotGate.Unlock()
		return report, err
	}
	abort = false
	build.router.AdvanceGeneration(generation)
	c.primaryRouter.Store(build.router)
	if build.epoch != nil {
		old := c.primaryEpoch
		c.primaryEpoch = build.epoch
		if old != nil {
			c.primaryEpochRetired = append(c.primaryEpochRetired, retiredPrimaryExactEpoch{epoch: old, gen: generation})
		}
	}
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.endReaderFence()
	c.snapshotGate.Unlock()
	published, err := manifestStore.Load()
	if err != nil {
		return report, err
	}
	published.Phase = storeio.GenerationMigrationPublished
	if err := manifestStore.Advance(published); err != nil {
		return report, err
	}
	if c.deferredCanonicalLane() {
		err = c.flushPublishedPhysicalLocked()
	} else {
		err = c.waitPublished(generation)
	}
	report.InstalledFileEnd = nextState.fileEnd
	report.StagingAllocatedBytes = published.StagingAllocatedBytes
	report.StagingUsedBytes = published.StagingUsedBytes
	return report, err
}

func (c *Collection) clearOnlineMigrationObserver() {
	c.writer.Lock()
	_ = c.committer.SetPublicationObserver(nil)
	c.onlineMigrationDirty, c.onlineMigrationObserver = nil, nil
	c.writer.Unlock()
}

func (c *Collection) buildOnlineCompactionGeneration(
	snapshot *Snapshot,
	manifestStore *storeio.GenerationMigrationManifestStore,
	manifest storeio.GenerationMigrationManifest,
) (onlineCompactionBuild, error) {
	var result onlineCompactionBuild
	sink, err := storeio.NewGenerationMigrationChainedSink(
		c.file, c.storeID, manifest.TargetGeneration,
		uint32(c.options.PageSize), 4<<20, 1,
		make([]byte, max(c.options.MaxPageSize, 512<<10)),
		func(bytes, logicalIDs uint64) (storeio.UnrootedGenerationReservation, storeio.GenerationMigrationManifest, error) {
			return c.growOnlineMigrationStaging(manifestStore, bytes, logicalIDs)
		},
	)
	if err != nil {
		return result, err
	}
	builder, err := storeio.NewPrimaryValueGraphStreamBuilderOptions(
		sink, c.options.skipIndexes, c.options.OpaqueValues,
	)
	if err != nil {
		return result, err
	}
	readRun := func(ref storeio.PageRef, dst []byte) error {
		_, err := c.file.ReadAt(dst, int64(ref.Offset))
		return err
	}
	var exactBuilder *storeio.GenerationMigrationExactRunBuilder
	var exactRuns *storeio.GenerationMigrationExactRunSet
	live := make(map[uint32]*[storeio.TermPostingTileChunks]uint64)
	if len(c.options.indexes) != 0 {
		exactRuns, err = storeio.NewGenerationMigrationExactRunSet(sink, readRun, uint32(c.options.MaxPageSize))
		if err != nil {
			return result, err
		}
		exactBuilder, err = storeio.NewStreamingGenerationMigrationExactRunBuilder(
			sink, uint32(c.options.MaxPageSize), 4096, 1<<20, exactRuns.Add,
		)
		if err != nil {
			return result, err
		}
	}
	type row struct{ keyAt, keyEnd, valueAt, valueEnd int }
	rows := make([]row, 0, min(256, c.options.MaxBatchDocuments))
	arena := make([]byte, 0, min(4<<20, c.options.MaxDocumentBytes+c.options.MaxKeyBytes))
	records := make([]storeio.CommonPrimaryLeafRecord, cap(rows))
	placements := make([]storeio.PrimaryGraphPlacement, cap(rows))
	exactRecords := make([]storeio.PrimaryGraphRecord, cap(rows))
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		records = records[:len(rows)]
		for index, source := range rows {
			key := arena[source.keyAt:source.keyEnd]
			value := arena[source.valueAt:source.valueEnd]
			records[index] = storeio.CommonPrimaryLeafRecord{Key: key}
			if len(value) <= c.options.InlineValueBytes {
				records[index].Value.Inline = value
			} else {
				head, err := c.stagePrimaryOverflowChainToSink(sink, value, manifest.TargetGeneration)
				if err != nil {
					return err
				}
				records[index].Value.Overflow = head
			}
		}
		var windowPlacements []storeio.PrimaryGraphPlacement
		if exactBuilder != nil {
			windowPlacements = placements[:len(rows)]
		}
		if err := builder.StageWindow(records, windowPlacements); err != nil {
			return err
		}
		if exactBuilder != nil {
			windowExact := exactRecords[:len(rows)]
			for index, source := range rows {
				windowExact[index] = storeio.BorrowPrimaryGraphRecord(
					arena[source.keyAt:source.keyEnd], arena[source.valueAt:source.valueEnd],
				)
				placement := windowPlacements[index]
				tile := uint32(placement.Bucket)<<2 | uint32(placement.Slot>>6)
				mask := live[tile]
				if mask == nil {
					mask = new([storeio.TermPostingTileChunks]uint64)
					live[tile] = mask
				}
				mask[0] |= uint64(1) << uint(placement.Slot&63)
			}
			if err := stagePrimaryExactRunWindow(exactBuilder, windowExact, windowPlacements, c.options.indexes); err != nil {
				return err
			}
		}
		result.documents += uint64(len(rows))
		rows, arena = rows[:0], arena[:0]
		return nil
	}
	_, err = snapshot.RangeRawBuffer(nil, func(key, value []byte) error {
		if len(rows) != 0 && (len(rows) == cap(rows) || len(arena)+len(key)+len(value) > 4<<20) {
			if err := flush(); err != nil {
				return err
			}
		}
		keyAt := len(arena)
		arena = append(arena, key...)
		keyEnd := len(arena)
		valueAt := len(arena)
		arena = append(arena, value...)
		rows = append(rows, row{keyAt, keyEnd, valueAt, len(arena)})
		return nil
	})
	if err != nil {
		return result, err
	}
	if err := flush(); err != nil {
		return result, err
	}
	if result.documents == 0 {
		return result, storeio.ErrInvalidWrite
	}
	result.primary, err = builder.Finish()
	if err != nil {
		return result, err
	}
	if exactBuilder != nil {
		if _, err := exactBuilder.Finish(); err != nil {
			return result, err
		}
		merged, err := exactRuns.Finish()
		if err != nil {
			return result, err
		}
		if merged.Pages == 0 {
			rootPage, err := sink.AllocatePage(storeio.PagePrimaryExactRoot, uint32(c.options.PageSize), 0)
			if err != nil {
				return result, err
			}
			if _, err := storeio.EncodePrimaryExactRootPage(
				rootPage.Bytes(), sink.StoreIdentity(), sink.BuildGeneration(),
				rootPage.Ref().LogicalID, make([]storeio.PrimaryExactRootEntry, len(c.options.indexes)),
			); err != nil {
				return result, err
			}
			if err := rootPage.Stage(); err != nil {
				return result, err
			}
			result.exact = rootPage.Ref()
		} else {
			result.exact, err = buildPrimaryExactIndexesFromMergedRun(
				sink, readRun, merged, uint32(len(c.options.indexes)),
				uint32(c.options.PageSize), uint32(c.options.MaxPageSize),
				func(tile uint32) *[storeio.TermPostingTileChunks]uint64 { return live[tile] },
			)
			if err != nil {
				return result, err
			}
		}
	}
	if c.options.pageCatalog != nil && c.options.pageCatalog.CanonicalSize() != 0 {
		result.catalog, err = storeio.StageCanonicalPageCatalog(sink, c.options.pageCatalog, uint32(c.options.PageSize))
		if err != nil {
			return result, err
		}
	}
	if err := sink.Sync(); err != nil {
		return result, err
	}
	state := c.state.Load()
	result.router, err = storeio.BuildResidentPrimaryRouter(c.cache, result.primary, storeio.GlobalTabletCatalogBounds{
		StoreID: c.storeID, SelectedRootGeneration: state.root.Generation,
		FileEnd: state.fileEnd, NextLogicalID: state.root.NextLogicalID,
	})
	if err != nil {
		return result, err
	}
	if result.exact != (storeio.PageRef{}) {
		target := &fileStoreState{root: state.root, fileEnd: state.fileEnd, freeHead: state.freeHead}
		target.root.Generation++
		target.root.PrimaryRoot = result.primary
		target.root.ExactIndexRoot = result.exact
		result.epoch, err = c.buildPrimaryExactEpoch(target, result.router)
	}
	return result, err
}
