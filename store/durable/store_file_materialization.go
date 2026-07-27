package durable

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/internal/storeio"
)

// tryMaterializeFileUpdate replaces one projection-neutral inline document
// behind its existing PageRef. It leaves every query route unchanged: readers
// never consult the journal and never branch on a delta or tombstone.
//
// handled=false is an ordinary eligibility fallback, not an error. The caller
// proceeds through the existing copy-on-write transaction unchanged.
func (c *Collection) tryMaterializeFileUpdate(
	state *fileStoreState,
	key, src []byte,
	newIndex vibejson.Index,
	location storeio.KeyLocation,
	match *fileFingerprintMatch,
) (handled bool, err error) {
	if c == nil || state == nil || match == nil ||
		c.synchronous() ||
		c.options.MaterializationDamageGranule == 0 ||
		c.materializationBlock == nil {
		return false, nil
	}
	c.materializationAttempts.Add(1)
	fallback := func() (bool, error) {
		c.materializationFallbacks.Add(1)
		return false, nil
	}

	ref := match.documentRef
	oldValue := match.value.value
	if ref.Kind != storeio.PageDocument || match.view.grouped ||
		oldValue.Overflow != (storeio.PageRef{}) ||
		len(oldValue.Inline) != len(src) ||
		len(src) == 0 || int(ref.Length) > c.options.MaxPageSize {
		return fallback()
	}
	generation := state.root.Generation + 1
	if generation == 0 {
		return false, storeio.ErrGenerationOrder
	}

	oldIndex, indexErr := c.buildOldFileIndex(oldValue.Inline)
	if indexErr != nil {
		return false, indexErr
	}
	projectionsEqual, projectionErr :=
		c.fileMaterializationProjectionsEqual(oldIndex, newIndex)
	if projectionErr != nil {
		return false, projectionErr
	}
	if !projectionsEqual {
		return fallback()
	}

	record := storeio.DocumentRecord{
		Key: key, JSON: src, Slot: location.Slot,
	}
	edit := [1]fileChunkEdit{{
		record: record, slot: location.Slot, keep: true,
	}}
	rows, live, rowsErr := c.buildFileRows(state, &match.view, edit[:])
	if rowsErr != nil {
		return false, rowsErr
	}
	columns, columnsErr := c.buildFileFloat64Columns(
		state, &match.view, location.Slot, &newIndex, true,
	)
	if columnsErr != nil {
		return false, columnsErr
	}
	size, sizeErr := c.fileDocumentPageSize(rows, columns)
	if sizeErr != nil {
		return false, sizeErr
	}
	if size != ref.Length {
		return fallback()
	}

	leafRef, zoneRef, leafView, leafLease, found, leafErr :=
		storeio.LookupResidentChunkTreeLeaf(
			c.cache, state.chunkRoot, location.Chunk,
			storeio.ChunkTreeBounds{
				FileEnd:       state.super.FileEnd,
				NextLogicalID: state.root.NextLogicalID,
			},
		)
	if leafErr != nil {
		return false, leafErr
	}
	if !found {
		return fallback()
	}
	defer leafLease.Release()
	if zoneRef != ref {
		return false, storeio.ErrChunkDirectoryCorrupt
	}

	oldZone := leafView.Zone(location.Chunk)
	nextZone := storeio.ChunkZone{}
	writeZones := leafView.HasZones()
	if merger := c.zoneMerger(
		rows, edit[:], zonePriorDocs(&match.view),
	); merger != nil {
		nextZone = merger.MergeChunkZone(oldZone)
		writeZones = true
	}
	zoneChanged := nextZone != oldZone ||
		writeZones != leafView.HasZones()

	totalImageBytes := int(ref.Length)
	if zoneChanged {
		totalImageBytes += int(leafRef.Length)
	}
	if totalImageBytes > len(c.materializationBefore) ||
		totalImageBytes > len(c.materializationAfter) {
		return fallback()
	}
	documentBefore := c.materializationBefore[:ref.Length]
	documentAfter := c.materializationAfter[:ref.Length]
	copy(documentBefore, match.document.Page())
	if _, encodeErr := storeio.EncodeDocumentPageWithColumns(
		documentAfter,
		storeio.DocumentPageHeader{
			StoreID: c.storeID,
			// The canonical reference identity is stable. Only the alternate
			// root and journal advance to generation.
			Generation: ref.Generation,
			LogicalID:  ref.LogicalID,
			PageSize:   ref.Length,
			ChunkID:    location.Chunk,
			Live:       live,
		},
		rows, columns, state.root.NextLogicalID,
		state.super.FileEnd, uint32(c.options.PageSize),
	); encodeErr != nil {
		return false, encodeErr
	}
	if bytes.Equal(documentBefore, documentAfter) {
		return fallback()
	}

	var leafBefore, leafAfter []byte
	if zoneChanged {
		documentBytes := int(ref.Length)
		leafBefore = c.materializationBefore[documentBytes:totalImageBytes:totalImageBytes]
		leafAfter = c.materializationAfter[documentBytes:totalImageBytes:totalImageBytes]
		copy(leafBefore, leafLease.Page())

		var refs [64]storeio.PageRef
		var zones [64]storeio.ChunkZone
		count := leafView.Len()
		laneRank := -1
		for rank := 0; rank < count; rank++ {
			child, ok := leafView.RefAt(rank)
			if !ok {
				return false, storeio.ErrChunkDirectoryCorrupt
			}
			refs[rank] = child
			zones[rank] = leafView.ZoneAt(rank)
			if chunk, ok := leafView.ChunkIDAt(rank); ok &&
				chunk == location.Chunk {
				laneRank = rank
			}
		}
		if laneRank < 0 || refs[laneRank] != ref {
			return false, storeio.ErrChunkDirectoryCorrupt
		}
		zones[laneRank] = nextZone
		var encodedZones []storeio.ChunkZone
		if writeZones {
			encodedZones = zones[:count]
		}
		header := leafView.Header()
		// Like the document target, the leaf keeps its canonical identity.
		header.Generation = leafRef.Generation
		header.LogicalID = leafRef.LogicalID
		header.PageSize = leafRef.Length
		if _, encodeErr := storeio.EncodeChunkDirectoryZonePage(
			leafAfter, header, refs[:count], encodedZones,
			state.super.FileEnd, state.root.NextLogicalID,
		); encodeErr != nil {
			return false, encodeErr
		}
		if bytes.Equal(leafBefore, leafAfter) {
			return false, storeio.ErrChunkDirectoryCorrupt
		}
	}

	var patchStorage [storeio.MaterializationJournalMaxPatches]storeio.MaterializationPatch
	documentTargetRank, leafTargetRank := uint16(0), uint16(1)
	if zoneChanged {
		documentTargetRank, leafTargetRank =
			fileMaterializationTargetRanks(ref, leafRef)
	}
	patches := patchStorage[:0]
	patchBytes := 0
	patchOK := true
	if zoneChanged && leafTargetRank == 0 {
		patches, patchBytes, patchOK = planFileMaterializationPatches(
			patches, leafBefore, leafAfter,
			uint32(c.options.MaterializationDamageGranule),
			leafTargetRank,
		)
	}
	if patchOK {
		var documentPatchBytes int
		patches, documentPatchBytes, patchOK =
			planFileMaterializationPatches(
				patches, documentBefore, documentAfter,
				uint32(c.options.MaterializationDamageGranule),
				documentTargetRank,
			)
		patchBytes += documentPatchBytes
	}
	if patchOK && zoneChanged && leafTargetRank == 1 {
		var leafPatchBytes int
		patches, leafPatchBytes, patchOK =
			planFileMaterializationPatches(
				patches, leafBefore, leafAfter,
				uint32(c.options.MaterializationDamageGranule),
				leafTargetRank,
			)
		patchBytes += leafPatchBytes
	}
	if !patchOK || patchBytes > storeio.MaterializationJournalMaxData {
		return fallback()
	}

	batch, beginErr := c.committer.BeginMaterialized(len(patches))
	if beginErr != nil {
		return false, beginErr
	}
	abort := true
	defer func() {
		if abort {
			err = errors.Join(err, batch.Abort())
		}
	}()
	sequence, sequenceErr := batch.MaterializationSequence()
	if sequenceErr != nil {
		return false, sequenceErr
	}
	header := storeio.MaterializationJournalHeader{
		StoreID: c.storeID, Sequence: sequence,
		TargetGeneration: generation,
		PageSize:         uint32(c.options.PageSize),
		SectorSize:       uint32(c.options.MaterializationDamageGranule),
	}
	var targets [2]storeio.MaterializationTarget
	var targetErr error
	targets[documentTargetRank], targetErr = storeio.BuildMaterializationTarget(
		header, ref, documentBefore, documentAfter, patches,
		documentTargetRank,
	)
	if targetErr != nil {
		return false, targetErr
	}
	targetCount := 1
	if zoneChanged {
		targets[leafTargetRank], targetErr = storeio.BuildMaterializationTarget(
			header, leafRef, leafBefore, leafAfter, patches,
			leafTargetRank,
		)
		if targetErr != nil {
			return false, targetErr
		}
		targetCount = 2
	}
	journal, journalErr := batch.MaterializationJournalBuffer()
	if journalErr != nil {
		return false, journalErr
	}
	if _, encodeErr := storeio.EncodeMaterializationJournal(
		journal, header, targets[:targetCount], patches,
	); encodeErr != nil {
		if errors.Is(encodeErr, storeio.ErrInvalidWrite) {
			return fallback()
		}
		return false, encodeErr
	}
	if sealErr := batch.SealMaterializationJournal(); sealErr != nil {
		return false, sealErr
	}
	nextRoot := state.root
	nextRoot.Generation = generation
	nextInline := storeio.InlineSuperblock{
		StoreID: c.storeID, Generation: generation,
		FileEnd:  state.super.FileEnd,
		PageSize: uint32(c.options.PageSize),
		State:    nextRoot, FreeDelta: c.inlineFree,
	}
	if rootErr := batch.SetInlineSuperblock(nextInline); rootErr != nil {
		return false, rootErr
	}
	if targetErr := batch.StageBuiltMaterializationTarget(
		int(documentTargetRank), targets[documentTargetRank], documentAfter,
	); targetErr != nil {
		return false, targetErr
	}
	if zoneChanged {
		if targetErr := batch.StageBuiltMaterializationTarget(
			int(leafTargetRank), targets[leafTargetRank], leafAfter,
		); targetErr != nil {
			return false, targetErr
		}
	}

	// Drop the resolving lease before taking the publication gate. Existing
	// readers retain generation leases, which SafeFromSnapshots checks while
	// this gate prevents a new old-generation snapshot from appearing.
	match.Release()
	leafLease.Release()
	c.snapshotGate.Lock()
	defer c.snapshotGate.Unlock()
	oldestMaterializedGeneration := ref.Generation
	if zoneChanged && leafRef.Generation < oldestMaterializedGeneration {
		oldestMaterializedGeneration = leafRef.Generation
	}
	if !c.leases.SafeFromSnapshots(oldestMaterializedGeneration) {
		c.materializationSnapshotSkips.Add(1)
		return fallback()
	}
	// Journal ranks follow physical offset, as required by the capsule format.
	// Cache publication is independently deterministic: document first, then
	// its directory leaf. Every pre-publication rollback below runs the inverse
	// cache order, leaf then document.
	if replaceErr := c.cache.ReplaceCanonicalDirty(
		ref, documentBefore, documentAfter, generation,
	); replaceErr != nil {
		if errors.Is(replaceErr, storeio.ErrCanonicalPageBusy) ||
			errors.Is(replaceErr, storeio.ErrCanonicalPageChanged) {
			c.materializationBusySkips.Add(1)
			return fallback()
		}
		return false, replaceErr
	}
	if zoneChanged {
		if replaceErr := c.cache.ReplaceCanonicalDirty(
			leafRef, leafBefore, leafAfter, generation,
		); replaceErr != nil {
			restoreErr := c.cache.RestoreCanonicalDirty(
				ref, documentBefore, documentAfter, generation,
			)
			if errors.Is(replaceErr, storeio.ErrCanonicalPageBusy) ||
				errors.Is(replaceErr, storeio.ErrCanonicalPageChanged) {
				c.materializationBusySkips.Add(1)
				if restoreErr == nil {
					return fallback()
				}
			}
			return false, errors.Join(replaceErr, restoreErr)
		}
	}
	if publishErr := batch.Publish(generation); publishErr != nil {
		var restoreErr error
		if zoneChanged {
			restoreErr = c.cache.RestoreCanonicalDirty(
				leafRef, leafBefore, leafAfter, generation,
			)
		}
		restoreErr = errors.Join(restoreErr, c.cache.RestoreCanonicalDirty(
			ref, documentBefore, documentAfter, generation,
		))
		return false, errors.Join(publishErr, restoreErr)
	}
	abort = false

	nextSuper := state.super
	nextSuper.Generation = generation
	nextState := &fileStoreState{
		root: nextRoot, super: nextSuper,
		keyRoot: state.keyRoot, chunkRoot: state.chunkRoot,
		indexRoot: state.indexRoot, freeHead: state.freeHead,
	}
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.materializationUpdates.Add(1)
	return true, nil
}

func planFileMaterializationPatches(
	dst []storeio.MaterializationPatch,
	before, after []byte,
	sectorSize uint32,
	target uint16,
) ([]storeio.MaterializationPatch, int, bool) {
	if sectorSize == 0 || len(before) != len(after) ||
		len(before)%int(sectorSize) != 0 {
		return dst, 0, false
	}
	total := 0
	for offset := 0; offset < len(before); offset += int(sectorSize) {
		end := offset + int(sectorSize)
		if bytes.Equal(before[offset:end], after[offset:end]) {
			continue
		}
		total += int(sectorSize)
		if len(dst) != 0 {
			last := &dst[len(dst)-1]
			if last.Target == target &&
				int(last.Offset)+len(last.Data) == offset {
				last.Data = before[int(last.Offset):end]
				continue
			}
		}
		if len(dst) == cap(dst) {
			return dst, total, false
		}
		dst = append(dst, storeio.MaterializationPatch{
			Target: target, Offset: uint32(offset), Data: before[offset:end],
		})
	}
	return dst, total, len(dst) != 0
}

func fileMaterializationTargetRanks(
	document, leaf storeio.PageRef,
) (documentRank, leafRank uint16) {
	if leaf.Offset < document.Offset {
		return 1, 0
	}
	return 0, 1
}
