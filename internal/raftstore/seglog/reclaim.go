package seglog

import (
	"errors"
	"fmt"
	"os"
)

var reclaimMinSegments = 8
var reclaimMaxSegments = maxRetiredSegments
var reclaimRemove = os.Remove
var reclaimSyncDir = syncDir
var reclaimBeforeRemove func(string)
var reclaimBeforeCheckpointRemove func(string)

type reclaimPublishPhase uint8

const (
	reclaimCheckpointAPublished reclaimPublishPhase = iota + 1
	reclaimPreparedPublished
	reclaimCheckpointBPublished
	reclaimDurablePublished
	reclaimReservePrepared
	reclaimReservePublished
	reclaimFileRemoved
	reclaimQueueClearFirst
	reclaimQueueClearSecond
)

var reclaimPublishHook func(reclaimPublishPhase) error

func reclaimThresholdReached(count, limit int, bytes, capacity uint64) bool {
	if count == 0 {
		return false
	}
	return count >= reclaimMinSegments || bytes >= capacity*2 || count >= limit
}

func runReclaimHook(phase reclaimPublishPhase) error {
	if reclaimPublishHook != nil {
		return reclaimPublishHook(phase)
	}
	return nil
}

// ReclaimDeadPrefix executes on the serial maintenance lane. It may block the
// caller, but never performs checkpoint, catalog, or file I/O on the append
// goroutine or while writeMu is held.
func (e *Engine) ReclaimDeadPrefix() error {
	if e == nil || e.reclaimRequests == nil || e.sealStop == nil {
		return ErrRaftState
	}
	result := make(chan error, 1)
	select {
	case e.reclaimRequests <- result:
	case <-e.sealStop:
		return os.ErrClosed
	}
	select {
	case err := <-result:
		return err
	case <-e.sealStop:
		if e.log != nil {
			if err := e.log.usable(); err != nil {
				return err
			}
		}
		return os.ErrClosed
	}
}

func (e *Engine) reclaimDeadPrefix() error {
	e.writeMu.Lock()
	if e.log != nil && e.log.metadata != nil && (e.log.metadata.slot.ReclaimPhase != reclaimNone || e.log.metadata.slot.RetiredCheckpointCount != 0) {
		if e.maintenanceBusy || e.sealPending || e.log.usable() != nil || e.log.metadata.slot.HasPending {
			e.writeMu.Unlock()
			return ErrBounds
		}
		e.maintenanceBusy = true
		e.writeMu.Unlock()
		defer func() {
			e.writeMu.Lock()
			e.maintenanceBusy = false
			e.writeMu.Unlock()
		}()
		return e.resumeReclaim()
	}
	if e.maintenanceBusy || e.sealPending || e.log == nil || e.log.usable() != nil || e.log.metadata == nil || e.log.metadata.needsHealing || e.log.metadata.slot.HasPending || e.log.metadata.slot.ReclaimPhase != reclaimNone {
		e.writeMu.Unlock()
		return ErrBounds
	}
	limit := min(reclaimMaxSegments, maxRetiredSegments)
	cut := 0
	usedBytes := uint64(0)
	for cut < len(e.log.state.Segments) && cut < limit {
		segment := e.log.state.Segments[cut]
		if segment.State != SegmentSealed || cut >= len(e.reclaimAfter) || e.liveSealed[segment.ID] != 0 || e.reclaimAfter[cut] == 0 || e.reclaimAfter[cut] > e.sealedSequence {
			break
		}
		if usedBytes > ^uint64(0)-segment.Bytes {
			e.writeMu.Unlock()
			return ErrBounds
		}
		usedBytes += segment.Bytes
		cut++
	}
	if !reclaimThresholdReached(cut, limit, usedBytes, e.log.state.SegmentCapacity) {
		e.writeMu.Unlock()
		return ErrBounds
	}
	e.maintenanceBusy = true
	baseSlot := e.log.metadata.slot
	removed := e.log.state.Segments[:cut]
	retained := e.log.state.Segments[cut:]
	oldBanks := e.log.metadata.bankSlots
	e.writeMu.Unlock()
	var compacted []SegmentMeta
	var compactedFences []uint64
	if cap(retained) > max(2*(len(retained)+1), 256) {
		newCapacity := max(len(retained)+1, 256)
		compacted = make([]SegmentMeta, len(retained), newCapacity)
		copy(compacted, retained)
		compactedFences = make([]uint64, len(retained), newCapacity)
	}
	defer func() {
		e.writeMu.Lock()
		e.maintenanceBusy = false
		e.writeMu.Unlock()
	}()

	anchor := removed[len(removed)-1]
	record := catalogRecord{Kind: catalogAnchor, AnchorID: anchor.ID, AnchorGeneration: anchor.Generation, AnchorHash: anchor.Hash}
	tail, catalogHash, err := e.log.metadata.previewRecord(record, baseSlot.Generation+1)
	if err != nil {
		return fmt.Errorf("reclaim preview anchor: %w", err)
	}
	checkpointAID := derivedCheckpointID(e.authKey, baseSlot.LogID, baseSlot.Generation+1, tail, catalogHash, anchor.ID, anchor.Generation, anchor.Hash, checkpointRoleReclaimA)
	checkpointA := catalogCheckpoint{ID: checkpointAID, LogID: baseSlot.LogID, Generation: baseSlot.Generation + 1, Tail: tail, CatalogHash: catalogHash, AnchorID: anchor.ID, AnchorGeneration: anchor.Generation, AnchorHash: anchor.Hash, Segments: retained, BaseSequence: e.sealedSequence, GroupIDs: e.sealedSummaryOrder, GroupSummaries: e.sealedSummaries}
	checkpointAHash, err := catalogCheckpointWriter(e.log.dir, checkpointA, e.authKey)
	if err != nil {
		return fmt.Errorf("reclaim checkpoint A: %w", err)
	}
	if err = runReclaimHook(reclaimCheckpointAPublished); err != nil {
		return err
	}
	cutA := baseSlot
	cutA.Generation++
	cutA.AnchorID, cutA.AnchorGeneration, cutA.AnchorHash = anchor.ID, anchor.Generation, anchor.Hash
	cutA.CheckpointID, cutA.CheckpointTail, cutA.CheckpointHash = [16]byte(checkpointAID), tail, checkpointAHash
	cutA.PreviousCheckpointID, cutA.PreviousCheckpointTail, cutA.PreviousCheckpointHash = [16]byte{}, 0, [32]byte{}
	cutA.ReclaimPhase, cutA.RetiredCount = reclaimPrepared, uint8(len(removed))
	for i := range removed {
		cutA.Retired[i] = retiredDescriptor{ID: removed[i].ID, Generation: removed[i].Generation, FileID: removed[i].FileID, Bytes: removed[i].Bytes, PreviousHash: removed[i].PreviousHash, Hash: removed[i].Hash}
	}
	if err = addCheckpointRetirements(&cutA, oldBanks); err != nil {
		return fmt.Errorf("reclaim checkpoint retirement intent: %w", err)
	}
	if err = e.log.metadata.publish(cutA, &record); err != nil {
		return fmt.Errorf("reclaim cut A: %w", err)
	}
	if err = runReclaimHook(reclaimPreparedPublished); err != nil {
		return err
	}
	cutA = e.log.metadata.slot
	checkpointBID := derivedCheckpointID(e.authKey, baseSlot.LogID, cutA.Generation+1, tail, catalogHash, anchor.ID, anchor.Generation, anchor.Hash, checkpointRoleReclaimB)
	checkpointB := checkpointA
	checkpointB.ID, checkpointB.Generation = checkpointBID, cutA.Generation+1
	checkpointBHash, err := catalogCheckpointWriter(e.log.dir, checkpointB, e.authKey)
	if err != nil {
		return fmt.Errorf("reclaim checkpoint B: %w", err)
	}
	if err = runReclaimHook(reclaimCheckpointBPublished); err != nil {
		return err
	}
	cutB := cutA
	cutB.Generation++
	cutB.ReclaimPhase = reclaimDurable
	cutB.PreviousCheckpointID, cutB.PreviousCheckpointTail, cutB.PreviousCheckpointHash = cutB.CheckpointID, cutB.CheckpointTail, cutB.CheckpointHash
	cutB.CheckpointID, cutB.CheckpointTail, cutB.CheckpointHash = [16]byte(checkpointBID), tail, checkpointBHash
	if validateErr := validateMetadataSlot(cutB); validateErr != nil {
		return fmt.Errorf("reclaim cut B slot: %w", validateErr)
	}
	if err = e.log.metadata.publish(cutB, nil); err != nil {
		return fmt.Errorf("reclaim cut B: %w", err)
	}
	if err = runReclaimHook(reclaimDurablePublished); err != nil {
		return err
	}
	nextSegments := retained
	if compacted != nil {
		nextSegments = compacted
	}
	if err = e.finishDurableReclaim(oldBanks, nextSegments, cut, compactedFences); err != nil {
		return fmt.Errorf("finish durable reclaim: %w", err)
	}
	return nil
}

// resumeReclaim completes an authenticated interrupted reclaim before the
// Engine starts its maintenance worker or can accept a Rotate. PREPARED has
// only one recovery bank and may not delete; it first creates an independent
// checkpoint and publishes DURABLE through the opposite bank. DURABLE owns the
// exact retired queue and can idempotently finish unlink+dirsync and clear it.
func (e *Engine) resumeReclaim() error {
	if e == nil || e.log == nil || e.log.metadata == nil {
		return ErrCorrupt
	}
	slot := e.log.metadata.slot
	if slot.ReclaimPhase == reclaimNone {
		if slot.RetiredCheckpointCount != 0 {
			return e.finishCheckpointRetirements()
		}
		return nil
	}
	oldBanks := e.log.metadata.bankSlots
	if slot.ReclaimPhase == reclaimPrepared {
		retained := e.log.state.Segments
		for len(retained) != 0 && retained[0].ID <= slot.AnchorID {
			retained = retained[1:]
		}
		checkpointID := derivedCheckpointID(e.authKey, slot.LogID, slot.Generation+1, slot.CatalogTail, slot.CatalogHash, slot.AnchorID, slot.AnchorGeneration, slot.AnchorHash, checkpointRoleReclaimB)
		checkpoint := catalogCheckpoint{ID: checkpointID, LogID: slot.LogID, Generation: slot.Generation + 1, Tail: slot.CatalogTail, CatalogHash: slot.CatalogHash, AnchorID: slot.AnchorID, AnchorGeneration: slot.AnchorGeneration, AnchorHash: slot.AnchorHash, Segments: retained, BaseSequence: e.sealedSequence, GroupIDs: e.sealedSummaryOrder, GroupSummaries: e.sealedSummaries}
		checkpointHash, err := catalogCheckpointWriter(e.log.dir, checkpoint, e.authKey)
		if err != nil {
			return err
		}
		if err = runReclaimHook(reclaimCheckpointBPublished); err != nil {
			return err
		}
		next := slot
		next.Generation++
		next.ReclaimPhase = reclaimDurable
		next.PreviousCheckpointID, next.PreviousCheckpointTail, next.PreviousCheckpointHash = next.CheckpointID, next.CheckpointTail, next.CheckpointHash
		next.CheckpointID, next.CheckpointTail, next.CheckpointHash = [16]byte(checkpointID), next.CatalogTail, checkpointHash
		if err = e.log.metadata.publish(next, nil); err != nil {
			return err
		}
		if err = runReclaimHook(reclaimDurablePublished); err != nil {
			return err
		}
		e.log.state.Generation = next.Generation
		slot = e.log.metadata.slot
	}
	if slot.ReclaimPhase != reclaimDurable {
		return ErrCorrupt
	}
	return e.finishDurableReclaim(oldBanks, nil, 0, nil)
}

func (e *Engine) finishDurableReclaim(oldBanks [2]metadataSlot, replacement []SegmentMeta, removedCount int, compactedFences []uint64) error {
	slot := e.log.metadata.slot
	if slot.ReclaimPhase != reclaimDurable || slot.RetiredCount == 0 {
		return ErrCorrupt
	}
	e.writeMu.Lock()
	if replacement == nil {
		cut := 0
		for cut < len(e.log.state.Segments) && e.log.state.Segments[cut].ID <= slot.AnchorID {
			cut++
		}
		if cut != 0 {
			e.log.state.Segments = e.log.state.Segments[cut:]
			e.reclaimAfter = e.reclaimAfter[cut:]
		}
	}
	e.log.state.AnchorID, e.log.state.AnchorGeneration, e.log.state.AnchorHash = slot.AnchorID, slot.AnchorGeneration, slot.AnchorHash
	e.log.state.Generation = slot.Generation
	// A newly initiated reclaim already prepared a compact replacement. Resume
	// on Open starts from the checkpoint's compact descriptor allocation.
	if replacement != nil {
		retainedFences := e.reclaimAfter[removedCount:]
		if compactedFences != nil {
			copy(compactedFences, retainedFences)
			retainedFences = compactedFences
		}
		e.log.state.Segments = replacement
		e.reclaimAfter = retainedFences
	}
	for i := range e.readers {
		if e.readers[i].file != nil && e.readers[i].id <= slot.AnchorID {
			if closeErr := e.readers[i].file.Close(); closeErr != nil {
				e.writeMu.Unlock()
				return closeErr
			}
			e.readers[i] = segmentReader{}
		}
	}
	e.writeMu.Unlock()

	// Fill empty reserve ownership from the dead prefix before unlinking. The
	// lifecycle certificate remains authenticated while the identity area is
	// zeroed, so an interrupted conversion resumes from the same retired intent.
	next := slot
	var recycledFiles [2]*os.File
	for reserveSlot := range next.Reserves {
		if next.Reserves[reserveSlot].Ready {
			continue
		}
		retiredIndex := -1
		for i := 0; i < int(next.RetiredCount); i++ {
			if next.RetiredReserveMask&(uint32(1)<<i) == 0 {
				retiredIndex = i
				break
			}
		}
		if retiredIndex < 0 {
			break
		}
		retired := next.Retired[retiredIndex]
		descriptor := reserveDescriptor{FileID: retired.FileID, Capacity: e.log.state.SegmentCapacity, Ready: true}
		file, openErr := os.OpenFile(segmentPath(e.log.dir, retired.FileID), os.O_RDWR, 0)
		if openErr != nil {
			for _, opened := range recycledFiles {
				if opened != nil {
					_ = opened.Close()
				}
			}
			return openErr
		}
		if recycleErr := recycleRetiredSegment(file, descriptor, e.log.state.LogID, e.authKey); recycleErr != nil {
			_ = file.Close()
			for _, opened := range recycledFiles {
				if opened != nil {
					_ = opened.Close()
				}
			}
			return recycleErr
		}
		recycledFiles[reserveSlot] = file
		next.Reserves[reserveSlot] = descriptor
		next.RetiredReserveMask |= uint32(1) << retiredIndex
	}
	if next.RetiredReserveMask != slot.RetiredReserveMask {
		if err := runReclaimHook(reclaimReservePrepared); err != nil {
			for _, file := range recycledFiles {
				if file != nil {
					_ = file.Close()
				}
			}
			return err
		}
		next.Generation++
		if err := e.log.metadata.publish(next, nil); err != nil {
			for _, file := range recycledFiles {
				if file != nil {
					_ = file.Close()
				}
			}
			return err
		}
		if err := runReclaimHook(reclaimReservePublished); err != nil {
			for _, file := range recycledFiles {
				if file != nil {
					_ = file.Close()
				}
			}
			return err
		}
		e.writeMu.Lock()
		for i := range recycledFiles {
			if recycledFiles[i] != nil {
				e.log.reserveFiles[i] = recycledFiles[i]
				e.log.state.Reserves[i] = next.Reserves[i]
			}
		}
		e.log.state.Generation = next.Generation
		e.writeMu.Unlock()
		slot = e.log.metadata.slot
	}

	for i := 0; i < int(slot.RetiredCount); i++ {
		if slot.RetiredReserveMask&(uint32(1)<<i) != 0 {
			continue
		}
		retired := slot.Retired[i]
		path := segmentPath(e.log.dir, retired.FileID)
		file, openErr := os.Open(path)
		if errors.Is(openErr, os.ErrNotExist) {
			if err := reclaimSyncDir(e.log.dir); err != nil {
				return err
			}
			continue
		}
		if openErr != nil {
			return openErr
		}
		opened, statErr := file.Stat()
		if statErr == nil {
			var derived SegmentMeta
			derived, _, statErr = readUnpublishedSealedFile(file, retired.FileID, e.log.state.SegmentCapacity, e.log.state.LogID, retired.ID-1, retired.PreviousHash, e.authKey)
			if statErr == nil && (derived.ID != retired.ID || derived.Generation != retired.Generation || derived.Hash != retired.Hash) {
				statErr = ErrCorrupt
			}
		}
		closeErr := file.Close()
		if statErr != nil || closeErr != nil {
			return errors.Join(statErr, closeErr)
		}
		if reclaimBeforeRemove != nil {
			reclaimBeforeRemove(path)
		}
		if err := removeExactPublishedPath(opened, path, e.log.dir); err != nil {
			return err
		}
		if err := runReclaimHook(reclaimFileRemoved); err != nil {
			return err
		}
	}
	clearA := slot
	clearA.Generation++
	clearA.ReclaimPhase, clearA.RetiredCount, clearA.RetiredReserveMask = reclaimNone, 0, 0
	clear(clearA.Retired[:])
	if err := e.log.metadata.publish(clearA, nil); err != nil {
		return err
	}
	if err := runReclaimHook(reclaimQueueClearFirst); err != nil {
		return err
	}
	clearB := clearA
	clearB.Generation++
	if err := e.log.metadata.publish(clearB, nil); err != nil {
		return err
	}
	if err := runReclaimHook(reclaimQueueClearSecond); err != nil {
		return err
	}
	e.writeMu.Lock()
	e.log.state.Generation = clearB.Generation
	e.writeMu.Unlock()
	return e.finishCheckpointRetirements()
}

func addCheckpointRetirements(next *metadataSlot, old [2]metadataSlot) error {
	kept := [2][16]byte{next.CheckpointID, next.PreviousCheckpointID}
	for i := range old {
		for _, previous := range []bool{false, true} {
			slot := old[i]
			if previous {
				slot.CheckpointID, slot.CheckpointTail, slot.CheckpointHash = slot.PreviousCheckpointID, slot.PreviousCheckpointTail, slot.PreviousCheckpointHash
			}
			if slot.CheckpointID == ([16]byte{}) || slot.CheckpointID == kept[0] || slot.CheckpointID == kept[1] {
				continue
			}
			id := fileID(slot.CheckpointID)
			found := false
			for j := 0; j < int(next.RetiredCheckpointCount); j++ {
				if next.RetiredCheckpoints[j].ID == id {
					if next.RetiredCheckpoints[j].Hash != slot.CheckpointHash {
						return ErrCorrupt
					}
					found = true
				}
			}
			if !found {
				if next.RetiredCheckpointCount >= maxRetiredCheckpoints {
					return ErrBounds
				}
				next.RetiredCheckpoints[next.RetiredCheckpointCount] = retiredCheckpointDescriptor{ID: id, Hash: slot.CheckpointHash}
				next.RetiredCheckpointCount++
			}
		}
	}
	return nil
}

func (e *Engine) finishCheckpointRetirements() error {
	slot := e.log.metadata.slot
	for i := 0; i < int(slot.RetiredCheckpointCount); i++ {
		retired := slot.RetiredCheckpoints[i]
		path := e.log.dir + string(os.PathSeparator) + checkpointFileName(retired.ID)
		opened, err := authenticateCheckpointPath(path, retired.ID, slot.LogID, retired.Hash, e.authKey)
		if errors.Is(err, os.ErrNotExist) {
			if err = reclaimSyncDir(e.log.dir); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if reclaimBeforeCheckpointRemove != nil {
			reclaimBeforeCheckpointRemove(path)
		}
		if err = removeExactPublishedPath(opened, path, e.log.dir); err != nil {
			return err
		}
	}
	if slot.RetiredCheckpointCount == 0 {
		return nil
	}
	clearA := slot
	clearA.Generation++
	clearA.RetiredCheckpointCount = 0
	clear(clearA.RetiredCheckpoints[:])
	if err := e.log.metadata.publish(clearA, nil); err != nil {
		return err
	}
	clearB := clearA
	clearB.Generation++
	if err := e.log.metadata.publish(clearB, nil); err != nil {
		return err
	}
	e.log.state.Generation = clearB.Generation
	return nil
}
