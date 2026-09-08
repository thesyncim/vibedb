package seglog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var reclaimMinSegments = 8
var reclaimMaxSegments = maxRetiredSegments
var reclaimRemove = os.Remove
var reclaimSyncDir = syncDir
var reclaimBeforeRemove func(string)
var reclaimBeforeCheckpointRemove func(string)

type reclaimRequest struct {
	result chan error
}

// reclaimTicket is the immutable hand-off between the short authenticated
// logical cut and the physical cleaner. The cleaner never derives retired
// identities from mutable segment state after this hand-off.
type reclaimTicket struct {
	slot           metadataSlot
	retired        []retiredDescriptor
	done           chan error
	checkpointOnly bool
}

type reclaimPublishPhase uint8

const (
	reclaimCheckpointAPublished reclaimPublishPhase = iota + 1
	reclaimPreparedPublished
	reclaimCheckpointBPublished
	reclaimDurablePublished
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
	case e.reclaimRequests <- reclaimRequest{result: result}:
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

func (e *Engine) beginReclaim() (*reclaimTicket, error) {
	e.writeMu.Lock()
	if e.closing {
		e.writeMu.Unlock()
		return nil, os.ErrClosed
	}
	if e.log != nil && e.log.metadata != nil && (e.log.metadata.slot.ReclaimPhase != reclaimNone || e.log.metadata.slot.RetiredCheckpointCount != 0) {
		if e.cleanupTicket != nil || e.maintenanceBusy || e.sealPending || e.log.usable() != nil || e.log.metadata.slot.HasPending {
			e.writeMu.Unlock()
			return nil, ErrBounds
		}
		slot := e.log.metadata.slot
		if slot.ReclaimPhase == reclaimPrepared {
			e.writeMu.Unlock()
			if err := e.resumeReclaim(); err != nil {
				return nil, err
			}
			// resumeReclaim completes the interrupted cut, including its
			// physical cleanup, on this serial lane. Do not start a fresh
			// threshold scan after the recovered prefix has already been
			// removed; the caller's request is complete.
			e.writeMu.Lock()
			completed := e.log.metadata.slot.ReclaimPhase == reclaimNone && e.log.metadata.slot.RetiredCheckpointCount == 0
			e.writeMu.Unlock()
			if completed {
				return nil, nil
			}
			return e.beginReclaim()
		}
		if slot.ReclaimPhase == reclaimNone {
			// Checkpoint-file retirement is already an authenticated maintenance
			// ticket. It shares the cleaner with segment retirement and must not
			// be interpreted as a segment cut.
			ticket := &reclaimTicket{
				slot:           slot,
				done:           make(chan error, 1),
				checkpointOnly: true,
			}
			e.cleanupTicket = ticket
			e.writeMu.Unlock()
			if err := e.enqueueCleanupTicket(ticket); err != nil {
				e.writeMu.Lock()
				if e.cleanupTicket == ticket {
					e.cleanupTicket = nil
				}
				e.writeMu.Unlock()
				return nil, err
			}
			return ticket, nil
		}
		if slot.RetiredReserveMask != 0 {
			e.writeMu.Unlock()
			return nil, fmt.Errorf("%w: retired reserve conversion", ErrCorrupt)
		}
		// A crash can leave the authenticated DURABLE slot published before
		// its in-memory cut was installed. Reconstruct that cut once before
		// handing the same immutable ticket to the cleaner.
		e.writeMu.Unlock()
		if err := e.installDurableReclaimState(nil, 0, nil); err != nil {
			return nil, err
		}
		e.writeMu.Lock()
		slot = e.log.metadata.slot
		ticket := &reclaimTicket{slot: slot, retired: append([]retiredDescriptor(nil), slot.Retired[:slot.RetiredCount]...), done: make(chan error, 1), checkpointOnly: slot.ReclaimPhase == reclaimNone}
		e.cleanupTicket = ticket
		e.writeMu.Unlock()
		if err := e.enqueueCleanupTicket(ticket); err != nil {
			e.writeMu.Lock()
			if e.cleanupTicket == ticket {
				e.cleanupTicket = nil
			}
			e.writeMu.Unlock()
			return nil, err
		}
		return ticket, nil
	}
	if e.maintenanceBusy || e.sealPending || e.log == nil || e.log.usable() != nil || e.log.metadata == nil || e.log.metadata.needsHealing || e.log.metadata.slot.HasPending || e.log.metadata.slot.ReclaimPhase != reclaimNone {
		e.writeMu.Unlock()
		return nil, ErrBounds
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
			return nil, ErrBounds
		}
		usedBytes += segment.Bytes
		cut++
	}
	if !reclaimThresholdReached(cut, limit, usedBytes, e.log.state.SegmentCapacity) {
		e.writeMu.Unlock()
		return nil, ErrBounds
	}
	e.maintenanceBusy = true
	baseSlot := e.log.metadata.slot
	removed := e.log.state.Segments[:cut]
	retained := e.log.state.Segments[cut:]
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
		return nil, fmt.Errorf("reclaim preview anchor: %w", err)
	}
	checkpointAID := derivedCheckpointID(e.authKey, baseSlot.LogID, baseSlot.Generation+1, tail, catalogHash, anchor.ID, anchor.Generation, anchor.Hash, checkpointRoleReclaimA)
	checkpointA := catalogCheckpoint{ID: checkpointAID, LogID: baseSlot.LogID, Generation: baseSlot.Generation + 1, Tail: tail, CatalogHash: catalogHash, AnchorID: anchor.ID, AnchorGeneration: anchor.Generation, AnchorHash: anchor.Hash, Segments: retained, BaseSequence: e.sealedSequence, GroupIDs: e.sealedSummaryOrder, GroupSummaries: e.sealedSummaries}
	checkpointAHash, err := catalogCheckpointWriter(e.log.dir, checkpointA, e.authKey)
	if err != nil {
		return nil, fmt.Errorf("reclaim checkpoint A: %w", err)
	}
	if err = runReclaimHook(reclaimCheckpointAPublished); err != nil {
		return nil, err
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
	if err = e.log.metadata.publish(cutA, &record); err != nil {
		cleanupUnpublishedCheckpoint(e.log.dir, checkpointA, checkpointAHash, e.authKey)
		return nil, fmt.Errorf("reclaim cut A: %w", err)
	}
	if err = runReclaimHook(reclaimPreparedPublished); err != nil {
		return nil, err
	}
	cutA = e.log.metadata.slot
	checkpointBID := derivedCheckpointID(e.authKey, baseSlot.LogID, cutA.Generation+1, tail, catalogHash, anchor.ID, anchor.Generation, anchor.Hash, checkpointRoleReclaimB)
	checkpointB := checkpointA
	checkpointB.ID, checkpointB.Generation = checkpointBID, cutA.Generation+1
	checkpointBHash, err := catalogCheckpointWriter(e.log.dir, checkpointB, e.authKey)
	if err != nil {
		return nil, fmt.Errorf("reclaim checkpoint B: %w", err)
	}
	if err = runReclaimHook(reclaimCheckpointBPublished); err != nil {
		return nil, err
	}
	cutB := cutA
	cutB.Generation++
	cutB.ReclaimPhase = reclaimDurable
	cutB.PreviousCheckpointID, cutB.PreviousCheckpointTail, cutB.PreviousCheckpointHash = cutB.CheckpointID, cutB.CheckpointTail, cutB.CheckpointHash
	cutB.CheckpointID, cutB.CheckpointTail, cutB.CheckpointHash = [16]byte(checkpointBID), tail, checkpointBHash
	if validateErr := validateMetadataSlot(cutB); validateErr != nil {
		cleanupUnpublishedCheckpoint(e.log.dir, checkpointB, checkpointBHash, e.authKey)
		return nil, fmt.Errorf("reclaim cut B slot: %w", validateErr)
	}
	if err = e.log.metadata.publish(cutB, nil); err != nil {
		cleanupUnpublishedCheckpoint(e.log.dir, checkpointB, checkpointBHash, e.authKey)
		return nil, fmt.Errorf("reclaim cut B: %w", err)
	}
	if err = runReclaimHook(reclaimDurablePublished); err != nil {
		return nil, err
	}
	nextSegments := retained
	if compacted != nil {
		nextSegments = compacted
	}
	if err = e.installDurableReclaimState(nextSegments, cut, compactedFences); err != nil {
		return nil, fmt.Errorf("install durable reclaim: %w", err)
	}
	e.writeMu.Lock()
	slot := e.log.metadata.slot
	ticket := &reclaimTicket{slot: slot, retired: append([]retiredDescriptor(nil), slot.Retired[:slot.RetiredCount]...), done: make(chan error, 1)}
	e.maintenanceBusy = false
	e.cleanupTicket = ticket
	e.writeMu.Unlock()
	if err := e.enqueueCleanupTicket(ticket); err != nil {
		e.writeMu.Lock()
		if e.cleanupTicket == ticket {
			e.cleanupTicket = nil
		}
		e.writeMu.Unlock()
		return nil, err
	}
	return ticket, nil
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
			cleanupUnpublishedCheckpoint(e.log.dir, checkpoint, checkpointHash, e.authKey)
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
	if slot.RetiredReserveMask != 0 {
		// The current cleaner never reuses a retired identity. A persisted
		// conversion marker belongs to the retired implementation and cannot
		// be safely completed by this format-preserving path.
		return fmt.Errorf("%w: retired reserve conversion", ErrCorrupt)
	}
	if err := e.installDurableReclaimState(nil, 0, nil); err != nil {
		return err
	}
	ticket := &reclaimTicket{slot: slot, retired: append([]retiredDescriptor(nil), slot.Retired[:slot.RetiredCount]...), done: make(chan error, 1)}
	return e.finishRetiredFiles(ticket)
}

// installDurableReclaimState publishes the in-memory consequence of an
// authenticated durable cut. It deliberately does not reuse or unlink any
// retired file; those operations belong to the cleaner ticket.
func (e *Engine) installDurableReclaimState(replacement []SegmentMeta, removedCount int, compactedFences []uint64) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if e.log == nil || e.log.metadata == nil {
		return ErrCorrupt
	}
	slot := e.log.metadata.slot
	if slot.ReclaimPhase != reclaimDurable || slot.RetiredCount == 0 {
		return ErrCorrupt
	}
	if replacement == nil {
		cut := 0
		for cut < len(e.log.state.Segments) && e.log.state.Segments[cut].ID <= slot.AnchorID {
			cut++
		}
		if cut != 0 {
			e.log.state.Segments = e.log.state.Segments[cut:]
			e.reclaimAfter = e.reclaimAfter[cut:]
		}
	} else {
		if removedCount < 0 || removedCount > len(e.reclaimAfter) {
			return ErrCorrupt
		}
		retainedFences := e.reclaimAfter[removedCount:]
		if compactedFences != nil {
			copy(compactedFences, retainedFences)
			retainedFences = compactedFences
		}
		e.log.state.Segments = replacement
		e.reclaimAfter = retainedFences
	}
	e.log.state.AnchorID, e.log.state.AnchorGeneration, e.log.state.AnchorHash = slot.AnchorID, slot.AnchorGeneration, slot.AnchorHash
	e.log.state.Generation = slot.Generation

	e.readerMu.Lock()
	e.ensureReaderCondLocked()
	for i, reader := range e.readers {
		if reader == nil || reader.file == nil || reader.id > slot.AnchorID {
			continue
		}
		if err := e.detachReaderLocked(reader); err != nil {
			e.readerMu.Unlock()
			return err
		}
		// Keep the fixed reader-slot shape for existing callers; a pinned old
		// record remains in detachedReaders until its final lease closes.
		e.readers[i] = &segmentReader{}
	}
	e.readerMu.Unlock()
	return nil
}

func (e *Engine) enqueueCleanupTicket(ticket *reclaimTicket) error {
	if ticket == nil || e.cleanupRequests == nil || e.cleanupStop == nil {
		return ErrRaftState
	}
	select {
	case e.cleanupRequests <- ticket:
		return nil
	case <-e.cleanupStop:
		return os.ErrClosed
	}
}

func (e *Engine) runCleanupWorker() {
	defer close(e.cleanupDone)
	for {
		select {
		case ticket := <-e.cleanupRequests:
			e.processCleanupTicket(ticket)
		case <-e.cleanupStop:
			for {
				select {
				case ticket := <-e.cleanupRequests:
					e.processCleanupTicket(ticket)
				default:
					return
				}
			}
		}
	}
}

func (e *Engine) processCleanupTicket(ticket *reclaimTicket) {
	if ticket == nil {
		return
	}
	var err error
	if ticket.checkpointOnly {
		err = e.finishCheckpointRetirements()
	} else {
		err = e.finishRetiredFiles(ticket)
	}
	e.writeMu.Lock()
	if e.cleanupTicket == ticket {
		e.cleanupTicket = nil
		e.cleanupErr = err
	}
	if err != nil && errors.Is(err, ErrCorrupt) && e.log != nil {
		e.log.poison(err)
	}
	e.writeMu.Unlock()
	ticket.done <- err
}

func (e *Engine) waitRetiredReaders(anchor uint64) {
	e.readerMu.Lock()
	e.ensureReaderCondLocked()
	for {
		pending := false
		for _, reader := range e.detachedReaders {
			if reader != nil && reader.id <= anchor && reader.leases.Load() != 0 {
				pending = true
				break
			}
		}
		if !pending {
			break
		}
		e.readerCond.Wait()
	}
	e.readerMu.Unlock()
}

// removeExactPublishedPathNoSync verifies and removes one authenticated path
// without syncing the directory. A caller batches the final directory sync
// after all paths in one ticket have been removed.
func removeExactPublishedPathNoSync(opened os.FileInfo, path string) error {
	check, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if opened == nil || !os.SameFile(opened, check) {
		return ErrCorrupt
	}
	if err = reclaimRemove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err = os.Lstat(path); err == nil {
		return ErrCorrupt
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (e *Engine) finishRetiredFiles(ticket *reclaimTicket) error {
	if ticket == nil || e.log == nil || e.log.metadata == nil {
		return ErrCorrupt
	}
	e.waitRetiredReaders(ticket.slot.AnchorID)

	e.metadataMu.Lock()
	e.writeMu.Lock()
	if err := e.log.usable(); err != nil {
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
		return err
	}
	slot := e.log.metadata.slot
	if slot.ReclaimPhase != reclaimDurable || slot.RetiredCount == 0 || int(slot.RetiredCount) != len(ticket.retired) {
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
		return ErrCorrupt
	}
	for i, retired := range ticket.retired {
		if slot.Retired[i] != retired {
			e.writeMu.Unlock()
			e.metadataMu.Unlock()
			return ErrCorrupt
		}
	}
	dir, logID, capacity := e.log.dir, slot.LogID, e.log.state.SegmentCapacity
	e.writeMu.Unlock()
	e.metadataMu.Unlock()

	for _, retired := range ticket.retired {
		path := segmentPath(dir, retired.FileID)
		file, openErr := os.Open(path)
		if errors.Is(openErr, os.ErrNotExist) {
			continue
		}
		if openErr != nil {
			return openErr
		}
		opened, statErr := file.Stat()
		if statErr == nil {
			var derived SegmentMeta
			derived, _, statErr = readUnpublishedSealedFile(file, retired.FileID, capacity, logID, retired.ID-1, retired.PreviousHash, e.authKey)
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
		if err := removeExactPublishedPathNoSync(opened, path); err != nil {
			return err
		}
		if err := runReclaimHook(reclaimFileRemoved); err != nil {
			return err
		}
	}
	if err := reclaimSyncDir(dir); err != nil {
		return err
	}

	// The ticket owns only the segment retirement. Start the paired clear from
	// the latest slot so rotations and checkpoint-queue additions survive.
	e.metadataMu.Lock()
	e.writeMu.Lock()
	latest := e.log.metadata.slot
	if latest.ReclaimPhase != reclaimDurable || latest.RetiredCount != uint8(len(ticket.retired)) {
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
		return ErrCorrupt
	}
	for i, retired := range ticket.retired {
		if latest.Retired[i] != retired {
			e.writeMu.Unlock()
			e.metadataMu.Unlock()
			return ErrCorrupt
		}
	}
	clearA := latest
	clearA.Generation++
	clearA.ReclaimPhase, clearA.RetiredCount, clearA.RetiredReserveMask = reclaimNone, 0, 0
	clear(clearA.Retired[:])
	if err := e.log.metadata.publish(clearA, nil); err != nil {
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
		return err
	}
	if err := runReclaimHook(reclaimQueueClearFirst); err != nil {
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
		return err
	}
	clearB := e.log.metadata.slot
	clearB.Generation++
	if err := e.log.metadata.publish(clearB, nil); err != nil {
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
		return err
	}
	if err := runReclaimHook(reclaimQueueClearSecond); err != nil {
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
		return err
	}
	e.log.state.Generation = clearB.Generation
	e.writeMu.Unlock()
	e.metadataMu.Unlock()
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

// checkpointRetirementBankState reports whether a queued checkpoint is still
// named by either metadata bank. An invalid bank is a healing obligation: its
// bytes are not trusted for deletion, but the queued file must remain until an
// authenticated clone replaces that bank.
func checkpointRetirementBankState(store *metadataStore, retired retiredCheckpointDescriptor) (referenced, needsHealing bool, err error) {
	if store == nil || store.slotIndex >= uint8(len(store.bankSlots)) || !store.bankUsable[store.slotIndex] {
		return false, false, ErrCorrupt
	}
	for bank, slot := range store.bankSlots {
		if !store.bankUsable[bank] {
			needsHealing = true
			continue
		}
		for _, ref := range checkpointRefs(slot) {
			if ref.id != retired.ID {
				continue
			}
			if ref.hash != retired.Hash {
				return false, false, ErrCorrupt
			}
			referenced = true
		}
	}
	return referenced, needsHealing, nil
}

func checkpointRetirementInSlot(slot metadataSlot, id fileID) (retiredCheckpointDescriptor, bool) {
	for i := 0; i < int(slot.RetiredCheckpointCount); i++ {
		if slot.RetiredCheckpoints[i].ID == id {
			return slot.RetiredCheckpoints[i], true
		}
	}
	return retiredCheckpointDescriptor{}, false
}

// finishCheckpointRetirements authenticates and removes only the exact queue
// captured from a serialized slot. It first heals/converges both metadata
// banks, then subtracts the completed IDs from the latest slot so intervening
// checkpoint publications cannot be lost.
func (e *Engine) finishCheckpointRetirements() error {
	if e == nil || e.log == nil || e.log.metadata == nil {
		return ErrCorrupt
	}

	// A queued ID may still be present in the other fallback bank when opening
	// an older slot. Publish at most two authenticated clones to converge both
	// banks before any unlink is attempted.
	for attempt := 0; attempt < len(e.log.metadata.bankSlots); attempt++ {
		e.metadataMu.Lock()
		e.writeMu.Lock()
		if err := e.log.usable(); err != nil {
			e.writeMu.Unlock()
			e.metadataMu.Unlock()
			return err
		}
		slot := e.log.metadata.slot
		if slot.RetiredCheckpointCount == 0 {
			e.writeMu.Unlock()
			e.metadataMu.Unlock()
			return nil
		}
		needsClone := false
		for i := 0; i < int(slot.RetiredCheckpointCount); i++ {
			referenced, needsHealing, err := checkpointRetirementBankState(e.log.metadata, slot.RetiredCheckpoints[i])
			if err != nil {
				e.writeMu.Unlock()
				e.metadataMu.Unlock()
				return err
			}
			needsClone = needsClone || referenced || needsHealing
		}
		if !needsClone {
			ticket := make([]retiredCheckpointDescriptor, slot.RetiredCheckpointCount)
			copy(ticket, slot.RetiredCheckpoints[:slot.RetiredCheckpointCount])
			e.writeMu.Unlock()
			e.metadataMu.Unlock()
			return e.removeCheckpointRetirementTicket(ticket)
		}
		next := slot
		next.Generation++
		if err := e.log.metadata.publish(next, nil); err != nil {
			e.writeMu.Unlock()
			e.metadataMu.Unlock()
			return err
		}
		e.log.state.Generation = next.Generation
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
	}
	return ErrBounds
}

func (e *Engine) removeCheckpointRetirementTicket(ticket []retiredCheckpointDescriptor) error {
	completed := make(map[fileID][32]byte, len(ticket))
	for _, retired := range ticket {
		if retired.ID == (fileID{}) || retired.Hash == ([32]byte{}) {
			return ErrCorrupt
		}
		// Metadata publication is the serialization boundary for the namespace
		// claim. Recheck it immediately before authentication/unlink in case an
		// ordinary checkpoint advanced while this ticket was doing I/O.
		e.metadataMu.Lock()
		e.writeMu.Lock()
		current := e.log.metadata.slot
		referenced, needsHealing, err := checkpointRetirementBankState(e.log.metadata, retired)
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
		if err != nil {
			return err
		}
		if referenced || needsHealing {
			return ErrBounds
		}
		path := filepath.Join(e.log.dir, checkpointFileName(retired.ID))
		opened, err := authenticateCheckpointPath(path, retired.ID, current.LogID, retired.Hash, e.authKey)
		if errors.Is(err, os.ErrNotExist) {
			if err = reclaimSyncDir(e.log.dir); err != nil {
				return err
			}
			completed[retired.ID] = retired.Hash
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
		completed[retired.ID] = retired.Hash
	}

	// Merge the exact completed set into the latest slot. New queue entries
	// appended by an intervening rotation remain in place and are carried by
	// both paired publications.
	e.metadataMu.Lock()
	e.writeMu.Lock()
	defer func() {
		e.writeMu.Unlock()
		e.metadataMu.Unlock()
	}()
	latest := e.log.metadata.slot
	if latest.RetiredCheckpointCount == 0 {
		return ErrCorrupt
	}
	var remaining [maxRetiredCheckpoints]retiredCheckpointDescriptor
	remainingCount := uint8(0)
	for i := 0; i < int(latest.RetiredCheckpointCount); i++ {
		retired := latest.RetiredCheckpoints[i]
		if doneHash, done := completed[retired.ID]; done {
			if doneHash != retired.Hash {
				return ErrCorrupt
			}
			continue
		}
		remaining[remainingCount] = retired
		remainingCount++
	}
	for id := range completed {
		if _, found := checkpointRetirementInSlot(latest, id); !found {
			return ErrCorrupt
		}
	}
	clearA := latest
	clearA.Generation++
	clearA.RetiredCheckpointCount = remainingCount
	clear(clearA.RetiredCheckpoints[:])
	copy(clearA.RetiredCheckpoints[:], remaining[:remainingCount])
	if err := e.log.metadata.publish(clearA, nil); err != nil {
		return err
	}
	clearB := e.log.metadata.slot
	clearB.Generation++
	if err := e.log.metadata.publish(clearB, nil); err != nil {
		return err
	}
	e.log.state.Generation = clearB.Generation
	return nil
}
