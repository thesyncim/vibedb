package seglog

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

const metadataName = "META"

type metadataStore struct {
	dir          string
	file         *os.File
	key          [32]byte
	slot         metadataSlot
	slotIndex    uint8
	slotBuffer   [metadataSlotBytes]byte
	recordBuf    [catalogRecordBytes]byte
	needsHealing bool
}

var metadataPhysicalFile = metadataAllocateThrough

func preallocateMetadataRunway(file *os.File, through uint64) error {
	if through >= 1<<32 {
		return ErrBounds
	}
	if err := metadataPhysicalFile(file, through); err != nil {
		return err
	}
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	allocated, ok := allocatedFileBytes(stat)
	// META has one intentional sparse alignment hole between slot 1 and the
	// catalog. Apart from that bounded hole, physical blocks must cover the
	// requested absolute runway; this detects a no-op second replenishment.
	// Creation verifies before the two slots are written, so the portable
	// allowance includes the whole fixed 16 KiB prefix. Subsequent checks still
	// detect a missing runway because this allowance never grows.
	const boundedHole = metadataCatalogStart
	if !ok || allocated > ^uint64(0)-boundedHole || allocated+boundedHole < through {
		return ErrBounds
	}
	return nil
}

func stateFromMetadata(slot metadataSlot, sealed []SegmentMeta) metadataState {
	segments := make([]SegmentMeta, len(sealed), len(sealed)+1)
	copy(segments, sealed)
	if slot.HasPending {
		segments = append(segments, SegmentMeta{ID: slot.Pending.ID, Generation: slot.Pending.Generation, Bytes: slot.Pending.Bytes, Records: slot.Pending.Records, PreviousHash: slot.Pending.PreviousHash, Hash: slot.Pending.Hash, FileID: slot.Pending.FileID, State: SegmentFrozenPending})
	}
	return metadataState{Generation: slot.Generation, ActiveID: slot.Active.ID, ActiveGeneration: slot.Active.Generation, DurableSegmentID: slot.Active.ID, DurableOffset: segmentHeaderBytes, LogID: slot.LogID, ActiveFileID: slot.Active.FileID, SegmentCapacity: slot.Active.Capacity, AnchorID: slot.AnchorID, AnchorGeneration: slot.AnchorGeneration, AnchorHash: slot.AnchorHash, Reserves: slot.Reserves, Segments: segments}
}

func metadataSlotFromState(slot metadataSlot, state metadataState) metadataSlot {
	slot.Generation, slot.LogID = state.Generation, state.LogID
	slot.AnchorID, slot.AnchorGeneration, slot.AnchorHash = state.AnchorID, state.AnchorGeneration, state.AnchorHash
	slot.Active = activeDescriptor{FileID: state.ActiveFileID, ID: state.ActiveID, Generation: state.ActiveGeneration, PreviousID: state.ActiveID - 1, Capacity: state.SegmentCapacity}
	slot.Reserves = state.Reserves
	slot.HasPending, slot.Pending = false, pendingDescriptor{}
	if state.ActiveID == 1 {
		slot.Active.PreviousID = 0
	} else if len(state.Segments) != 0 {
		slot.Active.PreviousHash = state.Segments[len(state.Segments)-1].Hash
	} else {
		slot.Active.PreviousHash = state.AnchorHash
	}
	if len(state.Segments) != 0 && pendingSegment(state.Segments[len(state.Segments)-1]) {
		pending := state.Segments[len(state.Segments)-1]
		slot.HasPending = true
		slot.Pending = pendingDescriptor{FileID: pending.FileID, ID: pending.ID, Generation: pending.Generation, Bytes: pending.Bytes, Records: pending.Records, PreviousHash: pending.PreviousHash, Hash: pending.Hash}
	}
	return slot
}

func createMetadataStore(dir string, initial metadataSlot, key [32]byte) (*metadataStore, error) {
	tmp := filepath.Join(dir, ".META.tmp")
	final := filepath.Join(dir, metadataName)
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	store := &metadataStore{dir: dir, file: file, key: key, slot: initial}
	clean := true
	defer func() {
		if clean {
			_ = file.Close()
			_ = os.Remove(tmp)
		}
	}()
	if err = file.Truncate(metadataCatalogStart); err != nil {
		return nil, err
	}
	if err = preallocateMetadataRunway(file, metadataCatalogStart+catalogCheckpointHardRecords*catalogRecordBytes); err != nil {
		return nil, err
	}
	if err = marshalMetadataSlot(store.slotBuffer[:], initial, key); err != nil {
		return nil, err
	}
	if err = writeFullAt(file, store.slotBuffer[:], metadataSlot0Offset); err == nil {
		err = writeFullAt(file, store.slotBuffer[:], metadataSlot1Offset)
	}
	if err == nil {
		err = file.Sync()
	}
	if err == nil {
		err = os.Rename(tmp, final)
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		return nil, err
	}
	clean = false
	return store, nil
}

func openMetadataStore(dir string, logID [16]byte, key [32]byte) (*metadataStore, []SegmentMeta, error) {
	file, err := os.OpenFile(filepath.Join(dir, metadataName), os.O_RDWR, 0)
	if err != nil {
		return nil, nil, err
	}
	store := &metadataStore{dir: dir, file: file, key: key}
	var slots [2]metadataSlot
	var valid [2]bool
	var usedPrevious [2]bool
	var candidates [2][]SegmentMeta
	for i, off := range []int64{metadataSlot0Offset, metadataSlot1Offset} {
		if readFullAt(file, store.slotBuffer[:], off) != nil {
			continue
		}
		slot, decodeErr := unmarshalMetadataSlot(store.slotBuffer[:], key)
		if decodeErr == nil && slot.LogID == logID {
			if segments, fallback, chainErr := readCatalogBounded(dir, file, slot, key); chainErr == nil {
				slots[i], candidates[i], valid[i] = slot, segments, true
				usedPrevious[i] = fallback
			}
		}
	}
	chosen := -1
	if valid[0] {
		chosen = 0
	}
	if valid[1] && (chosen < 0 || slots[1].Generation > slots[chosen].Generation) {
		chosen = 1
	}
	if chosen < 0 {
		_ = file.Close()
		return nil, nil, ErrCorrupt
	}
	store.slot, store.slotIndex = slots[chosen], uint8(chosen)
	if usedPrevious[chosen] {
		store.slot.CheckpointID, store.slot.CheckpointTail, store.slot.CheckpointHash = store.slot.PreviousCheckpointID, store.slot.PreviousCheckpointTail, store.slot.PreviousCheckpointHash
		store.slot.PreviousCheckpointID, store.slot.PreviousCheckpointTail, store.slot.PreviousCheckpointHash = [16]byte{}, 0, [32]byte{}
		store.needsHealing = true
	}
	return store, candidates[chosen], nil
}

func readCatalogBounded(dir string, file *os.File, slot metadataSlot, key [32]byte) ([]SegmentMeta, bool, error) {
	if slot.CatalogTail < metadataCatalogStart || (slot.CatalogTail-metadataCatalogStart)%catalogRecordBytes != 0 {
		return nil, false, ErrCorrupt
	}
	segments, tail, digest, lastRecordGeneration, err := readCatalogCheckpoint(dir, slot, key)
	usedPrevious := false
	if err != nil && slot.PreviousCheckpointID != ([16]byte{}) {
		fallback := slot
		fallback.CheckpointID = slot.PreviousCheckpointID
		fallback.CheckpointTail = slot.PreviousCheckpointTail
		fallback.CheckpointHash = slot.PreviousCheckpointHash
		segments, tail, digest, lastRecordGeneration, err = readCatalogCheckpoint(dir, fallback, key)
		usedPrevious = err == nil
	}
	if err != nil {
		return nil, false, err
	}
	count := (slot.CatalogTail - tail) / catalogRecordBytes
	if count > catalogCheckpointHardRecords {
		return nil, false, ErrBounds
	}
	if cap(segments)-len(segments) < int(count) {
		grown := make([]SegmentMeta, len(segments), len(segments)+int(count))
		copy(grown, segments)
		segments = grown
	}
	lastID, lastGeneration, lastHash := slot.AnchorID, slot.AnchorGeneration, slot.AnchorHash
	if len(segments) != 0 {
		last := segments[len(segments)-1]
		lastID, lastGeneration, lastHash = last.ID, last.Generation, last.Hash
	}
	var raw [catalogRecordBytes]byte
	for tail < slot.CatalogTail {
		if err := readFullAt(file, raw[:], int64(tail)); err != nil {
			return nil, false, err
		}
		record, recordHash, err := unmarshalCatalogRecord(raw[:], key)
		if err != nil || record.PreviousTail != tail || record.PreviousHash != digest || record.Generation <= lastRecordGeneration || record.Generation > slot.Generation {
			return nil, false, ErrCorrupt
		}
		switch record.Kind {
		case catalogSeal:
			segment := record.Segment
			segment.PreviousHash = lastHash
			segment.FileID = record.FileID
			if segment.ID != lastID+1 || segment.Generation != lastGeneration+1 {
				return nil, false, ErrCorrupt
			}
			segments = append(segments, segment)
			lastID, lastGeneration, lastHash = segment.ID, segment.Generation, segment.Hash
		case catalogAnchor:
			if record.AnchorID < lastID || record.AnchorGeneration < lastGeneration {
				return nil, false, ErrCorrupt
			}
			cut := 0
			for cut < len(segments) && segments[cut].ID <= record.AnchorID {
				cut++
			}
			segments = append(segments[:0], segments[cut:]...)
			lastID, lastGeneration, lastHash = record.AnchorID, record.AnchorGeneration, record.AnchorHash
		}
		tail += catalogRecordBytes
		digest = recordHash
		lastRecordGeneration = record.Generation
	}
	if digest != slot.CatalogHash {
		return nil, false, ErrCorrupt
	}
	if slot.HasPending {
		if slot.Pending.ID != lastID+1 || slot.Pending.Generation != lastGeneration+1 || slot.Pending.PreviousHash != lastHash || slot.Active.PreviousID != slot.Pending.ID || slot.Active.PreviousHash != slot.Pending.Hash {
			return nil, false, ErrCorrupt
		}
	} else if slot.Active.PreviousID != lastID || slot.Active.Generation != lastGeneration+1 || slot.Active.PreviousHash != lastHash {
		return nil, false, ErrCorrupt
	}
	owned := make(map[fileID]struct{}, len(segments)+4)
	for i := range segments {
		if _, duplicate := owned[segments[i].FileID]; duplicate {
			return nil, false, ErrCorrupt
		}
		owned[segments[i].FileID] = struct{}{}
	}
	for _, id := range []fileID{slot.Active.FileID, slot.Pending.FileID, slot.Reserves[0].FileID, slot.Reserves[1].FileID} {
		if id == (fileID{}) {
			continue
		}
		if _, duplicate := owned[id]; duplicate {
			return nil, false, ErrCorrupt
		}
		owned[id] = struct{}{}
	}
	return segments, usedPrevious, nil
}

func catalogSuffixRecords(slot metadataSlot) uint64 {
	base := uint64(metadataCatalogStart)
	if slot.CheckpointID != ([16]byte{}) {
		base = slot.CheckpointTail
	}
	if slot.CatalogTail < base {
		return ^uint64(0)
	}
	return (slot.CatalogTail - base) / catalogRecordBytes
}

func (store *metadataStore) previewRecord(record catalogRecord, generation uint64) (uint64, [32]byte, error) {
	if store == nil || generation != store.slot.Generation+1 {
		return 0, [32]byte{}, ErrCorrupt
	}
	record.Generation = generation
	record.PreviousTail = store.slot.CatalogTail
	record.PreviousHash = store.slot.CatalogHash
	var encoded [catalogRecordBytes]byte
	digest, err := marshalCatalogRecord(encoded[:], record, store.key)
	return store.slot.CatalogTail + catalogRecordBytes, digest, err
}

func (store *metadataStore) publish(next metadataSlot, record *catalogRecord) error {
	if store == nil || store.file == nil || next.Generation != store.slot.Generation+1 {
		return ErrCorrupt
	}
	if record != nil {
		record.Generation = next.Generation
		record.PreviousTail = store.slot.CatalogTail
		record.PreviousHash = store.slot.CatalogHash
		digest, err := marshalCatalogRecord(store.recordBuf[:], *record, store.key)
		if err != nil {
			return err
		}
		if err = writeFullAt(store.file, store.recordBuf[:], int64(store.slot.CatalogTail)); err != nil {
			return err
		}
		next.CatalogTail = store.slot.CatalogTail + catalogRecordBytes
		next.CatalogHash = digest
	} else if next.CatalogTail != store.slot.CatalogTail || next.CatalogHash != store.slot.CatalogHash {
		return ErrCorrupt
	}
	if err := marshalMetadataSlot(store.slotBuffer[:], next, store.key); err != nil {
		return err
	}
	nextIndex := store.slotIndex ^ 1
	offset := int64(metadataSlot0Offset)
	if nextIndex != 0 {
		offset = metadataSlot1Offset
	}
	if err := writeFullAt(store.file, store.slotBuffer[:], offset); err != nil {
		return err
	}
	if err := store.file.Sync(); err != nil {
		return err
	}
	store.slot, store.slotIndex = next, nextIndex
	return nil
}

func (store *metadataStore) Close() error {
	if store == nil || store.file == nil {
		return nil
	}
	err := store.file.Close()
	store.file = nil
	return err
}

func writeFullAt(file *os.File, data []byte, offset int64) error {
	for len(data) != 0 {
		n, err := file.WriteAt(data, offset)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data, offset = data[n:], offset+int64(n)
	}
	return nil
}

func closeMetadataAndFile(store *metadataStore, file *os.File) error {
	var first, second error
	if store != nil {
		first = store.Close()
	}
	if file != nil {
		second = file.Close()
	}
	return errors.Join(first, second)
}
