package seglog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func metadataTestKey() [32]byte { return sha256.Sum256([]byte("seglog canonical metadata test key")) }

func metadataTestSlot() metadataSlot {
	return metadataSlot{
		Generation:  9,
		LogID:       [16]byte{1},
		CatalogTail: metadataCatalogStart,
		AnchorID:    2, AnchorGeneration: 2, AnchorHash: [32]byte{2},
		Active: activeDescriptor{
			FileID:       fileID{2},
			ID:           4,
			Generation:   4,
			PreviousID:   3,
			PreviousHash: [32]byte{3},
			Capacity:     32 << 20,
		},
		Pending: pendingDescriptor{
			FileID:       fileID{3},
			ID:           3,
			Generation:   3,
			Bytes:        segmentHeaderBytes,
			PreviousHash: [32]byte{2},
			Hash:         [32]byte{3},
		},
		HasPending: true,
		Reserves: [2]reserveDescriptor{
			{FileID: fileID{4}, Capacity: 32 << 20, Ready: true},
			{FileID: fileID{5}, Capacity: 32 << 20, Ready: true},
		},
	}
}

func TestMetadataSlotRoundTripAndCanonicalPadding(t *testing.T) {
	key, want := metadataTestKey(), metadataTestSlot()
	buf := make([]byte, metadataSlotBytes)
	if err := marshalMetadataSlot(buf, want, key); err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalMetadataSlot(buf, key)
	if err != nil || got != want {
		t.Fatalf("slot=%+v err=%v", got, err)
	}
	for _, offset := range []int{8, 440, metadataSlotMACOffset, metadataSlotCRCOffset} {
		bad := append([]byte(nil), buf...)
		bad[offset] ^= 1
		if _, err = unmarshalMetadataSlot(bad, key); err == nil {
			t.Fatalf("corruption at %d accepted", offset)
		}
	}
	wrong := key
	wrong[0] ^= 1
	if _, err = unmarshalMetadataSlot(buf, wrong); err == nil {
		t.Fatal("wrong metadata key accepted")
	}
}

func TestMetadataSlotRejectsNoncanonicalReserveAndDuplicateOwnership(t *testing.T) {
	key, slot := metadataTestKey(), metadataTestSlot()
	buf := make([]byte, metadataSlotBytes)
	if err := marshalMetadataSlot(buf, slot, key); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte){
		"not ready with identity": func(b []byte) { b[376+24] = 0 },
		"reserved state":          func(b []byte) { b[376+24] = 2 },
		"reserved bytes":          func(b []byte) { b[376+25] = 1 },
		"duplicate active":        func(b []byte) { copy(b[376:392], b[184:200]) },
		"duplicate reserve":       func(b []byte) { copy(b[408:424], b[376:392]) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := append([]byte(nil), buf...)
			mutate(bad)
			remacMetadataSlot(bad, key)
			if _, err := unmarshalMetadataSlot(bad, key); err == nil {
				t.Fatal("noncanonical ownership accepted")
			}
		})
	}
}

func TestCatalogRecordExact192BytesAndHashChain(t *testing.T) {
	key := metadataTestKey()
	segment := SegmentMeta{ID: 7, Generation: 7, Bytes: 4096, Records: 2, IndexOffset: 2048, IndexBytes: 4096 - 2048 - segmentFooterBytes, Hash: [32]byte{7}, State: SegmentSealed}
	record := catalogRecord{Kind: catalogSeal, Generation: 11, PreviousTail: metadataCatalogStart, Segment: segment, FileID: fileID{9}}
	buf := make([]byte, catalogRecordBytes)
	digest, err := marshalCatalogRecord(buf, record, key)
	if err != nil {
		t.Fatal(err)
	}
	got, gotDigest, err := unmarshalCatalogRecord(buf, key)
	if err != nil || got != record || gotDigest != digest {
		t.Fatalf("record=%+v digest=%x err=%v", got, gotDigest, err)
	}
	for cut := 0; cut < catalogRecordBytes; cut++ {
		if _, _, err = unmarshalCatalogRecord(buf[:cut], key); err == nil {
			t.Fatalf("partial record cut=%d accepted", cut)
		}
	}
	bad := append([]byte(nil), buf...)
	bad[28] ^= 1
	if _, _, err = unmarshalCatalogRecord(bad, key); err == nil {
		t.Fatal("catalog chain corruption accepted")
	}
}

func TestCatalogCheckpointBoundsOpenToSuffix(t *testing.T) {
	dir := t.TempDir()
	activeFile := fileID{1}
	initial := metadataSlot{Generation: 1, LogID: testLogID, CatalogTail: metadataCatalogStart, Active: activeDescriptor{FileID: activeFile, ID: 1, Generation: 1, Capacity: 1 << 20}}
	store, err := createMetadataStore(dir, initial, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	segments := make([]SegmentMeta, 0, 20)
	firstCheckpointID := fileID{0xc1}
	previousHash := [32]byte{}
	for id := uint64(1); id <= 20; id++ {
		hash := sha256.Sum256([]byte{byte(id)})
		segment := SegmentMeta{ID: id, Generation: id, Bytes: segmentHeaderBytes + sealedIndexHeaderBytes + segmentFooterBytes, Records: 1, IndexOffset: segmentHeaderBytes, IndexBytes: sealedIndexHeaderBytes, PreviousHash: previousHash, Hash: hash, FileID: fileID{byte(id + 1)}, State: SegmentSealed}
		recordSegment := segment
		recordSegment.PreviousHash, recordSegment.FileID = [32]byte{}, fileID{}
		next := store.slot
		next.Generation++
		next.Active = activeDescriptor{FileID: activeFile, ID: id + 1, Generation: id + 1, PreviousID: id, PreviousHash: hash, Capacity: 1 << 20}
		if err = store.publish(next, &catalogRecord{Kind: catalogSeal, Segment: recordSegment, FileID: segment.FileID}); err != nil {
			t.Fatal(err)
		}
		segments = append(segments, segment)
		previousHash = hash
		if id == 16 {
			checkpointID := firstCheckpointID
			checkpointHash, writeErr := writeCatalogCheckpoint(dir, catalogCheckpoint{ID: checkpointID, LogID: testLogID, Generation: store.slot.Generation, Tail: store.slot.CatalogTail, CatalogHash: store.slot.CatalogHash, Segments: segments}, testAuthKey)
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			checkpointSlot := store.slot
			checkpointSlot.Generation++
			checkpointSlot.CheckpointID = [16]byte(checkpointID)
			checkpointSlot.CheckpointTail = checkpointSlot.CatalogTail
			checkpointSlot.CheckpointHash = checkpointHash
			if err = store.publish(checkpointSlot, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	alias := store.slot
	alias.Active.FileID = segments[0].FileID
	if _, _, _, aliasErr := readCatalogBounded(dir, store.file, alias, testAuthKey); !errors.Is(aliasErr, ErrCorrupt) {
		t.Fatalf("sealed/active FileID alias accepted: %v", aliasErr)
	}
	secondCheckpointID := fileID{0xc2}
	secondHash, err := writeCatalogCheckpoint(dir, catalogCheckpoint{ID: secondCheckpointID, LogID: testLogID, Generation: store.slot.Generation, Tail: store.slot.CatalogTail, CatalogHash: store.slot.CatalogHash, Segments: segments}, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	secondSlot := store.slot
	secondSlot.Generation++
	secondSlot.PreviousCheckpointID, secondSlot.PreviousCheckpointTail, secondSlot.PreviousCheckpointHash = secondSlot.CheckpointID, secondSlot.CheckpointTail, secondSlot.CheckpointHash
	secondSlot.CheckpointID, secondSlot.CheckpointTail, secondSlot.CheckpointHash = [16]byte(secondCheckpointID), secondSlot.CatalogTail, secondHash
	if err = store.publish(secondSlot, nil); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	meta, err := os.OpenFile(filepath.Join(dir, metadataName), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = meta.WriteAt([]byte{0xff}, metadataCatalogStart); err != nil {
		t.Fatal(err)
	}
	if err = meta.Close(); err != nil {
		t.Fatal(err)
	}
	currentCheckpoint := filepath.Join(dir, checkpointFileName(secondCheckpointID))
	checkpoint, err := os.OpenFile(currentCheckpoint, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = checkpoint.WriteAt([]byte{0xff}, 20); err != nil {
		t.Fatal(err)
	}
	if err = checkpoint.Close(); err != nil {
		t.Fatal(err)
	}
	opened, got, err := openMetadataStore(dir, testLogID, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if len(got) != 20 || got[19].ID != 20 || catalogSuffixRecords(opened.slot) != 4 || !opened.needsHealing || opened.slot.CheckpointID != [16]byte(firstCheckpointID) || opened.slot.PreviousCheckpointID != ([16]byte{}) {
		t.Fatalf("segments=%d suffix=%d", len(got), catalogSuffixRecords(opened.slot))
	}
	// The physical bank was recoverable only through its previous checkpoint at
	// record 16. Ring overwrite admission must use that verified floor, not the
	// corrupt current checkpoint's record-20 cursor retained in the raw slot.
	oldRing := catalogRingRecords
	catalogRingRecords = 4
	t.Cleanup(func() { catalogRingRecords = oldRing })
	hash := sha256.Sum256([]byte{21})
	segment := SegmentMeta{ID: 21, Generation: 21, Bytes: segmentHeaderBytes + sealedIndexHeaderBytes + segmentFooterBytes, Records: 1, IndexOffset: segmentHeaderBytes, IndexBytes: sealedIndexHeaderBytes, Hash: hash, State: SegmentSealed}
	next := opened.slot
	next.Generation++
	next.Active = activeDescriptor{FileID: activeFile, ID: 22, Generation: 22, PreviousID: 21, PreviousHash: hash, Capacity: 1 << 20}
	if publishErr := opened.publish(next, &catalogRecord{Kind: catalogSeal, Segment: segment, FileID: fileID{22}}); !errors.Is(publishErr, ErrBounds) {
		t.Fatalf("fallback-dependent ring overwrite = %v", publishErr)
	}
}

func TestOpenMarksNonchosenPreviousCheckpointBankForHealing(t *testing.T) {
	dir := t.TempDir()
	initial := metadataSlot{Generation: 1, LogID: testLogID, CatalogTail: metadataCatalogStart, Active: activeDescriptor{FileID: fileID{1}, ID: 1, Generation: 1, Capacity: 1 << 20}}
	store, err := createMetadataStore(dir, initial, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	segmentHash := sha256.Sum256([]byte("healing segment"))
	segment := SegmentMeta{ID: 1, Generation: 1, Bytes: segmentHeaderBytes + sealedIndexHeaderBytes + segmentFooterBytes, Records: 1, IndexOffset: segmentHeaderBytes, IndexBytes: sealedIndexHeaderBytes, Hash: segmentHash, FileID: fileID{9}, State: SegmentSealed}
	next := store.slot
	next.Generation++
	next.Active = activeDescriptor{FileID: fileID{1}, ID: 2, Generation: 2, PreviousID: 1, PreviousHash: segmentHash, Capacity: 1 << 20}
	if err = store.publish(next, &catalogRecord{Kind: catalogSeal, Segment: SegmentMeta{ID: segment.ID, Generation: segment.Generation, Bytes: segment.Bytes, Records: segment.Records, IndexOffset: segment.IndexOffset, IndexBytes: segment.IndexBytes, Hash: segment.Hash, State: SegmentSealed}, FileID: segment.FileID}); err != nil {
		t.Fatal(err)
	}
	initial = store.slot
	ids := [3]fileID{{0xa1}, {0xb2}, {0xc3}}
	var hashes [3][32]byte
	for i := range ids {
		hashes[i], err = writeCatalogCheckpoint(dir, catalogCheckpoint{ID: ids[i], LogID: testLogID, Generation: initial.Generation, Tail: initial.CatalogTail, CatalogHash: initial.CatalogHash, Segments: []SegmentMeta{segment}}, testAuthKey)
		if err != nil {
			t.Fatal(err)
		}
	}
	low := initial
	low.Generation = 10
	low.CheckpointID, low.CheckpointTail, low.CheckpointHash = [16]byte(ids[1]), initial.CatalogTail, hashes[1]
	low.PreviousCheckpointID, low.PreviousCheckpointTail, low.PreviousCheckpointHash = [16]byte(ids[0]), initial.CatalogTail, hashes[0]
	high := initial
	high.Generation = 11
	high.CheckpointID, high.CheckpointTail, high.CheckpointHash = [16]byte(ids[2]), initial.CatalogTail, hashes[2]
	var raw [metadataSlotBytes]byte
	if err = marshalMetadataSlot(raw[:], low, testAuthKey); err == nil {
		err = writeFullAt(store.file, raw[:], metadataSlot0Offset)
	}
	if err == nil {
		err = marshalMetadataSlot(raw[:], high, testAuthKey)
	}
	if err == nil {
		err = writeFullAt(store.file, raw[:], metadataSlot1Offset)
	}
	if err == nil {
		err = store.file.Sync()
	}
	if closeErr := store.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	currentLow := filepath.Join(dir, checkpointFileName(ids[1]))
	file, err := os.OpenFile(currentLow, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, 20); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	opened, _, err := openMetadataStore(dir, testLogID, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	if opened.slotIndex != 1 || opened.slot.Generation != high.Generation || !opened.needsHealing || opened.bankRecoveryFloor[0] != initial.CatalogTail {
		t.Fatalf("chosen=%d generation=%d healing=%v floors=%v", opened.slotIndex, opened.slot.Generation, opened.needsHealing, opened.bankRecoveryFloor)
	}
	repair := opened.slot
	repair.Generation++
	if err = opened.publish(repair, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, fallback, err := readCatalogBounded(dir, opened.file, opened.bankSlots[0], testAuthKey); err != nil || fallback {
		t.Fatalf("repaired bank still fallback-dependent: fallback=%v err=%v", fallback, err)
	}
	if opened.needsHealing {
		t.Fatal("nonchosen fallback bank remained degraded after rewrite")
	}
	if err = opened.Close(); err != nil {
		t.Fatal(err)
	}
	low = initial
	low.Generation = 20
	low.CheckpointID, low.CheckpointTail, low.CheckpointHash = [16]byte(ids[2]), initial.CatalogTail, hashes[2]
	high = initial
	high.Generation = 21
	high.CheckpointID, high.CheckpointTail, high.CheckpointHash = [16]byte(ids[1]), initial.CatalogTail, hashes[1]
	high.PreviousCheckpointID, high.PreviousCheckpointTail, high.PreviousCheckpointHash = [16]byte(ids[0]), initial.CatalogTail, hashes[0]
	meta, err := os.OpenFile(filepath.Join(dir, metadataName), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = marshalMetadataSlot(raw[:], low, testAuthKey); err == nil {
		err = writeFullAt(meta, raw[:], metadataSlot0Offset)
	}
	if err == nil {
		err = marshalMetadataSlot(raw[:], high, testAuthKey)
	}
	if err == nil {
		err = writeFullAt(meta, raw[:], metadataSlot1Offset)
	}
	if err == nil {
		err = meta.Sync()
	}
	if closeErr := meta.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	opened, _, err = openMetadataStore(dir, testLogID, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if opened.slotIndex != 1 || !opened.needsHealing || opened.slot.CheckpointID != [16]byte(ids[0]) {
		t.Fatalf("chosen fallback not normalized: index=%d healing=%v slot=%+v", opened.slotIndex, opened.needsHealing, opened.slot)
	}
	firstRepair := opened.slot
	firstRepair.Generation++
	if err = opened.publish(firstRepair, nil); err != nil {
		t.Fatal(err)
	}
	if !opened.needsHealing {
		t.Fatal("one opposite-bank publish falsely healed chosen fallback bank")
	}
	secondRepair := opened.slot
	secondRepair.Generation++
	if err = opened.publish(secondRepair, nil); err != nil {
		t.Fatal(err)
	}
	if opened.needsHealing || !opened.bankHealthy[0] || !opened.bankHealthy[1] {
		t.Fatalf("two-bank healing incomplete: healing=%v healthy=%v", opened.needsHealing, opened.bankHealthy)
	}
}

func TestMetadataRingRejectsNoopPhysicalAllocation(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "meta-ring-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err = file.Truncate(metadataCatalogStart); err != nil {
		t.Fatal(err)
	}
	old := metadataPhysicalFile
	metadataPhysicalFile = func(*os.File, uint64) error { return nil }
	defer func() { metadataPhysicalFile = old }()
	if err = preallocateMetadataRing(file); !errors.Is(err, ErrBounds) {
		t.Fatalf("no-op ring allocation = %v", err)
	}
}

func TestMetadataRingRequiresBothRecoverableBanksBeforeOverwrite(t *testing.T) {
	oldRing := catalogRingRecords
	catalogRingRecords = 4
	t.Cleanup(func() { catalogRingRecords = oldRing })
	dir := t.TempDir()
	activeFile := fileID{1}
	initial := metadataSlot{Generation: 1, LogID: testLogID, CatalogTail: metadataCatalogStart, Active: activeDescriptor{FileID: activeFile, ID: 1, Generation: 1, Capacity: 1 << 20}}
	store, err := createMetadataStore(dir, initial, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	segments := make([]SegmentMeta, 0, 5)
	previousHash := [32]byte{}
	publishSegment := func(id uint64) error {
		hash := sha256.Sum256([]byte{byte(id)})
		segment := SegmentMeta{ID: id, Generation: id, Bytes: segmentHeaderBytes + sealedIndexHeaderBytes + segmentFooterBytes, Records: 1, IndexOffset: segmentHeaderBytes, IndexBytes: sealedIndexHeaderBytes, PreviousHash: previousHash, Hash: hash, FileID: fileID{byte(id + 1)}, State: SegmentSealed}
		recordSegment := segment
		recordSegment.PreviousHash, recordSegment.FileID = [32]byte{}, fileID{}
		next := store.slot
		next.Generation++
		next.Active = activeDescriptor{FileID: activeFile, ID: id + 1, Generation: id + 1, PreviousID: id, PreviousHash: hash, Capacity: 1 << 20}
		if publishErr := store.publish(next, &catalogRecord{Kind: catalogSeal, Segment: recordSegment, FileID: segment.FileID}); publishErr != nil {
			return publishErr
		}
		segments = append(segments, segment)
		previousHash = hash
		return nil
	}
	for id := uint64(1); id <= 4; id++ {
		if err = publishSegment(id); err != nil {
			t.Fatal(err)
		}
	}
	// An unreadable physical bank is never permission to overwrite its ring
	// dependency; the first checkpoint cut below explicitly repairs that bank.
	store.bankUsable[1] = false
	before := store.slot
	if err = publishSegment(5); !errors.Is(err, ErrBounds) {
		t.Fatalf("unsafe overwrite = %v", err)
	}
	if store.slot != before || len(segments) != 4 {
		t.Fatal("unsafe overwrite mutated state")
	}
	for bank := byte(0); bank < 2; bank++ {
		checkpointID := fileID{0xd0 + bank}
		digest, writeErr := writeCatalogCheckpoint(dir, catalogCheckpoint{ID: checkpointID, LogID: testLogID, Generation: store.slot.Generation, Tail: store.slot.CatalogTail, CatalogHash: store.slot.CatalogHash, Segments: segments}, testAuthKey)
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		next := store.slot
		next.Generation++
		next.PreviousCheckpointID, next.PreviousCheckpointTail, next.PreviousCheckpointHash = [16]byte{}, 0, [32]byte{}
		next.CheckpointID, next.CheckpointTail, next.CheckpointHash = [16]byte(checkpointID), next.CatalogTail, digest
		if err = store.publish(next, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = publishSegment(5); err != nil {
		t.Fatalf("safe overwrite = %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	opened, got, err := openMetadataStore(dir, testLogID, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if len(got) != 5 || got[4].ID != 5 {
		t.Fatalf("recovered segments=%v", got)
	}
}

func remacMetadataSlot(buf []byte, key [32]byte) {
	// Re-encode the test's deliberate semantic corruptions with valid integrity
	// bytes so the decoder must reject canonical state, not merely a stale CRC.
	mac := newMetadataTestMAC(key, buf[:metadataSlotMACOffset])
	copy(buf[metadataSlotMACOffset:metadataSlotCRCOffset], mac[:])
	binary.LittleEndian.PutUint32(buf[metadataSlotCRCOffset:], crc32.Checksum(buf[:metadataSlotCRCOffset], crcTable))
}

func newMetadataTestMAC(key [32]byte, payload []byte) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(payload)
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}
