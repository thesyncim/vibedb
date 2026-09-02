package seglog

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	catalogCheckpointHeaderBytes  = 176
	catalogCheckpointEntryBytes   = 96
	catalogCheckpointTrailerBytes = 36
	catalogCheckpointMaxEntries   = 1 << 20
	catalogCheckpointMaxBytes     = catalogCheckpointHeaderBytes + catalogCheckpointMaxEntries*catalogCheckpointEntryBytes + (256 << 20) + catalogCheckpointTrailerBytes
)

const (
	checkpointRoleOrdinary byte = iota + 1
	checkpointRoleReclaimA
	checkpointRoleReclaimB
)

func derivedCheckpointID(key [32]byte, logID [16]byte, generation, tail uint64, catalogHash [32]byte, anchorID, anchorGeneration uint64, anchorHash [32]byte, role byte) fileID {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("vibedb/seglog/checkpoint-id"))
	var fixed [57]byte
	copy(fixed[:16], logID[:])
	binary.LittleEndian.PutUint64(fixed[16:24], generation)
	binary.LittleEndian.PutUint64(fixed[24:32], tail)
	binary.LittleEndian.PutUint64(fixed[32:40], anchorID)
	binary.LittleEndian.PutUint64(fixed[40:48], anchorGeneration)
	fixed[48] = role
	_, _ = mac.Write(fixed[:49])
	_, _ = mac.Write(catalogHash[:])
	_, _ = mac.Write(anchorHash[:])
	sum := mac.Sum(nil)
	var id fileID
	copy(id[:], sum[:16])
	return id
}

// Test seams may lower these bounds. Production replays at most 4096 fixed
// 192-byte records (<768 KiB), while checkpoint work starts at half that bound.
var catalogCheckpointSoftRecords uint64 = 2048
var catalogCheckpointHardRecords uint64 = 4096

var catalogCheckpointMagic = [8]byte{'V', 'D', 'B', 'C', 'A', 'T', 'C', 'P'}
var catalogCheckpointWriter = writeCatalogCheckpoint
var checkpointWriteFullAt = writeFullAt
var checkpointFileSync = func(file *os.File) error { return file.Sync() }
var checkpointRename = os.Rename
var errCheckpointTempRemoved = errors.New("seglog: invalid checkpoint temp removed")
var checkpointBeforeTempCleanup func(string)

func cleanupInvalidCheckpointTemp(opened os.FileInfo, path, dir string) {
	if checkpointBeforeTempCleanup != nil {
		checkpointBeforeTempCleanup(path)
	}
	cleanupUnpublishedPath(opened, path, dir)
}

type checkpointPublishPhase uint8

const (
	checkpointTempWritten checkpointPublishPhase = iota + 1
	checkpointFileSynced
	checkpointRenamed
	checkpointDirectorySynced
)

var checkpointPublishHook func(checkpointPublishPhase) error

type catalogCheckpoint struct {
	ID                         fileID
	LogID                      [16]byte
	Generation, Tail           uint64
	CatalogHash                [32]byte
	AnchorID, AnchorGeneration uint64
	AnchorHash                 [32]byte
	Segments                   []SegmentMeta
	Final                      SegmentMeta // optional replacement for the final pending descriptor
	BaseSequence               uint64
	GroupIDs                   []uint64
	GroupSummaries             map[uint64]sealedRunSummary
}

type checkpointBase struct {
	Sequence uint64
	Groups   []checkpointGroupSummary
}

func checkpointFileName(id fileID) string {
	return "catalog-checkpoint-" + segmentFileName(id)[len("segment-"):len("segment-")+32] + ".dat"
}

func writeCatalogCheckpoint(dir string, checkpoint catalogCheckpoint, key [32]byte) ([32]byte, error) {
	var zero [32]byte
	if checkpoint.ID == (fileID{}) || checkpoint.LogID == ([16]byte{}) || checkpoint.Generation == 0 || checkpoint.Tail < metadataCatalogStart || (checkpoint.Tail-metadataCatalogStart)%catalogRecordBytes != 0 || checkpoint.CatalogHash == zero || key == zero || len(checkpoint.Segments) == 0 && checkpoint.Final.ID != 0 {
		return zero, ErrCorrupt
	}
	if len(checkpoint.Segments) > catalogCheckpointMaxEntries {
		return zero, ErrBounds
	}
	var summaryScratch [256]byte
	summaryBytes := uint64(0)
	previousGroup := uint64(0)
	for i := range checkpoint.GroupIDs {
		groupID := checkpoint.GroupIDs[i]
		summary, exists := checkpoint.GroupSummaries[groupID]
		encoded, encodeErr := appendCheckpointGroupSummary(summaryScratch[:0], previousGroup, checkpointGroupSummary{GroupID: groupID, Summary: summary})
		if !exists || encodeErr != nil || summaryBytes > ^uint64(0)-uint64(len(encoded)) {
			return zero, ErrCorrupt
		}
		summaryBytes += uint64(len(encoded))
		previousGroup = groupID
	}
	if len(checkpoint.GroupIDs) != 0 && checkpoint.BaseSequence == 0 || len(checkpoint.GroupIDs) == 0 && checkpoint.BaseSequence != 0 || len(checkpoint.GroupIDs) != len(checkpoint.GroupSummaries) || summaryBytes > uint64(catalogCheckpointMaxBytes) {
		return zero, ErrCorrupt
	}
	var header [catalogCheckpointHeaderBytes]byte
	copy(header[:8], catalogCheckpointMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], canonicalFormatMarker)
	binary.LittleEndian.PutUint16(header[10:12], catalogCheckpointHeaderBytes)
	binary.LittleEndian.PutUint64(header[16:24], checkpoint.Generation)
	copy(header[24:40], checkpoint.LogID[:])
	copy(header[40:56], checkpoint.ID[:])
	binary.LittleEndian.PutUint64(header[56:64], checkpoint.Tail)
	copy(header[64:96], checkpoint.CatalogHash[:])
	binary.LittleEndian.PutUint64(header[96:104], checkpoint.AnchorID)
	binary.LittleEndian.PutUint64(header[104:112], checkpoint.AnchorGeneration)
	copy(header[112:144], checkpoint.AnchorHash[:])
	binary.LittleEndian.PutUint64(header[144:152], uint64(len(checkpoint.Segments)))
	binary.LittleEndian.PutUint64(header[152:160], checkpoint.BaseSequence)
	binary.LittleEndian.PutUint64(header[160:168], uint64(len(checkpoint.GroupIDs)))
	binary.LittleEndian.PutUint64(header[168:176], summaryBytes)
	previousID, previousGeneration, previousHash := checkpoint.AnchorID, checkpoint.AnchorGeneration, checkpoint.AnchorHash
	for i := range checkpoint.Segments {
		segment := checkpoint.Segments[i]
		if i == len(checkpoint.Segments)-1 && checkpoint.Final.ID != 0 {
			segment = checkpoint.Final
		}
		if !validSegmentGeometry(segment) || segment.ID != previousID+1 || segment.Generation != previousGeneration+1 || segment.PreviousHash != previousHash || segment.FileID == (fileID{}) || segment.Hash == zero {
			return zero, ErrCorrupt
		}
		// Segment FileID uniqueness was established while opening each exact
		// authenticated segment identity. The streamed checkpoint store does not
		// duplicate that O(live) ownership index.
		previousID, previousGeneration, previousHash = segment.ID, segment.Generation, segment.Hash
	}
	tmp := filepath.Join(dir, "."+checkpointFileName(checkpoint.ID)+".tmp")
	final := filepath.Join(dir, checkpointFileName(checkpoint.ID))
	if _, statErr := os.Stat(final); statErr == nil {
		return validateExistingCatalogCheckpoint(dir, checkpoint, key)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return zero, statErr
	}
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	if errors.Is(err, os.ErrExist) {
		digest, existingErr := validateExistingCatalogCheckpoint(dir, checkpoint, key)
		if !errors.Is(existingErr, errCheckpointTempRemoved) {
			return digest, existingErr
		}
		file, err = os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	}
	if err != nil {
		return zero, err
	}
	created, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return zero, err
	}
	ok := false
	renamed := false
	hookCrash := false
	defer func() {
		_ = file.Close()
		if !ok && !renamed && !hookCrash {
			cleanupUnpublishedPath(created, tmp, dir)
		}
	}()
	mac := hmac.New(sha256.New, key[:])
	crc := crc32.New(crcTable)
	offset := int64(0)
	writePart := func(part []byte) error {
		if writeErr := checkpointWriteFullAt(file, part, offset); writeErr != nil {
			return writeErr
		}
		offset += int64(len(part))
		_, _ = mac.Write(part)
		_, _ = crc.Write(part)
		return nil
	}
	err = writePart(header[:])
	var entry [catalogCheckpointEntryBytes]byte
	for i := 0; err == nil && i < len(checkpoint.Segments); i++ {
		clear(entry[:])
		segment := checkpoint.Segments[i]
		if i == len(checkpoint.Segments)-1 && checkpoint.Final.ID != 0 {
			segment = checkpoint.Final
		}
		binary.LittleEndian.PutUint64(entry[0:8], segment.ID)
		binary.LittleEndian.PutUint64(entry[8:16], segment.Generation)
		copy(entry[16:32], segment.FileID[:])
		binary.LittleEndian.PutUint64(entry[32:40], segment.Bytes)
		binary.LittleEndian.PutUint64(entry[40:48], segment.Records)
		binary.LittleEndian.PutUint64(entry[48:56], segment.IndexOffset)
		binary.LittleEndian.PutUint64(entry[56:64], segment.IndexBytes)
		copy(entry[64:96], segment.Hash[:])
		err = writePart(entry[:])
	}
	previousGroup = 0
	for i := 0; err == nil && i < len(checkpoint.GroupIDs); i++ {
		groupID := checkpoint.GroupIDs[i]
		encoded, _ := appendCheckpointGroupSummary(summaryScratch[:0], previousGroup, checkpointGroupSummary{GroupID: groupID, Summary: checkpoint.GroupSummaries[groupID]})
		err = writePart(encoded)
		previousGroup = groupID
	}
	var digest [32]byte
	_ = mac.Sum(digest[:0])
	var trailer [catalogCheckpointTrailerBytes]byte
	copy(trailer[:32], digest[:])
	_, _ = crc.Write(digest[:])
	binary.LittleEndian.PutUint32(trailer[32:36], crc.Sum32())
	if err == nil {
		err = checkpointWriteFullAt(file, trailer[:], offset)
	}
	if err == nil {
		if checkpointPublishHook != nil {
			err = checkpointPublishHook(checkpointTempWritten)
			hookCrash = err != nil
		}
	}
	if err == nil {
		err = checkpointFileSync(file)
	}
	if err == nil && checkpointPublishHook != nil {
		err = checkpointPublishHook(checkpointFileSynced)
		hookCrash = err != nil
	}
	if err == nil {
		err = file.Close()
	}
	if err == nil {
		err = checkpointRename(tmp, final)
		renamed = err == nil
	}
	if err == nil && checkpointPublishHook != nil {
		err = checkpointPublishHook(checkpointRenamed)
		hookCrash = err != nil
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err == nil && checkpointPublishHook != nil {
		err = checkpointPublishHook(checkpointDirectorySynced)
		hookCrash = err != nil
	}
	if err != nil {
		return zero, err
	}
	ok = true
	return digest, nil
}

func sameCheckpointSegment(a, b SegmentMeta) bool {
	return a.ID == b.ID && a.Generation == b.Generation && a.Bytes == b.Bytes && a.Records == b.Records && a.IndexOffset == b.IndexOffset && a.IndexBytes == b.IndexBytes && a.PreviousHash == b.PreviousHash && a.Hash == b.Hash && a.FileID == b.FileID && a.State == b.State
}

func validateExistingCatalogCheckpoint(dir string, checkpoint catalogCheckpoint, key [32]byte) ([32]byte, error) {
	final := filepath.Join(dir, checkpointFileName(checkpoint.ID))
	tmp := filepath.Join(dir, "."+checkpointFileName(checkpoint.ID)+".tmp")
	path := final
	temporary := false
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		path, temporary = tmp, true
	}
	file, err := os.Open(path)
	if err != nil {
		return [32]byte{}, err
	}
	stat, err := file.Stat()
	if err != nil || stat.Size() < catalogCheckpointTrailerBytes || stat.Size() > int64(catalogCheckpointMaxBytes) {
		_ = file.Close()
		if temporary && err == nil {
			cleanupInvalidCheckpointTemp(stat, path, dir)
			return [32]byte{}, errCheckpointTempRemoved
		}
		return [32]byte{}, errors.Join(ErrCorrupt, err)
	}
	var trailer [catalogCheckpointTrailerBytes]byte
	if err = readFullAt(file, trailer[:], stat.Size()-catalogCheckpointTrailerBytes); err != nil {
		_ = file.Close()
		if temporary {
			cleanupInvalidCheckpointTemp(stat, path, dir)
			return [32]byte{}, errCheckpointTempRemoved
		}
		return [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], trailer[:32])
	slot := metadataSlot{Generation: checkpoint.Generation, LogID: checkpoint.LogID, CatalogTail: checkpoint.Tail, CatalogHash: checkpoint.CatalogHash, CheckpointID: [16]byte(checkpoint.ID), CheckpointTail: checkpoint.Tail, CheckpointHash: digest, AnchorID: checkpoint.AnchorID, AnchorGeneration: checkpoint.AnchorGeneration, AnchorHash: checkpoint.AnchorHash}
	segments, base, _, _, _, err := readCatalogCheckpointFile(file, slot, key)
	if err != nil || len(segments) != len(checkpoint.Segments) || base.Sequence != checkpoint.BaseSequence || len(base.Groups) != len(checkpoint.GroupIDs) {
		_ = file.Close()
		if temporary {
			cleanupInvalidCheckpointTemp(stat, path, dir)
			return [32]byte{}, errCheckpointTempRemoved
		}
		return [32]byte{}, errors.Join(ErrCorrupt, err)
	}
	for i := range segments {
		want := checkpoint.Segments[i]
		if i == len(segments)-1 && checkpoint.Final.ID != 0 {
			want = checkpoint.Final
		}
		if !sameCheckpointSegment(segments[i], want) {
			_ = file.Close()
			if temporary {
				cleanupInvalidCheckpointTemp(stat, path, dir)
				return [32]byte{}, errCheckpointTempRemoved
			}
			return [32]byte{}, ErrCorrupt
		}
	}
	for i := range base.Groups {
		want, ok := checkpoint.GroupSummaries[checkpoint.GroupIDs[i]]
		if !ok || base.Groups[i].GroupID != checkpoint.GroupIDs[i] || base.Groups[i].Summary != want {
			_ = file.Close()
			if temporary {
				cleanupInvalidCheckpointTemp(stat, path, dir)
				return [32]byte{}, errCheckpointTempRemoved
			}
			return [32]byte{}, ErrCorrupt
		}
	}
	if err = file.Close(); err != nil {
		return [32]byte{}, err
	}
	if temporary {
		current, statErr := os.Stat(tmp)
		if statErr != nil || !os.SameFile(stat, current) {
			return [32]byte{}, errors.Join(ErrCorrupt, statErr)
		}
		if err = checkpointRename(tmp, final); err != nil {
			return [32]byte{}, err
		}
		if err = syncDir(dir); err != nil {
			return [32]byte{}, err
		}
	}
	return digest, nil
}

func authenticateCheckpointPath(path string, wantID fileID, wantLogID [16]byte, wantHash [32]byte, key [32]byte) (os.FileInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil || stat.Size() < catalogCheckpointHeaderBytes+catalogCheckpointTrailerBytes || stat.Size() > int64(catalogCheckpointMaxBytes) {
		_ = file.Close()
		return nil, errors.Join(ErrCorrupt, err)
	}
	var header [catalogCheckpointHeaderBytes]byte
	var trailer [catalogCheckpointTrailerBytes]byte
	if err = readFullAt(file, header[:], 0); err == nil {
		err = readFullAt(file, trailer[:], stat.Size()-catalogCheckpointTrailerBytes)
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	var id, logID [16]byte
	var catalogHash, anchorHash, digest [32]byte
	copy(logID[:], header[24:40])
	copy(id[:], header[40:56])
	copy(catalogHash[:], header[64:96])
	copy(anchorHash[:], header[112:144])
	copy(digest[:], trailer[:32])
	count, summaryBytes := binary.LittleEndian.Uint64(header[144:152]), binary.LittleEndian.Uint64(header[168:176])
	contentBytes := uint64(stat.Size() - catalogCheckpointTrailerBytes)
	segmentBytes := count * catalogCheckpointEntryBytes
	if key == ([32]byte{}) || string(header[:8]) != string(catalogCheckpointMagic[:]) || binary.LittleEndian.Uint16(header[8:10]) != canonicalFormatMarker || binary.LittleEndian.Uint16(header[10:12]) != catalogCheckpointHeaderBytes || !allZero(header[12:16]) || id != [16]byte(wantID) || logID != wantLogID || digest != wantHash || count > catalogCheckpointMaxEntries || segmentBytes/catalogCheckpointEntryBytes != count || summaryBytes > uint64(catalogCheckpointMaxBytes) || uint64(catalogCheckpointHeaderBytes)+segmentBytes > contentBytes || uint64(catalogCheckpointHeaderBytes)+segmentBytes+summaryBytes != contentBytes {
		_ = file.Close()
		return nil, ErrCorrupt
	}
	mac := hmac.New(sha256.New, key[:])
	crc := crc32.New(crcTable)
	section := io.NewSectionReader(file, 0, int64(contentBytes))
	var scratch [32 << 10]byte
	for {
		n, readErr := section.Read(scratch[:])
		if n != 0 {
			_, _ = mac.Write(scratch[:n])
			_, _ = crc.Write(scratch[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = file.Close()
			return nil, readErr
		}
	}
	var authenticated [32]byte
	_ = mac.Sum(authenticated[:0])
	_, _ = crc.Write(trailer[:32])
	if !hmac.Equal(authenticated[:], trailer[:32]) || binary.LittleEndian.Uint32(trailer[32:36]) != crc.Sum32() {
		_ = file.Close()
		return nil, ErrCorrupt
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	return stat, nil
}

func readCatalogCheckpoint(dir string, slot metadataSlot, key [32]byte) ([]SegmentMeta, checkpointBase, uint64, [32]byte, uint64, error) {
	return readCatalogCheckpointPath(filepath.Join(dir, checkpointFileName(fileID(slot.CheckpointID))), slot, key)
}

func readCatalogCheckpointPath(path string, slot metadataSlot, key [32]byte) ([]SegmentMeta, checkpointBase, uint64, [32]byte, uint64, error) {
	var zero [32]byte
	if slot.CheckpointID == ([16]byte{}) {
		return nil, checkpointBase{}, metadataCatalogStart, zero, 0, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, checkpointBase{}, 0, zero, 0, err
	}
	segments, base, tail, digest, generation, readErr := readCatalogCheckpointFile(file, slot, key)
	return segments, base, tail, digest, generation, errors.Join(readErr, file.Close())
}

func readCatalogCheckpointFile(file *os.File, slot metadataSlot, key [32]byte) ([]SegmentMeta, checkpointBase, uint64, [32]byte, uint64, error) {
	var zero [32]byte
	if slot.CheckpointID == ([16]byte{}) {
		return nil, checkpointBase{}, metadataCatalogStart, zero, 0, nil
	}
	stat, err := file.Stat()
	if err != nil || stat.Size() < catalogCheckpointHeaderBytes+catalogCheckpointTrailerBytes || stat.Size() > int64(catalogCheckpointMaxBytes) {
		return nil, checkpointBase{}, 0, zero, 0, errors.Join(ErrCorrupt, err)
	}
	var header [catalogCheckpointHeaderBytes]byte
	if err = readFullAt(file, header[:], 0); err != nil {
		return nil, checkpointBase{}, 0, zero, 0, err
	}
	if string(header[:8]) != string(catalogCheckpointMagic[:]) || binary.LittleEndian.Uint16(header[8:10]) != canonicalFormatMarker || binary.LittleEndian.Uint16(header[10:12]) != catalogCheckpointHeaderBytes || !allZero(header[12:16]) {
		return nil, checkpointBase{}, 0, zero, 0, ErrCorrupt
	}
	generation, tail := binary.LittleEndian.Uint64(header[16:24]), binary.LittleEndian.Uint64(header[56:64])
	var logID, gotID [16]byte
	copy(logID[:], header[24:40])
	copy(gotID[:], header[40:56])
	var catalogHash, anchorHash [32]byte
	copy(catalogHash[:], header[64:96])
	anchorID, anchorGeneration := binary.LittleEndian.Uint64(header[96:104]), binary.LittleEndian.Uint64(header[104:112])
	copy(anchorHash[:], header[112:144])
	count := binary.LittleEndian.Uint64(header[144:152])
	base := checkpointBase{Sequence: binary.LittleEndian.Uint64(header[152:160])}
	groupCount, summaryBytes := binary.LittleEndian.Uint64(header[160:168]), binary.LittleEndian.Uint64(header[168:176])
	segmentBytes := count * catalogCheckpointEntryBytes
	contentBytes := uint64(stat.Size() - catalogCheckpointTrailerBytes)
	if count > catalogCheckpointMaxEntries || segmentBytes/catalogCheckpointEntryBytes != count || summaryBytes > uint64(catalogCheckpointMaxBytes) || uint64(catalogCheckpointHeaderBytes)+segmentBytes > contentBytes || uint64(catalogCheckpointHeaderBytes)+segmentBytes+summaryBytes != contentBytes || (groupCount == 0) != (base.Sequence == 0) || logID != slot.LogID || gotID != slot.CheckpointID || tail != slot.CheckpointTail || tail > slot.CatalogTail || generation > slot.Generation || catalogHash == zero || anchorID != slot.AnchorID || anchorGeneration != slot.AnchorGeneration || anchorHash != slot.AnchorHash {
		return nil, checkpointBase{}, 0, zero, 0, ErrCorrupt
	}
	var trailer [catalogCheckpointTrailerBytes]byte
	if err = readFullAt(file, trailer[:], int64(contentBytes)); err != nil {
		return nil, checkpointBase{}, 0, zero, 0, err
	}
	mac := hmac.New(sha256.New, key[:])
	crc := crc32.New(crcTable)
	section := io.NewSectionReader(file, 0, int64(contentBytes))
	var verifyScratch [32 << 10]byte
	for {
		n, readErr := section.Read(verifyScratch[:])
		if n != 0 {
			_, _ = mac.Write(verifyScratch[:n])
			_, _ = crc.Write(verifyScratch[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, checkpointBase{}, 0, zero, 0, readErr
		}
	}
	var digest [32]byte
	_ = mac.Sum(digest[:0])
	_, _ = crc.Write(trailer[:32])
	if !hmac.Equal(digest[:], trailer[:32]) || digest != slot.CheckpointHash || binary.LittleEndian.Uint32(trailer[32:36]) != crc.Sum32() {
		return nil, checkpointBase{}, 0, zero, 0, ErrCorrupt
	}
	segments := make([]SegmentMeta, 0, count)
	previousID, previousGeneration, previousHash := anchorID, anchorGeneration, anchorHash
	offset := int64(catalogCheckpointHeaderBytes)
	var entry [catalogCheckpointEntryBytes]byte
	for range count {
		if err = readFullAt(file, entry[:], offset); err != nil {
			return nil, checkpointBase{}, 0, zero, 0, err
		}
		segment := SegmentMeta{ID: binary.LittleEndian.Uint64(entry[0:8]), Generation: binary.LittleEndian.Uint64(entry[8:16]), Bytes: binary.LittleEndian.Uint64(entry[32:40]), Records: binary.LittleEndian.Uint64(entry[40:48]), IndexOffset: binary.LittleEndian.Uint64(entry[48:56]), IndexBytes: binary.LittleEndian.Uint64(entry[56:64]), PreviousHash: previousHash, State: SegmentSealed}
		copy(segment.FileID[:], entry[16:32])
		copy(segment.Hash[:], entry[64:96])
		if !validSegmentGeometry(segment) || segment.ID != previousID+1 || segment.Generation != previousGeneration+1 || segment.FileID == (fileID{}) || segment.Hash == zero {
			return nil, checkpointBase{}, 0, zero, 0, ErrCorrupt
		}
		segments = append(segments, segment)
		previousID, previousGeneration, previousHash = segment.ID, segment.Generation, segment.Hash
		offset += catalogCheckpointEntryBytes
	}
	if groupCount > uint64(summaryBytes)/4+1 || groupCount > catalogCheckpointMaxEntries {
		return nil, checkpointBase{}, 0, zero, 0, ErrCorrupt
	}
	base.Groups = make([]checkpointGroupSummary, 0, groupCount)
	reader := bufio.NewReaderSize(io.NewSectionReader(file, offset, int64(summaryBytes)), 32<<10)
	previousGroup := uint64(0)
	for range groupCount {
		group, decodeErr := readCheckpointGroupSummary(reader, previousGroup)
		if decodeErr != nil {
			return nil, checkpointBase{}, 0, zero, 0, ErrCorrupt
		}
		base.Groups = append(base.Groups, group)
		previousGroup = group.GroupID
	}
	if _, readErr := reader.ReadByte(); readErr != io.EOF {
		return nil, checkpointBase{}, 0, zero, 0, ErrCorrupt
	}
	return segments, base, tail, catalogHash, generation, nil
}

func cleanupUnpublishedCheckpoint(dir string, checkpoint catalogCheckpoint, digest [32]byte, key [32]byte) {
	path := filepath.Join(dir, checkpointFileName(checkpoint.ID))
	file, err := os.Open(path)
	if err != nil {
		return
	}
	opened, statErr := file.Stat()
	_ = file.Close()
	if statErr != nil {
		return
	}
	slot := metadataSlot{Generation: checkpoint.Generation, LogID: checkpoint.LogID, CatalogTail: checkpoint.Tail, CatalogHash: checkpoint.CatalogHash, CheckpointID: [16]byte(checkpoint.ID), CheckpointTail: checkpoint.Tail, CheckpointHash: digest, AnchorID: checkpoint.AnchorID, AnchorGeneration: checkpoint.AnchorGeneration, AnchorHash: checkpoint.AnchorHash}
	if _, _, _, _, _, err = readCatalogCheckpoint(dir, slot, key); err != nil {
		return
	}
	cleanupUnpublishedPath(opened, path, dir)
}
