package seglog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	catalogCheckpointHeaderBytes  = 160
	catalogCheckpointEntryBytes   = 96
	catalogCheckpointTrailerBytes = 36
	catalogCheckpointMaxEntries   = 1 << 20
	catalogCheckpointMaxBytes     = catalogCheckpointHeaderBytes + catalogCheckpointMaxEntries*catalogCheckpointEntryBytes + catalogCheckpointTrailerBytes
)

// Test seams may lower these bounds. Production replays at most 4096 fixed
// 192-byte records (<768 KiB), while checkpoint work starts at half that bound.
var catalogCheckpointSoftRecords uint64 = 2048
var catalogCheckpointHardRecords uint64 = 4096

var catalogCheckpointMagic = [8]byte{'V', 'D', 'B', 'C', 'A', 'T', 'C', 'P'}
var catalogCheckpointWriter = writeCatalogCheckpoint
var checkpointWriteFullAt = writeFullAt
var checkpointFileSync = func(file *os.File) error { return file.Sync() }
var checkpointRename = os.Rename

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
	total := catalogCheckpointHeaderBytes + len(checkpoint.Segments)*catalogCheckpointEntryBytes + catalogCheckpointTrailerBytes
	data := make([]byte, total)
	copy(data[:8], catalogCheckpointMagic[:])
	binary.LittleEndian.PutUint16(data[8:10], canonicalFormatMarker)
	binary.LittleEndian.PutUint16(data[10:12], catalogCheckpointHeaderBytes)
	binary.LittleEndian.PutUint64(data[16:24], checkpoint.Generation)
	copy(data[24:40], checkpoint.LogID[:])
	copy(data[40:56], checkpoint.ID[:])
	binary.LittleEndian.PutUint64(data[56:64], checkpoint.Tail)
	copy(data[64:96], checkpoint.CatalogHash[:])
	binary.LittleEndian.PutUint64(data[96:104], checkpoint.AnchorID)
	binary.LittleEndian.PutUint64(data[104:112], checkpoint.AnchorGeneration)
	copy(data[112:144], checkpoint.AnchorHash[:])
	binary.LittleEndian.PutUint64(data[144:152], uint64(len(checkpoint.Segments)))
	previousID, previousGeneration, previousHash := checkpoint.AnchorID, checkpoint.AnchorGeneration, checkpoint.AnchorHash
	owned := make(map[fileID]struct{}, len(checkpoint.Segments))
	offset := catalogCheckpointHeaderBytes
	for i := range checkpoint.Segments {
		segment := checkpoint.Segments[i]
		if i == len(checkpoint.Segments)-1 && checkpoint.Final.ID != 0 {
			segment = checkpoint.Final
		}
		if !validSegmentGeometry(segment) || segment.ID != previousID+1 || segment.Generation != previousGeneration+1 || segment.PreviousHash != previousHash || segment.FileID == (fileID{}) || segment.Hash == zero {
			return zero, ErrCorrupt
		}
		if _, duplicate := owned[segment.FileID]; duplicate {
			return zero, ErrCorrupt
		}
		owned[segment.FileID] = struct{}{}
		entry := data[offset : offset+catalogCheckpointEntryBytes]
		binary.LittleEndian.PutUint64(entry[0:8], segment.ID)
		binary.LittleEndian.PutUint64(entry[8:16], segment.Generation)
		copy(entry[16:32], segment.FileID[:])
		binary.LittleEndian.PutUint64(entry[32:40], segment.Bytes)
		binary.LittleEndian.PutUint64(entry[40:48], segment.Records)
		binary.LittleEndian.PutUint64(entry[48:56], segment.IndexOffset)
		binary.LittleEndian.PutUint64(entry[56:64], segment.IndexBytes)
		copy(entry[64:96], segment.Hash[:])
		previousID, previousGeneration, previousHash = segment.ID, segment.Generation, segment.Hash
		offset += catalogCheckpointEntryBytes
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(data[:total-catalogCheckpointTrailerBytes])
	var digest [32]byte
	_ = mac.Sum(digest[:0])
	copy(data[total-catalogCheckpointTrailerBytes:total-4], digest[:])
	binary.LittleEndian.PutUint32(data[total-4:], crc32.Checksum(data[:total-4], crcTable))
	tmp := filepath.Join(dir, "."+checkpointFileName(checkpoint.ID)+".tmp")
	final := filepath.Join(dir, checkpointFileName(checkpoint.ID))
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
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
	if err = checkpointWriteFullAt(file, data, 0); err == nil {
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

func readCatalogCheckpoint(dir string, slot metadataSlot, key [32]byte) ([]SegmentMeta, uint64, [32]byte, uint64, error) {
	var zero [32]byte
	if slot.CheckpointID == ([16]byte{}) {
		return nil, metadataCatalogStart, zero, 0, nil
	}
	id := fileID(slot.CheckpointID)
	file, err := os.Open(filepath.Join(dir, checkpointFileName(id)))
	if err != nil {
		return nil, 0, zero, 0, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || stat.Size() < catalogCheckpointHeaderBytes+catalogCheckpointTrailerBytes || stat.Size() > int64(catalogCheckpointMaxBytes) {
		return nil, 0, zero, 0, errors.Join(ErrCorrupt, err)
	}
	data := make([]byte, int(stat.Size()))
	if err = readFullAt(file, data, 0); err != nil {
		return nil, 0, zero, 0, err
	}
	if len(data) < catalogCheckpointHeaderBytes+catalogCheckpointTrailerBytes || (len(data)-catalogCheckpointHeaderBytes-catalogCheckpointTrailerBytes)%catalogCheckpointEntryBytes != 0 || string(data[:8]) != string(catalogCheckpointMagic[:]) || binary.LittleEndian.Uint16(data[8:10]) != canonicalFormatMarker || binary.LittleEndian.Uint16(data[10:12]) != catalogCheckpointHeaderBytes || !allZero(data[12:16]) || !allZero(data[152:160]) {
		return nil, 0, zero, 0, ErrCorrupt
	}
	trailer := len(data) - catalogCheckpointTrailerBytes
	if binary.LittleEndian.Uint32(data[len(data)-4:]) != crc32.Checksum(data[:len(data)-4], crcTable) {
		return nil, 0, zero, 0, ErrCorrupt
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(data[:trailer])
	var digest [32]byte
	_ = mac.Sum(digest[:0])
	if !hmac.Equal(digest[:], data[trailer:len(data)-4]) || digest != slot.CheckpointHash {
		return nil, 0, zero, 0, ErrCorrupt
	}
	generation, tail := binary.LittleEndian.Uint64(data[16:24]), binary.LittleEndian.Uint64(data[56:64])
	var logID, gotID [16]byte
	copy(logID[:], data[24:40])
	copy(gotID[:], data[40:56])
	var catalogHash, anchorHash [32]byte
	copy(catalogHash[:], data[64:96])
	anchorID, anchorGeneration := binary.LittleEndian.Uint64(data[96:104]), binary.LittleEndian.Uint64(data[104:112])
	copy(anchorHash[:], data[112:144])
	count := binary.LittleEndian.Uint64(data[144:152])
	if logID != slot.LogID || gotID != slot.CheckpointID || tail != slot.CheckpointTail || tail > slot.CatalogTail || count != uint64((len(data)-catalogCheckpointHeaderBytes-catalogCheckpointTrailerBytes)/catalogCheckpointEntryBytes) || generation > slot.Generation || catalogHash == zero || anchorID != slot.AnchorID || anchorGeneration != slot.AnchorGeneration || anchorHash != slot.AnchorHash {
		return nil, 0, zero, 0, ErrCorrupt
	}
	segments := make([]SegmentMeta, 0, count)
	owned := make(map[fileID]struct{}, count)
	previousID, previousGeneration, previousHash := anchorID, anchorGeneration, anchorHash
	offset := catalogCheckpointHeaderBytes
	for range count {
		entry := data[offset : offset+catalogCheckpointEntryBytes]
		segment := SegmentMeta{ID: binary.LittleEndian.Uint64(entry[0:8]), Generation: binary.LittleEndian.Uint64(entry[8:16]), Bytes: binary.LittleEndian.Uint64(entry[32:40]), Records: binary.LittleEndian.Uint64(entry[40:48]), IndexOffset: binary.LittleEndian.Uint64(entry[48:56]), IndexBytes: binary.LittleEndian.Uint64(entry[56:64]), PreviousHash: previousHash, State: SegmentSealed}
		copy(segment.FileID[:], entry[16:32])
		copy(segment.Hash[:], entry[64:96])
		if !validSegmentGeometry(segment) || segment.ID != previousID+1 || segment.Generation != previousGeneration+1 || segment.FileID == (fileID{}) || segment.Hash == zero {
			return nil, 0, zero, 0, ErrCorrupt
		}
		if _, duplicate := owned[segment.FileID]; duplicate {
			return nil, 0, zero, 0, ErrCorrupt
		}
		owned[segment.FileID] = struct{}{}
		segments = append(segments, segment)
		previousID, previousGeneration, previousHash = segment.ID, segment.Generation, segment.Hash
		offset += catalogCheckpointEntryBytes
	}
	return segments, tail, catalogHash, generation, nil
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
	if _, _, _, _, err = readCatalogCheckpoint(dir, slot, key); err != nil {
		return
	}
	cleanupUnpublishedPath(opened, path, dir)
}
