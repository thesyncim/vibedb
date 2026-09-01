package seglog

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var reservePhysicalFile = reservePhysical

func segmentFileName(id fileID) string {
	var encoded [32]byte
	hex.Encode(encoded[:], id[:])
	return "segment-" + string(encoded[:]) + ".dat"
}

func randomFileID() (fileID, error) {
	var id fileID
	if _, err := rand.Read(id[:]); err != nil {
		return fileID{}, err
	}
	if id == (fileID{}) {
		return fileID{}, ErrCorrupt
	}
	return id, nil
}

func prepareReserve(dir string, capacity uint64) (*os.File, reserveDescriptor, error) {
	if capacity < segmentHeaderBytes || capacity >= 1<<32 {
		return nil, reserveDescriptor{}, ErrBounds
	}
	id, err := randomFileID()
	if err != nil {
		return nil, reserveDescriptor{}, err
	}
	path := filepath.Join(dir, segmentFileName(id))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	if err != nil {
		return nil, reserveDescriptor{}, err
	}
	if err = reservePhysicalFile(file, capacity); err == nil {
		err = verifyPhysicalReserve(file, capacity)
	}
	if err == nil {
		err = file.Sync()
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		cleanupUnpublishedFile(file, path, dir)
		return nil, reserveDescriptor{}, err
	}
	return file, reserveDescriptor{FileID: id, Capacity: capacity, Ready: true}, nil
}

// cleanupUnpublishedFile removes only the exact inode created by this call.
// A namespace substitution is ambiguous and deliberately left for quarantine.
func cleanupUnpublishedFile(file *os.File, path, dir string) {
	opened, statErr := file.Stat()
	pathStat, pathErr := os.Lstat(path)
	if statErr != nil || pathErr != nil || !os.SameFile(opened, pathStat) {
		_ = file.Close()
		return
	}
	if file.Close() != nil {
		return
	}
	cleanupUnpublishedPath(opened, path, dir)
}

func cleanupUnpublishedPath(opened os.FileInfo, path, dir string) {
	check, checkErr := os.Lstat(path)
	if checkErr != nil || !os.SameFile(opened, check) {
		return
	}
	if os.Remove(path) == nil {
		_ = syncDir(dir)
	}
}

func verifyPhysicalReserve(file *os.File, capacity uint64) error {
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() != 0 {
		return fmt.Errorf("%w: reserve logical EOF", ErrCorrupt)
	}
	allocated, ok := allocatedFileBytes(stat)
	if !ok || allocated < capacity {
		return fmt.Errorf("%w: reserve short physical allocation %d/%d", ErrBounds, allocated, capacity)
	}
	return nil
}

func reconcileTentativeReserve(file *os.File, descriptor reserveDescriptor, slot metadataSlot) error {
	if err := verifyPhysicalReserve(file, descriptor.Capacity); err == nil {
		return nil
	}
	stat, err := file.Stat()
	if err != nil || stat.Size() != segmentHeaderBytes {
		return errors.Join(ErrCorrupt, err)
	}
	var raw [segmentHeaderBytes]byte
	if err = readFullAt(file, raw[:], 0); err != nil {
		return err
	}
	header, err := unmarshalSegmentHeader(raw[:])
	if err != nil || header.ID != slot.Active.ID+1 || header.Generation != slot.Active.Generation+1 || header.PreviousID != slot.Active.ID || header.LogID != slot.LogID || header.FileID != descriptor.FileID || header.Capacity != descriptor.Capacity {
		return errors.Join(ErrCorrupt, err)
	}
	if err = file.Truncate(0); err == nil {
		err = reservePhysicalFile(file, descriptor.Capacity)
	}
	if err == nil {
		err = file.Sync()
	}
	if err != nil {
		return err
	}
	return verifyPhysicalReserve(file, descriptor.Capacity)
}
