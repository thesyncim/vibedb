package seglog

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

var reservePhysicalFile = reservePhysical
var recycleIdentityWrite = writeFullAt
var recycleTruncate = func(file *os.File, size int64) error { return file.Truncate(size) }
var recycleFileSync = func(file *os.File) error { return file.Sync() }

const (
	reserveHeaderBytes     = 128
	reserveMACOffset       = reserveHeaderBytes - 36
	reserveCRCOffset       = reserveHeaderBytes - 4
	reserveCertificateKind = 1
)

var reserveMagic = [8]byte{'V', 'D', 'B', 'R', 'E', 'S', 'E', 'R'}

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

func prepareReserve(dir string, capacity uint64, logID [16]byte, key [32]byte) (*os.File, reserveDescriptor, error) {
	if capacity < segmentHeaderBytes || capacity >= 1<<32 || logID == ([16]byte{}) || key == ([32]byte{}) {
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
		err = verifyPhysicalAllocation(file, capacity, 0)
	}
	descriptor := reserveDescriptor{FileID: id, Capacity: capacity, Ready: true}
	if err == nil {
		certificate := marshalReserveCertificate(logID, descriptor, key)
		err = writeFullAt(file, certificate[:], segmentIdentityBytes)
	}
	// Writing the lifecycle certificate establishes exactly segmentHeaderBytes
	// of logical EOF. Do not truncate, even to that same size: Linux releases
	// KEEP_SIZE preallocation beyond EOF on ftruncate.
	if err == nil {
		err = verifyPhysicalReserve(file, descriptor, logID, key)
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
	return file, descriptor, nil
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

// removeExactPublishedPath is the durable counterpart used after an
// authenticated retirement intent is authoritative. It reports success only
// when the exact inode is absent and that namespace change is directory-durable.
// A substituted path is corruption, not something reclamation may unlink.
func removeExactPublishedPath(opened os.FileInfo, path, dir string) error {
	check, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		// The path may be absent because a previous unlink succeeded but its
		// directory sync failed. Re-sync before retiring the durable intent.
		return reclaimSyncDir(dir)
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
	return reclaimSyncDir(dir)
}

func verifyPhysicalAllocation(file *os.File, capacity uint64, wantSize int64) error {
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() != wantSize {
		return fmt.Errorf("%w: reserve logical EOF", ErrCorrupt)
	}
	allocated, ok := allocatedFileBytes(stat)
	if !ok || allocated < capacity {
		return fmt.Errorf("%w: reserve short physical allocation %d/%d", ErrBounds, allocated, capacity)
	}
	return nil
}

// restoreActiveAllocation runs only during cold recovery, after replay has
// authenticated the retained prefix. A torn-tail truncation can release the
// KEEP_SIZE reservation, including if a previous restart crashed between the
// truncation and reallocation. Restore and prove headroom before startup sync
// and before exposing the writer; ENOSPC remains an explicit startup failure.
func restoreActiveAllocation(file *os.File, capacity uint64, wantSize int64) error {
	err := verifyPhysicalAllocation(file, capacity, wantSize)
	if err == nil || !errors.Is(err, ErrBounds) {
		return err
	}
	if err := reservePhysicalFile(file, capacity); err != nil {
		return err
	}
	return verifyPhysicalAllocation(file, capacity, wantSize)
}

func marshalReserveCertificate(logID [16]byte, descriptor reserveDescriptor, key [32]byte) (out [reserveHeaderBytes]byte) {
	copy(out[:8], reserveMagic[:])
	binary.LittleEndian.PutUint16(out[8:10], canonicalFormatMarker)
	binary.LittleEndian.PutUint16(out[10:12], reserveHeaderBytes)
	binary.LittleEndian.PutUint32(out[12:16], reserveCertificateKind)
	copy(out[16:32], logID[:])
	copy(out[32:48], descriptor.FileID[:])
	binary.LittleEndian.PutUint64(out[48:56], descriptor.Capacity)
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(out[:reserveMACOffset])
	copy(out[reserveMACOffset:reserveCRCOffset], mac.Sum(nil))
	binary.LittleEndian.PutUint32(out[reserveCRCOffset:], crc32.Checksum(out[:reserveCRCOffset], crcTable))
	return out
}

func marshalSegmentPrefix(header segmentHeader, key [32]byte) (out [segmentHeaderBytes]byte) {
	copy(out[:segmentIdentityBytes], marshalSegmentHeader(header))
	certificate := marshalReserveCertificate(header.LogID, reserveDescriptor{FileID: header.FileID, Capacity: header.Capacity, Ready: true}, key)
	copy(out[segmentIdentityBytes:], certificate[:])
	return out
}

func validReserveCertificate(raw []byte, descriptor reserveDescriptor, logID [16]byte, key [32]byte) bool {
	if len(raw) != reserveHeaderBytes || string(raw[:8]) != string(reserveMagic[:]) || binary.LittleEndian.Uint16(raw[8:10]) != canonicalFormatMarker || binary.LittleEndian.Uint16(raw[10:12]) != reserveHeaderBytes || binary.LittleEndian.Uint32(raw[12:16]) != reserveCertificateKind || binary.LittleEndian.Uint32(raw[reserveCRCOffset:]) != crc32.Checksum(raw[:reserveCRCOffset], crcTable) || !allZero(raw[56:reserveMACOffset]) {
		return false
	}
	var gotLogID [16]byte
	var gotFileID fileID
	copy(gotLogID[:], raw[16:32])
	copy(gotFileID[:], raw[32:48])
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(raw[:reserveMACOffset])
	return gotLogID == logID && gotFileID == descriptor.FileID && binary.LittleEndian.Uint64(raw[48:56]) == descriptor.Capacity && hmac.Equal(raw[reserveMACOffset:reserveCRCOffset], mac.Sum(nil))
}

func verifyPhysicalReserve(file *os.File, descriptor reserveDescriptor, logID [16]byte, key [32]byte) error {
	if !descriptor.Ready || descriptor.FileID == (fileID{}) || descriptor.Capacity < segmentHeaderBytes || descriptor.Capacity >= 1<<32 || logID == ([16]byte{}) || key == ([32]byte{}) {
		return ErrCorrupt
	}
	if err := verifyPhysicalAllocation(file, descriptor.Capacity, segmentHeaderBytes); err != nil {
		return err
	}
	var raw [segmentHeaderBytes]byte
	if err := readFullAt(file, raw[:], 0); err != nil {
		return err
	}
	if !allZero(raw[:segmentIdentityBytes]) || !validReserveCertificate(raw[segmentIdentityBytes:], descriptor, logID, key) {
		return ErrCorrupt
	}
	return nil
}

func verifyLifecycleCertificate(file *os.File, descriptor reserveDescriptor, logID [16]byte, key [32]byte) error {
	var raw [reserveHeaderBytes]byte
	if err := readFullAt(file, raw[:], segmentIdentityBytes); err != nil {
		return err
	}
	if !validReserveCertificate(raw[:], descriptor, logID, key) {
		return ErrCorrupt
	}
	return nil
}

// recycleRetiredSegment preserves the keyed lifecycle certificate while
// destroying only the retired segment identity. Every crash cut therefore has
// an authenticated owner even if the identity write tears. The caller may
// publish the returned READY descriptor only after this function succeeds.
func recycleRetiredSegment(file *os.File, descriptor reserveDescriptor, logID [16]byte, key [32]byte) error {
	if err := verifyLifecycleCertificate(file, descriptor, logID, key); err != nil {
		return err
	}
	if err := verifyPhysicalReserve(file, descriptor, logID, key); err == nil {
		if err = recycleFileSync(file); err != nil {
			return err
		}
		return verifyPhysicalReserve(file, descriptor, logID, key)
	}
	var zeroIdentity [segmentIdentityBytes]byte
	if err := recycleIdentityWrite(file, zeroIdentity[:], 0); err != nil {
		return err
	}
	if err := recycleFileSync(file); err != nil {
		return err
	}
	if err := recycleTruncate(file, segmentHeaderBytes); err != nil {
		return err
	}
	if err := reservePhysicalFile(file, descriptor.Capacity); err != nil {
		return err
	}
	if err := verifyPhysicalReserve(file, descriptor, logID, key); err != nil {
		return err
	}
	if err := recycleFileSync(file); err != nil {
		return err
	}
	return verifyPhysicalReserve(file, descriptor, logID, key)
}

func reconcileTentativeReserve(file *os.File, descriptor reserveDescriptor, slot metadataSlot, key [32]byte) error {
	if err := verifyPhysicalReserve(file, descriptor, slot.LogID, key); err == nil {
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
	header, err := unmarshalSegmentHeader(raw[:segmentIdentityBytes])
	if err != nil || !validReserveCertificate(raw[segmentIdentityBytes:], descriptor, slot.LogID, key) || header.ID != slot.Active.ID+1 || header.Generation != slot.Active.Generation+1 || header.PreviousID != slot.Active.ID || header.LogID != slot.LogID || header.FileID != descriptor.FileID || header.Capacity != descriptor.Capacity {
		return errors.Join(ErrCorrupt, err)
	}
	// A segment header is CRC-protected, but is accepted only because every
	// identity field is bound by this authenticated reserve owner.
	var zeroIdentity [segmentIdentityBytes]byte
	err = writeFullAt(file, zeroIdentity[:], 0)
	if err == nil {
		err = file.Sync()
	}
	if err != nil {
		return err
	}
	return verifyPhysicalReserve(file, descriptor, slot.LogID, key)
}
