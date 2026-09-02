package seglog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"syscall"
	"testing"
)

func TestReserveCertificateRejectsCRCOnlyForgery(t *testing.T) {
	file, descriptor, err := prepareReserve(t.TempDir(), 1<<20, testLogID, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var raw [reserveHeaderBytes]byte
	if err = readFullAt(file, raw[:], segmentIdentityBytes); err != nil {
		t.Fatal(err)
	}
	raw[56] ^= 1
	binary.LittleEndian.PutUint32(raw[reserveCRCOffset:], crc32.Checksum(raw[:reserveCRCOffset], crcTable))
	if err = writeFullAt(file, raw[:], segmentIdentityBytes); err != nil {
		t.Fatal(err)
	}
	if err = verifyPhysicalReserve(file, descriptor, testLogID, testAuthKey); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("CRC-only reserve forgery = %v", err)
	}
}

func TestTentativeReserveRequiresAuthenticatedOwnerAndRestoresCertificate(t *testing.T) {
	file, descriptor, err := prepareReserve(t.TempDir(), 1<<20, testLogID, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	previousHash := [32]byte{7}
	header := segmentHeader{ID: 2, Generation: 2, PreviousID: 1, PreviousHash: previousHash, LogID: testLogID, FileID: descriptor.FileID, Capacity: descriptor.Capacity}
	if err = activateReserve(file, header, testAuthKey); err != nil {
		t.Fatal(err)
	}
	slot := metadataSlot{LogID: testLogID, Active: activeDescriptor{ID: 1, Generation: 1, FileID: fileID{99}, Capacity: descriptor.Capacity, PreviousHash: [32]byte{6}}, Reserves: [2]reserveDescriptor{descriptor}}
	if err = reconcileTentativeReserve(file, descriptor, slot, testAuthKey); err != nil {
		t.Fatal(err)
	}
	if err = verifyPhysicalReserve(file, descriptor, testLogID, testAuthKey); err != nil {
		t.Fatalf("restored certificate = %v", err)
	}
	if err = activateReserve(file, header, testAuthKey); err != nil {
		t.Fatal(err)
	}
	wrong := slot
	wrong.Active.ID = 9
	if err = reconcileTentativeReserve(file, descriptor, wrong, testAuthKey); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unbound active header = %v", err)
	}
}

func TestPrepareReserveKeepsAuthenticatedLifecyclePrefixAndProvesAllocation(t *testing.T) {
	file, descriptor, err := prepareReserve(t.TempDir(), 1<<20, testLogID, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || stat.Size() != segmentHeaderBytes || !descriptor.Ready || descriptor.Capacity != 1<<20 {
		t.Fatalf("stat=%v descriptor=%+v err=%v", stat, descriptor, err)
	}
}

func TestPrepareReserveRejectsENOSPCAndShortAllocation(t *testing.T) {
	saved := reservePhysicalFile
	defer func() { reservePhysicalFile = saved }()
	reservePhysicalFile = func(*os.File, uint64) error { return syscall.ENOSPC }
	if file, _, err := prepareReserve(t.TempDir(), 1<<20, testLogID, testAuthKey); file != nil || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("ENOSPC file=%v err=%v", file, err)
	}
	reservePhysicalFile = func(*os.File, uint64) error { return nil }
	if file, _, err := prepareReserve(t.TempDir(), 1<<20, testLogID, testAuthKey); file != nil || !errors.Is(err, ErrBounds) {
		t.Fatalf("short allocation file=%v err=%v", file, err)
	}
}

func TestRepeatedReserveENOSPCLeavesDirectoryBounded(t *testing.T) {
	dir := t.TempDir()
	saved := reservePhysicalFile
	reservePhysicalFile = func(*os.File, uint64) error { return syscall.ENOSPC }
	t.Cleanup(func() { reservePhysicalFile = saved })
	for i := 0; i < 32; i++ {
		if file, _, err := prepareReserve(dir, 1<<20, testLogID, testAuthKey); file != nil || !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("attempt %d file=%v err=%v", i, file, err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("attempt %d leaked %d entries", i, len(entries))
		}
	}
}

func TestDiscardedPreparedReservesLeaveDirectoryBounded(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 8; i++ {
		file, descriptor, err := prepareReserve(dir, 1<<20, testLogID, testAuthKey)
		if err != nil {
			t.Fatal(err)
		}
		cleanupUnpublishedFile(file, segmentPath(dir, descriptor.FileID), dir)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("attempt %d leaked %d entries", i, len(entries))
		}
	}
}
