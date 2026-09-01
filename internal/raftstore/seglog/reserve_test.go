package seglog

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestPrepareReserveKeepsZeroEOFAndProvesAllocation(t *testing.T) {
	file, descriptor, err := prepareReserve(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || stat.Size() != 0 || !descriptor.Ready || descriptor.Capacity != 1<<20 {
		t.Fatalf("stat=%v descriptor=%+v err=%v", stat, descriptor, err)
	}
}

func TestPrepareReserveRejectsENOSPCAndShortAllocation(t *testing.T) {
	saved := reservePhysicalFile
	defer func() { reservePhysicalFile = saved }()
	reservePhysicalFile = func(*os.File, uint64) error { return syscall.ENOSPC }
	if file, _, err := prepareReserve(t.TempDir(), 1<<20); file != nil || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("ENOSPC file=%v err=%v", file, err)
	}
	reservePhysicalFile = func(*os.File, uint64) error { return nil }
	if file, _, err := prepareReserve(t.TempDir(), 1<<20); file != nil || !errors.Is(err, ErrBounds) {
		t.Fatalf("short allocation file=%v err=%v", file, err)
	}
}

func TestRepeatedReserveENOSPCLeavesDirectoryBounded(t *testing.T) {
	dir := t.TempDir()
	saved := reservePhysicalFile
	reservePhysicalFile = func(*os.File, uint64) error { return syscall.ENOSPC }
	t.Cleanup(func() { reservePhysicalFile = saved })
	for i := 0; i < 32; i++ {
		if file, _, err := prepareReserve(dir, 1<<20); file != nil || !errors.Is(err, syscall.ENOSPC) {
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
		file, descriptor, err := prepareReserve(dir, 1<<20)
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
