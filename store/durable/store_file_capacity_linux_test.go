//go:build linux

package durable

import (
	"errors"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPhysicalCapacityLinuxStrictAllocationUnsharesOrAcceptsOnlyExt4(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-linux-unshare-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	production := linuxStrictAllocationOps
	defer func() { linuxStrictAllocationOps = production }()

	const target = int64(4 * 4096)
	var modes []uint32
	linuxStrictAllocationOps.fallocate = func(
		_ int, mode uint32, _ int64, length int64,
	) error {
		modes = append(modes, mode)
		if mode == 0 {
			return file.Truncate(length)
		}
		return nil
	}
	if err := strictlyAllocateFile(file, 0, target); err != nil {
		t.Fatal(err)
	}
	if len(modes) != 2 || modes[0] != 0 ||
		modes[1] != unix.FALLOC_FL_UNSHARE_RANGE {
		t.Fatalf("strict fallocate modes = %#v, want mode-zero then unshare", modes)
	}

	modes = modes[:0]
	linuxStrictAllocationOps.fallocate = func(
		_ int, mode uint32, _ int64, length int64,
	) error {
		modes = append(modes, mode)
		if mode == 0 {
			return file.Truncate(length)
		}
		return unix.EOPNOTSUPP
	}
	linuxStrictAllocationOps.fstatfs = func(_ int, stat *unix.Statfs_t) error {
		stat.Type = unix.EXT4_SUPER_MAGIC
		return nil
	}
	if err := strictlyAllocateFile(file, target, target); err != nil {
		t.Fatalf("ext4 unshare fallback = %v", err)
	}
	linuxStrictAllocationOps.fstatfs = func(_ int, stat *unix.Statfs_t) error {
		stat.Type = unix.XFS_SUPER_MAGIC
		return nil
	}
	if err := strictlyAllocateFile(file, target, target); !errors.Is(err, unix.EOPNOTSUPP) {
		t.Fatalf("unknown reflink filesystem fallback = %v, want EOPNOTSUPP", err)
	}
	for _, hardErr := range []error{unix.EINVAL, unix.ENOSYS} {
		statfsCalls := 0
		linuxStrictAllocationOps.fallocate = func(
			_ int, mode uint32, _ int64, length int64,
		) error {
			if mode == 0 {
				return file.Truncate(length)
			}
			return hardErr
		}
		linuxStrictAllocationOps.fstatfs = func(_ int, _ *unix.Statfs_t) error {
			statfsCalls++
			return nil
		}
		if err := strictlyAllocateFile(file, target, target); !errors.Is(err, hardErr) {
			t.Fatalf("unshare error %v = %v", hardErr, err)
		}
		if statfsCalls != 0 {
			t.Fatalf("unshare error %v performed %d filesystem fallbacks", hardErr, statfsCalls)
		}
	}
	linuxStrictAllocationOps.fallocate = func(
		_ int, _ uint32, _ int64, _ int64,
	) error {
		return nil
	}
	if err := file.Truncate(target - 1); err != nil {
		t.Fatal(err)
	}
	if err := strictlyAllocateFile(file, target-1, target); err == nil {
		t.Fatal("strict allocation accepted an EOF below its target")
	}
}

func TestPhysicalCapacityLinuxUnsharesReflinkOnOpen(t *testing.T) {
	options, initial := cappedAsyncFileStoreOptions(t)
	source, err := os.CreateTemp(t.TempDir(), "physical-capacity-linux-reflink-source-*")
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	collection, err := Create(source, options)
	if err != nil {
		t.Skipf("filesystem cannot establish a sealed allocation certificate: %v", err)
	}
	target := initial + 64*uint64(options.PageSize)
	if err := collection.EnsurePhysicalAllocation(target); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	clone, err := os.CreateTemp(t.TempDir(), "physical-capacity-linux-reflink-clone-*")
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()
	if err := unix.IoctlFileClone(int(clone.Fd()), int(source.Fd())); err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) ||
			errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EXDEV) {
			t.Skipf("filesystem does not support reflink fixture: %v", err)
		}
		t.Fatal(err)
	}
	if err := clone.Sync(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(clone, Options{Durability: DurabilityAsyncVisible})
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.PhysicalHighWaterBytes(); got != target {
		t.Fatalf("reflink reopen certificate = %d, want %d", got, target)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalCapacityLinuxRepairsPunchedInteriorOnOpen(t *testing.T) {
	options, initial := cappedAsyncFileStoreOptions(t)
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-linux-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) {
			t.Skipf("filesystem cannot establish a sealed allocation certificate: %v", err)
		}
		t.Fatal(err)
	}
	target := initial + 128*uint64(options.PageSize)
	if err := collection.EnsurePhysicalAllocation(target); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	var before unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &before); err != nil {
		t.Fatal(err)
	}
	holeOffset := int64(initial + 32*uint64(options.PageSize))
	holeLength := int64(16 * options.PageSize)
	if err := unix.Fallocate(
		int(file.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
		holeOffset, holeLength,
	); err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) ||
			errors.Is(err, unix.EINVAL) {
			t.Skipf("filesystem does not support punched-hole fixture: %v", err)
		}
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	var punched unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &punched); err != nil {
		t.Fatal(err)
	}
	if punched.Blocks >= before.Blocks {
		t.Fatalf("hole punch did not reduce allocated blocks: before=%d after=%d", before.Blocks, punched.Blocks)
	}

	reopened, err := Open(file, Options{Durability: DurabilityAsyncVisible})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.PhysicalHighWaterBytes() != target {
		t.Fatalf("repaired high-water = %d, want %d", reopened.PhysicalHighWaterBytes(), target)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	var repaired unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &repaired); err != nil {
		t.Fatal(err)
	}
	if repaired.Blocks < before.Blocks || uint64(repaired.Blocks)*512 < target {
		t.Fatalf("repaired allocation blocks=%d bytes=%d, want prefix %d", repaired.Blocks, uint64(repaired.Blocks)*512, target)
	}
}
