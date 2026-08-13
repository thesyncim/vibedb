//go:build linux

package storeio

import (
	"errors"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestStrictlyAllocateFileUnsharesOrAcceptsOnlyExt4(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "strict-allocation-*")
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
	if err := StrictlyAllocateFile(file, target); err != nil {
		t.Fatal(err)
	}
	if len(modes) != 2 || modes[0] != 0 || modes[1] != unix.FALLOC_FL_UNSHARE_RANGE {
		t.Fatalf("strict fallocate modes = %#v, want mode-zero then unshare", modes)
	}

	linuxStrictAllocationOps.fallocate = func(
		_ int, mode uint32, _ int64, length int64,
	) error {
		if mode == 0 {
			return file.Truncate(length)
		}
		return unix.EOPNOTSUPP
	}
	linuxStrictAllocationOps.fstatfs = func(_ int, stat *unix.Statfs_t) error {
		stat.Type = unix.EXT4_SUPER_MAGIC
		return nil
	}
	if err := StrictlyAllocateFile(file, target); err != nil {
		t.Fatalf("ext4 unshare fallback = %v", err)
	}
	linuxStrictAllocationOps.fstatfs = func(_ int, stat *unix.Statfs_t) error {
		stat.Type = unix.XFS_SUPER_MAGIC
		return nil
	}
	if err := StrictlyAllocateFile(file, target); !errors.Is(err, ErrStrictAllocationUnsupported) ||
		!errors.Is(err, unix.EOPNOTSUPP) {
		t.Fatalf("reflink filesystem fallback = %v", err)
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
		if err := StrictlyAllocateFile(file, target); !errors.Is(err, hardErr) {
			t.Fatalf("unshare error %v = %v", hardErr, err)
		}
		if statfsCalls != 0 {
			t.Fatalf("unshare error %v performed %d filesystem fallbacks", hardErr, statfsCalls)
		}
	}

	linuxStrictAllocationOps.fallocate = func(
		_ int, mode uint32, _ int64, _ int64,
	) error {
		if mode == 0 {
			return file.Truncate(target - 1)
		}
		return nil
	}
	if err := StrictlyAllocateFile(file, target); !errors.Is(err, ErrStrictAllocationUnsupported) {
		t.Fatalf("successful allocation with short EOF = %v, want unsupported", err)
	}
}
