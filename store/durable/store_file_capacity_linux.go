//go:build linux

package durable

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

var linuxStrictAllocationOps = struct {
	fallocate func(int, uint32, int64, int64) error
	fstatfs   func(int, *unix.Statfs_t) error
}{
	fallocate: unix.Fallocate,
	fstatfs:   unix.Fstatfs,
}

func strictlyAllocateFile(file *os.File, _ int64, target int64) error {
	if file == nil || target <= 0 {
		return fmt.Errorf("invalid strict main-file allocation")
	}
	fd := int(file.Fd())
	// Mode zero over [0,target) is absolute and idempotent. It repairs punched
	// holes and grows EOF without a sparse Truncate fallback.
	if err := linuxStrictAllocationOps.fallocate(fd, 0, 0, target); err != nil {
		return err
	}
	// Plain fallocate does not privatize reflinked extents. Unsharing the entire
	// prefix reserves the copy-on-write space too, so later overwrites cannot
	// discover ENOSPC inside an apparently allocated certificate. ext4 has no
	// writable reflink support and rejects this mode; accept that one filesystem
	// identity after mode-zero allocation and fail closed everywhere else.
	if err := linuxStrictAllocationOps.fallocate(
		fd, unix.FALLOC_FL_UNSHARE_RANGE, 0, target,
	); err != nil {
		if err != unix.EOPNOTSUPP {
			return err
		}
		var filesystem unix.Statfs_t
		if statErr := linuxStrictAllocationOps.fstatfs(
			fd, &filesystem,
		); statErr != nil {
			return statErr
		}
		if uint64(filesystem.Type) != uint64(unix.EXT4_SUPER_MAGIC) {
			return fmt.Errorf(
				"strict main-file unshare unsupported on filesystem %#x: %w",
				uint64(filesystem.Type), err,
			)
		}
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < target {
		return fmt.Errorf("strict main-file allocation did not grow EOF")
	}
	return nil
}

func strictlySyncAllocatedFile(file *os.File) error { return file.Sync() }
