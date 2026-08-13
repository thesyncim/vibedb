//go:build linux

package storeio

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

func strictlyAllocateFile(file *os.File, target int64) error {
	fd := int(file.Fd())
	// Mode zero over the absolute prefix grows EOF and repairs interior sparse
	// ranges. There is deliberately no Truncate fallback: virtual length is not
	// a physical allocation proof.
	if err := linuxStrictAllocationOps.fallocate(fd, 0, 0, target); err != nil {
		return err
	}
	// Plain fallocate does not reserve the copy-on-write break of a reflinked
	// extent. Unshare the same complete prefix before it can become a capacity
	// certificate. ext4 has no writable reflink support and reports
	// EOPNOTSUPP; accept only that exact, descriptor-proved filesystem identity.
	if err := linuxStrictAllocationOps.fallocate(
		fd, unix.FALLOC_FL_UNSHARE_RANGE, 0, target,
	); err != nil {
		if err != unix.EOPNOTSUPP {
			return err
		}
		var filesystem unix.Statfs_t
		if statErr := linuxStrictAllocationOps.fstatfs(fd, &filesystem); statErr != nil {
			return statErr
		}
		if uint64(filesystem.Type) != uint64(unix.EXT4_SUPER_MAGIC) {
			return fmt.Errorf(
				"%w: unshare unsupported on filesystem %#x: %w",
				ErrStrictAllocationUnsupported, uint64(filesystem.Type), err,
			)
		}
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < target {
		return fmt.Errorf("%w: allocation did not grow EOF", ErrStrictAllocationUnsupported)
	}
	return nil
}
