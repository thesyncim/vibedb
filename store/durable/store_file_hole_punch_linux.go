//go:build linux

package durable

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func punchFileStoreHole(
	file *os.File,
	offset, length uint64,
) (bool, error) {
	if file == nil {
		return false, nil
	}
	for attempt := 0; attempt < fileStoreHolePunchMaxAttempts; attempt++ {
		err := unix.Fallocate(
			int(file.Fd()),
			unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
			int64(offset), int64(length),
		)
		if err == nil {
			return true, nil
		}
		if errors.Is(err, unix.EINTR) &&
			attempt+1 < fileStoreHolePunchMaxAttempts {
			continue
		}
		if errors.Is(err, unix.EOPNOTSUPP) ||
			errors.Is(err, unix.ENOTSUP) ||
			errors.Is(err, unix.ENOSYS) ||
			errors.Is(err, unix.EINVAL) {
			return false, nil
		}
		return true, err
	}
	return true, unix.EINTR
}
