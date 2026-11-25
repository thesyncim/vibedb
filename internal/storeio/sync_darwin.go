//go:build darwin

package storeio

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

const darwinSyncMaxEINTRRetries = 8

// dataSync is the acknowledgement boundary on Darwin. fsync only transfers
// bytes from the host to the drive cache and explicitly does not guarantee
// survival of power loss; F_FULLFSYNC additionally asks the drive to drain
// that volatile cache. A durability mode must not silently weaken merely
// because the stronger operation costs more.
func dataSync(file *os.File) error {
	defer runtime.KeepAlive(file)
	fd := file.Fd()
	for retries := 0; ; retries++ {
		_, err := unix.FcntlInt(fd, unix.F_FULLFSYNC, 0)
		if !errors.Is(err, unix.EINTR) || retries == darwinSyncMaxEINTRRetries {
			return err
		}
	}
}

// filesystemSync implements the ordinary filesystem-strength checkpoint
// boundary. os.File.Sync uses F_FULLFSYNC on Darwin, but ordinary mode must not
// pay for that power-safe device-cache drain.
func filesystemSync(file *os.File) error {
	defer runtime.KeepAlive(file)
	fd := int(file.Fd())
	for retries := 0; ; retries++ {
		err := unix.Fsync(fd)
		if !errors.Is(err, unix.EINTR) || retries == darwinSyncMaxEINTRRetries {
			return err
		}
	}
}

// dataBarrier orders the data-page phase before the following alternate-root
// write. F_BARRIERFSYNC performs an fsync and issues a device barrier, which is
// the Darwin primitive intended to order two phases without draining the
// device queue. The final root phase still uses file.Sync, so this changes
// neither Commit's return point nor its existing durability guarantee.
//
// HFS and APFS implement the command. Preserve portable behavior on other
// filesystems that reject it, but propagate descriptor and I/O failures.
func dataBarrier(file *os.File) error {
	defer runtime.KeepAlive(file)
	fd := file.Fd()
	var err error
	for retries := 0; ; retries++ {
		_, err = unix.FcntlInt(fd, unix.F_BARRIERFSYNC, 0)
		if !errors.Is(err, unix.EINTR) || retries == darwinSyncMaxEINTRRetries {
			break
		}
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) ||
		errors.Is(err, unix.ENOTTY) {
		return dataSync(file)
	}
	return err
}
