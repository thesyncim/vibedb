//go:build darwin

package storeio

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// materializationPhaseBarrier orders one completed phase before the next
// without draining the device queue. Patch-only commits use it between the
// journal and combined target/root phase; hybrids additionally use it between
// data and the alternate root. APFS and HFS implement F_BARRIERFSYNC; an
// unsupported filesystem falls back to the stronger full flush rather than
// weakening canonical-overwrite recovery.
func materializationPhaseBarrier(file *os.File) error {
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
		return materializationSync(file)
	}
	return err
}

// materializationSync is the final commit/recovery durability boundary on
// Darwin. F_FULLFSYNC drains volatile drive caches before acknowledgement.
func materializationSync(file *os.File) error {
	defer runtime.KeepAlive(file)
	fd := file.Fd()
	for retries := 0; ; retries++ {
		_, err := unix.FcntlInt(fd, unix.F_FULLFSYNC, 0)
		if !errors.Is(err, unix.EINTR) || retries == darwinSyncMaxEINTRRetries {
			return err
		}
	}
}
