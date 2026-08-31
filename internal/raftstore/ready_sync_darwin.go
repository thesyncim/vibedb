//go:build darwin

package raftstore

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// syncReadyRecord orders the append-only Ready record before the alternate
// current-slot write. The final current-slot File.Sync remains the power-safe
// acknowledgement boundary. APFS and HFS implement F_BARRIERFSYNC; retain the
// stronger portable operation on filesystems that do not.
func syncReadyRecord(file *os.File) error {
	_, err := unix.FcntlInt(file.Fd(), unix.F_BARRIERFSYNC, 0)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) ||
		errors.Is(err, unix.ENOTTY) {
		return file.Sync()
	}
	return err
}
