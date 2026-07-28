//go:build linux

package storeio

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocateRecoveryJournal reserves the whole journal file up front with
// fallocate. This is not an optimization but a correctness requirement on
// Linux: a journal that extended its file under each acknowledgement sync would
// pay ext4/xfs metadata journaling on every record and lose the entire point of
// the append+fdatasync lane. fallocate allocates the blocks and sets the size
// once, so every later positional append writes into already-allocated space
// and fdatasync commits data only.
func preallocateRecoveryJournal(file *os.File, size int64) error {
	if err := unix.Fallocate(int(file.Fd()), 0, 0, size); err != nil {
		if err == unix.EOPNOTSUPP || err == unix.ENOSYS {
			// Some filesystems (tmpfs on older kernels) reject fallocate. Fall
			// back to a size-setting truncate; the metadata-journaling concern
			// does not apply to those filesystems.
			return file.Truncate(size)
		}
		return err
	}
	return nil
}
