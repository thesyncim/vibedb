//go:build linux

package seglog

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func reservePhysical(file *os.File, capacity uint64) error {
	if capacity == 0 || capacity >= 1<<32 {
		return ErrBounds
	}
	if err := unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, int64(capacity)); err != nil {
		return fmt.Errorf("seglog: reserve physical segment: %w", err)
	}
	return nil
}
