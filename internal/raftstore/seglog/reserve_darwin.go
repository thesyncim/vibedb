//go:build darwin

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
	allocation := &unix.Fstore_t{Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE, Length: int64(capacity)}
	if err := unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, allocation); err != nil {
		return fmt.Errorf("seglog: reserve physical segment: %w", err)
	}
	return nil
}
