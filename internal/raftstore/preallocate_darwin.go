//go:build darwin

package raftstore

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func preallocate(file *os.File, size int64) error {
	allocation := &unix.Fstore_t{Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE, Length: size}
	if err := unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, allocation); err != nil {
		return fmt.Errorf("physically preallocate WAL: %w", err)
	}
	if err := file.Truncate(size); err != nil {
		return fmt.Errorf("set preallocated WAL size: %w", err)
	}
	return nil
}

func ensureAllocated(file *os.File, size int64) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL allocation: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Blocks < 0 || stat.Blocks > int64(^uint64(0)>>1)/512 || stat.Blocks*512 < size {
		return fmt.Errorf("%w: WAL physical allocation is smaller than sealed capacity", ErrFull)
	}
	return nil
}
