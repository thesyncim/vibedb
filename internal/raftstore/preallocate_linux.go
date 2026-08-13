//go:build linux

package raftstore

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func preallocate(file *os.File, size int64) error {
	if err := unix.Fallocate(int(file.Fd()), 0, 0, size); err != nil {
		return fmt.Errorf("physically preallocate WAL: %w", err)
	}
	return nil
}

func ensureAllocated(file *os.File, size int64) error {
	if err := unix.Fallocate(int(file.Fd()), 0, 0, size); err != nil {
		return fmt.Errorf("restore physical WAL allocation: %w", err)
	}
	return nil
}
