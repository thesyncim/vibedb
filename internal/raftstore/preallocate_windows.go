//go:build windows

package raftstore

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func preallocate(file *os.File, size int64) error {
	allocation := struct{ AllocationSize int64 }{AllocationSize: size}
	if err := windows.SetFileInformationByHandle(windows.Handle(file.Fd()), windows.FileAllocationInfo, (*byte)(unsafe.Pointer(&allocation)), uint32(unsafe.Sizeof(allocation))); err != nil {
		return fmt.Errorf("reserve WAL allocation: %w", err)
	}
	if err := file.Truncate(size); err != nil {
		return fmt.Errorf("set preallocated WAL size: %w", err)
	}
	return nil
}

func ensureAllocated(_ *os.File, _ int64) error {
	return ErrPlatformUnsupported
}
