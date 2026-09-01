//go:build linux

package raftstore

import (
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

func defaultDurableReadyWrite() func(*os.File, []byte, int64) (int, error) {
	return writeDurableReadyAt
}

// writeDurableReadyAt submits the complete contiguous group and its data-only
// durability requirement as one Linux operation. The WAL is fixed-size and
// preallocated, so no file-size metadata is required for recovery.
func writeDurableReadyAt(file *os.File, data []byte, offset int64) (int, error) {
	if file == nil || len(data) == 0 || offset < 0 {
		return 0, unix.EINVAL
	}
	iovec := unix.Iovec{Base: &data[0], Len: uint64(len(data))}
	const longBits = 64
	low := uintptr(offset)
	high := uintptr(uint64(offset) >> (longBits - 1) >> 1)
	written, _, errno := unix.Syscall6(
		unix.SYS_PWRITEV2,
		file.Fd(), uintptr(unsafe.Pointer(&iovec)), 1,
		low, high, uintptr(unix.RWF_DSYNC),
	)
	runtime.KeepAlive(data)
	if errno != 0 {
		return int(written), errno
	}
	return int(written), nil
}
