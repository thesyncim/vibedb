//go:build linux

package raftstore

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncReadyRecord makes the appended record durable before the alternate
// current-slot write. The WAL is fixed-size and preallocated, so fdatasync is
// sufficient for the record bytes; the final File.Sync retains the stronger
// acknowledgement boundary for the selected image.
func syncReadyRecord(file *os.File) error {
	return unix.Fdatasync(int(file.Fd()))
}
