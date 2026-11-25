//go:build linux

package storeio

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// dataSync is the power-safe checkpoint boundary on Linux. Keep fsync for
// this strongest lane: unlike an ordinary data-page fence, the contract
// includes the inode metadata needed to recover the file after power loss.
func dataSync(file *os.File) error { return file.Sync() }

// filesystemSync implements the ordinary filesystem-strength checkpoint
// boundary. CheckpointSyncFilesystem preallocates and writes existing page
// extents, so fdatasync provides the required data ordering without forcing
// unrelated inode metadata. Retry only EINTR: I/O errors may discard dirty
// pages on Linux, so any other error must fail closed immediately.
func filesystemSync(file *os.File) error {
	defer runtime.KeepAlive(file)
	fd := int(file.Fd())
	const maxEINTRRetries = 8
	for retries := 0; ; retries++ {
		err := unix.Fdatasync(fd)
		if !errors.Is(err, unix.EINTR) || retries == maxEINTRRetries {
			return err
		}
	}
}

func dataBarrier(file *os.File) error { return dataSync(file) }
