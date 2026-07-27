//go:build linux

package storeio

import (
	"os"

	"golang.org/x/sys/unix"
)

// dataSync is the power-safe checkpoint boundary on Linux. Keep fsync for
// this strongest lane: unlike an ordinary data-page fence, the contract
// includes the inode metadata needed to recover the file after power loss.
func dataSync(file *os.File) error { return file.Sync() }

// filesystemSync implements the ordinary filesystem-strength checkpoint
// boundary. CheckpointSyncFilesystem preallocates and writes existing page
// extents, so fdatasync provides the required data ordering without forcing
// unrelated inode metadata. Do not retry an error: Linux may discard the
// dirty pages associated with a failed sync, and the committer must fail
// closed instead.
func filesystemSync(file *os.File) error {
	return unix.Fdatasync(int(file.Fd()))
}

func dataBarrier(file *os.File) error { return dataSync(file) }
