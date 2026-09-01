//go:build linux

package seglog

import (
	"os"

	"golang.org/x/sys/unix"
)

func metadataAllocateThrough(file *os.File, through uint64) error {
	return unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, int64(through))
}
