//go:build darwin

package seglog

import (
	"os"

	"golang.org/x/sys/unix"
)

func metadataAllocateThrough(file *os.File, through uint64) error {
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if uint64(stat.Size()) >= through {
		return nil
	}
	store := &unix.Fstore_t{Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE, Length: int64(through - uint64(stat.Size()))}
	return unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, store)
}
