//go:build darwin

package storeio

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func preallocatePortableFile(file *os.File, target int64) error {
	info, err := file.Stat()
	if err != nil || info.Size() >= target {
		return err
	}
	// Reserve growth through the native allocator when supported. This does
	// not claim to unshare existing APFS clones or reserve later COW breaks.
	allocation := unix.Fstore_t{
		Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE,
		Length: target - info.Size(),
	}
	err = unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, &allocation)
	if err != nil && !errors.Is(err, unix.ENOTSUP) && !errors.Is(err, unix.ENOSYS) {
		return err
	}
	return growPortableFile(file, target)
}
