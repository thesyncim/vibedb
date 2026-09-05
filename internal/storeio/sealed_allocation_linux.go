//go:build linux

package storeio

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func preallocatePortableFile(file *os.File, target int64) error {
	err := unix.Fallocate(int(file.Fd()), 0, 0, target)
	if err == nil {
		return nil
	}
	if !errors.Is(err, unix.EOPNOTSUPP) && !errors.Is(err, unix.ENOSYS) {
		return err
	}
	return growPortableFile(file, target)
}
