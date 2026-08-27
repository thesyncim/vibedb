//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package storeio

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockWriterPlatform(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("%w: %w", ErrWriterLocked, err)
		}
		return err
	}
	return nil
}

func unlockWriterPlatform(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
