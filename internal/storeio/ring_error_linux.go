//go:build linux

package storeio

import (
	"errors"
	"syscall"
)

func ringSetupUnavailable(err error) bool {
	return errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) ||
		errors.Is(err, syscall.ENOMEM) || errors.Is(err, syscall.ENOSYS) ||
		errors.Is(err, syscall.EOPNOTSUPP)
}
