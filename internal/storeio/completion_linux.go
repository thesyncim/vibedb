//go:build linux

package storeio

import "syscall"

// Err converts a negative completion result to syscall.Errno.
func (c Completion) Err() error {
	if c.Result >= 0 {
		return nil
	}
	return syscall.Errno(-c.Result)
}
