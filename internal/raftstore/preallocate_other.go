//go:build !darwin && !linux && !windows

package raftstore

import (
	"fmt"
	"os"
)

func preallocate(_ *os.File, _ int64) error {
	return fmt.Errorf("%w: physical WAL preallocation is unsupported on this platform", ErrInvalid)
}

func ensureAllocated(_ *os.File, _ int64) error {
	return ErrPlatformUnsupported
}
