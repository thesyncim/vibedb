//go:build !darwin && !linux && !windows

package durable

import (
	"fmt"
	"os"
)

func strictlyAllocateFile(_ *os.File, _, _ int64) error {
	return fmt.Errorf("strict main-file allocation is unsupported on this platform")
}

func strictlySyncAllocatedFile(file *os.File) error { return file.Sync() }
