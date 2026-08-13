//go:build windows

package durable

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func strictlyAllocateFile(_ *os.File, _, _ int64) error {
	// Windows exposes AllocationSize mutation, but this package has no
	// proof that the complete prefix is non-sparse after foreign hole creation.
	// Refuse sealed capacity until that proof is implemented.
	return fmt.Errorf("%w", windows.ERROR_NOT_SUPPORTED)
}

func strictlySyncAllocatedFile(file *os.File) error { return file.Sync() }
