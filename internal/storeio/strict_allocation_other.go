//go:build !linux

package storeio

import (
	"fmt"
	"os"
)

func strictlyAllocateFile(_ *os.File, _ int64) error {
	return fmt.Errorf("%w on this platform", ErrStrictAllocationUnsupported)
}
