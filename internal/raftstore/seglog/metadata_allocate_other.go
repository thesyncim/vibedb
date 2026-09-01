//go:build !darwin && !linux

package seglog

import (
	"fmt"
	"os"
)

func metadataAllocateThrough(_ *os.File, _ uint64) error {
	return fmt.Errorf("%w: metadata physical allocation unsupported", ErrBounds)
}
