//go:build !linux && !darwin

package seglog

import (
	"fmt"
	"os"
)

func reservePhysical(_ *os.File, _ uint64) error {
	return fmt.Errorf("%w: physical segment reservation is unsupported", ErrBounds)
}
