//go:build darwin

package durable

import (
	"fmt"
	"os"
)

func strictlyAllocateFile(file *os.File, current, target int64) error {
	if file == nil || current < 0 || target <= 0 || current > target {
		return fmt.Errorf("invalid strict main-file allocation")
	}
	// F_PREALLOCATE can reserve a growing tail, but Darwin exposes no complete
	// physical extent-map proof that rules out an interior sparse range. SEEK_HOLE
	// is explicitly advisory and may report only the virtual EOF. A sealed
	// collection must therefore fail closed on Darwin until such a proof is
	// available; Truncate or F_PREALLOCATE without proof would weaken the API.
	return fmt.Errorf("strict main-file allocation proof is unavailable on Darwin")
}

func strictlySyncAllocatedFile(_ *os.File) error {
	return fmt.Errorf("strict main-file allocation proof is unavailable on Darwin")
}
