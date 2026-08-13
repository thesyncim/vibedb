//go:build darwin

package durable

import (
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func strictlyAllocateFile(file *os.File, current, target int64) error {
	if file == nil || current < 0 || target <= 0 || current > target {
		return fmt.Errorf("invalid strict main-file allocation")
	}
	return storeio.StrictlyAllocateFile(file, target)
}

func strictlySyncAllocatedFile(_ *os.File) error {
	return fmt.Errorf("strict main-file allocation proof is unavailable on Darwin")
}
