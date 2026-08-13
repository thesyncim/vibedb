//go:build linux

package durable

import (
	"os"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func strictlyAllocateFile(file *os.File, _ int64, target int64) error {
	return storeio.StrictlyAllocateFile(file, target)
}

func strictlySyncAllocatedFile(file *os.File) error { return file.Sync() }
