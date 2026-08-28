//go:build !windows

package distributedtxn

import (
	"errors"
	"os"
	"path/filepath"
)

func installJournalCompaction(from, to string, _, _ *os.File) (bool, bool, error) {
	return false, false, os.Rename(from, to)
}

func syncJournalDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
