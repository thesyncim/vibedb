//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package driver

import (
	"errors"
	"os"
)

func replaceCatalogFile(from, to string) error {
	return os.Rename(from, to)
}

func publishNewPath(from, to string) error {
	return os.Rename(from, to)
}

func createPublishableTableTemp(directory, pattern string) (*os.File, error) {
	return os.CreateTemp(directory, pattern)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
