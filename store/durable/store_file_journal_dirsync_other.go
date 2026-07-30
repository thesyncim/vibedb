//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package durable

import (
	"errors"
	"os"
	"path/filepath"
)

func syncRecoveryJournalParent(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
