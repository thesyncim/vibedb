//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package durable

import (
	"errors"
	"os"
	"path/filepath"
)

// syncRecoveryJournalParent makes the freshly created sibling's namespace
// entry durable before the store publishes a root that requires it.
func syncRecoveryJournalParent(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
