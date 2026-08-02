//go:build unix

package gateway

import (
	"errors"
	"os"
)

// fsyncDir flushes a directory's own metadata so a rename published inside it
// survives a crash. On unix the directory is opened and synced; the recipe
// matches the SQL catalog's durable publication.
func fsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	return errors.Join(syncErr, closeErr)
}
