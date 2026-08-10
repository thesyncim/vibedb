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

// fsyncCatalogRoot flushes the already pinned catalog namespace rather than
// resolving its path again after publication.
func fsyncCatalogRoot(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func catalogDurabilitySupported() bool { return true }

func replaceCatalogEntry(root *os.Root, temporary, destination string) error {
	return root.Rename(temporary, destination)
}
