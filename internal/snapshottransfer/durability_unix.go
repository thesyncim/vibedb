//go:build unix

package snapshottransfer

import (
	"errors"
	"os"
)

func replaceRepositoryEntry(root *os.Root, temporary, destination string) error {
	return root.Rename(temporary, destination)
}

func syncRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
