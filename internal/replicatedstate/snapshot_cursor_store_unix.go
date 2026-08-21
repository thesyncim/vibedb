//go:build unix

package replicatedstate

import (
	"errors"
	"os"
)

func replaceSnapshotCursorEntry(root *os.Root, temporary, destination string) error {
	return root.Rename(temporary, destination)
}

func syncSnapshotCursorRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
