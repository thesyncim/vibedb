//go:build unix

package rangesplit

import (
	"errors"
	"os"
)

func replaceChildStageCursorEntry(root *os.Root, temporary, destination string) error {
	return root.Rename(temporary, destination)
}

func syncChildStageCursorRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
