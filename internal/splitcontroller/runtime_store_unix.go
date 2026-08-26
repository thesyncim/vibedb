//go:build unix

package splitcontroller

import (
	"errors"
	"os"
)

func replaceRuntimeState(root *os.Root, temporary, destination string) error {
	return root.Rename(temporary, destination)
}

func syncRuntimeRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
