package raftstore

import (
	"errors"
	"os"
)

func syncPinnedDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
