//go:build unix

package clusterbackup

import (
	"errors"
	"os"
)

func replaceBackupEntry(root *os.Root, source, destination string) error {
	if _, err := root.Lstat(destination); err == nil {
		return ErrRepository
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return root.Rename(source, destination)
}

func syncBackupRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
