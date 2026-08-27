package main

import (
	"os"
	"testing"
)

func restoreRF3PrivateProcessRoot(t *testing.T) string {
	t.Helper()
	// testing.TempDir's numbered child inherits the ambient umask (0777 at
	// creation), but activation authority/session directories require 0700.
	// MkdirTemp supplies that exact private contract without changing umask.
	root, err := os.MkdirTemp(t.TempDir(), "restore-authority-")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestRestoreRF3FixtureRootIsPrivate(t *testing.T) {
	root := restoreRF3PrivateProcessRoot(t)
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		t.Fatalf("restore fixture authority root must be a private real directory: info=%v err=%v", info, err)
	}
}
