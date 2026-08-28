package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadBoundedRestoreInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operation.bin")
	if err := os.WriteFile(path, []byte("bound"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := readBoundedRestoreInput(path, 5)
	if err != nil || string(raw) != "bound" {
		t.Fatalf("raw=%q err=%v", raw, err)
	}
	if _, err = readBoundedRestoreInput(path, 4); err == nil {
		t.Fatal("oversized restore input accepted")
	}
	if _, err = readBoundedRestoreInput("relative", 5); err == nil {
		t.Fatal("relative restore input accepted")
	}
	empty := filepath.Join(t.TempDir(), "empty")
	if err = os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = readBoundedRestoreInput(empty, 5); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty err=%v", err)
	}
}
