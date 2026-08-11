//go:build !linux && !darwin

package raftstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestUnsupportedPlatformFailsBeforeWALMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unsupported.wal")
	if _, err := Create(path, testIdentity(), testKey(), testBootstrap(), testOptions()); !errors.Is(err, ErrPlatformUnsupported) {
		t.Fatalf("Create = %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported Create mutated WAL path: %v", err)
	}
	if _, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), testOptions()); !errors.Is(err, ErrPlatformUnsupported) {
		t.Fatalf("Open = %v", err)
	}
}
