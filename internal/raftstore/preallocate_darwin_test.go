//go:build darwin

package raftstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestOpenRejectsPunchedHoleOnDarwin(t *testing.T) {
	options := testFormatOptions()
	options.ops = fileOps{preallocate: preallocate, ensureAllocated: ensureAllocated}
	path := filepath.Join(t.TempDir(), "hole.wal")
	store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	punch := &unix.Fstore_t{Offset: options.MaxFileBytes - (2 << 20), Length: 1 << 20}
	if err := unix.FcntlFstore(file.Fd(), unix.F_PUNCHHOLE, punch); err != nil {
		_ = file.Close()
		t.Skipf("filesystem cannot punch WAL hole: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options); !errors.Is(err, ErrFull) {
		t.Fatalf("Open sparse WAL = %v", err)
	}
}
