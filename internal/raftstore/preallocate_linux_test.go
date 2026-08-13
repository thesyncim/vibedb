//go:build linux

package raftstore

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestOpenRepairsPunchedHoleBeforeReturningHandle(t *testing.T) {
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
	before := allocatedBlocks(t, file)
	offset, length := options.MaxFileBytes-(2<<20), int64(1<<20)
	if err := unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, offset, length); err != nil {
		_ = file.Close()
		t.Skipf("filesystem cannot punch WAL hole: %v", err)
	}
	afterPunch := allocatedBlocks(t, file)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if afterPunch >= before {
		t.Skipf("filesystem did not report reduced allocation: before=%d after=%d", before, afterPunch)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatalf("Open repair: %v", err)
	}
	defer reopened.Close()
	if repaired := allocatedBlocks(t, reopened.file); repaired < before {
		t.Fatalf("allocation was not restored: before=%d repaired=%d", before, repaired)
	}
}

func allocatedBlocks(t *testing.T, file *os.File) int64 {
	t.Helper()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("file Stat did not expose allocation blocks")
	}
	return stat.Blocks * 512
}
