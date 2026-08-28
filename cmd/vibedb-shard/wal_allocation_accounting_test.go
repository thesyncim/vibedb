//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// Each fixture owns its WAL parent directory. Include private family,
// candidate and retained generations, not just the public logical filename.
// Count physical inodes once: activation may give one inode several names.
func rf3FaultWALDirectoryAllocatedBytes(paths []string) (int64, error) {
	return rf3FaultWALAllocatedWithInfo(paths, func(entry os.DirEntry) (os.FileInfo, error) {
		return entry.Info()
	})
}

var errWALAllocationSampleChanged = errors.New("WAL allocation sample changed during reclamation")

func rf3FaultWALAllocatedWithInfo(paths []string, info func(os.DirEntry) (os.FileInfo, error)) (int64, error) {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		var total int64
		total, err = rf3FaultWALAllocationSample(paths, info)
		if !errors.Is(err, errWALAllocationSampleChanged) {
			return total, err
		}
	}
	return 0, err
}

func rf3FaultWALAllocationSample(paths []string, inspect func(os.DirEntry) (os.FileInfo, error)) (int64, error) {
	type identity struct{ device, inode uint64 }
	seen := make(map[identity]struct{})
	var total int64
	for _, path := range paths {
		logical, err := os.Lstat(path)
		if err != nil || !logical.Mode().IsRegular() {
			return 0, errors.Join(fmt.Errorf("invalid logical WAL %q", path), err)
		}
		parent, base := filepath.Dir(path), filepath.Base(path)
		entries, err := os.ReadDir(parent)
		if err != nil {
			return 0, err
		}
		for _, entry := range entries {
			name := entry.Name()
			if name != base && !strings.HasPrefix(name, base+".") && !strings.HasPrefix(name, ".vibedb-raft-") {
				continue
			}
			info, err := inspect(entry)
			if err != nil {
				if name != base && errors.Is(err, os.ErrNotExist) {
					return 0, errors.Join(errWALAllocationSampleChanged, err)
				}
				return 0, err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !info.Mode().IsRegular() || !ok || stat.Blocks < 0 || stat.Blocks > math.MaxInt64/512 {
				return 0, fmt.Errorf("invalid physical WAL evidence %q", filepath.Join(parent, name))
			}
			id := identity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}
			if _, duplicate := seen[id]; duplicate {
				continue
			}
			seen[id] = struct{}{}
			allocated := int64(stat.Blocks) * 512
			if allocated > math.MaxInt64-total {
				return 0, fmt.Errorf("physical WAL allocation overflow")
			}
			total += allocated
		}
	}
	return total, nil
}

func TestWALAllocationRetriesOnlyVanishedPrivateEntries(t *testing.T) {
	root := t.TempDir()
	logical := filepath.Join(root, "member.wal")
	private := filepath.Join(root, ".vibedb-raft-retired.wal")
	for _, path := range []string{logical, private} {
		if err := os.WriteFile(path, make([]byte, 4096), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	removed := false
	got, err := rf3FaultWALAllocatedWithInfo([]string{logical}, func(entry os.DirEntry) (os.FileInfo, error) {
		if entry.Name() == filepath.Base(private) && !removed {
			removed = true
			if err := os.Remove(private); err != nil {
				t.Fatal(err)
			}
		}
		return entry.Info()
	})
	info, statErr := os.Stat(logical)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if err != nil || !removed || got != info.Sys().(*syscall.Stat_t).Blocks*512 {
		t.Fatalf("reclaimed sample: allocated=%d removed=%t err=%v", got, removed, err)
	}
	if err := os.WriteFile(private, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	_, err = rf3FaultWALAllocatedWithInfo([]string{logical}, func(entry os.DirEntry) (os.FileInfo, error) {
		if entry.Name() == filepath.Base(private) {
			attempts++
			return nil, os.ErrNotExist
		}
		return entry.Info()
	})
	if !errors.Is(err, errWALAllocationSampleChanged) || attempts != 3 {
		t.Fatalf("unstable sample: attempts=%d err=%v", attempts, err)
	}
	attempts = 0
	_, err = rf3FaultWALAllocatedWithInfo([]string{logical}, func(entry os.DirEntry) (os.FileInfo, error) {
		if entry.Name() == filepath.Base(logical) {
			attempts++
			return nil, os.ErrNotExist
		}
		return entry.Info()
	})
	if !errors.Is(err, os.ErrNotExist) || errors.Is(err, errWALAllocationSampleChanged) || attempts != 1 {
		t.Fatalf("missing logical entry was retried: attempts=%d err=%v", attempts, err)
	}
}

func TestWALAllocationIncludesPrivateGenerationsOnce(t *testing.T) {
	root := t.TempDir()
	write := func(name string, size int) string {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	logical := write("member.wal", 4096)
	family := write(".vibedb-raft-test.family", 4096)
	retained := write(".vibedb-raft-test.g0001.wal", 8192)
	write("member.vdb", 65536) // SQL bytes are not WAL bytes.
	alias := filepath.Join(root, ".vibedb-raft-test.g0002.wal")
	if err := os.Link(logical, alias); err != nil {
		t.Fatal(err)
	}
	var want int64
	for _, path := range []string{logical, family, retained} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		want += info.Sys().(*syscall.Stat_t).Blocks * 512
	}
	got, err := rf3FaultWALDirectoryAllocatedBytes([]string{logical, logical})
	if err != nil || got != want || got == 0 {
		t.Fatalf("allocated=%d want=%d err=%v", got, want, err)
	}
	if err := os.Remove(retained); err != nil {
		t.Fatal(err)
	}
	after, err := rf3FaultWALDirectoryAllocatedBytes([]string{logical})
	if err != nil || after >= got {
		t.Fatalf("retirement did not reduce physical accounting: before=%d after=%d err=%v", got, after, err)
	}
}

func TestWALAllocationRejectsMissingLogicalFileAndSymlink(t *testing.T) {
	root := t.TempDir()
	logical := filepath.Join(root, "member.wal")
	if _, err := rf3FaultWALDirectoryAllocatedBytes([]string{logical}); err == nil {
		t.Fatal("missing logical WAL accepted")
	}
	if err := os.WriteFile(logical, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(logical, filepath.Join(root, ".vibedb-raft-alias")); err != nil {
		t.Fatal(err)
	}
	if _, err := rf3FaultWALDirectoryAllocatedBytes([]string{logical}); err == nil {
		t.Fatal("symlink WAL evidence accepted")
	}
}
