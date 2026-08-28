//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Two maintenance cadences below the original bound distinguish settled space
// from a brief gap between discarding a stale candidate and building its exact
// replacement. This observes maintenance, never drives or deletes it.
func waitWALRetentionSettlement(ctx context.Context, maximum int64, stableFor time.Duration,
	sample func() (int64, error)) (int64, error) {
	if maximum <= 0 || stableFor <= 0 || sample == nil {
		return 0, errors.New("invalid WAL settlement bounds")
	}
	var stableSince time.Time
	var allocated int64
	var sampleErr error
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return allocated, errors.Join(ctx.Err(), sampleErr)
		default:
		}
		allocated, sampleErr = sample()
		if sampleErr != nil && !errors.Is(sampleErr, errWALAllocationSampleChanged) {
			return allocated, sampleErr
		}
		if sampleErr != nil || allocated <= 0 || allocated > maximum {
			stableSince = time.Time{}
		} else if stableSince.IsZero() {
			stableSince = time.Now()
		} else if time.Since(stableSince) >= stableFor {
			return allocated, nil
		}
		select {
		case <-ctx.Done():
			return allocated, errors.Join(ctx.Err(), sampleErr)
		case <-ticker.C:
		}
	}
}

func logWALRetentionInventory(t testing.TB, paths []string) {
	t.Helper()
	for _, path := range paths {
		parent, base := filepath.Dir(path), filepath.Base(path)
		entries, err := os.ReadDir(parent)
		if err != nil {
			t.Logf("WAL inventory %q: %v", parent, err)
			continue
		}
		shown := 0
		for _, entry := range entries {
			if entry.Name() != base && !strings.HasPrefix(entry.Name(), base+".") && !strings.HasPrefix(entry.Name(), ".vibedb-raft-") {
				continue
			}
			if shown == 48 {
				t.Logf("WAL inventory %q: further names omitted from diagnostics only", parent)
				break
			}
			shown++
			info, err := entry.Info()
			if err != nil {
				t.Logf("WAL inventory %q: %v", filepath.Join(parent, entry.Name()), err)
				continue
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				t.Logf("WAL inventory %q: missing physical stat", filepath.Join(parent, entry.Name()))
				continue
			}
			t.Logf("WAL inventory %q mode=%s device=%d inode=%d links=%d bytes=%d blocks=%d",
				filepath.Join(parent, entry.Name()), info.Mode(), stat.Dev, stat.Ino, stat.Nlink, info.Size(), stat.Blocks)
		}
	}
}

func TestWALRetentionSettlementRequiresStableOriginalBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	calls := 0
	got, err := waitWALRetentionSettlement(ctx, 100, 60*time.Millisecond, func() (int64, error) {
		calls++
		switch calls {
		case 1, 3:
			return 200, nil
		case 4:
			return 0, errWALAllocationSampleChanged
		default:
			return 100, nil
		}
	})
	if err != nil || got != 100 || calls < 6 {
		t.Fatalf("settlement=%d calls=%d err=%v", got, calls, err)
	}
}

func TestWALRetentionSettlementDoesNotHidePersistentOrInvalidStorage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	got, err := waitWALRetentionSettlement(ctx, 100, time.Millisecond, func() (int64, error) { return 101, nil })
	if got != 101 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("persistent excess=%d err=%v", got, err)
	}
	want := errors.New("invalid physical inode")
	_, err = waitWALRetentionSettlement(t.Context(), 100, time.Millisecond, func() (int64, error) { return 0, want })
	if !errors.Is(err, want) {
		t.Fatalf("invalid evidence error=%v", err)
	}
}

func TestWALRetentionSettlementCountsPrivatePhysicalInodes(t *testing.T) {
	root := t.TempDir()
	logical, private := filepath.Join(root, "member.wal"), filepath.Join(root, ".vibedb-raft-candidate")
	for _, path := range []string{logical, private} {
		if err := os.WriteFile(path, make([]byte, 8192), 0600); err != nil {
			t.Fatal(err)
		}
	}
	logicalInfo, err := os.Stat(logical)
	if err != nil {
		t.Fatal(err)
	}
	maximum := int64(logicalInfo.Sys().(*syscall.Stat_t).Blocks) * 512
	if maximum == 0 {
		t.Skip("filesystem does not report allocated fixture blocks")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	got, err := waitWALRetentionSettlement(ctx, maximum, time.Millisecond, func() (int64, error) { return rf3FaultWALDirectoryAllocatedBytes([]string{logical}) })
	if !errors.Is(err, context.DeadlineExceeded) || got <= maximum {
		t.Fatalf("private inode ignored: got=%d maximum=%d err=%v", got, maximum, err)
	}
}
