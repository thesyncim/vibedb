//go:build linux

package raftstore

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestNodeStoreMaintainsExactlyTwoPhysicalReserveSegments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{
		SegmentBytes: DefaultNodeSegmentBytes, MaxWaveBytes: 1 << 20, MaxSegmentEvents: 4096,
		RecentWaves: 1024, MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8,
	}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := os.ReadDir(filepath.Join(dir, nodeLogDir))
	if err != nil {
		t.Fatal(err)
	}
	segments, allocated := 0, int64(0)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "segment-") {
			continue
		}
		info, statErr := entry.Info()
		if statErr != nil {
			t.Fatal(statErr)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatalf("segment %q has no Linux stat", entry.Name())
		}
		segments++
		allocated += stat.Blocks * 512
	}
	want := int64(3 * DefaultNodeSegmentBytes)
	if segments != 3 || allocated != want {
		t.Fatalf("segments=%d allocated=%d want exact active+two reserves=%d", segments, allocated, want)
	}
}
