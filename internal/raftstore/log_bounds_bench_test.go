package raftstore

import (
	"path/filepath"
	"testing"
)

func BenchmarkNodeLogCoordinates(b *testing.B) {
	store, err := CreateNodeStore(filepath.Join(b.TempDir(), "node"), testNodeIdentity(), testKey(),
		[]NodeBootstrap{{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)}},
		NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16,
			MaxEntriesPerGroup: 16, ReaderSlots: 1, MaxGroups: 2})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	group := store.Group(1)
	b.Run("bounds", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := group.LogBounds(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("initial-state", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := group.InitialState(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
