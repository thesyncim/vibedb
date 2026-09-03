package raftstore

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestNodeStoreIdenticalSnapshotsAreIsolatedByGroup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1, MaxGroups: 4}
	snapshot := nodeSnapshot(1, 1, 1)
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{
		{Descriptor: testGroupDescriptor(1), Snapshot: snapshot},
		{Descriptor: testGroupDescriptor(2), Snapshot: snapshot},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for group := uint64(1); group <= 2; group++ {
		got, snapshotErr := store.Group(group).Snapshot()
		if snapshotErr != nil || !proto.Equal(got, snapshot) {
			t.Fatalf("group %d snapshot=%v err=%v", group, got, snapshotErr)
		}
	}
	files, err := os.ReadDir(filepath.Join(dir, nodeCheckpointDir))
	if err != nil || len(files) != 2 {
		t.Fatalf("checkpoint files=%v err=%v; want one authenticated object per group", files, err)
	}
}
