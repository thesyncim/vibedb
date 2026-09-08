package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRF3RecoveredChildReopensItsWALWithoutNodeBootstrap(t *testing.T) {
	root := t.TempDir()
	keyPath := filepath.Join(root, "wal-key")
	material := bytes.Repeat([]byte{0x2a}, 32)
	if err := os.WriteFile(keyPath, material, 0o600); err != nil {
		t.Fatal(err)
	}
	identity := raftstore.Identity{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		Distribution: "data", Shard: "child", AllocationGeneration: 3,
		ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}, MemberID: 2,
		StoreID: [16]byte{6},
	}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{Data: []byte("retained-child"), Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
	}}
	var keyMaterial [32]byte
	copy(keyMaterial[:], material)
	key := raftstore.Key{ID: "child-key", Material: keyMaterial, Wrapped: []byte("retained-provider-metadata")}
	walPath := filepath.Join(root, "split-children", "operation", "child-1", "child.wal")
	if err := os.MkdirAll(filepath.Dir(walPath), 0o700); err != nil {
		t.Fatal(err)
	}
	wal, err := raftstore.Create(walPath, identity, key, raftstore.Bootstrap{
		TopologyRecoveryEpoch: 7, Snapshot: bootstrap,
	}, raftstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	key, err = loadRF3WALKey(key.ID, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := raftstore.Open(walPath, identity, 7, key, raftstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	owner := &rf3NodeOwner{}
	if got := rf3SelectedNodeOwner(owner, 0, 1); got != owner {
		t.Fatal("manifest group did not retain shared node-log ownership")
	}
	if got := rf3SelectedNodeOwner(owner, 1, 1); got != nil {
		t.Fatal("adopted child retained shared node-log ownership")
	}
	source := &preparedRF3Group{wal: reopened}
	if source.nodeOwner != nil || source.nodeLog != nil || source.recoveryLog() != reopened {
		t.Fatal("adopted child did not retain the independent WAL backend")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(walPath), "node-bootstrap.pb")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child unexpectedly depends on node bootstrap: %v", err)
	}
	registry := rf3ManifestSplitChildRegistry{WAL: rf3ManifestSplitChildWAL{
		KeyID: key.ID, KeyMaterialPath: keyPath,
	}}
	childKey, err := loadRF3SplitChildWALKey(registry, source)
	if err != nil || !bytes.Equal(childKey.Wrapped, []byte("retained-provider-metadata")) {
		t.Fatalf("reopened child WAL key metadata = %q, %v", childKey.Wrapped, err)
	}
	clear(childKey.Material[:])
	snapshot, err := source.recoveryLog().Snapshot()
	if err != nil || !bytes.Equal(snapshot.GetData(), bootstrap.GetData()) {
		t.Fatalf("reopened child WAL snapshot = %q, %v", snapshot.GetData(), err)
	}
}
