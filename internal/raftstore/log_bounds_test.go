package raftstore

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestStoreLogBoundsMatchesVisibleCommitWithoutAllocation(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1,
		Entries: []*pb.Entry{entry(2, 2, "durable")}, HardState: hard(2, 1), MustSync: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2)}); err != nil {
		t.Fatal(err)
	}
	last, commit, err := store.LogBounds()
	if err != nil || last != 2 || commit != 2 {
		t.Fatalf("visible log bounds=%d/%d: %v", last, commit, err)
	}
	if floor, err := store.DurableCommit(); err != nil || floor != 1 {
		t.Fatalf("visible commit was confused with a persisted commit: %d: %v", floor, err)
	}
	if allocs := testing.AllocsPerRun(1000, func() { _, _, _ = store.LogBounds() }); allocs != 0 {
		t.Fatalf("LogBounds allocated %v", allocs)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LogBounds(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed bounds: %v", err)
	}
}

func TestNodeLogBoundsNeedsNoCheckpointReadOrAllocation(t *testing.T) {
	store, _, _ := registrationTestStore(t)
	group := store.Group(1)
	last, commit, err := group.LogBounds()
	if err != nil || last != 1 || commit != 1 {
		t.Fatalf("bootstrap log bounds=%d/%d: %v", last, commit, err)
	}
	if _, err := store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	if err := group.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1,
		Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "durable")}, HardState: hard(2, 2)}); err != nil {
		t.Fatal(err)
	}
	last, commit, err = group.LogBounds()
	if err != nil || last != 2 || commit != 2 {
		t.Fatalf("updated log bounds=%d/%d: %v", last, commit, err)
	}
	if allocs := testing.AllocsPerRun(1000, func() { _, _, _ = group.LogBounds() }); allocs != 0 {
		t.Fatalf("node LogBounds allocated %v", allocs)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := group.LogBounds(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed node bounds: %v", err)
	}
}
