package raftstore

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
)

func coordinateFixture(t *testing.T) (*NodeStore, string, NodeStoreOptions) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{
		{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)},
		{Descriptor: testGroupDescriptor(20), Snapshot: nodeSnapshot(20, 1, 1)},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err = store.BeginIncarnations([]uint64{1, 2}); err != nil {
		t.Fatal(err)
	}
	return store, dir, options
}

func TestNodeCoordinatesDoNotWaitForInflightDurability(t *testing.T) {
	store, dir, options := coordinateFixture(t)
	for _, group := range []uint64{1, 2} {
		if last, err := store.Group(group).LastIndex(); err != nil || last != 1 {
			t.Fatalf("warm group %d: %d %v", group, last, err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	store.SetDataSyncForTesting(func(file *os.File) error { close(entered); <-release; return file.Sync() })
	done := make(chan error, 1)
	go func() {
		done <- store.Group(1).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "value")}, HardState: hard(2, 2), MustSync: true})
	}()
	<-entered
	reads := make(chan error, 1)
	go func() {
		for _, group := range []uint64{1, 2} {
			view := store.Group(group)
			first, e := view.FirstIndex()
			if e != nil || first != 2 {
				reads <- errors.New("wrong first index before sync")
				return
			}
			last, e := view.LastIndex()
			if e != nil || last != 1 {
				reads <- errors.New("exposed unsynced last index")
				return
			}
			last, commit, e := view.LogBounds()
			if e != nil || last != 1 || commit != 1 {
				reads <- errors.New("exposed unsynced commit")
				return
			}
		}
		reads <- nil
	}()
	select {
	case err := <-reads:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("Raft coordinates blocked behind another group's data sync")
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	store.SetDataSyncForTesting(func(file *os.File) error { return file.Sync() })
	if last, commit, err := store.Group(1).LogBounds(); err != nil || last != 2 || commit != 2 {
		t.Fatalf("published bounds %d %d: %v", last, commit, err)
	}
	if err := store.publishGroupCheckpointSequenced(1, nodeSnapshot(10, 2, 2)); err != nil {
		t.Fatal(err)
	}
	if first, err := store.Group(1).FirstIndex(); err != nil || first != 3 {
		t.Fatalf("checkpoint first %d: %v", first, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Group(1).FirstIndex(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	reopened, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if first, e := reopened.Group(1).FirstIndex(); e != nil || first != 3 {
		t.Fatalf("recovered first %d: %v", first, e)
	}
	if last, commit, e := reopened.Group(1).LogBounds(); e != nil || last != 2 || commit != 2 {
		t.Fatalf("recovered bounds %d %d: %v", last, commit, e)
	}
}

func TestNodeCoordinatesInvalidateOnDurabilityFailure(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	for _, group := range []uint64{1, 2} {
		if _, err := store.Group(group).LastIndex(); err != nil {
			t.Fatal(err)
		}
	}
	injected := errors.New("sync outcome unknown")
	store.SetDataSyncForTesting(func(*os.File) error { return injected })
	err := store.Group(1).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "value")}, HardState: hard(2, 2), MustSync: true})
	if !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatal(err)
	}
	for _, group := range []uint64{1, 2} {
		view := store.Group(group)
		if _, e := view.FirstIndex(); !errors.Is(e, ErrPersistenceUnknown) {
			t.Fatal(e)
		}
		if _, e := view.LastIndex(); !errors.Is(e, ErrPersistenceUnknown) {
			t.Fatal(e)
		}
		if _, _, e := view.LogBounds(); !errors.Is(e, ErrPersistenceUnknown) {
			t.Fatal(e)
		}
	}
}

func TestNodeCoordinatesTrackSuffixReplacementWithoutAllocatingReads(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	view := store.Group(1)
	if _, err := view.LastIndex(); err != nil {
		t.Fatal(err)
	}
	if err := view.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "a"), typedEntry(3, 2, pb.EntryNormal, "b"), typedEntry(4, 2, pb.EntryNormal, "c")}, HardState: hard(2, 2), MustSync: true}); err != nil {
		t.Fatal(err)
	}
	if last, _, err := view.LogBounds(); err != nil || last != 4 {
		t.Fatalf("append last=%d err=%v", last, err)
	}
	if err := view.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{typedEntry(3, 3, pb.EntryNormal, "replacement")}, HardState: hard(3, 3), MustSync: true}); err != nil {
		t.Fatal(err)
	}
	if last, commit, err := view.LogBounds(); err != nil || last != 3 || commit != 3 {
		t.Fatalf("replacement %d %d: %v", last, commit, err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := view.FirstIndex(); err != nil {
			panic(err)
		}
		if _, err := view.LastIndex(); err != nil {
			panic(err)
		}
		if _, _, err := view.LogBounds(); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("coordinate read allocations=%v", allocations)
	}
	for group := uint64(100); group < 200; group++ {
		if _, err := store.Group(group).LastIndex(); err == nil {
			t.Fatal("accepted unknown group")
		}
	}
	if len(store.coordinates) != 1 {
		t.Fatal("unknown group reads grew the coordinate map")
	}
}

func TestNodeCoordinatesObserveIndependentEngineFailure(t *testing.T) {
	store, _, _ := coordinateFixture(t)
	ready := []NodeReady{{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "value")}, HardState: hard(2, 2), MustSync: true}}}
	if err := store.PersistWave(ready); err != nil {
		t.Fatal(err)
	}
	view := store.Group(1)
	if _, err := view.LastIndex(); err != nil {
		t.Fatal(err)
	}
	// Poison the engine directly, bypassing the NodeStore wrapper. Serving
	// copies must also observe failures published by engine maintenance paths.
	err := store.engine.PersistWave(seglog.Wave{ID: nodeWaveID(ready), Batches: []seglog.ReadyBatch{{GroupID: 1}}})
	if !errors.Is(err, seglog.ErrWaveConflict) {
		t.Fatal(err)
	}
	if store.poisoned != nil {
		t.Fatal("fixture went through NodeStore failure publication")
	}
	if _, err := view.LastIndex(); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatal(err)
	}
}
