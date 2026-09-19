package raftstore

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestNodeReplicaReplacementFencesOldLogAndSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(10), Snapshot: nodeSnapshot(10, 1, 1)}}, NodeStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	old, _ := store.GroupByID(testGroupDescriptor(10).GroupID)
	previous, _ := old.Descriptor()
	if _, err := store.BeginIncarnations([]uint64{previous.LogKey}); err != nil {
		t.Fatal(err)
	}
	next := previous
	next.LogKey, next.MemberID, next.StoreID = 0, 4, [16]byte{94}
	snapshot := nodeSnapshot(10, 5, 2)
	snapshot.Metadata.ConfState = &pb.ConfState{Voters: []uint64{2, 3, 4}}
	for reopen := 0; reopen < 3; reopen++ {
		if reopen != 0 {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), NodeStoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CheckpointDescriptorCatalog(); err != nil {
				t.Fatal(err)
			}
		}
		sequencer, err := NewNodeSubmissionSequencer(store, 8)
		if err != nil {
			t.Fatal(err)
		}
		submit := func(prior *GroupDescriptor, image *pb.Snapshot) error {
			var submission Submission
			if err := submission.Initialize(); err != nil {
				return err
			}
			var err error
			if prior == nil {
				err = submission.PrepareRegisterGroupWithSnapshotAt(next, image, 7)
			} else {
				err = submission.PrepareReplaceGroupWithSnapshotAt(*prior, next, image, 7)
			}
			if err != nil {
				return err
			}
			if _, err = sequencer.TrySubmit(&submission); err != nil {
				return err
			}
			_, err = submission.Wait()
			return err
		}
		if reopen == 0 {
			if err := submit(nil, snapshot); !errors.Is(err, ErrIdentityMismatch) {
				t.Fatalf("unproved replacement: %v", err)
			}
			wrong := previous
			wrong.StoreID[0]++
			if err := submit(&wrong, snapshot); !errors.Is(err, ErrIdentityMismatch) {
				t.Fatalf("wrong predecessor: %v", err)
			}
			stillPresent := nodeSnapshot(10, 5, 2)
			stillPresent.Metadata.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
			if err := submit(&previous, stillPresent); !errors.Is(err, ErrIdentityMismatch) {
				t.Fatalf("live predecessor: %v", err)
			}
		}
		for retry := 0; retry < 2; retry++ {
			if err := submit(&previous, snapshot); err != nil {
				t.Fatalf("reopen=%d retry=%d: %v", reopen, retry, err)
			}
		}
		current, _ := store.GroupByID(previous.GroupID)
		descriptor, err := current.Descriptor()
		if err != nil || descriptor.MemberID != 4 || descriptor.LogKey == previous.LogKey {
			t.Fatalf("current=%+v err=%v", descriptor, err)
		}
		if incarnation, err := current.NodeIncarnation(); err != nil || incarnation != 7 {
			t.Fatalf("incarnation=%d err=%v", incarnation, err)
		}
		if oldDescriptor, err := store.Group(previous.LogKey).Descriptor(); err != nil || oldDescriptor != previous {
			t.Fatalf("old storage changed: %+v %v", oldDescriptor, err)
		}
		// The replacement wave invalidates old in-flight persistence even if
		// a stale runtime retains the old GroupView after close/publication.
		var stale Submission
		_ = stale.Initialize()
		_ = stale.Prepare(NodeReady{GroupID: previous.LogKey, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1,
			Entries: []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "stale")}, HardState: hard(2, 2)}})
		if _, err := sequencer.TrySubmit(&stale); err != nil {
			t.Fatal(err)
		}
		if _, err := stale.Wait(); err == nil {
			t.Fatal("old runtime persisted after replacement")
		}
		if err := sequencer.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
