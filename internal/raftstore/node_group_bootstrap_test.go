package raftstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
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

func registrationTestStore(t *testing.T) (*NodeStore, string, NodeStoreOptions) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1, MaxGroups: 2}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{
		{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dir, options
}

func assertRegisteredSnapshot(t *testing.T, store *NodeStore, snapshot *pb.Snapshot) {
	t.Helper()
	view, exists := store.GroupByID(testGroupDescriptor(2).GroupID)
	if !exists || view.group != 2 {
		t.Fatalf("group was not registered: exists=%v view=%v", exists, view)
	}
	metadata, exists := store.engine.Metadata(2)
	if !exists || metadata.Hard != (seglog.HardState{Term: snapshot.GetMetadata().GetTerm(), Commit: snapshot.GetMetadata().GetIndex()}) ||
		metadata.NodeIncarnation != 1 || metadata.ReadyID != 0 || metadata.FirstIndex != snapshot.GetMetadata().GetIndex()+1 ||
		metadata.LastIndex != snapshot.GetMetadata().GetIndex() {
		t.Fatalf("bootstrap metadata=%+v exists=%v", metadata, exists)
	}
	got, err := view.Snapshot()
	if err != nil || !proto.Equal(got, snapshot) {
		t.Fatalf("bootstrap snapshot=%v err=%v", got, err)
	}
}

func TestNodeStoreRegistersSnapshotAndFenceInOneLogWave(t *testing.T) {
	store, dir, options := registrationTestStore(t)
	snapshot := nodeSnapshot(2, 41, 7)
	snapshot.Metadata.ConfState = &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	syncs := 0
	store.SetDataSyncForTesting(func(file *os.File) error {
		syncs++
		return file.Sync()
	})
	incarnation, err := store.RegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot)
	if err != nil || incarnation != (GroupIncarnation{GroupID: 2, Incarnation: 1}) || syncs != 1 {
		t.Fatalf("register=%+v err=%v node-log syncs=%d", incarnation, err, syncs)
	}
	assertRegisteredSnapshot(t, store, snapshot)
	// The group limit is now full. A retry must still succeed without another
	// log write, and a different request must not be mistaken for that retry.
	if again, retryErr := store.RegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot); retryErr != nil || again != incarnation || syncs != 1 {
		t.Fatalf("full-capacity retry=%+v err=%v syncs=%d", again, retryErr, syncs)
	}
	if _, err = store.RegisterGroupWithSnapshot(testGroupDescriptor(3), snapshot); !errors.Is(err, ErrBounds) {
		t.Fatalf("over capacity=%v", err)
	}
	if err = store.engine.DeepVerify(); err != nil {
		t.Fatalf("active bootstrap log verification: %v", err)
	}
	if err = store.engine.Rotate(nil); err != nil {
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
	assertRegisteredSnapshot(t, store, snapshot)
	if err = store.engine.DeepVerify(); err != nil {
		t.Fatalf("sealed bootstrap log verification: %v", err)
	}
	if again, retryErr := store.RegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot); retryErr != nil || again != incarnation {
		t.Fatalf("reopened retry=%+v err=%v", again, retryErr)
	}
	// Ready 1 can append immediately after the snapshot without a second
	// bootstrap fence or an invented empty prefix.
	if err = store.Group(2).Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1,
		Entries: []*pb.Entry{typedEntry(42, 7, pb.EntryNormal, "after bootstrap")}, HardState: hard(7, 42)}); err != nil {
		t.Fatal(err)
	}
}

func TestNodeStoreSnapshotRegistrationRejectsInvalidAndConflictingBase(t *testing.T) {
	store, _, _ := registrationTestStore(t)
	descriptor := testGroupDescriptor(2)
	invalid := []*pb.Snapshot{nil, {}, nodeSnapshot(2, 0, 1), nodeSnapshot(2, 1, 0)}
	missingMember := nodeSnapshot(2, 1, 1)
	missingMember.Metadata.ConfState.Voters = []uint64{2, 3, 4}
	invalid = append(invalid, missingMember)
	for i, snapshot := range invalid {
		if _, err := store.RegisterGroupWithSnapshot(descriptor, snapshot); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid %d=%v", i, err)
		}
		if _, exists := store.GroupByID(descriptor.GroupID); exists {
			t.Fatalf("invalid %d published descriptor", i)
		}
	}
	snapshot := nodeSnapshot(2, 41, 7)
	if _, err := store.RegisterGroupWithSnapshot(descriptor, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []*pb.Snapshot{nodeSnapshot(2, 40, 7), nodeSnapshot(2, 41, 8), nodeSnapshot(3, 41, 7)} {
		if _, err := store.RegisterGroupWithSnapshot(descriptor, candidate); !errors.Is(err, ErrRetryConflict) {
			t.Fatalf("conflicting snapshot=%v err=%v", candidate, err)
		}
	}
	descriptor.StoreID[0] ^= 1
	if _, err := store.RegisterGroupWithSnapshot(descriptor, snapshot); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("changed descriptor=%v", err)
	}
	assertRegisteredSnapshot(t, store, snapshot)
}

func TestNodeStoreSnapshotRegistrationCrashCutsLeaveNoPartialGroup(t *testing.T) {
	for _, phase := range []CheckpointPhase{CheckpointTempWritten, CheckpointFileSynced, CheckpointRenamed, CheckpointDirectorySynced, CheckpointBeforeLogReference} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			store, dir, options := registrationTestStore(t)
			injected := errors.New("interrupted bootstrap")
			store.checkpointHookTest = func(got CheckpointPhase) error {
				if got == phase {
					return injected
				}
				return nil
			}
			store.checkpointLeaveTempTest = phase == CheckpointTempWritten || phase == CheckpointFileSynced
			snapshot := nodeSnapshot(2, 41, 7)
			if _, err := store.RegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot); !errors.Is(err, injected) {
				t.Fatalf("injected registration=%v", err)
			}
			if _, exists := store.GroupByID(testGroupDescriptor(2).GroupID); exists {
				t.Fatal("descriptor published before the complete bootstrap wave")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, exists := store.GroupByID(testGroupDescriptor(2).GroupID); exists {
				t.Fatal("recovery inferred a descriptor from an unreferenced snapshot")
			}
			if _, err = store.RegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot); err != nil {
				t.Fatalf("retry after interrupted publication=%v", err)
			}
			assertRegisteredSnapshot(t, store, snapshot)
		})
	}
}

func TestNodeStoreSnapshotRegistrationUnknownSyncRecoversAtomically(t *testing.T) {
	store, dir, options := registrationTestStore(t)
	injected := errors.New("lost bootstrap sync result")
	store.SetDataSyncForTesting(func(file *os.File) error {
		if err := file.Sync(); err != nil {
			return err
		}
		return injected
	})
	snapshot := nodeSnapshot(2, 41, 7)
	if _, err := store.RegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot); !errors.Is(err, ErrPersistenceUnknown) || !errors.Is(err, injected) {
		t.Fatalf("unknown outcome=%v", err)
	}
	if _, exists := store.GroupByID(testGroupDescriptor(2).GroupID); exists {
		t.Fatal("uncertain bootstrap was acknowledged")
	}
	_ = store.Close()
	store, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertRegisteredSnapshot(t, store, snapshot)
	store.SetDataSyncForTesting(func(*os.File) error { t.Fatal("exact recovered retry performed another log sync"); return nil })
	if _, err = store.RegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot); err != nil {
		t.Fatal(err)
	}
}

func TestNodeSubmissionSnapshotRegistrationOrdersBeforeFirstReady(t *testing.T) {
	store, _, _ := registrationTestStore(t)
	if _, err := store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	first := true
	store.persistWaveTest = func(wave seglog.Wave) error {
		if first {
			first = false
			close(entered)
			<-release
		}
		return store.engine.PersistWave(wave)
	}
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer sequencer.Close()
	snapshot := nodeSnapshot(2, 41, 7)
	if _, err = store.RegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot); !errors.Is(err, ErrSequencerActive) {
		t.Fatalf("direct sequencer bypass=%v", err)
	}
	prior := preparedSubmission(t, 1, 1)
	prior.Ready.Batch.Entries = []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "prior")}
	prior.Ready.Batch.HardState = hard(2, 2)
	if _, err = sequencer.TrySubmit(prior); err != nil {
		t.Fatal(err)
	}
	<-entered
	// Release the deliberately blocked writer before deferred shutdown even
	// when preparation/assertion fails.
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var control Submission
	if err = control.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err = control.PrepareRegisterGroupWithSnapshot(testGroupDescriptor(2), snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err = sequencer.TrySubmit(&control); err != nil {
		t.Fatal(err)
	}
	next := preparedSubmission(t, 2, 1)
	next.Ready.Batch.Entries = []*pb.Entry{typedEntry(42, 7, pb.EntryNormal, "next")}
	next.Ready.Batch.HardState = hard(7, 42)
	if _, err = sequencer.TrySubmit(next); err != nil {
		t.Fatal(err)
	}
	close(release)
	var lastTicket uint64
	for _, cell := range []*Submission{prior, &control, next} {
		ticket, waitErr := cell.Wait()
		if waitErr != nil || ticket <= lastTicket {
			t.Fatalf("ticket=%d previous=%d err=%v", ticket, lastTicket, waitErr)
		}
		lastTicket = ticket
	}
	descriptor, incarnation, ok := control.RegisteredGroup()
	if !ok || descriptor.LogKey != 2 || incarnation != (GroupIncarnation{GroupID: 2, Incarnation: 1}) {
		t.Fatalf("result=%+v %+v ok=%v", descriptor, incarnation, ok)
	}
	metadata, _ := store.engine.Metadata(2)
	if metadata.ReadyID != 1 || metadata.Hard.Commit != 42 || metadata.Checkpoint.Index != 41 {
		t.Fatalf("post-bootstrap Ready=%+v", metadata)
	}
	if err = control.PrepareRegisterGroupWithSnapshot(testGroupDescriptor(2), nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil snapshot prepare=%v", err)
	}
	if _, err = sequencer.TrySubmit(&control); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid prepare left stale submission=%v", err)
	}
	if err = control.Prepare(NodeReady{GroupID: 2, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2}}); err != nil {
		t.Fatal(err)
	}
	if control.snapshot != nil {
		t.Fatal("reused Ready cell retained bootstrap snapshot")
	}
}
