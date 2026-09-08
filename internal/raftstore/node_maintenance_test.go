package raftstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
)

func submitNodeMaintenanceReady(t testing.TB, q *NodeSubmissionSequencer, group, incarnation, readyID, index uint64, data string) {
	t.Helper()
	var submission Submission
	if err := submission.Initialize(); err != nil {
		t.Fatal(err)
	}
	entry := typedEntry(index, 2, pb.EntryNormal, data)
	if err := submission.Prepare(NodeReady{GroupID: group, Batch: raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         readyID,
		Entries:         []*pb.Entry{entry},
		HardState:       hard(2, index),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.TrySubmit(&submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); err != nil {
		t.Fatalf("ready group=%d incarnation=%d readyID=%d index=%d: %v", group, incarnation, readyID, index, err)
	}
}

func submitNodeMaintenanceCheckpoint(t testing.TB, q *NodeSubmissionSequencer, group, member, index uint64) {
	t.Helper()
	var submission Submission
	if err := submission.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err := submission.PrepareCheckpoint(group, nodeSnapshot(member, index, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.TrySubmit(&submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); err != nil {
		t.Fatal(err)
	}
}

func drainNodeMaintenanceWake(t testing.TB, q *NodeSubmissionSequencer) {
	t.Helper()
	for {
		select {
		case <-q.NodeMaintenanceWake():
		default:
			return
		}
	}
}

func nodeLogFileCount(t testing.TB, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, nodeLogDir))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			count++
		}
	}
	return count
}

func TestNodeMaintenanceSignalsDurableControlWaves(t *testing.T) {
	_, store, _ := createDescriptorCatalogTestStore(t, 8)
	defer store.Close()
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	drainNodeMaintenanceWake(t, q)

	var registration Submission
	if err = registration.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err = registration.PrepareRegisterGroup(testGroupDescriptor(200)); err != nil {
		t.Fatal(err)
	}
	if _, err = q.TrySubmit(&registration); err != nil {
		t.Fatal(err)
	}
	if _, err = registration.Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.NodeMaintenanceWake():
	default:
		t.Fatal("successful registration did not signal node maintenance")
	}

	var checkpoint Submission
	if err = checkpoint.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err = checkpoint.PrepareCheckpoint(1, nodeSnapshot(100, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err = q.TrySubmit(&checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err = checkpoint.Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.NodeMaintenanceWake():
	default:
		t.Fatal("successful group checkpoint did not signal node maintenance")
	}

	if err = store.engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.NodeMaintenanceWake():
	case <-time.After(5 * time.Second):
		t.Fatal("successful seal did not signal node maintenance")
	}
}

func TestNodeMaintenanceSkipsUnchangedDescriptorCatalog(t *testing.T) {
	dir, store, _ := createDescriptorCatalogTestStore(t, 8)
	defer store.Close()
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.MaintainNodeLog(); !errors.Is(err, seglog.ErrBounds) {
		t.Fatalf("initial metadata-only pass=%v, want below-threshold reclaim", err)
	}
	metadata, ok := store.engine.Metadata(nodeDescriptorGroup)
	if !ok || metadata.Checkpoint.Index != uint64(len(store.descriptors)) {
		t.Fatalf("descriptor checkpoint=%+v descriptors=%d", metadata, len(store.descriptors))
	}
	called := false
	store.descriptorCheckpointHookTest = func(DescriptorCheckpointPhase) error {
		called = true
		return errors.New("unchanged catalog should not be written")
	}
	if err = q.MaintainNodeLog(); !errors.Is(err, seglog.ErrBounds) {
		t.Fatalf("unchanged metadata-only pass=%v", err)
	}
	if called {
		t.Fatal("unchanged descriptor catalog performed checkpoint I/O")
	}
	if count := nodeLogFileCount(t, dir); count == 0 {
		t.Fatal("node log directory unexpectedly empty")
	}
}

func TestNodeMaintenanceLeavesLiveGroupPrefixPinned(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := descriptorCatalogTestOptions(8)
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{
		Descriptor: testGroupDescriptor(100),
		Snapshot:   nodeSnapshot(100, 1, 1),
	}}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	second := testGroupDescriptor(200)
	if _, err = store.RegisterGroupWithSnapshot(second, nodeSnapshot(200, 1, 1)); err != nil {
		t.Fatal(err)
	}
	incarnations, err := store.BeginIncarnations([]uint64{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(2); index <= 9; index++ {
		submitNodeMaintenanceReady(t, q, 1, incarnations[0].Incarnation, index-1, index, "group-one-history")
		if index == 2 {
			submitNodeMaintenanceReady(t, q, 2, incarnations[1].Incarnation, 1, 2, "live-group-two")
		}
		if err = store.engine.Rotate(nil); err != nil {
			t.Fatal(err)
		}
		if err = store.engine.WaitSeal(); err != nil {
			t.Fatal(err)
		}
	}
	submitNodeMaintenanceCheckpoint(t, q, 1, 100, 9)
	if err = store.engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = store.engine.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	before := nodeLogFileCount(t, dir)
	if err = q.MaintainNodeLog(); !errors.Is(err, seglog.ErrBounds) {
		t.Fatalf("maintenance with live group=%v", err)
	}
	after := nodeLogFileCount(t, dir)
	if after != before {
		t.Fatalf("live group failed to pin node-log prefix: before=%d after=%d", before, after)
	}
	entries, err := store.Group(2).Entries(2, 3, 1<<20)
	if err != nil || len(entries) != 1 || string(entries[0].Data) != "live-group-two" {
		t.Fatalf("live group row after maintenance: entries=%v err=%v", entries, err)
	}
	submitNodeMaintenanceCheckpoint(t, q, 2, second.MemberID, 2)
	if err = store.engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = store.engine.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	before = nodeLogFileCount(t, dir)
	if err = q.MaintainNodeLog(); err != nil && !errors.Is(err, seglog.ErrBounds) {
		t.Fatalf("maintenance after live-group checkpoint=%v", err)
	}
	if after = nodeLogFileCount(t, dir); after >= before {
		t.Fatalf("checkpointed live group did not release dead prefix: before=%d after=%d", before, after)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
}
