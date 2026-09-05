package raftstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	"github.com/thesyncim/vibedb/internal/rf3bench"
	pb "go.etcd.io/raft/v3/raftpb"
)

func submitNodeMaintenanceReady(t testing.TB, q *NodeSubmissionSequencer, group, incarnation, id, index uint64, data []byte) {
	t.Helper()
	var submission Submission
	if err := submission.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err := submission.Prepare(NodeReady{GroupID: group, Batch: raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: id,
		Entries: []*pb.Entry{typedEntry(index, 2, pb.EntryNormal, string(data))}, HardState: hard(2, index),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.TrySubmit(&submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); err != nil {
		t.Fatal(err)
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

func nodeLogFileBytes(t testing.TB, dir string) int64 {
	t.Helper()
	footprint, err := rf3bench.MeasureFootprint(dir)
	if err != nil {
		t.Fatal(err)
	}
	return int64(footprint.ApparentBytes)
}

func nodeLogAllocatedBytes(t testing.TB, dir string) int64 {
	t.Helper()
	footprint, err := rf3bench.MeasureFootprint(dir)
	if err != nil {
		t.Fatal(err)
	}
	return int64(footprint.AllocatedBytes)
}

// This is a three-replica storage/recovery test, not a network throughput test.
// A slow group pins the oldest shared segment until its own checkpoint becomes
// durable; a newer checkpoint in another group cannot authorize its deletion.
func TestNodeLogMaintenanceReclaimsThreeReplicaHistory(t *testing.T) {
	var totalBefore, totalAfter int64
	var allocatedBefore, allocatedAfter int64
	for replica := byte(1); replica <= 3; replica++ {
		dir := filepath.Join(t.TempDir(), "node")
		identity := testNodeIdentity()
		identity.NodeID[15] = replica
		options := NodeStoreOptions{SegmentBytes: 1 << 20, MaxWaveBytes: 512 << 10,
			MaxSegmentEvents: 512, RecentWaves: 128, MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8}
		store, err := CreateNodeStore(dir, identity, testKey(), []NodeBootstrap{
			{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)},
			{Descriptor: testGroupDescriptor(2), Snapshot: nodeSnapshot(2, 1, 1)},
		}, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		incarnations, err := store.BeginIncarnations([]uint64{1, 2})
		if err != nil {
			t.Fatal(err)
		}
		q, err := NewNodeSubmissionSequencer(store, 8)
		if err != nil {
			t.Fatal(err)
		}
		seal := func() {
			t.Helper()
			if err := store.engine.Rotate(nil); err != nil {
				t.Fatal(err)
			}
			if err := store.engine.WaitSeal(); err != nil {
				t.Fatal(err)
			}
		}
		payload := bytes.Repeat([]byte{replica}, 128<<10)
		for index := uint64(2); index <= 9; index++ {
			submitNodeMaintenanceReady(t, q, 1, incarnations[0].Incarnation, index-1, index, payload)
			if index == 2 {
				submitNodeMaintenanceReady(t, q, 2, incarnations[1].Incarnation, 1, 2, []byte("retained slow-group row"))
			}
			seal()
		}
		submitNodeMaintenanceCheckpoint(t, q, 1, 1, 9)
		if err := q.MaintainNodeLog(); err != nil {
			t.Fatal(err)
		}
		seal()
		pinned := nodeLogFileBytes(t, dir)
		if err := q.MaintainNodeLog(); err != nil {
			t.Fatal(err)
		}
		if got := nodeLogFileBytes(t, dir); got != pinned {
			t.Fatalf("replica %d: slow group did not pin history: %d -> %d", replica, pinned, got)
		}
		entries, err := store.Group(2).Entries(2, 3, 1<<20)
		if err != nil || len(entries) != 1 || string(entries[0].Data) != "retained slow-group row" {
			t.Fatalf("slow-group read after maintenance: %v, %v", entries, err)
		}
		submitNodeMaintenanceCheckpoint(t, q, 2, 2, 2)
		seal()
		before := nodeLogFileBytes(t, dir)
		allocatedBefore += nodeLogAllocatedBytes(t, dir)
		if err := q.MaintainNodeLog(); err != nil {
			t.Fatal(err)
		}
		after := nodeLogFileBytes(t, dir)
		allocatedAfter += nodeLogAllocatedBytes(t, dir)
		if before-after < 8*int64(len(payload)) {
			t.Fatalf("replica %d retained dead row history: before=%d after=%d", replica, before, after)
		}
		totalBefore += before
		totalAfter += after
		// The same current generation still accepts and serves the next write.
		submitNodeMaintenanceReady(t, q, 1, incarnations[0].Incarnation, 9, 10, []byte("new row after reclaim"))
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store, err = OpenNodeStore(dir, identity, testKey(), options)
		if err != nil {
			t.Fatal(err)
		}
		for group, index := range map[uint64]uint64{1: 9, 2: 2} {
			snapshot, err := store.Group(group).Snapshot()
			if err != nil || snapshot.GetMetadata().GetIndex() != index {
				t.Fatalf("reopened group %d checkpoint: %v, %v", group, snapshot, err)
			}
		}
		entries, err = store.Group(1).Entries(10, 11, 1<<20)
		if err != nil || len(entries) != 1 || string(entries[0].Data) != "new row after reclaim" {
			t.Fatalf("reopened next row: %v, %v", entries, err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("three-replica apparent node bytes: before=%d after=%d reclaimed=%d (%.2f%%); preallocated blocks excluded",
		totalBefore, totalAfter, totalBefore-totalAfter, 100*float64(totalBefore-totalAfter)/float64(totalBefore))
	t.Logf("three-replica allocated node bytes including active+two reserves per replica: before=%d after=%d reclaimed=%d (%.2f%%)",
		allocatedBefore, allocatedAfter, allocatedBefore-allocatedAfter, 100*float64(allocatedBefore-allocatedAfter)/float64(allocatedBefore))
}

func TestNodeLogMaintenanceCatalogIODoesNotBlockReadsOrAppends(t *testing.T) {
	_, store, _ := createDescriptorCatalogTestStore(t, 8)
	defer store.Close()
	incarnations, err := store.BeginIncarnations([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	submitNodeMaintenanceReady(t, q, 1, incarnations[0].Incarnation, 1, 2, []byte("existing row"))
	entered, release := make(chan struct{}), make(chan struct{})
	store.descriptorCheckpointHookTest = func(phase DescriptorCheckpointPhase) error {
		if phase == DescriptorCheckpointTempWritten {
			close(entered)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- q.MaintainNodeLog() }()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance did not enter catalog I/O")
	}
	foreground := make(chan error, 1)
	go func() {
		entries, err := store.Group(1).Entries(2, 3, 1<<20)
		if err == nil && (len(entries) != 1 || string(entries[0].Data) != "existing row") {
			err = ErrCorrupt
		}
		if err == nil {
			var submission Submission
			err = submission.Initialize()
			if err == nil {
				err = submission.Prepare(NodeReady{GroupID: 1, Batch: raftmodel.PersistBatch{
					NodeIncarnation: incarnations[0].Incarnation, ReadyID: 2,
					Entries: []*pb.Entry{typedEntry(3, 2, pb.EntryNormal, "new row")}, HardState: hard(2, 3),
				}})
			}
			if err == nil {
				_, err = q.TrySubmit(&submission)
			}
			if err == nil {
				_, err = submission.Wait()
			}
		}
		foreground <- err
	}()
	select {
	case err := <-foreground:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("catalog I/O blocked foreground read/append")
	}
}

func TestNodeLogMaintenanceRetriesCatalogFailureAndSkipsUnchangedCatalog(t *testing.T) {
	dir, store, _ := createDescriptorCatalogTestStore(t, 8)
	defer store.Close()
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("catalog I/O failure")
	store.descriptorCheckpointHookTest = func(DescriptorCheckpointPhase) error { return injected }
	if err := q.MaintainNodeLog(); !errors.Is(err, injected) {
		t.Fatalf("maintenance lost catalog error: %v", err)
	}
	store.descriptorCheckpointHookTest = nil
	if err := q.MaintainNodeLog(); err != nil {
		t.Fatal(err)
	}
	sequence, _ := store.engine.AppendWitness()
	store.descriptorCheckpointHookTest = func(DescriptorCheckpointPhase) error { return injected }
	if err := q.MaintainNodeLog(); err != nil {
		t.Fatalf("unchanged catalog was republished: %v", err)
	}
	if got, _ := store.engine.AppendWitness(); got != sequence {
		t.Fatalf("unchanged maintenance appended %d waves", got-sequence)
	}
	entries, err := os.ReadDir(filepath.Join(dir, nodeCheckpointDir))
	if err != nil {
		t.Fatal(err)
	}
	catalogs := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "descriptor-catalog-") {
			catalogs++
		}
	}
	if catalogs != 1 {
		t.Fatalf("catalog files=%d, want one", catalogs)
	}
}

func TestNodeLogMaintenanceDoesNotHidePublishedEngineFailure(t *testing.T) {
	_, store, _ := createDescriptorCatalogTestStore(t, 8)
	defer store.Close()
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.MaintainNodeLog(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("independent engine failure")
	store.engine.SetDataSyncForTesting(func(*os.File) error { return injected })
	// Model a failure published independently of NodeStore's own poison box.
	err = store.engine.PersistWave(seglog.Wave{ID: seglog.WaveID{9, 8, 7}, Batches: []seglog.ReadyBatch{
		{GroupID: 1, Hard: &seglog.HardState{Term: 2, Commit: 1}},
	}})
	if !errors.Is(err, injected) {
		t.Fatalf("engine failure=%v", err)
	}
	if err := q.MaintainNodeLog(); !errors.Is(err, ErrPersistenceUnknown) || !errors.Is(err, injected) {
		t.Fatalf("maintenance treated a poisoned engine as below-threshold: %v", err)
	}
}
