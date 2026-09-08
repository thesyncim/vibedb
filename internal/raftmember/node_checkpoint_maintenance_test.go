package raftmember

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
)

func nodeMaintenanceDescriptor(identity raftstore.Identity, member uint64) raftstore.GroupDescriptor {
	return raftstore.GroupDescriptor{
		TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
		AllocationGeneration:  identity.AllocationGeneration,
		MemberID:              member,
		GroupID:               identity.GroupID,
		ShardIncarnation:      identity.ShardIncarnation,
		StoreID:               identity.StoreID,
		Distribution:          identity.Distribution,
		Shard:                 identity.Shard,
	}
}

func nodeMaintenanceSnapshot(member uint64) *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	return &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index:     &index,
		Term:      &term,
		ConfState: &pb.ConfState{Voters: []uint64{member}},
	}}
}

func TestNodeMaintenanceDeferredRejectsPermanentErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "bounds", err: seglog.ErrBounds, want: true},
		{name: "joined bounds and backpressure", err: errors.Join(seglog.ErrBounds, raftstore.ErrSubmissionBackpressure), want: true},
		{name: "corrupt", err: seglog.ErrCorrupt, want: false},
		{name: "joined corrupt and bounds", err: errors.Join(seglog.ErrCorrupt, seglog.ErrBounds), want: false},
		{name: "unknown", err: raftstore.ErrPersistenceUnknown, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := nodeMaintenanceDeferred(test.err); got != test.want {
				t.Fatalf("nodeMaintenanceDeferred(%v)=%t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestNodeCheckpointCoordinatorMaintainsRegisteredDescriptors(t *testing.T) {
	base := testWALIdentity(241)
	node := raftstore.NodeIdentity{ClusterID: base.ClusterID, ClusterIncarnation: base.ClusterIncarnation, NodeID: base.StoreID}
	descriptor := nodeMaintenanceDescriptor(base, base.MemberID)
	dir := filepath.Join(t.TempDir(), "node")
	options := raftstore.NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8}
	store, err := raftstore.CreateNodeStore(dir, node, testWALKey(), []raftstore.NodeBootstrap{{
		Descriptor: descriptor,
		Snapshot:   nodeMaintenanceSnapshot(base.MemberID),
	}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	sequencer, err := raftstore.NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	coordinator, err := NewNodeCheckpointCoordinator(sequencer, 2)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := coordinator.Close(); err != nil {
			t.Error(err)
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()

	waitForCatalog := func(wantAttempts uint64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			stats := sequencer.Stats()
			catalogs, globErr := filepath.Glob(filepath.Join(dir, "checkpoints", "descriptor-catalog-*.chk"))
			if globErr == nil && stats.NodeMaintenanceAttempts >= wantAttempts && len(catalogs) == 1 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("maintenance attempts=%d catalogs=%v err=%v", stats.NodeMaintenanceAttempts, catalogs, globErr)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitForCatalog(1)

	second := testWALIdentity(242)
	second.GroupID[15] = base.GroupID[15] + 1
	second.StoreID[15] = base.StoreID[15] + 1
	secondDescriptor := nodeMaintenanceDescriptor(second, base.MemberID+1)
	var registration raftstore.Submission
	if err = registration.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err = registration.PrepareRegisterGroup(secondDescriptor); err != nil {
		t.Fatal(err)
	}
	if _, err = sequencer.TrySubmit(&registration); err != nil {
		t.Fatal(err)
	}
	if _, err = registration.Wait(); err != nil {
		t.Fatal(err)
	}
	waitForCatalog(2)
}
