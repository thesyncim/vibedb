package raftmember

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestNodeRuntimePersistenceSharesOneSequencerWithoutGroupWorkers(t *testing.T) {
	base := testWALIdentity(3)
	var nodeID [16]byte
	copy(nodeID[:], []byte("runtime-node-001"))
	node := raftstore.NodeIdentity{ClusterID: base.ClusterID, ClusterIncarnation: base.ClusterIncarnation, NodeID: nodeID}
	descriptors := make([]raftstore.GroupDescriptor, 3)
	boots := make([]raftstore.NodeBootstrap, 3)
	identities := make([]RuntimeIdentity, 3)
	for i := range descriptors {
		member := uint64(i + 1)
		identity := base
		identity.MemberID = member
		identity.GroupID[15] += byte(i)
		identity.StoreID[15] += byte(i)
		descriptors[i] = raftstore.GroupDescriptor{TopologyRecoveryEpoch: 7, AllocationGeneration: identity.AllocationGeneration, MemberID: member, GroupID: identity.GroupID, ShardIncarnation: identity.ShardIncarnation, StoreID: identity.StoreID, Distribution: identity.Distribution, Shard: identity.Shard}
		index, term := uint64(1), uint64(1)
		boots[i] = raftstore.NodeBootstrap{Descriptor: descriptors[i], Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{member}}}}}
		identities[i] = RuntimeIdentity{Group: GroupKey{ClusterID: base.ClusterID, ClusterIncarnation: base.ClusterIncarnation, TopologyRecoveryEpoch: 7, ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID}, Distribution: identity.Distribution, Shard: identity.Shard, AllocationGeneration: identity.AllocationGeneration, MemberID: member, StoreID: identity.StoreID, NodeIncarnation: 1}
	}
	options := raftstore.NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8}
	store, err := raftstore.CreateNodeStore(filepath.Join(t.TempDir(), "node"), node, testWALKey(), boots, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	var syncs atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	store.SetDataSyncForTesting(func(file *os.File) error {
		if syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return file.Sync()
	})
	sequencer, err := raftstore.NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	lanes := make([]*NodeRuntimePersistence, 3)
	for i := range lanes {
		lanes[i], err = BindNodeRuntimePersistence(store, sequencer, identities[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	stale := identities[0]
	stale.NodeIncarnation++
	if _, err = BindNodeRuntimePersistence(store, sequencer, stale); !errors.Is(err, ErrNodePersistenceBinding) {
		t.Fatalf("stale incarnation binding=%v", err)
	}
	wrongIncarnation := raftmodel.PersistBatch{NodeIncarnation: 2, ReadyID: 1}
	if _, err = lanes[0].Submit(wrongIncarnation); !errors.Is(err, ErrNodePersistenceBinding) {
		t.Fatalf("wrong incarnation submit=%v", err)
	}
	for i := range lanes {
		entryIndex, entryTerm := uint64(2), uint64(2)
		if _, err = lanes[i].Submit(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{{Index: &entryIndex, Term: &entryTerm}}}); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			<-entered
		}
	}
	close(release)
	for i := range lanes {
		if _, err = lanes[i].Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if got := syncs.Load(); got != 2 {
		t.Fatalf("shared sequencer syncs=%d want one leading singleton plus one fused two-group wave", got)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAdoptNodeRuntimeDrivesWorkerFreeDurableReady(t *testing.T) {
	legacy := testWALIdentity(17)
	var nodeID [16]byte
	copy(nodeID[:], []byte("runtime-node-017"))
	node := raftstore.NodeIdentity{ClusterID: legacy.ClusterID, ClusterIncarnation: legacy.ClusterIncarnation, NodeID: nodeID}
	descriptor := raftstore.GroupDescriptor{
		TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
		AllocationGeneration:  legacy.AllocationGeneration,
		MemberID:              legacy.MemberID,
		GroupID:               legacy.GroupID,
		ShardIncarnation:      legacy.ShardIncarnation,
		StoreID:               legacy.StoreID,
		Distribution:          legacy.Distribution,
		Shard:                 legacy.Shard,
	}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{
		Data: []byte("raftmember-static-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: []uint64{legacy.MemberID}},
		},
	}
	options := raftstore.NodeStoreOptions{
		MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256,
		RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8,
	}
	store, err := raftstore.CreateNodeStore(
		filepath.Join(t.TempDir(), "node"), node, testWALKey(),
		[]raftstore.NodeBootstrap{{Descriptor: descriptor, Snapshot: bootstrap}}, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	group := store.Group(1)
	_, database, _ := prepareSQLRoot(t, legacy, "node-runtime")
	authority := testAuthorityProfile()
	base, err := BindPreparedNodeSQL(group, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind node runtime SQL", err)
	if err != nil {
		t.Fatal(err)
	}
	apply, _, err := OpenPreparedNodeApply(group, database, authority, base, testApplyOptions())
	skipIfStrictAllocationUnsupported(t, "open node runtime apply", err)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	identity, err := RuntimeIdentityFromNodeGroup(group, apply)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := raftstore.NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	persistence, err := BindNodeRuntimePersistence(store, sequencer, identity)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := AdoptNodeRuntime(persistence, database, apply)
	if err != nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatal(err)
	}
	if runtime.wal != nil || runtime.stable != group || runtime.pipelined == nil ||
		runtime.pipelined.workerWake != nil || runtime.pipelined.done != nil {
		t.Fatalf("node Runtime retained a per-group WAL worker: wal=%p worker=%v done=%v", runtime.wal, runtime.pipelined.workerWake, runtime.pipelined.done)
	}
	if err = runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	var workspace ReadyWorkspace
	for step := 0; step < 1000; step++ {
		result, driveErr := runtime.DriveReady(&workspace, func(OutboundMessage) error { return nil }, settleTestApplied)
		if driveErr != nil {
			t.Fatalf("DriveReady step %d: %v", step, driveErr)
		}
		if runtime.pipelined.nodeSubmission {
			if _, waitErr := persistence.Wait(); waitErr != nil {
				t.Fatalf("node persistence step %d: %v", step, waitErr)
			}
			continue
		}
		if !result.Progressed() {
			break
		}
		if step == 999 {
			t.Fatal("node Runtime Ready drain did not converge")
		}
	}
	status, err := runtime.Status()
	if err != nil || status.Term == 0 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = group.LastIndex(); err != nil {
		t.Fatalf("Runtime closed shared node store: %v", err)
	}
}
