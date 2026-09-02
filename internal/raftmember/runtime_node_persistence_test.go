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
	options := raftstore.NodeStoreOptions{Store: testWALOptions(), FrameBytes: 1 << 20, Events: 256, WaveIDs: 64, EntriesPerGroup: 64, CachedSegments: 1, Groups: 8}
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
