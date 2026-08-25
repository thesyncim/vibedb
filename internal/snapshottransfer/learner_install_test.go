package snapshottransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestExactLearnerConfStateRequiresTargetLearnerAndSourceVoter(t *testing.T) {
	autoLeave := true
	valid := &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	if !exactLearnerConfState(valid, 1, 3) {
		t.Fatal("rejected exact learner configuration")
	}
	for name, conf := range map[string]*pb.ConfState{
		"target voter":    {Voters: []uint64{1, 3}},
		"source learner":  {Voters: []uint64{2}, Learners: []uint64{1, 3}},
		"target absent":   {Voters: []uint64{1, 2}},
		"joint":           {Voters: []uint64{1}, VotersOutgoing: []uint64{1}, Learners: []uint64{3}},
		"learners next":   {Voters: []uint64{1}, LearnersNext: []uint64{3}},
		"automatic leave": {Voters: []uint64{1}, Learners: []uint64{3}, AutoLeave: &autoLeave},
	} {
		t.Run(name, func(t *testing.T) {
			if exactLearnerConfState(conf, 1, 3) {
				t.Fatalf("accepted %+v", conf)
			}
		})
	}
}

func TestInstallPublishedLearnerRetriesExactIncarnationAfterHostBoundary(t *testing.T) {
	id := func(seed byte) (out [16]byte) {
		for i := range out {
			out[i] = seed + byte(i)
		}
		return
	}
	authority := sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 2, ProtectionEpoch: 3, OwnershipEpoch: 4,
		SchemaGeneration: 5, RoutingVersion: 6, RouteGeneration: 7,
	}
	binding := sqldriver.ReplicatedShardStoreBinding{
		ClusterID: id(1), ClusterIncarnation: id(2), TopologyRecoveryEpoch: 3,
		Distribution: "learner", Shard: "all", AllocationGeneration: 4,
		ShardIncarnation: id(3), GroupID: id(4), MemberID: 1, StoreID: id(5),
		Authority: authority,
	}
	bootstrapIndex, bootstrapTerm := uint64(1), uint64(1)
	conf := &pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	bootstrap := &pb.Snapshot{Data: []byte("learner-install-bootstrap"), Metadata: &pb.SnapshotMetadata{
		Index: &bootstrapIndex, Term: &bootstrapTerm, ConfState: conf,
	}}
	options := sqldriver.ReplicatedApplyOptions{
		MaxSessions: 8, RetryWindow: 4,
		TxnLimits: durable.TxnLimits{MaxCollections: 2, MaxDocuments: 1024, MaxBytes: 32 << 20},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
	prepare := func(t *testing.T, name string, bind sqldriver.ReplicatedShardStoreBinding) (string, *sqldriver.Database, sqldriver.ReplicatedShardStoreIdentity) {
		t.Helper()
		path := filepath.Join(t.TempDir(), name+".vdb")
		database, err := sqldriver.InitializeShardStore(path, sqldriver.ShardStoreBinding{
			Distribution:         distribution.DistributionName(bind.Distribution),
			Shard:                distribution.ShardID(bind.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(bind.AllocationGeneration),
		})
		if err != nil {
			t.Fatal(err)
		}
		session, err := database.NewSession(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		statement, err := session.Prepare(context.Background(), `CREATE TABLE docs (PRIMARY KEY (id))`)
		if err == nil {
			_, err = statement.Exec(context.Background(), nil)
		}
		if statement != nil {
			err = errors.Join(err, statement.Close())
		}
		err = errors.Join(err, session.Close())
		if err != nil {
			t.Fatal(err)
		}
		identity, err := database.BindReplicatedShardStore(bind, "docs")
		if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
			_ = database.Close()
			t.Skipf("sealed sidecars require strict allocation: %v", err)
		}
		if err != nil {
			t.Fatal(err)
		}
		return path, database, identity
	}

	_, source, sourceIdentity := prepare(t, "source", binding)
	apply, _, err := source.OpenReplicatedApply(sourceIdentity, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(&encoded, cut,
		replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: MinChunkBytes})
	err = errors.Join(err, cut.Close(), apply.Close(), source.Close())
	if err != nil {
		t.Fatal(err)
	}

	targetBinding := binding
	targetBinding.MemberID, targetBinding.StoreID = 2, id(9)
	targetPath, target, targetIdentity := prepare(t, "target", targetBinding)
	payload := encoded.Bytes()
	descriptor := Descriptor{
		Group: raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
			TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, ShardIncarnation: binding.ShardIncarnation,
			GroupID: binding.GroupID},
		SourceMember: 1, TargetMember: 2, TargetStore: targetBinding.StoreID,
		TargetIncarnation: 1, SchemaGeneration: authority.SchemaGeneration,
		ReplicaSetVersion: manifest.State.ReplicaSetVersion,
		SnapshotIndex:     manifest.State.Applied, SnapshotTerm: manifest.State.LastTerm,
		Lineage: manifest.State.LastEntryDigest, ArtifactHash: sha256.Sum256(payload),
		ArtifactBytes: uint64(len(payload)), ChunkBytes: MinChunkBytes,
	}
	repository, err := OpenRepository(filepath.Join(t.TempDir(), "artifacts"),
		Limits{MaxArtifacts: 1, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	appendAll(t, repository, descriptor, payload, 0)
	cursorPath := filepath.Join(t.TempDir(), "cursor")
	cursor, err := replicatedstate.OpenSnapshotCursorStore(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	walIdentity := raftstore.Identity{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		Distribution: binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration, ShardIncarnation: binding.ShardIncarnation,
		GroupID: binding.GroupID, MemberID: 2, StoreID: targetBinding.StoreID,
	}
	key := raftstore.Key{ID: "learner-install", Wrapped: []byte("wrapped")}
	for i := range key.Material {
		key.Material[i] = byte(i + 1)
	}
	walOptions := raftstore.Options{MaxFileBytes: 160 << 20,
		MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: 1024,
		MaxEntries: 8192, MaxLiveBytes: raftstore.MinimumReadyLiveBytes}
	hostLimits := multiraft.Limits{MaxGroups: 2, MaxQueueItems: 8, MaxQueueBytes: 8 << 20,
		MaxGroupItems: 4, MaxGroupBytes: 4 << 20, MaxOutboxItems: 8,
		MaxOutboxBytes: 4 << 20, MaxPendingTicks: 4}
	host, err := multiraft.NewHost(hostLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	plan := LearnerInstallPlan{Repository: repository, Descriptor: descriptor, Cursor: cursor,
		Database: target, SQLIdentity: targetIdentity, ApplyOptions: options,
		StaticBootstrap: bootstrap, ExpectedConfState: conf,
		WALPath: filepath.Join(t.TempDir(), "learner.wal"), WALIdentity: walIdentity,
		WALKey: key, WALOptions: walOptions, Authority: authority, Host: host}
	fault := errors.New("stop before Host.Add")
	previous := learnerInstallFaultHook
	learnerInstallFaultHook = func(point learnerInstallFaultPoint) error {
		if point == learnerInstallBeforeHostAdd {
			return fault
		}
		return nil
	}
	_, err = InstallPublishedLearner(plan)
	learnerInstallFaultHook = previous
	if !errors.Is(err, fault) {
		t.Fatalf("Host boundary fault = %v", err)
	}

	reopened, _, err := sqldriver.OpenReplicatedShardStoreWithApplyForSettlement(
		targetPath, targetIdentity, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.Database = reopened
	identity, err := InstallPublishedLearner(plan)
	if err != nil || identity.NodeIncarnation != descriptor.TargetIncarnation {
		t.Fatalf("exact retry identity=%+v err=%v", identity, err)
	}
	if status, err := host.Status(descriptor.Group); err != nil || status.MemberID != 2 {
		t.Fatalf("installed learner status=%+v err=%v", status, err)
	}
}
