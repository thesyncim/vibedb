package snapshottransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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

func TestLearnerInstallSettlementRetainsApplyUntilCloseSuccess(t *testing.T) {
	settlement := &LearnerInstallSettlement{
		apply: &sqldriver.ReplicatedApply{}, database: &sqldriver.Database{},
	}
	fault := errors.New("apply close retry")
	previousApply, previousDatabase := learnerInstallCloseApply, learnerInstallCloseDatabase
	defer func() {
		learnerInstallCloseApply, learnerInstallCloseDatabase = previousApply, previousDatabase
	}()
	applyCalls, databaseCalls := 0, 0
	learnerInstallCloseApply = func(*sqldriver.ReplicatedApply) error {
		applyCalls++
		if applyCalls <= 2 {
			return fault
		}
		return nil
	}
	learnerInstallCloseDatabase = func(*sqldriver.Database) error {
		databaseCalls++
		return nil
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := settlement.settle(); !errors.Is(err, fault) || settlement.apply == nil ||
			settlement.database == nil || databaseCalls != 0 {
			t.Fatalf("attempt %d apply=%v database=%v databaseCalls=%d err=%v",
				attempt, settlement.apply, settlement.database, databaseCalls, err)
		}
	}
	if err := settlement.settle(); err != nil || settlement.apply != nil ||
		settlement.database != nil || applyCalls != 3 || databaseCalls != 1 {
		t.Fatalf("settled apply=%v database=%v calls=%d/%d err=%v",
			settlement.apply, settlement.database, applyCalls, databaseCalls, err)
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
	options.TxnLimits.MaxCollections = int(sourceIdentity.RelationCount) + 2
	requiredDocuments, err := replicatedstate.RequiredBundleTransactionDocuments(
		sourceIdentity.UserLimits.MaxBatchDocuments, options.RetryWindow, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	options.TxnLimits.MaxDocuments = requiredDocuments
	floor, err := sqldriver.ReplicatedApplyTransactionByteFloor(
		sourceIdentity, options.RetryWindow,
	)
	if err != nil {
		t.Fatal(err)
	}
	options.TxnLimits.MaxBytes = floor
	apply, _, err := source.OpenReplicatedApply(sourceIdentity, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	command := replication.Command{
		Kind:      replication.CommandSessionOpen,
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration, ShardIncarnation: binding.ShardIncarnation,
		GroupID: binding.GroupID, ReplicaSetVersion: 1,
		ActivePolicyGeneration: authority.ActivePolicyGeneration, ProtectionEpoch: authority.ProtectionEpoch,
		OwnershipEpoch: authority.OwnershipEpoch, SchemaGeneration: authority.SchemaGeneration,
		RoutingVersion: authority.RoutingVersion, RouteGeneration: authority.RouteGeneration,
		Tenant: []byte("tenant"), ClientID: replication.ID128{9}, ClientSequence: 1,
		Fingerprint:          sha256.Sum256([]byte("learner-session")),
		NextDeadlineUnixNano: 2_000_000_000_000_000_000,
	}
	sessionCommand, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	meta := raftmodel.ApplyMeta{Index: 2, Term: 1, Type: pb.EntryNormal}
	if _, err = apply.ApplyNormal(meta, sessionCommand); err != nil {
		t.Fatal(err)
	}
	lookup, err := apply.LookupCompletion(sessionCommand)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ClientEpoch == 0 {
		t.Fatalf("session completion=%+v err=%v", completion, err)
	}
	sourceSessionCompletion := bytes.Clone(lookup.Bytes)
	partitioner := learnerInstallCapturePartitioner(t, sourceIdentity)
	capture, err := apply.BeginRangeSplitCapture(partitioner)
	if err != nil || capture.Head() != 2 {
		t.Fatalf("source capture head=%d err=%v", capture.Head(), err)
	}
	keyBytes, ok := orderedkey.AppendJSONString(nil, []byte(`"doc-1"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode document key")
	}
	document := []byte(`{"id":"doc-1","value":"replicated"}`)
	command.Kind = replication.CommandMutationBatch
	command.ClientEpoch, command.ClientSequence = completion.ClientEpoch, 2
	command.Fingerprint = sha256.Sum256([]byte("learner-document"))
	command.NextDeadlineUnixNano = 0
	command.Batches = []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
		Kind: replication.MutationPut, Key: keyBytes, Value: document,
	}}}}
	documentCommand, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	meta.Index = 3
	if _, err = apply.ApplyNormal(meta, documentCommand); err != nil {
		t.Fatal(err)
	}
	if capture.Head() != 3 {
		t.Fatalf("source capture tail head=%d", capture.Head())
	}
	documentLookup, err := apply.LookupCompletion(documentCommand)
	if err != nil {
		t.Fatal(err)
	}
	sourceCompletion := bytes.Clone(documentLookup.Bytes)
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
	hostLimits := multiraft.Limits{
		MaxGroups: 2, MaxQueueItems: 8, MaxQueueBytes: raftmodel.MaxInboundMessageBytes,
		MaxGroupItems: 4, MaxGroupBytes: raftmodel.MaxInboundMessageBytes,
		MaxOutboxItems: 8, MaxOutboxBytes: raftmodel.MaxInboundMessageBytes,
		MaxPendingTicks: 4,
	}
	rejectedHost, err := multiraft.NewHost(hostLimits)
	if err != nil {
		t.Fatal(err)
	}
	if err = rejectedHost.Close(); err != nil {
		t.Fatal(err)
	}
	settlement := &LearnerInstallSettlement{}
	plan := LearnerInstallPlan{Repository: repository, Descriptor: descriptor, Cursor: cursor,
		Database: target, SQLIdentity: targetIdentity, ApplyOptions: options,
		StaticBootstrap: bootstrap, ExpectedConfState: conf,
		WALPath: filepath.Join(t.TempDir(), "learner.wal"), WALIdentity: walIdentity,
		WALKey: key, WALOptions: walOptions, Authority: authority, Host: rejectedHost,
		Settlement: settlement}
	_, err = InstallPublishedLearner(plan)
	if !errors.Is(err, multiraft.ErrHostClosed) {
		t.Fatalf("Host boundary fault = %v", err)
	}
	if _, statusErr := rejectedHost.Status(descriptor.Group); !errors.Is(statusErr, multiraft.ErrHostClosed) {
		t.Fatalf("closed rejecting Host unexpectedly owns learner: %v", statusErr)
	}
	closeFault := errors.New("runtime close retry")
	previousClose := learnerInstallCloseRuntime
	defer func() { learnerInstallCloseRuntime = previousClose }()
	closeAttempts := 0
	learnerInstallCloseRuntime = func(runtime *raftmember.Runtime) error {
		closeAttempts++
		if closeAttempts <= 2 {
			return closeFault
		}
		return previousClose(runtime)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if settleErr := settlement.settle(); !errors.Is(settleErr, closeFault) || settlement.runtime == nil {
			t.Fatalf("retained close attempt %d runtime=%v err=%v", attempt, settlement.runtime, settleErr)
		}
	}
	if err = settlement.settle(); err != nil || settlement.runtime != nil || closeAttempts != 3 {
		t.Fatalf("settled retained runtime=%v attempts=%d err=%v", settlement.runtime, closeAttempts, err)
	}
	learnerInstallCloseRuntime = previousClose

	reopened, _, err := sqldriver.OpenReplicatedShardStoreWithApplyForSettlement(
		targetPath, targetIdentity, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	reopenedApply, _, err := reopened.OpenReplicatedApply(targetIdentity, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if targetCompletion, err := reopenedApply.LookupCompletion(documentCommand); err != nil ||
		!bytes.Equal(targetCompletion.Bytes, sourceCompletion) {
		t.Fatalf("target completion=%x err=%v", targetCompletion.Bytes, err)
	}
	if targetSession, err := reopenedApply.LookupCompletion(sessionCommand); err != nil ||
		!bytes.Equal(targetSession.Bytes, sourceSessionCompletion) {
		t.Fatalf("target session completion=%x err=%v", targetSession.Bytes, err)
	}
	read, err := reopenedApply.PointReadInto(
		1, keyBytes, manifest.State.Applied,
		targetIdentity.Relations[0].Limits.MaxDocumentBytes, nil,
	)
	if err != nil || !read.Found || !bytes.Equal(read.Value, document) {
		t.Fatalf("target document=%q found=%v err=%v", read.Value, read.Found, err)
	}
	recoveredCapture, err := reopenedApply.BeginRangeSplitCapture(partitioner)
	if err != nil || recoveredCapture.Head() != 3 {
		t.Fatalf("installed capture head=%d err=%v", recoveredCapture.Head(), err)
	}
	targetCut, err := reopenedApply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	targetManifest, err := replicatedstate.WriteSnapshotArtifact(io.Discard, targetCut,
		replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: MinChunkBytes})
	err = errors.Join(err, targetCut.Close(), reopenedApply.Close(), reopened.Close())
	if err != nil || targetManifest.ImageDigest != manifest.ImageDigest ||
		targetManifest.CaptureRows != manifest.CaptureRows ||
		targetManifest.CaptureImageDigest != manifest.CaptureImageDigest ||
		targetManifest.UserRows != 1 ||
		targetManifest.State.Applied != manifest.State.Applied ||
		targetManifest.State.DataChainDigest != manifest.State.DataChainDigest ||
		targetManifest.State.LastEntryDigest != manifest.State.LastEntryDigest ||
		targetManifest.State.SessionCount != manifest.State.SessionCount ||
		targetManifest.State.SessionSlotCount != manifest.State.SessionSlotCount ||
		targetManifest.State.LastKind != manifest.State.LastKind ||
		targetManifest.State.SnapshotBaseDigest == manifest.State.SnapshotBaseDigest {
		t.Fatalf("target snapshot rows=%d digest=%x/%x state=%+v err=%v",
			targetManifest.UserRows, targetManifest.ImageDigest, manifest.ImageDigest,
			targetManifest.State, err)
	}
	reopened, _, err = sqldriver.OpenReplicatedShardStoreWithApplyForSettlement(
		targetPath, targetIdentity, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	host, err := multiraft.NewHost(hostLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	plan.Database, plan.Host = reopened, host
	identity, err := InstallPublishedLearner(plan)
	if err != nil || identity.NodeIncarnation != descriptor.TargetIncarnation {
		t.Fatalf("exact retry identity=%+v err=%v", identity, err)
	}
	if status, err := host.Status(descriptor.Group); err != nil || status.MemberID != 2 {
		t.Fatalf("installed learner status=%+v err=%v", status, err)
	}
}

func learnerInstallCapturePartitioner(
	t testing.TB, base sqldriver.ReplicatedShardStoreIdentity,
) *rangesplit.Partitioner {
	t.Helper()
	full := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	manifest, err := distribution.NewManifest(
		distribution.DistributionName(base.Binding.Distribution),
		distribution.RoutingVersion(base.Binding.Authority.RoutingVersion),
		[]distribution.Shard{{
			ID:                   distribution.ShardID(base.Binding.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(base.Binding.AllocationGeneration),
			Range:                full, Leaders: []distribution.EndpointID{"source"},
			Epoch: distribution.OwnershipEpoch(base.Binding.Authority.OwnershipEpoch),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := autosplit.PlanSplit(manifest, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: autosplit.SourceIdentity{
				Distribution:         distribution.DistributionName(base.Binding.Distribution),
				Shard:                distribution.ShardID(base.Binding.Shard),
				AllocationGeneration: distribution.ShardAllocationGeneration(base.Binding.AllocationGeneration),
				Range:                full, BucketBits: distribution.DefaultVirtualBucketBits,
				RoutingVersion: distribution.RoutingVersion(base.Binding.Authority.RoutingVersion),
				OwnershipEpoch: distribution.OwnershipEpoch(base.Binding.Authority.OwnershipEpoch),
			},
			Kind:       autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{{0x80}}, BoundaryCount: 1,
			CandidateBin: 32, BenefitPPM: 1,
		},
		RetainChild:         0,
		NextRoutingVersion:  distribution.RoutingVersion(base.Binding.Authority.RoutingVersion + 1),
		AllocationHighWater: distribution.ShardAllocationGeneration(base.Binding.AllocationGeneration),
		Destinations: []autosplit.Destination{{
			Shard:                "learner-capture-right",
			AllocationGeneration: distribution.ShardAllocationGeneration(base.Binding.AllocationGeneration + 1),
			Leaders:              []distribution.EndpointID{"destination"},
			OwnershipEpoch:       distribution.OwnershipEpoch(base.Binding.Authority.OwnershipEpoch + 1),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := rangesplit.NewPartitioner(
		plan, base.UserTable, []string{"/id"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	return partitioner
}
