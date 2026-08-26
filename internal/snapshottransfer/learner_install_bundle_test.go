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
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func learnerBundleDatabase(
	t *testing.T,
	name string,
	binding sqldriver.ReplicatedShardStoreBinding,
) (string, *sqldriver.Database, sqldriver.ReplicatedShardStoreIdentity) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".vdb")
	database, err := sqldriver.InitializeShardStore(path, sqldriver.ShardStoreBinding{
		Distribution: distribution.DistributionName(binding.Distribution),
		Shard:        distribution.ShardID(binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(
			binding.AllocationGeneration,
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`CREATE TABLE docs (PRIMARY KEY (id))`,
		`CREATE TABLE email_claims (PRIMARY KEY (key))`,
	} {
		statement, prepareErr := session.Prepare(context.Background(), sql)
		if prepareErr == nil {
			_, prepareErr = statement.Exec(context.Background(), nil)
		}
		if statement != nil {
			prepareErr = errors.Join(prepareErr, statement.Close())
		}
		if prepareErr != nil {
			_ = session.Close()
			_ = database.Close()
			t.Fatal(prepareErr)
		}
	}
	if err := session.Close(); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	identity, err := database.BindReplicatedShardStoreBundle(
		binding, "docs", []sqldriver.ReplicatedGlobalIndexRelation{{
			Relation: 2, Table: "email_claims", IndexID: 41,
			Incarnation: 7, LocatorCount: 1, Unique: true,
			KeyEncoding: sqldriver.ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			BucketBits:    distribution.DefaultVirtualBucketBits,
		}},
	)
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		_ = database.Close()
		t.Skipf("sealed sidecars require strict allocation: %v", err)
	}
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if identity.RelationCount != 2 || len(identity.Relations) != 2 {
		_ = database.Close()
		t.Fatalf("bundle identity relations=%d/%d", identity.RelationCount, len(identity.Relations))
	}
	return path, database, identity
}

func learnerBundleOptions(
	t *testing.T,
	identity sqldriver.ReplicatedShardStoreIdentity,
) sqldriver.ReplicatedApplyOptions {
	t.Helper()
	const retryWindow = uint16(4)
	documents := 0
	for i := range identity.Relations {
		documents += identity.Relations[i].Limits.MaxBatchDocuments
	}
	documents = min(documents, replication.MaxMutations)
	maxDocuments, err := replicatedstate.RequiredBundleTransactionDocuments(
		documents, retryWindow, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	options := sqldriver.ReplicatedApplyOptions{
		MaxSessions: 8, RetryWindow: retryWindow,
		TxnLimits: durable.TxnLimits{
			MaxCollections: int(identity.RelationCount) + 2,
			MaxDocuments:   maxDocuments,
		},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
	options.TxnLimits.MaxBytes, err = sqldriver.ReplicatedApplyTransactionByteFloor(
		identity, retryWindow,
	)
	if err != nil {
		t.Fatal(err)
	}
	return options
}

func TestInstallPublishedLearnerInstallsRelationBundleAndReexportsCertificate(t *testing.T) {
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
		Distribution: "learner-bundle", Shard: "all", AllocationGeneration: 4,
		ShardIncarnation: id(3), GroupID: id(4), MemberID: 1, StoreID: id(5),
		Authority: authority,
	}
	bootstrapIndex, bootstrapTerm := uint64(1), uint64(1)
	conf := &pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	bootstrap := &pb.Snapshot{
		Data: []byte("learner-bundle-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &bootstrapIndex, Term: &bootstrapTerm, ConfState: conf,
		},
	}

	_, source, sourceIdentity := learnerBundleDatabase(t, "source", binding)
	options := learnerBundleOptions(t, sourceIdentity)
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
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: authority.ActivePolicyGeneration,
		ProtectionEpoch: authority.ProtectionEpoch, OwnershipEpoch: authority.OwnershipEpoch,
		SchemaGeneration: authority.SchemaGeneration, RoutingVersion: authority.RoutingVersion,
		RouteGeneration: authority.RouteGeneration, Tenant: []byte("tenant"),
		ClientID: replication.ID128{9}, ClientSequence: 1,
		Fingerprint:          sha256.Sum256([]byte("learner-bundle-session")),
		NextDeadlineUnixNano: 2_000_000_000_000_000_000,
	}
	sessionCommand, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 1, Type: pb.EntryNormal,
	}, sessionCommand); err != nil {
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
	document := []byte(`{"email":"a","id":"doc-1"}`)
	baseKey, ok := orderedkey.AppendJSONString(nil, []byte(`"doc-1"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode base key")
	}
	globalKey, locator := []byte{0x91, 0x01, 'a'}, []byte(`["doc-1"]`)
	command.Kind = replication.CommandMutationBatch
	command.ClientEpoch, command.ClientSequence = completion.ClientEpoch, 2
	command.Fingerprint = sha256.Sum256([]byte("learner-bundle-put"))
	command.NextDeadlineUnixNano = 0
	command.Batches = []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: baseKey, Value: document,
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: locator,
		}}},
	}
	documentCommand, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 3, Term: 1, Type: pb.EntryNormal,
	}, documentCommand); err != nil {
		t.Fatal(err)
	}
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var sourceArtifact bytes.Buffer
	sourceManifest, writeErr := replicatedstate.WriteSnapshotArtifact(
		&sourceArtifact, cut,
		replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: MinChunkBytes},
	)
	err = errors.Join(writeErr, cut.Close(), apply.Close(), source.Close())
	if err != nil || !sourceManifest.Bundle || len(sourceManifest.Relations) != 2 ||
		sourceManifest.Relations[0].Rows != 1 || sourceManifest.Relations[1].Rows != 1 {
		t.Fatalf("source manifest=%+v err=%v", sourceManifest, err)
	}

	targetBinding := binding
	targetBinding.MemberID, targetBinding.StoreID = 2, id(9)
	targetPath, target, targetIdentity := learnerBundleDatabase(t, "target", targetBinding)
	payload := sourceArtifact.Bytes()
	descriptor := Descriptor{
		Group: raftmember.GroupKey{
			ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
			TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
			ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
		},
		SourceMember: 1, TargetMember: 2, TargetStore: targetBinding.StoreID,
		TargetIncarnation: 1, SchemaGeneration: authority.SchemaGeneration,
		ReplicaSetVersion: sourceManifest.State.ReplicaSetVersion,
		SnapshotIndex:     sourceManifest.State.Applied, SnapshotTerm: sourceManifest.State.LastTerm,
		Lineage: sourceManifest.State.LastEntryDigest, ArtifactHash: sha256.Sum256(payload),
		ArtifactBytes: uint64(len(payload)), ChunkBytes: MinChunkBytes,
	}
	repository, err := OpenRepository(
		filepath.Join(t.TempDir(), "artifacts"),
		Limits{MaxArtifacts: 1, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	appendAll(t, repository, descriptor, payload, 0)
	cursor, err := replicatedstate.OpenSnapshotCursorStore(filepath.Join(t.TempDir(), "cursor"))
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	walIdentity := raftstore.Identity{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		Distribution: binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		MemberID: 2, StoreID: targetBinding.StoreID,
	}
	walKey := raftstore.Key{ID: "learner-bundle", Wrapped: []byte("wrapped")}
	for i := range walKey.Material {
		walKey.Material[i] = byte(i + 1)
	}
	host, err := multiraft.NewHost(multiraft.Limits{
		MaxGroups: 2, MaxQueueItems: 8, MaxQueueBytes: raftmodel.MaxInboundMessageBytes,
		MaxGroupItems: 4, MaxGroupBytes: raftmodel.MaxInboundMessageBytes,
		MaxOutboxItems: 8, MaxOutboxBytes: raftmodel.MaxInboundMessageBytes,
		MaxPendingTicks: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	settlement := &LearnerInstallSettlement{}
	plan := LearnerInstallPlan{
		Repository: repository, Descriptor: descriptor, Cursor: cursor,
		Database: target, SQLIdentity: targetIdentity, ApplyOptions: options,
		StaticBootstrap: bootstrap, ExpectedConfState: conf,
		WALPath: filepath.Join(t.TempDir(), "learner.wal"), WALIdentity: walIdentity,
		WALKey: walKey,
		WALOptions: raftstore.Options{
			MaxFileBytes: 160 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
			MaxRecords: 1024, MaxEntries: 8192,
			MaxLiveBytes: raftstore.MinimumReadyLiveBytes,
		},
		Authority: authority, Host: host, Settlement: settlement,
	}
	fault := errors.New("inspect installed bundle before host transfer")
	previousFault := learnerInstallFaultHook
	defer func() { learnerInstallFaultHook = previousFault }()
	learnerInstallFaultHook = func(point learnerInstallFaultPoint) error {
		if point == learnerInstallBeforeHostAdd {
			return fault
		}
		return nil
	}
	_, installErr := InstallPublishedLearner(plan)
	learnerInstallFaultHook = previousFault
	if !errors.Is(installErr, fault) || settlement.runtime == nil {
		t.Fatalf("install boundary runtime=%v err=%v", settlement.runtime, installErr)
	}
	if err := settlement.Close(); err != nil {
		t.Fatal(err)
	}

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
	for _, check := range []struct {
		relation replication.RelationID
		key      []byte
		want     []byte
		limit    int
	}{
		{1, baseKey, document, targetIdentity.Relations[0].Limits.MaxDocumentBytes},
		{2, globalKey, locator, targetIdentity.Relations[1].Limits.MaxDocumentBytes},
	} {
		read, readErr := reopenedApply.PointReadInto(
			check.relation, check.key, sourceManifest.State.Applied, check.limit, nil,
		)
		if readErr != nil || !read.Found || !bytes.Equal(read.Value, check.want) {
			t.Fatalf("installed relation %d value=%q found=%v err=%v",
				check.relation, read.Value, read.Found, readErr)
		}
	}
	targetCut, err := reopenedApply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var first, second bytes.Buffer
	targetManifest, firstErr := replicatedstate.WriteSnapshotArtifact(
		&first, targetCut,
		replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: MinChunkBytes},
	)
	firstErr = errors.Join(firstErr, targetCut.Close())
	targetCut, err = reopenedApply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	secondManifest, secondErr := replicatedstate.WriteSnapshotArtifact(
		&second, targetCut,
		replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: MinChunkBytes},
	)
	secondErr = errors.Join(secondErr, targetCut.Close(), reopenedApply.Close(), reopened.Close())
	if firstErr != nil || secondErr != nil || !bytes.Equal(first.Bytes(), second.Bytes()) ||
		targetManifest.Digest != secondManifest.Digest {
		t.Fatalf("target re-export deterministic=%v digest=%x/%x err=%v/%v",
			bytes.Equal(first.Bytes(), second.Bytes()), targetManifest.Digest,
			secondManifest.Digest, firstErr, secondErr)
	}
	if !targetManifest.Bundle ||
		targetManifest.RelationManifestDigest != sourceManifest.RelationManifestDigest ||
		targetManifest.ImageDigest != sourceManifest.ImageDigest ||
		targetManifest.CaptureRows != sourceManifest.CaptureRows ||
		targetManifest.CaptureImageDigest != sourceManifest.CaptureImageDigest ||
		len(targetManifest.Relations) != len(sourceManifest.Relations) ||
		targetManifest.UserRows != sourceManifest.UserRows {
		t.Fatalf("target/source bundle certificate mismatch target=%+v source=%+v",
			targetManifest, sourceManifest)
	}
	for i := range sourceManifest.Relations {
		got, want := targetManifest.Relations[i], sourceManifest.Relations[i]
		if got.Relation != want.Relation || got.Kind != want.Kind ||
			!bytes.Equal(got.Collection, want.Collection) || got.Rows != want.Rows ||
			got.ImageDigest != want.ImageDigest {
			t.Fatalf("relation %d certificate got=%+v want=%+v", i+1, got, want)
		}
	}
	if targetManifest.State.Applied != sourceManifest.State.Applied ||
		targetManifest.State.DataChainDigest != sourceManifest.State.DataChainDigest ||
		targetManifest.State.LastEntryDigest != sourceManifest.State.LastEntryDigest ||
		targetManifest.State.SnapshotBaseDigest == sourceManifest.State.SnapshotBaseDigest {
		t.Fatalf("installed publication target=%+v source=%+v",
			targetManifest.State, sourceManifest.State)
	}
	retryDatabase, _, err := sqldriver.OpenReplicatedShardStoreWithApplyForSettlement(
		targetPath, targetIdentity, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.Database = retryDatabase
	plan.Settlement = &LearnerInstallSettlement{}
	identity, err := InstallPublishedLearner(plan)
	if err != nil || identity.NodeIncarnation != descriptor.TargetIncarnation {
		t.Fatalf("successful bundle install identity=%+v err=%v", identity, err)
	}
	if status, err := host.Status(descriptor.Group); err != nil || status.MemberID != 2 {
		t.Fatalf("bundle learner host status=%+v err=%v", status, err)
	}
}
