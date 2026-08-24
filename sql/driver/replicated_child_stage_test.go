package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestReplicatedChildStageArtifactProfileBindsExactSQLTarget(t *testing.T) {
	fixture := newReplicatedChildSourceFixture(t)
	identity := ReplicatedShardStoreIdentity{
		Binding: testReplicatedBinding(71), UserTable: "docs", UserPrimaryKey: "/id",
	}
	identity.Binding.Distribution = "accounts"
	identity.Binding.Shard = "right"
	identity.Binding.AllocationGeneration = 5
	identity.Binding.Authority.OwnershipEpoch = 17
	identity.Binding.Authority.RoutingVersion = 23
	options := testReplicatedApplyOptions()
	options.Placement.Range = fixture.childRange
	if err := validateReplicatedChildArtifactProfile(
		identity, fixture.partitioner, fixture.artifactManifest, options,
	); err != nil {
		t.Fatal(err)
	}
	wrong := identity
	wrong.Binding.Shard = "neighbor"
	if err := validateReplicatedChildArtifactProfile(
		wrong, fixture.partitioner, fixture.artifactManifest, options,
	); !errors.Is(err, ErrReplicatedChildStageProof) {
		t.Fatalf("cross-shard profile error = %v", err)
	}
	wrong = identity
	wrong.Binding.Distribution = "other"
	if err := validateReplicatedChildArtifactProfile(
		wrong, fixture.partitioner, fixture.artifactManifest, options,
	); !errors.Is(err, ErrReplicatedChildStageProof) {
		t.Fatalf("cross-distribution profile error = %v", err)
	}
}

func TestReplicatedChildSourceFixtureSealsCertifiedImage(t *testing.T) {
	fixture := newReplicatedChildSourceFixture(t)
	file, err := os.OpenFile(
		filepath.Join(t.TempDir(), "destination.vdb"),
		os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, durable.Options{
		MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
		MaxBatchDocuments: 64, MaxBatchBytes: replication.MaxCommandBytes + 64*256,
	})
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close(); _ = file.Close() })
	stage, err := rangesplit.NewChildStage(
		fixture.partitioner, fixture.artifactManifest, collection, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	var cursorRaw []byte
	persist := func(raw []byte) error {
		cursorRaw = append(cursorRaw[:0], raw...)
		return nil
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(fixture.artifact), persist); err != nil {
		t.Fatal(err)
	}
	fixture.catchUpAndSeal(t, func(batch rangesplit.TailBatch) error {
		return stage.ApplyTailBatch(batch, persist)
	})
	cursor, ok := stage.Cursor()
	if !ok || cursor.Phase() != rangesplit.ChildStageSealed || len(cursorRaw) == 0 {
		t.Fatalf("sealed fixture cursor = %+v, %v, bytes=%d", cursor, ok, len(cursorRaw))
	}
	certificate := fixture.certify(t, cursor)
	if err := fixture.partitioner.VerifyCutoverCertificate(certificate); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedChildStageNoCopyApplyHandoffAndUnknownPublicationRetry(t *testing.T) {
	fixture := newReplicatedChildSourceFixture(t)
	targetBinding := testReplicatedBinding(91)
	targetBinding.Distribution = string(fixture.partitioner.SourceDistribution())
	targetBinding.Shard = "right"
	targetBinding.AllocationGeneration = 5
	targetBinding.Authority.OwnershipEpoch = 17
	targetBinding.Authority.RoutingVersion = 23
	targetBinding.Authority.RouteGeneration = 29

	path := filepath.Join(t.TempDir(), "child.vdb")
	db, err := InitializeShardStore(path, ShardStoreBinding{
		Distribution: distribution.DistributionName(targetBinding.Distribution),
		Shard:        distribution.ShardID(targetBinding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(
			targetBinding.AllocationGeneration,
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	session, err := db.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(session, `CREATE TABLE docs (PRIMARY KEY (id))`, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := db.BindReplicatedShardStore(targetBinding, "docs")
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		t.Skipf("sealed replicated sidecars require strict allocation support: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	applyOptions := testReplicatedApplyOptions()
	applyOptions.Placement.Range = fixture.childRange

	core := db.connector.db
	core.mu.RLock()
	finalCollection := core.tables["docs"].collection
	core.mu.RUnlock()
	stage, err := db.OpenReplicatedChildStage(
		base, fixture.partitioner, fixture.artifactManifest, nil,
		applyOptions, rangesplit.ChildStageOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stage.Close() })
	if opened, err := db.NewSession(context.Background()); opened != nil ||
		!errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("SQL session during stage = %v, %v", opened, err)
	}

	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(fixture.artifact), persist); err != nil {
		t.Fatal(err)
	}
	fixture.catchUpAndSeal(t, func(batch rangesplit.TailBatch) error {
		return stage.ApplyTailBatch(batch, persist)
	})
	cursor, ok := stage.Cursor()
	if !ok || cursor.Phase() != rangesplit.ChildStageSealed {
		t.Fatalf("sealed cursor = %+v, %v", cursor, ok)
	}
	certificate := fixture.certify(t, cursor)

	if result, err := stage.Activate(
		(rangesplit.CutoverCertificate{}), fixture.targetBootstrap,
		replicatedstate.SnapshotArtifactOptions{},
	); !errors.Is(err, ErrReplicatedChildStageProof) || result.Apply != nil ||
		result.ApplyIdentity != (ReplicatedApplyIdentity{}) || result.SnapshotBase != nil {
		t.Fatalf("wrong certificate activation = %+v, %v", result, err)
	}
	invalid := replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: 1}
	if result, err := stage.Activate(certificate, fixture.targetBootstrap, invalid); err == nil ||
		result.Apply != nil || result.ApplyIdentity != (ReplicatedApplyIdentity{}) ||
		result.SnapshotBase != nil || result.ArtifactManifest.Digest != ([32]byte{}) {
		t.Fatalf("invalid artifact options activation = %+v, %v", result, err)
	}
	core.mu.RLock()
	if core.catalog.ReplicatedApply != nil || core.replicatedApplyCollection != nil {
		core.mu.RUnlock()
		t.Fatal("artifact preflight failure published hidden apply storage")
	}
	core.mu.RUnlock()

	unknown, err := stage.activate(
		certificate, fixture.targetBootstrap,
		replicatedstate.SnapshotArtifactOptions{},
		func(database *database) (bool, error) {
			published, persistErr := database.persistCatalogLocked()
			if persistErr != nil {
				return published, persistErr
			}
			return true, durable.ErrCommitOutcomeUnknown
		},
	)
	if unknown.Apply != nil || unknown.ApplyIdentity == (ReplicatedApplyIdentity{}) ||
		!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("unknown descriptor publication = %+v, %v", unknown, err)
	}
	if opened, err := db.NewSession(context.Background()); opened != nil ||
		!errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("SQL session after unknown publication = %v, %v", opened, err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := db.NewSession(context.Background()); opened != nil ||
		!errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("SQL session after unknown publication and stage close = %v, %v", opened, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if ordinary, _, err := OpenReplicatedShardStoreWithApplyForSettlement(
		path, base, applyOptions,
	); ordinary != nil || !errors.Is(err, durable.ErrCheckpointGroupCorrupt) {
		if ordinary != nil {
			_ = ordinary.Close()
		}
		t.Fatalf("ordinary open of seed-pending image = %v, %v", ordinary, err)
	}
	resumed, resumedIdentity, err := OpenReplicatedShardStoreForChildStageResume(
		path, base, applyOptions,
	)
	if err != nil || resumedIdentity != unknown.ApplyIdentity {
		t.Fatalf("child-stage resume open = %+v, %v", resumedIdentity, err)
	}
	db = resumed
	if opened, err := db.NewSession(context.Background()); opened != nil ||
		!errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("SQL session before sealed-stage reclaim = %v, %v", opened, err)
	}
	if apply, _, err := db.OpenReplicatedApply(
		base, fixture.targetBootstrap, applyOptions,
	); apply != nil || !errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("ordinary apply before sealed-stage reclaim = %v, %v", apply, err)
	}
	core = db.connector.db
	core.mu.RLock()
	finalCollection = core.tables["docs"].collection
	core.mu.RUnlock()
	stage, err = db.OpenReplicatedChildStage(
		base, fixture.partitioner, fixture.artifactManifest, persisted,
		applyOptions, rangesplit.ChildStageOptions{},
	)
	if err != nil {
		t.Fatalf("reclaim sealed child stage: %v", err)
	}

	restoreSeedFault := durable.InstallCheckpointGroupInitialCertificateFaultForFacadeTest()
	uncertainSeed, err := stage.Activate(
		certificate, fixture.targetBootstrap,
		replicatedstate.SnapshotArtifactOptions{},
	)
	restoreSeedFault()
	if uncertainSeed.Apply != nil || uncertainSeed.ApplyIdentity != unknown.ApplyIdentity ||
		!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("unknown initial seed certificate = %+v, %v", uncertainSeed, err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := db.NewSession(context.Background()); opened != nil ||
		!errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("SQL session after unknown seed certificate and stage close = %v, %v", opened, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, resumedIdentity, err = OpenReplicatedShardStoreForChildStageResume(
		path, base, applyOptions,
	)
	if err != nil || resumedIdentity != unknown.ApplyIdentity {
		t.Fatalf("initial-certificate child resume = %+v, %v", resumedIdentity, err)
	}
	db = resumed
	stage, err = db.OpenReplicatedChildStage(
		base, fixture.partitioner, fixture.artifactManifest, persisted,
		applyOptions, rangesplit.ChildStageOptions{},
	)
	if err != nil {
		t.Fatalf("reclaim stage after initial seed certificate: %v", err)
	}

	activated, err := stage.Activate(
		certificate, fixture.targetBootstrap,
		replicatedstate.SnapshotArtifactOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if activated.Apply == nil || activated.ApplyIdentity != unknown.ApplyIdentity ||
		activated.SnapshotBase == nil || activated.ArtifactManifest.UserRows != 1 ||
		activated.ArtifactManifest.State.Applied != certificate.SourceCut().Applied {
		t.Fatalf("activation = %+v", activated)
	}
	if err := activated.Apply.AdmitCommand(nil); !errors.Is(err, ErrReplicatedApplyBasePending) {
		t.Fatalf("AdmitCommand before base install = %v", err)
	}
	if _, err := activated.Apply.ApplyNormal(raftmodel.ApplyMeta{
		Index: certificate.SourceCut().Applied + 1, Term: certificate.SourceCut().Term,
		Type: pb.EntryNormal,
	}, nil); !errors.Is(err, ErrReplicatedApplyBasePending) {
		t.Fatalf("ApplyNormal before base install = %v", err)
	}
	if _, err := activated.Apply.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: certificate.SourceCut().Applied + 1, Term: certificate.SourceCut().Term,
		Type: pb.EntryConfChange,
	}, new(pb.ConfState)); !errors.Is(err, ErrReplicatedApplyBasePending) {
		t.Fatalf("ApplyConfiguration before base install = %v", err)
	}
	if _, err := activated.Apply.LookupCompletion(nil); !errors.Is(err, ErrReplicatedApplyBasePending) {
		t.Fatalf("LookupCompletion before base install = %v", err)
	}
	if snapshot, err := activated.Apply.SnapshotArtifactCut(); snapshot != nil ||
		!errors.Is(err, ErrReplicatedApplyBasePending) {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		t.Fatalf("SnapshotArtifactCut before base install = %v, %v", snapshot, err)
	}
	if _, err := activated.Apply.InstallSnapshot(fixture.targetBootstrap); !errors.Is(err, ErrReplicatedApplyBasePending) {
		t.Fatalf("wrong InstallSnapshot before base install = %v", err)
	}
	core.mu.RLock()
	if core.tables["docs"].collection != finalCollection ||
		core.replicatedChildStageClaim != nil || core.replicatedApplyClaim != activated.Apply {
		core.mu.RUnlock()
		t.Fatal("activation copied user storage or failed claim handoff")
	}
	core.mu.RUnlock()
	if opened, err := db.NewSession(context.Background()); opened != nil ||
		!errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("SQL session before runtime ownership = %v, %v", opened, err)
	}
	openedBase, err := replicatedstate.OpenSnapshotBase(activated.SnapshotBase)
	if err != nil || openedBase.Manifest.Digest != activated.ArtifactManifest.Digest ||
		openedBase.Manifest.State.Binding != replicatedStateBinding(base) {
		t.Fatalf("snapshot base = %+v, %v", openedBase, err)
	}
	firstBaseDigest := openedBase.Digest
	if err := activated.Apply.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, resumedIdentity, err = OpenReplicatedShardStoreForChildStageResume(
		path, base, applyOptions,
	)
	if err != nil || resumedIdentity != activated.ApplyIdentity {
		t.Fatalf("final-certificate child resume = %+v, %v", resumedIdentity, err)
	}
	db = resumed
	if opened, err := db.NewSession(context.Background()); opened != nil ||
		!errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("SQL session after final seed certificate = %v, %v", opened, err)
	}
	stage, err = db.OpenReplicatedChildStage(
		base, fixture.partitioner, fixture.artifactManifest, persisted,
		applyOptions, rangesplit.ChildStageOptions{},
	)
	if err != nil {
		t.Fatalf("reclaim stage after final seed certificate: %v", err)
	}
	retried, err := stage.Activate(
		certificate, fixture.targetBootstrap,
		replicatedstate.SnapshotArtifactOptions{},
	)
	if err != nil || retried.ApplyIdentity != activated.ApplyIdentity ||
		retried.ArtifactManifest.Digest != activated.ArtifactManifest.Digest {
		t.Fatalf("retry after final seed certificate = %+v, %v", retried, err)
	}
	retriedBase, err := replicatedstate.OpenSnapshotBase(retried.SnapshotBase)
	if err != nil || retriedBase.Digest != firstBaseDigest {
		t.Fatalf("retried snapshot base = %+v, %v", retriedBase, err)
	}
	activated = retried
	core = db.connector.db
	core.mu.RLock()
	finalCollection = core.tables["docs"].collection
	core.mu.RUnlock()
	if _, err := activated.Apply.InstallSnapshot(activated.SnapshotBase); err != nil {
		t.Fatal(err)
	}
	if err := activated.Apply.Close(); err != nil {
		t.Fatal(err)
	}
	if session, err := db.NewSession(context.Background()); err != nil {
		t.Fatalf("SQL reads after explicit apply close: %v", err)
	} else if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	value, found, err := finalCollection.AppendRaw(nil, fixture.key)
	if err != nil || !found || !bytes.Equal(value, fixture.document) {
		t.Fatalf("final staged row = %q, found=%v, err=%v", value, found, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(
		path, base, activated.ApplyIdentity,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedApply, reopenedIdentity, err := reopened.OpenReplicatedApply(
		base, fixture.targetBootstrap, applyOptions,
	)
	if err != nil || reopenedIdentity != activated.ApplyIdentity ||
		reopenedApply.Published().Applied != certificate.SourceCut().Applied {
		t.Fatalf("reopened child apply = %+v, %+v, %v", reopenedApply, reopenedIdentity, err)
	}
	if err := reopenedApply.Close(); err != nil {
		t.Fatal(err)
	}
}

type replicatedChildSourceFixture struct {
	partitioner      *rangesplit.Partitioner
	machine          *replicatedstate.Machine
	binding          replicatedstate.Binding
	targetBootstrap  *pb.Snapshot
	capture          *rangesplit.SourceCapture
	tail             rangesplit.TailCursor
	artifact         []byte
	artifactManifest rangesplit.ChildArtifactManifest
	childRange       distribution.KeyRange
	key              []byte
	document         []byte
	sealed           bool
}

type replicatedChildSourceValidator struct{}

func (replicatedChildSourceValidator) ValidatePut(_, _ []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func (replicatedChildSourceValidator) ValidateDelete(
	_, _ []byte,
	_ bool,
) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func newReplicatedChildSourceFixture(t testing.TB) *replicatedChildSourceFixture {
	t.Helper()
	full := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	current, err := distribution.NewManifest("accounts", 22, []distribution.Shard{{
		ID: "source", AllocationGeneration: 4, Range: full,
		Leaders: []distribution.EndpointID{"node-a"}, Epoch: 9,
	}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := autosplit.PlanSplit(current, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: autosplit.SourceIdentity{
				Distribution: "accounts", Shard: "source", AllocationGeneration: 4,
				Range: full, BucketBits: distribution.DefaultVirtualBucketBits,
				RoutingVersion: 22, OwnershipEpoch: 9,
			},
			Kind:       autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{{0x80}}, BoundaryCount: 1,
			CandidateBin: 32, BenefitPPM: 1,
		},
		RetainChild: 0, NextRoutingVersion: 23, AllocationHighWater: 4,
		Destinations: []autosplit.Destination{{
			Shard: "right", AllocationGeneration: 5,
			Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 17,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := rangesplit.NewPartitioner(
		plan, "docs", []string{"/id"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	child, ok := plan.Child(1)
	if !ok {
		t.Fatal("missing destination child")
	}
	document, key := documentForReplicatedChild(t, child.Range)

	dir := t.TempDir()
	create := func(name string, options durable.Options) *durable.Collection {
		file, err := os.OpenFile(
			filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		collection, err := durable.Create(file, options)
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close(); _ = file.Close() })
		return collection
	}
	systemLimits := replicatedApplySystemLimits(8)
	systemCollection := create("system", durable.Options{
		OpaqueValues: true,
		MaxKeyBytes:  systemLimits.MaxKeyBytes, MaxDocumentBytes: systemLimits.MaxDocumentBytes,
		MaxBatchDocuments: systemLimits.MaxBatchDocuments,
		MaxBatchBytes:     systemLimits.MaxBatchBytes,
	})
	userCollection := create("user", durable.Options{
		MaxKeyBytes: 256, MaxDocumentBytes: 4096,
		MaxBatchDocuments: 4, MaxBatchBytes: 32 << 10,
	})
	captureCollection := create("capture", durable.Options{
		OpaqueValues:     true,
		MaxDocumentBytes: 128 << 10, MaxBatchDocuments: 1, MaxBatchBytes: 256 << 10,
	})
	target := func(collection *durable.Collection) replicatedstate.CollectionTarget {
		return replicatedstate.CollectionTarget{
			Collection: collection, Validation: replicatedstate.ValidationDeterministicMutation,
			ValidationDigest: sha256.Sum256([]byte("driver-range-split-source")),
			Validator:        replicatedChildSourceValidator{},
			Limits: replicatedstate.CollectionLimits{
				MaxKeyBytes: collection.MaxKeyBytes(), MaxDocumentBytes: collection.MaxDocumentBytes(),
				MaxDistinctMutations: collection.MaxBatchDocuments(),
				MaxBatchBytes:        collection.MaxBatchBytes(),
			},
		}
	}
	system := target(systemCollection)
	system.Validation = replicatedstate.ValidationOpaqueBinary
	system.ValidationDigest = [32]byte{}
	system.Validator = nil
	user := target(userCollection)
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	binding := replicatedstate.Binding{
		ClusterID: replicatedChildID(1), ClusterIncarnation: replicatedChildID(2),
		TopologyRecoveryEpoch: 3, Distribution: "accounts", Shard: "source",
		AllocationGeneration: 4, ShardIncarnation: replicatedChildID(4),
		GroupID: replicatedChildID(5), ActivePolicyGeneration: 11,
		ProtectionEpoch: 13, OwnershipEpoch: 9, SchemaGeneration: 19,
		RoutingVersion: 22, RouteGeneration: 28,
	}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{
		Data: []byte("driver-range-split-source-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
	machineOptions := replicatedstate.Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 3,
			MaxDocuments: max(
				user.Limits.MaxDistinctMutations+4,
				systemLimits.MaxBatchDocuments+1,
			),
			MaxBytes: 64 << 20,
		},
		MaxSessions: 128,
		RetryWindow: 8,
	}
	machine, err := replicatedstate.Open(
		binding, bootstrap, system,
		replicatedstate.UserCollection{Name: "docs", Target: user}, log, machineOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	open := replicatedChildSessionOpen(binding)
	if err := machine.AdmitCommand(open); err != nil {
		t.Fatalf("admit source session open: %v", err)
	}
	if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, open); err != nil {
		t.Fatal(err)
	}
	lookup, err := machine.LookupCompletion(open)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened ||
		completion.ClientEpoch != 2 {
		t.Fatalf("source session-open completion = %+v, %v", completion, err)
	}
	command := replicatedChildCommand(binding, completion.ClientEpoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 3, Term: 2, Type: pb.EntryNormal,
	}, command); err != nil {
		t.Fatal(err)
	}
	capture, err := rangesplit.NewSourceCapture(
		partitioner, "driver-child-capture", captureCollection,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}
	cut, err := machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	artifactOptions := rangesplit.ChildArtifactOptions{
		TargetChunkBytes: rangesplit.MinChildArtifactChunkBytes,
	}
	artifactOptions.Writers[1] = &artifact
	artifactOptions.PayloadBuffers[1] = make([]byte, 0, rangesplit.MaxChildArtifactChunkBytes)
	var artifactWorkspace rangesplit.ChildArtifactWorkspace
	set, err := partitioner.WriteChildArtifacts(cut, artifactOptions, &artifactWorkspace)
	closeErr := cut.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("write child artifact=%v close=%v", err, closeErr)
	}
	tail, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	targetIndex, targetTerm := uint64(1), uint64(1)
	targetBootstrap := &pb.Snapshot{
		Data: []byte("driver-range-split-target-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &targetIndex, Term: &targetTerm,
			ConfState: &pb.ConfState{Voters: []uint64{9}},
		},
	}
	return &replicatedChildSourceFixture{
		partitioner: partitioner, machine: machine, binding: binding,
		targetBootstrap: targetBootstrap,
		capture:         capture, tail: tail, artifact: bytes.Clone(artifact.Bytes()),
		artifactManifest: set.Children[1], childRange: child.Range,
		key: key, document: document,
	}
}

func documentForReplicatedChild(
	t testing.TB,
	target distribution.KeyRange,
) ([]byte, []byte) {
	t.Helper()
	program, err := distribution.CompileDocumentPointProgram(
		[]string{"/id"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	primary, err := vibejson.CompilePointer("/id")
	if err != nil {
		t.Fatal(err)
	}
	var workspace distribution.DocumentPointWorkspace
	for sequence := 0; sequence < 100_000; sequence++ {
		document := []byte(fmt.Sprintf(`{"id":"child-%d","state":"ready"}`, sequence))
		document, err = vibejson.AppendCanonicalize(nil, document)
		if err != nil {
			t.Fatal(err)
		}
		point, err := program.Point(document, &workspace)
		if err != nil {
			t.Fatal(err)
		}
		if !target.Contains(point) {
			continue
		}
		key, err := documentKey(document, "/id", primary, 256)
		if err != nil {
			t.Fatal(err)
		}
		return document, []byte(key)
	}
	t.Fatal("failed to find a document in the destination child")
	return nil, nil
}

func (f *replicatedChildSourceFixture) catchUpAndSeal(
	t testing.TB,
	target rangesplit.TailSink,
) {
	t.Helper()
	if f.sealed {
		t.Fatal("fixture already sealed")
	}
	if _, err := f.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 4, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	f.translateNext(t, target)
	transition, err := replicatedstate.AppendOwnershipTransition(
		nil,
		replicatedstate.OwnershipTransition{
			From: f.binding, ExpectedReplicaSetVersion: 4,
			SourceMember: 1, TargetMember: 2,
			ToOwnershipEpoch: f.binding.OwnershipEpoch + 1,
			ToRoutingVersion: 23, ToRouteGeneration: 29,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 5, Term: 2, Type: pb.EntryNormal,
	}, transition); err != nil {
		t.Fatal(err)
	}
	f.translateNext(t, target)
	f.sealed = true
}

func (f *replicatedChildSourceFixture) translateNext(
	t testing.TB,
	target rangesplit.TailSink,
) {
	t.Helper()
	var captureWorkspace rangesplit.SourceCaptureWorkspace
	entry, ok, err := f.capture.NextTailEntry(f.tail, &captureWorkspace)
	if err != nil || !ok {
		t.Fatalf("next captured tail entry ok=%v err=%v", ok, err)
	}
	sinks := []rangesplit.TailSink{
		func(rangesplit.TailBatch) error { return nil }, target,
	}
	var workspace rangesplit.TailWorkspace
	f.tail, _, err = f.partitioner.TranslateTailEntry(f.tail, entry, sinks, &workspace)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *replicatedChildSourceFixture) certify(
	t testing.TB,
	cursor rangesplit.ChildStageCursor,
) rangesplit.CutoverCertificate {
	t.Helper()
	var workspace rangesplit.CutoverWorkspace
	certificate, err := f.partitioner.CertifyCutover(
		f.capture, f.tail, []rangesplit.ChildStageCursor{cursor}, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func replicatedChildCommand(
	binding replicatedstate.Binding,
	clientEpoch, sequence uint64,
	mutations ...replication.Mutation,
) []byte {
	command := replicatedChildCommandValue(binding, clientEpoch, sequence, mutations)
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		panic(err)
	}
	return encoded
}

func replicatedChildCommandValue(
	binding replicatedstate.Binding,
	clientEpoch, sequence uint64,
	mutations []replication.Mutation,
) replication.Command {
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x5c})
	return replication.Command{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("tenant"),
		ClientID: replicatedChildID(40), ClientEpoch: clientEpoch, ClientSequence: sequence,
		Fingerprint: fingerprint, Collection: "docs", Mutations: mutations,
	}
}

func replicatedChildSessionOpen(binding replicatedstate.Binding) []byte {
	command := replicatedChildCommandValue(binding, 0, 1, nil)
	command.Kind = replication.CommandSessionOpen
	command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	command.Fingerprint = sha256.Sum256([]byte("driver/child-test-session-open"))
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		panic(err)
	}
	return encoded
}

func replicatedChildID(seed byte) replication.ID128 {
	var id replication.ID128
	for ordinal := range id {
		id[ordinal] = seed + byte(ordinal)
	}
	return id
}
