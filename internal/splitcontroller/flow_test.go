package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestReconcileRealProofFlowSurvivesSealAndPublicationCrashWindows(t *testing.T) {
	plan, catalog, target, split := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	state := flowSourceState(t, source.machine)
	observed := Observation{
		Catalog: catalog, SourceState: state, SourceStatus: testLeaderStatus(state),
	}
	assertFlowAction(t, plan, observed, ActionStartCapture)

	capture, err := rangesplit.NewSourceCapture(
		plan.partitioner, "controller-flow-capture", source.capture,
	)
	if err != nil || source.machine.BeginTransitionCapture(capture) != nil {
		t.Fatal(err)
	}
	observed.Capture = capture
	assertFlowAction(t, plan, observed, ActionBuildArtifacts)

	cut, err := source.machine.Snapshot("docs")
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
	artifacts, err := plan.partitioner.WriteChildArtifacts(
		cut, artifactOptions, &artifactWorkspace,
	)
	closeErr := cut.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("build artifacts = %v, close = %v", err, closeErr)
	}
	observed.Artifacts = &artifacts
	action := assertFlowAction(t, plan, observed, ActionStageChild)
	if action.Child != 1 {
		t.Fatalf("stage child = %d, want 1", action.Child)
	}

	childDatabase, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = childDatabase.Close() })
	childCollection, err := childDatabase.CreateCollection(
		"child", durable.Options{MaxBatchDocuments: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := rangesplit.NewChildStage(
		plan.partitioner, artifacts.Children[1], childCollection, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	var stageRaw []byte
	persistStage := func(raw []byte) error {
		stageRaw = append(stageRaw[:0], raw...)
		return nil
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact.Bytes()), persistStage); err != nil {
		t.Fatal(err)
	}
	stageCursor, ok := stage.Cursor()
	if !ok {
		t.Fatal("missing child stage cursor")
	}
	observed.Stages[1] = &stageCursor
	assertFlowAction(t, plan, observed, ActionCatchUpTail)

	tail, err := plan.partitioner.InitialTailCursor(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	observed.Tail = &tail
	if _, err := source.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	state = flowSourceState(t, source.machine)
	observed.SourceState = state
	observed.SourceStatus = testLeaderStatus(state)
	assertFlowAction(t, plan, observed, ActionCatchUpTail)

	flowTranslateNext(t, plan.partitioner, capture, &tail, stage, persistStage)
	stageCursor, _ = stage.Cursor()
	observed.Stages[1] = &stageCursor
	assertFlowAction(t, plan, observed, ActionSealSource)

	seal, err := plan.AppendSourceSeal(
		make([]byte, 0, replicatedstate.MaxOwnershipTransitionBytes), state, 1, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 3, Term: 2, Type: pb.EntryNormal,
	}, seal); err != nil {
		t.Fatal(err)
	}
	state = flowSourceState(t, source.machine)
	observed.SourceState = state
	observed.SourceStatus = testLeaderStatus(state)
	assertFlowAction(t, plan, observed, ActionCatchUpTail)

	flowTranslateNext(t, plan.partitioner, capture, &tail, stage, persistStage)
	if !tail.Sealed() {
		t.Fatal("translated source seal did not seal tail")
	}
	stageCursor, _ = stage.Cursor()
	observed.Stages[1] = &stageCursor
	assertFlowAction(t, plan, observed, ActionCertifyCutover)

	var cutoverWorkspace rangesplit.CutoverWorkspace
	certificate, err := plan.partitioner.CertifyCutover(
		capture, tail, []rangesplit.ChildStageCursor{stageCursor}, &cutoverWorkspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	observed.Certificate = &certificate
	action = assertFlowAction(t, plan, observed, ActionActivateChild)
	if action.Child != 1 {
		t.Fatalf("activation child = %d, want 1", action.Child)
	}
	if err := stage.CheckActivationCoordinates(
		certificate, flowTargetBinding(target.SQL.Binding),
	); err != nil {
		t.Fatal(err)
	}

	child := &ChildObservation{
		Child: 1, Phase: ChildPhaseActivated,
		ApplyIdentity: sqldriver.ReplicatedApplyIdentity{
			Storage: "apply", ValidationDigest: sha256.Sum256([]byte("flow-apply")),
			MaxCompletions: 8,
		},
		ApplyProfile: sqldriver.ReplicatedApplyCapacityProfile{
			Binding: target.SQL.Binding, Initialized: true,
			Applied: certificate.SourceCut().Applied, MaxCompletions: 8,
		},
	}
	observed.Children[1] = child
	assertFlowAction(t, plan, observed, ActionCreateChildWAL)
	child.Phase = ChildPhaseWALCreated
	child.WALBinding = target.SQL.Binding
	assertFlowAction(t, plan, observed, ActionAdoptChildRuntime)
	child.Phase = ChildPhaseRuntimeAdopted
	child.RuntimeIdentity = testRuntimeIdentity(target)
	child.RuntimeStatus = raftmemberReadyStatus(target, certificate.SourceCut().Applied)
	assertFlowAction(t, plan, observed, ActionPruneRetained)

	pruner, err := rangesplit.NewRetainedPruner(plan.partitioner, certificate, nil)
	if err != nil {
		t.Fatal(err)
	}
	var pruneRaw []byte
	var pruneWorkspace rangesplit.RetainedPruneWorkspace
	pruneCut, err := source.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	batch, hasBatch, advanceErr := pruner.Advance(
		pruneCut, capture, rangesplit.RetainedPruneLimits{},
		func(raw []byte) error {
			pruneRaw = append(pruneRaw[:0], raw...)
			return nil
		},
		&pruneWorkspace,
	)
	closeErr = pruneCut.Close()
	if advanceErr != nil || closeErr != nil || hasBatch || batch.Count != 0 || len(pruneRaw) == 0 {
		t.Fatalf(
			"empty retained prune = %+v, batch=%v, bytes=%d, err=%v, close=%v",
			pruner.Cursor(), hasBatch, len(pruneRaw), advanceErr, closeErr,
		)
	}
	prune := pruner.Cursor()
	observed.Prune = &prune
	assertFlowAction(t, plan, observed, ActionPublishCatalog)

	next, err := plan.BuildCatalogTransition(catalog, certificate, prune)
	if err != nil {
		t.Fatal(err)
	}
	observed.Catalog = next
	assertFlowAction(t, plan, observed, ActionAwaitCatalogDrain)
	recovered, err := RecoverPlan(next, 19, split, plan.partitioner, []ChildTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	observed.OlderCatalogDrained = true
	assertFlowAction(t, recovered, observed, ActionComplete)
	if _, err := plan.BuildCatalogTransition(next, certificate, prune); err == nil {
		t.Fatal("already-published catalog accepted as a new transition source")
	}
}

type flowSource struct {
	machine   *replicatedstate.Machine
	bootstrap *pb.Snapshot
	capture   *durable.Collection
}

type flowSourceValidator struct{}

func (flowSourceValidator) ValidatePut(_, _ []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func (flowSourceValidator) ValidateDelete(
	_, _ []byte,
	_ bool,
) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func newFlowSource(t testing.TB, plan *Plan) flowSource {
	t.Helper()
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
	systemCollection := create("system", durable.Options{})
	userCollection := create("user", durable.Options{
		MaxDocumentBytes: 4096, MaxBatchDocuments: 4, MaxBatchBytes: 32 << 10,
	})
	captureCollection := create("capture", durable.Options{
		MaxDocumentBytes: 128 << 10, MaxBatchDocuments: 1, MaxBatchBytes: 256 << 10,
	})
	target := func(collection *durable.Collection) replicatedstate.CollectionTarget {
		return replicatedstate.CollectionTarget{
			Collection: collection, Validation: replicatedstate.ValidationDeterministicMutation,
			ValidationDigest: sha256.Sum256([]byte("split-controller-flow")),
			Validator:        flowSourceValidator{},
			Limits: replicatedstate.CollectionLimits{
				MaxKeyBytes: collection.MaxKeyBytes(), MaxDocumentBytes: collection.MaxDocumentBytes(),
				MaxDistinctMutations: collection.MaxBatchDocuments(),
				MaxBatchBytes:        collection.MaxBatchBytes(),
			},
		}
	}
	system := target(systemCollection)
	system.Validation = replicatedstate.ValidationSchemaFreeJSON
	system.ValidationDigest = [32]byte{}
	system.Validator = nil
	user := target(userCollection)
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	binding := replicatedstate.Binding{
		ClusterID: replication.ID128(testID(1)), ClusterIncarnation: replication.ID128(testID(2)),
		TopologyRecoveryEpoch: 1,
		Distribution:          string(plan.source.Distribution), Shard: string(plan.source.Shard),
		AllocationGeneration: uint64(plan.source.AllocationGeneration),
		ShardIncarnation:     replication.ID128(testID(7)), GroupID: replication.ID128(testID(8)),
		ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: uint64(plan.source.OwnershipEpoch), SchemaGeneration: 1,
		RoutingVersion: uint64(plan.source.RoutingVersion), RouteGeneration: plan.current,
	}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{
		Data: []byte("split-controller-flow-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
	machine, err := replicatedstate.Open(
		binding, bootstrap, system,
		replicatedstate.UserCollection{Name: "docs", Target: user}, log,
		replicatedstate.Options{
			TxnLimits: durable.TxnLimits{
				MaxCollections: 3, MaxDocuments: user.Limits.MaxDistinctMutations + 3,
				MaxBytes: 64 << 20,
			},
			MaxCompletions: 128,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return flowSource{machine: machine, bootstrap: bootstrap, capture: captureCollection}
}

func flowSourceState(t testing.TB, machine *replicatedstate.Machine) replicatedstate.State {
	t.Helper()
	snapshot, err := machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	state := snapshot.State()
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	return state
}

func flowTranslateNext(
	t testing.TB,
	partitioner *rangesplit.Partitioner,
	capture *rangesplit.SourceCapture,
	tail *rangesplit.TailCursor,
	stage *rangesplit.ChildStage,
	persist rangesplit.ChildStageCursorPersistence,
) {
	t.Helper()
	var captureWorkspace rangesplit.SourceCaptureWorkspace
	entry, ok, err := capture.NextTailEntry(*tail, &captureWorkspace)
	if err != nil || !ok {
		t.Fatalf("next captured entry ok=%v err=%v", ok, err)
	}
	sinks := []rangesplit.TailSink{
		func(rangesplit.TailBatch) error { return nil },
		func(batch rangesplit.TailBatch) error { return stage.ApplyTailBatch(batch, persist) },
	}
	var tailWorkspace rangesplit.TailWorkspace
	next, _, err := partitioner.TranslateTailEntry(*tail, entry, sinks, &tailWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	*tail = next
}

func assertFlowAction(
	t testing.TB,
	plan *Plan,
	observed Observation,
	want ActionKind,
) Action {
	t.Helper()
	action, err := Reconcile(plan, observed)
	if err != nil || action.Kind != want {
		t.Fatalf("action = %+v, want %v, err = %v", action, want, err)
	}
	return action
}

func raftmemberReadyStatus(target ChildTarget, applied uint64) raftmember.RuntimeStatus {
	return raftmember.RuntimeStatus{
		MemberID: target.WAL.MemberID, LeaderID: target.WAL.MemberID,
		Term: 1, Commit: applied, Applied: applied, RaftState: raft.StateLeader,
	}
}

func flowTargetBinding(binding sqldriver.ReplicatedShardStoreBinding) replicatedstate.Binding {
	return replicatedstate.Binding{
		ClusterID:              replication.ID128(binding.ClusterID),
		ClusterIncarnation:     replication.ID128(binding.ClusterIncarnation),
		TopologyRecoveryEpoch:  binding.TopologyRecoveryEpoch,
		Distribution:           binding.Distribution,
		Shard:                  binding.Shard,
		AllocationGeneration:   binding.AllocationGeneration,
		ShardIncarnation:       replication.ID128(binding.ShardIncarnation),
		GroupID:                replication.ID128(binding.GroupID),
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         binding.Authority.OwnershipEpoch,
		SchemaGeneration:       binding.Authority.SchemaGeneration,
		RoutingVersion:         binding.Authority.RoutingVersion,
		RouteGeneration:        binding.Authority.RouteGeneration,
	}
}
