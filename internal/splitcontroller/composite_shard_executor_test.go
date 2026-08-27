package splitcontroller

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/splitcapture"
	pb "go.etcd.io/raft/v3/raftpb"
)

type testPruneFactory struct{}

type testRetainedPruneProposer struct{}

type testCaptureActivationProposer struct{}

type recordingCaptureActivationProposer struct{ body []byte }

func (p *recordingCaptureActivationProposer) ProposeSourceCaptureActivation(_ context.Context, _ OperationID, _ raftservice.ServingFence, body []byte) error {
	p.body = append([]byte(nil), body...)
	return nil
}

func TestCaptureActivationUsesCoherentCutAfterTopologySessionOpen(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	fixture := newFlowSource(t, plan)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	store, err := OpenDurableRuntimeStore(t.TempDir(), plan.OperationID(), testManifestDigest("capture-session-open"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source, err := NewLocalSourceActions(store, fixture.machine, fixture.capture)
	if err != nil {
		t.Fatal(err)
	}
	observed := flowSourceState(t, fixture.machine)
	serving := sourceServingState(t, fixture.machine, observed)
	// Opening the topology session necessarily advances the applied cut after
	// the gateway's observation, without changing any placement authority.
	if _, err := fixture.machine.ApplyNormal(raftmodel.ApplyMeta{Index: observed.Applied + 1, Term: observed.LastTerm, Type: pb.EntryNormal}, nil); err != nil {
		t.Fatal(err)
	}
	current := flowSourceState(t, fixture.machine)
	for _, mutate := range []func(*replicatedstate.State){
		func(candidate *replicatedstate.State) { candidate.Binding.OwnershipEpoch++ },
		func(candidate *replicatedstate.State) { candidate.Binding.SchemaGeneration++ },
		func(candidate *replicatedstate.State) { candidate.Binding.RouteGeneration++ },
		func(candidate *replicatedstate.State) { candidate.ReplicaSetVersion++ },
		func(candidate *replicatedstate.State) { candidate.Applied = observed.Applied - 1 },
		func(candidate *replicatedstate.State) {
			candidate.Applied = observed.Applied
			candidate.LastEntryDigest[0]++
		},
	} {
		candidate := current
		mutate(&candidate)
		if captureCutAdvancesWithinAuthority(observed, candidate) {
			t.Fatal("capture accepted a substituted authority or cut")
		}
	}
	proposer := &recordingCaptureActivationProposer{}
	if _, err := source.ExecuteActivateCapture(t.Context(), plan, observed, serving, proposer); err != nil {
		t.Fatalf("session-open advancement stranded capture activation: %v", err)
	}
	command, err := splitcapture.OpenCommand(proposer.body)
	if err != nil || command.PriorApplied != current.Applied || command.PriorEntryDigest != current.LastEntryDigest || command.PriorDataChainDigest != current.DataChainDigest {
		t.Fatalf("activation did not bind the current coherent cut: %v", err)
	}
}

func (testCaptureActivationProposer) ProposeSourceCaptureActivation(
	context.Context, OperationID, raftservice.ServingFence, []byte,
) error {
	return nil
}

func (testRetainedPruneProposer) ProposeRetainedPrune(
	context.Context, OperationID, raftservice.ServingFence, rangesplit.RetainedPruneBatch,
) error {
	return nil
}

func (testPruneFactory) OpenRetainedPruneProposer(
	context.Context, *Plan, Observation,
) (RetainedPruneProposer, func() error, error) {
	return nil, func() error { return nil }, nil
}

func TestCompositeShardActionExecutorDispatchesDurableSourceCapture(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	sourceFixture := newFlowSource(t, plan)
	if _, err := sourceFixture.machine.InstallSnapshot(sourceFixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	store, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("composite-source"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source, err := NewLocalSourceActions(store, sourceFixture.machine, sourceFixture.capture)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewCompositeShardActionExecutor(CompositeShardActionExecutorOptions{
		Operation: plan.OperationID(), Actions: actionBit(ActionStartCapture), Source: source,
		Capture: testCaptureActivationProposer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	state := flowSourceState(t, sourceFixture.machine)
	observed := Observation{
		Catalog: catalog, SourceState: state, SourceStatus: testLeaderStatus(state),
		SourceServing: sourceServingState(t, sourceFixture.machine, state),
	}
	action, err := Reconcile(plan, observed)
	if err != nil || action.Kind != ActionStartCapture {
		t.Fatalf("action=%+v err=%v", action, err)
	}
	if err = executor.ExecuteSplitAction(t.Context(), plan, observed, action); err != nil {
		t.Fatal(err)
	}
	if _, _, present, err := store.LoadSourceCaptureDescriptor(plan.partitioner); err != nil || !present {
		t.Fatalf("capture present=%v err=%v", present, err)
	}
}

func TestCompositeShardActionExecutorRejectsGatewayAuthority(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	for _, kind := range []ActionKind{
		ActionAwaitSourceLeader, ActionPublishCatalog, ActionAwaitCatalogDrain, ActionComplete,
	} {
		if executor, err := NewCompositeShardActionExecutor(CompositeShardActionExecutorOptions{
			Operation: plan.OperationID(), Actions: actionBit(kind),
		}); executor != nil || err == nil {
			t.Fatalf("kind=%v executor=%v err=%v", kind, executor, err)
		}
	}
}

func TestCompositeShardActionExecutorRequiresOnePruneSessionAuthority(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	sourceFixture := newFlowSource(t, plan)
	store, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("composite-prune"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source, err := NewLocalSourceActions(store, sourceFixture.machine, sourceFixture.capture)
	if err != nil {
		t.Fatal(err)
	}
	base := CompositeShardActionExecutorOptions{
		Operation: plan.OperationID(), Actions: actionBit(ActionPruneRetained), Source: source,
	}
	if executor, err := NewCompositeShardActionExecutor(base); executor != nil || err == nil {
		t.Fatalf("missing authority executor=%v err=%v", executor, err)
	}
	base.PruneFactory = testPruneFactory{}
	if _, err = NewCompositeShardActionExecutor(base); err != nil {
		t.Fatal(err)
	}
	base.Prune = testRetainedPruneProposer{}
	if executor, err := NewCompositeShardActionExecutor(base); executor != nil || err == nil {
		t.Fatalf("ambiguous authority executor=%v err=%v", executor, err)
	}
}
