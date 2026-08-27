package splitcontroller

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type testPruneFactory struct{}

type testRetainedPruneProposer struct{}

type testCaptureActivationProposer struct{}

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
