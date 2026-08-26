package splitcontroller

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

type planAdmissionGrantFactoryStub struct {
	grants []ShardActionGrant
	calls  int
}

func (factory *planAdmissionGrantFactoryStub) BuildAdmittedShardActionGrants(
	_ context.Context, _ *gateway.Snapshot, plan *Plan, _ PlanAdmission, _ []*RuntimeStoreLease,
) ([]ShardActionGrant, error) {
	factory.calls++
	result := append([]ShardActionGrant(nil), factory.grants...)
	for index := range result {
		result[index].Plan = plan
	}
	return result, nil
}

func TestBoundPlanAdmissionRebindsOnePublishedCatalogGeneration(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state}
	action, err := Reconcile(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	target, err := remoteActionTarget(plan, observed, action)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	published := plan.targetSnapshotForTest(t)
	second, err := NewPlanAdmission(published, plan)
	if err != nil || second.PlanDigest != first.PlanDigest ||
		second.CatalogGeneration != first.CatalogGeneration+1 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	recovered, err := second.Open(published)
	if err != nil {
		t.Fatal(err)
	}
	dynamic, err := NewDynamicShardActionGrants(1)
	if err != nil {
		t.Fatal(err)
	}
	factory := &planAdmissionGrantFactoryStub{grants: []ShardActionGrant{{
		Operation: plan.OperationID(), PlanDigest: first.PlanDigest, Target: target,
		Observer: &testPlanObserver{operation: plan.OperationID(), observed: observed},
		Executor: new(recordingShardActionExecutor), Actions: actionBit(action.Kind),
	}}}
	binder, err := NewBoundPlanAdmissionBinder(factory, dynamic)
	if err != nil {
		t.Fatal(err)
	}
	newLease := func(marker string) *RuntimeStoreLease {
		store, openErr := OpenDurableRuntimeStore(
			t.TempDir(), plan.OperationID(), testManifestDigest(marker),
		)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { _ = store.Close() })
		return &RuntimeStoreLease{store: store}
	}
	if err = binder.BindPlanAdmission(t.Context(), catalog, plan, first,
		[]*RuntimeStoreLease{newLease("before-publish")}); err != nil {
		t.Fatal(err)
	}
	if err = binder.BindPlanAdmission(t.Context(), published, recovered, second,
		[]*RuntimeStoreLease{newLease("after-publish")}); err != nil {
		t.Fatal(err)
	}
	resolved, _, err := newShardActionRuntimeDispatcherForTest(t, dynamic).ObserveSplit(
		t.Context(), plan.OperationID(), second.PlanDigest, target,
	)
	if err != nil || resolved != plan || factory.calls != 1 {
		t.Fatalf("resolved=%p recovered=%p calls=%d err=%v", resolved, recovered, factory.calls, err)
	}
}

func newShardActionRuntimeDispatcherForTest(
	t testing.TB, grants shardActionGrantResolver,
) *ShardActionRuntimeDispatcher {
	t.Helper()
	runtime, err := NewShardActionRuntimeDispatcher(grants)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestDynamicShardActionGrantAppearsOnlyThroughBoundAdmission(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state}
	action, err := Reconcile(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	target, err := remoteActionTarget(plan, observed, action)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	dynamic, err := NewDynamicShardActionGrants(2)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewShardActionRuntimeDispatcher(dynamic)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = runtime.ObserveSplit(
		t.Context(), plan.OperationID(), admission.PlanDigest, target,
	); err == nil {
		t.Fatal("unadmitted action capability was visible")
	}
	observer := &testPlanObserver{operation: plan.OperationID(), observed: observed}
	executor := new(recordingShardActionExecutor)
	factory := &planAdmissionGrantFactoryStub{grants: []ShardActionGrant{{
		Operation: plan.OperationID(), PlanDigest: admission.PlanDigest, Target: target,
		Plan: plan, Observer: observer, Executor: executor, Actions: 1 << uint(action.Kind-1),
	}}}
	binder, err := NewBoundPlanAdmissionBinder(factory, dynamic)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("dynamic-grant"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease := &RuntimeStoreLease{store: store}
	if err = binder.BindPlanAdmission(t.Context(), catalog, plan, admission, []*RuntimeStoreLease{lease}); err != nil {
		t.Fatal(err)
	}
	resolved, cut, err := runtime.ObserveSplit(
		t.Context(), plan.OperationID(), admission.PlanDigest, target,
	)
	if err != nil || resolved != plan || cut.Catalog != catalog || factory.calls != 1 {
		t.Fatalf("resolved=%p calls=%d err=%v", resolved, factory.calls, err)
	}
}
