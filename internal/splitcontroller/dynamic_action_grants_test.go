package splitcontroller

import (
	"context"
	"testing"
)

type planAdmissionGrantFactoryStub struct {
	grants []ShardActionGrant
	calls  int
}

func (factory *planAdmissionGrantFactoryStub) BuildAdmittedShardActionGrants(
	_ context.Context, _ *Plan, _ PlanAdmission, _ []*RuntimeStoreLease,
) ([]ShardActionGrant, error) {
	factory.calls++
	return factory.grants, nil
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
	if err = binder.BindPlanAdmission(t.Context(), plan, admission, []*RuntimeStoreLease{lease}); err != nil {
		t.Fatal(err)
	}
	resolved, cut, err := runtime.ObserveSplit(
		t.Context(), plan.OperationID(), admission.PlanDigest, target,
	)
	if err != nil || resolved != plan || cut.Catalog != catalog || factory.calls != 1 {
		t.Fatalf("resolved=%p calls=%d err=%v", resolved, factory.calls, err)
	}
}
