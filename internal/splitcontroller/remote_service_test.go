package splitcontroller

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type testShardActionRuntime struct {
	plan     *Plan
	observed Observation
	calls    int
}

func (runtime *testShardActionRuntime) ObserveSplit(
	_ context.Context, operation OperationID,
) (*Plan, Observation, error) {
	if runtime.plan.OperationID() != operation {
		return nil, Observation{}, ErrRemoteExecution
	}
	return runtime.plan, runtime.observed, nil
}

func (runtime *testShardActionRuntime) ExecuteSplitAction(
	_ context.Context, plan *Plan, _ Observation, action Action,
) error {
	if plan != runtime.plan || action.Kind != ActionAwaitSourceLeader {
		return ErrRemoteExecution
	}
	runtime.calls++
	return nil
}

func TestRemoteActionServiceReconcilesExactRequestBeforeExecution(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state}
	action, err := Reconcile(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	request, err := appendRemoteStepRequest(nil, plan, observed, action)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &testShardActionRuntime{plan: plan, observed: observed}
	service, err := NewRemoteActionService(runtime)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteAction(context.Background(), rafttransport.PeerIdentity{}, request)
	if err != nil || runtime.calls != 1 || response.Operation != request.Operation ||
		response.Step != request.Step {
		t.Fatalf("response=%+v calls=%d err=%v", response, runtime.calls, err)
	}
	tampered := request
	tampered.Fence.Applied++
	if _, err = service.ExecuteAction(context.Background(), rafttransport.PeerIdentity{}, tampered); err == nil || runtime.calls != 1 {
		t.Fatalf("tampered request err=%v calls=%d", err, runtime.calls)
	}
}
