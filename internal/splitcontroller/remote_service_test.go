package splitcontroller

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"go.etcd.io/raft/v3"
)

type testShardActionRuntime struct {
	plan     *Plan
	observed Observation
	calls    int
}

type witnessedActionExecutorStub struct{ calls int }

func (stub *witnessedActionExecutorStub) ExecuteSplitAction(
	context.Context, *Plan, Observation, Action,
) error {
	return ErrRemoteExecution
}

func (stub *witnessedActionExecutorStub) ExecuteAuthorizedSplitAction(
	_ context.Context, _ *Plan, _ Observation, action Action,
) error {
	if action.Kind != ActionStartCapture {
		return ErrRemoteExecution
	}
	stub.calls++
	return nil
}

func (runtime *testShardActionRuntime) ObserveSplit(
	_ context.Context, operation OperationID, digest [32]byte, _ ShardActionTarget,
) (*Plan, Observation, error) {
	intent, err := AppendPlanIntent(nil, runtime.observed.Catalog, runtime.plan)
	if err != nil || runtime.plan.OperationID() != operation || sha256.Sum256(intent) != digest {
		return nil, Observation{}, ErrRemoteExecution
	}
	return runtime.plan, runtime.observed, nil
}

func (runtime *testShardActionRuntime) ExecuteSplitAction(
	_ context.Context, _ ShardActionTarget, plan *Plan, _ Observation, action Action,
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

func TestRemoteActionServiceExecutesGatewayWitnessWithoutShardController(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	binding := state.Binding
	serving := raftservice.ServingState{
		Identity: raftmember.RuntimeIdentity{
			Group: raftmember.GroupKey{
				ClusterID: [16]byte(binding.ClusterID), ClusterIncarnation: [16]byte(binding.ClusterIncarnation),
				TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
				ShardIncarnation:      [16]byte(binding.ShardIncarnation), GroupID: [16]byte(binding.GroupID),
			},
			Distribution: binding.Distribution, Shard: binding.Shard,
			AllocationGeneration: binding.AllocationGeneration,
			MemberID:             1, StoreID: [16]byte{8}, NodeIncarnation: 1,
		},
		Command: raftservice.CommandFence{
			ReplicaSetVersion:      state.ReplicaSetVersion,
			ActivePolicyGeneration: binding.ActivePolicyGeneration, ProtectionEpoch: binding.ProtectionEpoch,
			OwnershipEpoch: binding.OwnershipEpoch, SchemaGeneration: binding.SchemaGeneration,
			RelationManifestDigest: planRelationManifestDigest(plan), RoutingVersion: binding.RoutingVersion,
			RouteGeneration: binding.RouteGeneration,
		},
		Status: raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: state.LastTerm,
			Commit: state.Applied, Applied: state.Applied, RaftState: raft.StateLeader},
	}
	observed := Observation{Catalog: catalog, SourceState: state,
		SourceStatus: serving.Status, SourceServing: serving}
	action, err := Reconcile(plan, observed)
	if err != nil || action.Kind != ActionStartCapture {
		t.Fatalf("action=%+v err=%v", action, err)
	}
	request, err := appendRemoteStepRequest(nil, plan, observed, action)
	if err != nil {
		t.Fatal(err)
	}
	target, err := OpenRemoteActionTarget(request)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "runtime")
	if err = os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := OpenRuntimeStoreRegistry(root, [32]byte{9}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	lease, err := registry.Acquire(plan.OperationID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release() })
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	executor := &witnessedActionExecutorStub{}
	grants, err := NewDynamicShardActionGrants(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = grants.Install([]ShardActionGrant{{
		Operation: plan.OperationID(), PlanDigest: request.PlanDigest, Target: target,
		Plan: plan, Executor: executor, Actions: actionBit(ActionStartCapture),
		Admission: admission, Catalog: catalog, Leases: []*RuntimeStoreLease{lease},
	}}); err != nil {
		t.Fatal(err)
	}
	runtime, err := NewShardActionRuntimeDispatcher(grants)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewRemoteActionService(runtime)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteAction(t.Context(), rafttransport.PeerIdentity{}, request)
	if err != nil || response.Code == 0 || executor.calls != 1 {
		t.Fatalf("response=%+v calls=%d err=%v", response, executor.calls, err)
	}
}
