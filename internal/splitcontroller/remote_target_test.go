package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type recordingShardActionExecutor struct {
	calls  int
	action Action
}

func (executor *recordingShardActionExecutor) ExecuteSplitAction(
	_ context.Context, _ *Plan, _ Observation, action Action,
) error {
	executor.calls++
	executor.action = action
	return nil
}

func TestShardActionRuntimeDispatcherRequiresExactOperationPlanAndGroupGrant(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	action, err := Reconcile(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	target, err := remoteActionTarget(plan, observed, action)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(intent)
	observer := &testPlanObserver{operation: plan.OperationID(), observed: observed}
	executor := new(recordingShardActionExecutor)
	grants, err := NewStaticShardActionGrants([]ShardActionGrant{{
		Operation: plan.OperationID(), PlanDigest: digest, Target: target, Plan: plan,
		Observer: observer, Executor: executor, Actions: 1 << uint(action.Kind-1),
	}})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewShardActionRuntimeDispatcher(grants)
	if err != nil {
		t.Fatal(err)
	}
	resolved, cut, err := runtime.ObserveSplit(
		context.Background(), plan.OperationID(), digest, target,
	)
	if err != nil || resolved != plan || cut.Catalog != catalog || observer.calls != 1 {
		t.Fatalf("plan=%p cut=%+v calls=%d err=%v", resolved, cut, observer.calls, err)
	}
	if err = runtime.ExecuteSplitAction(context.Background(), target, resolved, cut, action); err != nil || executor.calls != 1 || executor.action != action {
		t.Fatalf("calls=%d action=%+v err=%v", executor.calls, executor.action, err)
	}
	wrong := target
	wrong.Group.GroupID[0]++
	if _, _, err = runtime.ObserveSplit(
		context.Background(), plan.OperationID(), digest, wrong,
	); !errors.Is(err, ErrRemoteExecution) || observer.calls != 1 {
		t.Fatalf("wrong target reached observer: calls=%d err=%v", observer.calls, err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, ok := grants.resolve(plan.OperationID(), digest, target); !ok {
			panic("grant disappeared")
		}
	}); allocations != 0 {
		t.Fatalf("warm grant lookup allocations=%f", allocations)
	}
}

func TestExactShardActionRouteResolverUsesPreGrantedUnpublishedGroup(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	action, err := Reconcile(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	request, err := appendRemoteStepRequest(nil, plan, observed, action)
	if err != nil {
		t.Fatal(err)
	}
	target, err := OpenRemoteActionTarget(request)
	if err != nil {
		t.Fatal(err)
	}
	route := routeForShardActionTarget(target)
	resolver, err := NewExactShardActionRouteResolver([]ShardActionRouteGrant{{
		Operation: plan.OperationID(), PlanDigest: request.PlanDigest, Target: target, Route: route,
	}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.ResolveShardControl(context.Background(), target, action, request)
	if err != nil || resolved.Group != target.Group || resolved.AllocationGeneration != target.Allocation {
		t.Fatalf("route=%+v err=%v", resolved, err)
	}
	wrong := target
	wrong.Allocation++
	if _, err = resolver.ResolveShardControl(
		context.Background(), wrong, action, request,
	); !errors.Is(err, ErrShardControlRoute) {
		t.Fatalf("wrong target err=%v", err)
	}
}

func TestRemoteActionWitnessCarriesDetachedCaptureHead(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{
		Catalog: catalog, SourceState: state, SourceNode: rafttransport.NodeID{1}, CaptureHead: 77,
	}
	action := Action{Kind: ActionBuildArtifacts}
	request, err := appendRemoteStepRequest(nil, plan, observed, action)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := openRemoteStepPayload(request)
	if err != nil {
		t.Fatal(err)
	}
	cut, err := openRemoteWitnessObservation(payload)
	if err != nil || cut.CaptureHead != observed.CaptureHead || cut.Capture != nil {
		t.Fatalf("capture=%d pointer=%p err=%v", cut.CaptureHead, cut.Capture, err)
	}
}

func TestPreparedReplicaMatchAuthenticatesEveryLocalRuntimeField(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	target, ok := plan.Target(1)
	if !ok || len(target.Replicas) == 0 {
		t.Fatal("missing prepared child target")
	}
	original := target.Replicas[0]
	if !targetMatchesPreparedReplica(target, original) {
		t.Fatal("exact prepared replica rejected")
	}
	mutations := []func(*ChildReplicaTarget){
		func(replica *ChildReplicaTarget) { replica.WALPath += ".forged" },
		func(replica *ChildReplicaTarget) { replica.SQLPath += ".forged" },
		func(replica *ChildReplicaTarget) { replica.RuntimeRoot += ".forged" },
		func(replica *ChildReplicaTarget) { replica.WAL.StoreID[0]++ },
		func(replica *ChildReplicaTarget) { replica.Apply.Storage += "-forged" },
		func(replica *ChildReplicaTarget) { replica.ControlEndpoint += "-forged" },
	}
	for index, mutate := range mutations {
		forged := original
		mutate(&forged)
		if targetMatchesPreparedReplica(target, forged) {
			t.Fatalf("mutation %d retained prepared authority", index)
		}
	}
}

func TestRemoteActionEnvelopeBindsOwnedRangeAndRejectsNonCanonicalBytes(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	action, err := Reconcile(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	request, err := appendRemoteStepRequest(nil, plan, observed, action)
	if err != nil || len(request.Payload) == 0 || len(request.Payload) > MaxRemoteStepPayloadBytes {
		t.Fatalf("payload=%d err=%v", len(request.Payload), err)
	}
	wrongAction := action
	wrongAction.Child = 1
	if _, err = appendRemoteStepRequest(nil, plan, observed, wrongAction); err == nil {
		t.Fatal("source action accepted a child target")
	}
	before := remoteObservationStateDigest(state)
	state.Binding.OwnedRange.Start[7]++
	if after := remoteObservationStateDigest(state); after == before {
		t.Fatal("owned range missing from state digest")
	}
	request.Payload = append(request.Payload, ' ')
	if _, err = OpenRemoteActionTarget(request); err == nil {
		t.Fatal("trailing bytes accepted")
	}
}

func FuzzOpenRemoteActionTarget(f *testing.F) {
	plan, catalog, _, _ := testPlan(f)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	action, err := Reconcile(plan, observed)
	if err != nil {
		f.Fatal(err)
	}
	request, err := appendRemoteStepRequest(nil, plan, observed, action)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(request.Payload)
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, payload []byte) {
		candidate := request
		candidate.Payload = payload
		target, openErr := OpenRemoteActionTarget(candidate)
		if openErr == nil && !target.valid() {
			t.Fatal("decoder returned invalid target")
		}
	})
}

func routeForShardActionTarget(target ShardActionTarget) gateway.ReplicatedRoute {
	command := raftservice.CommandFence{
		ReplicaSetVersion: 1, ActivePolicyGeneration: target.Authority.ActivePolicyGeneration,
		ProtectionEpoch: target.Authority.ProtectionEpoch, OwnershipEpoch: target.Authority.OwnershipEpoch,
		SchemaGeneration:       target.Authority.SchemaGeneration,
		RelationManifestDigest: target.RelationManifestDigest,
		RoutingVersion:         target.Authority.RoutingVersion, RouteGeneration: target.Authority.RouteGeneration,
	}
	replicas := make([]gateway.ReplicatedEndpoint, gateway.ServingReplicaCount)
	for index := range replicas {
		replicas[index] = gateway.ReplicatedEndpoint{
			Member: uint64(index + 1), Node: rafttransport.NodeID{byte(index + 1)},
			StoreID: [16]byte{byte(index + 1)}, NodeIncarnation: 1,
			ControlEndpoint: string(rune('a' + index)), ControlAddress: string(rune('x' + index)),
		}
	}
	if target.Member > uint64(len(replicas)) {
		replicas[0].Member = target.Member
	}
	return gateway.ReplicatedRoute{
		Distribution: "pre-granted", Shard: "unpublished", Group: target.Group,
		AllocationGeneration: target.Allocation, Command: command, Replicas: replicas,
	}
}
