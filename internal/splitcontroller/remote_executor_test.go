package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
)

type testShardControlRouter struct {
	calls   int
	request shardcontrol.Request
	lost    bool
}

func TestRemoteExecutionRevisionOrdersAfterLegacyActions(t *testing.T) {
	first := remoteExecutionSequence(Action{Kind: ActionStartCapture}, 1)
	for kind := ActionAwaitSourceLeader; kind <= ActionComplete; kind++ {
		for child := uint8(0); child < 3; child++ {
			if first <= remoteActionWitnessSequence(Action{Kind: kind, Child: child}) {
				t.Fatal("upgraded execution revision regresses a retained legacy witness")
			}
		}
	}
	if remoteExecutionSequence(Action{Kind: ActionCatchUpTail}, 20) <= remoteExecutionSequence(Action{Kind: ActionSealSource}, 19) {
		t.Fatal("post-seal catch-up regressed its witness sequence")
	}
	if remoteExecutionSequence(Action{Kind: ActionStartCapture}, math.MaxUint64) != 0 {
		t.Fatal("exhausted execution sequence wrapped")
	}
}

type testShardControlRouterFunc func(context.Context, Action, shardcontrol.Request) (shardcontrol.Response, error)

func (fn testShardControlRouterFunc) ExecuteShardControl(ctx context.Context, action Action, request shardcontrol.Request) (shardcontrol.Response, error) {
	return fn(ctx, action, request)
}

func TestRemoteExecutionPartialFanoutRecoversExactWaveBeforeAdvancing(t *testing.T) {
	plan, catalog, _, _ := testPlanWithChildLeaders(t, []distribution.EndpointID{"node-b", "node-c", "node-d"})
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	intent, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	action := Action{Kind: ActionActivateChild, Child: 1}
	id := [32]byte(plan.OperationID())
	cursor := replicatedActionCursor(action)
	journal := &memoryReplicatedOperationJournal{present: true, record: gateway.ReplicatedOperationRecord{
		ID: id, Kind: gateway.ReplicatedOperationSplit, State: gateway.ReplicatedOperationRunning,
		Revision: 2, CatalogGeneration: catalog.Generation(), Cursor: cursor,
		Proof: replicatedActionProof(id, cursor), Intent: intent, IntentDigest: sha256.Sum256(intent),
	}}
	var mu sync.Mutex
	frames := make(map[uint64][]byte)
	calls := make(map[uint64]int)
	lost := true
	router := testShardControlRouterFunc(func(_ context.Context, _ Action, request shardcontrol.Request) (shardcontrol.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		requests, err := openRemoteExecution(journal.record, action)
		if err != nil || len(requests) != gateway.ServingReplicaCount || journal.record.ExecutionSettled {
			return shardcontrol.Response{}, errors.New("fanout was not durably recorded before RPC")
		}
		payload, err := openRemoteStepPayload(request)
		if err != nil {
			return shardcontrol.Response{}, err
		}
		member := payload.Target.Member
		raw, err := shardcontrol.AppendRequest(nil, &request)
		if err != nil {
			return shardcontrol.Response{}, err
		}
		if previous, ok := frames[member]; ok && !bytes.Equal(previous, raw) {
			return shardcontrol.Response{}, errors.New("partial-fanout retry changed its exact request")
		}
		frames[member] = raw
		calls[member]++
		if member == 2 && lost {
			lost = false
			return shardcontrol.Response{}, errors.New("lost member 2 reply")
		}
		return shardcontrol.Response{Code: shardcontrol.ResultAccepted, Operation: request.Operation,
			Step: request.Step, ResultDigest: sha256.Sum256(request.Payload)}, nil
	})
	if err := executeRemoteActionWave(t.Context(), journal, plan, observed, action, router, true); err == nil || journal.record.ExecutionSettled || len(journal.record.Execution) == 0 || len(calls) != gateway.ServingReplicaCount {
		t.Fatalf("partial fanout requests=%d bytes=%d settled=%t err=%v", len(calls), len(journal.record.Execution), journal.record.ExecutionSettled, err)
	}
	// Only the catalog record survives controller restart. A different live
	// observation already requests another action, but cannot erase the wave.
	restarted := &memoryReplicatedOperationJournal{present: true, record: journal.record}
	journal = restarted
	observed.SourceState.Applied++
	next, err := Reconcile(plan, observed)
	if err != nil || next == action {
		t.Fatalf("fixture did not advance observation: action=%+v err=%v", next, err)
	}
	got, err := ExecuteReplicatedStep(t.Context(), restarted, plan, observed,
		func(ctx context.Context, _ OperationID, pending Action) error {
			if pending != action {
				return errors.New("observation skipped an unsettled child wave")
			}
			return executeRemoteActionWave(ctx, restarted, plan, observed, pending, router, true)
		})
	if err != nil || got != action || !restarted.record.ExecutionSettled || restarted.record.Cursor != cursor {
		t.Fatalf("restart action=%+v settled=%t err=%v", got, restarted.record.ExecutionSettled, err)
	}
	for member := uint64(1); member <= gateway.ServingReplicaCount; member++ {
		if calls[member] != 2 {
			t.Fatalf("member %d calls=%d, want exact original and retry", member, calls[member])
		}
	}
}

func (router *testShardControlRouter) ExecuteShardControl(
	_ context.Context, action Action, request shardcontrol.Request,
) (shardcontrol.Response, error) {
	router.calls++
	router.request = request
	if router.lost {
		router.lost = false
		return shardcontrol.Response{}, errors.New("lost action reply")
	}
	digest := sha256.Sum256(request.Payload)
	return shardcontrol.Response{
		Code: shardcontrol.ResultAccepted, Operation: request.Operation, Step: request.Step,
		ResultDigest: digest,
	}, nil
}

func TestRemoteExecutionRetriesFrozenRequestThenAdvancesRepeatedAction(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	journal := &memoryReplicatedOperationJournal{}
	router := &testShardControlRouter{lost: true}
	if _, err := ExecuteRemoteReplicatedStep(t.Context(), journal, plan, observed, router); err == nil {
		t.Fatal("expected lost reply")
	}
	first, err := shardcontrol.AppendRequest(nil, &router.request)
	if err != nil {
		t.Fatal(err)
	}
	firstStep := router.request.Step
	// A new controller observes a harmless later apply cut. It must still
	// replay exactly the request that may already be in the shard journal.
	observed.SourceState.Applied++
	if _, err := ExecuteRemoteReplicatedStep(t.Context(), journal, plan, observed, router); err != nil {
		t.Fatal(err)
	}
	retried, err := shardcontrol.AppendRequest(nil, &router.request)
	if err != nil || !bytes.Equal(first, retried) {
		t.Fatalf("outcome-unknown request was rebuilt at a different cut: %v", err)
	}
	// Once the preceding invocation settles, the same action at a newer cut
	// is a new wave, never a same-step payload substitution.
	if _, err := ExecuteRemoteReplicatedStep(t.Context(), journal, plan, observed, router); err != nil {
		t.Fatal(err)
	}
	if router.request.Step == firstStep {
		t.Fatal("successive action waves share a durable retry identity")
	}
}

func TestExecuteRemoteReplicatedStepPersistsThenSendsExactFencedAction(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: catalog, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	journal := &memoryReplicatedOperationJournal{unknownNext: true}
	router := new(testShardControlRouter)
	action, err := ExecuteRemoteReplicatedStep(
		context.Background(), journal, plan, observed, router,
	)
	if err != nil || action.Kind != ActionAwaitSourceLeader || router.calls != 1 ||
		journal.retries != 1 || router.request.Operation != [32]byte(plan.OperationID()) ||
		router.request.PlanDigest == router.request.Operation ||
		router.request.Fence.CatalogGeneration != catalog.Generation() ||
		router.request.Fence.Applied != state.Applied ||
		router.request.Fence.ReplicaSetVersion != state.ReplicaSetVersion {
		t.Fatalf("action=%+v calls=%d retries=%d request=%+v err=%v",
			action, router.calls, journal.retries, router.request, err)
	}
}
