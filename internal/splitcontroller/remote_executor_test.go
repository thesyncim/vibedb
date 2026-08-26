package splitcontroller

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
)

type testShardControlRouter struct {
	calls   int
	request shardcontrol.Request
}

func (router *testShardControlRouter) ExecuteShardControl(
	_ context.Context, action Action, request shardcontrol.Request,
) (shardcontrol.Response, error) {
	router.calls++
	router.request = request
	digest := sha256.Sum256(request.Payload)
	return shardcontrol.Response{
		Code: shardcontrol.ResultAccepted, Operation: request.Operation, Step: request.Step,
		ResultDigest: digest,
	}, nil
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
