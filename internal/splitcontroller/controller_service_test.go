package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
)

type testControllerCatalog struct {
	*memoryReplicatedOperationJournal
	catalog *gateway.Snapshot
}

func (catalog *testControllerCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	return catalog.catalog, nil
}

func (catalog *testControllerCatalog) ReadOperationIDs(context.Context) ([][32]byte, error) {
	if catalog == nil || catalog.memoryReplicatedOperationJournal == nil ||
		!catalog.memoryReplicatedOperationJournal.present {
		return nil, nil
	}
	return [][32]byte{catalog.memoryReplicatedOperationJournal.record.ID}, nil
}

type testPlanObserver struct {
	operation OperationID
	observed  Observation
	calls     int
}

func TestDirectControllerPassKeepsCatalogAuthorityInGateway(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: snapshot, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	action, err := Reconcile(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := AppendPlanIntent(nil, snapshot, plan)
	if err != nil {
		t.Fatal(err)
	}
	id := [32]byte(plan.OperationID())
	cursor := replicatedActionCursor(action)
	record := gateway.ReplicatedOperationRecord{
		ID: id, Kind: gateway.ReplicatedOperationSplit,
		State: gateway.ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: snapshot.Generation(), Cursor: cursor,
		Proof: replicatedActionProof(id, cursor), IntentDigest: sha256.Sum256(intent), Intent: intent,
	}
	journal := &memoryReplicatedOperationJournal{record: record, present: true}
	catalog := &testControllerCatalog{memoryReplicatedOperationJournal: journal, catalog: snapshot}
	observer := &testPlanObserver{operation: plan.OperationID(), observed: observed}
	router := new(testShardControlRouter)
	controller, err := NewControllerService(catalog, observer, router)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := RunDirectControllerPass(t.Context(), catalog, controller)
	if err != nil || pass.Discovered != 1 || pass.Triggered != 1 || pass.Completed != 0 ||
		observer.calls != 1 || router.calls != 1 || journal.record.State != gateway.ReplicatedOperationRunning {
		t.Fatalf("pass=%+v observer=%d router=%d record=%+v err=%v",
			pass, observer.calls, router.calls, journal.record, err)
	}
}

func (observer *testPlanObserver) ObservePlan(
	_ context.Context, plan *Plan,
) (Observation, error) {
	if plan.OperationID() != observer.operation {
		return Observation{}, ErrControllerTrigger
	}
	observer.calls++
	return observer.observed, nil
}

func TestControllerServiceReconstructsRF3PlanBeforeFencedRemoteStep(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: snapshot, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	action, err := Reconcile(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := AppendPlanIntent(nil, snapshot, plan)
	if err != nil {
		t.Fatal(err)
	}
	id := [32]byte(plan.OperationID())
	cursor := replicatedActionCursor(action)
	record := gateway.ReplicatedOperationRecord{
		ID: id, Kind: gateway.ReplicatedOperationSplit,
		State: gateway.ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: snapshot.Generation(), Cursor: cursor,
		Proof: replicatedActionProof(id, cursor), IntentDigest: sha256.Sum256(intent), Intent: intent,
	}
	journal := &memoryReplicatedOperationJournal{record: record, present: true}
	catalog := &testControllerCatalog{memoryReplicatedOperationJournal: journal, catalog: snapshot}
	observer := &testPlanObserver{operation: plan.OperationID(), observed: observed}
	router := new(testShardControlRouter)
	service, err := NewControllerService(catalog, observer, router)
	if err != nil {
		t.Fatal(err)
	}
	request, err := AppendReconcileTrigger(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteAction(
		context.Background(), rafttransport.PeerIdentity{}, request,
	)
	if err != nil || response.Operation != request.Operation || response.Step != request.Step ||
		observer.calls != 1 || router.calls != 1 ||
		journal.record.State != gateway.ReplicatedOperationRunning || journal.record.Revision != 4 ||
		journal.record.ExecutionRevision != 3 || !journal.record.ExecutionSettled {
		t.Fatalf("response=%+v observer=%d router=%d state=%d revision=%d execution=%d settled=%t err=%v",
			response, observer.calls, router.calls, journal.record.State, journal.record.Revision,
			journal.record.ExecutionRevision, journal.record.ExecutionSettled, err)
	}
	requests, err := openRemoteExecution(journal.record, action)
	if err != nil || len(requests) != 1 {
		t.Fatalf("retained execution requests=%d err=%v", len(requests), err)
	}
	retained, err := shardcontrol.AppendRequest(nil, &requests[0])
	if err != nil {
		t.Fatal(err)
	}
	sent, err := shardcontrol.AppendRequest(nil, &router.request)
	if err != nil || !bytes.Equal(retained, sent) {
		t.Fatalf("sent request differs from its durable execution: %v", err)
	}
	if _, err = service.ExecuteAction(
		context.Background(), rafttransport.PeerIdentity{}, request,
	); err == nil {
		t.Fatal("stale trigger reached execution instead of requiring journal replay")
	}
}

func TestControllerServiceRecoversPendingWaveWithoutNewObservation(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 1
	observed := Observation{Catalog: snapshot, SourceState: state, SourceNode: rafttransport.NodeID{1}}
	journal := &memoryReplicatedOperationJournal{}
	router := &testShardControlRouter{lost: true}
	if _, err := ExecuteRemoteReplicatedStep(t.Context(), journal, plan, observed, router); err == nil {
		t.Fatal("expected lost reply")
	}
	first, err := shardcontrol.AppendRequest(nil, &router.request)
	if err != nil {
		t.Fatal(err)
	}
	// This observer refuses the operation: exact pending replay must not ask
	// it for a new cut after a remote effect may already have committed.
	observer := &testPlanObserver{}
	catalog := &testControllerCatalog{memoryReplicatedOperationJournal: journal, catalog: snapshot}
	service, err := NewControllerService(catalog, observer, router)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ExecuteReplicatedOperation(t.Context(), journal.record.ID); err != nil {
		t.Fatalf("pending replay required unavailable new observation: %v", err)
	}
	retried, err := shardcontrol.AppendRequest(nil, &router.request)
	if err != nil || !bytes.Equal(first, retried) || observer.calls != 0 || router.calls != 2 || !journal.record.ExecutionSettled {
		t.Fatalf("pending request changed or remained unsettled: %v", err)
	}
}
