package splitcontroller

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type testControllerCatalog struct {
	*memoryReplicatedOperationJournal
	catalog *gateway.Snapshot
}

func (catalog *testControllerCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	return catalog.catalog, nil
}

type testPlanObserver struct {
	operation OperationID
	observed  Observation
	calls     int
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
	observed := Observation{Catalog: snapshot, SourceState: state}
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
		journal.record.State != gateway.ReplicatedOperationRunning || journal.record.Revision != 2 {
		t.Fatalf("response=%+v observer=%d router=%d record=%+v err=%v",
			response, observer.calls, router.calls, journal.record, err)
	}
	if _, err = service.ExecuteAction(
		context.Background(), rafttransport.PeerIdentity{}, request,
	); err == nil {
		t.Fatal("stale trigger reached execution instead of requiring journal replay")
	}
}
