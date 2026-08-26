package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
)

func TestControlServiceDurablyReplaysControllerResultAfterRestart(t *testing.T) {
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
	directory := &memoryReplicatedOperationJournal{record: record, present: true}
	catalog := &testControllerCatalog{memoryReplicatedOperationJournal: directory, catalog: snapshot}
	observer := &testPlanObserver{operation: plan.OperationID(), observed: observed}
	router := new(testShardControlRouter)
	controller, err := NewControllerService(catalog, observer, router)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &testShardActionRuntime{plan: plan, observed: observed}
	remote, err := NewRemoteActionService(runtime)
	if err != nil {
		t.Fatal(err)
	}
	node := rafttransport.NodeID{7}
	path := filepath.Join(t.TempDir(), "split-actions")
	open := func() *ControlService {
		service, openErr := OpenControlService(
			path, shardcontrol.JournalLimits{MaxRecords: 16, MaxFileBytes: 1 << 20},
			[]shardcontrol.ActionGrant{{
				Node: node, Actions: 1 << uint(shardcontrol.ActionReconcileSplit-1),
			}}, controller, remote,
		)
		if openErr != nil {
			t.Fatal(openErr)
		}
		return service
	}
	request, err := AppendReconcileTrigger(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	peer := rafttransport.PeerIdentity{Node: node}
	service := open()
	first, err := service.journal.ExecuteControl(context.Background(), peer, request)
	if err != nil || observer.calls != 1 || router.calls != 1 {
		t.Fatalf("first=%+v observer=%d router=%d err=%v", first, observer.calls, router.calls, err)
	}
	if err = service.Close(); err != nil {
		t.Fatal(err)
	}
	service = open()
	defer service.Close()
	second, err := service.journal.ExecuteControl(context.Background(), peer, request)
	if err != nil || second.Code != first.Code || second.ResultDigest != first.ResultDigest ||
		!bytes.Equal(second.Payload, first.Payload) || observer.calls != 1 || router.calls != 1 {
		t.Fatalf("second=%+v observer=%d router=%d err=%v", second, observer.calls, router.calls, err)
	}
}

func TestControlServiceRejectsPartialRuntime(t *testing.T) {
	service, err := OpenControlService(
		filepath.Join(t.TempDir(), "split-actions"),
		shardcontrol.JournalLimits{MaxRecords: 1, MaxFileBytes: 1 << 20},
		nil, nil, nil,
	)
	if service != nil || err == nil {
		t.Fatalf("service=%v err=%v", service, err)
	}
}
