package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
)

type mixedControllerDirectory struct {
	*testControllerCatalog
	ids     [][32]byte
	records map[[32]byte]gateway.ReplicatedOperationRecord
	errors  map[[32]byte]error
}

func (directory *mixedControllerDirectory) ReadOperationIDs(context.Context) ([][32]byte, error) {
	return append([][32]byte(nil), directory.ids...), nil
}

func (directory *mixedControllerDirectory) ReadOperation(
	ctx context.Context, id [32]byte,
) (gateway.ReplicatedOperationRecord, error) {
	if err := directory.errors[id]; err != nil {
		return gateway.ReplicatedOperationRecord{}, err
	}
	if record, ok := directory.records[id]; ok {
		return record, nil
	}
	return directory.testControllerCatalog.ReadOperation(ctx, id)
}

func validMixedWitness(snapshot *gateway.Snapshot, id [32]byte, kind gateway.ReplicatedOperationKind) gateway.ReplicatedOperationRecord {
	intent := []byte{byte(kind), id[0], 0x5a}
	return gateway.ReplicatedOperationRecord{
		ID: id, Kind: kind, State: gateway.ReplicatedOperationPlanned,
		Revision: 1, CatalogGeneration: snapshot.Generation(), Proof: [32]byte{byte(kind) + 1},
		IntentDigest: sha256.Sum256(intent), Intent: intent,
	}
}

func newDirectControllerLoopFixture(t testing.TB) (
	*gateway.Snapshot, *testControllerCatalog, *testPlanObserver, *testShardControlRouter,
	gateway.ReplicatedOperationRecord,
) {
	t.Helper()
	plan, snapshot, _, _ := testPlan(t)
	return newControllerLoopFixture(t, plan, snapshot)
}

func newReplicatedProjectionControllerLoopFixture(t testing.TB) (
	*gateway.Snapshot, *testControllerCatalog, *testPlanObserver, *testShardControlRouter,
	gateway.ReplicatedOperationRecord,
) {
	t.Helper()
	plan, snapshot, _ := testReplicatedProjectionPlan(t)
	return newControllerLoopFixture(t, plan, snapshot)
}

func newControllerLoopFixture(t testing.TB, plan *Plan, snapshot *gateway.Snapshot) (
	*gateway.Snapshot, *testControllerCatalog, *testPlanObserver, *testShardControlRouter,
	gateway.ReplicatedOperationRecord,
) {
	t.Helper()
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
	operation := [32]byte(plan.OperationID())
	record := gateway.ReplicatedOperationRecord{
		ID: operation, Kind: gateway.ReplicatedOperationSplit,
		State: gateway.ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: snapshot.Generation(), Cursor: replicatedActionCursor(action),
		Proof:        replicatedActionProof(operation, replicatedActionCursor(action)),
		IntentDigest: sha256.Sum256(intent), Intent: intent,
	}
	journal := &memoryReplicatedOperationJournal{record: record, present: true}
	catalog := &testControllerCatalog{memoryReplicatedOperationJournal: journal, catalog: snapshot}
	observer := &testPlanObserver{operation: plan.OperationID(), observed: observed}
	router := new(testShardControlRouter)
	return snapshot, catalog, observer, router, record
}

func TestControllerPassSkipsValidNonSplitWitnessesAndRunsOnlySplit(t *testing.T) {
	snapshot, base, observer, router, splitRecord := newDirectControllerLoopFixture(t)
	ids := [][32]byte{{0x01}, {0x02}, {0x03}, splitRecord.ID}
	extra := map[[32]byte]gateway.ReplicatedOperationRecord{
		ids[0]: validMixedWitness(snapshot, ids[0], gateway.ReplicatedOperationMove),
		ids[1]: validMixedWitness(snapshot, ids[1], gateway.ReplicatedOperationSchema),
		ids[2]: validMixedWitness(snapshot, ids[2], gateway.ReplicatedOperationBackup),
	}
	before := make(map[[32]byte]gateway.ReplicatedOperationRecord, len(extra))
	for id, record := range extra {
		before[id] = record
	}
	directory := &mixedControllerDirectory{
		testControllerCatalog: base,
		ids:                   ids, records: extra,
	}
	controller, err := NewControllerService(directory, observer, router)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := RunDirectControllerPass(context.Background(), directory, controller)
	if err != nil || pass.Discovered != 4 || pass.Triggered != 1 || pass.Completed != 0 ||
		observer.calls != 1 || router.calls != 1 || base.record.State != gateway.ReplicatedOperationRunning {
		t.Fatalf("pass=%+v observer=%d router=%d split=%+v err=%v", pass, observer.calls, router.calls, base.record, err)
	}
	for id, want := range before {
		if got := directory.records[id]; !got.Equal(want) {
			t.Fatalf("non-split witness %x changed: got=%+v want=%+v", id, got, want)
		}
	}
}

func TestControllerPassSkipsValidNonSplitWitnessesInRemoteLoop(t *testing.T) {
	snapshot, base, _, _, splitRecord := newReplicatedProjectionControllerLoopFixture(t)
	ids := [][32]byte{{0x11}, {0x12}, {0x13}, splitRecord.ID}
	extra := map[[32]byte]gateway.ReplicatedOperationRecord{
		ids[0]: validMixedWitness(snapshot, ids[0], gateway.ReplicatedOperationSchema),
		ids[1]: validMixedWitness(snapshot, ids[1], gateway.ReplicatedOperationMove),
		ids[2]: validMixedWitness(snapshot, ids[2], gateway.ReplicatedOperationBackup),
	}
	directory := &mixedControllerDirectory{testControllerCatalog: base, ids: ids, records: extra}
	client := newRecordingControllerTriggerClient()
	pass, err := RunControllerPass(context.Background(), directory, client)
	if err != nil || pass.Discovered != 4 || pass.Triggered != 1 || pass.Completed != 0 || client.calls != 1 {
		t.Fatalf("pass=%+v triggerCalls=%d err=%v", pass, client.calls, err)
	}
	if client.request.Operation != splitRecord.ID || client.request.Action != shardcontrol.ActionReconcileSplit {
		t.Fatalf("triggered non-split operation: request=%+v", client.request)
	}
}

func TestControllerPassRejectsMalformedDirectoryEntriesAndSkipsMissing(t *testing.T) {
	snapshot, base, observer, router, _ := newDirectControllerLoopFixture(t)
	missingID := [32]byte{0x21}
	missing := &mixedControllerDirectory{
		testControllerCatalog: &testControllerCatalog{
			memoryReplicatedOperationJournal: &memoryReplicatedOperationJournal{}, catalog: snapshot,
		},
		ids: [][32]byte{missingID},
	}
	controller, err := NewControllerService(missing, observer, router)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := RunDirectControllerPass(context.Background(), missing, controller)
	if err != nil || pass.Discovered != 1 || pass.Triggered != 0 {
		t.Fatalf("missing record was not skippable: pass=%+v err=%v", pass, err)
	}

	for name, mutate := range map[string]func(gateway.ReplicatedOperationRecord) gateway.ReplicatedOperationRecord{
		"unknown kind": func(record gateway.ReplicatedOperationRecord) gateway.ReplicatedOperationRecord {
			record.Kind = gateway.ReplicatedOperationKind(99)
			return record
		},
		"wrong id": func(record gateway.ReplicatedOperationRecord) gateway.ReplicatedOperationRecord {
			record.ID[0]++
			return record
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, freshBase, freshObserver, freshRouter, splitRecord := newDirectControllerLoopFixture(t)
			badID := splitRecord.ID
			badRecord := mutate(splitRecord)
			directory := &mixedControllerDirectory{
				testControllerCatalog: freshBase,
				ids:                   [][32]byte{badID}, records: map[[32]byte]gateway.ReplicatedOperationRecord{badID: badRecord},
			}
			freshController, controllerErr := NewControllerService(directory, freshObserver, freshRouter)
			if controllerErr != nil {
				t.Fatal(controllerErr)
			}
			_, passErr := RunDirectControllerPass(context.Background(), directory, freshController)
			if !errors.Is(passErr, ErrControllerTrigger) {
				t.Fatalf("malformed record error=%v", passErr)
			}
		})
	}

	readErr := errors.New("directory read failed")
	_, base, observer, router, _ = newDirectControllerLoopFixture(t)
	directory := &mixedControllerDirectory{
		testControllerCatalog: base,
		ids:                   [][32]byte{{0x31}}, errors: map[[32]byte]error{{0x31}: readErr},
	}
	controller, err = NewControllerService(directory, observer, router)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RunDirectControllerPass(context.Background(), directory, controller); !errors.Is(err, readErr) {
		t.Fatalf("read failure was not propagated: %v", err)
	}
}

func TestRemoteControllerPassRejectsMalformedDirectoryEntriesAndSkipsMissing(t *testing.T) {
	_, base, _, _, _ := newReplicatedProjectionControllerLoopFixture(t)
	missingID := [32]byte{0x41}
	missing := &mixedControllerDirectory{testControllerCatalog: base, ids: [][32]byte{missingID}}
	client := newRecordingControllerTriggerClient()
	pass, err := RunControllerPass(context.Background(), missing, client)
	if err != nil || pass.Discovered != 1 || pass.Triggered != 0 || client.calls != 0 {
		t.Fatalf("missing remote record was not skippable: pass=%+v calls=%d err=%v", pass, client.calls, err)
	}

	for name, mutate := range map[string]func(gateway.ReplicatedOperationRecord) gateway.ReplicatedOperationRecord{
		"unknown kind": func(record gateway.ReplicatedOperationRecord) gateway.ReplicatedOperationRecord {
			record.Kind = gateway.ReplicatedOperationKind(99)
			return record
		},
		"wrong id": func(record gateway.ReplicatedOperationRecord) gateway.ReplicatedOperationRecord {
			record.ID[0]++
			return record
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, freshBase, _, _, split := newReplicatedProjectionControllerLoopFixture(t)
			badID := split.ID
			directory := &mixedControllerDirectory{
				testControllerCatalog: freshBase,
				ids:                   [][32]byte{badID},
				records:               map[[32]byte]gateway.ReplicatedOperationRecord{badID: mutate(split)},
			}
			if _, passErr := RunControllerPass(context.Background(), directory, newRecordingControllerTriggerClient()); !errors.Is(passErr, ErrControllerTrigger) {
				t.Fatalf("malformed remote record error=%v", passErr)
			}
		})
	}

	readErr := errors.New("remote directory read failed")
	_, base, _, _, _ = newReplicatedProjectionControllerLoopFixture(t)
	directory := &mixedControllerDirectory{
		testControllerCatalog: base,
		ids:                   [][32]byte{{0x51}}, errors: map[[32]byte]error{{0x51}: readErr},
	}
	if _, err := RunControllerPass(context.Background(), directory, newRecordingControllerTriggerClient()); !errors.Is(err, readErr) {
		t.Fatalf("remote read failure was not propagated: %v", err)
	}
}

type recordingControllerTriggerClient struct {
	calls   int
	route   gateway.ReplicatedRoute
	request shardcontrol.Request
}

func newRecordingControllerTriggerClient() *recordingControllerTriggerClient {
	return new(recordingControllerTriggerClient)
}

func (client *recordingControllerTriggerClient) TriggerSplitController(
	_ context.Context, route gateway.ReplicatedRoute, request shardcontrol.Request,
) (shardcontrol.Response, error) {
	client.calls++
	client.route = route
	client.request = request
	return shardcontrol.Response{Code: shardcontrol.ResultAccepted, Operation: request.Operation, Step: request.Step}, nil
}
