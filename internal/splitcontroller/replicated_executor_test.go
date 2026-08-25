package splitcontroller

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

type memoryReplicatedOperationJournal struct {
	record      gateway.ReplicatedOperationRecord
	present     bool
	unknownNext bool
	pending     bool
	retries     int
}

func (journal *memoryReplicatedOperationJournal) ReadOperation(
	_ context.Context, id [32]byte,
) (gateway.ReplicatedOperationRecord, error) {
	if !journal.present || journal.record.ID != id {
		return gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing
	}
	return journal.record, nil
}

func (journal *memoryReplicatedOperationJournal) PublishOperation(
	_ context.Context, expected uint64, record gateway.ReplicatedOperationRecord,
) error {
	current := uint64(0)
	if journal.present {
		current = journal.record.Revision
	}
	if current != expected || record.Revision != expected+1 {
		return gateway.ErrReplicatedCatalogConflict
	}
	journal.record, journal.present = record, true
	if journal.unknownNext {
		journal.unknownNext, journal.pending = false, true
		return errors.Join(gateway.ErrReplicatedCatalogPending, errors.New("response lost"))
	}
	return nil
}

func (journal *memoryReplicatedOperationJournal) DeleteOperation(
	_ context.Context, id [32]byte, revision uint64,
) error {
	if !journal.present {
		return nil
	}
	if journal.record.ID != id || journal.record.Revision != revision ||
		journal.record.State < gateway.ReplicatedOperationComplete {
		return gateway.ErrReplicatedCatalogConflict
	}
	journal.present = false
	if journal.unknownNext {
		journal.unknownNext, journal.pending = false, true
		return errors.Join(gateway.ErrReplicatedCatalogPending, errors.New("response lost"))
	}
	return nil
}

func (journal *memoryReplicatedOperationJournal) RetryPending(context.Context) error {
	if !journal.pending {
		return gateway.ErrReplicatedCatalogPending
	}
	journal.pending = false
	journal.retries++
	return nil
}

func TestExecuteReplicatedStepCrashResumeAndAdvance(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	observed := Observation{Catalog: catalog, SourceState: testSourceState(plan)}
	journal := &memoryReplicatedOperationJournal{unknownNext: true}
	calls := 0
	execute := func(_ context.Context, id OperationID, action Action) error {
		calls++
		if id != plan.OperationID() || action.Kind != ActionAwaitSourceLeader {
			t.Fatalf("execution id=%x action=%+v", id, action)
		}
		return nil
	}
	action, err := ExecuteReplicatedStep(context.Background(), journal, plan, observed, execute)
	if err != nil || action.Kind != ActionAwaitSourceLeader || calls != 1 ||
		journal.record.State != gateway.ReplicatedOperationRunning || journal.retries != 1 {
		t.Fatalf("first action=%+v calls=%d record=%+v err=%v", action, calls, journal.record, err)
	}
	// Crash after execute but before observation changes: exact action retries.
	if _, err = ExecuteReplicatedStep(context.Background(), journal, plan, observed, execute); err != nil || calls != 2 {
		t.Fatalf("resume calls=%d err=%v", calls, err)
	}
	// A controller-local stale/non-running record cannot authorize another action.
	journal.record.State = gateway.ReplicatedOperationPlanned
	journal.record.Cursor[0]++
	if _, err = ExecuteReplicatedStep(context.Background(), journal, plan, observed, execute); !errors.Is(err, ErrReplicatedExecution) {
		t.Fatalf("conflicting record err=%v", err)
	}
}

func TestReplicatedTerminalGCSettlesUnknownDelete(t *testing.T) {
	record := gateway.ReplicatedOperationRecord{
		ID: [32]byte{8}, Kind: gateway.ReplicatedOperationSplit,
		State: gateway.ReplicatedOperationComplete, Revision: 9,
		CatalogGeneration: 5, Proof: [32]byte{7},
	}
	journal := &memoryReplicatedOperationJournal{
		record: record, present: true, unknownNext: true,
	}
	if err := settleReplicatedOperationDelete(context.Background(), journal, record); err != nil {
		t.Fatal(err)
	}
	if journal.present || journal.pending || journal.retries != 1 {
		t.Fatalf("terminal GC state = %+v", journal)
	}
}
