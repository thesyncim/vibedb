package replicaaction

import (
	"errors"
	"os"
	"testing"
)

func TestSourceRetirementsSurviveCloseBoundaryCrash(t *testing.T) {
	for _, state := range []State{Running, RetirementAuthorized, Complete} {
		t.Run(string(rune('0'+state)), func(t *testing.T) {
			path := t.TempDir()
			journal, err := OpenFileJournal(path, 8)
			if err != nil {
				t.Fatal(err)
			}
			request := actionFixture(t, SourceRetirement)
			record := Record{Request: request, Revision: 1, State: Running}
			if err = journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
				t.Fatal(err)
			}
			if state != Running {
				record.Revision++
				record.State = RetirementAuthorized
				if err = journal.PublishReplicaAction(t.Context(), 1, record); err != nil {
					t.Fatal(err)
				}
			}
			if state == Complete {
				record.Revision++
				record.State = Complete
				if err = journal.PublishReplicaAction(t.Context(), 2, record); err != nil {
					t.Fatal(err)
				}
			}
			if err = journal.Close(); err != nil {
				t.Fatal(err)
			}
			journal, err = OpenFileJournal(path, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			records, err := journal.SourceRetirements(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if state == Running {
				if len(records) != 0 {
					t.Fatal("unproven retirement suppressed startup")
				}
				return
			}
			if len(records) != 1 || !equalRecord(records[0], record) {
				t.Fatalf("recovered tombstones=%+v, want %+v", records, record)
			}
			records[0].Request.Fence.StoreID[0]++
			reopened, err := journal.ReadReplicaAction(t.Context(), request.Operation, request.Kind)
			if err != nil || !equalRecord(reopened, record) {
				t.Fatalf("inventory mutated retained request: %+v, %v", reopened, err)
			}
		})
	}
}

func TestSourceRetirementsRequireDurableAuthorizedMarker(t *testing.T) {
	journal, err := OpenFileJournal(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	record := Record{Request: actionFixture(t, SourceRetirement), Revision: 1, State: Running}
	if err = journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("retirement directory sync unavailable")
	journal.syncRoot = func(*os.Root) error { return failure }
	record.State, record.Revision = RetirementAuthorized, 2
	if err = journal.PublishReplicaAction(t.Context(), 1, record); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("unsynced authorization=%v", err)
	}
	if records, err := journal.SourceRetirements(t.Context()); !errors.Is(err, ErrOutcomeUnknown) || len(records) != 0 {
		t.Fatalf("unsynced tombstone inventory=%+v, %v", records, err)
	}
	journal.syncRoot = syncReplicaActionRoot
	if records, err := journal.SourceRetirements(t.Context()); err != nil || len(records) != 1 || !equalRecord(records[0], record) {
		t.Fatalf("settled tombstone inventory=%+v, %v", records, err)
	}
}

func TestSourceRetirementsRejectClosedJournal(t *testing.T) {
	journal, err := OpenFileJournal(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = journal.SourceRetirements(t.Context()); !errors.Is(err, ErrControl) {
		t.Fatalf("closed journal inventory=%v", err)
	}
}
