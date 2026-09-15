package replicaaction

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileJournalExactPublicationRequiresDirectorySync(t *testing.T) {
	for _, test := range []struct {
		name  string
		kind  Kind
		state State
	}{
		{name: "initial", kind: OwnershipTransition, state: Running},
		{name: "complete", kind: OwnershipTransition, state: Complete},
		{name: "retirement_authorized", kind: SourceRetirement, state: RetirementAuthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "actions")
			journal, err := OpenFileJournal(path, 2)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = journal.Close() })
			next := Record{Request: actionFixture(t, test.kind), Revision: 1, State: Running}
			var prior Record
			var expected uint64
			if test.state != Running {
				prior = cloneRecord(next)
				if err := journal.PublishReplicaAction(t.Context(), 0, prior); err != nil {
					t.Fatal(err)
				}
				expected = prior.Revision
				next.Revision, next.State = expected+1, test.state
			}
			fault := errors.New("injected directory sync failure")
			syncs := 0
			journal.syncRoot = func(*os.Root) error {
				syncs++
				return fault
			}
			service := &Service{journal: journal}
			if err := service.publishExact(t.Context(), expected, next); !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, fault) {
				t.Fatalf("uncertain publication resolved without durable rename: %v", err)
			}
			if syncs != 2 {
				t.Fatalf("publish and exact read should each retry sync, got %d", syncs)
			}
			key := replicaActionJournalKey(next.Request.Operation, next.Request.Kind)
			cached, found := journal.records[key]
			if expected == 0 && found || expected != 0 && (!found || !equalRecord(cached, prior)) {
				t.Fatalf("uncertain record entered durable map: found=%v record=%+v", found, cached)
			}
			if len(next.Request.Command) > 0 {
				want := cloneRecord(next)
				next.Request.Command[0] ^= 0xff
				if journal.pending == nil || !equalRecord(*journal.pending, want) {
					t.Fatal("pending record aliases caller request bytes")
				}
				next = want
			}
			if _, err := journal.ReadReplicaAction(t.Context(), next.Request.Operation, next.Request.Kind); !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("read bypassed unresolved directory sync: %v", err)
			}
			if err := journal.PublishReplicaAction(t.Context(), expected, next); !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("retry bypassed unresolved directory sync: %v", err)
			}
			name := filepath.Join(path, replicaActionJournalName(key))
			before, err := os.Stat(name)
			if err != nil {
				t.Fatal(err)
			}
			journal.syncRoot = syncReplicaActionRoot
			if err := service.publishExact(t.Context(), expected, next); err != nil {
				t.Fatalf("exact retry after sync recovery: %v", err)
			}
			after, err := os.Stat(name)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("exact retry rewrote pending rename: %v", err)
			}
			got, err := journal.ReadReplicaAction(t.Context(), next.Request.Operation, next.Request.Kind)
			if err != nil || !equalRecord(got, next) || journal.pending != nil {
				t.Fatalf("settled record=%+v pending=%v err=%v", got, journal.pending != nil, err)
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
			journal, err = OpenFileJournal(path, 2)
			if err != nil {
				t.Fatal(err)
			}
			got, err = journal.ReadReplicaAction(t.Context(), next.Request.Operation, next.Request.Kind)
			if err != nil || !equalRecord(got, next) {
				t.Fatalf("recovered settled record=%+v err=%v", got, err)
			}
		})
	}
}

func TestFileJournalCloseReportsPendingDirectorySync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actions")
	journal, err := OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	fault := errors.New("injected directory sync failure")
	journal.syncRoot = func(*os.Root) error { return fault }
	record := Record{Request: actionFixture(t, SourceRetirement), Revision: 1, State: Running}
	if err := journal.PublishReplicaAction(t.Context(), 0, record); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("publish pending record: %v", err)
	}
	if err := journal.Close(); !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, fault) {
		t.Fatalf("close hid unresolved rename: %v", err)
	}
	journal, err = OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := journal.ReadReplicaAction(t.Context(), record.Request.Operation, record.Request.Kind)
	if err != nil || !equalRecord(got, record) {
		t.Fatalf("recovery did not settle renamed record: %+v %v", got, err)
	}
}

func TestFileJournalRecoverySyncsNamesWithoutTemporaryCleanup(t *testing.T) {
	path := t.TempDir()
	record := Record{Request: actionFixture(t, SourceRetirement), Revision: 1, State: Running}
	raw, err := appendReplicaActionJournalRecord(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	key := replicaActionJournalKey(record.Request.Operation, record.Request.Kind)
	if err := os.WriteFile(filepath.Join(path, replicaActionJournalName(key)), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	fault := errors.New("injected recovery directory sync failure")
	journal := &FileJournal{root: root, maxRecords: 1, records: make(map[[32]byte]Record),
		syncRoot: func(*os.Root) error { return fault }}
	if err := journal.recover(); !errors.Is(err, fault) {
		t.Fatalf("recovery trusted existing filename without sync: %v", err)
	}
}

func TestFileJournalPersistsRetirementAuthorizationAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actions")
	journal, err := OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	record := Record{Request: actionFixture(t, SourceRetirement), Revision: 1, State: Running}
	if err := journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
		t.Fatal(err)
	}
	record.Revision, record.State = 2, RetirementAuthorized
	if err := journal.PublishReplicaAction(t.Context(), 1, record); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := journal.ReadReplicaAction(t.Context(), record.Request.Operation, record.Request.Kind)
	if err != nil || !equalRecord(got, record) {
		t.Fatalf("recovered authorization=%+v err=%v", got, err)
	}
	record.Revision, record.State = 3, Complete
	if err := journal.PublishReplicaAction(t.Context(), 2, record); err != nil {
		t.Fatal(err)
	}
	if err := journal.PublishReplicaAction(t.Context(), 2, record); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale authorization accepted: %v", err)
	}
}

func TestFileJournalExactRetirementRetryAcceptsChangedLocators(t *testing.T) {
	path := t.TempDir()
	journal, err := OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	request := retirementServiceRequest(t, "127.0.0.1:1234")
	original := append([]byte(nil), request.Command...)
	record := Record{Request: request, Revision: 1, State: Running}
	if err := journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
		t.Fatal(err)
	}
	fault := errors.New("injected directory sync failure")
	journal.syncRoot = func(*os.Root) error { return fault }
	record.State, record.Revision = RetirementAuthorized, 2
	service := &Service{journal: journal}
	if err := service.publishExact(t.Context(), 1, record); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("authorization unexpectedly durable: %v", err)
	}
	request.Command[len(request.Command)-1] = '5'
	if journal.pending == nil || !bytesEqual(journal.pending.Request.Command, original) {
		t.Fatal("pending retirement retained mutable caller locator bytes")
	}
	retry := record
	retry.Request = retirementServiceRequest(t, "127.0.0.1:4321")
	if err := service.publishExact(t.Context(), 1, retry); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("changed locator bypassed pending durability: %v", err)
	}
	journal.syncRoot = syncReplicaActionRoot
	if err := service.publishExact(t.Context(), 1, retry); err != nil {
		t.Fatalf("same retirement with changed locator rejected: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	service.journal = journal
	got, err := service.loadOrCreate(t.Context(), retry.Request)
	if err != nil || !equalRecord(got, retry) || !bytesEqual(got.Request.Command, original) {
		t.Fatalf("recovered exact retirement replaced original locators: %+v, %v", got, err)
	}
}
