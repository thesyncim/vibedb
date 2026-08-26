package snapshottransfer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBootstrapJournalCodecRoundTripsRunningAndComplete(t *testing.T) {
	request, identity, _ := bootstrapControlFixture()
	for _, record := range []BootstrapRecord{
		{Request: request, Revision: 1, State: BootstrapRunning},
		{Request: request, Revision: 2, State: BootstrapComplete, Identity: identity},
	} {
		var storage [MaxBootstrapJournalBytes]byte
		raw, err := appendBootstrapJournalRecord(storage[:0], record)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := openBootstrapJournalRecord(raw)
		if err != nil || opened != record {
			t.Fatalf("opened=%+v want=%+v err=%v", opened, record, err)
		}
		reencoded, err := appendBootstrapJournalRecord(nil, opened)
		if err != nil || string(reencoded) != string(raw) {
			t.Fatalf("noncanonical re-encode err=%v", err)
		}
	}
}

func TestBootstrapFileJournalPersistsCASAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap-journal")
	request, identity, _ := bootstrapControlFixture()
	journal, err := OpenBootstrapFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	running := BootstrapRecord{Request: request, Revision: 1, State: BootstrapRunning}
	if err = journal.PublishBootstrap(context.Background(), 0, running); err != nil {
		t.Fatal(err)
	}
	if err = journal.PublishBootstrap(context.Background(), 0, running); !errors.Is(err, ErrBootstrapConflict) {
		t.Fatalf("duplicate create err=%v", err)
	}
	complete := BootstrapRecord{
		Request: request, Revision: 2, State: BootstrapComplete, Identity: identity,
	}
	if err = journal.PublishBootstrap(context.Background(), 1, complete); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}

	journal, err = OpenBootstrapFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	got, err := journal.ReadBootstrap(context.Background(), request.Operation)
	if err != nil || got != complete {
		t.Fatalf("record=%+v err=%v", got, err)
	}
	if err = journal.PublishBootstrap(context.Background(), 1, complete); !errors.Is(err, ErrBootstrapConflict) {
		t.Fatalf("stale CAS err=%v", err)
	}

	second := request
	second.Operation[0]++
	second.Step[0]++
	if err = journal.PublishBootstrap(context.Background(), 0, BootstrapRecord{
		Request: second, Revision: 1, State: BootstrapRunning,
	}); err != nil {
		t.Fatal(err)
	}
	third := request
	third.Operation[1]++
	third.Step[1]++
	if err = journal.PublishBootstrap(context.Background(), 0, BootstrapRecord{
		Request: third, Revision: 1, State: BootstrapRunning,
	}); !errors.Is(err, ErrBound) {
		t.Fatalf("capacity err=%v", err)
	}
}

func TestBootstrapFileJournalRecoveryRejectsCorruptionAndCleansTemporary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap-journal")
	request, _, _ := bootstrapControlFixture()
	journal, err := OpenBootstrapFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	name := bootstrapJournalRecordName(request.Operation)
	if err = os.WriteFile(filepath.Join(path, "."+name+".tmp"), []byte("discard"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenBootstrapFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(path, "."+name+".tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary still exists: %v", err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(path, name), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenBootstrapFileJournal(path, 1); !errors.Is(err, ErrBootstrapControl) {
		t.Fatalf("corrupt recovery err=%v", err)
	}
}

func TestBootstrapFileJournalEnforcesSingleWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap-journal")
	first, err := OpenBootstrapFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := OpenBootstrapFileJournal(path, 1); err == nil {
		_ = second.Close()
		t.Fatal("opened a second bootstrap writer")
	}
}
