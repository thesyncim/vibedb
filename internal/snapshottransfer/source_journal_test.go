package snapshottransfer

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestSourceJournalCodecCanonicalChecksum(t *testing.T) {
	request, descriptor := sourceControlFixture()
	for _, record := range []SourceControlRecord{
		{Request: request, Revision: 1, State: SourceControlRunning},
		{Request: request, Revision: 2, State: SourceControlComplete, Descriptor: descriptor},
	} {
		var storage [MaxSourceJournalBytes]byte
		raw, err := appendSourceJournalRecord(storage[:0], record)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := openSourceJournalRecord(raw)
		if err != nil || opened != record {
			t.Fatalf("open=%+v want=%+v err=%v", opened, record, err)
		}
		reencoded, err := appendSourceJournalRecord(nil, opened)
		if err != nil || string(reencoded) != string(raw) {
			t.Fatalf("noncanonical round trip: %v", err)
		}

		corrupt := append([]byte(nil), raw...)
		corrupt[len(corrupt)/2] ^= 1
		if _, err = openSourceJournalRecord(corrupt); !errors.Is(err, ErrSourceControl) {
			t.Fatalf("corrupt record accepted: %v", err)
		}

		noncanonical := append([]byte(nil), raw...)
		noncanonical[9] = 1
		body := noncanonical[:len(noncanonical)-sourceJournalChecksumBytes]
		binary.BigEndian.PutUint32(noncanonical[len(body):], crc32.Checksum(body, sourceJournalCRC))
		if _, err = openSourceJournalRecord(noncanonical); !errors.Is(err, ErrSourceControl) {
			t.Fatalf("checksummed noncanonical record accepted: %v", err)
		}
	}
}

func TestSourceFileJournalPersistsExactCASAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source-journal")
	request, descriptor := sourceControlFixture()
	journal, err := OpenSourceFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	running := SourceControlRecord{Request: request, Revision: 1, State: SourceControlRunning}
	if err = journal.PublishSourceExport(context.Background(), 0, running); err != nil {
		t.Fatal(err)
	}
	if err = journal.PublishSourceExport(context.Background(), 0, running); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("duplicate create err=%v", err)
	}
	complete := SourceControlRecord{
		Request: request, Revision: 2, State: SourceControlComplete, Descriptor: descriptor,
	}
	wrongRequest := complete
	wrongRequest.Request.Step[0]++
	if err = journal.PublishSourceExport(context.Background(), 1, wrongRequest); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("identity replacement err=%v", err)
	}
	if err = journal.PublishSourceExport(context.Background(), 1, complete); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}

	journal, err = OpenSourceFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	got, err := journal.ReadSourceExport(context.Background(), request.Operation)
	if err != nil || got != complete {
		t.Fatalf("record=%+v err=%v", got, err)
	}
	if err = journal.PublishSourceExport(context.Background(), 1, complete); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("stale CAS err=%v", err)
	}
	if err = journal.PublishSourceExport(context.Background(), 2, complete); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("terminal rewrite err=%v", err)
	}

	second := running
	second.Request.Operation[1]++
	second.Request.Step[1]++
	if err = journal.PublishSourceExport(context.Background(), 0, second); err != nil {
		t.Fatal(err)
	}
	third := running
	third.Request.Operation[2]++
	third.Request.Step[2]++
	if err = journal.PublishSourceExport(context.Background(), 0, third); !errors.Is(err, ErrBound) {
		t.Fatalf("retention bound err=%v", err)
	}
}

func TestSourceFileJournalRecoveryRejectsReducedBoundCorruptionAndUnknownEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source-journal")
	request, _ := sourceControlFixture()
	journal, err := OpenSourceFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	first := SourceControlRecord{Request: request, Revision: 1, State: SourceControlRunning}
	second := first
	second.Request.Operation[1]++
	second.Request.Step[1]++
	if err = journal.PublishSourceExport(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	if err = journal.PublishSourceExport(context.Background(), 0, second); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSourceFileJournal(path, 1); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("reduced retention recovery err=%v", err)
	}

	if err = os.WriteFile(filepath.Join(path, "unknown"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSourceFileJournal(path, 2); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("unknown entry recovery err=%v", err)
	}
	if err = os.Remove(filepath.Join(path, "unknown")); err != nil {
		t.Fatal(err)
	}
	name := sourceJournalName(request.Operation)
	if err = os.WriteFile(filepath.Join(path, name), []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSourceFileJournal(path, 2); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("corrupt record recovery err=%v", err)
	}
}

func TestSourceFileJournalCleansExactTornTemporary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source-journal")
	request, _ := sourceControlFixture()
	journal, err := OpenSourceFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(path, "."+sourceJournalName(request.Operation)+".tmp")
	if err = os.WriteFile(temporary, []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenSourceFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary survived recovery: %v", err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(path, ".unknown.tmp"), []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSourceFileJournal(path, 1); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("noncanonical temporary accepted: %v", err)
	}
}

func TestSourceFileJournalRejectsSymlinksAndSecondWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source-journal")
	first, err := OpenSourceFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if second, openErr := OpenSourceFileJournal(path, 1); openErr == nil {
		_ = second.Close()
		t.Fatal("opened second writer")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}

	request, _ := sourceControlFixture()
	name := sourceJournalName(request.Operation)
	if err = os.Symlink("journal.lock", filepath.Join(path, name)); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSourceFileJournal(path, 1); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("record symlink err=%v", err)
	}
	if err = os.Remove(filepath.Join(path, name)); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("journal.lock", filepath.Join(path, "."+name+".tmp")); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSourceFileJournal(path, 1); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("temporary symlink err=%v", err)
	}

	realPath := filepath.Join(t.TempDir(), "real")
	if err = os.Mkdir(realPath, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedPath := filepath.Join(t.TempDir(), "linked")
	if err = os.Symlink(realPath, linkedPath); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSourceFileJournal(linkedPath+string(os.PathSeparator), 1); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("root symlink err=%v", err)
	}
}

func TestSourceFileJournalRejectsInvalidBoundsAndCancellation(t *testing.T) {
	if _, err := OpenSourceFileJournal("", 1); !errors.Is(err, ErrBound) {
		t.Fatalf("empty path err=%v", err)
	}
	if _, err := OpenSourceFileJournal(t.TempDir(), AbsoluteMaxSourceRecords+1); !errors.Is(err, ErrBound) {
		t.Fatalf("large bound err=%v", err)
	}
	journal, err := OpenSourceFileJournal(filepath.Join(t.TempDir(), "source-journal"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	request, _ := sourceControlFixture()
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("cancelled")
	cancel(want)
	if _, err = journal.ReadSourceExport(ctx, request.Operation); !errors.Is(err, want) {
		t.Fatalf("read cancellation err=%v", err)
	}
	if err = journal.PublishSourceExport(ctx, 0, SourceControlRecord{
		Request: request, Revision: 1, State: SourceControlRunning,
	}); !errors.Is(err, want) {
		t.Fatalf("publish cancellation err=%v", err)
	}
}
