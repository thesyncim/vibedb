package replicaaction

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestReplicaActionJournalCodecCanonicalChecksum(t *testing.T) {
	for _, kind := range []Kind{OwnershipTransition, SourceRetirement} {
		request := actionFixture(t, kind)
		for _, state := range []State{Running, Complete} {
			record := Record{Request: request, Revision: uint64(state), State: state}
			raw, err := appendReplicaActionJournalRecord(nil, record)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := openReplicaActionJournalRecord(raw)
			if err != nil || !equalRecord(opened, record) {
				t.Fatalf("open=%+v err=%v", opened, err)
			}
			reencoded, err := appendReplicaActionJournalRecord(nil, opened)
			if err != nil || !bytesEqual(raw, reencoded) {
				t.Fatalf("noncanonical round trip: %v", err)
			}

			corrupt := append([]byte(nil), raw...)
			corrupt[len(corrupt)/2] ^= 1
			if _, err = openReplicaActionJournalRecord(corrupt); !errors.Is(err, ErrControl) {
				t.Fatalf("corrupt record accepted: %v", err)
			}

			noncanonical := append([]byte(nil), raw...)
			noncanonical[9] = 1
			body := noncanonical[:len(noncanonical)-replicaActionJournalChecksumBytes]
			binary.LittleEndian.PutUint32(noncanonical[len(body):], crc32.Checksum(body, replicaActionJournalCRC))
			if _, err = openReplicaActionJournalRecord(noncanonical); !errors.Is(err, ErrControl) {
				t.Fatalf("checksummed noncanonical record accepted: %v", err)
			}
		}
	}
}

func TestFileJournalPersistsCASAndDetachedBytesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica-actions")
	request := actionFixture(t, OwnershipTransition)
	journal, err := OpenFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	running := Record{Request: cloneRequest(request), Revision: 1, State: Running}
	if err = journal.PublishReplicaAction(context.Background(), 0, running); err != nil {
		t.Fatal(err)
	}
	request.Command[0] ^= 0xff
	got, err := journal.ReadReplicaAction(context.Background(), running.Request.Operation, running.Request.Kind)
	if err != nil || !equalRecord(got, running) {
		t.Fatalf("record=%+v err=%v", got, err)
	}
	got.Request.Command[0] ^= 0xff
	again, err := journal.ReadReplicaAction(context.Background(), running.Request.Operation, running.Request.Kind)
	if err != nil || !equalRecord(again, running) {
		t.Fatalf("read aliased journal bytes: %+v %v", again, err)
	}
	complete := running
	complete.Revision = 2
	complete.State = Complete
	if err = journal.PublishReplicaAction(context.Background(), 1, complete); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}

	journal, err = OpenFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	got, err = journal.ReadReplicaAction(context.Background(), complete.Request.Operation, complete.Request.Kind)
	if err != nil || !equalRecord(got, complete) {
		t.Fatalf("recovered=%+v err=%v", got, err)
	}
	if err = journal.PublishReplicaAction(context.Background(), 1, complete); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS err=%v", err)
	}
	if err = journal.PublishReplicaAction(context.Background(), 2, complete); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal rewrite err=%v", err)
	}
}

func TestFileJournalFailsClosedAtRetentionBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica-actions")
	journal, err := OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	first := Record{Request: actionFixture(t, SourceRetirement), Revision: 1, State: Running}
	if err = journal.PublishReplicaAction(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Request.Operation[1]++
	second.Request.Step[1]++
	if err = journal.PublishReplicaAction(context.Background(), 0, second); !errors.Is(err, ErrBound) {
		t.Fatalf("retention bound err=%v", err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenFileJournal(path, 0); !errors.Is(err, ErrBound) {
		t.Fatalf("invalid bound err=%v", err)
	}
	if journal, err = OpenFileJournal(path, 1); err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err = journal.ReadReplicaAction(context.Background(), first.Request.Operation, first.Request.Kind); err != nil {
		t.Fatal(err)
	}
}

func TestFileJournalRecoveryEnforcesReducedRetentionBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica-actions")
	journal, err := OpenFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	first := Record{Request: actionFixture(t, SourceRetirement), Revision: 1, State: Running}
	second := first
	second.Request.Operation[1]++
	second.Request.Step[1]++
	if err = journal.PublishReplicaAction(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	if err = journal.PublishReplicaAction(context.Background(), 0, second); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenFileJournal(path, 1); !errors.Is(err, ErrControl) {
		t.Fatalf("reduced recovery bound err=%v", err)
	}
}

func TestFileJournalRecoveryCleansRegularTemporaryAndRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica-actions")
	request := actionFixture(t, SourceRetirement)
	journal, err := OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	name := replicaActionJournalName(request.Operation)
	temporary := filepath.Join(path, "."+name+".tmp")
	if err = os.WriteFile(temporary, []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary survived recovery: %v", err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(path, name), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenFileJournal(path, 1); !errors.Is(err, ErrControl) {
		t.Fatalf("corrupt recovery err=%v", err)
	}
}

func TestFileJournalRejectsSymlinksAndSecondWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica-actions")
	first, err := OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if second, openErr := OpenFileJournal(path, 1); openErr == nil {
		_ = second.Close()
		t.Fatal("opened second writer")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}

	operation := actionFixture(t, SourceRetirement).Operation
	name := replicaActionJournalName(operation)
	if err = os.Symlink("journal.lock", filepath.Join(path, name)); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenFileJournal(path, 1); !errors.Is(err, ErrControl) {
		t.Fatalf("record symlink err=%v", err)
	}
	if err = os.Remove(filepath.Join(path, name)); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("journal.lock", filepath.Join(path, "."+name+".tmp")); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenFileJournal(path, 1); !errors.Is(err, ErrControl) {
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
	if _, err = OpenFileJournal(linkedPath+string(os.PathSeparator), 1); !errors.Is(err, ErrControl) {
		t.Fatalf("root symlink err=%v", err)
	}
}

func TestFileJournalSeparatesMoveActionsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actions")
	journal, err := OpenFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	ownership := actionFixture(t, OwnershipTransition)
	retirement := actionFixture(t, SourceRetirement)
	retirement.Operation = ownership.Operation
	for _, request := range []Request{ownership, retirement} {
		if err := journal.PublishReplicaAction(t.Context(), 0, Record{Request: request, Revision: 1, State: Running}); err != nil {
			t.Fatalf("publish kind %d for shared move: %v", request.Kind, err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenFileJournal(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for _, request := range []Request{ownership, retirement} {
		record, err := journal.ReadReplicaAction(t.Context(), request.Operation, request.Kind)
		if err != nil || !equalRequest(record.Request, request) {
			t.Fatalf("recover kind %d: %v", request.Kind, err)
		}
		changed := record
		changed.Request.Step[0] ^= 1
		changed.Revision, changed.State = 2, Complete
		if err := journal.PublishReplicaAction(t.Context(), 1, changed); !errors.Is(err, ErrConflict) {
			t.Fatalf("changed action accepted: %v", err)
		}
		record.Revision, record.State = 2, Complete
		if err := journal.PublishReplicaAction(t.Context(), 1, record); err != nil {
			t.Fatal(err)
		}
	}
}
