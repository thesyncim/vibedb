package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"sync"
	"testing"
)

func TestRecoveryJournalReplayReusesRecordSizedScratch(t *testing.T) {
	journal, _ := createTestJournal(t, 8<<20)
	defer journal.Close()
	value := bytes.Repeat([]byte{'v'}, 2<<20)
	if _, err := journal.Append(RecoveryRecordKindPut, 2, []byte("key"), value); err != nil {
		t.Fatal(err)
	}
	if err := journal.Sync(false); err != nil {
		t.Fatal(err)
	}
	count := 0
	visit := func(record RecoveryRecord) error {
		if record.Generation != 2 || !bytes.Equal(record.Key, []byte("key")) || !bytes.Equal(record.Value, value) {
			return errors.New("replay changed exact row")
		}
		count++
		return nil
	}
	if err := journal.Replay(1, visit); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 20 {
		if err := journal.Replay(1, visit); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	if count != 21 {
		t.Fatalf("replayed %d records", count)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 128<<10 {
		t.Fatalf("warm real journal replay allocated %d bytes; record scratch was not reused", allocated)
	}
	if cap(journal.replayScratch.buffer) < len(value) || cap(journal.replayScratch.buffer) > recoveryReplayRetainedBytes {
		t.Fatalf("cached bytes=%d", cap(journal.replayScratch.buffer))
	}
}

func TestRecoveryJournalReplayNestedAndConcurrentBorrowing(t *testing.T) {
	journal, _ := createTestJournal(t, 1<<20)
	defer journal.Close()
	first, second := bytes.Repeat([]byte{'a'}, 100<<10), bytes.Repeat([]byte{'b'}, 100<<10)
	for i, value := range [][]byte{first, second} {
		if _, err := journal.Append(RecoveryRecordKindPut, uint64(i+2), []byte{byte('a' + i)}, value); err != nil {
			t.Fatal(err)
		}
	}
	visit := func(record RecoveryRecord) error {
		want := first
		if record.Generation == 3 {
			want = second
		}
		if !bytes.Equal(record.Value, want) {
			return errors.New("borrowed row overwritten")
		}
		return nil
	}
	if err := journal.Replay(1, func(record RecoveryRecord) error {
		if err := visit(record); err != nil {
			return err
		}
		if err := journal.Replay(1, visit); err != nil {
			return err
		}
		return visit(record)
	}); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	errorsOut := make(chan error, 4)
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 5 {
				if err := journal.Replay(1, visit); err != nil {
					errorsOut <- err
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
}

func TestRecoveryJournalReplayScratchRereadsAndPreservesErrors(t *testing.T) {
	journal, _ := createTestJournal(t, 1<<20)
	defer journal.Close()
	appendPut(t, journal, 2, "key", "value")
	stop := errors.New("stop callback")
	if err := journal.Replay(1, func(RecoveryRecord) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("callback error=%v", err)
	}
	if cap(journal.replayScratch.buffer) == 0 {
		t.Fatal("callback error lost bounded scratch")
	}
	original := make([]byte, RecoveryJournalMinSectorSize)
	if _, err := readFullAt(journal.file, original, recoveryJournalRegionStart); err != nil {
		t.Fatal(err)
	}
	// A warmed buffer must not bypass disk reads or turn torn bytes into the
	// previous valid record. Existing alternate-layout tests cover hard errors.
	if _, err := journal.file.WriteAt([]byte{0xff}, recoveryJournalRegionStart+RecoveryJournalRecordPrefixSize); err != nil {
		t.Fatal(err)
	}
	if err := journal.Replay(1, func(RecoveryRecord) error { t.Fatal("replayed stale cached bytes over torn record"); return nil }); err != nil {
		t.Fatalf("torn tail=%v", err)
	}
	// Authenticated semantic damage remains a hard error with a warm buffer.
	original[6] = 1
	bodyEnd := RecoveryJournalRecordPrefixSize + len("key") + len("value")
	crc := PageChecksum(original[:bodyEnd])
	binary.LittleEndian.PutUint32(original[bodyEnd:bodyEnd+4], crc)
	binary.LittleEndian.PutUint32(original[bodyEnd+4:bodyEnd+8], ^crc)
	if _, err := journal.file.WriteAt(original, recoveryJournalRegionStart); err != nil {
		t.Fatal(err)
	}
	if err := journal.Replay(1, func(RecoveryRecord) error { t.Fatal("semantic corruption exposed a record"); return nil }); !errors.Is(err, ErrRecoveryJournalRecord) {
		t.Fatalf("semantic damage=%v", err)
	}
}

func TestRecoveryJournalReplayReuseDoesNotExpandSmallReadAhead(t *testing.T) {
	journal, _ := createTestJournal(t, 8<<20)
	defer journal.Close()
	appendPut(t, journal, 2, "key", "value")
	stream := recoveryRecordStream{buffer: make([]byte, 0, recoveryReplayRetainedBytes)}
	if err := stream.open(journal.file, journal.header.Capacity); err != nil {
		t.Fatal(err)
	}
	if record, _, err := stream.record(0, journal.header.SectorSize, 1); err != nil || !bytes.Equal(record.Value, []byte("value")) {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if len(stream.buffer) != recoveryReadWindowBytes {
		t.Fatalf("small replay read %d bytes with reused storage", len(stream.buffer))
	}
}

func TestRecoveryJournalReplayScratchDoesNotRetainMaximumOrRepopulateClosed(t *testing.T) {
	var scratch recoveryReplayScratch
	scratch.put(make([]byte, recoveryReplayRetainedBytes+1))
	if scratch.buffer != nil {
		t.Fatal("retained oversized scratch")
	}
	scratch.put(make([]byte, recoveryReadWindowBytes))
	active := scratch.take()
	if cap(active) != recoveryReadWindowBytes || scratch.buffer != nil {
		t.Fatal("active buffer not detached")
	}
	scratch.close()
	scratch.put(active)
	if scratch.buffer != nil {
		t.Fatal("closed cache repopulated")
	}
}
