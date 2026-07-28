package storeio

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func testJournalHeader(t *testing.T, capacity uint64) RecoveryJournalHeader {
	t.Helper()
	var storeID, journalID [16]byte
	for i := range storeID {
		storeID[i] = byte(i + 1)
		journalID[i] = byte(0x40 + i)
	}
	return RecoveryJournalHeader{
		StoreID:        storeID,
		JournalID:      journalID,
		PageSize:       4096,
		SectorSize:     RecoveryJournalMinSectorSize,
		BaseGeneration: 1,
		BaseSequence:   0,
		Capacity:       capacity,
	}
}

func createTestJournal(t *testing.T, capacity uint64) (*RecoveryJournal, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.rjournal")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open journal file: %v", err)
	}
	rj, err := CreateRecoveryJournal(file, testJournalHeader(t, capacity))
	if err != nil {
		file.Close()
		t.Fatalf("create journal: %v", err)
	}
	return rj, path
}

func reopenTestJournal(t *testing.T, path string) *RecoveryJournal {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("reopen journal file: %v", err)
	}
	rj, err := OpenRecoveryJournal(file)
	if err != nil {
		file.Close()
		t.Fatalf("open journal: %v", err)
	}
	return rj
}

func appendPut(t *testing.T, rj *RecoveryJournal, generation uint64, key, value string) {
	t.Helper()
	if _, err := rj.Append(recoveryRecordKindPut, generation, []byte(key), []byte(value)); err != nil {
		t.Fatalf("append gen %d: %v", generation, err)
	}
	if err := rj.Sync(false); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func replayAll(t *testing.T, rj *RecoveryJournal, base uint64) []RecoveryRecord {
	t.Helper()
	var out []RecoveryRecord
	err := rj.Replay(base, func(rec RecoveryRecord) error {
		out = append(out, RecoveryRecord{
			Sequence:   rec.Sequence,
			Generation: rec.Generation,
			Kind:       rec.Kind,
			Key:        append([]byte(nil), rec.Key...),
			Value:      append([]byte(nil), rec.Value...),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return out
}

func TestRecoveryJournalHeaderRoundTrip(t *testing.T) {
	h := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	h.RecycleCount = 3
	buf := make([]byte, RecoveryJournalHeaderSize)
	if _, err := EncodeRecoveryJournalHeader(buf, h); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeRecoveryJournalHeader(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != h {
		t.Fatalf("round trip mismatch: %+v != %+v", got, h)
	}
	// Corruptions must fail closed.
	for _, mut := range []struct {
		name  string
		apply func([]byte)
	}{
		{"magic", func(b []byte) { b[0] ^= 0xff }},
		{"version", func(b []byte) { b[8] ^= 0xff }},
		{"checksum", func(b []byte) { b[RecoveryJournalHeaderSize-8] ^= 0x01 }},
		{"identity", func(b []byte) { b[16] ^= 0xff }},
		{"recyclecount", func(b []byte) {
			// Zero the recycle count and reseal so only the invariant rejects it.
			for i := 80; i < 88; i++ {
				b[i] = 0
			}
			sum := PageChecksum(b[:RecoveryJournalHeaderSize-8])
			b[RecoveryJournalHeaderSize-8] = byte(sum)
			b[RecoveryJournalHeaderSize-7] = byte(sum >> 8)
			b[RecoveryJournalHeaderSize-6] = byte(sum >> 16)
			b[RecoveryJournalHeaderSize-5] = byte(sum >> 24)
			b[RecoveryJournalHeaderSize-4] = byte(^sum)
			b[RecoveryJournalHeaderSize-3] = byte(^sum >> 8)
			b[RecoveryJournalHeaderSize-2] = byte(^sum >> 16)
			b[RecoveryJournalHeaderSize-1] = byte(^sum >> 24)
		}},
	} {
		corrupt := append([]byte(nil), buf...)
		mut.apply(corrupt)
		if _, err := DecodeRecoveryJournalHeader(corrupt); err == nil {
			t.Fatalf("%s corruption accepted", mut.name)
		}
	}
}

func TestRecoveryRecordRoundTrip(t *testing.T) {
	rec := RecoveryRecord{Sequence: 5, Generation: 9, Kind: recoveryRecordKindPut,
		Key: []byte("alpha"), Value: []byte("value-bytes")}
	buf := make([]byte, 4096)
	n, err := EncodeRecoveryRecord(buf, RecoveryJournalMinSectorSize, rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if n%RecoveryJournalMinSectorSize != 0 {
		t.Fatalf("record not sector aligned: %d", n)
	}
	got, padded, err := DecodeRecoveryRecord(buf, RecoveryJournalMinSectorSize, 5)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if padded != n || got.Sequence != 5 || got.Generation != 9 ||
		string(got.Key) != "alpha" || string(got.Value) != "value-bytes" {
		t.Fatalf("round trip mismatch: %+v padded=%d", got, padded)
	}
	// Wrong expected sequence is a monotonic-order violation.
	if _, _, err := DecodeRecoveryRecord(buf, RecoveryJournalMinSectorSize, 6); !errors.Is(err, ErrRecoveryJournalRecord) {
		t.Fatalf("expected sequence mismatch rejected, got %v", err)
	}
	// A single flipped body bit fails the CRC.
	corrupt := append([]byte(nil), buf...)
	corrupt[RecoveryJournalRecordPrefixSize+1] ^= 0x01
	if _, _, err := DecodeRecoveryRecord(corrupt, RecoveryJournalMinSectorSize, 5); !errors.Is(err, ErrRecoveryJournalRecord) {
		t.Fatalf("corrupt body accepted, got %v", err)
	}
}

func TestRecoveryJournalAppendReopenReplay(t *testing.T) {
	rj, path := createTestJournal(t, 64*RecoveryJournalMinSectorSize)
	for gen := uint64(2); gen <= 6; gen++ {
		appendPut(t, rj, gen, "k", "v")
	}
	if err := rj.file.Sync(); err != nil {
		t.Fatalf("fsync: %v", err)
	}
	rj.Close()

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	if err := rj.Pair(rj.header.StoreID, rj.header.JournalID); err != nil {
		t.Fatalf("pair: %v", err)
	}
	// scanTail must position the cursor and next sequence exactly past the five
	// durable records.
	if rj.NextSequence() != 6 {
		t.Fatalf("next sequence = %d, want 6", rj.NextSequence())
	}
	all := replayAll(t, rj, 1)
	if len(all) != 5 {
		t.Fatalf("replay yielded %d records, want 5", len(all))
	}
	for i, rec := range all {
		if rec.Generation != uint64(i+2) || rec.Sequence != uint64(i+1) {
			t.Fatalf("record %d: gen=%d seq=%d", i, rec.Generation, rec.Sequence)
		}
	}
	// The generation filter skips records already folded into a newer root.
	filtered := replayAll(t, rj, 4)
	if len(filtered) != 2 {
		t.Fatalf("filtered replay yielded %d, want 2 (gens 5,6)", len(filtered))
	}
	if filtered[0].Generation != 5 || filtered[1].Generation != 6 {
		t.Fatalf("filtered gens = %d,%d", filtered[0].Generation, filtered[1].Generation)
	}
}

func TestRecoveryJournalTornTailTruncates(t *testing.T) {
	rj, path := createTestJournal(t, 64*RecoveryJournalMinSectorSize)
	fj := NewFaultJournal(rj)
	// Three clean records, then a torn fourth.
	appendPut(t, rj, 2, "k", "v")
	appendPut(t, rj, 3, "k", "v")
	appendPut(t, rj, 4, "k", "v")
	fj.Program(JournalFaultPlan{Phase: JournalFaultTornAppend, AppendIndex: 3})
	// The manager believes the torn append succeeded (a write the OS reported
	// complete before power loss). Recovery must discover it torn.
	if _, err := rj.Append(recoveryRecordKindPut, 5, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("torn append returned error: %v", err)
	}
	if !fj.Faulted() {
		t.Fatal("torn append fault did not fire")
	}
	rj.Close()

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	all := replayAll(t, rj, 1)
	if len(all) != 3 {
		t.Fatalf("torn tail replay yielded %d, want 3", len(all))
	}
	// The re-derived cursor must sit on the torn record so the next append
	// overwrites it.
	if rj.NextSequence() != 4 {
		t.Fatalf("next sequence after torn tail = %d, want 4", rj.NextSequence())
	}
}

func TestRecoveryJournalReorderHoleStopsReplay(t *testing.T) {
	rj, path := createTestJournal(t, 64*RecoveryJournalMinSectorSize)
	fj := NewFaultJournal(rj)
	appendPut(t, rj, 2, "k", "v")
	appendPut(t, rj, 3, "k", "v")
	// Drop the third record's write, then land two later ones. A device that
	// reordered writes could leave exactly this: later records present on disk,
	// an earlier one missing.
	fj.Program(JournalFaultPlan{Phase: JournalFaultDropAppend, AppendIndex: 2})
	if _, err := rj.Append(recoveryRecordKindPut, 4, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("dropped append error: %v", err)
	}
	fj.Program(JournalFaultPlan{Phase: JournalFaultNone})
	appendPut(t, rj, 5, "k", "v")
	appendPut(t, rj, 6, "k", "v")
	rj.Close()

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	all := replayAll(t, rj, 1)
	if len(all) != 2 {
		t.Fatalf("reorder hole replay yielded %d, want 2 (records before the gap)", len(all))
	}
	if all[1].Generation != 3 {
		t.Fatalf("last replayed generation = %d, want 3", all[1].Generation)
	}
}

func TestRecoveryJournalFullForcesRecycle(t *testing.T) {
	// Small capacity so a handful of records exhaust it.
	rj, path := createTestJournal(t, 4*RecoveryJournalMinSectorSize)
	gen := uint64(2)
	appended := 0
	for {
		if !rj.Fits(1, 1) {
			break
		}
		_, err := rj.Append(recoveryRecordKindPut, gen, []byte("k"), []byte("v"))
		if errors.Is(err, ErrRecoveryJournalFull) {
			break
		}
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := rj.Sync(false); err != nil {
			t.Fatalf("sync: %v", err)
		}
		gen++
		appended++
	}
	if appended == 0 {
		t.Fatal("capacity too small to append any record")
	}
	// Full journal returns ErrRecoveryJournalFull rather than extending the file.
	if _, err := rj.Append(recoveryRecordKindPut, gen, []byte("k"), []byte("v")); !errors.Is(err, ErrRecoveryJournalFull) {
		t.Fatalf("full journal append = %v, want ErrRecoveryJournalFull", err)
	}
	// Recycle past the checkpointed generation and resume appending.
	checkpointGen := gen - 1
	if err := rj.Recycle(checkpointGen); err != nil {
		t.Fatalf("recycle: %v", err)
	}
	if rj.Cursor() != 0 {
		t.Fatalf("cursor not reset after recycle: %d", rj.Cursor())
	}
	appendPut(t, rj, gen, "k2", "postrecycle")
	rj.Close()

	// After recycle the stale pre-recycle records must be invisible: the new
	// base sequence anchor rejects them and the base generation filter skips
	// any that somehow validated.
	rj = reopenTestJournal(t, path)
	defer rj.Close()
	if rj.BaseGeneration() != checkpointGen {
		t.Fatalf("recovered base generation = %d, want %d", rj.BaseGeneration(), checkpointGen)
	}
	all := replayAll(t, rj, rj.BaseGeneration())
	if len(all) != 1 || string(all[0].Value) != "postrecycle" {
		t.Fatalf("post-recycle replay = %+v, want exactly the post-recycle record", all)
	}
}

func TestRecoveryJournalRecycleCrashFallsBack(t *testing.T) {
	rj, path := createTestJournal(t, 64*RecoveryJournalMinSectorSize)
	fj := NewFaultJournal(rj)
	appendPut(t, rj, 2, "k", "v")
	appendPut(t, rj, 3, "k", "v")
	// Crash mid-recycle: the opposite header slot is half written. The live slot
	// (base generation 1) survives, and recovery re-applies its records onto the
	// newer root idempotently.
	fj.Program(JournalFaultPlan{Phase: JournalFaultTornRecycle})
	if err := rj.Recycle(3); err == nil {
		t.Fatal("torn recycle returned nil error")
	}
	if !fj.Faulted() {
		t.Fatal("torn recycle fault did not fire")
	}
	rj.Close()

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	if rj.BaseGeneration() != 1 {
		t.Fatalf("after torn recycle base generation = %d, want 1 (fell back)", rj.BaseGeneration())
	}
	all := replayAll(t, rj, 1)
	if len(all) != 2 {
		t.Fatalf("fallback replay yielded %d, want 2", len(all))
	}
}

func TestRecoveryJournalIdentityPairing(t *testing.T) {
	rj, path := createTestJournal(t, 8*RecoveryJournalMinSectorSize)
	rj.Close()
	rj = reopenTestJournal(t, path)
	defer rj.Close()
	good := rj.header
	if err := rj.Pair(good.StoreID, good.JournalID); err != nil {
		t.Fatalf("matching identity rejected: %v", err)
	}
	var wrong [16]byte
	wrong[0] = 0xaa
	if err := rj.Pair(wrong, good.JournalID); !errors.Is(err, ErrRecoveryJournalIdentity) {
		t.Fatalf("wrong store id = %v, want identity mismatch", err)
	}
	if err := rj.Pair(good.StoreID, wrong); !errors.Is(err, ErrRecoveryJournalIdentity) {
		t.Fatalf("wrong journal id = %v, want identity mismatch", err)
	}
}

func TestRecoveryJournalENOSPCAppend(t *testing.T) {
	rj, _ := createTestJournal(t, 64*RecoveryJournalMinSectorSize)
	defer rj.Close()
	fj := NewFaultJournal(rj)
	fj.Program(JournalFaultPlan{Phase: JournalFaultENOSPCAppend, AppendIndex: 0})
	beforeCursor := rj.Cursor()
	beforeSeq := rj.NextSequence()
	_, err := rj.Append(recoveryRecordKindPut, 2, []byte("k"), []byte("v"))
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("append error = %v, want ENOSPC", err)
	}
	if rj.Cursor() != beforeCursor || rj.NextSequence() != beforeSeq {
		t.Fatal("failed append advanced cursor or sequence")
	}
}

func TestRecoveryJournalENOSPCPreallocation(t *testing.T) {
	saved := recoveryJournalPreallocate
	recoveryJournalPreallocate = func(*os.File, int64) error { return syscall.ENOSPC }
	defer func() { recoveryJournalPreallocate = saved }()
	path := filepath.Join(t.TempDir(), "store.rjournal")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer file.Close()
	if _, err := CreateRecoveryJournal(file, testJournalHeader(t, 8*RecoveryJournalMinSectorSize)); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("create error = %v, want ENOSPC", err)
	}
}

func TestRecoveryJournalRecoveryTimeBound(t *testing.T) {
	// A full journal must replay in a bounded number of records: capacity over
	// the minimum padded record size. This is the recovery-time ceiling.
	capacity := uint64(128 * RecoveryJournalMinSectorSize)
	rj, path := createTestJournal(t, capacity)
	gen := uint64(2)
	for rj.Fits(1, 0) {
		if _, err := rj.Append(recoveryRecordKindPut, gen, []byte("k"), nil); err != nil {
			t.Fatalf("append: %v", err)
		}
		gen++
	}
	if err := rj.Sync(true); err != nil {
		t.Fatalf("sync: %v", err)
	}
	rj.Close()

	minPadded := uint64(recoveryRecordPadded(RecoveryJournalMinSectorSize, 1, 0))
	bound := capacity / minPadded
	rj = reopenTestJournal(t, path)
	defer rj.Close()
	all := replayAll(t, rj, 1)
	if uint64(len(all)) > bound {
		t.Fatalf("replayed %d records, exceeds capacity bound %d", len(all), bound)
	}
	if uint64(len(all)) != bound {
		t.Fatalf("filled journal replayed %d records, want the full %d", len(all), bound)
	}
}
