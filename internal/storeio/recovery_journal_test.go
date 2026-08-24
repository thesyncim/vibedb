package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
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
		Format:         RecoveryJournalFormat,
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
	header := testJournalHeader(t, capacity)
	rj, err := CreateRecoveryJournal(file, header)
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

func TestRecoveryJournalEmptyScanAndReplayDoNotAllocate(t *testing.T) {
	rj, _ := createTestJournal(t, 2<<20)
	defer rj.Close()
	var scanErr error
	if allocs := testing.AllocsPerRun(10, func() {
		scanErr = rj.scanTail()
	}); allocs != 0 {
		t.Fatalf("empty tail scan allocations = %.1f, want 0", allocs)
	}
	if scanErr != nil {
		t.Fatal(scanErr)
	}
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
		copied := RecoveryRecord{
			Sequence:   rec.Sequence,
			Generation: rec.Generation,
			Kind:       rec.Kind,
			Key:        append([]byte(nil), rec.Key...),
			Value:      append([]byte(nil), rec.Value...),
		}
		for i := range rec.Entries {
			copied.Entries = append(copied.Entries, RecoveryBatchEntry{
				Kind:  rec.Entries[i].Kind,
				Key:   append([]byte(nil), rec.Entries[i].Key...),
				Value: append([]byte(nil), rec.Entries[i].Value...),
			})
		}
		out = append(out, copied)
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

func TestRecoveryJournalCurrentGrammarRequiresExactSector(t *testing.T) {
	header := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	header.SectorSize = 2 * RecoveryJournalMinSectorSize
	if _, err := EncodeRecoveryJournalHeader(
		make([]byte, RecoveryJournalHeaderSize), header,
	); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("encode noncurrent sector = %v, want corruption", err)
	}
}

func TestRecoveryJournalCurrentFormatAndNonCurrentDomainRejection(t *testing.T) {
	header := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	header.RecycleCount = 3
	encoded := make([]byte, RecoveryJournalHeaderSize)
	if _, err := EncodeRecoveryJournalHeader(encoded, header); err != nil {
		t.Fatalf("EncodeRecoveryJournalHeader: %v", err)
	}
	if got := binary.LittleEndian.Uint32(encoded[8:12]); got != RecoveryJournalFormat {
		t.Fatalf("wire format = %d, want %d", got, RecoveryJournalFormat)
	}
	decoded, err := DecodeRecoveryJournalHeader(encoded)
	if err != nil {
		t.Fatalf("DecodeRecoveryJournalHeader: %v", err)
	}
	if decoded != header {
		t.Fatalf("decoded header = %+v, want %+v", decoded, header)
	}

	unsupported := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	unsupported.Format = RecoveryJournalFormat + 1
	unsupported.RecycleCount = 1
	if _, err := EncodeRecoveryJournalHeader(
		encoded, unsupported,
	); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("encode unsupported format = %v, want corrupt", err)
	}
	// Build an otherwise-valid checksummed unsupported header. Decode must
	// reject the format word itself, not merely its CRC.
	if _, err := EncodeRecoveryJournalHeader(encoded, header); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(encoded[8:12], unsupported.Format)
	checksum := PageChecksum(encoded[:RecoveryJournalHeaderSize-8])
	binary.LittleEndian.PutUint32(
		encoded[RecoveryJournalHeaderSize-8:RecoveryJournalHeaderSize-4], checksum,
	)
	binary.LittleEndian.PutUint32(encoded[RecoveryJournalHeaderSize-4:], ^checksum)
	if _, err := DecodeRecoveryJournalHeader(
		encoded,
	); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("decode checksummed unsupported format = %v, want corrupt", err)
	}

	nonCurrentDomain := append([]byte(nil), encoded...)
	copy(nonCurrentDomain[:8], "NOTJRNL!")
	checksum = PageChecksum(nonCurrentDomain[:RecoveryJournalHeaderSize-8])
	binary.LittleEndian.PutUint32(
		nonCurrentDomain[RecoveryJournalHeaderSize-8:RecoveryJournalHeaderSize-4], checksum,
	)
	binary.LittleEndian.PutUint32(nonCurrentDomain[RecoveryJournalHeaderSize-4:], ^checksum)
	if _, err := DecodeRecoveryJournalHeader(nonCurrentDomain); !errors.Is(
		err, ErrRecoveryJournalCorrupt,
	) {
		t.Fatalf("decode non-current header domain = %v, want corrupt", err)
	}

	rj, path := createTestJournal(t, 16*RecoveryJournalMinSectorSize)
	if err := rj.Recycle(2, false); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if rj.Header().Format != RecoveryJournalFormat {
		t.Fatalf("recycled format = %d, want current", rj.Header().Format)
	}
	if err := rj.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rj = reopenTestJournal(t, path)
	defer rj.Close()
	if rj.Header().Format != RecoveryJournalFormat {
		t.Fatalf("reopened format = %d, want current", rj.Header().Format)
	}
}

func TestRecoveryJournalRejectsHostileCapacityBeforeAllocation(t *testing.T) {
	h := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	h.RecycleCount = 1
	buf := make([]byte, RecoveryJournalHeaderSize)
	if _, err := EncodeRecoveryJournalHeader(buf, h); err != nil {
		t.Fatal(err)
	}

	oversized := RecoveryJournalMaxCapacityBytes +
		uint64(RecoveryJournalMinSectorSize)
	binary.LittleEndian.PutUint64(buf[72:80], oversized)
	checksum := PageChecksum(buf[:RecoveryJournalHeaderSize-8])
	binary.LittleEndian.PutUint32(
		buf[RecoveryJournalHeaderSize-8:RecoveryJournalHeaderSize-4],
		checksum,
	)
	binary.LittleEndian.PutUint32(
		buf[RecoveryJournalHeaderSize-4:], ^checksum,
	)
	if _, err := DecodeRecoveryJournalHeader(buf); !errors.Is(
		err, ErrRecoveryJournalCorrupt,
	) {
		t.Fatalf("oversized checksummed header = %v, want corrupt", err)
	}

	path := filepath.Join(t.TempDir(), "invalid.rjournal")
	file, err := os.OpenFile(
		path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for name, mutate := range map[string]func(*RecoveryJournalHeader){
		"zero sector": func(h *RecoveryJournalHeader) {
			h.SectorSize = 0
		},
		"oversized capacity": func(h *RecoveryJournalHeader) {
			h.Capacity = oversized
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := testJournalHeader(
				t, 8*RecoveryJournalMinSectorSize,
			)
			mutate(&candidate)
			if _, err := CreateRecoveryJournal(
				file, candidate,
			); !errors.Is(err, ErrRecoveryJournalCorrupt) {
				t.Fatalf("CreateRecoveryJournal = %v, want corrupt", err)
			}
		})
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

func TestRecoveryRecordNonCurrentDomainCannotDecodeAsCurrent(t *testing.T) {
	rec := RecoveryRecord{
		Sequence: 1, Generation: 2, Kind: RecoveryRecordKindPut,
		Key: []byte("key"), Value: []byte("value"),
	}
	buf := make([]byte, RecoveryJournalMinSectorSize)
	n, err := EncodeRecoveryRecord(buf, RecoveryJournalMinSectorSize, rec)
	if err != nil {
		t.Fatalf("EncodeRecoveryRecord: %v", err)
	}
	// Replace the authenticated record domain with an arbitrary non-current
	// value and reseal the record. It is hard semantic corruption, never a
	// truncatable current tail.
	binary.LittleEndian.PutUint32(buf[:4], 0x214f4e21) // "!NO!"
	bodyEnd := RecoveryJournalRecordPrefixSize + len(rec.Key) + len(rec.Value)
	checksum := PageChecksum(buf[:bodyEnd])
	binary.LittleEndian.PutUint32(buf[bodyEnd:bodyEnd+4], checksum)
	binary.LittleEndian.PutUint32(buf[bodyEnd+4:bodyEnd+8], ^checksum)
	if _, _, err := DecodeRecoveryRecord(
		buf[:n], RecoveryJournalMinSectorSize, 1,
	); !errors.Is(err, ErrRecoveryJournalRecord) ||
		errors.Is(err, errRecoveryJournalTruncatableTail) {
		t.Fatalf("DecodeRecoveryRecord non-current domain = %v, want hard rejection", err)
	}
}

func TestRecoveryJournalSemanticDamageIsHardForScanAndReplay(t *testing.T) {
	t.Run("unknown batch entry kind", func(t *testing.T) {
		entry := RecoveryBatchEntry{
			Kind: RecoveryRecordKindPut, Key: []byte("account:9"), Value: []byte("17"),
		}
		rj, path := createTestJournal(t, 8<<10)
		if _, err := rj.AppendDeltaBatch(2, []RecoveryBatchEntry{entry}); err != nil {
			t.Fatalf("AppendDeltaBatch: %v", err)
		}
		if err := rj.Sync(false); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		header := rj.Header()
		if err := rj.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("open for corruption: %v", err)
		}
		record := make([]byte, RecoveryJournalMinSectorSize)
		if _, err := file.ReadAt(record, recoveryJournalRegionStart); err != nil {
			t.Fatalf("read record: %v", err)
		}
		bodyEnd := RecoveryJournalRecordPrefixSize +
			int(binary.LittleEndian.Uint32(record[28:32]))
		entryAt := RecoveryJournalRecordPrefixSize
		binary.LittleEndian.PutUint16(record[entryAt:entryAt+2], 0x7777)
		checksum := PageChecksum(record[:bodyEnd])
		binary.LittleEndian.PutUint32(record[bodyEnd:bodyEnd+4], checksum)
		binary.LittleEndian.PutUint32(record[bodyEnd+4:bodyEnd+8], ^checksum)
		if _, err := file.WriteAt(record, recoveryJournalRegionStart); err != nil {
			t.Fatalf("write semantic corruption: %v", err)
		}

		if _, err := OpenRecoveryJournal(file); !errors.Is(
			err, ErrRecoveryJournalRecord,
		) || errors.Is(err, errRecoveryJournalTruncatableTail) {
			t.Fatalf("Open semantic damage = %v, want hard record error", err)
		}
		manager := newRecoveryJournalManager(file, header)
		if err := manager.Replay(1, func(RecoveryRecord) error {
			t.Fatal("semantic damage reached replay callback")
			return nil
		}); !errors.Is(err, ErrRecoveryJournalRecord) ||
			errors.Is(err, errRecoveryJournalTruncatableTail) {
			t.Fatalf("Replay semantic damage = %v, want hard record error", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close corrupt journal: %v", err)
		}
	})
}

func TestRecoveryJournalChecksumValidUnknownRecordKindIsHard(t *testing.T) {
	rj, path := createTestJournal(t, 8<<10)
	if _, err := rj.Append(
		RecoveryRecordKindPut, 2, []byte("key"), []byte("value"),
	); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := rj.Sync(false); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	header := rj.Header()
	if err := rj.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	record := make([]byte, RecoveryJournalMinSectorSize)
	if _, err := file.ReadAt(record, recoveryJournalRegionStart); err != nil {
		t.Fatalf("read record: %v", err)
	}
	binary.LittleEndian.PutUint16(record[4:6], 0x7777)
	bodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(record[24:28])) +
		int(binary.LittleEndian.Uint32(record[28:32]))
	checksum := PageChecksum(record[:bodyEnd])
	binary.LittleEndian.PutUint32(record[bodyEnd:bodyEnd+4], checksum)
	binary.LittleEndian.PutUint32(record[bodyEnd+4:bodyEnd+8], ^checksum)
	if _, err := file.WriteAt(record, recoveryJournalRegionStart); err != nil {
		t.Fatalf("write unknown kind: %v", err)
	}

	if _, err := OpenRecoveryJournal(file); !errors.Is(
		err, ErrRecoveryJournalRecord,
	) || errors.Is(err, errRecoveryJournalTruncatableTail) {
		t.Fatalf("Open unknown kind = %v, want hard record error", err)
	}
	manager := newRecoveryJournalManager(file, header)
	if err := manager.Replay(1, func(RecoveryRecord) error {
		t.Fatal("unknown record kind reached replay callback")
		return nil
	}); !errors.Is(err, ErrRecoveryJournalRecord) ||
		errors.Is(err, errRecoveryJournalTruncatableTail) {
		t.Fatalf("Replay unknown kind = %v, want hard record error", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupt journal: %v", err)
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
	if err := rj.Pair(rj.header.StoreID, rj.header.JournalID, rj.header.PageSize, rj.header.BaseGeneration); err != nil {
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
	// A short,nil WriteAt is still a failed append. The manager must report it
	// and leave its logical cursor on the torn bytes for recovery to truncate.
	if _, err := rj.Append(
		recoveryRecordKindPut, 5, []byte("k"), []byte("v"),
	); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("torn append = %v, want io.ErrShortWrite", err)
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
	if err := rj.Recycle(checkpointGen, true); err != nil {
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

func TestRecoveryJournalGrowCapacityPreservesLivePrefix(t *testing.T) {
	const initial = uint64(2 * RecoveryJournalMinSectorSize)
	rj, path := createTestJournal(t, initial)
	appendPut(t, rj, 2, "a", "before")
	beforeCursor := rj.Cursor()
	beforeSequence := rj.NextSequence()
	beforeCount := rj.Header().RecycleCount

	const grown = uint64(5 * RecoveryJournalMinSectorSize)
	if err := rj.GrowCapacity(grown, true); err != nil {
		t.Fatalf("GrowCapacity: %v", err)
	}
	if got := rj.Header().Capacity; got != grown {
		t.Fatalf("grown capacity = %d, want %d", got, grown)
	}
	if rj.Cursor() != beforeCursor || rj.NextSequence() != beforeSequence ||
		rj.Header().RecycleCount != beforeCount+1 || rj.headerSlot != 1 {
		t.Fatalf(
			"growth changed live state: cursor=%d sequence=%d count=%d slot=%d",
			rj.Cursor(), rj.NextSequence(), rj.Header().RecycleCount,
			rj.headerSlot,
		)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(recoveryJournalRegionStart) + int64(grown); info.Size() != want {
		t.Fatalf("grown file size = %d, want %d", info.Size(), want)
	}
	appendPut(t, rj, 3, "b", string(make([]byte, 1_200)))
	if err := rj.Close(); err != nil {
		t.Fatal(err)
	}

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	if rj.Header().Capacity != grown || rj.Cursor() <= beforeCursor {
		t.Fatalf("reopened grown state = capacity %d cursor %d",
			rj.Header().Capacity, rj.Cursor())
	}
	replayed := replayAll(t, rj, 1)
	if len(replayed) != 2 || string(replayed[0].Key) != "a" ||
		string(replayed[1].Key) != "b" || len(replayed[1].Value) != 1_200 {
		t.Fatalf("replay after growth = %+v", replayed)
	}
}

func TestRecoveryJournalGrowCapacityRecycleCountExhaustionIsSideEffectFree(t *testing.T) {
	const initial = uint64(2 * RecoveryJournalMinSectorSize)
	rj, path := createTestJournal(t, initial)
	defer rj.Close()
	appendPut(t, rj, 2, "a", "before")
	rj.header.RecycleCount = ^uint64(0)

	beforeBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeHeader := rj.Header()
	beforeCursor := rj.Cursor()
	beforeSequence := rj.NextSequence()
	beforeSlot := rj.headerSlot
	writes := 0
	syncs := 0
	originalWriteAt := rj.writeAt
	originalSync := rj.journalSync
	originalDataSync := rj.journalDataSync
	rj.writeAt = func(p []byte, off int64) (int, error) {
		writes++
		return originalWriteAt(p, off)
	}
	rj.journalSync = func(file *os.File) error {
		syncs++
		return originalSync(file)
	}
	rj.journalDataSync = func(file *os.File) error {
		syncs++
		return originalDataSync(file)
	}

	err = rj.GrowCapacity(4*RecoveryJournalMinSectorSize, true)
	if !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("terminal GrowCapacity = %v, want ErrInvalidWrite", err)
	}
	afterBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if writes != 0 || syncs != 0 {
		t.Fatalf("terminal GrowCapacity issued writes=%d syncs=%d", writes, syncs)
	}
	if afterInfo.Size() != beforeInfo.Size() || !bytes.Equal(afterBytes, beforeBytes) {
		t.Fatalf(
			"terminal GrowCapacity changed file: size %d -> %d, bytesEqual=%t",
			beforeInfo.Size(), afterInfo.Size(), bytes.Equal(afterBytes, beforeBytes),
		)
	}
	if rj.Header() != beforeHeader || rj.Cursor() != beforeCursor ||
		rj.NextSequence() != beforeSequence || rj.headerSlot != beforeSlot {
		t.Fatalf(
			"terminal GrowCapacity changed manager: header=%+v cursor=%d sequence=%d slot=%d",
			rj.Header(), rj.Cursor(), rj.NextSequence(), rj.headerSlot,
		)
	}
}

func TestRecoveryJournalGrowCapacityCrashFallsBack(t *testing.T) {
	const initial = uint64(4 * RecoveryJournalMinSectorSize)
	rj, path := createTestJournal(t, initial)
	appendPut(t, rj, 2, "k", "v")
	beforeCursor := rj.Cursor()
	beforeSequence := rj.NextSequence()
	beforeCount := rj.Header().RecycleCount
	fault := NewFaultJournal(rj)
	fault.Program(JournalFaultPlan{Phase: JournalFaultTornRecycle})
	if err := rj.GrowCapacity(8*RecoveryJournalMinSectorSize, true); err == nil {
		t.Fatal("torn growth returned nil error")
	}
	if !fault.Faulted() {
		t.Fatal("growth header fault did not fire")
	}
	if rj.Header().Capacity != initial || rj.Cursor() != beforeCursor ||
		rj.NextSequence() != beforeSequence ||
		rj.Header().RecycleCount != beforeCount || rj.headerSlot != 0 {
		t.Fatalf(
			"failed growth changed manager: capacity=%d cursor=%d sequence=%d count=%d slot=%d",
			rj.Header().Capacity, rj.Cursor(), rj.NextSequence(),
			rj.Header().RecycleCount, rj.headerSlot,
		)
	}
	if err := rj.Close(); err != nil {
		t.Fatal(err)
	}

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	if rj.Header().Capacity != initial {
		t.Fatalf("fallback capacity = %d, want %d",
			rj.Header().Capacity, initial)
	}
	replayed := replayAll(t, rj, 1)
	if len(replayed) != 1 || string(replayed[0].Key) != "k" ||
		string(replayed[0].Value) != "v" {
		t.Fatalf("fallback replay = %+v", replayed)
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
	if err := rj.Recycle(3, true); err == nil {
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

func TestRecoveryJournalRecycleRequiresLiveGenerationCoverage(t *testing.T) {
	rj, _ := createTestJournal(t, 8*RecoveryJournalMinSectorSize)
	defer rj.Close()
	appendPut(t, rj, 2, "k", "v")

	beforeCursor := rj.Cursor()
	beforeCount := rj.Header().RecycleCount
	if got := rj.LiveEndGeneration(); got != 2 {
		t.Fatalf("live end = %d, want 2", got)
	}
	if err := rj.Recycle(1, false); !errors.Is(err, ErrGenerationOrder) {
		t.Fatalf("Recycle below live end = %v, want %v", err, ErrGenerationOrder)
	}
	if rj.BaseGeneration() != 1 || rj.Cursor() != beforeCursor ||
		rj.Header().RecycleCount != beforeCount {
		t.Fatalf(
			"refused recycle changed state: base=%d cursor=%d count=%d",
			rj.BaseGeneration(), rj.Cursor(), rj.Header().RecycleCount,
		)
	}
	if err := rj.Recycle(2, false); err != nil {
		t.Fatalf("Recycle through live end: %v", err)
	}
	if rj.BaseGeneration() != 2 || rj.Cursor() != 0 ||
		rj.LiveEndGeneration() != 2 {
		t.Fatalf(
			"covered recycle state: base=%d cursor=%d live-end=%d",
			rj.BaseGeneration(), rj.Cursor(), rj.LiveEndGeneration(),
		)
	}
}

func TestRecoveryJournalExactRecycleNoOpAndTerminalRefusalAreSideEffectFree(t *testing.T) {
	rj, path := createTestJournal(t, 8*RecoveryJournalMinSectorSize)
	defer rj.Close()
	fault := NewFaultJournal(rj)
	beforeBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeHeader := rj.Header()
	beforeCursor := rj.Cursor()
	beforeSequence := rj.NextSequence()
	beforeSlot := rj.headerSlot
	if err := rj.Recycle(rj.BaseGeneration(), true); err != nil {
		t.Fatalf("exact clean recycle: %v", err)
	}
	if fault.Syncs() != 0 {
		t.Fatalf("exact clean recycle issued %d syncs", fault.Syncs())
	}
	afterBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterBytes, beforeBytes) || rj.Header() != beforeHeader ||
		rj.Cursor() != beforeCursor || rj.NextSequence() != beforeSequence ||
		rj.headerSlot != beforeSlot {
		t.Fatalf(
			"exact clean recycle changed state: header=%+v cursor=%d sequence=%d slot=%d bytes=%t",
			rj.Header(), rj.Cursor(), rj.NextSequence(), rj.headerSlot,
			bytes.Equal(afterBytes, beforeBytes),
		)
	}

	// Exhaustion does not invalidate the already-exact target header. Advancing
	// its base still requires a successor and must fail before any file action.
	rj.header.RecycleCount = math.MaxUint64
	terminalHeader := rj.Header()
	if err := rj.Recycle(rj.BaseGeneration(), true); err != nil {
		t.Fatalf("terminal exact clean recycle: %v", err)
	}
	if err := rj.Recycle(rj.BaseGeneration()+1, true); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("terminal advancing recycle = %v, want ErrInvalidWrite", err)
	}
	afterBytes, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if fault.Syncs() != 0 || !bytes.Equal(afterBytes, beforeBytes) ||
		rj.Header() != terminalHeader || rj.Cursor() != beforeCursor ||
		rj.NextSequence() != beforeSequence || rj.headerSlot != beforeSlot {
		t.Fatalf(
			"terminal recycle changed state: syncs=%d header=%+v cursor=%d sequence=%d slot=%d bytes=%t",
			fault.Syncs(), rj.Header(), rj.Cursor(), rj.NextSequence(),
			rj.headerSlot, bytes.Equal(afterBytes, beforeBytes),
		)
	}
}

func TestRecoveryJournalIdentityPairing(t *testing.T) {
	rj, path := createTestJournal(t, 8*RecoveryJournalMinSectorSize)
	rj.Close()
	rj = reopenTestJournal(t, path)
	defer rj.Close()
	good := rj.header
	if err := rj.Pair(good.StoreID, good.JournalID, good.PageSize, good.BaseGeneration); err != nil {
		t.Fatalf("matching identity rejected: %v", err)
	}
	var wrong [16]byte
	wrong[0] = 0xaa
	if err := rj.Pair(wrong, good.JournalID, good.PageSize, good.BaseGeneration); !errors.Is(err, ErrRecoveryJournalIdentity) {
		t.Fatalf("wrong store id = %v, want identity mismatch", err)
	}
	if err := rj.Pair(good.StoreID, wrong, good.PageSize, good.BaseGeneration); !errors.Is(err, ErrRecoveryJournalIdentity) {
		t.Fatalf("wrong journal id = %v, want identity mismatch", err)
	}
	if err := rj.Pair(
		good.StoreID, good.JournalID, good.PageSize*2, good.BaseGeneration,
	); !errors.Is(err, ErrRecoveryJournalGeometry) {
		t.Fatalf("wrong page size = %v, want geometry mismatch", err)
	}
	if good.BaseGeneration == 0 {
		// A journal ahead of the root that selected it is a mixed-epoch
		// bundle: the store half is older, and acknowledgements recycled in
		// the gap are unrecoverable, so the pair fails closed.
		if err := rj.Pair(
			good.StoreID, good.JournalID, good.PageSize, 0,
		); err != nil {
			t.Fatalf("equal-generation pair = %v, want nil", err)
		}
	}
	rj.header.BaseGeneration = 7
	if err := rj.Pair(
		good.StoreID, good.JournalID, good.PageSize, 3,
	); !errors.Is(err, ErrRecoveryJournalEpoch) {
		t.Fatalf("journal ahead of root = %v, want epoch mismatch", err)
	}
	rj.header.BaseGeneration = good.BaseGeneration
}

func TestRecoveryJournalRecycleSelectsSyncStrength(t *testing.T) {
	for _, tc := range []struct {
		name      string
		powerSafe bool
		wantSync  int
		wantData  int
	}{
		{name: "filesystem", wantSync: 1},
		{name: "power-safe", powerSafe: true, wantData: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rj, _ := createTestJournal(
				t, 8*RecoveryJournalMinSectorSize,
			)
			defer rj.Close()

			syncs, dataSyncs := 0, 0
			rj.journalSync = func(*os.File) error {
				syncs++
				return nil
			}
			rj.journalDataSync = func(*os.File) error {
				dataSyncs++
				return nil
			}
			if err := rj.Recycle(2, tc.powerSafe); err != nil {
				t.Fatalf("Recycle: %v", err)
			}
			if syncs != tc.wantSync || dataSyncs != tc.wantData {
				t.Fatalf(
					"sync calls filesystem/data = %d/%d, want %d/%d",
					syncs, dataSyncs, tc.wantSync, tc.wantData,
				)
			}
			if rj.BaseGeneration() != 2 || rj.Cursor() != 0 {
				t.Fatalf(
					"recycled base/cursor = %d/%d, want 2/0",
					rj.BaseGeneration(), rj.Cursor(),
				)
			}
		})
	}
}

// TestRecoveryJournalRecycleFailureLeavesManagerUnchanged pins the recycle
// commit discipline: the in-memory header, slot, and cursor advance only after
// the header write AND its sync both succeed, so a failed recycle leaves the
// manager describing exactly the on-disk state — same base guarding the
// regression check, same cursor appending after the live records — and a later
// clean recycle of the same generation completes normally.
func TestRecoveryJournalRecycleFailureLeavesManagerUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		plan JournalFaultPlan
	}{
		{"header-write", JournalFaultPlan{Phase: JournalFaultENOSPCRecycle}},
		// SyncIndex 2 skips the two appendPut barriers below and faults the
		// recycle's own sync, the write's fallible second half.
		{"header-sync", JournalFaultPlan{Phase: JournalFaultSyncError, SyncIndex: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rj, _ := createTestJournal(t, 64*RecoveryJournalMinSectorSize)
			defer rj.Close()
			fj := NewFaultJournal(rj)
			appendPut(t, rj, 2, "k", "v")
			appendPut(t, rj, 3, "k", "v")
			beforeCursor := rj.Cursor()
			beforeCount := rj.Header().RecycleCount

			fj.Program(tc.plan)
			if err := rj.Recycle(3, true); err == nil {
				t.Fatal("faulted recycle returned nil error")
			}
			if !fj.Faulted() {
				t.Fatal("programmed recycle fault did not fire")
			}
			if rj.BaseGeneration() != 1 || rj.Cursor() != beforeCursor ||
				rj.Header().RecycleCount != beforeCount || rj.headerSlot != 0 {
				t.Fatalf("failed recycle half-applied the header: base=%d cursor=%d count=%d slot=%d",
					rj.BaseGeneration(), rj.Cursor(), rj.Header().RecycleCount, rj.headerSlot)
			}

			// With the device healthy again the same recycle must complete whole.
			fj.Program(JournalFaultPlan{})
			if err := rj.Recycle(3, true); err != nil {
				t.Fatalf("clean recycle after failure: %v", err)
			}
			if rj.BaseGeneration() != 3 || rj.Cursor() != 0 ||
				rj.Header().RecycleCount != beforeCount+1 || rj.headerSlot != 1 {
				t.Fatalf("clean recycle did not commit: base=%d cursor=%d count=%d slot=%d",
					rj.BaseGeneration(), rj.Cursor(), rj.Header().RecycleCount, rj.headerSlot)
			}
		})
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
