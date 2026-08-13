package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
		FormatVersion:  RecoveryJournalFormatLegacy,
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
	return createTestJournalFormat(
		t, capacity, RecoveryJournalFormatLegacy,
	)
}

func createTestJournalFormat(
	t *testing.T, capacity uint64, formatVersion uint32,
) (*RecoveryJournal, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.rjournal")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open journal file: %v", err)
	}
	header := testJournalHeader(t, capacity)
	header.FormatVersion = formatVersion
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
				Kind:        rec.Entries[i].Kind,
				Key:         append([]byte(nil), rec.Entries[i].Key...),
				Value:       append([]byte(nil), rec.Entries[i].Value...),
				ScalarPatch: rec.Entries[i].ScalarPatch,
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

func TestRecoveryJournalHeaderFormatVersionRoundTripAndPreservation(t *testing.T) {
	for _, formatVersion := range []uint32{
		RecoveryJournalFormatLegacy,
		RecoveryJournalFormatScalarPatch,
	} {
		t.Run(fmt.Sprintf("format-%d", formatVersion), func(t *testing.T) {
			header := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
			header.FormatVersion = formatVersion
			header.RecycleCount = 3
			encoded := make([]byte, RecoveryJournalHeaderSize)
			if _, err := EncodeRecoveryJournalHeader(encoded, header); err != nil {
				t.Fatalf("EncodeRecoveryJournalHeader: %v", err)
			}
			if got := binary.LittleEndian.Uint32(encoded[8:12]); got != formatVersion {
				t.Fatalf("wire format version = %d, want %d", got, formatVersion)
			}
			decoded, err := DecodeRecoveryJournalHeader(encoded)
			if err != nil {
				t.Fatalf("DecodeRecoveryJournalHeader: %v", err)
			}
			if decoded != header {
				t.Fatalf("decoded header = %+v, want %+v", decoded, header)
			}
		})
	}

	unsupported := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	unsupported.FormatVersion = RecoveryJournalFormatConditional + 1
	unsupported.RecycleCount = 1
	encoded := make([]byte, RecoveryJournalHeaderSize)
	if _, err := EncodeRecoveryJournalHeader(
		encoded, unsupported,
	); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("encode unsupported version = %v, want corrupt", err)
	}
	// Build an otherwise-valid checksummed unsupported header from a legacy
	// image. Decode must reject the feature word itself, not merely its CRC.
	legacy := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	legacy.RecycleCount = 1
	if _, err := EncodeRecoveryJournalHeader(encoded, legacy); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(encoded[8:12], unsupported.FormatVersion)
	checksum := PageChecksum(encoded[:RecoveryJournalHeaderSize-8])
	binary.LittleEndian.PutUint32(
		encoded[RecoveryJournalHeaderSize-8:RecoveryJournalHeaderSize-4], checksum,
	)
	binary.LittleEndian.PutUint32(encoded[RecoveryJournalHeaderSize-4:], ^checksum)
	if _, err := DecodeRecoveryJournalHeader(
		encoded,
	); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("decode checksummed unsupported version = %v, want corrupt", err)
	}

	rj, path := createTestJournalFormat(
		t, 16*RecoveryJournalMinSectorSize,
		RecoveryJournalFormatScalarPatch,
	)
	if rj.Header().FormatVersion != RecoveryJournalFormatScalarPatch {
		t.Fatalf("created format = %d, want scalar", rj.Header().FormatVersion)
	}
	if err := rj.Recycle(2, false); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if rj.Header().FormatVersion != RecoveryJournalFormatScalarPatch {
		t.Fatalf("recycled format = %d, want scalar", rj.Header().FormatVersion)
	}
	if err := rj.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rj = reopenTestJournal(t, path)
	defer rj.Close()
	if rj.Header().FormatVersion != RecoveryJournalFormatScalarPatch {
		t.Fatalf("reopened format = %d, want scalar", rj.Header().FormatVersion)
	}
	if DevelopmentFormatVersion != RecoveryJournalFormatLegacy {
		t.Fatalf("legacy wire version = %d, old binary expected %d",
			RecoveryJournalFormatLegacy, DevelopmentFormatVersion)
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

func TestRecoveryScalarPatchBatchEntryRoundTrip(t *testing.T) {
	result := []byte(`{"id":1,"score":-7}`)
	metadata := RecoveryScalarPatchMetadata{
		CanonicalOffset:        16,
		OldScalarLength:        2,
		ExpectedResultChecksum: PageChecksum(result),
	}
	entries := []RecoveryBatchEntry{
		{Kind: RecoveryRecordKindPut, Key: []byte("before"), Value: []byte(`{"v":1}`)},
		{
			Kind:        RecoveryRecordKindScalarPatch,
			Key:         []byte("account:1"),
			Value:       []byte("-7"),
			ScalarPatch: metadata,
		},
		{Kind: RecoveryRecordKindDelete, Key: []byte("after")},
	}
	plan, ok := prepareRecoveryBatch(RecoveryJournalMinSectorSize, entries)
	if !ok {
		t.Fatal("prepare scalar-patch batch declined valid entries")
	}
	wantBody := uint64(
		3*RecoveryBatchEntryHeaderSize +
			RecoveryScalarPatchMetadataSize +
			len("before") + len(`{"v":1}`) +
			len("account:1") + len("-7") + len("after"),
	)
	if plan.bodyLen != wantBody {
		t.Fatalf("batch body length = %d, want %d", plan.bodyLen, wantBody)
	}

	buf := make([]byte, plan.PaddedSize())
	encoded, err := encodeRecoveryBatchRecordPrepared(
		buf, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence: 7, Generation: 11, Kind: RecoveryRecordKindBatch,
			Entries: entries,
		},
		plan,
	)
	if err != nil {
		t.Fatalf("encode scalar-patch batch: %v", err)
	}
	if encoded != plan.PaddedSize() {
		t.Fatalf("encoded bytes = %d, want %d", encoded, plan.PaddedSize())
	}
	got, padded, err := DecodeRecoveryRecord(
		buf, RecoveryJournalMinSectorSize, 7,
	)
	if err != nil {
		t.Fatalf("decode scalar-patch batch: %v", err)
	}
	if padded != encoded || got.Kind != RecoveryRecordKindBatch ||
		got.Generation != 11 || len(got.Entries) != len(entries) {
		t.Fatalf("decoded batch = %+v padded=%d, want 3-entry batch/%d",
			got, padded, encoded)
	}
	patch := got.Entries[1]
	if patch.Kind != RecoveryRecordKindScalarPatch ||
		string(patch.Key) != "account:1" || string(patch.Value) != "-7" ||
		patch.ScalarPatch != metadata {
		t.Fatalf("decoded scalar patch = %+v, want metadata %+v", patch, metadata)
	}

	// Pin the new entry's wire layout without changing the framing of existing
	// put/delete entries: fixed header, fixed metadata, then borrowed key/value.
	firstBytes := RecoveryBatchEntryHeaderSize + len("before") + len(`{"v":1}`)
	entryAt := RecoveryJournalRecordPrefixSize + firstBytes
	if binary.LittleEndian.Uint16(buf[entryAt:entryAt+2]) !=
		RecoveryRecordKindScalarPatch ||
		binary.LittleEndian.Uint16(buf[entryAt+2:entryAt+4]) != 0 ||
		binary.LittleEndian.Uint32(buf[entryAt+4:entryAt+8]) != uint32(len("account:1")) ||
		binary.LittleEndian.Uint32(buf[entryAt+8:entryAt+12]) != uint32(len("-7")) {
		t.Fatalf("scalar-patch entry header malformed: %x",
			buf[entryAt:entryAt+RecoveryBatchEntryHeaderSize])
	}
	metadataAt := entryAt + RecoveryBatchEntryHeaderSize
	if binary.LittleEndian.Uint16(buf[metadataAt:metadataAt+2]) !=
		metadata.CanonicalOffset ||
		buf[metadataAt+2] != metadata.OldScalarLength ||
		buf[metadataAt+3] != 0 ||
		binary.LittleEndian.Uint32(buf[metadataAt+4:metadataAt+8]) !=
			metadata.ExpectedResultChecksum {
		t.Fatalf("scalar-patch metadata wire bytes = %x, want %+v",
			buf[metadataAt:metadataAt+RecoveryScalarPatchMetadataSize], metadata)
	}
}

func TestRecoveryScalarPatchIsBatchEntryOnly(t *testing.T) {
	rec := RecoveryRecord{
		Sequence: 1, Generation: 2, Kind: RecoveryRecordKindScalarPatch,
		Key: []byte("k"), Value: []byte("1"),
	}
	buf := make([]byte, RecoveryJournalMinSectorSize)
	if _, err := EncodeRecoveryRecord(
		buf, RecoveryJournalMinSectorSize, rec,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("standalone scalar-patch encode = %v, want invalid write", err)
	}

	rec.Kind = RecoveryRecordKindPut
	n, err := EncodeRecoveryRecord(buf, RecoveryJournalMinSectorSize, rec)
	if err != nil {
		t.Fatalf("encode ordinary record: %v", err)
	}
	binary.LittleEndian.PutUint16(buf[4:6], RecoveryRecordKindScalarPatch)
	bodyEnd := RecoveryJournalRecordPrefixSize + len(rec.Key) + len(rec.Value)
	checksum := PageChecksum(buf[:bodyEnd])
	binary.LittleEndian.PutUint32(buf[bodyEnd:bodyEnd+4], checksum)
	binary.LittleEndian.PutUint32(buf[bodyEnd+4:bodyEnd+8], ^checksum)
	if _, _, err := DecodeRecoveryRecord(
		buf[:n], RecoveryJournalMinSectorSize, 1,
	); !errors.Is(err, ErrRecoveryJournalRecord) ||
		errors.Is(err, errRecoveryJournalTruncatableTail) {
		t.Fatalf("standalone scalar-patch decode = %v, want hard record error", err)
	}
}

func TestRecoveryScalarPatchRequiresHeaderFeatureVersion(t *testing.T) {
	entry := RecoveryBatchEntry{
		Kind:  RecoveryRecordKindScalarPatch,
		Key:   []byte("account:1"),
		Value: []byte("7"),
		ScalarPatch: RecoveryScalarPatchMetadata{
			CanonicalOffset:        12,
			OldScalarLength:        1,
			ExpectedResultChecksum: 0x10203040,
		},
	}
	entries := []RecoveryBatchEntry{entry}

	legacy, _ := createTestJournalFormat(
		t, 8<<10, RecoveryJournalFormatLegacy,
	)
	defer legacy.Close()
	beforeCursor, beforeSequence := legacy.Cursor(), legacy.NextSequence()
	if _, err := legacy.PrepareBatch(entries); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("legacy PrepareBatch = %v, want invalid write", err)
	}
	if _, err := legacy.AppendBatch(2, entries); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("legacy AppendBatch = %v, want invalid write", err)
	}
	foreignPlan, ok := prepareRecoveryBatch(
		RecoveryJournalMinSectorSize, entries,
	)
	if !ok {
		t.Fatal("scalar-format planner rejected valid entry")
	}
	if legacy.PreparedBatchFits(foreignPlan) {
		t.Fatal("legacy journal accepted scalar-format prepared plan")
	}
	if _, err := legacy.AppendPreparedBatch(
		2, entries, foreignPlan,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("legacy AppendPreparedBatch = %v, want invalid write", err)
	}
	if legacy.Cursor() != beforeCursor || legacy.NextSequence() != beforeSequence {
		t.Fatalf("legacy rejection advanced cursor/sequence: %d/%d -> %d/%d",
			beforeCursor, beforeSequence, legacy.Cursor(), legacy.NextSequence())
	}

	scalar, path := createTestJournalFormat(
		t, 8<<10, RecoveryJournalFormatScalarPatch,
	)
	if _, err := scalar.AppendBatch(2, entries); err != nil {
		t.Fatalf("scalar-format AppendBatch: %v", err)
	}
	if err := scalar.Sync(false); err != nil {
		t.Fatalf("scalar-format Sync: %v", err)
	}
	if err := scalar.Close(); err != nil {
		t.Fatalf("scalar-format Close: %v", err)
	}
	scalar = reopenTestJournal(t, path)
	defer scalar.Close()
	replayed := replayAll(t, scalar, 1)
	if len(replayed) != 1 || len(replayed[0].Entries) != 1 ||
		replayed[0].Entries[0].Kind != RecoveryRecordKindScalarPatch ||
		string(replayed[0].Entries[0].Value) != "7" ||
		replayed[0].Entries[0].ScalarPatch != entry.ScalarPatch {
		t.Fatalf("scalar-format replay = %+v, want one scalar patch", replayed)
	}
}

func TestRecoveryScalarPatchStrictValidation(t *testing.T) {
	valid := RecoveryBatchEntry{
		Kind:  RecoveryRecordKindScalarPatch,
		Key:   []byte("key"),
		Value: []byte("-999999999999999999"),
		ScalarPatch: RecoveryScalarPatchMetadata{
			CanonicalOffset:        ^uint16(0) - 18,
			OldScalarLength:        recoveryScalarPatchMaxCanonicalBytes,
			ExpectedResultChecksum: 0,
		},
	}
	if _, ok := prepareRecoveryBatch(
		RecoveryJournalMinSectorSize, []RecoveryBatchEntry{valid},
	); !ok {
		t.Fatal("valid boundary scalar patch was rejected")
	}

	tests := []struct {
		name   string
		mutate func(*RecoveryBatchEntry)
	}{
		{name: "zero old length", mutate: func(e *RecoveryBatchEntry) {
			e.ScalarPatch.OldScalarLength = 0
		}},
		{name: "old scalar too long", mutate: func(e *RecoveryBatchEntry) {
			e.ScalarPatch.OldScalarLength = recoveryScalarPatchMaxCanonicalBytes + 1
		}},
		{name: "removed spelling overruns window", mutate: func(e *RecoveryBatchEntry) {
			e.ScalarPatch.CanonicalOffset = ^uint16(0)
			e.ScalarPatch.OldScalarLength = 2
		}},
		{name: "new spelling overruns window", mutate: func(e *RecoveryBatchEntry) {
			e.ScalarPatch.CanonicalOffset = ^uint16(0)
			e.ScalarPatch.OldScalarLength = 1
			e.Value = []byte("10")
		}},
		{name: "empty new scalar", mutate: func(e *RecoveryBatchEntry) {
			e.Value = nil
		}},
		{name: "noncanonical integer", mutate: func(e *RecoveryBatchEntry) {
			e.Value = []byte("01")
		}},
		{name: "unsupported string scalar", mutate: func(e *RecoveryBatchEntry) {
			e.Value = []byte(`"x"`)
		}},
		{name: "metadata on put", mutate: func(e *RecoveryBatchEntry) {
			e.Kind = RecoveryRecordKindPut
			e.Value = []byte("value")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := valid
			tc.mutate(&entry)
			entries := []RecoveryBatchEntry{entry}
			if _, ok := prepareRecoveryBatch(
				RecoveryJournalMinSectorSize, entries,
			); ok {
				t.Fatal("invalid scalar patch prepared successfully")
			}
			if got := RecoveryBatchRecordPaddedSize(
				RecoveryJournalMinSectorSize, entries,
			); got != maxIntValue {
				t.Fatalf("invalid scalar patch padded size = %d, want max int", got)
			}
		})
	}
}

func TestRecoveryScalarPatchDecoderRejectsCorruptionAndBounds(t *testing.T) {
	entry := RecoveryBatchEntry{
		Kind:  RecoveryRecordKindScalarPatch,
		Key:   []byte("account:7"),
		Value: []byte("17"),
		ScalarPatch: RecoveryScalarPatchMetadata{
			CanonicalOffset:        12,
			OldScalarLength:        2,
			ExpectedResultChecksum: 0x12345678,
		},
	}
	entries := []RecoveryBatchEntry{entry}
	plan, ok := prepareRecoveryBatch(RecoveryJournalMinSectorSize, entries)
	if !ok {
		t.Fatal("prepare valid scalar patch")
	}
	encoded := make([]byte, plan.PaddedSize())
	if _, err := encodeRecoveryBatchRecordPrepared(
		encoded, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence: 3, Generation: 4, Kind: RecoveryRecordKindBatch,
			Entries: entries,
		},
		plan,
	); err != nil {
		t.Fatalf("encode: %v", err)
	}
	bodyEnd := RecoveryJournalRecordPrefixSize + int(plan.bodyLen)
	metadataAt := RecoveryJournalRecordPrefixSize + RecoveryBatchEntryHeaderSize
	valueAt := metadataAt + RecoveryScalarPatchMetadataSize + len(entry.Key)
	reseal := func(buf []byte) {
		checksum := PageChecksum(buf[:bodyEnd])
		binary.LittleEndian.PutUint32(buf[bodyEnd:bodyEnd+4], checksum)
		binary.LittleEndian.PutUint32(buf[bodyEnd+4:bodyEnd+8], ^checksum)
	}

	tests := []struct {
		name   string
		reseal bool
		mutate func([]byte)
	}{
		{name: "metadata checksum damage", mutate: func(buf []byte) {
			buf[metadataAt+4] ^= 1
		}},
		{name: "reserved metadata byte", reseal: true, mutate: func(buf []byte) {
			buf[metadataAt+3] = 1
		}},
		{name: "zero old length", reseal: true, mutate: func(buf []byte) {
			buf[metadataAt+2] = 0
		}},
		{name: "offset plus old length overflow", reseal: true, mutate: func(buf []byte) {
			binary.LittleEndian.PutUint16(buf[metadataAt:metadataAt+2], ^uint16(0))
			buf[metadataAt+2] = 2
		}},
		{name: "noncanonical new scalar", reseal: true, mutate: func(buf []byte) {
			copy(buf[valueAt:valueAt+2], "01")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := append([]byte(nil), encoded...)
			tc.mutate(corrupt)
			if tc.reseal {
				reseal(corrupt)
			}
			if _, _, err := DecodeRecoveryRecord(
				corrupt, RecoveryJournalMinSectorSize, 3,
			); !errors.Is(err, ErrRecoveryJournalRecord) {
				t.Fatalf("corrupt scalar patch decode = %v, want record error", err)
			}
		})
	}
}

func TestRecoveryScalarPatchPreparedPlanRevalidatesMetadata(t *testing.T) {
	rj, _ := createTestJournalFormat(
		t, 8<<10, RecoveryJournalFormatScalarPatch,
	)
	defer rj.Close()
	entries := []RecoveryBatchEntry{{
		Kind:  RecoveryRecordKindScalarPatch,
		Key:   []byte("key"),
		Value: []byte("1"),
		ScalarPatch: RecoveryScalarPatchMetadata{
			CanonicalOffset:        7,
			OldScalarLength:        1,
			ExpectedResultChecksum: 9,
		},
	}}
	plan, err := rj.PrepareBatch(entries)
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	entries[0].ScalarPatch.OldScalarLength = 0
	writes := 0
	rj.writeAt = func(p []byte, _ int64) (int, error) {
		writes++
		return len(p), nil
	}
	beforeCursor, beforeSequence := rj.Cursor(), rj.NextSequence()
	if _, err := rj.AppendPreparedBatch(
		2, entries, plan,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("AppendPreparedBatch with damaged metadata = %v, want invalid write", err)
	}
	if writes != 0 || rj.Cursor() != beforeCursor ||
		rj.NextSequence() != beforeSequence {
		t.Fatalf("rejected metadata wrote/advanced: writes=%d cursor=%d/%d sequence=%d/%d",
			writes, beforeCursor, rj.Cursor(), beforeSequence, rj.NextSequence())
	}
}

func TestRecoveryScalarPatchDowngradedHeaderFailsClosed(t *testing.T) {
	entry := RecoveryBatchEntry{
		Kind:  RecoveryRecordKindScalarPatch,
		Key:   []byte("key"),
		Value: []byte("1"),
		ScalarPatch: RecoveryScalarPatchMetadata{
			CanonicalOffset:        7,
			OldScalarLength:        1,
			ExpectedResultChecksum: 0xaabbccdd,
		},
	}
	rj, path := createTestJournalFormat(
		t, 8<<10, RecoveryJournalFormatScalarPatch,
	)
	if _, err := rj.AppendBatch(2, []RecoveryBatchEntry{entry}); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := rj.Sync(false); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := rj.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for downgrade: %v", err)
	}
	headerBytes := make([]byte, RecoveryJournalHeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		t.Fatalf("read header: %v", err)
	}
	binary.LittleEndian.PutUint32(
		headerBytes[8:12], RecoveryJournalFormatLegacy,
	)
	checksum := PageChecksum(headerBytes[:RecoveryJournalHeaderSize-8])
	binary.LittleEndian.PutUint32(
		headerBytes[RecoveryJournalHeaderSize-8:RecoveryJournalHeaderSize-4],
		checksum,
	)
	binary.LittleEndian.PutUint32(
		headerBytes[RecoveryJournalHeaderSize-4:], ^checksum,
	)
	if _, err := file.WriteAt(headerBytes, 0); err != nil {
		t.Fatalf("write downgraded header: %v", err)
	}
	downgraded, err := DecodeRecoveryJournalHeader(headerBytes)
	if err != nil {
		t.Fatalf("decode downgraded header: %v", err)
	}
	if downgraded.FormatVersion != RecoveryJournalFormatLegacy {
		t.Fatalf("downgraded format = %d, want legacy", downgraded.FormatVersion)
	}

	if _, err := OpenRecoveryJournal(file); !errors.Is(
		err, ErrRecoveryJournalRecord,
	) || errors.Is(err, errRecoveryJournalTruncatableTail) {
		t.Fatalf("Open downgraded scalar journal = %v, want hard record error", err)
	}
	manager := newRecoveryJournalManager(file, downgraded)
	if err := manager.Replay(1, func(RecoveryRecord) error {
		t.Fatal("downgraded scalar record reached replay callback")
		return nil
	}); !errors.Is(err, ErrRecoveryJournalRecord) ||
		errors.Is(err, errRecoveryJournalTruncatableTail) {
		t.Fatalf("Replay downgraded scalar journal = %v, want hard record error", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close downgraded file: %v", err)
	}
}

func TestRecoveryJournalSemanticDamageIsHardForScanAndReplay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(record []byte, entry, metadata, value int)
	}{
		{
			name: "unknown batch entry kind",
			mutate: func(record []byte, entry, _, _ int) {
				binary.LittleEndian.PutUint16(record[entry:entry+2], 0x7777)
			},
		},
		{
			name: "invalid scalar value",
			mutate: func(record []byte, _, _, value int) {
				copy(record[value:value+2], "01")
			},
		},
		{
			name: "reserved scalar metadata",
			mutate: func(record []byte, _, metadata, _ int) {
				record[metadata+3] = 1
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := RecoveryBatchEntry{
				Kind:  RecoveryRecordKindScalarPatch,
				Key:   []byte("account:9"),
				Value: []byte("17"),
				ScalarPatch: RecoveryScalarPatchMetadata{
					CanonicalOffset:        12,
					OldScalarLength:        2,
					ExpectedResultChecksum: 0x12345678,
				},
			}
			rj, path := createTestJournalFormat(
				t, 8<<10, RecoveryJournalFormatScalarPatch,
			)
			if _, err := rj.AppendBatch(2, []RecoveryBatchEntry{entry}); err != nil {
				t.Fatalf("AppendBatch: %v", err)
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
			metadataAt := entryAt + RecoveryBatchEntryHeaderSize
			valueAt := metadataAt + RecoveryScalarPatchMetadataSize + len(entry.Key)
			tc.mutate(record, entryAt, metadataAt, valueAt)
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
}

func TestRecoveryJournalChecksumValidUnknownRecordKindIsHard(t *testing.T) {
	rj, path := createTestJournalFormat(
		t, 8<<10, RecoveryJournalFormatScalarPatch,
	)
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
