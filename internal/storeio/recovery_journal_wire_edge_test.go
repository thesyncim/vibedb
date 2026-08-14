package storeio

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func wireEdgeSealRecoveryRecord(record []byte, bodyEnd int) {
	checksum := PageChecksum(record[:bodyEnd])
	binary.LittleEndian.PutUint32(record[bodyEnd:bodyEnd+4], checksum)
	binary.LittleEndian.PutUint32(record[bodyEnd+4:bodyEnd+8], ^checksum)
}

func wireEdgeSealRecoveryHeader(header []byte) {
	checksum := PageChecksum(header[:RecoveryJournalHeaderSize-8])
	binary.LittleEndian.PutUint32(
		header[RecoveryJournalHeaderSize-8:RecoveryJournalHeaderSize-4], checksum,
	)
	binary.LittleEndian.PutUint32(
		header[RecoveryJournalHeaderSize-4:], ^checksum,
	)
}

func wireEdgeRecoveryRecordWithSequence(
	t *testing.T, sequence, generation uint64,
) []byte {
	t.Helper()
	record := make([]byte, RecoveryJournalMinSectorSize)
	n, err := EncodeRecoveryRecord(
		record, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence: sequence, Generation: generation,
			Kind: RecoveryRecordKindPut, Key: []byte("k"), Value: []byte("v"),
		},
	)
	if err != nil {
		t.Fatalf("EncodeRecoveryRecord: %v", err)
	}
	return record[:n]
}

func wireEdgeRecoveryRecordWithZeroSequence(t *testing.T, generation uint64) []byte {
	t.Helper()
	record := wireEdgeRecoveryRecordWithSequence(t, 1, generation)
	binary.LittleEndian.PutUint64(record[8:16], 0)
	bodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(record[24:28])) +
		int(binary.LittleEndian.Uint32(record[28:32]))
	wireEdgeSealRecoveryRecord(record, bodyEnd)
	return record
}

func wireEdgeCreateRecoveryJournal(
	t *testing.T, baseSequence uint64,
) (*RecoveryJournal, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wire-edge.rjournal")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create journal file: %v", err)
	}
	header := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	header.BaseSequence = baseSequence
	journal, err := CreateRecoveryJournal(file, header)
	if err != nil {
		_ = file.Close()
		t.Fatalf("CreateRecoveryJournal: %v", err)
	}
	return journal, path
}

func wireEdgeOpenRecoveryJournal(
	t *testing.T, path string,
) (*RecoveryJournal, error) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open journal file: %v", err)
	}
	journal, err := OpenRecoveryJournal(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return journal, nil
}

func TestRecoveryJournalSequenceExhaustion(t *testing.T) {
	t.Run("terminal sequence is admitted once", func(t *testing.T) {
		journal, path := wireEdgeCreateRecoveryJournal(t, ^uint64(0)-1)
		sequence, err := journal.Append(
			RecoveryRecordKindPut, 2, []byte("terminal"), []byte("value"),
		)
		if err != nil {
			t.Fatalf("terminal Append: %v", err)
		}
		if sequence != ^uint64(0) || journal.NextSequence() != 0 {
			t.Fatalf(
				"terminal append sequence/next = %d/%d, want %d/0",
				sequence, journal.NextSequence(), ^uint64(0),
			)
		}
		terminalCursor := journal.Cursor()
		if journal.Fits(1, 1) {
			t.Fatal("Fits accepted a record after sequence exhaustion")
		}
		if _, err := journal.Append(
			RecoveryRecordKindPut, 3, []byte("next"), []byte("value"),
		); !errors.Is(err, ErrRecoveryJournalFull) {
			t.Fatalf("post-exhaustion Append = %v, want ErrRecoveryJournalFull", err)
		}
		entries := []RecoveryBatchEntry{
			{Kind: RecoveryRecordKindPut, Key: []byte("batch"), Value: []byte("value")},
		}
		if journal.FitsBatch(entries) {
			t.Fatal("FitsBatch accepted a record after sequence exhaustion")
		}
		if _, err := journal.AppendBatch(3, entries); !errors.Is(
			err, ErrRecoveryJournalFull,
		) {
			t.Fatalf("post-exhaustion AppendBatch = %v, want ErrRecoveryJournalFull", err)
		}
		if journal.FitsConditionalBatch(entries) {
			t.Fatal("FitsConditionalBatch accepted a record after sequence exhaustion")
		}
		var markerID [16]byte
		markerID[0] = 1
		if _, err := journal.AppendConditionalBatch(
			3, markerID, 1, 1, entries,
		); !errors.Is(err, ErrRecoveryJournalFull) {
			t.Fatalf(
				"post-exhaustion AppendConditionalBatch = %v, want ErrRecoveryJournalFull",
				err,
			)
		}
		if journal.Cursor() != terminalCursor || journal.NextSequence() != 0 {
			t.Fatalf(
				"failed append changed cursor/sequence = %d/%d, want %d/0",
				journal.Cursor(), journal.NextSequence(), terminalCursor,
			)
		}
		if err := journal.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// A valid-looking current-domain sequence-0 record follows the terminal
		// record. Reopen must stop at MaxUint64 and never inspect this record.
		zero := wireEdgeRecoveryRecordWithZeroSequence(t, 3)
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("open for forged tail: %v", err)
		}
		if _, err := file.WriteAt(
			zero, int64(recoveryJournalRegionStart+terminalCursor),
		); err != nil {
			_ = file.Close()
			t.Fatalf("write forged sequence-0 tail: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close forged tail: %v", err)
		}

		reopened, err := wireEdgeOpenRecoveryJournal(t, path)
		if err != nil {
			t.Fatalf("OpenRecoveryJournal after terminal record: %v", err)
		}
		defer reopened.Close()
		if reopened.Cursor() != terminalCursor || reopened.NextSequence() != 0 {
			t.Fatalf(
				"reopened cursor/sequence = %d/%d, want %d/0",
				reopened.Cursor(), reopened.NextSequence(), terminalCursor,
			)
		}
	})

	t.Run("exhausted base ignores stale region and decoder rejects sequence zero", func(t *testing.T) {
		journal, path := wireEdgeCreateRecoveryJournal(t, ^uint64(0))
		if journal.NextSequence() != 0 {
			t.Fatalf("created next sequence = %d, want 0", journal.NextSequence())
		}
		if err := journal.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		empty, err := wireEdgeOpenRecoveryJournal(t, path)
		if err != nil {
			t.Fatalf("open empty exhausted journal: %v", err)
		}
		if empty.Cursor() != 0 || empty.NextSequence() != 0 {
			t.Fatalf(
				"empty exhausted cursor/sequence = %d/%d, want 0/0",
				empty.Cursor(), empty.NextSequence(),
			)
		}
		if empty.Fits(1, 1) {
			t.Fatal("Fits accepted a record under an exhausted base")
		}
		if _, err := empty.Append(
			RecoveryRecordKindPut, 2, []byte("next"), []byte("value"),
		); !errors.Is(err, ErrRecoveryJournalFull) {
			t.Fatalf("exhausted-base Append = %v, want ErrRecoveryJournalFull", err)
		}
		if err := empty.Close(); err != nil {
			t.Fatalf("close empty exhausted journal: %v", err)
		}

		zero := wireEdgeRecoveryRecordWithZeroSequence(t, 2)
		if _, _, err := DecodeRecoveryRecord(
			zero, RecoveryJournalMinSectorSize, 0,
		); !errors.Is(err, ErrRecoveryJournalRecord) ||
			errors.Is(err, errRecoveryJournalTruncatableTail) {
			t.Fatalf("decode sequence 0 = %v, want hard record error", err)
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("open for sequence-0 record: %v", err)
		}
		if _, err := file.WriteAt(zero, recoveryJournalRegionStart); err != nil {
			_ = file.Close()
			t.Fatalf("write sequence-0 record: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close sequence-0 record file: %v", err)
		}
		opened, err := wireEdgeOpenRecoveryJournal(t, path)
		if err != nil {
			t.Fatalf("open exhausted journal with stale current-magic bytes: %v", err)
		}
		defer opened.Close()
		if opened.Cursor() != 0 || opened.NextSequence() != 0 {
			t.Fatalf(
				"stale-region open cursor/sequence = %d/%d, want 0/0",
				opened.Cursor(), opened.NextSequence(),
			)
		}
	})
}

func TestRecoveryJournalEqualRecycleCountConflictFailsClosed(t *testing.T) {
	journal, path := wireEdgeCreateRecoveryJournal(t, 0)
	header := journal.Header()
	if err := journal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	conflict := header
	conflict.JournalID[0] ^= 0xff
	encoded := make([]byte, RecoveryJournalHeaderSize)
	if _, err := EncodeRecoveryJournalHeader(encoded, conflict); err != nil {
		t.Fatalf("EncodeRecoveryJournalHeader conflict: %v", err)
	}
	if decoded, err := DecodeRecoveryJournalHeader(encoded); err != nil || decoded != conflict {
		t.Fatalf("conflicting slot is not independently valid: decoded=%+v err=%v", decoded, err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for conflicting slot: %v", err)
	}
	if _, err := file.WriteAt(encoded, RecoveryJournalHeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("write conflicting slot: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close conflicting slot: %v", err)
	}
	if opened, err := wireEdgeOpenRecoveryJournal(t, path); !errors.Is(
		err, ErrRecoveryJournalCorrupt,
	) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("equal-count conflicting headers = %v, want corrupt", err)
	}
}

func TestRecoveryJournalAuthenticatedInvalidNewestHeaderCannotHideGrowth(t *testing.T) {
	const initial = uint64(RecoveryJournalMinSectorSize)
	journal, path := createTestJournal(t, initial)
	appendPut(t, journal, 2, "before", "growth")
	const grown = uint64(4 * RecoveryJournalMinSectorSize)
	if err := journal.GrowCapacity(grown, true); err != nil {
		t.Fatalf("GrowCapacity: %v", err)
	}
	appendPut(t, journal, 3, "after", string(make([]byte, 1_200)))
	newestSlot := journal.headerSlot
	if journal.Header().Capacity != grown || newestSlot == 0 {
		t.Fatalf(
			"grown header capacity/slot = %d/%d, want %d/nonzero",
			journal.Header().Capacity, newestSlot, grown,
		)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for header corruption: %v", err)
	}
	newest := make([]byte, RecoveryJournalHeaderSize)
	if _, err := file.ReadAt(
		newest, int64(newestSlot)*RecoveryJournalHeaderSize,
	); err != nil {
		_ = file.Close()
		t.Fatalf("read newest header: %v", err)
	}
	// Reserved byte 92 is part of the authenticated current header grammar.
	newest[92] = 1
	wireEdgeSealRecoveryHeader(newest)
	if !recoveryJournalHeaderAuthenticated(newest) {
		_ = file.Close()
		t.Fatal("test header is not checksum-authenticated")
	}
	if _, err := DecodeRecoveryJournalHeader(newest); !errors.Is(
		err, ErrRecoveryJournalCorrupt,
	) {
		_ = file.Close()
		t.Fatalf("semantic-invalid newest header decode = %v, want corrupt", err)
	}
	if _, err := file.WriteAt(
		newest, int64(newestSlot)*RecoveryJournalHeaderSize,
	); err != nil {
		_ = file.Close()
		t.Fatalf("write newest header: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupted journal: %v", err)
	}

	opened, err := wireEdgeOpenRecoveryJournal(t, path)
	if opened != nil {
		_ = opened.Close()
	}
	if !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf(
			"open with authenticated invalid grown header = %v, want corrupt",
			err,
		)
	}
}

func wireEdgeRecoverySemanticRecords(t *testing.T) map[string][]byte {
	t.Helper()
	result := make(map[string][]byte)

	standalone := wireEdgeRecoveryRecordWithSequence(t, 1, 2)
	binary.LittleEndian.PutUint16(standalone[4:6], 0x7777)
	standaloneBodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(standalone[24:28])) +
		int(binary.LittleEndian.Uint32(standalone[28:32]))
	wireEdgeSealRecoveryRecord(standalone, standaloneBodyEnd)
	result["standalone-atomic"] = standalone

	atomicEntries := []RecoveryBatchEntry{
		{Kind: RecoveryRecordKindPut, Key: []byte("a"), Value: []byte("1")},
		{Kind: RecoveryRecordKindPut, Key: []byte("b"), Value: []byte("2")},
	}
	atomicPlan, ok := prepareRecoveryBatch(RecoveryJournalMinSectorSize, atomicEntries)
	if !ok {
		t.Fatal("prepare atomic batch")
	}
	atomic := make([]byte, atomicPlan.padded)
	if _, err := encodeRecoveryBatchRecordPrepared(
		atomic, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence: 1, Generation: 2, Kind: RecoveryRecordKindBatch,
			Entries: atomicEntries,
		}, atomicPlan,
	); err != nil {
		t.Fatalf("encode atomic batch: %v", err)
	}
	secondEntry := RecoveryJournalRecordPrefixSize +
		RecoveryBatchEntryHeaderSize + len(atomicEntries[0].Key) + len(atomicEntries[0].Value)
	secondKey := secondEntry + RecoveryBatchEntryHeaderSize
	atomic[secondKey] = 'a'
	atomicBodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(atomic[28:32]))
	wireEdgeSealRecoveryRecord(atomic, atomicBodyEnd)
	result["atomic-batch"] = atomic

	deltaEntries := []RecoveryBatchEntry{
		{Kind: RecoveryRecordKindPut, Key: []byte("a"), Value: []byte("1")},
		{Kind: RecoveryRecordKindDelete, Key: []byte("b")},
	}
	deltaPlan, ok := prepareRecoveryDeltaBatch(RecoveryJournalMinSectorSize, deltaEntries)
	if !ok {
		t.Fatal("prepare delta batch")
	}
	delta := make([]byte, deltaPlan.padded)
	if _, err := encodeRecoveryBatchRecordPrepared(
		delta, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence: 1, Generation: 2, Kind: RecoveryRecordKindDeltaBatch,
			Entries: deltaEntries,
		}, deltaPlan,
	); err != nil {
		t.Fatalf("encode delta batch: %v", err)
	}
	binary.LittleEndian.PutUint64(delta[16:24], 1)
	deltaBodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(delta[28:32]))
	wireEdgeSealRecoveryRecord(delta, deltaBodyEnd)
	result["delta-batch"] = delta

	conditionalEntries := []RecoveryBatchEntry{
		{Kind: RecoveryRecordKindPut, Key: []byte("a"), Value: []byte("1")},
	}
	conditionalPlan, ok := prepareRecoveryConditionalBatch(
		RecoveryJournalMinSectorSize, conditionalEntries,
	)
	if !ok {
		t.Fatal("prepare conditional batch")
	}
	var markerID [16]byte
	markerID[0] = 1
	conditional := make([]byte, conditionalPlan.padded)
	if _, err := encodeRecoveryConditionalBatchRecordPrepared(
		conditional, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence: 1, Generation: 2, Kind: RecoveryRecordKindConditionalBatch,
			Entries: conditionalEntries,
			Conditional: RecoveryConditionalHeader{
				MarkerID: markerID, MarkerEpoch: 1, TxnID: 1,
			},
		}, conditionalPlan,
	); err != nil {
		t.Fatalf("encode conditional batch: %v", err)
	}
	epochAt := RecoveryJournalRecordPrefixSize + 16
	binary.LittleEndian.PutUint64(conditional[epochAt:epochAt+8], 0)
	conditionalBodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(conditional[28:32]))
	wireEdgeSealRecoveryRecord(conditional, conditionalBodyEnd)
	result["conditional-batch"] = conditional

	return result
}

func TestRecoveryJournalChecksumValidSemanticErrorsAreHard(t *testing.T) {
	for name, record := range wireEdgeRecoverySemanticRecords(t) {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeRecoveryRecord(
				record, RecoveryJournalMinSectorSize, 1,
			); !errors.Is(err, ErrRecoveryJournalRecord) ||
				errors.Is(err, errRecoveryJournalTruncatableTail) {
				t.Fatalf("DecodeRecoveryRecord = %v, want hard record error", err)
			}

			journal, path := wireEdgeCreateRecoveryJournal(t, 0)
			if err := journal.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatalf("open for semantic record: %v", err)
			}
			if _, err := file.WriteAt(record, recoveryJournalRegionStart); err != nil {
				_ = file.Close()
				t.Fatalf("write semantic record: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close semantic record file: %v", err)
			}
			if opened, err := wireEdgeOpenRecoveryJournal(t, path); !errors.Is(
				err, ErrRecoveryJournalRecord,
			) || errors.Is(err, errRecoveryJournalTruncatableTail) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatalf("OpenRecoveryJournal = %v, want hard record error", err)
			}
		})
	}
}

func TestRecoveryJournalAuthenticatedKnownKindLayoutMismatchIsHard(t *testing.T) {
	entries := []RecoveryBatchEntry{{
		Kind: RecoveryRecordKindPut, Key: []byte("batch-key"), Value: []byte("batch-value"),
	}}
	plan, ok := prepareRecoveryBatch(RecoveryJournalMinSectorSize, entries)
	if !ok {
		t.Fatal("prepare batch")
	}
	batchAsPut := make([]byte, plan.padded)
	if _, err := encodeRecoveryBatchRecordPrepared(
		batchAsPut, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence: 1, Generation: 2, Kind: RecoveryRecordKindBatch,
			Entries: entries,
		}, plan,
	); err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	binary.LittleEndian.PutUint16(batchAsPut[4:6], RecoveryRecordKindPut)
	batchBodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(batchAsPut[28:32]))
	wireEdgeSealRecoveryRecord(batchAsPut, batchBodyEnd)

	standaloneAsBatch := wireEdgeRecoveryRecordWithSequence(t, 1, 2)
	binary.LittleEndian.PutUint16(standaloneAsBatch[4:6], RecoveryRecordKindBatch)
	standaloneBodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(standaloneAsBatch[24:28])) +
		int(binary.LittleEndian.Uint32(standaloneAsBatch[28:32]))
	wireEdgeSealRecoveryRecord(standaloneAsBatch, standaloneBodyEnd)

	for name, record := range map[string][]byte{
		"batch-layout-as-put":        batchAsPut,
		"standalone-layout-as-batch": standaloneAsBatch,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeRecoveryRecord(
				record, RecoveryJournalMinSectorSize, 1,
			); !errors.Is(err, ErrRecoveryJournalRecord) ||
				errors.Is(err, errRecoveryJournalTruncatableTail) {
				t.Fatalf("DecodeRecoveryRecord = %v, want hard record error", err)
			}
		})
	}
}

func TestRecoveryJournalAuthenticatedCrossLayoutKindsAreHard(t *testing.T) {
	entries := []RecoveryBatchEntry{{
		Kind: RecoveryRecordKindPut, Key: []byte("batch-key"), Value: []byte("value"),
	}}
	plan, ok := prepareRecoveryBatch(RecoveryJournalMinSectorSize, entries)
	if !ok {
		t.Fatal("prepare atomic batch")
	}
	batchAsPut := make([]byte, plan.padded)
	if _, err := encodeRecoveryBatchRecordPrepared(
		batchAsPut, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence: 1, Generation: 2, Kind: RecoveryRecordKindBatch,
			Entries: entries,
		},
		plan,
	); err != nil {
		t.Fatalf("encode atomic batch: %v", err)
	}
	binary.LittleEndian.PutUint16(batchAsPut[4:6], RecoveryRecordKindPut)
	batchBodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(batchAsPut[28:32]))
	wireEdgeSealRecoveryRecord(batchAsPut, batchBodyEnd)

	standaloneAsBatch := wireEdgeRecoveryRecordWithSequence(t, 1, 2)
	binary.LittleEndian.PutUint16(
		standaloneAsBatch[4:6], RecoveryRecordKindBatch,
	)
	standaloneBodyEnd := RecoveryJournalRecordPrefixSize +
		int(binary.LittleEndian.Uint32(standaloneAsBatch[24:28])) +
		int(binary.LittleEndian.Uint32(standaloneAsBatch[28:32]))
	wireEdgeSealRecoveryRecord(standaloneAsBatch, standaloneBodyEnd)

	for name, record := range map[string][]byte{
		"batch layout with put kind":        batchAsPut,
		"standalone layout with batch kind": standaloneAsBatch,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeRecoveryRecord(
				record, RecoveryJournalMinSectorSize, 1,
			); !errors.Is(err, ErrRecoveryJournalRecord) ||
				errors.Is(err, errRecoveryJournalTruncatableTail) {
				t.Fatalf("DecodeRecoveryRecord = %v, want hard record error", err)
			}

			journal, path := wireEdgeCreateRecoveryJournal(t, 0)
			if err := journal.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatalf("open for cross-layout record: %v", err)
			}
			if _, err := file.WriteAt(record, recoveryJournalRegionStart); err != nil {
				_ = file.Close()
				t.Fatalf("write cross-layout record: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close cross-layout record file: %v", err)
			}
			if opened, err := wireEdgeOpenRecoveryJournal(t, path); !errors.Is(err, ErrRecoveryJournalRecord) ||
				errors.Is(err, errRecoveryJournalTruncatableTail) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatalf("OpenRecoveryJournal = %v, want hard record error", err)
			}
		})
	}
}

func TestRecoveryJournalLogicalRecordBeforePaddingMayReplay(t *testing.T) {
	entries := []RecoveryBatchEntry{
		{Kind: RecoveryRecordKindPut, Key: []byte("a"), Value: []byte("1")},
	}
	atomicPlan, ok := prepareRecoveryBatch(RecoveryJournalMinSectorSize, entries)
	if !ok {
		t.Fatal("prepare atomic batch")
	}
	deltaPlan, ok := prepareRecoveryDeltaBatch(RecoveryJournalMinSectorSize, entries)
	if !ok {
		t.Fatal("prepare delta batch")
	}
	conditionalPlan, ok := prepareRecoveryConditionalBatch(
		RecoveryJournalMinSectorSize, entries,
	)
	if !ok {
		t.Fatal("prepare conditional batch")
	}
	var markerID [16]byte
	markerID[0] = 1

	tests := []struct {
		name       string
		kind       uint16
		logicalLen int
		append     func(*RecoveryJournal) (uint64, error)
	}{
		{
			name: "scalar", kind: RecoveryRecordKindPut,
			logicalLen: RecoveryJournalRecordPrefixSize + len("key") + len("value") +
				RecoveryJournalRecordTrailerSize,
			append: func(journal *RecoveryJournal) (uint64, error) {
				return journal.Append(
					RecoveryRecordKindPut, 2, []byte("key"), []byte("value"),
				)
			},
		},
		{
			name: "atomic-batch", kind: RecoveryRecordKindBatch,
			logicalLen: RecoveryJournalRecordPrefixSize + int(atomicPlan.bodyLen) +
				RecoveryJournalRecordTrailerSize,
			append: func(journal *RecoveryJournal) (uint64, error) {
				return journal.AppendPreparedBatch(2, entries, atomicPlan)
			},
		},
		{
			name: "delta-batch", kind: RecoveryRecordKindDeltaBatch,
			logicalLen: RecoveryJournalRecordPrefixSize + int(deltaPlan.bodyLen) +
				RecoveryJournalRecordTrailerSize,
			append: func(journal *RecoveryJournal) (uint64, error) {
				return journal.AppendPreparedDeltaBatch(2, entries, deltaPlan)
			},
		},
		{
			name: "conditional-batch", kind: RecoveryRecordKindConditionalBatch,
			logicalLen: RecoveryJournalRecordPrefixSize + int(conditionalPlan.bodyLen) +
				RecoveryJournalRecordTrailerSize,
			append: func(journal *RecoveryJournal) (uint64, error) {
				return journal.AppendPreparedConditionalBatch(
					2, markerID, 1, 1, entries, conditionalPlan,
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal, path := wireEdgeCreateRecoveryJournal(t, 0)
			if test.logicalLen >= RecoveryJournalMinSectorSize {
				t.Fatalf(
					"logical length %d is not before sector padding %d",
					test.logicalLen, RecoveryJournalMinSectorSize,
				)
			}
			journal.writeAt = func(payload []byte, offset int64) (int, error) {
				return journal.file.WriteAt(payload[:test.logicalLen], offset)
			}
			if sequence, err := test.append(journal); sequence != 0 ||
				!errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("Append = sequence %d, err %v; want 0, io.ErrShortWrite", sequence, err)
			}
			if journal.Cursor() != 0 || journal.NextSequence() != 1 {
				t.Fatalf(
					"short append changed cursor/sequence = %d/%d, want 0/1",
					journal.Cursor(), journal.NextSequence(),
				)
			}
			if err := journal.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			reopened, err := wireEdgeOpenRecoveryJournal(t, path)
			if err != nil {
				t.Fatalf("OpenRecoveryJournal: %v", err)
			}
			defer reopened.Close()
			if reopened.Cursor() != RecoveryJournalMinSectorSize ||
				reopened.NextSequence() != 2 {
				t.Fatalf(
					"reopened cursor/sequence = %d/%d, want %d/2",
					reopened.Cursor(), reopened.NextSequence(), RecoveryJournalMinSectorSize,
				)
			}
			records := replayAll(t, reopened, 1)
			if len(records) != 1 || records[0].Kind != test.kind ||
				records[0].Generation != 2 {
				t.Fatalf("replayed records = %+v, want one kind %d generation 2", records, test.kind)
			}
		})
	}
}
