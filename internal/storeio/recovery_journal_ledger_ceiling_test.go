package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRecoveryJournalLedgerCeilingAppendReopen(t *testing.T) {
	const (
		entryCount = 258
		keyBytes   = 106
		valueBytes = 16 << 20
		wantBytes  = valueBytes + 60*RecoveryJournalMinSectorSize
	)
	// Exercise the actual admitted rectangle: one maximum opaque value and
	// every maximum-width key, including the largest session-release count.
	// Non-JSON bytes ensure this proves binary replay, not text normalization.
	value := make([]byte, valueBytes)
	for i := range value {
		value[i] = byte(i*31 + i/256)
	}
	keys := make([]byte, entryCount*keyBytes)
	entries := make([]RecoveryBatchEntry, entryCount)
	for i := range entries {
		key := keys[i*keyBytes : (i+1)*keyBytes]
		for j := range key {
			key[j] = byte(j*17 + i)
		}
		binary.BigEndian.PutUint16(key, uint16(i))
		entries[i] = RecoveryBatchEntry{Kind: RecoveryRecordKindDelete, Key: key}
	}
	entries[0].Kind = RecoveryRecordKindPut
	entries[0].Value = value
	if got := RecoveryConditionalBatchRecordPaddedSize(RecoveryJournalMinSectorSize, entries); got != wantBytes {
		t.Fatalf("actual conditional record = %d bytes, want %d", got, wantBytes)
	}
	if uint64(wantBytes) != RecoveryJournalMaxCapacityBytes {
		t.Fatalf("ledger record = %d, global bound = %d", wantBytes, RecoveryJournalMaxCapacityBytes)
	}
	conditional := testConditionalHeader(t)
	assertRecord := func(t *testing.T, record RecoveryRecord) {
		t.Helper()
		if record.Kind != RecoveryRecordKindConditionalBatch || record.Sequence != 1 ||
			record.Generation != 2 || record.Conditional != conditional || len(record.Entries) != entryCount {
			t.Fatal("record metadata or entry count changed")
		}
		for i, got := range record.Entries {
			want := entries[i]
			if got.Kind != want.Kind || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
				t.Fatalf("entry %d differs from appended binary bytes", i)
			}
		}
	}
	t.Run("binary-codec", func(t *testing.T) {
		plan, ok := prepareRecoveryConditionalBatch(RecoveryJournalMinSectorSize, entries)
		if !ok || plan.PaddedSize() != wantBytes {
			t.Fatalf("actual conditional plan: size=%d valid=%t", plan.PaddedSize(), ok)
		}
		record := RecoveryRecord{Kind: RecoveryRecordKindConditionalBatch,
			Sequence: 1, Generation: 2, Conditional: conditional, Entries: entries}
		encoded := make([]byte, wantBytes)
		if _, err := encodeRecoveryConditionalBatchRecordPrepared(
			encoded[:wantBytes-RecoveryJournalMinSectorSize], RecoveryJournalMinSectorSize, record, plan,
		); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("one-sector-short encoding = %v, want invalid write", err)
		}
		if n, err := encodeRecoveryConditionalBatchRecordPrepared(
			encoded, RecoveryJournalMinSectorSize, record, plan,
		); err != nil || n != wantBytes {
			t.Fatalf("encoded bytes=%d err=%v", n, err)
		}
		decoded, n, err := DecodeRecoveryRecord(encoded, RecoveryJournalMinSectorSize, 1)
		if err != nil || n != wantBytes {
			t.Fatalf("decoded bytes=%d err=%v", n, err)
		}
		assertRecord(t, decoded)
	})
	for _, test := range []struct {
		name     string
		capacity uint64
		fits     bool
	}{
		{"one-sector-short", wantBytes - RecoveryJournalMinSectorSize, false},
		{"exact", wantBytes, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger.rjournal")
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			header := testJournalHeader(t, test.capacity)
			header.SealedCapacity = true
			journal, err := CreateRecoveryJournal(file, header)
			if err != nil {
				file.Close()
				if runtime.GOOS != "linux" && errors.Is(err, ErrStrictAllocationUnsupported) {
					t.Skip("sealed physical allocation requires Linux; actual record geometry checked above")
				}
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if journal != nil {
					_ = journal.Close()
				}
			})
			if got := journal.FitsConditionalBatch(entries); got != test.fits {
				t.Fatalf("fits = %t, want %t", got, test.fits)
			}
			sequence, err := journal.AppendConditionalBatch(2, conditional.MarkerID,
				conditional.MarkerEpoch, conditional.TxnID, entries)
			wantCursor, wantSequence := uint64(0), uint64(1)
			if test.fits {
				if err != nil || sequence != 1 {
					t.Fatalf("append sequence=%d err=%v", sequence, err)
				}
				wantCursor, wantSequence = wantBytes, 2
			} else if !errors.Is(err, ErrRecoveryJournalFull) || sequence != 0 {
				t.Fatalf("short append sequence=%d err=%v, want full", sequence, err)
			}
			if journal.Cursor() != wantCursor || journal.NextSequence() != wantSequence {
				t.Fatalf("append cursor=%d sequence=%d, want %d/%d",
					journal.Cursor(), journal.NextSequence(), wantCursor, wantSequence)
			}
			if err := journal.Sync(true); err != nil {
				t.Fatal(err)
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Size() != int64(recoveryJournalRegionStart)+int64(test.capacity) {
				t.Fatalf("sealed file size changed: info=%v err=%v", info, err)
			}
			file, err = os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			journal, err = OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{
				SealedCapacityBytes: test.capacity,
			})
			if err != nil {
				file.Close()
				t.Fatal(err)
			}
			if journal.Cursor() != wantCursor || journal.NextSequence() != wantSequence {
				t.Fatalf("reopened cursor=%d sequence=%d, want %d/%d",
					journal.Cursor(), journal.NextSequence(), wantCursor, wantSequence)
			}
			seen := 0
			if err := journal.Replay(1, func(record RecoveryRecord) error {
				seen++
				assertRecord(t, record)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if (test.fits && seen != 1) || (!test.fits && seen != 0) {
				t.Fatalf("replayed records = %d, fits=%t", seen, test.fits)
			}
		})
	}
}
