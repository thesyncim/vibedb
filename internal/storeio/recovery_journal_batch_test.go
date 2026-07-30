package storeio

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

// TestRecoveryBatchRecordRoundTrip proves a batch record encodes every entry
// under one sequence and generation, decodes them back in order, and interleaves
// with ordinary single records on the same monotonic sequence.
func TestRecoveryBatchRecordRoundTrip(t *testing.T) {
	rj, path := createTestJournal(t, 64<<10)

	appendPut(t, rj, 5, "alpha", "one")
	entries := []RecoveryBatchEntry{
		{Kind: recoveryRecordKindPut, Key: []byte("k1"), Value: []byte(`{"a":1}`)},
		{Kind: recoveryRecordKindDelete, Key: []byte("k2")},
		{Kind: recoveryRecordKindPut, Key: []byte("k3"), Value: []byte(`{"c":3}`)},
	}
	if _, err := rj.AppendBatch(6, entries); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if err := rj.Sync(false); err != nil {
		t.Fatalf("sync: %v", err)
	}
	appendPut(t, rj, 7, "omega", "last")
	_ = rj.Close()

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	var recs []RecoveryRecord
	if err := rj.Replay(0, func(rec RecoveryRecord) error {
		copied := RecoveryRecord{
			Sequence: rec.Sequence, Generation: rec.Generation, Kind: rec.Kind,
			Key:   append([]byte(nil), rec.Key...),
			Value: append([]byte(nil), rec.Value...),
		}
		for _, e := range rec.Entries {
			copied.Entries = append(copied.Entries, RecoveryBatchEntry{
				Kind:  e.Kind,
				Key:   append([]byte(nil), e.Key...),
				Value: append([]byte(nil), e.Value...),
			})
		}
		recs = append(recs, copied)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("replayed %d records, want 3 (put, batch, put)", len(recs))
	}
	if recs[0].Kind != recoveryRecordKindPut || string(recs[0].Key) != "alpha" {
		t.Fatalf("record 0 = %+v, want put alpha", recs[0])
	}
	batch := recs[1]
	if batch.Kind != recoveryRecordKindBatch || batch.Generation != 6 {
		t.Fatalf("record 1 = kind %d gen %d, want batch gen 6", batch.Kind, batch.Generation)
	}
	if batch.Sequence != recs[0].Sequence+1 {
		t.Fatalf("batch sequence %d, want %d", batch.Sequence, recs[0].Sequence+1)
	}
	if len(batch.Entries) != len(entries) {
		t.Fatalf("batch decoded %d entries, want %d", len(batch.Entries), len(entries))
	}
	for i, want := range entries {
		got := batch.Entries[i]
		if got.Kind != want.Kind || !bytes.Equal(got.Key, want.Key) ||
			!bytes.Equal(got.Value, want.Value) {
			t.Fatalf("entry %d = %+v, want %+v", i, got, want)
		}
	}
	if recs[2].Kind != recoveryRecordKindPut || string(recs[2].Key) != "omega" ||
		recs[2].Sequence != batch.Sequence+1 {
		t.Fatalf("record 2 = %+v, want put omega after the batch", recs[2])
	}
}

// TestRecoveryBatchRecordTornTailTruncates proves the single CRC over a batch
// record makes it all-or-nothing: damaging one byte of the record's body drops
// the whole batch from replay while every earlier record survives.
func TestRecoveryBatchRecordTornTailTruncates(t *testing.T) {
	rj, path := createTestJournal(t, 64<<10)
	appendPut(t, rj, 2, "survivor", "kept")
	cursorBeforeBatch := rj.Cursor()
	entries := []RecoveryBatchEntry{
		{Kind: recoveryRecordKindPut, Key: []byte("x1"), Value: []byte("v1")},
		{Kind: recoveryRecordKindPut, Key: []byte("x2"), Value: []byte("v2")},
	}
	if _, err := rj.AppendBatch(3, entries); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if err := rj.Sync(false); err != nil {
		t.Fatalf("sync: %v", err)
	}
	_ = rj.Close()

	// Flip a byte inside the batch record's entry body. The batch CRC must reject
	// the whole record, truncating replay before it.
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("reopen journal file: %v", err)
	}
	corruptOffset := int64(recoveryJournalRegionStart) +
		int64(cursorBeforeBatch) + RecoveryJournalRecordPrefixSize + 4
	var one [1]byte
	if _, err := file.ReadAt(one[:], corruptOffset); err != nil {
		t.Fatalf("read byte: %v", err)
	}
	one[0] ^= 0xFF
	if _, err := file.WriteAt(one[:], corruptOffset); err != nil {
		t.Fatalf("corrupt byte: %v", err)
	}
	_ = file.Close()

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	recs := replayAll(t, rj, 0)
	if len(recs) != 1 || string(recs[0].Key) != "survivor" {
		t.Fatalf("replayed %d records, want only the pre-batch survivor: %+v", len(recs), recs)
	}
}

// TestRecoveryBatchRecordPaddedSizeAndFits proves the sizing helper and FitsBatch
// agree with a real append: a batch reported to fit appends, and its consumed
// cursor is exactly the reported padded size.
func TestRecoveryBatchRecordPaddedSizeAndFits(t *testing.T) {
	entries := []RecoveryBatchEntry{
		{Kind: recoveryRecordKindPut, Key: []byte("key-one"), Value: []byte(`{"n":123}`)},
		{Kind: recoveryRecordKindDelete, Key: []byte("key-two")},
	}
	padded := RecoveryBatchRecordPaddedSize(RecoveryJournalMinSectorSize, entries)
	if padded%RecoveryJournalMinSectorSize != 0 || padded == 0 {
		t.Fatalf("padded size %d is not a positive sector multiple", padded)
	}
	payloadBytes := 0
	for i := range entries {
		payloadBytes += len(entries[i].Key) + len(entries[i].Value)
	}
	if got := RecoveryBatchRecordPaddedSizeForPayload(
		RecoveryJournalMinSectorSize, len(entries), payloadBytes,
	); got != padded {
		t.Fatalf("payload-only padded size = %d, want exact entry size %d",
			got, padded)
	}
	rj, _ := createTestJournal(t, 8<<10)
	if !rj.FitsBatch(entries) {
		t.Fatal("FitsBatch = false on an empty journal, want true")
	}
	plan, err := rj.PrepareBatch(entries)
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	if plan.PaddedSize() != padded {
		t.Fatalf("prepared padded size = %d, want %d",
			plan.PaddedSize(), padded)
	}
	if !rj.PreparedBatchFits(plan) {
		t.Fatal("PreparedBatchFits = false on an empty journal, want true")
	}
	before := rj.Cursor()
	if _, err := rj.AppendPreparedBatch(2, entries, plan); err != nil {
		t.Fatalf("append prepared batch: %v", err)
	}
	if got := rj.Cursor() - before; got != uint64(padded) {
		t.Fatalf("append consumed %d bytes, want reported padded %d", got, padded)
	}

	// A batch that cannot fit the remaining capacity reports full and does not
	// append.
	small, _ := createTestJournal(t, uint64(RecoveryJournalMinSectorSize))
	big := make([]RecoveryBatchEntry, 64)
	for i := range big {
		big[i] = RecoveryBatchEntry{Kind: recoveryRecordKindPut, Key: []byte("kkkkkkkk"), Value: make([]byte, 256)}
	}
	if small.FitsBatch(big) {
		t.Fatal("FitsBatch = true for an over-large batch, want false")
	}
	if _, err := small.AppendBatch(2, big); !errors.Is(err, ErrRecoveryJournalFull) {
		t.Fatalf("AppendBatch over capacity = %v, want ErrRecoveryJournalFull", err)
	}
}

// TestRecoveryBatchPreparedPlanFailsClosedAfterLengthChange proves an opaque
// layout cannot turn a caller mutation between preflight and append into a
// truncated or malformed write. The encoder revalidates the planned body while
// copying and leaves both cursor and sequence unchanged on a mismatch.
func TestRecoveryBatchPreparedPlanFailsClosedAfterLengthChange(t *testing.T) {
	rj, _ := createTestJournal(t, 8<<10)
	entries := []RecoveryBatchEntry{{
		Kind:  recoveryRecordKindPut,
		Key:   []byte("key"),
		Value: []byte("value"),
	}}
	plan, err := rj.PrepareBatch(entries)
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	cursor := rj.cursor
	sequence := rj.nextSequence
	writes := 0
	rj.writeAt = func(p []byte, _ int64) (int, error) {
		writes++
		return len(p), nil
	}
	entries[0].Value = append(entries[0].Value, '!')
	if _, err := rj.AppendPreparedBatch(
		2, entries, plan,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("AppendPreparedBatch after length change = %v, want invalid write",
			err)
	}
	if writes != 0 {
		t.Fatalf("write calls = %d, want 0", writes)
	}
	if rj.cursor != cursor || rj.nextSequence != sequence {
		t.Fatalf("journal advanced after rejected plan: cursor %d->%d sequence %d->%d",
			cursor, rj.cursor, sequence, rj.nextSequence)
	}
}
