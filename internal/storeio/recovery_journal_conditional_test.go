package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"
)

func testConditionalHeader(t *testing.T) RecoveryConditionalHeader {
	t.Helper()
	var markerID [16]byte
	for i := range markerID {
		markerID[i] = byte(0xA0 + i)
	}
	return RecoveryConditionalHeader{
		MarkerID:    markerID,
		MarkerEpoch: 7,
		TxnID:       42,
	}
}

func testConditionalEntries() []RecoveryBatchEntry {
	return []RecoveryBatchEntry{
		{Kind: RecoveryRecordKindPut, Key: []byte("k1"), Value: []byte(`{"a":1}`)},
		{Kind: RecoveryRecordKindDelete, Key: []byte("k2")},
		{Kind: RecoveryRecordKindPut, Key: []byte("k3"), Value: []byte(`{"c":3}`)},
	}
}

func TestRecoveryJournalReplicatedSQLConditionalCeiling(t *testing.T) {
	const (
		entryCount   = 64
		payloadBytes = (16 << 20) + entryCount*256
	)
	want := (16 << 20) + 34*RecoveryJournalMinSectorSize
	got := RecoveryBatchRecordPaddedSizeForPayload(
		RecoveryJournalMinSectorSize,
		entryCount,
		payloadBytes+RecoveryConditionalHeaderSize,
	)
	if got != want {
		t.Fatalf("replicated SQL conditional ceiling = %d, want %d", got, want)
	}
	if uint64(got) != RecoveryJournalMaxCapacityBytes {
		t.Fatalf(
			"replicated SQL conditional ceiling = %d, journal clamp = %d",
			got, RecoveryJournalMaxCapacityBytes,
		)
	}
}

func replayConditionalAll(
	t *testing.T, rj *RecoveryJournal, base uint64,
) []RecoveryRecord {
	t.Helper()
	var out []RecoveryRecord
	err := rj.Replay(base, func(rec RecoveryRecord) error {
		copied := RecoveryRecord{
			Sequence:    rec.Sequence,
			Generation:  rec.Generation,
			Kind:        rec.Kind,
			Key:         append([]byte(nil), rec.Key...),
			Value:       append([]byte(nil), rec.Value...),
			Conditional: rec.Conditional,
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

// TestRecoveryConditionalBatchFramingGolden pins the kind-5 wire layout: the
// 32-byte envelope, the conditional header offsets, entry framing, and CRC
// coverage over envelope + conditional header + entries.
func TestRecoveryConditionalBatchFramingGolden(t *testing.T) {
	entries := testConditionalEntries()
	conditional := testConditionalHeader(t)
	plan, ok := prepareRecoveryConditionalBatch(
		RecoveryJournalMinSectorSize, entries,
	)
	if !ok {
		t.Fatal("prepare conditional batch")
	}
	encoded := make([]byte, plan.PaddedSize())
	if _, err := encodeRecoveryConditionalBatchRecordPrepared(
		encoded, RecoveryJournalMinSectorSize,
		RecoveryRecord{
			Sequence:    3,
			Generation:  4,
			Kind:        RecoveryRecordKindConditionalBatch,
			Entries:     entries,
			Conditional: conditional,
		},
		plan,
	); err != nil {
		t.Fatalf("encode: %v", err)
	}

	if got := binary.LittleEndian.Uint32(encoded[0:4]); got != recoveryRecordMagic {
		t.Fatalf("magic = %#x, want %#x", got, recoveryRecordMagic)
	}
	if got := binary.LittleEndian.Uint16(encoded[4:6]); got != RecoveryRecordKindConditionalBatch {
		t.Fatalf("kind = %d, want %d", got, RecoveryRecordKindConditionalBatch)
	}
	if got := binary.LittleEndian.Uint16(encoded[6:8]); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
	if got := binary.LittleEndian.Uint64(encoded[8:16]); got != 3 {
		t.Fatalf("sequence = %d, want 3", got)
	}
	if got := binary.LittleEndian.Uint64(encoded[16:24]); got != 4 {
		t.Fatalf("generation = %d, want 4", got)
	}
	if got := binary.LittleEndian.Uint32(encoded[24:28]); got != uint32(len(entries)) {
		t.Fatalf("entry count = %d, want %d", got, len(entries))
	}
	bodyLen := binary.LittleEndian.Uint32(encoded[28:32])
	if bodyLen != uint32(plan.bodyLen) {
		t.Fatalf("bodyLen = %d, want %d", bodyLen, plan.bodyLen)
	}
	if bodyLen < RecoveryConditionalHeaderSize {
		t.Fatalf("bodyLen %d shorter than conditional header", bodyLen)
	}

	headerAt := RecoveryJournalRecordPrefixSize
	if !bytes.Equal(encoded[headerAt:headerAt+16], conditional.MarkerID[:]) {
		t.Fatalf("MarkerID mismatch at offset %d", headerAt)
	}
	if got := binary.LittleEndian.Uint64(encoded[headerAt+16 : headerAt+24]); got != conditional.MarkerEpoch {
		t.Fatalf("MarkerEpoch at %d = %d, want %d",
			headerAt+16, got, conditional.MarkerEpoch)
	}
	if got := binary.LittleEndian.Uint64(encoded[headerAt+24 : headerAt+32]); got != conditional.TxnID {
		t.Fatalf("TxnID at %d = %d, want %d",
			headerAt+24, got, conditional.TxnID)
	}

	entryAt := headerAt + RecoveryConditionalHeaderSize
	for i, want := range entries {
		gotKind := binary.LittleEndian.Uint16(encoded[entryAt : entryAt+2])
		if gotKind != want.Kind {
			t.Fatalf("entry %d kind = %d, want %d", i, gotKind, want.Kind)
		}
		if binary.LittleEndian.Uint16(encoded[entryAt+2:entryAt+4]) != 0 {
			t.Fatalf("entry %d reserved nonzero", i)
		}
		keyLen := binary.LittleEndian.Uint32(encoded[entryAt+4 : entryAt+8])
		valueLen := binary.LittleEndian.Uint32(encoded[entryAt+8 : entryAt+12])
		if keyLen != uint32(len(want.Key)) || valueLen != uint32(len(want.Value)) {
			t.Fatalf("entry %d lengths = %d/%d, want %d/%d",
				i, keyLen, valueLen, len(want.Key), len(want.Value))
		}
		keyStart := entryAt + RecoveryBatchEntryHeaderSize
		valueStart := keyStart + int(keyLen)
		entryEnd := valueStart + int(valueLen)
		if !bytes.Equal(encoded[keyStart:valueStart], want.Key) ||
			!bytes.Equal(encoded[valueStart:entryEnd], want.Value) {
			t.Fatalf("entry %d payload mismatch", i)
		}
		entryAt = entryEnd
	}
	bodyEnd := RecoveryJournalRecordPrefixSize + int(bodyLen)
	if entryAt != bodyEnd {
		t.Fatalf("entries ended at %d, want bodyEnd %d", entryAt, bodyEnd)
	}
	checksum := PageChecksum(encoded[:bodyEnd])
	if got := binary.LittleEndian.Uint32(encoded[bodyEnd : bodyEnd+4]); got != checksum {
		t.Fatalf("CRC = %#x, want %#x over envelope+conditional+entries", got, checksum)
	}
	if got := binary.LittleEndian.Uint32(encoded[bodyEnd+4 : bodyEnd+8]); got != ^checksum {
		t.Fatalf("CRC complement = %#x, want %#x", got, ^checksum)
	}
	for i := bodyEnd + RecoveryJournalRecordTrailerSize; i < len(encoded); i++ {
		if encoded[i] != 0 {
			t.Fatalf("padding byte %d = %#x, want 0", i, encoded[i])
		}
	}
	if len(encoded)%RecoveryJournalMinSectorSize != 0 {
		t.Fatalf("padded length %d not a sector multiple", len(encoded))
	}

	decoded, padded, err := DecodeRecoveryRecord(
		encoded, RecoveryJournalMinSectorSize, 3,
	)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if padded != len(encoded) {
		t.Fatalf("decode padded = %d, want %d", padded, len(encoded))
	}
	if decoded.Kind != RecoveryRecordKindConditionalBatch ||
		decoded.Sequence != 3 || decoded.Generation != 4 ||
		decoded.Conditional != conditional ||
		len(decoded.Entries) != len(entries) {
		t.Fatalf("decoded = %+v", decoded)
	}
	for i, want := range entries {
		got := decoded.Entries[i]
		if got.Kind != want.Kind ||
			!bytes.Equal(got.Key, want.Key) ||
			!bytes.Equal(got.Value, want.Value) {
			t.Fatalf("decoded entry %d = %+v, want %+v", i, got, want)
		}
	}
}

// TestRecoveryConditionalBatchVersionGating proves the format-word gate and
// the mutual exclusion of kind 5 with legacy/scalar-patch journals, and of
// scalar-patch entries with the conditional format.
func TestRecoveryConditionalBatchVersionGating(t *testing.T) {
	entries := testConditionalEntries()
	conditional := testConditionalHeader(t)

	for _, formatVersion := range []uint32{
		RecoveryJournalFormatLegacy,
		RecoveryJournalFormatScalarPatch,
	} {
		t.Run(formatName(formatVersion)+"-rejects-kind5-append", func(t *testing.T) {
			rj, _ := createTestJournalFormat(t, 8<<10, formatVersion)
			defer rj.Close()
			beforeCursor, beforeSequence := rj.Cursor(), rj.NextSequence()
			if _, err := rj.AppendConditionalBatch(
				2, conditional.MarkerID, conditional.MarkerEpoch,
				conditional.TxnID, entries,
			); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("AppendConditionalBatch = %v, want invalid write", err)
			}
			if rj.Cursor() != beforeCursor || rj.NextSequence() != beforeSequence {
				t.Fatalf("rejection advanced cursor/sequence")
			}
		})
	}

	// A kind-5 record planted in a legacy journal fails closed on open/replay.
	t.Run("legacy-open-rejects-kind5-bytes", func(t *testing.T) {
		rj, path := createTestJournalFormat(
			t, 8<<10, RecoveryJournalFormatLegacy,
		)
		plan, ok := prepareRecoveryConditionalBatch(
			rj.header.SectorSize, entries,
		)
		if !ok {
			t.Fatal("prepare")
		}
		encoded := make([]byte, plan.PaddedSize())
		if _, err := encodeRecoveryConditionalBatchRecordPrepared(
			encoded, rj.header.SectorSize,
			RecoveryRecord{
				Sequence:    1,
				Generation:  2,
				Kind:        RecoveryRecordKindConditionalBatch,
				Entries:     entries,
				Conditional: conditional,
			},
			plan,
		); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if _, err := rj.writeAt(
			encoded, recoveryJournalRegionStart,
		); err != nil {
			t.Fatalf("plant kind-5: %v", err)
		}
		if err := rj.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer file.Close()
		if _, err := OpenRecoveryJournal(file); !errors.Is(
			err, ErrRecoveryJournalRecord,
		) || errors.Is(err, errRecoveryJournalTruncatableTail) {
			t.Fatalf("Open legacy+kind5 = %v, want hard record error", err)
		}
		header := rjHeaderFromFile(t, file)
		if header.FormatVersion != RecoveryJournalFormatLegacy {
			t.Fatalf("planted under format %d, want legacy", header.FormatVersion)
		}
		manager := newRecoveryJournalManager(file, header)
		if err := manager.Replay(1, func(RecoveryRecord) error {
			t.Fatal("kind-5 reached replay under legacy header")
			return nil
		}); !errors.Is(err, ErrRecoveryJournalRecord) ||
			errors.Is(err, errRecoveryJournalTruncatableTail) {
			t.Fatalf("Replay legacy+kind5 = %v, want hard record error", err)
		}
	})

	// Scalar-patch-era binaries know only formats 0 and 1. Format word 2 is
	// outside that set, so their open path rejects the header before decode.
	t.Run("conditional-header-rejected-by-scalar-patch-gate", func(t *testing.T) {
		header := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
		header.FormatVersion = RecoveryJournalFormatConditional
		header.RecycleCount = 1
		encoded := make([]byte, RecoveryJournalHeaderSize)
		if _, err := EncodeRecoveryJournalHeader(encoded, header); err != nil {
			t.Fatalf("EncodeRecoveryJournalHeader: %v", err)
		}
		formatWord := binary.LittleEndian.Uint32(encoded[8:12])
		if formatWord != RecoveryJournalFormatConditional {
			t.Fatalf("format word = %d, want %d",
				formatWord, RecoveryJournalFormatConditional)
		}
		// Exact predicate an older scalar-patch binary applies.
		if formatWord == RecoveryJournalFormatLegacy ||
			formatWord == RecoveryJournalFormatScalarPatch {
			t.Fatal("conditional format collides with a pre-conditional gate value")
		}
		supportedByScalarPatchBinary := formatWord == RecoveryJournalFormatLegacy ||
			formatWord == RecoveryJournalFormatScalarPatch
		if supportedByScalarPatchBinary {
			t.Fatal("scalar-patch-era gate accepted conditional format word")
		}
	})

	// A scalar-patch entry inside a conditional journal fails closed.
	t.Run("conditional-journal-rejects-scalar-patch", func(t *testing.T) {
		rj, _ := createTestJournalFormat(
			t, 8<<10, RecoveryJournalFormatConditional,
		)
		defer rj.Close()
		scalar := []RecoveryBatchEntry{{
			Kind:  RecoveryRecordKindScalarPatch,
			Key:   []byte("account:1"),
			Value: []byte("7"),
			ScalarPatch: RecoveryScalarPatchMetadata{
				CanonicalOffset:        12,
				OldScalarLength:        1,
				ExpectedResultChecksum: 0x10203040,
			},
		}}
		if _, err := rj.PrepareBatch(scalar); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("PrepareBatch(scalar) = %v, want invalid write", err)
		}
		if _, err := rj.AppendBatch(2, scalar); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("AppendBatch(scalar) = %v, want invalid write", err)
		}
		if _, err := rj.AppendConditionalBatch(
			2, conditional.MarkerID, conditional.MarkerEpoch,
			conditional.TxnID, scalar,
		); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("AppendConditionalBatch(scalar) = %v, want invalid write", err)
		}

		// Plant a checksum-valid kind-3 scalar-patch batch under a conditional
		// header: open/replay must fail closed, not apply the patch.
		scalarRJ, path := createTestJournalFormat(
			t, 8<<10, RecoveryJournalFormatScalarPatch,
		)
		if _, err := scalarRJ.AppendBatch(2, scalar); err != nil {
			t.Fatalf("seed scalar batch: %v", err)
		}
		if err := scalarRJ.Sync(false); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		if err := scalarRJ.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("open for rewrite: %v", err)
		}
		headerBytes := make([]byte, RecoveryJournalHeaderSize)
		if _, err := file.ReadAt(headerBytes, 0); err != nil {
			t.Fatalf("read header: %v", err)
		}
		binary.LittleEndian.PutUint32(
			headerBytes[8:12], RecoveryJournalFormatConditional,
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
			t.Fatalf("rewrite header: %v", err)
		}
		if _, err := OpenRecoveryJournal(file); !errors.Is(
			err, ErrRecoveryJournalRecord,
		) || errors.Is(err, errRecoveryJournalTruncatableTail) {
			t.Fatalf("Open conditional+scalar = %v, want hard record error", err)
		}
		_ = file.Close()
	})

	// Happy path: conditional journal accepts kinds 1/2/3/5.
	t.Run("conditional-journal-accepts-mixed-kinds", func(t *testing.T) {
		rj, path := createTestJournalFormat(
			t, 64<<10, RecoveryJournalFormatConditional,
		)
		if _, err := rj.Append(
			RecoveryRecordKindPut, 2, []byte("solo"), []byte("v"),
		); err != nil {
			t.Fatalf("Append put: %v", err)
		}
		if _, err := rj.AppendBatch(3, []RecoveryBatchEntry{
			{Kind: RecoveryRecordKindPut, Key: []byte("b"), Value: []byte("1")},
			{Kind: RecoveryRecordKindDelete, Key: []byte("d")},
		}); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		if _, err := rj.AppendConditionalBatch(
			4, conditional.MarkerID, conditional.MarkerEpoch,
			conditional.TxnID, entries,
		); err != nil {
			t.Fatalf("AppendConditionalBatch: %v", err)
		}
		if err := rj.Sync(false); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		if err := rj.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		rj = reopenTestJournal(t, path)
		defer rj.Close()
		if rj.Header().FormatVersion != RecoveryJournalFormatConditional {
			t.Fatalf("reopened format = %d, want conditional",
				rj.Header().FormatVersion)
		}
		recs := replayConditionalAll(t, rj, 1)
		if len(recs) != 3 {
			t.Fatalf("replayed %d, want 3", len(recs))
		}
		if recs[0].Kind != RecoveryRecordKindPut ||
			recs[1].Kind != RecoveryRecordKindBatch ||
			recs[2].Kind != RecoveryRecordKindConditionalBatch {
			t.Fatalf("kinds = %d,%d,%d", recs[0].Kind, recs[1].Kind, recs[2].Kind)
		}
		if recs[2].Conditional != conditional || len(recs[2].Entries) != 3 {
			t.Fatalf("conditional record = %+v", recs[2])
		}
	})
}

func formatName(formatVersion uint32) string {
	switch formatVersion {
	case RecoveryJournalFormatLegacy:
		return "legacy"
	case RecoveryJournalFormatScalarPatch:
		return "scalar-patch"
	case RecoveryJournalFormatConditional:
		return "conditional"
	default:
		return "unknown"
	}
}

func rjHeaderFromFile(t *testing.T, file *os.File) RecoveryJournalHeader {
	t.Helper()
	buf := make([]byte, RecoveryJournalHeaderSize)
	if _, err := file.ReadAt(buf, 0); err != nil {
		t.Fatalf("read header: %v", err)
	}
	h, err := DecodeRecoveryJournalHeader(buf)
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	return h
}

// TestRecoveryConditionalBatchTornTailPrefixSweep proves every byte prefix of a
// journal ending in a kind-5 record either decodes a strict complete-record
// prefix or truncates; never a partial conditional record.
func TestRecoveryConditionalBatchTornTailPrefixSweep(t *testing.T) {
	entries := testConditionalEntries()
	conditional := testConditionalHeader(t)
	rj, path := createTestJournalFormat(
		t, 64<<10, RecoveryJournalFormatConditional,
	)
	if _, err := rj.Append(
		RecoveryRecordKindPut, 2, []byte("survivor"), []byte("kept"),
	); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cursorBefore := rj.Cursor()
	if _, err := rj.AppendConditionalBatch(
		3, conditional.MarkerID, conditional.MarkerEpoch,
		conditional.TxnID, entries,
	); err != nil {
		t.Fatalf("AppendConditionalBatch: %v", err)
	}
	if err := rj.Sync(false); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	completeSize := rj.Cursor()
	kind5Size := completeSize - cursorBefore
	if err := rj.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	recordOff := recoveryJournalRegionStart + int(cursorBefore)
	recordEnd := recoveryJournalRegionStart + int(completeSize)
	if recordEnd > len(full) || uint64(kind5Size) == 0 {
		t.Fatalf("record span [%d,%d) kind5=%d file=%d",
			recordOff, recordEnd, kind5Size, len(full))
	}

	// Encode-level sweep: every prefix shorter than the CRC-sealed payload
	// fails closed. Padding bytes after the trailer are not required for
	// decode — the decoder reports the padded size once the seal validates —
	// so success is legal only at or past the sealed length, and never yields
	// a partial entry list.
	record := full[recordOff:recordEnd]
	bodyLen := binary.LittleEndian.Uint32(record[28:32])
	sealedLen := RecoveryJournalRecordPrefixSize + int(bodyLen) +
		RecoveryJournalRecordTrailerSize
	if sealedLen <= 0 || sealedLen > len(record) {
		t.Fatalf("sealedLen %d outside record %d", sealedLen, len(record))
	}
	for n := 0; n < len(record); n++ {
		rec, padded, err := DecodeRecoveryRecord(
			record[:n], RecoveryJournalMinSectorSize, 2,
		)
		if n < sealedLen {
			if err == nil {
				t.Fatalf("unsealed prefix %d decoded as kind %d padded %d",
					n, rec.Kind, padded)
			}
			if !errors.Is(err, ErrRecoveryJournalRecord) {
				t.Fatalf("prefix %d err = %v, want record error", n, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("sealed prefix %d decode: %v", n, err)
		}
		if padded != len(record) ||
			rec.Kind != RecoveryRecordKindConditionalBatch ||
			rec.Conditional != conditional ||
			len(rec.Entries) != len(entries) {
			t.Fatalf("sealed prefix %d = %+v padded %d", n, rec, padded)
		}
	}
	rec, padded, err := DecodeRecoveryRecord(
		record, RecoveryJournalMinSectorSize, 2,
	)
	if err != nil {
		t.Fatalf("full kind-5 decode: %v", err)
	}
	if padded != len(record) ||
		rec.Kind != RecoveryRecordKindConditionalBatch ||
		rec.Conditional != conditional ||
		len(rec.Entries) != len(entries) {
		t.Fatalf("full decode = %+v padded %d", rec, padded)
	}

	// File-level sweep: keep the preallocated file length and rewrite only the
	// kind-5 span so Open can still read the capacity region. Prefixes short of
	// the CRC seal truncate before the conditional record; at or past the seal
	// the record replays whole — never a partial entry list.
	for n := 0; n <= len(record); n++ {
		image := append([]byte(nil), full...)
		copy(image[recordOff:recordOff+n], record[:n])
		clear(image[recordOff+n : recordEnd])
		tmp := path + ".prefix"
		if err := os.WriteFile(tmp, image, 0o600); err != nil {
			t.Fatalf("WriteFile prefix %d: %v", n, err)
		}
		file, err := os.OpenFile(tmp, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("open prefix %d: %v", n, err)
		}
		opened, err := OpenRecoveryJournal(file)
		if err != nil {
			_ = file.Close()
			t.Fatalf("prefix %d open = %v", n, err)
		}
		var got []RecoveryRecord
		replayErr := opened.Replay(1, func(r RecoveryRecord) error {
			if r.Kind == RecoveryRecordKindConditionalBatch {
				if r.Conditional != conditional ||
					len(r.Entries) != len(entries) {
					return errors.New("partial conditional record surfaced")
				}
			}
			got = append(got, RecoveryRecord{
				Kind:        r.Kind,
				Key:         append([]byte(nil), r.Key...),
				Conditional: r.Conditional,
				Entries:     append([]RecoveryBatchEntry(nil), r.Entries...),
			})
			return nil
		})
		_ = opened.Close()
		if replayErr != nil {
			t.Fatalf("prefix %d replay: %v", n, replayErr)
		}
		if len(got) < 1 || got[0].Kind != RecoveryRecordKindPut ||
			string(got[0].Key) != "survivor" {
			t.Fatalf("prefix %d missing survivor: %+v", n, got)
		}
		switch len(got) {
		case 1:
			if n >= sealedLen {
				t.Fatalf("sealed prefix %d replayed only survivor", n)
			}
		case 2:
			if n < sealedLen {
				t.Fatalf("unsealed prefix %d/%d replayed a conditional record",
					n, sealedLen)
			}
			if got[1].Kind != RecoveryRecordKindConditionalBatch ||
				got[1].Conditional != conditional ||
				len(got[1].Entries) != len(entries) {
				t.Fatalf("prefix %d incomplete conditional: %+v", n, got[1])
			}
		default:
			t.Fatalf("prefix %d replayed %d records", n, len(got))
		}
	}
}

// TestRecoveryConditionalBatchRecycleDropsStaleSequence proves stale kind-5
// bytes left after recycle are not decodable as live under the new base
// sequence anchor.
func TestRecoveryConditionalBatchRecycleDropsStaleSequence(t *testing.T) {
	entries := testConditionalEntries()
	conditional := testConditionalHeader(t)
	rj, path := createTestJournalFormat(
		t, 16*RecoveryJournalMinSectorSize,
		RecoveryJournalFormatConditional,
	)
	if _, err := rj.AppendConditionalBatch(
		2, conditional.MarkerID, conditional.MarkerEpoch,
		conditional.TxnID, entries,
	); err != nil {
		t.Fatalf("AppendConditionalBatch: %v", err)
	}
	if err := rj.Sync(false); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	staleSequence := uint64(1)
	if err := rj.Recycle(2, true); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if rj.Cursor() != 0 {
		t.Fatalf("cursor after recycle = %d, want 0", rj.Cursor())
	}
	// Fresh conditional record under the new sequence anchor.
	conditional.TxnID = 99
	if _, err := rj.AppendConditionalBatch(
		3, conditional.MarkerID, conditional.MarkerEpoch,
		conditional.TxnID, entries[:1],
	); err != nil {
		t.Fatalf("post-recycle append: %v", err)
	}
	if err := rj.Sync(false); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := rj.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rj = reopenTestJournal(t, path)
	defer rj.Close()
	recs := replayConditionalAll(t, rj, 2)
	if len(recs) != 1 {
		t.Fatalf("post-recycle replay = %d records, want 1: %+v", len(recs), recs)
	}
	if recs[0].Kind != RecoveryRecordKindConditionalBatch ||
		recs[0].Conditional.TxnID != 99 ||
		recs[0].Sequence == staleSequence {
		t.Fatalf("live record = %+v, stale sequence %d must not decode",
			recs[0], staleSequence)
	}
	if rj.Header().BaseSequence < staleSequence {
		t.Fatalf("base sequence %d did not advance past stale %d",
			rj.Header().BaseSequence, staleSequence)
	}
}

// TestRecoveryConditionalBatchAppendAllocationsMatchBatch proves appending a
// kind-5 record allocates within the same budget as an equivalent kind-3 batch
// on a warm scratch buffer.
func TestRecoveryConditionalBatchAppendAllocationsMatchBatch(t *testing.T) {
	entries := testConditionalEntries()
	conditional := testConditionalHeader(t)

	batchRJ, _ := createTestJournalFormat(
		t, 1<<20, RecoveryJournalFormatConditional,
	)
	defer batchRJ.Close()
	condRJ, _ := createTestJournalFormat(
		t, 1<<20, RecoveryJournalFormatConditional,
	)
	defer condRJ.Close()
	batchRJ.writeAt = func(p []byte, _ int64) (int, error) { return len(p), nil }
	condRJ.writeAt = func(p []byte, _ int64) (int, error) { return len(p), nil }

	// Warm scratch so steady-state appends do not pay the buffer grow.
	if _, err := batchRJ.AppendBatch(2, entries); err != nil {
		t.Fatalf("warm AppendBatch: %v", err)
	}
	if _, err := condRJ.AppendConditionalBatch(
		2, conditional.MarkerID, conditional.MarkerEpoch,
		conditional.TxnID, entries,
	); err != nil {
		t.Fatalf("warm AppendConditionalBatch: %v", err)
	}

	var batchErr, condErr error
	batchAllocs := testing.AllocsPerRun(50, func() {
		batchRJ.cursor = 0
		batchRJ.nextSequence = 1
		_, batchErr = batchRJ.AppendBatch(2, entries)
	})
	if batchErr != nil {
		t.Fatalf("AppendBatch: %v", batchErr)
	}
	condAllocs := testing.AllocsPerRun(50, func() {
		condRJ.cursor = 0
		condRJ.nextSequence = 1
		_, condErr = condRJ.AppendConditionalBatch(
			2, conditional.MarkerID, conditional.MarkerEpoch,
			conditional.TxnID, entries,
		)
	})
	if condErr != nil {
		t.Fatalf("AppendConditionalBatch: %v", condErr)
	}
	if condAllocs > batchAllocs {
		t.Fatalf("conditional append allocations = %.1f, batch = %.1f; want conditional <= batch",
			condAllocs, batchAllocs)
	}
}
