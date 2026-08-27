package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"testing"
)

func TestRecoveryJournalStreamMatchesWholeRegionAcrossWindows(t *testing.T) {
	rj, _ := createTestJournal(t, 1<<20)
	defer rj.Close()
	for i := range 180 {
		n := 733
		if i == 89 {
			n = 3*recoveryReadWindowBytes + 13
		}
		if _, err := rj.Append(RecoveryRecordKindPut, uint64(i+2),
			[]byte(fmt.Sprintf("key-%03d", i)), bytes.Repeat([]byte{byte(i)}, n)); err != nil {
			t.Fatal(err)
		}
	}
	region := make([]byte, rj.header.Capacity)
	if _, err := readFullAt(rj.file, region, recoveryJournalRegionStart); err != nil {
		t.Fatal(err)
	}
	var stream recoveryRecordStream
	if err := stream.open(rj.file, rj.header.Capacity); err != nil {
		t.Fatal(err)
	}
	var cursor uint64
	for sequence := uint64(1); ; sequence++ {
		want, wantPadded, wantErr := DecodeRecoveryRecord(region[cursor:], rj.header.SectorSize, sequence)
		got, gotPadded, gotErr := stream.record(cursor, rj.header.SectorSize, sequence)
		if !sameRecoveryStreamError(gotErr, wantErr) || gotPadded != wantPadded || !reflect.DeepEqual(got, want) {
			t.Fatalf("sequence %d streamed=(%d,%v), whole=(%d,%v)", sequence, gotPadded, gotErr, wantPadded, wantErr)
		}
		if cap(stream.buffer) > 4*recoveryReadWindowBytes {
			t.Fatalf("stream reserved %d bytes for sub-256KiB records", cap(stream.buffer))
		}
		if wantErr != nil {
			break
		}
		cursor += uint64(wantPadded)
	}
	if cursor != rj.cursor {
		t.Fatalf("stream cursor=%d journal cursor=%d", cursor, rj.cursor)
	}
}

func sameRecoveryStreamError(got, want error) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	return errors.Is(got, errRecoveryJournalTruncatableTail) == errors.Is(want, errRecoveryJournalTruncatableTail) &&
		errors.Is(got, ErrRecoveryJournalRecord) == errors.Is(want, ErrRecoveryJournalRecord)
}

func TestRecoveryJournalStreamPreservesAlternateLayoutAuthentication(t *testing.T) {
	rj, _ := createTestJournal(t, 512<<10)
	defer rj.Close()
	for _, batch := range []bool{false, true} {
		value := bytes.Repeat([]byte{'x'}, recoveryReadWindowBytes+11)
		if batch {
			if _, err := rj.AppendBatch(2, []RecoveryBatchEntry{{Kind: RecoveryRecordKindPut, Key: []byte("key"), Value: value}}); err != nil {
				t.Fatal(err)
			}
		} else if _, err := rj.Append(RecoveryRecordKindPut, 2, []byte("key"), value); err != nil {
			t.Fatal(err)
		}
		original := make([]byte, rj.header.Capacity)
		if _, err := readFullAt(rj.file, original, recoveryJournalRegionStart); err != nil {
			t.Fatal(err)
		}
		bodyEnd := RecoveryJournalRecordPrefixSize + int(binary.LittleEndian.Uint32(original[28:32]))
		if !batch {
			bodyEnd += int(binary.LittleEndian.Uint32(original[24:28]))
		}
		for _, damage := range []struct {
			name   string
			change func([]byte)
		}{
			{"magic", func(b []byte) { binary.LittleEndian.PutUint32(b[:4], 0x12345678) }},
			{"zero_magic", func(b []byte) { clear(b[:4]) }},
			{"kind", func(b []byte) { binary.LittleEndian.PutUint16(b[4:6], 0x7777) }},
			{"other_layout", func(b []byte) {
				kind := uint16(recoveryRecordKindBatch)
				if batch {
					kind = recoveryRecordKindPut
				}
				binary.LittleEndian.PutUint16(b[4:6], kind)
			}},
			{"reserved", func(b []byte) { b[6] = 1 }},
			{"zero_sequence", func(b []byte) { clear(b[8:16]) }},
			{"zero_generation", func(b []byte) { clear(b[16:24]) }},
			{"oversized_length", func(b []byte) { binary.LittleEndian.PutUint32(b[28:32], ^uint32(0)) }},
		} {
			for _, reseal := range []bool{false, true} {
				candidate := bytes.Clone(original)
				damage.change(candidate)
				if reseal {
					crc := PageChecksum(candidate[:bodyEnd])
					binary.LittleEndian.PutUint32(candidate[bodyEnd:bodyEnd+4], crc)
					binary.LittleEndian.PutUint32(candidate[bodyEnd+4:bodyEnd+8], ^crc)
				}
				if _, err := rj.file.WriteAt(candidate, recoveryJournalRegionStart); err != nil {
					t.Fatal(err)
				}
				var stream recoveryRecordStream
				if err := stream.open(rj.file, rj.header.Capacity); err != nil {
					t.Fatal(err)
				}
				want, padded, wantErr := DecodeRecoveryRecord(candidate, rj.header.SectorSize, 1)
				got, gotPadded, gotErr := stream.record(0, rj.header.SectorSize, 1)
				if !sameRecoveryStreamError(gotErr, wantErr) || padded != gotPadded || !reflect.DeepEqual(got, want) {
					t.Fatalf("batch=%t damage=%s reseal=%t streamed=%v whole=%v", batch, damage.name, reseal, gotErr, wantErr)
				}
			}
		}
		// Reset fixture bytes and manager state without claiming a durability
		// transition: this test compares decoder behavior, not journal recycle.
		clear(original)
		if _, err := rj.file.WriteAt(original, recoveryJournalRegionStart); err != nil {
			t.Fatal(err)
		}
		rj.cursor, rj.nextSequence = 0, 1
		rj.family = recoveryRecordFamilyEmpty
		rj.atomicLastGeneration, rj.atomicLastKind = 0, 0
	}
}

func TestRecoveryJournalStreamRejectsTruncatedUnusedSuffixBeforeReplay(t *testing.T) {
	rj, _ := createTestJournal(t, 1<<20)
	defer rj.Close()
	appendPut(t, rj, 2, "key", "value")
	if err := rj.file.Truncate(int64(recoveryJournalRegionStart) + int64(rj.header.Capacity) - 1); err != nil {
		t.Fatal(err)
	}
	if err := rj.Replay(1, func(RecoveryRecord) error {
		t.Fatal("truncated journal exposed a replay record")
		return nil
	}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Replay error=%v", err)
	}
	if err := rj.scanTail(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("scan error=%v", err)
	}
}

func TestRecoveryJournalSmallLiveWindowDoesNotMaterializeCapacity(t *testing.T) {
	rj, _ := createTestJournal(t, 16<<20)
	defer rj.Close()
	appendPut(t, rj, 2, "key", "value")
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if err := rj.scanTail(); err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := rj.Replay(1, func(rec RecoveryRecord) error {
		count++
		if !bytes.Equal(rec.Key, []byte("key")) || !bytes.Equal(rec.Value, []byte("value")) {
			t.Fatal("replay changed row")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if count != 1 {
		t.Fatalf("replayed %d rows", count)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4*recoveryReadWindowBytes {
		t.Fatalf("tiny scan+replay allocated %d bytes for a %d-byte journal", allocated, rj.header.Capacity)
	}
}
