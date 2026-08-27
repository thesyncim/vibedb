package storeio

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"
)

func TestRecoveryJournalReplayImpossibleTailLengthsStayWindowBounded(t *testing.T) {
	journal, _ := createTestJournal(t, 16<<20)
	defer journal.Close()
	value := bytes.Repeat([]byte{'v'}, recoveryReadWindowBytes)
	if _, err := journal.Append(RecoveryRecordKindPut, 2, []byte("key"), value); err != nil {
		t.Fatal(err)
	}
	// A recycled or interrupted suffix is not guaranteed to have zero length
	// words. Neither candidate checksum can exist at these advertised offsets.
	var tail [RecoveryJournalRecordPrefixSize + RecoveryJournalRecordTrailerSize]byte
	binary.LittleEndian.PutUint32(tail[:4], recoveryRecordMagic)
	binary.LittleEndian.PutUint16(tail[4:6], RecoveryRecordKindPut)
	binary.LittleEndian.PutUint64(tail[8:16], 2)
	binary.LittleEndian.PutUint64(tail[16:24], 3)
	binary.LittleEndian.PutUint32(tail[24:28], ^uint32(0))
	binary.LittleEndian.PutUint32(tail[28:32], ^uint32(0))
	if _, err := journal.file.WriteAt(tail[:], int64(recoveryJournalRegionStart)+int64(journal.Cursor())); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 5 {
		count := 0
		if err := journal.Replay(1, func(record RecoveryRecord) error {
			count++
			if record.Generation != 2 || !bytes.Equal(record.Value, value) {
				t.Fatal("torn suffix changed admitted prefix")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("replayed %d records", count)
		}
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("five exact-prefix replays with impossible suffix lengths allocated %d bytes", allocated)
	if allocated > 512<<10 {
		t.Fatalf("impossible tail lengths allocated %d bytes for tiny live window", allocated)
	}
}

func TestRecoveryJournalStreamPossibleChecksumEndpointsMatchWholeDecoder(t *testing.T) {
	journal, _ := createTestJournal(t, 1<<20)
	defer journal.Close()
	for _, test := range []struct {
		name                string
		kind                uint16
		keyBytes, bodyBytes uint32
		sealBatch           bool
	}{
		{"both impossible", RecoveryRecordKindPut, ^uint32(0), ^uint32(0), false},
		{"standalone impossible batch possible", RecoveryRecordKindPut, ^uint32(0), 32, false},
		{"authenticated alternate batch", RecoveryRecordKindPut, ^uint32(0), 32, true},
		{"authenticated unknown kind", 0x7777, ^uint32(0), 32, true},
		{"authenticated batch oversized count", RecoveryRecordKindBatch, ^uint32(0), 32, true},
		{"both endpoint boundary", RecoveryRecordKindPut, 7, (1 << 20) - 47, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			region := make([]byte, journal.header.Capacity)
			binary.LittleEndian.PutUint32(region[:4], recoveryRecordMagic)
			binary.LittleEndian.PutUint16(region[4:6], test.kind)
			binary.LittleEndian.PutUint64(region[8:16], 1)
			binary.LittleEndian.PutUint64(region[16:24], 2)
			binary.LittleEndian.PutUint32(region[24:28], test.keyBytes)
			binary.LittleEndian.PutUint32(region[28:32], test.bodyBytes)
			if test.sealBatch {
				end := RecoveryJournalRecordPrefixSize + int(test.bodyBytes)
				crc := PageChecksum(region[:end])
				binary.LittleEndian.PutUint32(region[end:end+4], crc)
				binary.LittleEndian.PutUint32(region[end+4:end+8], ^crc)
			}
			if _, err := journal.file.WriteAt(region, recoveryJournalRegionStart); err != nil {
				t.Fatal(err)
			}
			_, wantPadded, wantErr := DecodeRecoveryRecord(region, journal.header.SectorSize, 1)
			var stream recoveryRecordStream
			if err := stream.open(journal.file, journal.header.Capacity); err != nil {
				t.Fatal(err)
			}
			_, gotPadded, gotErr := stream.record(0, journal.header.SectorSize, 1)
			if !sameRecoveryStreamError(gotErr, wantErr) || gotPadded != wantPadded {
				t.Fatalf("stream=(%d,%v) whole=(%d,%v)", gotPadded, gotErr, wantPadded, wantErr)
			}
			if test.bodyBytes == 32 || test.bodyBytes == ^uint32(0) {
				if cap(stream.buffer) > recoveryReadWindowBytes {
					t.Fatalf("impossible endpoint allocated %d bytes", cap(stream.buffer))
				}
			}
		})
	}
}
