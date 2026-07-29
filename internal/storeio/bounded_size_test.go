package storeio

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"unsafe"
)

func TestCheckedSizeArithmeticRejectsOverflowAndNarrowing(t *testing.T) {
	if got, ok := checkedSizeAdd(math.MaxUint32-1, 1, math.MaxUint32); !ok ||
		got != math.MaxUint32 {
		t.Fatalf("bounded add = (%d, %v), want (%d, true)",
			got, ok, uint64(math.MaxUint32))
	}
	if _, ok := checkedSizeAdd(math.MaxUint32, 1, math.MaxUint32); ok {
		t.Fatal("bounded add accepted a format overflow")
	}
	if got, ok := checkedSizeMul(math.MaxUint16, 2, math.MaxUint32); !ok ||
		got != 2*math.MaxUint16 {
		t.Fatalf("bounded multiply = (%d, %v)", got, ok)
	}
	if _, ok := checkedSizeMul(math.MaxUint32, 2, math.MaxUint32); ok {
		t.Fatal("bounded multiply accepted a format overflow")
	}

	intLimit := uint64(maxIntValue)
	if got, ok := checkedSizeInt(intLimit, math.MaxUint64); !ok ||
		uint64(got) != intLimit {
		t.Fatalf("int boundary = (%d, %v), want (%d, true)",
			got, ok, intLimit)
	}
	if intLimit != math.MaxUint64 {
		if _, ok := checkedSizeInt(intLimit+1, math.MaxUint64); ok {
			t.Fatal("int conversion accepted an architecture overflow")
		}
	}
}

func TestRecoveryRecordSizingRejectsWireAndAddressSpaceOverflow(t *testing.T) {
	padded, ok := checkedRecoveryRecordPadded(
		RecoveryJournalMinSectorSize, 1, 0,
	)
	if !ok || padded != RecoveryJournalMinSectorSize {
		t.Fatalf("small padded record = (%d, %v), want (%d, true)",
			padded, ok, RecoveryJournalMinSectorSize)
	}
	for name, test := range map[string]func() bool{
		"zero sector": func() bool {
			_, ok := checkedRecoveryRecordPadded(0, 1, 0)
			return ok
		},
		"negative key": func() bool {
			_, ok := checkedRecoveryRecordPadded(
				RecoveryJournalMinSectorSize, -1, 0,
			)
			return ok
		},
		"native int overflow": func() bool {
			_, ok := checkedRecoveryRecordPadded(
				RecoveryJournalMinSectorSize, maxIntValue, 1,
			)
			return ok
		},
		"padding overflow": func() bool {
			_, ok := checkedRecoveryPadRaw(
				RecoveryJournalMinSectorSize, uint64(maxIntValue),
			)
			return ok
		},
	} {
		t.Run(name, func(t *testing.T) {
			if test() {
				t.Fatal("unrepresentable record size was accepted")
			}
		})
	}
	if got := RecoveryRecordPaddedSize(0, 1, 0); got != maxIntValue {
		t.Fatalf("invalid public size = %d, want saturated %d",
			got, maxIntValue)
	}
}

func TestRecoveryBatchBodySizingStopsAtUint32Boundary(t *testing.T) {
	limit := uint64(math.MaxUint32)
	body := limit - RecoveryBatchEntryHeaderSize - 1
	if got, ok := checkedRecoveryBatchBodyAdd(body, 1, 0); !ok ||
		got != limit {
		t.Fatalf("exact boundary = (%d, %v), want (%d, true)",
			got, ok, limit)
	}
	if _, ok := checkedRecoveryBatchBodyAdd(body+1, 1, 0); ok {
		t.Fatal("batch body accepted a uint32 overflow")
	}
	if _, ok := checkedRecoveryBatchBodyAdd(0, 0, 0); ok {
		t.Fatal("batch body accepted an empty key")
	}
	if _, ok := checkedRecoveryBatchBodyAdd(0, limit+1, 0); ok {
		t.Fatal("batch body accepted an over-wide key")
	}
}

func TestRecoveryBatchDecodeBoundsCountBeforeAllocation(t *testing.T) {
	frame := make([]byte,
		RecoveryJournalRecordPrefixSize+RecoveryJournalRecordTrailerSize)
	binary.LittleEndian.PutUint32(frame[0:4], recoveryRecordMagic)
	binary.LittleEndian.PutUint16(frame[4:6], recoveryRecordKindBatch)
	binary.LittleEndian.PutUint64(frame[8:16], 1)
	binary.LittleEndian.PutUint64(frame[16:24], 1)
	binary.LittleEndian.PutUint32(frame[24:28], math.MaxUint32)
	// bodyLen is zero: the claimed count cannot possibly fit. Seal the frame so
	// rejection is specifically the allocation preflight, not a checksum miss.
	checksum := PageChecksum(frame[:RecoveryJournalRecordPrefixSize])
	binary.LittleEndian.PutUint32(
		frame[RecoveryJournalRecordPrefixSize:], checksum,
	)
	binary.LittleEndian.PutUint32(
		frame[RecoveryJournalRecordPrefixSize+4:], ^checksum,
	)

	if _, _, err := DecodeRecoveryRecord(
		frame, RecoveryJournalMinSectorSize, 1,
	); !errors.Is(err, ErrRecoveryJournalRecord) {
		t.Fatalf("oversized batch count = %v, want ErrRecoveryJournalRecord", err)
	}
}

func TestRecoveryBatchEntryArenaChecksElementWidth(t *testing.T) {
	entryBytes := uint64(unsafe.Sizeof(RecoveryBatchEntry{}))
	overflowing := uint64(maxIntValue)/entryBytes + 1
	if recoveryBatchEntryArenaFits(overflowing) {
		t.Fatalf("entry arena accepted %d elements of %d bytes",
			overflowing, entryBytes)
	}
}

func TestRepresentableRecordLargerThanJournalReportsFull(t *testing.T) {
	valueBytes := int(RecoveryJournalMaxCapacityBytes)
	raw := RecoveryJournalRecordPrefixSize + 1 + valueBytes +
		RecoveryJournalRecordTrailerSize
	sector := int(RecoveryJournalMinSectorSize)
	wantPadded := (raw + sector - 1) / sector * sector
	if got := RecoveryRecordPaddedSize(
		RecoveryJournalMinSectorSize, 1, valueBytes,
	); got != wantPadded {
		t.Fatalf("large representable record size = %d, want %d",
			got, wantPadded)
	}

	rj, _ := createTestJournal(t, uint64(RecoveryJournalMinSectorSize))
	defer rj.Close()
	if rj.Fits(1, valueBytes) {
		t.Fatal("record larger than the journal reported that it fits")
	}
	if _, err := rj.Append(
		recoveryRecordKindPut, 2, []byte("k"), make([]byte, valueBytes),
	); !errors.Is(err, ErrRecoveryJournalFull) {
		t.Fatalf("oversized append = %v, want ErrRecoveryJournalFull", err)
	}
}
