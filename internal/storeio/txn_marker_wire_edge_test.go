package storeio

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"
)

func wireEdgeSealTxnMarkerHeader(header []byte) {
	checksum := PageChecksum(header[:TxnMarkerHeaderSize-8])
	binary.LittleEndian.PutUint32(
		header[TxnMarkerHeaderSize-8:TxnMarkerHeaderSize-4], checksum,
	)
	binary.LittleEndian.PutUint32(header[TxnMarkerHeaderSize-4:], ^checksum)
}

func wireEdgeSealTxnMarkerRecord(record []byte, bodyEnd int) {
	checksum := PageChecksum(record[:bodyEnd])
	binary.LittleEndian.PutUint32(record[bodyEnd:bodyEnd+4], checksum)
	binary.LittleEndian.PutUint32(record[bodyEnd+4:bodyEnd+8], ^checksum)
}

func wireEdgeRewriteTxnMarkerBaseSequence(
	t *testing.T, path string, header TxnMarkerHeader, baseSequence uint64,
) {
	t.Helper()
	header.BaseSequence = baseSequence
	encoded := make([]byte, TxnMarkerHeaderSize)
	if _, err := EncodeTxnMarkerHeader(encoded, header); err != nil {
		t.Fatalf("EncodeTxnMarkerHeader: %v", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open marker for header rewrite: %v", err)
	}
	for slot := 0; slot < txnMarkerHeaderSlots; slot++ {
		if _, err := file.WriteAt(encoded, int64(slot*TxnMarkerHeaderSize)); err != nil {
			_ = file.Close()
			t.Fatalf("write marker header slot %d: %v", slot, err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close marker header rewrite: %v", err)
	}
}

func wireEdgeTxnDecisionWithSequence(
	t *testing.T, sequence, txnID uint64, targets []TxnCollectionRef) []byte {
	t.Helper()
	padded, ok := checkedTxnDecisionPaddedSize(len(targets))
	if !ok {
		t.Fatal("checkedTxnDecisionPaddedSize rejected test participants")
	}
	record := make([]byte, padded)
	if _, err := encodeTxnDecisionRecord(
		record, sequence, txnID, targets,
	); err != nil {
		t.Fatalf("encodeTxnDecisionRecord: %v", err)
	}
	return record
}

func wireEdgeTxnDecisionWithZeroSequence(
	t *testing.T, txnID uint64,
) []byte {
	t.Helper()
	targets := testTxnCollectionRefs(1)
	record := wireEdgeTxnDecisionWithSequence(t, 1, txnID, targets)
	binary.LittleEndian.PutUint64(record[8:16], 0)
	bodyEnd := TxnMarkerRecordPrefixSize + len(targets)*TxnCollectionRefSize
	wireEdgeSealTxnMarkerRecord(record, bodyEnd)
	return record
}

func wireEdgeTxnRetirementWithZeroSequence(t *testing.T) []byte {
	t.Helper()
	storeID := testTxnCollectionRefs(1)[0].StoreID
	padded, ok := checkedTxnRetirementPaddedSize()
	if !ok {
		t.Fatal("checkedTxnRetirementPaddedSize")
	}
	record := make([]byte, padded)
	if _, err := encodeTxnRetirementRecord(record, 1, storeID); err != nil {
		t.Fatalf("encodeTxnRetirementRecord: %v", err)
	}
	binary.LittleEndian.PutUint64(record[8:16], 0)
	wireEdgeSealTxnMarkerRecord(record, TxnMarkerRecordPrefixSize)
	return record
}

func TestTxnMarkerSequenceExhaustion(t *testing.T) {
	t.Run("terminal sequence is admitted once", func(t *testing.T) {
		marker, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
		header := marker.Header()
		if err := marker.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		wireEdgeRewriteTxnMarkerBaseSequence(t, path, header, ^uint64(0)-1)

		marker, _, err := OpenTxnMarker(path, TxnMarkerOptions{})
		if err != nil {
			t.Fatalf("OpenTxnMarker: %v", err)
		}
		dcsn, err := marker.AppendDecision(1, testTxnCollectionRefs(1))
		if err != nil {
			t.Fatalf("terminal AppendDecision: %v", err)
		}
		if dcsn != ^uint64(0) || marker.NextSequence() != 0 {
			t.Fatalf(
				"terminal decision dcsn/next = %d/%d, want %d/0",
				dcsn, marker.NextSequence(), ^uint64(0),
			)
		}
		terminalCursor := marker.Cursor()
		if _, err := marker.AppendDecision(
			2, testTxnCollectionRefs(1),
		); !errors.Is(err, ErrTxnMarkerFull) {
			t.Fatalf("post-exhaustion AppendDecision = %v, want ErrTxnMarkerFull", err)
		}
		retiredStore := testTxnCollectionRefs(2)[1].StoreID
		if _, err := marker.AppendRetirement(retiredStore); !errors.Is(
			err, ErrTxnMarkerFull,
		) {
			t.Fatalf("post-exhaustion AppendRetirement = %v, want ErrTxnMarkerFull", err)
		}
		if marker.Cursor() != terminalCursor || marker.NextSequence() != 0 {
			t.Fatalf(
				"failed decision changed cursor/sequence = %d/%d, want %d/0",
				marker.Cursor(), marker.NextSequence(), terminalCursor,
			)
		}
		if err := marker.Close(); err != nil {
			t.Fatalf("close terminal marker: %v", err)
		}

		// The terminal record is followed by an authenticated sequence-0 record.
		// A correct scanner stops after MaxUint64 and never examines this tail.
		zero := wireEdgeTxnDecisionWithZeroSequence(t, 2)
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("open marker for forged tail: %v", err)
		}
		if _, err := file.WriteAt(
			zero, int64(txnMarkerRegionStart+terminalCursor),
		); err != nil {
			_ = file.Close()
			t.Fatalf("write forged sequence-0 tail: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close forged marker tail: %v", err)
		}

		reopened, decisions, err := OpenTxnMarker(path, TxnMarkerOptions{})
		if err != nil {
			t.Fatalf("reopen marker after terminal record: %v", err)
		}
		defer reopened.Close()
		if reopened.Cursor() != terminalCursor || reopened.NextSequence() != 0 {
			t.Fatalf(
				"reopened cursor/sequence = %d/%d, want %d/0",
				reopened.Cursor(), reopened.NextSequence(), terminalCursor,
			)
		}
		if decisions.MaxDCSN() != ^uint64(0) {
			t.Fatalf("MaxDCSN = %d, want %d", decisions.MaxDCSN(), ^uint64(0))
		}
		if _, ok := decisions.Lookup(header.MarkerID, header.Epoch, 1); !ok {
			t.Fatal("terminal decision missing after reopen")
		}
		if _, ok := decisions.Lookup(header.MarkerID, header.Epoch, 2); ok {
			t.Fatal("scanner admitted sequence-0 record after terminal decision")
		}
	})

	t.Run("exhausted base ignores stale region and decoder rejects sequence zero", func(t *testing.T) {
		marker, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
		header := marker.Header()
		if err := marker.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		wireEdgeRewriteTxnMarkerBaseSequence(t, path, header, ^uint64(0))

		empty, decisions, err := OpenTxnMarker(path, TxnMarkerOptions{})
		if err != nil {
			t.Fatalf("open empty exhausted marker: %v", err)
		}
		if empty.Cursor() != 0 || empty.NextSequence() != 0 ||
			decisions.MaxDCSN() != 0 {
			t.Fatalf(
				"empty exhausted cursor/sequence/max = %d/%d/%d, want 0/0/0",
				empty.Cursor(), empty.NextSequence(), decisions.MaxDCSN(),
			)
		}
		if _, err := empty.AppendDecision(
			1, testTxnCollectionRefs(1),
		); !errors.Is(err, ErrTxnMarkerFull) {
			t.Fatalf("exhausted AppendDecision = %v, want ErrTxnMarkerFull", err)
		}
		storeID := testTxnCollectionRefs(2)[1].StoreID
		if _, err := empty.AppendRetirement(storeID); !errors.Is(err, ErrTxnMarkerFull) {
			t.Fatalf("exhausted AppendRetirement = %v, want ErrTxnMarkerFull", err)
		}
		if empty.Cursor() != 0 || empty.NextSequence() != 0 {
			t.Fatalf(
				"failed appends changed cursor/sequence = %d/%d, want 0/0",
				empty.Cursor(), empty.NextSequence(),
			)
		}
		if err := empty.Close(); err != nil {
			t.Fatalf("close empty exhausted marker: %v", err)
		}

		for name, record := range map[string][]byte{
			"decision":   wireEdgeTxnDecisionWithZeroSequence(t, 1),
			"retirement": wireEdgeTxnRetirementWithZeroSequence(t),
		} {
			t.Run(name, func(t *testing.T) {
				if _, _, err := decodeTxnMarkerRecord(
					record, 0,
				); !errors.Is(err, ErrTxnMarkerRecord) ||
					errors.Is(err, errTxnMarkerTruncatableTail) {
					t.Fatalf("decode sequence 0 = %v, want hard record error", err)
				}
			})
		}

		zero := wireEdgeTxnDecisionWithZeroSequence(t, 1)
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("open marker for sequence-0 record: %v", err)
		}
		if _, err := file.WriteAt(zero, txnMarkerRegionStart); err != nil {
			_ = file.Close()
			t.Fatalf("write sequence-0 marker record: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close sequence-0 marker file: %v", err)
		}
		opened, staleDecisions, err := OpenTxnMarker(path, TxnMarkerOptions{})
		if err != nil {
			t.Fatalf("open exhausted marker with stale current-magic bytes: %v", err)
		}
		defer opened.Close()
		if opened.Cursor() != 0 || opened.NextSequence() != 0 ||
			staleDecisions.MaxDCSN() != 0 {
			t.Fatalf(
				"stale-region open cursor/sequence/max = %d/%d/%d, want 0/0/0",
				opened.Cursor(), opened.NextSequence(), staleDecisions.MaxDCSN(),
			)
		}
	})
}

func TestTxnMarkerEqualRecycleCountConflictFailsClosed(t *testing.T) {
	marker, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
	header := marker.Header()
	if err := marker.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	conflict := header
	conflict.MarkerID[0] ^= 0xff
	encoded := make([]byte, TxnMarkerHeaderSize)
	if _, err := EncodeTxnMarkerHeader(encoded, conflict); err != nil {
		t.Fatalf("EncodeTxnMarkerHeader conflict: %v", err)
	}
	if decoded, err := DecodeTxnMarkerHeader(encoded); err != nil || decoded != conflict {
		t.Fatalf("conflicting slot is not independently valid: decoded=%+v err=%v", decoded, err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for conflicting marker slot: %v", err)
	}
	if _, err := file.WriteAt(encoded, TxnMarkerHeaderSize); err != nil {
		_ = file.Close()
		t.Fatalf("write conflicting marker slot: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close conflicting marker slot: %v", err)
	}
	if opened, _, err := OpenTxnMarker(
		path, TxnMarkerOptions{},
	); !errors.Is(err, ErrTxnMarkerCorrupt) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("equal-count conflicting marker headers = %v, want corrupt", err)
	}
}

func TestTxnMarkerAuthenticatedInvalidNewestHeaderCannotResurrectEpoch(t *testing.T) {
	marker, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
	if _, err := marker.AppendDecision(1, testTxnCollectionRefs(1)); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}
	if err := marker.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := marker.Recycle(2); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	newestSlot := marker.headerSlot
	if marker.Header().Epoch != 2 || newestSlot == 0 {
		t.Fatalf(
			"recycled marker epoch/slot = %d/%d, want 2/nonzero",
			marker.Header().Epoch, newestSlot,
		)
	}
	if err := marker.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for header corruption: %v", err)
	}
	newest := make([]byte, TxnMarkerHeaderSize)
	if _, err := file.ReadAt(
		newest, int64(newestSlot)*TxnMarkerHeaderSize,
	); err != nil {
		_ = file.Close()
		t.Fatalf("read newest marker header: %v", err)
	}
	// Reserved byte 68 is authenticated and has no current meaning.
	newest[68] = 1
	wireEdgeSealTxnMarkerHeader(newest)
	if !txnMarkerHeaderAuthenticated(newest) {
		_ = file.Close()
		t.Fatal("test marker header is not checksum-authenticated")
	}
	if _, err := DecodeTxnMarkerHeader(newest); !errors.Is(
		err, ErrTxnMarkerCorrupt,
	) {
		_ = file.Close()
		t.Fatalf("semantic-invalid newest marker decode = %v, want corrupt", err)
	}
	if _, err := file.WriteAt(
		newest, int64(newestSlot)*TxnMarkerHeaderSize,
	); err != nil {
		_ = file.Close()
		t.Fatalf("write newest marker header: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupted marker: %v", err)
	}

	opened, _, err := OpenTxnMarker(path, TxnMarkerOptions{})
	if opened != nil {
		_ = opened.Close()
	}
	if !errors.Is(err, ErrTxnMarkerCorrupt) {
		t.Fatalf(
			"open with authenticated invalid newest marker header = %v, want corrupt",
			err,
		)
	}
}

func TestTxnMarkerEpochZeroRejected(t *testing.T) {
	marker, _ := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
	header := marker.Header()
	defer marker.Close()

	invalid := header
	invalid.Epoch = 0
	encoded := make([]byte, TxnMarkerHeaderSize)
	if _, err := EncodeTxnMarkerHeader(
		encoded, invalid,
	); !errors.Is(err, ErrTxnMarkerCorrupt) {
		t.Fatalf("EncodeTxnMarkerHeader epoch 0 = %v, want corrupt", err)
	}
	if _, err := EncodeTxnMarkerHeader(encoded, header); err != nil {
		t.Fatalf("EncodeTxnMarkerHeader valid: %v", err)
	}
	binary.LittleEndian.PutUint64(encoded[32:40], 0)
	wireEdgeSealTxnMarkerHeader(encoded)
	if _, err := DecodeTxnMarkerHeader(encoded); !errors.Is(err, ErrTxnMarkerCorrupt) {
		t.Fatalf("DecodeTxnMarkerHeader checksum-valid epoch 0 = %v, want corrupt", err)
	}
}

func wireEdgeTxnSemanticDecisions(t *testing.T) map[string][]byte {
	t.Helper()
	targets := testTxnCollectionRefs(2)
	zeroTxnID := wireEdgeTxnDecisionWithSequence(t, 1, 1, targets)
	binary.LittleEndian.PutUint64(zeroTxnID[16:24], 0)
	bodyEnd := TxnMarkerRecordPrefixSize + len(targets)*TxnCollectionRefSize
	wireEdgeSealTxnMarkerRecord(zeroTxnID, bodyEnd)

	duplicateTarget := wireEdgeTxnDecisionWithSequence(t, 1, 1, targets)
	firstTarget := TxnMarkerRecordPrefixSize
	secondTarget := firstTarget + TxnCollectionRefSize
	copy(
		duplicateTarget[secondTarget:secondTarget+16],
		duplicateTarget[firstTarget:firstTarget+16],
	)
	wireEdgeSealTxnMarkerRecord(duplicateTarget, bodyEnd)

	return map[string][]byte{
		"zero-txn-id":           zeroTxnID,
		"duplicate-participant": duplicateTarget,
	}
}

func TestTxnMarkerChecksumValidSemanticDecisionErrorsAreHard(t *testing.T) {
	for name, record := range wireEdgeTxnSemanticDecisions(t) {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeTxnMarkerRecord(
				record, 1,
			); !errors.Is(err, ErrTxnMarkerRecord) ||
				errors.Is(err, errTxnMarkerTruncatableTail) {
				t.Fatalf("decode semantic decision = %v, want hard record error", err)
			}

			marker, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
			if err := marker.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatalf("open for semantic decision: %v", err)
			}
			if _, err := file.WriteAt(record, txnMarkerRegionStart); err != nil {
				_ = file.Close()
				t.Fatalf("write semantic decision: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close semantic decision file: %v", err)
			}
			if opened, _, err := OpenTxnMarker(
				path, TxnMarkerOptions{},
			); !errors.Is(err, ErrTxnMarkerRecord) ||
				errors.Is(err, errTxnMarkerTruncatableTail) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatalf("OpenTxnMarker semantic decision = %v, want hard error", err)
			}
		})
	}
}

func TestTxnMarkerAuthenticatedCrossLayoutKindsAreHard(t *testing.T) {
	targets := testTxnCollectionRefs(2)
	decisionAsRetirement := wireEdgeTxnDecisionWithSequence(
		t, 1, 1, targets,
	)
	binary.LittleEndian.PutUint16(
		decisionAsRetirement[4:6], TxnMarkerRecordKindRetirement,
	)
	wireEdgeSealTxnMarkerRecord(
		decisionAsRetirement,
		TxnMarkerRecordPrefixSize+len(targets)*TxnCollectionRefSize,
	)

	retirementSize, ok := checkedTxnRetirementPaddedSize()
	if !ok {
		t.Fatal("checkedTxnRetirementPaddedSize")
	}
	retirementAsDecision := make([]byte, retirementSize)
	if _, err := encodeTxnRetirementRecord(
		retirementAsDecision, 1, targets[0].StoreID,
	); err != nil {
		t.Fatalf("encodeTxnRetirementRecord: %v", err)
	}
	binary.LittleEndian.PutUint16(
		retirementAsDecision[4:6], TxnMarkerRecordKindDecision,
	)
	wireEdgeSealTxnMarkerRecord(
		retirementAsDecision, TxnMarkerRecordPrefixSize,
	)

	for name, record := range map[string][]byte{
		"decision layout with retirement kind": decisionAsRetirement,
		"retirement layout with decision kind": retirementAsDecision,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeTxnMarkerRecord(
				record, 1,
			); !errors.Is(err, ErrTxnMarkerRecord) ||
				errors.Is(err, errTxnMarkerTruncatableTail) {
				t.Fatalf("decodeTxnMarkerRecord = %v, want hard record error", err)
			}

			marker, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
			if err := marker.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatalf("open for cross-layout marker record: %v", err)
			}
			if _, err := file.WriteAt(record, txnMarkerRegionStart); err != nil {
				_ = file.Close()
				t.Fatalf("write cross-layout marker record: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close cross-layout marker record: %v", err)
			}
			opened, _, err := OpenTxnMarker(path, TxnMarkerOptions{})
			if opened != nil {
				_ = opened.Close()
			}
			if !errors.Is(err, ErrTxnMarkerRecord) ||
				errors.Is(err, errTxnMarkerTruncatableTail) {
				t.Fatalf("OpenTxnMarker = %v, want hard record error", err)
			}
		})
	}
}

func TestTxnMarkerDecisionLogicalRecordBeforePaddingMayReplay(t *testing.T) {
	marker, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
	header := marker.Header()
	targets := testTxnCollectionRefs(2)
	logicalLen := TxnMarkerRecordPrefixSize + len(targets)*TxnCollectionRefSize +
		TxnMarkerRecordTrailerSize
	padded, ok := checkedTxnDecisionPaddedSize(len(targets))
	if !ok || logicalLen >= padded {
		t.Fatalf("logical/padded decision lengths = %d/%d", logicalLen, padded)
	}
	marker.writeAt = func(payload []byte, offset int64) (int, error) {
		return marker.file.WriteAt(payload[:logicalLen], offset)
	}
	if dcsn, err := marker.AppendDecision(7, targets); dcsn != 0 ||
		!errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("AppendDecision = dcsn %d, err %v; want 0, io.ErrShortWrite", dcsn, err)
	}
	if marker.Cursor() != 0 || marker.NextSequence() != 1 {
		t.Fatalf(
			"short decision changed cursor/sequence = %d/%d, want 0/1",
			marker.Cursor(), marker.NextSequence(),
		)
	}
	if err := marker.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, decisions, err := OpenTxnMarker(path, TxnMarkerOptions{})
	if err != nil {
		t.Fatalf("OpenTxnMarker: %v", err)
	}
	defer reopened.Close()
	if reopened.Cursor() != uint64(padded) || reopened.NextSequence() != 2 {
		t.Fatalf(
			"reopened cursor/sequence = %d/%d, want %d/2",
			reopened.Cursor(), reopened.NextSequence(), padded,
		)
	}
	got, found := decisions.Lookup(header.MarkerID, header.Epoch, 7)
	if !found || len(got) != len(targets) {
		t.Fatalf("reopened decision = found %v participants %d, want true/%d", found, len(got), len(targets))
	}
}
