package storeio

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"
)

func TestOpenTxnMarkerRejectsAuthenticatedUnknownKinds(t *testing.T) {
	targets := testTxnCollectionRefs(2)
	decision := wireEdgeTxnDecisionWithSequence(t, 1, 1, targets)
	binary.LittleEndian.PutUint16(decision[4:6], 0x7fff)
	wireEdgeSealTxnMarkerRecord(
		decision, TxnMarkerRecordPrefixSize+len(targets)*TxnCollectionRefSize,
	)

	retirementSize, ok := checkedTxnRetirementPaddedSize()
	if !ok {
		t.Fatal("checkedTxnRetirementPaddedSize")
	}
	retirement := make([]byte, retirementSize)
	if _, err := encodeTxnRetirementRecord(
		retirement, 1, targets[0].StoreID,
	); err != nil {
		t.Fatalf("encodeTxnRetirementRecord: %v", err)
	}
	binary.LittleEndian.PutUint16(retirement[4:6], 0x7fff)
	wireEdgeSealTxnMarkerRecord(retirement, TxnMarkerRecordPrefixSize)

	for name, record := range map[string][]byte{
		"decision layout":   decision,
		"retirement layout": retirement,
	} {
		t.Run(name, func(t *testing.T) {
			marker, path := createTestTxnMarker(
				t, 8*TxnMarkerMinSectorSize,
			)
			if err := marker.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatalf("open marker for unknown kind: %v", err)
			}
			if _, err := file.WriteAt(record, txnMarkerRegionStart); err != nil {
				_ = file.Close()
				t.Fatalf("write authenticated unknown kind: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close authenticated unknown kind: %v", err)
			}

			opened, decisions, err := OpenTxnMarker(path, TxnMarkerOptions{})
			if opened != nil {
				_ = opened.Close()
			}
			if !errors.Is(err, ErrTxnMarkerRecord) ||
				errors.Is(err, errTxnMarkerTruncatableTail) {
				t.Fatalf("OpenTxnMarker = %v, want hard ErrTxnMarkerRecord", err)
			}
			if decisions != nil {
				t.Fatalf("OpenTxnMarker returned decisions after hard unknown-kind error")
			}
		})
	}
}

func TestTxnMarkerAuthenticatedKnownKindLayoutMismatchIsHard(t *testing.T) {
	targets := testTxnCollectionRefs(2)
	decisionAsRetirement := wireEdgeTxnDecisionWithSequence(t, 1, 1, targets)
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
	wireEdgeSealTxnMarkerRecord(retirementAsDecision, TxnMarkerRecordPrefixSize)

	for name, record := range map[string][]byte{
		"decision-layout-as-retirement": decisionAsRetirement,
		"retirement-layout-as-decision": retirementAsDecision,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeTxnMarkerRecord(
				record, 1,
			); !errors.Is(err, ErrTxnMarkerRecord) ||
				errors.Is(err, errTxnMarkerTruncatableTail) {
				t.Fatalf("decodeTxnMarkerRecord = %v, want hard record error", err)
			}
		})
	}
}

func TestTxnMarkerOversizedAuthenticatedDecisionLayoutCannotTruncate(t *testing.T) {
	const targetCount = TxnMarkerMaxCollections + 1
	bodyEnd := TxnMarkerRecordPrefixSize + targetCount*TxnCollectionRefSize
	record := make([]byte, bodyEnd+TxnMarkerRecordTrailerSize)
	binary.LittleEndian.PutUint32(record[0:4], txnMarkerRecordMagic)
	// A retirement kind over a decision-shaped authenticated body exercises the
	// cross-layout gate; semantic participant limits are checked only after the
	// complete record has been authenticated.
	binary.LittleEndian.PutUint16(record[4:6], TxnMarkerRecordKindRetirement)
	binary.LittleEndian.PutUint64(record[8:16], 1)
	binary.LittleEndian.PutUint64(record[16:24], 1)
	binary.LittleEndian.PutUint32(record[24:28], targetCount)
	wireEdgeSealTxnMarkerRecord(record, bodyEnd)
	if _, _, err := decodeTxnMarkerRecord(
		record, 1,
	); !errors.Is(err, ErrTxnMarkerRecord) ||
		errors.Is(err, errTxnMarkerTruncatableTail) {
		t.Fatalf("decodeTxnMarkerRecord = %v, want hard record error", err)
	}
}
