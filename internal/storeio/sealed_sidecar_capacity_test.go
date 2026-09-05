package storeio

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"
)

func resealSidecarHeader(buf []byte) {
	checksum := PageChecksum(buf[:len(buf)-8])
	binary.LittleEndian.PutUint32(buf[len(buf)-8:len(buf)-4], checksum)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], ^checksum)
}

func TestRecoveryJournalSealedCapacityHeaderBitIsStrict(t *testing.T) {
	header := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
	header.SealedCapacity = true
	header.RecycleCount = 1
	buf := make([]byte, RecoveryJournalHeaderSize)
	if _, err := EncodeRecoveryJournalHeader(buf, header); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRecoveryJournalHeader(buf)
	if err != nil || decoded != header {
		t.Fatalf("sealed recovery header = (%+v,%v), want %+v", decoded, err, header)
	}
	if got := binary.LittleEndian.Uint32(buf[88:92]); got != 1 {
		t.Fatalf("recovery header flags = %#x, want bit 0", got)
	}
	if !allZero(buf[92 : RecoveryJournalHeaderSize-8]) {
		t.Fatal("recovery header reserved suffix is non-zero")
	}
	const recoveryPrefix = "564a4f55524e414c00000000000200000102030405060708090a0b0c0d0e0f10404142434445464748494a4b4c4d4e4f0010000000020000010000000000000000000000000000000010000000000000010000000000000001000000"
	if got := hex.EncodeToString(buf[:92]); got != recoveryPrefix {
		t.Fatalf("recovery header prefix = %s, want %s", got, recoveryPrefix)
	}
	if got, want := binary.LittleEndian.Uint32(buf[RecoveryJournalHeaderSize-8:]),
		uint32(0xb5c955b0); got != want {
		t.Fatalf("recovery header checksum = %#08x, want %#08x", got, want)
	}
	binary.LittleEndian.PutUint32(buf[88:92], recoveryJournalFlagSealedCapacity|4)
	resealSidecarHeader(buf)
	if _, err := DecodeRecoveryJournalHeader(buf); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("unknown recovery header flag = %v, want corrupt", err)
	}
	if _, err := EncodeRecoveryJournalHeader(buf, header); err != nil {
		t.Fatal(err)
	}
	buf[92] = 1
	resealSidecarHeader(buf)
	if _, err := DecodeRecoveryJournalHeader(buf); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("resealed recovery reserved suffix = %v, want corrupt", err)
	}
	if _, err := EncodeRecoveryJournalHeader(buf, header); err != nil {
		t.Fatal(err)
	}
	buf[RecoveryJournalHeaderSize-9] = 1
	resealSidecarHeader(buf)
	if _, err := DecodeRecoveryJournalHeader(buf); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("resealed recovery reserved tail = %v, want corrupt", err)
	}
}

func TestTxnMarkerSealedCapacityHeaderBitIsStrict(t *testing.T) {
	var markerID [16]byte
	markerID[0] = 1
	header := TxnMarkerHeader{
		Format:   TxnMarkerFormat,
		MarkerID: markerID, Epoch: 1,
		Capacity:       8 * TxnMarkerMinSectorSize,
		SealedCapacity: true,
		RecycleCount:   1,
	}
	buf := make([]byte, TxnMarkerHeaderSize)
	if _, err := EncodeTxnMarkerHeader(buf, header); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeTxnMarkerHeader(buf)
	if err != nil || decoded != header {
		t.Fatalf("sealed marker header = (%+v,%v), want %+v", decoded, err, header)
	}
	if got := binary.LittleEndian.Uint32(buf[64:68]); got != 1 {
		t.Fatalf("marker header flags = %#x, want bit 0", got)
	}
	if !allZero(buf[68 : TxnMarkerHeaderSize-8]) {
		t.Fatal("marker header reserved suffix is non-zero")
	}
	const markerPrefix = "5654584e4d524b00000000000002000001000000000000000000000000000000010000000000000000000000000000000010000000000000010000000000000001000000"
	if got := hex.EncodeToString(buf[:68]); got != markerPrefix {
		t.Fatalf("marker header prefix = %s, want %s", got, markerPrefix)
	}
	if got, want := binary.LittleEndian.Uint32(buf[TxnMarkerHeaderSize-8:]),
		uint32(0x939043cc); got != want {
		t.Fatalf("marker header checksum = %#08x, want %#08x", got, want)
	}
	binary.LittleEndian.PutUint32(buf[64:68], txnMarkerFlagSealedCapacity|4)
	resealSidecarHeader(buf)
	if _, err := DecodeTxnMarkerHeader(buf); !errors.Is(err, ErrTxnMarkerCorrupt) {
		t.Fatalf("unknown marker header flag = %v, want corrupt", err)
	}
	if _, err := EncodeTxnMarkerHeader(buf, header); err != nil {
		t.Fatal(err)
	}
	buf[68] = 1
	resealSidecarHeader(buf)
	if _, err := DecodeTxnMarkerHeader(buf); !errors.Is(err, ErrTxnMarkerCorrupt) {
		t.Fatalf("resealed marker reserved suffix = %v, want corrupt", err)
	}
	if _, err := EncodeTxnMarkerHeader(buf, header); err != nil {
		t.Fatal(err)
	}
	buf[TxnMarkerHeaderSize-9] = 1
	resealSidecarHeader(buf)
	if _, err := DecodeTxnMarkerHeader(buf); !errors.Is(err, ErrTxnMarkerCorrupt) {
		t.Fatalf("resealed marker reserved tail = %v, want corrupt", err)
	}
}
