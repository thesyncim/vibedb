package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func txnMarkerFuzzSeedEmpty(t testing.TB) []byte {
	t.Helper()
	m, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return data
}

func txnMarkerFuzzSeedOneDecision(t testing.TB) []byte {
	t.Helper()
	m, path := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
	if _, err := m.AppendDecision(3, testTxnCollectionRefs(2)); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return data
}

func txnMarkerFuzzSeedTornDecision(t testing.TB) []byte {
	t.Helper()
	m, path := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
	fm := NewFaultTxnMarker(m)
	fm.Program(TxnMarkerFaultPlan{Phase: TxnMarkerFaultTornAppend, AppendIndex: 0})
	if _, err := m.AppendDecision(3, testTxnCollectionRefs(2)); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("torn AppendDecision = %v, want io.ErrShortWrite", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return data
}

func txnMarkerFuzzSeedWrongCRC(t testing.TB) []byte {
	t.Helper()
	data := txnMarkerFuzzSeedOneDecision(t)
	if len(data) > txnMarkerRegionStart+TxnMarkerRecordPrefixSize {
		data[txnMarkerRegionStart+TxnMarkerRecordPrefixSize] ^= 0xff
	}
	return data
}

func txnMarkerFuzzSeedNonCurrentDomain(t testing.TB) []byte {
	t.Helper()
	data := txnMarkerFuzzSeedEmpty(t)
	for slot := 0; slot < txnMarkerHeaderSlots; slot++ {
		off := slot * TxnMarkerHeaderSize
		if len(data) < off+TxnMarkerHeaderSize {
			break
		}
		copy(data[off:off+8], "NOTMARK!")
		sum := PageChecksum(data[off : off+TxnMarkerHeaderSize-8])
		binary.LittleEndian.PutUint32(
			data[off+TxnMarkerHeaderSize-8:off+TxnMarkerHeaderSize-4], sum,
		)
		binary.LittleEndian.PutUint32(
			data[off+TxnMarkerHeaderSize-4:off+TxnMarkerHeaderSize], ^sum,
		)
	}
	return data
}

func FuzzTxnMarkerOpen(f *testing.F) {
	f.Add(txnMarkerFuzzSeedEmpty(f))
	f.Add(txnMarkerFuzzSeedOneDecision(f))
	f.Add(txnMarkerFuzzSeedTornDecision(f))
	f.Add(txnMarkerFuzzSeedWrongCRC(f))
	f.Add(txnMarkerFuzzSeedNonCurrentDomain(f))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0}, TxnMarkerHeaderSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > int(TxnMarkerMaxCapacityBytes)+txnMarkerRegionStart {
			data = data[:TxnMarkerMaxCapacityBytes+txnMarkerRegionStart]
		}
		dir := t.TempDir()
		path := filepath.Join(dir, "txn.vtm")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		m, decisions, err := OpenTxnMarker(path, TxnMarkerOptions{})
		if err != nil {
			return
		}
		defer m.Close()
		if m.Header().Capacity == 0 ||
			m.Header().Capacity > TxnMarkerMaxCapacityBytes {
			t.Fatalf("open accepted unbounded capacity %d", m.Header().Capacity)
		}
		if decisions == nil {
			t.Fatal("nil decisions on successful open")
		}
		if decisions.MaxDCSN() > 0 && m.NextSequence() != decisions.MaxDCSN()+1 {
			t.Fatalf(
				"sequence/cursor desync: next=%d maxDCSN=%d",
				m.NextSequence(), decisions.MaxDCSN(),
			)
		}
	})
}
