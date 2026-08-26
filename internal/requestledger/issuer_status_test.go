package requestledger

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func issuerStatusFixture(t *testing.T) (IssuerHighwaterRecord, IssuerSequenceRecord, AckRecord) {
	t.Helper()
	key := testKey(true)
	key.IssuerSequence = 1
	ack, sequence := gcCompleteAckForKey(t, key)
	highwater, err := NewIssuerHighwater(key)
	if err != nil {
		t.Fatal(err)
	}
	highwater, err = AdmitIssuerSequence(highwater, key, ack.RequestDigest, highwater.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	return highwater, sequence, ack
}

func TestIssuerLaneStatusCanonicalWitness(t *testing.T) {
	highwater, sequence, ack := issuerStatusFixture(t)
	status, err := NewIssuerLaneStatus(highwater, &sequence, &ack)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendIssuerLaneStatus(nil, status)
	if err != nil || len(raw) != IssuerLaneStatusBytes {
		t.Fatalf("append bytes=%d err=%v", len(raw), err)
	}
	opened, err := OpenIssuerLaneStatus(raw)
	if err != nil || opened.Highwater != highwater || opened.Sequence != sequence ||
		opened.Ack != ack || !opened.NextFound || !opened.AdvanceReady {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	reencoded, err := AppendIssuerLaneStatus(nil, opened)
	if err != nil || string(reencoded) != string(raw) {
		t.Fatal("issuer status is not uniquely canonical")
	}

	for _, at := range []int{4, 5, 8, 16, 16 + IssuerHighwaterRecordBytes,
		16 + IssuerHighwaterRecordBytes + IssuerSequenceRecordBytes} {
		forged := append([]byte(nil), raw...)
		forged[at] ^= 1
		binary.LittleEndian.PutUint32(forged[len(forged)-4:],
			crc32.Checksum(forged[:len(forged)-4], castagnoli))
		if _, err := OpenIssuerLaneStatus(forged); err == nil {
			t.Fatalf("forgery at %d accepted", at)
		}
	}
}

func TestIssuerLaneStatusStopsAtActiveAndGap(t *testing.T) {
	highwater, complete, _ := issuerStatusFixture(t)
	key := testKey(true)
	key.IssuerSequence = 1
	active, err := NewIssuerSequence(key, complete.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	status, err := NewIssuerLaneStatus(highwater, &active, nil)
	if err != nil || !status.NextFound || status.AdvanceReady {
		t.Fatalf("active status=%+v err=%v", status, err)
	}
	if _, err := NewIssuerLaneStatus(highwater, &complete, nil); err == nil {
		t.Fatal("GC-complete sequence without its ACK witness accepted")
	}

	emptyKey := key
	emptyKey.IssuerSequence = 2
	empty, err := NewIssuerHighwater(emptyKey)
	if err != nil {
		t.Fatal(err)
	}
	gap, err := NewIssuerLaneStatus(empty, nil, nil)
	if err != nil || gap.NextFound || gap.AdvanceReady {
		t.Fatalf("gap status=%+v err=%v", gap, err)
	}
}
