package gateway

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func TestControlPlaneDocumentKeysMatchCanonicalIDAndByteOrder(t *testing.T) {
	firstID := [32]byte{31: 1}
	secondID := [32]byte{31: 2}
	first := replicatedOperationKey(firstID)
	second := replicatedOperationKey(secondID)
	if bytes.Compare(first[:], second[:]) >= 0 {
		t.Fatal("operation key order does not preserve 256-bit byte order")
	}
	var idStorage [controlPlaneOperationIDBytes]byte
	id := appendReplicatedOperationDocumentID(idStorage[:0], firstID)
	want := fixedControlPlaneKey(id)
	if !bytes.Equal(first[:], want) {
		t.Fatalf("operation key=%x want ordered /id=%x", first, want)
	}
	raw, err := appendControlPlaneDocument(nil, id, []byte(`{"n":1}`), 1024)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openControlPlaneDocument(raw, id, 1024)
	if err != nil || !bytes.Equal(opened, []byte(`{"n":1}`)) {
		t.Fatalf("opened payload=%q err=%v", opened, err)
	}
}

func TestControlPlaneOperationKeyConstructionAllocatesNothing(t *testing.T) {
	id := [32]byte{0: 0x81, 7: 0x43, 31: 0xff}
	var sink [controlPlaneOperationKeyBytes]byte
	allocations := testing.AllocsPerRun(1000, func() {
		sink = replicatedOperationKey(id)
	})
	if allocations != 0 || sink == ([controlPlaneOperationKeyBytes]byte{}) {
		t.Fatalf("operation key allocations=%v key=%x", allocations, sink)
	}
}

func TestControlPlaneEnvelopeIsCanonicalUniqueAndExactlyBounded(t *testing.T) {
	id := replicatedOperationDirectoryDocumentID[:]
	maximum := maxReplicatedOperationDirectoryBytes
	payloadBytes := maximum - controlPlaneEnvelopeBaseBytes - len(id)
	payload := make([]byte, payloadBytes)
	payload[0], payload[len(payload)-1] = '"', '"'
	for index := 1; index < len(payload)-1; index++ {
		payload[index] = 'a'
	}
	raw, err := appendControlPlaneDocument(nil, id, payload, maximum)
	if err != nil || len(raw) != maximum {
		t.Fatalf("boundary envelope length=%d want=%d err=%v", len(raw), maximum, err)
	}
	opened, err := openControlPlaneDocument(raw, id, maximum)
	if err != nil || !bytes.Equal(opened, payload) {
		t.Fatalf("boundary open length=%d err=%v", len(opened), err)
	}
	oversizedPayload := make([]byte, len(payload)+1)
	oversizedPayload[0], oversizedPayload[len(oversizedPayload)-1] = '"', '"'
	for index := 1; index < len(oversizedPayload)-1; index++ {
		oversizedPayload[index] = 'a'
	}
	if _, err = appendControlPlaneDocument(nil, id, oversizedPayload, maximum); err == nil {
		t.Fatal("one-byte oversized envelope accepted")
	}
	if maxReplicatedCatalogBytes != replication.MaxMutationValueBytes {
		t.Fatalf("catalog envelope bound=%d exceeds mutation bound=%d",
			maxReplicatedCatalogBytes, replication.MaxMutationValueBytes)
	}

	for _, malformed := range [][]byte{
		append(append([]byte(nil), raw...), ' '),
		[]byte(`{"payload":"x","id":"operation/directory"}`),
		[]byte(`{"id":"operation\/directory","payload":"x"}`),
		[]byte(`{"id":"operation/directory","payload":"x","z":0}`),
	} {
		if _, err = openControlPlaneDocument(malformed, id, maximum); err == nil {
			t.Fatalf("noncanonical envelope accepted: %q", malformed)
		}
	}
}

func TestReplicatedCatalogEnvelopeRejectsBeforeGrowingRetainedArena(t *testing.T) {
	snapshot := testSnapshot(t, 1)
	raw, err := appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := openControlPlaneDocument(
		raw, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenSnapshotDocument(payload)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Generation() != snapshot.Generation() {
		t.Fatalf("opened generation=%d want=%d", opened.Generation(), snapshot.Generation())
	}

	storage := make([]byte, 0, 256)
	before := cap(storage)
	rejected, err := appendReplicatedCatalogDocument(storage, snapshot, 128)
	if err == nil {
		t.Fatal("undersized replicated catalog envelope accepted")
	}
	if cap(rejected) != before {
		t.Fatalf("rejected catalog grew retained arena from %d to %d", before, cap(rejected))
	}
}

func TestControlPlaneEnvelopeOpenAllocatesNothing(t *testing.T) {
	id := replicatedOperationDirectoryDocumentID[:]
	raw, err := appendControlPlaneDocument(nil, id, []byte(`{"ids":[]}`), 1024)
	if err != nil {
		t.Fatal(err)
	}
	var payload []byte
	allocations := testing.AllocsPerRun(1000, func() {
		payload, err = openControlPlaneDocument(raw, id, 1024)
	})
	if err != nil || allocations != 0 || !bytes.Equal(payload, []byte(`{"ids":[]}`)) {
		t.Fatalf("open allocations=%v payload=%q err=%v", allocations, payload, err)
	}
}

func TestOperationEnvelopeRejectsNoncanonical256BitIdentity(t *testing.T) {
	record := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{0xab}, Kind: ReplicatedOperationSplit,
		State: ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: 1, Proof: [32]byte{1},
	})
	raw, err := appendReplicatedOperation(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	needle := []byte("operation/ab")
	position := bytes.Index(raw, needle)
	if position < 0 {
		t.Fatalf("missing operation identity in %q", raw)
	}
	damaged := append([]byte(nil), raw...)
	damaged[position+len("operation/")] = 'A'
	if _, err = openReplicatedOperation(damaged); err == nil {
		t.Fatal("uppercase operation identity accepted")
	}
}
