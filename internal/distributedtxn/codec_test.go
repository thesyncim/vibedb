package distributedtxn

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

func testID() ID {
	var id ID
	copy(id[:], "txn-identity-0001")
	return id
}

func digest(value string) Digest { return sha256.Sum256([]byte(value)) }

func TestCoordinatorRoundTripAndBorrowing(t *testing.T) {
	record := CoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1, RoutingVersion: 9,
		RecoveryDeadline: 1234,
		Participants: []ParticipantRef{
			{Shard: []byte("-80"), AllocationGeneration: 2, OwnershipEpoch: 3, MutationDigest: digest("a"), State: ParticipantStaged},
			{Shard: []byte("80-"), AllocationGeneration: 4, OwnershipEpoch: 5, MutationDigest: digest("b"), State: ParticipantStaged},
		},
	}
	encoded, err := AppendCoordinator([]byte("prefix"), record)
	if err != nil {
		t.Fatal(err)
	}
	encoded = encoded[len("prefix"):]
	if len(encoded) != coordinatorHeaderBytes+4+2*50+len("-80")+len("80-") {
		t.Fatalf("encoded size = %d", len(encoded))
	}
	got, err := OpenCoordinator(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != record.ID || got.State != record.State || got.Revision != 1 || len(got.Participants) != 2 {
		t.Fatalf("round trip = %+v", got)
	}
	offset := bytes.Index(encoded, []byte("80-"))
	if !bytes.Equal(got.Participants[1].Shard, []byte("80-")) || offset < 0 ||
		&got.Participants[1].Shard[0] != &encoded[offset] {
		t.Fatal("participant identity did not decode as a borrowed view")
	}
	reencoded, err := AppendCoordinator(nil, got)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("re-encode differs: %v", err)
	}
}

func TestParticipantRoundTripAndExactSize(t *testing.T) {
	mutation := []byte("compact-binary-sql-and-params")
	record := ParticipantRecord{
		ID: testID(), State: ParticipantStaged, Revision: 1, RoutingVersion: 9,
		AllocationGeneration: 2, OwnershipEpoch: 3, CoordinatorShard: []byte("-80"),
		CoordinatorAllocation: 2, MutationDigest: sha256.Sum256(mutation), Mutation: mutation,
	}
	encoded, err := AppendParticipant(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	want := participantHeaderBytes + len(record.CoordinatorShard) + len(mutation) + 4
	if len(encoded) != want {
		t.Fatalf("size = %d, want %d", len(encoded), want)
	}
	got, err := OpenParticipant(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != record.ID || got.MutationDigest != record.MutationDigest ||
		!bytes.Equal(got.Mutation, mutation) || !bytes.Equal(got.CoordinatorShard, []byte("-80")) {
		t.Fatalf("round trip = %+v", got)
	}
	offset := bytes.Index(encoded, mutation)
	if offset < 0 || &got.Mutation[0] != &encoded[offset] {
		t.Fatal("mutation did not borrow record storage")
	}
}

func TestRecordCorruptionAndOrderingRejected(t *testing.T) {
	record := CoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1, RoutingVersion: 1,
		Participants: []ParticipantRef{
			{Shard: []byte("b"), AllocationGeneration: 1, OwnershipEpoch: 1, MutationDigest: digest("b"), State: ParticipantStaged},
			{Shard: []byte("a"), AllocationGeneration: 1, OwnershipEpoch: 1, MutationDigest: digest("a"), State: ParticipantStaged},
		},
	}
	if _, err := AppendCoordinator(nil, record); err == nil {
		t.Fatal("unsorted participants accepted")
	}
	record.Participants[0], record.Participants[1] = record.Participants[1], record.Participants[0]
	encoded, err := AppendCoordinator(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)/2] ^= 0x80
	if _, err := OpenCoordinator(encoded); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption = %v", err)
	}
}

func TestStateTransitions(t *testing.T) {
	if !CoordinatorStaging.CanTransitionTo(CoordinatorCommitted) ||
		!CoordinatorStaging.CanTransitionTo(CoordinatorAborted) ||
		CoordinatorCommitted.CanTransitionTo(CoordinatorAborted) ||
		!ParticipantStaged.CanTransitionTo(ParticipantApplied) ||
		ParticipantApplied.CanTransitionTo(ParticipantAborted) ||
		!ParticipantApplied.CanTransitionTo(ParticipantReleased) {
		t.Fatal("state transition matrix is not monotone")
	}
}

func BenchmarkCoordinatorCodec64Participants(b *testing.B) {
	participants := make([]ParticipantRef, MaxParticipants)
	for i := range participants {
		participants[i] = ParticipantRef{
			Shard:                []byte{byte('a' + i/26), byte('a' + i%26)},
			AllocationGeneration: 1, OwnershipEpoch: 1,
			MutationDigest: digest(string(rune(i + 1))), State: ParticipantStaged,
		}
	}
	record := CoordinatorRecord{ID: testID(), State: CoordinatorStaging, Revision: 1, RoutingVersion: 1, Participants: participants}
	dst := make([]byte, 0, MaxCoordinatorRecordBytes)
	participantsScratch := make([]ParticipantRef, MaxParticipants)
	b.ReportAllocs()
	b.SetBytes(int64(coordinatorHeaderBytes + 4 + len(participants)*52))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := AppendCoordinator(dst[:0], record)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := OpenCoordinatorInto(encoded, participantsScratch); err != nil {
			b.Fatal(err)
		}
	}
}
