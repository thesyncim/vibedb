package distributedtxn

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"
)

func testID() ID {
	var id ID
	copy(id[:], "txn-identity-0001")
	return id
}

func digest(value string) Digest { return sha256.Sum256([]byte(value)) }

func authorityWitness(value string) AuthorityWitness {
	digest := sha256.Sum256([]byte("authority:" + value))
	return AuthorityWitness(digest[:16])
}

func TestCoordinatorRoundTripAndBorrowing(t *testing.T) {
	record := CoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1, CatalogGeneration: 9,
		RecoveryDeadline: 1234,
		Participants: []ParticipantRef{
			{Distribution: []byte("docs"), Shard: []byte("-80"), RoutingVersion: 7, AllocationGeneration: 2, OwnershipEpoch: 3, AuthorityWitness: authorityWitness("a"), MutationDigest: digest("a"), State: ParticipantStaged},
			{Distribution: []byte("docs"), Shard: []byte("80-"), RoutingVersion: 7, AllocationGeneration: 4, OwnershipEpoch: 5, AuthorityWitness: authorityWitness("b"), MutationDigest: digest("b"), State: ParticipantStaged},
		},
	}
	encoded, err := AppendCoordinator([]byte("prefix"), record)
	if err != nil {
		t.Fatal(err)
	}
	encoded = encoded[len("prefix"):]
	if len(encoded) != coordinatorHeaderBytes+4+2*coordinatorEntryBytes+2*len("docs")+len("-80")+len("80-") {
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
		AllocationGeneration: 2, OwnershipEpoch: 3, CoordinatorDistribution: []byte("docs"), CoordinatorShard: []byte("-80"),
		CoordinatorAllocation: 2, CoordinatorRoutingVersion: 9, CoordinatorOwnershipEpoch: 3,
		BucketBits: 8, IntentScopes: []IntentScope{{Start: 1, End: 3}, {Start: 5, End: 6}},
		MutationDigest: sha256.Sum256(mutation), Mutation: mutation,
	}
	encoded, err := AppendParticipant(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	want := participantHeaderBytes + len(record.CoordinatorDistribution) + len(record.CoordinatorShard) +
		len(record.IntentScopes)*8 + len(mutation) + 4
	if len(encoded) != want {
		t.Fatalf("size = %d, want %d", len(encoded), want)
	}
	got, err := OpenParticipant(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != record.ID || got.MutationDigest != record.MutationDigest ||
		!bytes.Equal(got.Mutation, mutation) || !bytes.Equal(got.CoordinatorShard, []byte("-80")) ||
		!slices.Equal(got.IntentScopes, record.IntentScopes) {
		t.Fatalf("round trip = %+v", got)
	}
	offset := bytes.Index(encoded, mutation)
	if offset < 0 || &got.Mutation[0] != &encoded[offset] {
		t.Fatal("mutation did not borrow record storage")
	}
}

func TestRecordCorruptionAndOrderingRejected(t *testing.T) {
	record := CoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1, CatalogGeneration: 1,
		Participants: []ParticipantRef{
			{Distribution: []byte("docs"), Shard: []byte("b"), RoutingVersion: 1, AllocationGeneration: 1, OwnershipEpoch: 1, AuthorityWitness: authorityWitness("b"), MutationDigest: digest("b"), State: ParticipantStaged},
			{Distribution: []byte("docs"), Shard: []byte("a"), RoutingVersion: 1, AllocationGeneration: 1, OwnershipEpoch: 1, AuthorityWitness: authorityWitness("a"), MutationDigest: digest("a"), State: ParticipantStaged},
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

func TestAppendCoordinatorSignalsSegmentedFallback(t *testing.T) {
	participants := make([]ParticipantRef, MaxInlineParticipants+1)
	for i := range participants {
		participants[i] = ParticipantRef{
			Distribution: []byte("docs"), Shard: []byte{byte(i + 1)},
			RoutingVersion: 1, AllocationGeneration: 1, OwnershipEpoch: 1,
			AuthorityWitness: AuthorityWitness{byte(i + 1)},
			MutationDigest:   Digest{byte(i + 1)}, State: ParticipantStaged,
		}
	}
	_, err := AppendCoordinator(nil, CoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, Participants: participants,
	})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("AppendCoordinator wider than inline lane = %v, want ErrTooLarge", err)
	}
}

func TestStateTransitions(t *testing.T) {
	if !CoordinatorStaging.CanTransitionTo(CoordinatorCommitted) ||
		!CoordinatorStaging.CanTransitionTo(CoordinatorAborted) ||
		CoordinatorCommitted.CanTransitionTo(CoordinatorAborted) ||
		!ParticipantStaged.CanTransitionTo(ParticipantPrepared) ||
		ParticipantStaged.CanTransitionTo(ParticipantApplied) ||
		!ParticipantPrepared.CanTransitionTo(ParticipantApplied) ||
		ParticipantApplied.CanTransitionTo(ParticipantAborted) ||
		!ParticipantApplied.CanTransitionTo(ParticipantReleased) {
		t.Fatal("state transition matrix is not monotone")
	}
}

func BenchmarkCoordinatorCodec64Participants(b *testing.B) {
	participants := make([]ParticipantRef, MaxInlineParticipants)
	for i := range participants {
		participants[i] = ParticipantRef{
			Distribution:         []byte("docs"),
			Shard:                []byte{byte('a' + i/26), byte('a' + i%26)},
			RoutingVersion:       1,
			AllocationGeneration: 1, OwnershipEpoch: 1,
			MutationDigest: digest(string(rune(i + 1))), State: ParticipantStaged,
		}
	}
	record := CoordinatorRecord{ID: testID(), State: CoordinatorStaging, Revision: 1, CatalogGeneration: 1, Participants: participants}
	dst := make([]byte, 0, MaxCoordinatorRecordBytes)
	participantsScratch := make([]ParticipantRef, MaxInlineParticipants)
	encoded, err := AppendCoordinator(dst[:0], record)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err = AppendCoordinator(dst[:0], record)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := OpenCoordinatorInto(encoded, participantsScratch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParticipantCodecScopes(b *testing.B) {
	mutation := []byte("compact-mutation-batch")
	record := ParticipantRecord{
		ID: testID(), State: ParticipantStaged, Revision: 1, RoutingVersion: 9,
		AllocationGeneration: 2, OwnershipEpoch: 3,
		CoordinatorDistribution: []byte("docs"), CoordinatorShard: []byte("-80"),
		CoordinatorAllocation: 2, CoordinatorRoutingVersion: 9, CoordinatorOwnershipEpoch: 3,
		BucketBits: 20, IntentScopes: []IntentScope{
			{Start: 17, End: 18}, {Start: 91, End: 92},
			{Start: 700, End: 702}, {Start: 9000, End: 9001},
		},
		MutationDigest: sha256.Sum256(mutation), Mutation: mutation,
	}
	dst := make([]byte, 0, 1024)
	var scopes [MaxIntentScopes]IntentScope
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		encoded, err := AppendParticipant(dst[:0], record)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := OpenParticipantInto(encoded, scopes[:]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParticipantDigestScopes(b *testing.B) {
	mutation := make([]byte, 1024)
	scopes := []IntentScope{{Start: 17, End: 18}, {Start: 91, End: 92}, {Start: 700, End: 702}}
	b.ReportAllocs()
	b.SetBytes(int64(len(mutation)))
	b.ResetTimer()
	var digest Digest
	for range b.N {
		digest = ParticipantDigest(20, scopes, mutation)
	}
	if digest == (Digest{}) {
		b.Fatal("zero digest")
	}
}
