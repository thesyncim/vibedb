package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	transactionCodecBytesSink    []byte
	transactionCodecControlSink  TransactionControlView
	transactionCodecPayloadSink  TransactionCoordinatorPayloadView
	transactionCodecPageSink     TransactionManifestPageView
	transactionCodecMutationSink TransactionNativeMutationView
	transactionCodecIntentSink   TransactionIntentView
)

func transactionCodecID(seed byte) distributedtxn.ID {
	var id distributedtxn.ID
	for index := range id {
		id[index] = seed + byte(index)
	}
	return id
}

func transactionCodecReplicationID(seed byte) replication.ID128 {
	var id replication.ID128
	for index := range id {
		id[index] = seed + byte(index)
	}
	return id
}

func transactionCodecDigest(seed byte) distributedtxn.Digest {
	var digest distributedtxn.Digest
	for index := range digest {
		digest[index] = seed + byte(index)
	}
	return digest
}

func transactionCodecCommandDigest(seed byte) replication.Digest {
	var digest replication.Digest
	for index := range digest {
		digest[index] = seed + byte(index)
	}
	return digest
}

func transactionCodecMutation() replication.Mutation {
	return replication.Mutation{
		Kind:                replication.MutationPutDigestEqual,
		Key:                 []byte{0x81, 'k'},
		Value:               []byte(`{"n":2}`),
		ExpectedValueLength: 7,
		ExpectedValueDigest: transactionCodecCommandDigest(90),
	}
}

func transactionCodecControl(t testing.TB) TransactionControl {
	t.Helper()
	scopes := []distributedtxn.IntentScope{{Start: 1, End: 3}, {Start: 8, End: 13}}
	controlBytes, err := TransactionControlResidentBytes(len(scopes))
	if err != nil {
		t.Fatal(err)
	}
	mutationBytes, err := TransactionNativeMutationResidentBytes(transactionCodecMutation())
	if err != nil {
		t.Fatal(err)
	}
	intentBytes, err := TransactionIntentResidentBytes(len(transactionCodecMutation().Key))
	if err != nil {
		t.Fatal(err)
	}
	return TransactionControl{
		ID: transactionCodecID(1), Role: distributedtxn.ReplicatedRoleParticipant,
		State: uint8(distributedtxn.ParticipantPrepared), Revision: 2,
		PayloadKind:   distributedtxn.ReplicatedPayloadParticipantStage,
		PayloadDigest: transactionCodecDigest(20), PayloadBytes: 4096, PayloadCount: 1,
		PayloadRelationCount:        1,
		CoordinatorGroup:            transactionCodecReplicationID(30),
		CoordinatorShardIncarnation: transactionCodecReplicationID(50),
		CoordinatorAllocation:       71,
		MutationDigest:              transactionCodecDigest(70), BucketBits: 8, IntentScopes: scopes,
		ResidentControlBytes: controlBytes, ResidentMutationBytes: mutationBytes,
		ResidentIntentBytes:  intentBytes,
		LastOperation:        distributedtxn.ReplicatedPrepareParticipant,
		LastExpectedRevision: 1, LastCommandDigest: transactionCodecCommandDigest(110),
		LastResultCode: 1, LastAppliedIndex: 91,
	}
}

func transactionCodecCoordinatorPayload(t testing.TB) (distributedtxn.ID, []byte) {
	t.Helper()
	id := transactionCodecID(3)
	payload, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 7, RecoveryDeadline: 99,
		Participants: []distributedtxn.ParticipantRef{{
			Distribution: []byte("dist"), Shard: []byte("shard-a"),
			RoutingVersion: 9, AllocationGeneration: 10, OwnershipEpoch: 11,
			MutationDigest: transactionCodecDigest(13), State: distributedtxn.ParticipantStaged,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return id, payload
}

func transactionCodecManifestSegment(
	t testing.TB,
) (distributedtxn.ID, distributedtxn.ManifestSegment, distributedtxn.ManifestDescriptor) {
	t.Helper()
	id := transactionCodecID(4)
	pageScratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	var retained distributedtxn.ManifestSegment
	builder, err := distributedtxn.NewManifestBuilder(pageScratch, func(segment distributedtxn.ManifestSegment) error {
		retained = segment
		retained.Raw = bytes.Clone(segment.Raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append(distributedtxn.ParticipantRef{
		Distribution: []byte("dist"), Shard: []byte("shard-b"),
		RoutingVersion: 12, AllocationGeneration: 13, OwnershipEpoch: 14,
		MutationDigest: transactionCodecDigest(15), State: distributedtxn.ParticipantStaged,
	}); err != nil {
		t.Fatal(err)
	}
	descriptor, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return id, retained, descriptor
}

func TestTransactionSystemStorageKeysAreExactAndOrdered(t *testing.T) {
	id := transactionCodecID(1)
	control, err := TransactionControlStorageKey(distributedtxn.ReplicatedRoleParticipant, id)
	if err != nil || len(control) != transactionControlStorageKeyBytes ||
		control[0] != transactionControlPrefix || control[1] != byte(distributedtxn.ReplicatedRoleParticipant) ||
		!bytes.Equal(control[2:], id[:]) {
		t.Fatalf("control key=%x err=%v", control, err)
	}
	payload, err := TransactionCoordinatorPayloadStorageKey(id)
	if err != nil || payload[0] != transactionPayloadPrefix || !bytes.Equal(payload[1:], id[:]) {
		t.Fatalf("payload key=%x err=%v", payload, err)
	}
	page1, err := TransactionManifestPageStorageKey(id, 1)
	if err != nil {
		t.Fatal(err)
	}
	page256, err := TransactionManifestPageStorageKey(id, 256)
	if err != nil || bytes.Compare(page1[:], page256[:]) >= 0 {
		t.Fatalf("manifest keys not index ordered: %x / %x err=%v", page1, page256, err)
	}
	mutation1, err := TransactionNativeMutationStorageKey(id, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	mutation2, err := TransactionNativeMutationStorageKey(id, 2, 0)
	if err != nil || bytes.Compare(mutation1[:], mutation2[:]) >= 0 {
		t.Fatalf("mutation keys not relation/ordinal ordered: %x / %x err=%v", mutation1, mutation2, err)
	}
	rawKey := []byte{0x81, 'k'}
	intent, err := TransactionIntentStorageKey(2, rawKey)
	digest := sha256.Sum256(rawKey)
	if err != nil || intent[0] != transactionIntentPrefix ||
		!bytes.Equal(intent[3:], digest[:]) {
		t.Fatalf("intent key=%x err=%v", intent, err)
	}
	if _, err := TransactionControlStorageKey(distributedtxn.ReplicatedRoleInvalid, id); err == nil {
		t.Fatal("invalid control role accepted")
	}
	if _, err := TransactionNativeMutationStorageKey(id, 0, 0); err == nil {
		t.Fatal("zero relation accepted")
	}
}

func TestTransactionControlRoundTripRetryWitnessAndTombstone(t *testing.T) {
	control := transactionCodecControl(t)
	encoded, err := AppendTransactionControl(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, _ := TransactionControlResidentBytes(len(control.IntentScopes))
	if uint64(len(encoded)+transactionControlStorageKeyBytes) != wantBytes ||
		len(encoded) != transactionControlHeaderBytes+len(control.IntentScopes)*8+recordChecksumLen {
		t.Fatalf("control bytes=%d resident=%d want=%d", len(encoded), len(encoded)+transactionControlStorageKeyBytes, wantBytes)
	}
	var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
	view, err := OpenTransactionControlInto(encoded, scopes[:])
	if err != nil || view.ID != control.ID || view.Role != control.Role || view.State != control.State ||
		view.LastOperation != control.LastOperation ||
		view.LastExpectedRevision != control.LastExpectedRevision ||
		view.LastCommandDigest != control.LastCommandDigest ||
		view.LastResultCode != control.LastResultCode ||
		view.LastAppliedIndex != control.LastAppliedIndex ||
		view.PayloadRelationCount != control.PayloadRelationCount ||
		!bytes.Equal(view.Bytes(), encoded) || cap(view.Bytes()) != len(encoded) ||
		len(view.IntentScopes) != len(control.IntentScopes) {
		t.Fatalf("control view=%+v err=%v", view.TransactionControl, err)
	}
	key, err := view.StorageKey()
	wantKey, _ := TransactionControlStorageKey(control.Role, control.ID)
	if err != nil || key != wantKey {
		t.Fatalf("control storage key=%x want=%x err=%v", key, wantKey, err)
	}
	reencoded, err := AppendTransactionControl(nil, view.TransactionControl)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("control canonical reencode equal=%t err=%v", bytes.Equal(reencoded, encoded), err)
	}

	tombstone := control
	tombstone.State = uint8(distributedtxn.ParticipantReleased)
	tombstone.Revision = 4
	tombstone.ResidentMutationBytes = 0
	tombstone.ResidentIntentBytes = 0
	tombstone.AffectedRowsValid = true
	tombstone.AffectedRows = 7
	tombstone.LastOperation = distributedtxn.ReplicatedReleaseParticipant
	tombstone.LastExpectedRevision = 3
	tombstone.LastCommandDigest = transactionCodecCommandDigest(120)
	tombstone.LastAppliedIndex = 101
	retired, err := AppendTransactionControl(nil, tombstone)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenTransactionControlInto(retired, scopes[:])
	if err != nil || opened.PayloadDigest != control.PayloadDigest ||
		opened.MutationDigest != control.MutationDigest || opened.LastResultCode == 0 ||
		opened.LastAppliedIndex != tombstone.LastAppliedIndex || !opened.AffectedRowsValid ||
		opened.AffectedRows != 7 || opened.ResidentMutationBytes != 0 || opened.ResidentIntentBytes != 0 {
		t.Fatalf("released tombstone=%+v err=%v", opened.TransactionControl, err)
	}
}

func TestTransactionManifestControlRoundTripIncrementalSealWitness(t *testing.T) {
	id, segment, descriptor := transactionCodecManifestSegment(t)
	controlBytes, err := TransactionControlResidentBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	payloadBytes, err := TransactionCoordinatorPayloadResidentBytes(128)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := TransactionManifestPageResidentBytes(len(segment.Raw))
	if err != nil {
		t.Fatal(err)
	}
	control := TransactionControl{
		ID: id, Role: distributedtxn.ReplicatedRoleCoordinator,
		State: uint8(distributedtxn.CoordinatorStaging), Revision: 2,
		PayloadKind:   distributedtxn.ReplicatedPayloadManifestCoordinator,
		PayloadDigest: transactionCodecDigest(31),
		PayloadBytes:  descriptor.EncodedBytes, PayloadCount: descriptor.ParticipantCount,
		CoordinatorGroup:            transactionCodecReplicationID(32),
		CoordinatorShardIncarnation: transactionCodecReplicationID(48),
		CoordinatorAllocation:       64,
		MutationDigest:              transactionCodecDigest(65),
		ResidentControlBytes:        controlBytes,
		ResidentPayloadBytes:        payloadBytes,
		ResidentManifestBytes:       manifestBytes,
		LastOperation:               distributedtxn.ReplicatedStageManifestSegment,
		LastExpectedRevision:        1,
		LastCommandDigest:           transactionCodecCommandDigest(97),
		LastResultCode:              1,
		LastAppliedIndex:            12,
		ManifestNextPage:            segment.Index + 1,
		ManifestNextParticipant:     segment.FirstParticipant + uint64(segment.ParticipantCount),
		ManifestEncodedBytes:        uint64(len(segment.Raw)),
		ManifestChainDigest: advanceTransactionManifestChain(
			distributedtxn.Digest{}, segment.Index, segment.Digest,
		),
	}
	if got := finishTransactionManifestRoot(
		control.ManifestChainDigest,
		control.ManifestNextParticipant,
		control.ManifestEncodedBytes,
		control.ManifestNextPage,
	); got != descriptor.Root {
		t.Fatalf("incremental manifest root=%x want=%x", got, descriptor.Root)
	}
	encoded, err := AppendTransactionControl(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenTransactionControlInto(encoded, nil)
	if err != nil || view.ManifestNextPage != control.ManifestNextPage ||
		view.ManifestNextParticipant != control.ManifestNextParticipant ||
		view.ManifestEncodedBytes != control.ManifestEncodedBytes ||
		view.ManifestChainDigest != control.ManifestChainDigest {
		t.Fatalf("manifest control=%+v err=%v", view.TransactionControl, err)
	}
	reencoded, err := AppendTransactionControl(nil, view.TransactionControl)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("manifest control canonical reencode equal=%t err=%v", bytes.Equal(reencoded, encoded), err)
	}

	for name, mutate := range map[string]func(*TransactionControl){
		"partial tuple": func(candidate *TransactionControl) { candidate.ManifestChainDigest = distributedtxn.Digest{} },
		"page beyond maximum": func(candidate *TransactionControl) {
			candidate.ManifestNextPage = uint32(candidate.PayloadCount + 1)
		},
		"participant beyond descriptor": func(candidate *TransactionControl) {
			candidate.ManifestNextParticipant = candidate.PayloadCount + 1
		},
		"bytes beyond descriptor": func(candidate *TransactionControl) {
			candidate.ManifestEncodedBytes = candidate.PayloadBytes + 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := control
			mutate(&candidate)
			if _, err := AppendTransactionControl(nil, candidate); err == nil {
				t.Fatal("invalid manifest progress accepted")
			}
		})
	}

	inline := transactionCodecControl(t)
	inline.ManifestNextPage = 1
	inline.ManifestNextParticipant = 1
	inline.ManifestEncodedBytes = 1
	inline.ManifestChainDigest = transactionCodecDigest(130)
	if _, err := AppendTransactionControl(nil, inline); err == nil {
		t.Fatal("participant manifest progress accepted")
	}
}

func TestTransactionCoordinatorDecisionSurvivesRetiredTombstone(t *testing.T) {
	_, segment, descriptor := transactionCodecManifestSegment(t)
	controlBytes, _ := TransactionControlResidentBytes(0)
	control := TransactionControl{
		ID: transactionCodecID(33), Role: distributedtxn.ReplicatedRoleCoordinator,
		State: uint8(distributedtxn.CoordinatorRetired), Revision: 3,
		PayloadKind:   distributedtxn.ReplicatedPayloadManifestCoordinator,
		PayloadDigest: transactionCodecDigest(34), PayloadBytes: descriptor.EncodedBytes,
		PayloadCount:                descriptor.ParticipantCount,
		CoordinatorGroup:            transactionCodecReplicationID(35),
		CoordinatorShardIncarnation: transactionCodecReplicationID(51),
		CoordinatorAllocation:       67,
		MutationDigest:              transactionCodecDigest(68),
		CoordinatorDecision:         distributedtxn.CoordinatorCommitted,
		ResidentControlBytes:        controlBytes,
		LastOperation:               distributedtxn.ReplicatedRetireCoordinator,
		LastExpectedRevision:        2,
		LastCommandDigest:           transactionCodecCommandDigest(100),
		LastResultCode:              1,
		LastAppliedIndex:            101,
		ManifestNextPage:            segment.Index + 1,
		ManifestNextParticipant:     descriptor.ParticipantCount,
		ManifestEncodedBytes:        descriptor.EncodedBytes,
		ManifestChainDigest: advanceTransactionManifestChain(
			distributedtxn.Digest{}, segment.Index, segment.Digest,
		),
	}
	encoded, err := AppendTransactionControl(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenTransactionControlInto(encoded, nil)
	if err != nil || opened.CoordinatorDecision != distributedtxn.CoordinatorCommitted {
		t.Fatalf("retired decision=%d err=%v", opened.CoordinatorDecision, err)
	}
	for _, decision := range []distributedtxn.CoordinatorState{
		distributedtxn.CoordinatorInvalid,
		distributedtxn.CoordinatorStaging,
	} {
		candidate := control
		candidate.CoordinatorDecision = decision
		if _, err := AppendTransactionControl(nil, candidate); err == nil {
			t.Fatalf("retired decision %d accepted", decision)
		}
	}
}

func TestTransactionCoordinatorPayloadRoundTripBorrowAndAlias(t *testing.T) {
	id, payload := transactionCodecCoordinatorPayload(t)
	encoded, err := AppendTransactionCoordinatorPayload(
		nil, id, distributedtxn.ReplicatedPayloadCoordinator, payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	resident, _ := TransactionCoordinatorPayloadResidentBytes(len(payload))
	if uint64(len(encoded)+transactionPayloadStorageKeyBytes) != resident {
		t.Fatalf("payload resident=%d want=%d", len(encoded)+transactionPayloadStorageKeyBytes, resident)
	}
	view, err := OpenTransactionCoordinatorPayload(encoded)
	if err != nil || view.ID != id || view.Kind != distributedtxn.ReplicatedPayloadCoordinator ||
		!bytes.Equal(view.Payload, payload) || cap(view.Payload) != len(view.Payload) ||
		unsafe.SliceData(view.Payload) == unsafe.SliceData(payload) ||
		unsafe.SliceData(view.Payload) != unsafe.SliceData(encoded[transactionPayloadHeaderBytes:]) {
		t.Fatalf("payload view=%+v err=%v", view, err)
	}
	reencoded, err := AppendTransactionCoordinatorPayload(nil, view.ID, view.Kind, view.Payload)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("payload reencode equal=%t err=%v", bytes.Equal(reencoded, encoded), err)
	}
	arena := append(make([]byte, 0, len(encoded)), encoded...)
	aliased, err := OpenTransactionCoordinatorPayload(arena)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := AppendTransactionCoordinatorPayload(arena[:0], aliased.ID, aliased.Kind, aliased.Payload); !errors.Is(err, ErrCodecAlias) || len(got) != 0 {
		t.Fatalf("payload alias append bytes=%d err=%v", len(got), err)
	}
}

func TestTransactionManifestPageRoundTripScratchAndRetryIdentity(t *testing.T) {
	id, segment, _ := transactionCodecManifestSegment(t)
	encoded, err := AppendTransactionManifestPage(nil, id, segment)
	if err != nil {
		t.Fatal(err)
	}
	resident, _ := TransactionManifestPageResidentBytes(len(segment.Raw))
	if uint64(len(encoded)+transactionManifestKeyBytes) != resident {
		t.Fatalf("page resident=%d want=%d", len(encoded)+transactionManifestKeyBytes, resident)
	}
	participants := make([]distributedtxn.ParticipantRef, distributedtxn.MaxManifestPageParticipants)
	identities := make([]byte, distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2)
	view, err := OpenTransactionManifestPageInto(encoded, participants, identities)
	if err != nil || view.ID != id || view.Index != segment.Index ||
		view.FirstParticipant != segment.FirstParticipant ||
		view.ParticipantCount != segment.ParticipantCount || view.Digest != segment.Digest ||
		!bytes.Equal(view.Raw, segment.Raw) || cap(view.Raw) != len(view.Raw) {
		t.Fatalf("manifest page=%+v err=%v", view, err)
	}
	key, err := view.StorageKey()
	wantKey, _ := TransactionManifestPageStorageKey(id, segment.Index)
	if err != nil || key != wantKey {
		t.Fatalf("manifest key=%x want=%x err=%v", key, wantKey, err)
	}
	reencoded, err := AppendTransactionManifestPage(nil, id, distributedtxn.ManifestSegment{
		Index: segment.Index, FirstParticipant: segment.FirstParticipant,
		ParticipantCount: segment.ParticipantCount, Digest: segment.Digest, Raw: view.Raw,
	})
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("manifest reencode equal=%t err=%v", bytes.Equal(reencoded, encoded), err)
	}
}

func TestTransactionNativeMutationRoundTripCanonicalKindsAndAlias(t *testing.T) {
	id := transactionCodecID(8)
	prior := transactionCodecCommandDigest(40)
	for ordinal, mutation := range []replication.Mutation{
		{Kind: replication.MutationPut, Key: []byte("a"), Value: []byte("1")},
		{Kind: replication.MutationDelete, Key: []byte("b")},
		{Kind: replication.MutationPutAbsentOrEqual, Key: []byte("c"), Value: []byte("2")},
		{Kind: replication.MutationDeleteDigestEqual, Key: []byte("d"), ExpectedValueLength: 1, ExpectedValueDigest: prior},
		{Kind: replication.MutationPutDigestEqual, Key: []byte("e"), Value: []byte("3"), ExpectedValueLength: 1, ExpectedValueDigest: prior},
	} {
		encoded, err := AppendTransactionNativeMutation(nil, id, 2, uint32(ordinal), mutation)
		if err != nil {
			t.Fatalf("kind %d append: %v", mutation.Kind, err)
		}
		view, err := OpenTransactionNativeMutation(encoded)
		if err != nil || view.ID != id || view.Relation != 2 || view.Ordinal != uint32(ordinal) ||
			view.Mutation.Kind != mutation.Kind || !bytes.Equal(view.Mutation.Key, mutation.Key) ||
			!bytes.Equal(view.Mutation.Value, mutation.Value) ||
			cap(view.Mutation.Key) != len(view.Mutation.Key) || cap(view.Mutation.Value) != len(view.Mutation.Value) ||
			view.Digest != TransactionNativeMutationDigest(id, 2, uint32(ordinal), mutation) {
			t.Fatalf("kind %d view=%+v err=%v", mutation.Kind, view, err)
		}
		reencoded, err := AppendTransactionNativeMutation(nil, view.ID, view.Relation, view.Ordinal, view.Mutation)
		if err != nil || !bytes.Equal(reencoded, encoded) {
			t.Fatalf("kind %d canonical reencode equal=%t err=%v", mutation.Kind, bytes.Equal(reencoded, encoded), err)
		}
	}

	encoded, err := AppendTransactionNativeMutation(nil, id, 1, 0, transactionCodecMutation())
	if err != nil {
		t.Fatal(err)
	}
	arena := append(make([]byte, 0, len(encoded)), encoded...)
	view, err := OpenTransactionNativeMutation(arena)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := AppendTransactionNativeMutation(arena[:0], view.ID, view.Relation, view.Ordinal, view.Mutation); !errors.Is(err, ErrCodecAlias) || len(got) != 0 {
		t.Fatalf("mutation alias append bytes=%d err=%v", len(got), err)
	}
}

func TestTransactionIntentCollisionVerificationAndCanonicalReencode(t *testing.T) {
	id := transactionCodecID(10)
	rawKey := []byte("alpha")
	encoded, err := AppendTransactionIntent(nil, id, 3, rawKey)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenTransactionIntentForKey(encoded, 3, rawKey)
	if err != nil || view.ID != id || !view.MatchesKey(3, rawKey) || view.MatchesKey(3, []byte("bravo")) ||
		cap(view.RawKey) != len(view.RawKey) {
		t.Fatalf("intent view=%+v err=%v", view, err)
	}
	reencoded, err := AppendTransactionIntent(nil, view.ID, view.Relation, view.RawKey)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("intent canonical reencode equal=%t err=%v", bytes.Equal(reencoded, encoded), err)
	}

	// This is a valid intent value for another raw key. If it were returned
	// under alpha's digest key, mandatory raw-key verification rejects it.
	forged, err := AppendTransactionIntent(nil, id, 3, []byte("bravo"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenTransactionIntentForKey(forged, 3, rawKey); !errors.Is(err, ErrTransactionStateCorrupt) {
		t.Fatalf("collision-verification error=%v", err)
	}
}

func TestTransactionSystemRowsRejectCorruptionReservedAndExhaustion(t *testing.T) {
	control, err := AppendTransactionControl(nil, transactionCodecControl(t))
	if err != nil {
		t.Fatal(err)
	}
	id, payload := transactionCodecCoordinatorPayload(t)
	payloadRow, err := AppendTransactionCoordinatorPayload(nil, id, distributedtxn.ReplicatedPayloadCoordinator, payload)
	if err != nil {
		t.Fatal(err)
	}
	manifestID, segment, _ := transactionCodecManifestSegment(t)
	manifest, err := AppendTransactionManifestPage(nil, manifestID, segment)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := AppendTransactionNativeMutation(nil, id, 1, 0, transactionCodecMutation())
	if err != nil {
		t.Fatal(err)
	}
	intent, err := AppendTransactionIntent(nil, id, 1, transactionCodecMutation().Key)
	if err != nil {
		t.Fatal(err)
	}
	participants := make([]distributedtxn.ParticipantRef, distributedtxn.MaxManifestPageParticipants)
	identities := make([]byte, distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2)
	for _, test := range []struct {
		name     string
		row      []byte
		reserved int
		open     func([]byte) error
	}{
		{"control", control, 268, func(raw []byte) error {
			var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
			_, err := OpenTransactionControlInto(raw, scopes[:])
			return err
		}},
		{"payload", payloadRow, 72, func(raw []byte) error { _, err := OpenTransactionCoordinatorPayload(raw); return err }},
		{"manifest", manifest, 88, func(raw []byte) error {
			_, err := OpenTransactionManifestPageInto(raw, participants, identities)
			return err
		}},
		{"mutation", mutation, 120, func(raw []byte) error { _, err := OpenTransactionNativeMutation(raw); return err }},
		{"intent", intent, 72, func(raw []byte) error { _, err := OpenTransactionIntent(raw); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := bytes.Clone(test.row)
			corrupt[len(corrupt)/2] ^= 1
			if err := test.open(corrupt); !errors.Is(err, ErrTransactionStateCorrupt) {
				t.Fatalf("corruption error=%v", err)
			}
			reserved := bytes.Clone(test.row)
			reserved[test.reserved] = 1
			switch test.name {
			case "control":
				sealRecord(reserved, transactionControlChecksumDomain)
			case "payload":
				sealRecord(reserved, transactionPayloadChecksumDomain)
			case "manifest":
				sealRecord(reserved, transactionManifestChecksumDomain)
			case "mutation":
				sealRecord(reserved, transactionMutationChecksumDomain)
			case "intent":
				sealRecord(reserved, transactionIntentChecksumDomain)
			}
			if err := test.open(reserved); !errors.Is(err, ErrTransactionStateCorrupt) {
				t.Fatalf("reserved-byte error=%v", err)
			}
			if err := test.open(test.row[:len(test.row)-1]); !errors.Is(err, ErrTransactionStateCorrupt) {
				t.Fatalf("truncation error=%v", err)
			}
			trailing := append(bytes.Clone(test.row), 0)
			if err := test.open(trailing); !errors.Is(err, ErrTransactionStateCorrupt) {
				t.Fatalf("trailing-byte error=%v", err)
			}
		})
	}
}

func TestTransactionSystemRowsWarmPathsAllocateNothing(t *testing.T) {
	control := transactionCodecControl(t)
	controlEncoded, err := AppendTransactionControl(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	controlScratch := make([]byte, 0, len(controlEncoded))
	var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope

	id, payload := transactionCodecCoordinatorPayload(t)
	payloadEncoded, err := AppendTransactionCoordinatorPayload(nil, id, distributedtxn.ReplicatedPayloadCoordinator, payload)
	if err != nil {
		t.Fatal(err)
	}
	payloadScratch := make([]byte, 0, len(payloadEncoded))

	pageID, segment, _ := transactionCodecManifestSegment(t)
	pageEncoded, err := AppendTransactionManifestPage(nil, pageID, segment)
	if err != nil {
		t.Fatal(err)
	}
	pageScratch := make([]byte, 0, len(pageEncoded))
	participants := make([]distributedtxn.ParticipantRef, distributedtxn.MaxManifestPageParticipants)
	identities := make([]byte, distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2)

	mutation := transactionCodecMutation()
	mutationEncoded, err := AppendTransactionNativeMutation(nil, id, 1, 0, mutation)
	if err != nil {
		t.Fatal(err)
	}
	mutationScratch := make([]byte, 0, len(mutationEncoded))
	intentEncoded, err := AppendTransactionIntent(nil, id, 1, mutation.Key)
	if err != nil {
		t.Fatal(err)
	}
	intentScratch := make([]byte, 0, len(intentEncoded))

	if allocations := testing.AllocsPerRun(100, func() {
		transactionCodecBytesSink, err = AppendTransactionControl(controlScratch[:0], control)
		if err != nil {
			panic(err)
		}
		transactionCodecControlSink, err = OpenTransactionControlInto(controlEncoded, scopes[:])
		if err != nil {
			panic(err)
		}
	}); allocations != 0 && !(raceDetectorEnabled && allocations <= 2) {
		t.Fatalf("control warm allocations=%v", allocations)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		transactionCodecBytesSink, err = AppendTransactionCoordinatorPayload(
			payloadScratch[:0], id, distributedtxn.ReplicatedPayloadCoordinator, payload,
		)
		if err != nil {
			panic(err)
		}
		transactionCodecPayloadSink, err = OpenTransactionCoordinatorPayload(payloadEncoded)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 && !(raceDetectorEnabled && allocations <= 2) {
		t.Fatalf("payload warm allocations=%v", allocations)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		transactionCodecBytesSink, err = AppendTransactionManifestPage(pageScratch[:0], pageID, segment)
		if err != nil {
			panic(err)
		}
		transactionCodecPageSink, err = OpenTransactionManifestPageInto(pageEncoded, participants, identities)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 && !(raceDetectorEnabled && allocations <= 2) {
		t.Fatalf("manifest warm allocations=%v", allocations)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		transactionCodecBytesSink, err = AppendTransactionNativeMutation(mutationScratch[:0], id, 1, 0, mutation)
		if err != nil {
			panic(err)
		}
		transactionCodecMutationSink, err = OpenTransactionNativeMutation(mutationEncoded)
		if err != nil {
			panic(err)
		}
		transactionCodecBytesSink, err = AppendTransactionIntent(intentScratch[:0], id, 1, mutation.Key)
		if err != nil {
			panic(err)
		}
		transactionCodecIntentSink, err = OpenTransactionIntent(intentEncoded)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 && !(raceDetectorEnabled && allocations <= 2) {
		t.Fatalf("mutation+intent warm allocations=%v", allocations)
	}
}

func FuzzTransactionSystemRowsCanonical(f *testing.F) {
	control, _ := AppendTransactionControl(nil, transactionCodecControl(f))
	id, payload := transactionCodecCoordinatorPayload(f)
	payloadRow, _ := AppendTransactionCoordinatorPayload(nil, id, distributedtxn.ReplicatedPayloadCoordinator, payload)
	pageID, segment, _ := transactionCodecManifestSegment(f)
	page, _ := AppendTransactionManifestPage(nil, pageID, segment)
	mutation, _ := AppendTransactionNativeMutation(nil, id, 1, 0, transactionCodecMutation())
	intent, _ := AppendTransactionIntent(nil, id, 1, transactionCodecMutation().Key)
	for tag, row := range [][]byte{control, payloadRow, page, mutation, intent} {
		seed := append([]byte{byte(tag)}, row...)
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) < 2 {
			return
		}
		tag, row := int(raw[0]), raw[1:]
		var rebuilt []byte
		var err error
		switch tag {
		case 0:
			var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
			view, openErr := OpenTransactionControlInto(row, scopes[:])
			if openErr != nil {
				return
			}
			rebuilt, err = AppendTransactionControl(nil, view.TransactionControl)
		case 1:
			view, openErr := OpenTransactionCoordinatorPayload(row)
			if openErr != nil {
				return
			}
			rebuilt, err = AppendTransactionCoordinatorPayload(nil, view.ID, view.Kind, view.Payload)
		case 2:
			participants := make([]distributedtxn.ParticipantRef, distributedtxn.MaxManifestPageParticipants)
			identities := make([]byte, distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2)
			view, openErr := OpenTransactionManifestPageInto(row, participants, identities)
			if openErr != nil {
				return
			}
			rebuilt, err = AppendTransactionManifestPage(nil, view.ID, distributedtxn.ManifestSegment{
				Index: view.Index, FirstParticipant: view.FirstParticipant,
				ParticipantCount: view.ParticipantCount, Digest: view.Digest, Raw: view.Raw,
			})
		case 3:
			view, openErr := OpenTransactionNativeMutation(row)
			if openErr != nil {
				return
			}
			rebuilt, err = AppendTransactionNativeMutation(nil, view.ID, view.Relation, view.Ordinal, view.Mutation)
		case 4:
			view, openErr := OpenTransactionIntent(row)
			if openErr != nil {
				return
			}
			rebuilt, err = AppendTransactionIntent(nil, view.ID, view.Relation, view.RawKey)
		default:
			return
		}
		if err != nil || !bytes.Equal(rebuilt, row) {
			t.Fatalf("tag=%d accepted noncanonical row: rebuilt=%d row=%d err=%v", tag, len(rebuilt), len(row), err)
		}
	})
}
