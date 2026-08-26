package distributedtxn

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"testing"
)

func replicatedTestCoordinator(t testing.TB) []byte {
	t.Helper()
	payload, err := AppendCoordinator(nil, CoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100,
		Participants: []ParticipantRef{{
			Distribution: []byte("docs"), Shard: []byte("-80"),
			RoutingVersion: 3, AllocationGeneration: 5, OwnershipEpoch: 7,
			MutationDigest: digest("mutation"), State: ParticipantStaged,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func replicatedTestParticipant() ReplicatedCommand {
	group, incarnation := testID(), testID()
	group[0], incarnation[0] = 1, 2
	return ReplicatedCommand{
		Role: ReplicatedRoleParticipant, Operation: ReplicatedStageParticipant,
		ID: testID(), PayloadKind: ReplicatedPayloadParticipantStage,
		Participant: ParticipantStage{
			CoordinatorGroup: group, CoordinatorShardIncarnation: incarnation,
			CoordinatorAllocation: 11, BucketBits: 8,
			IntentScopes:   []IntentScope{{Start: 1, End: 3}, {Start: 7, End: 9}},
			MutationDigest: digest("canonical-relation-batches"),
		},
	}
}

func TestReplicatedParticipantStageRoundTripCanonicalAndCompact(t *testing.T) {
	command := replicatedTestParticipant()
	encoded, err := AppendReplicatedCommand([]byte("prefix"), command)
	if err != nil {
		t.Fatal(err)
	}
	encoded = encoded[len("prefix"):]
	wantBytes := replicatedCommandHeaderBytes + len(command.Participant.IntentScopes)*8 + 4
	if len(encoded) != wantBytes {
		t.Fatalf("encoded bytes = %d, want %d", len(encoded), wantBytes)
	}
	scratch := make([]IntentScope, MaxIntentScopes)
	view, err := OpenReplicatedCommandInto(encoded, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplicatedCommand(encoded); err != nil {
		t.Fatalf("allocation-free validation: %v", err)
	}
	if !bytes.Equal(view.Bytes(), encoded) || view.ID != command.ID ||
		view.Participant.MutationDigest != command.Participant.MutationDigest ||
		!slices.Equal(view.Participant.IntentScopes, command.Participant.IntentScopes) ||
		len(view.Payload) != 0 {
		t.Fatalf("round trip = %+v", view.ReplicatedCommand)
	}
	reencoded, err := AppendReplicatedCommand(nil, view.Command())
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("canonical re-encode differs: %v", err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if _, openErr := OpenReplicatedCommandInto(encoded, scratch); openErr != nil {
			panic(openErr)
		}
	}); got != 0 {
		t.Fatalf("borrowed decode allocs = %v", got)
	}
}

func TestReplicatedCoordinatorRecordGrammarsAndTransitions(t *testing.T) {
	inline := replicatedTestCoordinator(t)
	descriptor, pages := buildManifest(t, 4)
	manifestCoordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []ReplicatedCommand{
		{Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageCoordinator,
			ID: testID(), PayloadKind: ReplicatedPayloadCoordinator, Payload: inline},
		{Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestCoordinator,
			ID: testID(), PayloadKind: ReplicatedPayloadManifestCoordinator, Payload: manifestCoordinator},
		{Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestSegment,
			ID: testID(), ExpectedRevision: 1, PayloadKind: ReplicatedPayloadManifestSegment, Payload: pages[0]},
		{Role: ReplicatedRoleCoordinator, Operation: ReplicatedCommitCoordinator,
			ID: testID(), ExpectedRevision: 2, PayloadKind: ReplicatedPayloadNone},
		{Role: ReplicatedRoleParticipant, Operation: ReplicatedPrepareParticipant,
			ID: testID(), ExpectedRevision: 1, PayloadKind: ReplicatedPayloadNone},
	}
	for i, command := range cases {
		encoded, appendErr := AppendReplicatedCommand(nil, command)
		if appendErr != nil {
			t.Fatalf("case %d append: %v", i, appendErr)
		}
		if validateErr := ValidateReplicatedCommand(encoded); validateErr != nil {
			t.Fatalf("case %d validate: %v", i, validateErr)
		}
		view, openErr := OpenReplicatedCommand(encoded)
		if openErr != nil {
			t.Fatalf("case %d open: %v", i, openErr)
		}
		if !bytes.Equal(view.Payload, command.Payload) || !bytes.Equal(view.Bytes(), encoded) {
			t.Fatalf("case %d did not borrow exact bytes", i)
		}
		reencoded, reencodeErr := AppendReplicatedCommand(nil, view.Command())
		if reencodeErr != nil || !bytes.Equal(reencoded, encoded) {
			t.Fatalf("case %d re-encode: %v", i, reencodeErr)
		}
	}
}

func TestReplicatedCommandRejectsNonCanonicalAndMismatchedInputs(t *testing.T) {
	valid, err := AppendReplicatedCommand(nil, replicatedTestParticipant())
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(offset int, value byte) []byte {
		bad := bytes.Clone(valid)
		bad[offset] = value
		binary.LittleEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], castagnoli))
		return bad
	}
	for _, bad := range [][]byte{
		append(bytes.Clone(valid), 0),
		mutate(10, 1), mutate(20, 1), mutate(121, 1), mutate(124, 1),
		mutate(5, byte(ReplicatedRoleCoordinator)),
		mutate(7, byte(ReplicatedPayloadNone)),
	} {
		if _, openErr := OpenReplicatedCommand(bad); openErr == nil {
			t.Fatal("accepted trailing, reserved, role, or payload-kind corruption")
		}
		if validateErr := ValidateReplicatedCommand(bad); validateErr == nil {
			t.Fatal("validator accepted trailing, reserved, role, or payload-kind corruption")
		}
	}

	inline := replicatedTestCoordinator(t)
	inline[51] = 1 // reserved byte in the first VTC1 participant entry
	binary.LittleEndian.PutUint32(inline[len(inline)-4:], crc32.Checksum(inline[:len(inline)-4], castagnoli))
	if _, appendErr := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadCoordinator, Payload: inline,
	}); appendErr == nil {
		t.Fatal("accepted noncanonical embedded coordinator record")
	}

	wrong := replicatedTestParticipant()
	wrong.ExpectedRevision = 1
	if _, appendErr := AppendReplicatedCommand(nil, wrong); appendErr == nil {
		t.Fatal("accepted nonzero creation revision")
	}
	wrong = replicatedTestParticipant()
	wrong.Participant.IntentScopes[1] = IntentScope{Start: 2, End: 8}
	if _, appendErr := AppendReplicatedCommand(nil, wrong); appendErr == nil {
		t.Fatal("accepted overlapping participant scopes")
	}
}

func TestReplicatedCommandStrictBounds(t *testing.T) {
	command := replicatedTestParticipant()
	command.Participant.IntentScopes = make([]IntentScope, MaxIntentScopes)
	for i := range command.Participant.IntentScopes {
		start := uint32(i * 2)
		command.Participant.IntentScopes[i] = IntentScope{Start: start, End: start + 1}
	}
	command.Participant.BucketBits = 24
	encoded, err := AppendReplicatedCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenReplicatedCommandInto(encoded, make([]IntentScope, MaxIntentScopes-1)); err == nil {
		t.Fatal("accepted undersized caller scratch")
	}
	command.Participant.IntentScopes = append(command.Participant.IntentScopes, IntentScope{Start: 1000, End: 1001})
	if _, err = AppendReplicatedCommand(nil, command); err == nil {
		t.Fatal("accepted too many intent scopes")
	}
}

func TestReplicatedCommandPreSizedAppendAllocatesZero(t *testing.T) {
	participant := replicatedTestParticipant()
	participantBytes := replicatedCommandHeaderBytes + len(participant.Participant.IntentScopes)*8 + 4
	participantDst := make([]byte, 0, participantBytes)
	if got := testing.AllocsPerRun(1000, func() {
		var err error
		participantDst, err = AppendReplicatedCommand(participantDst[:0], participant)
		if err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("participant append allocs = %v", got)
	}

	_, pages := buildManifest(t, 4)
	segment := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestSegment,
		ID: testID(), ExpectedRevision: 1,
		PayloadKind: ReplicatedPayloadManifestSegment, Payload: pages[0],
	}
	segmentDst := make([]byte, 0, replicatedCommandHeaderBytes+len(pages[0])+4)
	if got := testing.AllocsPerRun(1000, func() {
		var err error
		segmentDst, err = AppendReplicatedCommand(segmentDst[:0], segment)
		if err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("manifest-segment append allocs = %v", got)
	}
}

func TestValidateReplicatedCommandAllocatesZero(t *testing.T) {
	participant, err := AppendReplicatedCommand(nil, replicatedTestParticipant())
	if err != nil {
		t.Fatal(err)
	}
	inline, err := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadCoordinator,
		Payload: replicatedTestCoordinator(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, pages := buildManifest(t, 4)
	segment, err := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestSegment,
		ID: testID(), ExpectedRevision: 1,
		PayloadKind: ReplicatedPayloadManifestSegment, Payload: pages[0],
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{
		"participant": participant, "inline": inline, "segment": segment,
	} {
		t.Run(name, func(t *testing.T) {
			if got := testing.AllocsPerRun(1000, func() {
				if validateErr := ValidateReplicatedCommand(raw); validateErr != nil {
					panic(validateErr)
				}
			}); got != 0 {
				t.Fatalf("validation allocs = %v", got)
			}
		})
	}
}
