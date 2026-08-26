package distributedtxn

import (
	"bytes"
	"errors"
	"testing"
)

func TestReplicatedCoordinatorAuthorityWitnessesAreStrictAndAllocationFree(t *testing.T) {
	refs := []ParticipantRef{manifestParticipant(1), manifestParticipant(2)}
	inline, err := AppendCoordinator(nil, CoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, Participants: refs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateReplicatedCoordinatorAuthorityWitnesses(inline); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if validateErr := ValidateReplicatedCoordinatorAuthorityWitnesses(inline); validateErr != nil {
			panic(validateErr)
		}
	}); allocations != 0 {
		t.Fatalf("inline allocations=%v, want 0", allocations)
	}

	staticRefs := append([]ParticipantRef(nil), refs...)
	staticRefs[1].AuthorityWitness = AuthorityWitness{}
	staticInline, err := AppendCoordinator(nil, CoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, Participants: staticRefs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenCoordinator(staticInline); err != nil {
		t.Fatalf("common static decoder rejected zero witness: %v", err)
	}
	if err = ValidateReplicatedCoordinatorAuthorityWitnesses(staticInline); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replicated inline zero witness error=%v", err)
	}
	group, incarnation := testID(), testID()
	group[0], incarnation[0] = 1, 2
	fused := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedBeginPrepareCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadCoordinator, Payload: staticInline,
		Participant: ParticipantStage{
			CoordinatorGroup: group, CoordinatorShardIncarnation: incarnation,
			CoordinatorAllocation: 1, MutationDigest: refs[0].MutationDigest,
		},
	}
	if _, err = AppendReplicatedCommand(nil, fused); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("fused inline codec accepted zero unselected witness: %v", err)
	}

	descriptor, pages := buildManifest(t, 4097)
	coordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	pageCount := len(pages)
	if pageCount > MaxManifestSegmentsPerCommand {
		pageCount = MaxManifestSegmentsPerCommand
	}
	start := bytes.Clone(coordinator)
	for index := 0; index < pageCount; index++ {
		start = append(start, pages[index]...)
	}
	if err = ValidateReplicatedCoordinatorAuthorityWitnesses(start); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if validateErr := ValidateReplicatedCoordinatorAuthorityWitnesses(start); validateErr != nil {
			panic(validateErr)
		}
	}); allocations != 0 {
		t.Fatalf("segmented allocations=%v, want 0", allocations)
	}
}

func TestManifestSequenceAuthorityWitnessesRejectZeroWithoutChangingStaticGrammar(t *testing.T) {
	refs := []ParticipantRef{manifestParticipant(10), manifestParticipant(11)}
	refs[1].AuthorityWitness = AuthorityWitness{}
	pageArena := make([]byte, ManifestSegmentBytes)
	var raw []byte
	builder, err := NewManifestBuilder(pageArena, func(segment ManifestSegment) error {
		raw = bytes.Clone(segment.Raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := range refs {
		if err = builder.Append(refs[index]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = builder.Seal(); err != nil {
		t.Fatal(err)
	}
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	if _, err = OpenManifestSegment(raw, participants, identities); err != nil {
		t.Fatalf("common static decoder rejected zero witness: %v", err)
	}
	sequence, err := OpenManifestSegmentSequence(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = sequence.ValidateAuthorityWitnesses(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replicated sequence zero witness error=%v", err)
	}
}
