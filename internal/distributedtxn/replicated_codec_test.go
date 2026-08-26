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
	descriptor, pages := buildManifest(t, 2048)
	manifestCoordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestStart := append(bytes.Clone(manifestCoordinator), pages[0]...)
	cases := []ReplicatedCommand{
		{Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageCoordinator,
			ID: testID(), PayloadKind: ReplicatedPayloadCoordinator, Payload: inline},
		{Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestCoordinator,
			ID: testID(), PayloadKind: ReplicatedPayloadManifestCoordinator, Payload: manifestStart},
		{Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestSegment,
			ID: testID(), ExpectedRevision: 1, PayloadKind: ReplicatedPayloadManifestSegment, Payload: pages[1]},
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
		if command.Operation == ReplicatedStageManifestCoordinator {
			coordinator, sequence, splitErr := OpenReplicatedManifestStart(view.Payload)
			if splitErr != nil || !bytes.Equal(coordinator, manifestCoordinator) ||
				sequence.Count() != 1 || !bytes.Equal(sequence.Bytes(), pages[0]) ||
				&coordinator[0] != &view.Payload[0] ||
				&sequence.Bytes()[0] != &view.Payload[ReplicatedManifestCoordinatorRecordBytes] {
				t.Fatalf("manifest-start borrowed split: %v", splitErr)
			}
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

	descriptor, pages := buildManifest(t, 2048)
	coordinator, appendErr := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100, Manifest: descriptor,
	})
	if appendErr != nil {
		t.Fatal(appendErr)
	}
	manifestCommand := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadManifestCoordinator,
	}
	manifestCommand.Payload = coordinator
	if _, appendErr = AppendReplicatedCommand(nil, manifestCommand); appendErr == nil {
		t.Fatal("accepted VTCM without atomic page zero")
	}
	manifestCommand.Payload = append(bytes.Clone(coordinator), pages[1]...)
	if _, appendErr = AppendReplicatedCommand(nil, manifestCommand); appendErr == nil {
		t.Fatal("accepted later page as atomic page zero")
	}
	later := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestSegment,
		ID: testID(), ExpectedRevision: 1,
		PayloadKind: ReplicatedPayloadManifestSegment, Payload: pages[0],
	}
	if _, appendErr = AppendReplicatedCommand(nil, later); appendErr == nil {
		t.Fatal("accepted page zero as a later manifest segment")
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

	descriptor, pages := buildManifest(t, 2048)
	segment := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestSegment,
		ID: testID(), ExpectedRevision: 1,
		PayloadKind: ReplicatedPayloadManifestSegment, Payload: pages[1],
	}
	segmentDst := make([]byte, 0, replicatedCommandHeaderBytes+len(pages[1])+4)
	if got := testing.AllocsPerRun(1000, func() {
		var err error
		segmentDst, err = AppendReplicatedCommand(segmentDst[:0], segment)
		if err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("manifest-segment append allocs = %v", got)
	}
	manifestCoordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100,
		Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestPayload := append(manifestCoordinator, pages[0]...)
	manifestStart := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadManifestCoordinator, Payload: manifestPayload,
	}
	manifestDst := make([]byte, 0, replicatedCommandHeaderBytes+len(manifestPayload)+4)
	if got := testing.AllocsPerRun(1000, func() {
		var appendErr error
		manifestDst, appendErr = AppendReplicatedCommand(manifestDst[:0], manifestStart)
		if appendErr != nil {
			panic(appendErr)
		}
	}); got != 0 {
		t.Fatalf("manifest-start append allocs = %v", got)
	}
}

func TestReplicatedCommandSizeExactParityAndAllocatesZero(t *testing.T) {
	participant := replicatedTestParticipant()
	size, err := ReplicatedCommandSize(participant)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := AppendReplicatedCommand(make([]byte, 0, size), participant)
	if err != nil || len(encoded) != size {
		t.Fatalf("participant size=%d encoded=%d err=%v", size, len(encoded), err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		measured, sizeErr := ReplicatedCommandSize(participant)
		if sizeErr != nil || measured != size {
			panic("replicated command size diverged")
		}
	}); got != 0 {
		t.Fatalf("replicated command size allocs = %v", got)
	}

	invalid := participant
	invalid.ID = ID{}
	if measured, sizeErr := ReplicatedCommandSize(invalid); sizeErr == nil || measured != 0 {
		t.Fatalf("invalid size=%d err=%v", measured, sizeErr)
	}
}

func TestAppendReplicatedCommandRejectsPayloadAppendRegionOverlapWithoutMutation(t *testing.T) {
	payload := replicatedTestCoordinator(t)
	base := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadCoordinator,
	}
	total := replicatedCommandHeaderBytes + len(payload) + replicatedCommandChecksumBytes
	tests := []struct {
		name  string
		start int
	}{
		{name: "exact_future_payload_destination", start: replicatedCommandHeaderBytes},
		{name: "fully_inside_append_region", start: 0},
		{name: "partial_append_region_overlap", start: total - len(payload)/2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backing := make([]byte, total+len(payload))
			copy(backing[tc.start:tc.start+len(payload)], payload)
			dst := backing[:0:total]
			command := base
			command.Payload = backing[tc.start : tc.start+len(payload) : tc.start+len(payload)]
			before := bytes.Clone(backing)
			got, err := AppendReplicatedCommand(dst, command)
			if err != ErrCorrupt {
				t.Fatalf("error=%v, want %v", err, ErrCorrupt)
			}
			if len(got) != len(dst) || !bytes.Equal(backing, before) {
				t.Fatal("overlap rejection modified destination backing")
			}
		})
	}

	command := base
	command.Payload = payload
	dst := make([]byte, 0, total)
	if got := testing.AllocsPerRun(1000, func() {
		var err error
		dst, err = AppendReplicatedCommand(dst[:0], command)
		if err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("non-overlapping append allocations=%v", got)
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
	descriptor, pages := buildManifest(t, 2048)
	segment, err := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestSegment,
		ID: testID(), ExpectedRevision: 1,
		PayloadKind: ReplicatedPayloadManifestSegment, Payload: pages[1],
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestCoordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestStart, err := AppendReplicatedCommand(nil, ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedStageManifestCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadManifestCoordinator,
		Payload: append(manifestCoordinator, pages[0]...),
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{
		"participant": participant, "inline": inline, "manifest-start": manifestStart,
		"segment": segment,
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

func fusedParticipantStage(operation ReplicatedOperation, ordinal uint32, mutation Digest) ParticipantStage {
	group, incarnation := testID(), testID()
	group[0], incarnation[0] = 31, 47
	return ParticipantStage{
		CoordinatorGroup: group, CoordinatorShardIncarnation: incarnation,
		CoordinatorAllocation: 19, BucketBits: 8,
		IntentScopes:   []IntentScope{{Start: 1, End: 4}, {Start: 8, End: 11}},
		MutationDigest: mutation, ParticipantOrdinal: ordinal,
	}
}

func appendManifestPages(dst []byte, pages [][]byte) []byte {
	for i := range pages {
		dst = append(dst, pages[i]...)
	}
	return dst
}

func TestFusedReplicatedOperationsUseFreshCanonicalCodes(t *testing.T) {
	if ReplicatedBeginPrepareCoordinator <= ReplicatedReleaseParticipant ||
		ReplicatedBeginPrepareManifestCoordinator <= ReplicatedReleaseParticipant ||
		ReplicatedAppendManifestSegments <= ReplicatedReleaseParticipant ||
		ReplicatedStagePrepareParticipant <= ReplicatedReleaseParticipant ||
		ReplicatedApplyReleaseParticipant <= ReplicatedReleaseParticipant ||
		ReplicatedAbortReleaseParticipant <= ReplicatedReleaseParticipant {
		t.Fatal("fused operation aliases an old split operation")
	}
	participant := ReplicatedCommand{
		Role: ReplicatedRoleParticipant, Operation: ReplicatedStagePrepareParticipant,
		ID: testID(), PayloadKind: ReplicatedPayloadParticipantStage,
		Participant: fusedParticipantStage(
			ReplicatedStagePrepareParticipant, 4096, digest("fused-participant"),
		),
	}
	encoded, err := AppendReplicatedCommand(nil, participant)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenReplicatedCommand(encoded)
	if err != nil || view.Operation != ReplicatedStagePrepareParticipant ||
		view.Participant.ParticipantOrdinal != 4096 {
		t.Fatalf("fused participant = %+v err=%v", view.ReplicatedCommand, err)
	}
	for _, operation := range []ReplicatedOperation{
		ReplicatedApplyReleaseParticipant, ReplicatedAbortReleaseParticipant,
	} {
		command := ReplicatedCommand{
			Role: ReplicatedRoleParticipant, Operation: operation, ID: testID(),
			ExpectedRevision: 2, PayloadKind: ReplicatedPayloadNone,
		}
		if _, err := AppendReplicatedCommand(nil, command); err != nil {
			t.Fatalf("operation %d: %v", operation, err)
		}
	}
	old := participant
	old.Operation = ReplicatedStageParticipant
	if _, err := AppendReplicatedCommand(nil, old); err == nil {
		t.Fatal("old stage accepted fused participant ordinal")
	}
}

func TestReplicatedParticipantAbortFenceIsCanonicalAndCompact(t *testing.T) {
	stage := fusedParticipantStage(
		ReplicatedAbortReleaseParticipant, 4096, digest("abort-fence"),
	)
	stage.BucketBits = 0
	stage.IntentScopes = nil
	command := ReplicatedCommand{
		Role: ReplicatedRoleParticipant, Operation: ReplicatedAbortReleaseParticipant,
		ID: testID(), PayloadKind: ReplicatedPayloadParticipantStage,
		Participant: stage,
	}
	encoded, err := AppendReplicatedCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != replicatedCommandHeaderBytes+replicatedCommandChecksumBytes {
		t.Fatalf("abort fence bytes=%d", len(encoded))
	}
	var scopes [MaxIntentScopes]IntentScope
	view, err := OpenReplicatedCommandInto(encoded, scopes[:])
	if err != nil || view.ExpectedRevision != 0 ||
		view.PayloadKind != ReplicatedPayloadParticipantStage ||
		view.Participant.MutationDigest != stage.MutationDigest ||
		view.Participant.ParticipantOrdinal != 4096 {
		t.Fatalf("abort fence=%+v err=%v", view.ReplicatedCommand, err)
	}
	reencoded, err := AppendReplicatedCommand(nil, view.Command())
	if err != nil || !bytes.Equal(encoded, reencoded) {
		t.Fatalf("abort fence canonical re-encode err=%v", err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if validateErr := ValidateReplicatedCommand(encoded); validateErr != nil {
			panic(validateErr)
		}
	}); got != 0 {
		t.Fatalf("abort fence validation allocations=%v", got)
	}
}

func TestReplicatedParticipantAbortFenceRejectsAmbiguousShapes(t *testing.T) {
	stage := fusedParticipantStage(
		ReplicatedAbortReleaseParticipant, 4096, digest("abort-fence"),
	)
	stage.BucketBits = 0
	stage.IntentScopes = nil
	valid := ReplicatedCommand{
		Role: ReplicatedRoleParticipant, Operation: ReplicatedAbortReleaseParticipant,
		ID: testID(), PayloadKind: ReplicatedPayloadParticipantStage,
		Participant: stage,
	}
	tests := []func(*ReplicatedCommand){
		func(c *ReplicatedCommand) { c.PayloadKind = ReplicatedPayloadNone },
		func(c *ReplicatedCommand) { c.ExpectedRevision = 2 },
		func(c *ReplicatedCommand) { c.Payload = []byte{1} },
		func(c *ReplicatedCommand) {
			c.Participant.BucketBits = 8
			c.Participant.IntentScopes = []IntentScope{{Start: 1, End: 2}}
		},
		func(c *ReplicatedCommand) { c.Participant.CoordinatorGroup = ID{} },
		func(c *ReplicatedCommand) { c.Participant.CoordinatorShardIncarnation = ID{} },
		func(c *ReplicatedCommand) { c.Participant.CoordinatorAllocation = 0 },
		func(c *ReplicatedCommand) { c.Participant.MutationDigest = Digest{} },
	}
	for index, mutate := range tests {
		candidate := valid
		mutate(&candidate)
		if _, err := AppendReplicatedCommand(nil, candidate); err == nil {
			t.Fatalf("accepted malformed abort fence %d", index)
		}
	}
	transition := valid
	transition.ExpectedRevision = 2
	transition.PayloadKind = ReplicatedPayloadNone
	transition.Participant = ParticipantStage{}
	if _, err := AppendReplicatedCommand(nil, transition); err != nil {
		t.Fatalf("ordinary abort-release rejected: %v", err)
	}
}

func TestFusedInlineCoordinatorBindsParticipantOrdinal(t *testing.T) {
	payload := replicatedTestCoordinator(t)
	stage := fusedParticipantStage(
		ReplicatedBeginPrepareCoordinator, 0, digest("mutation"),
	)
	command := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedBeginPrepareCoordinator,
		ID: testID(), PayloadKind: ReplicatedPayloadCoordinator,
		Payload: payload, Participant: stage,
	}
	encoded, err := AppendReplicatedCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplicatedCommand(encoded); err != nil {
		t.Fatal(err)
	}
	want := ParticipantRef{
		Distribution: []byte("docs"), Shard: []byte("-80"),
		RoutingVersion: 3, AllocationGeneration: 5, OwnershipEpoch: 7,
		MutationDigest: digest("mutation"), State: ParticipantStaged,
	}
	present, matches, err := ReplicatedCoordinatorBindsParticipant(payload, 0, want)
	if err != nil || !present || !matches {
		t.Fatalf("inline bind = %t/%t err=%v", present, matches, err)
	}
	command.Participant.MutationDigest[0] ^= 1
	if _, err := AppendReplicatedCommand(nil, command); err == nil {
		t.Fatal("inline begin accepted mismatched participant digest")
	}
}

func TestManifestSegmentSequenceFusedPackingAndOrdinalBinding(t *testing.T) {
	descriptor, pages := buildManifest(t, 17_000)
	if len(pages) <= MaxManifestSegmentsPerCommand {
		t.Fatalf("manifest pages=%d, want >%d", len(pages), MaxManifestSegmentsPerCommand)
	}
	initialBytes := appendManifestPages(nil, pages[:MaxManifestSegmentsPerCommand])
	initial, err := OpenManifestSegmentSequence(initialBytes)
	if err != nil || initial.Count() != MaxManifestSegmentsPerCommand ||
		initial.FirstIndex() != 0 || initial.FirstParticipant() != 0 ||
		initial.EncodedBytes() != uint64(len(initialBytes)) {
		t.Fatalf("initial sequence = %+v err=%v", initial, err)
	}
	var iterated int
	iterator := initial.Iterator()
	for iterator.Next() {
		segment := iterator.Segment()
		if segment.Index != uint32(iterated) || !bytes.Equal(segment.Raw, pages[iterated]) ||
			cap(segment.Raw) != len(segment.Raw) {
			t.Fatalf("segment %d = %+v", iterated, segment)
		}
		iterated++
	}
	if iterated != MaxManifestSegmentsPerCommand {
		t.Fatalf("iterated=%d", iterated)
	}

	coordinator, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 100, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	ordinal := uint32(initial.ParticipantCount() - 1)
	want := manifestParticipant(uint64(ordinal))
	command := ReplicatedCommand{
		Role:      ReplicatedRoleCoordinator,
		Operation: ReplicatedBeginPrepareManifestCoordinator,
		ID:        testID(), PayloadKind: ReplicatedPayloadManifestCoordinator,
		Payload: appendManifestPages(coordinator, pages[:MaxManifestSegmentsPerCommand]),
		Participant: fusedParticipantStage(
			ReplicatedBeginPrepareManifestCoordinator, ordinal, want.MutationDigest,
		),
	}
	encoded, err := AppendReplicatedCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplicatedCommand(encoded); err != nil {
		t.Fatal(err)
	}
	present, matches, err := ReplicatedCoordinatorBindsParticipant(
		command.Payload, uint64(ordinal), want,
	)
	if err != nil || !present || !matches {
		t.Fatalf("manifest bind = %t/%t err=%v", present, matches, err)
	}
	present, matches, err = ReplicatedCoordinatorBindsParticipant(
		command.Payload, initial.ParticipantCount(), manifestParticipant(initial.ParticipantCount()),
	)
	if err != nil || present || matches {
		t.Fatalf("deferred bind = %t/%t err=%v", present, matches, err)
	}

	laterBytes := appendManifestPages(nil, pages[MaxManifestSegmentsPerCommand:])
	later := ReplicatedCommand{
		Role: ReplicatedRoleCoordinator, Operation: ReplicatedAppendManifestSegments,
		ID: testID(), ExpectedRevision: 1,
		PayloadKind: ReplicatedPayloadManifestSegments, Payload: laterBytes,
	}
	if _, err := AppendReplicatedCommand(nil, later); err != nil {
		t.Fatal(err)
	}
	tooMany := appendManifestPages(nil, pages[:MaxManifestSegmentsPerCommand+1])
	if _, err := OpenManifestSegmentSequence(tooMany); err == nil {
		t.Fatal("accepted sixteen manifest pages")
	}
	nonMaximal := command
	nonMaximal.Payload = appendManifestPages(bytes.Clone(coordinator), pages[:1])
	if _, err := AppendReplicatedCommand(nil, nonMaximal); err == nil {
		t.Fatal("fused begin accepted non-maximal initial pack")
	}
}

func TestManifestSegmentSequenceStrictCorruptionAndZeroAllocation(t *testing.T) {
	_, pages := buildManifest(t, 4097)
	raw := appendManifestPages(nil, pages)
	sequence, err := OpenManifestSegmentSequence(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		opened, openErr := OpenManifestSegmentSequence(raw)
		if openErr != nil || opened.Count() != len(pages) {
			panic(openErr)
		}
	}); got != 0 {
		t.Fatalf("sequence open allocations=%v", got)
	}
	first := sequence.Iterator()
	if !first.Next() {
		t.Fatal("missing first page")
	}
	page := first.Segment()
	want := manifestParticipant(page.FirstParticipant)
	present, matches, err := ManifestSegmentMatchesParticipant(
		page.Raw, page.FirstParticipant, want,
	)
	if err != nil || !present || !matches {
		t.Fatalf("page match=%t/%t err=%v", present, matches, err)
	}
	present, matches, err = ManifestSegmentMatchesParticipant(
		page.Raw, page.FirstParticipant+uint64(page.ParticipantCount), want,
	)
	if err != nil || present || matches {
		t.Fatalf("outside match=%t/%t err=%v", present, matches, err)
	}

	bad := bytes.Clone(raw)
	bad[len(pages[0])+8] ^= 1
	binary.LittleEndian.PutUint32(
		bad[len(pages[0])+len(pages[1])-4:len(pages[0])+len(pages[1])],
		crc32.Checksum(bad[len(pages[0]):len(pages[0])+len(pages[1])-4], castagnoli),
	)
	if _, err := OpenManifestSegmentSequence(bad); err == nil {
		t.Fatal("accepted discontinuous page index")
	}
	if _, err := OpenManifestSegmentSequence(raw[:len(raw)-1]); err == nil {
		t.Fatal("accepted truncated page sequence")
	}
	if _, err := OpenManifestSegmentSequence(append(bytes.Clone(raw), 0)); err == nil {
		t.Fatal("accepted trailing sequence byte")
	}
}

func TestManifestSegmentSequenceFollowsStrictIdentityBoundary(t *testing.T) {
	_, pages := buildManifest(t, 4097)
	if len(pages) < 2 {
		t.Fatal("manifest did not cross a page boundary")
	}
	next, err := OpenManifestSegmentSequence(appendManifestPages(nil, pages[1:]))
	if err != nil {
		t.Fatal(err)
	}
	if err := ManifestSegmentSequenceFollows(pages[0], next); err != nil {
		t.Fatalf("ordered boundary: %v", err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if followErr := ManifestSegmentSequenceFollows(pages[0], next); followErr != nil {
			panic(followErr)
		}
	}); got != 0 {
		t.Fatalf("boundary validation allocations=%v", got)
	}

	onePage := func(participant ParticipantRef) []byte {
		t.Helper()
		arena := make([]byte, ManifestSegmentBytes)
		var raw []byte
		builder, buildErr := NewManifestBuilder(arena, func(segment ManifestSegment) error {
			raw = bytes.Clone(segment.Raw)
			return nil
		})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		if buildErr = builder.Append(participant); buildErr != nil {
			t.Fatal(buildErr)
		}
		if _, buildErr = builder.Seal(); buildErr != nil {
			t.Fatal(buildErr)
		}
		return raw
	}
	previous := onePage(manifestParticipant(100))
	for _, tc := range []struct {
		name  string
		index uint64
	}{
		{name: "equal", index: 100},
		{name: "reordered", index: 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate, openErr := OpenManifestSegmentSequence(onePage(manifestParticipant(tc.index)))
			if openErr != nil {
				t.Fatal(openErr)
			}
			if followErr := ManifestSegmentSequenceFollows(previous, candidate); followErr != ErrCorrupt {
				t.Fatalf("boundary error=%v, want %v", followErr, ErrCorrupt)
			}
		})
	}
}

func TestFusedReplicatedCommandExactMaximum(t *testing.T) {
	const want = 985336
	if MaxReplicatedCommandBytes != want {
		t.Fatalf("max replicated command=%d want=%d", MaxReplicatedCommandBytes, want)
	}
}
