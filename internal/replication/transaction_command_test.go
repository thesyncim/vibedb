package replication

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

func transactionControlID(seed byte) distributedtxn.ID {
	return distributedtxn.ID(testID(seed))
}

func encodeTransactionControl(
	t testing.TB,
	command distributedtxn.ReplicatedCommand,
) []byte {
	t.Helper()
	if command.ControllerEpoch == 0 {
		command.ControllerEpoch = 7
	}
	if command.ExecutionPinDigest == (distributedtxn.Digest{}) {
		command.ExecutionPinDigest = distributedtxn.Digest(sha256.Sum256([]byte("test-execution-pin")))
	}
	encoded, err := distributedtxn.AppendReplicatedCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testTransactionRetirementPayload(
	t testing.TB,
	summary distributedtxn.ReplicatedRetirementSummary,
) []byte {
	t.Helper()
	encoded, err := distributedtxn.AppendReplicatedRetirementSummary(nil, summary)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testParticipantStageCommand(t testing.TB) Command {
	t.Helper()
	command := testMultiRelationCommand()
	digest, err := TransactionMutationDigest(command.Batches)
	if err != nil {
		t.Fatal(err)
	}
	id := transactionControlID(0xd1)
	control := distributedtxn.ReplicatedCommand{
		Role:        distributedtxn.ReplicatedRoleParticipant,
		Operation:   distributedtxn.ReplicatedStageParticipant,
		ID:          id,
		PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            transactionControlID(0x31),
			CoordinatorShardIncarnation: transactionControlID(0x51),
			CoordinatorAllocation:       7,
			BucketBits:                  8,
			IntentScopes: []distributedtxn.IntentScope{
				{Start: 1, End: 3}, {Start: 7, End: 9},
			},
			MutationDigest: digest,
		},
	}
	command.Kind = CommandTransaction
	command.Transaction = encodeTransactionControl(t, control)
	command.ClientID = ID128(id)
	command.ClientEpoch = transactionParticipantEpoch
	command.ClientSequence = 1
	command.AckThrough = 0
	return command
}

func testFusedParticipantStageCommand(t testing.TB) Command {
	t.Helper()
	command := testParticipantStageCommand(t)
	view, err := distributedtxn.OpenReplicatedCommand(command.Transaction)
	if err != nil {
		t.Fatal(err)
	}
	control := view.Command()
	control.Operation = distributedtxn.ReplicatedStagePrepareParticipant
	control.Participant.ParticipantOrdinal = 7
	command.Transaction = encodeTransactionControl(t, control)
	sequence, err := TransactionClientSequence(command.Transaction)
	if err != nil {
		t.Fatal(err)
	}
	command.ClientSequence = sequence
	return command
}

func testTransactionTransitionCommand(
	t testing.TB,
	control distributedtxn.ReplicatedCommand,
) Command {
	t.Helper()
	command := testSessionRetireCommand()
	command.Kind = CommandTransaction
	command.Transaction = encodeTransactionControl(t, control)
	command.ClientID = ID128(control.ID)
	if control.Role == distributedtxn.ReplicatedRoleCoordinator {
		command.ClientEpoch = transactionCoordinatorEpoch
	} else {
		command.ClientEpoch = transactionParticipantEpoch
	}
	sequence, err := TransactionClientSequence(command.Transaction)
	if err != nil {
		t.Fatal(err)
	}
	command.ClientSequence = sequence
	command.AckThrough = 0
	return command
}

func TestTransactionParticipantStageRoundTripBorrowedAndZeroAllocation(t *testing.T) {
	command := testParticipantStageCommand(t)
	encoded := encodeCommand(t, command)
	view, err := OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind() != CommandTransaction || view.RelationCount() != len(command.Batches) ||
		view.MutationCount() != commandMutationCount(command) ||
		!bytes.Equal(view.TransactionBytes(), command.Transaction) {
		t.Fatalf("unexpected transaction view: kind=%d relations=%d mutations=%d", view.Kind(), view.RelationCount(), view.MutationCount())
	}
	if cap(view.TransactionBytes()) != len(view.TransactionBytes()) {
		t.Fatal("transaction bytes are not capacity clamped")
	}
	role, operation, ok := view.TransactionIdentity()
	if !ok || role != distributedtxn.ReplicatedRoleParticipant ||
		operation != distributedtxn.ReplicatedStageParticipant {
		t.Fatalf("transaction identity = (%d,%d,%t)", role, operation, ok)
	}
	var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
	control, err := view.OpenTransactionInto(scopes[:])
	if err != nil {
		t.Fatal(err)
	}
	if control.Operation != distributedtxn.ReplicatedStageParticipant ||
		control.Participant.MutationDigest == (distributedtxn.Digest{}) {
		t.Fatalf("unexpected control: %+v", control.ReplicatedCommand)
	}
	if got := testing.AllocsPerRun(1000, func() {
		opened, openErr := OpenCommand(encoded)
		openedRole, openedOperation, openedOK := opened.TransactionIdentity()
		if openErr != nil || len(opened.TransactionBytes()) == 0 || !openedOK ||
			openedRole != distributedtxn.ReplicatedRoleParticipant ||
			openedOperation != distributedtxn.ReplicatedStageParticipant {
			panic(openErr)
		}
	}); got != 0 {
		t.Fatalf("OpenCommand allocations = %v, want 0", got)
	}
	ordinary, err := OpenCommand(encodeCommand(t, testCommand()))
	if err != nil {
		t.Fatal(err)
	}
	if ordinaryRole, ordinaryOperation, ordinaryOK := ordinary.TransactionIdentity(); ordinaryOK || ordinaryRole != distributedtxn.ReplicatedRoleInvalid ||
		ordinaryOperation != distributedtxn.ReplicatedOperationInvalid {
		t.Fatalf("ordinary transaction identity = (%d,%d,%t)", ordinaryRole, ordinaryOperation, ordinaryOK)
	}
}

func TestFusedTransactionParticipantStagePrepareRoundTripBorrowedAndZeroAllocation(t *testing.T) {
	command := testFusedParticipantStageCommand(t)
	encoded := encodeCommand(t, command)
	view, err := OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	role, operation, ok := view.TransactionIdentity()
	if !ok || role != distributedtxn.ReplicatedRoleParticipant ||
		operation != distributedtxn.ReplicatedStagePrepareParticipant ||
		view.RelationCount() != len(command.Batches) ||
		view.MutationCount() != commandMutationCount(command) {
		t.Fatalf("fused transaction view = role:%d operation:%d ok:%t relations:%d mutations:%d",
			role, operation, ok, view.RelationCount(), view.MutationCount())
	}
	var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
	control, err := view.OpenTransactionInto(scopes[:])
	if err != nil || control.Participant.ParticipantOrdinal != 7 {
		t.Fatalf("fused transaction control=%+v err=%v", control.ReplicatedCommand, err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		opened, openErr := OpenCommand(encoded)
		openedRole, openedOperation, openedOK := opened.TransactionIdentity()
		if openErr != nil || !openedOK ||
			openedRole != distributedtxn.ReplicatedRoleParticipant ||
			openedOperation != distributedtxn.ReplicatedStagePrepareParticipant {
			panic(openErr)
		}
	}); got != 0 {
		t.Fatalf("fused OpenCommand allocations = %v, want 0", got)
	}
}

func TestFusedTransactionRelationBatchShapeAndDigestAreExact(t *testing.T) {
	fused := testFusedParticipantStageCommand(t)
	withoutBatches := fused
	withoutBatches.Batches = nil
	if _, err := AppendCommand(nil, withoutBatches); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("fused prepare without relation batches error=%v", err)
	}

	view, err := distributedtxn.OpenReplicatedCommand(fused.Transaction)
	if err != nil {
		t.Fatal(err)
	}
	wrongDigest := view.Command()
	wrongDigest.Participant.MutationDigest[0] ^= 1
	fused.Transaction = encodeTransactionControl(t, wrongDigest)
	if _, err = AppendCommand(nil, fused); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("fused prepare with mismatched mutation digest error=%v", err)
	}

	id := transactionControlID(0xef)
	applyRelease := testTransactionTransitionCommand(t, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedApplyReleaseParticipant,
		ID:        id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	})
	applyRelease.Batches = testMultiRelationCommand().Batches
	if _, err = AppendCommand(nil, applyRelease); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("fused apply-release with relation batches error=%v", err)
	}
}

func TestTransactionTransitionsCarryNoRelationBatches(t *testing.T) {
	id := transactionControlID(0xe1)
	cases := []distributedtxn.ReplicatedCommand{
		{Role: distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedPrepareParticipant, ID: id,
			ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone},
		{Role: distributedtxn.ReplicatedRoleCoordinator,
			Operation: distributedtxn.ReplicatedCommitCoordinator, ID: id,
			ExpectedRevision: 9, PayloadKind: distributedtxn.ReplicatedPayloadNone},
		{Role: distributedtxn.ReplicatedRoleCoordinator,
			Operation: distributedtxn.ReplicatedRetireCoordinator, ID: id,
			ExpectedRevision: 10, PayloadKind: distributedtxn.ReplicatedPayloadRetirement,
			Payload: testTransactionRetirementPayload(
				t, distributedtxn.ReplicatedRetirementSummary{},
			)},
		{Role: distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedApplyReleaseParticipant, ID: id,
			ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone},
		{Role: distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedAbortReleaseParticipant, ID: id,
			ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone},
	}
	for _, control := range cases {
		command := testTransactionTransitionCommand(t, control)
		encoded := encodeCommand(t, command)
		view, err := OpenCommand(encoded)
		if err != nil || view.RelationCount() != 0 || view.MutationCount() != 0 ||
			!bytes.Equal(view.TransactionBytes(), command.Transaction) {
			t.Fatalf("operation %d round trip: view=%+v err=%v", control.Operation, view, err)
		}
		command.Batches = testCommand().Batches
		if _, err = AppendCommand(nil, command); !errors.Is(err, ErrEnvelopeSemantic) {
			t.Fatalf("operation %d accepted relation batches: %v", control.Operation, err)
		}
	}
}

func TestTransactionMutationDigestBindsExactCanonicalRelationBytes(t *testing.T) {
	command := testParticipantStageCommand(t)
	digest, err := TransactionMutationDigest(command.Batches)
	if err != nil {
		t.Fatal(err)
	}
	other := testParticipantStageCommand(t)
	other.Batches[2].Mutations[1].Value = []byte("LAST")
	if got, digestErr := TransactionMutationDigest(other.Batches); digestErr != nil || got == digest {
		t.Fatalf("changed canonical bytes did not change digest: digest=%x err=%v", got, digestErr)
	}
	if _, err = AppendCommand(nil, other); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("append accepted mismatched control digest: %v", err)
	}

	encoded := encodeCommand(t, command)
	encoded[len(encoded)-envelopeChecksumBytes-1] ^= 0x20
	sealEnvelope(encoded)
	if _, err = OpenCommand(encoded); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("open accepted altered relation bytes: %v", err)
	}

	single := testCommand().Batches
	first, err := TransactionMutationDigest(single)
	if err != nil {
		t.Fatal(err)
	}
	single[0].Relation++
	second, err := TransactionMutationDigest(single)
	if err != nil || first == second {
		t.Fatalf("compact relation identity not bound: first=%x second=%x err=%v", first, second, err)
	}
}

func TestStrictConditionalMutationKindsAreCanonicalAndDigestBound(t *testing.T) {
	if MutationPutAbsent != 6 || MutationPutPresent != 7 {
		t.Fatalf("strict conditional mutation codes drifted: %d/%d", MutationPutAbsent, MutationPutPresent)
	}
	base := testCommand()
	base.Batches[0].Mutations = []Mutation{{
		Kind: MutationPutAbsent, Key: []byte("key"), Value: []byte("value"),
	}}
	insertDigest, err := TransactionMutationDigest(base.Batches)
	if err != nil {
		t.Fatal(err)
	}
	encoded := encodeCommand(t, base)
	view, err := OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	relations := view.RelationBatches()
	if !relations.Next() {
		t.Fatal("missing strict insert relation")
	}
	mutations := relations.Batch().Mutations()
	if !mutations.Next() {
		t.Fatal("missing strict insert mutation")
	}
	opened := mutations.Mutation()
	if opened.Kind != MutationPutAbsent || !bytes.Equal(opened.Key, []byte("key")) ||
		!bytes.Equal(opened.Value, []byte("value")) || len(opened.Compare) != 0 {
		t.Fatalf("strict insert view = %+v", opened)
	}

	base.Batches[0].Mutations[0].Kind = MutationPutPresent
	updateDigest, err := TransactionMutationDigest(base.Batches)
	if err != nil || updateDigest == insertDigest {
		t.Fatalf("conditional update digest=%x insert=%x err=%v", updateDigest, insertDigest, err)
	}
	reencoded := encodeCommand(t, base)
	update, err := OpenCommand(reencoded)
	if err != nil {
		t.Fatal(err)
	}
	updateRelations := update.RelationBatches()
	updateRelations.Next()
	updateMutations := updateRelations.Batch().Mutations()
	updateMutations.Next()
	if got := updateMutations.Mutation(); got.Kind != MutationPutPresent ||
		!bytes.Equal(got.Value, []byte("value")) || len(got.Compare) != 0 {
		t.Fatalf("conditional update view = %+v", got)
	}
}

func TestTransactionClientIdentityIsCanonical(t *testing.T) {
	participantID := transactionControlID(0xf1)
	prepare := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant, ID: participantID,
		ExpectedRevision: 8, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}
	participant := testTransactionTransitionCommand(t, prepare)
	if participant.ClientEpoch != 2 || participant.ClientSequence != 9 || participant.AckThrough != 0 {
		t.Fatalf("participant tuple = (%d,%d,%d)", participant.ClientEpoch, participant.ClientSequence, participant.AckThrough)
	}

	decisionID := transactionControlID(0xa7)
	commit := distributedtxn.ReplicatedCommand{Role: distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator, ID: decisionID,
		ExpectedRevision: 17, PayloadKind: distributedtxn.ReplicatedPayloadNone}
	abort := commit
	abort.Operation = distributedtxn.ReplicatedAbortCoordinator
	commitBytes := encodeTransactionControl(t, commit)
	abortBytes := encodeTransactionControl(t, abort)
	commitSequence, err := TransactionClientSequence(commitBytes)
	if err != nil {
		t.Fatal(err)
	}
	abortSequence, err := TransactionClientSequence(abortBytes)
	if err != nil || commitSequence != abortSequence || commitSequence != transactionCoordinatorDecisionTag|17 {
		t.Fatalf("decision sequences commit=%d abort=%d err=%v", commitSequence, abortSequence, err)
	}
	retire := commit
	retire.Operation = distributedtxn.ReplicatedRetireCoordinator
	retire.PayloadKind = distributedtxn.ReplicatedPayloadRetirement
	retire.Payload = testTransactionRetirementPayload(
		t, distributedtxn.ReplicatedRetirementSummary{},
	)
	retireBytes := encodeTransactionControl(t, retire)
	retireSequence, err := TransactionClientSequence(retireBytes)
	if err != nil || retireSequence == commitSequence || retireSequence != transactionCoordinatorRetireTag|17 {
		t.Fatalf("retire sequence=%d decision=%d err=%v", retireSequence, commitSequence, err)
	}

	valid := testTransactionTransitionCommand(t, prepare)
	for _, mutate := range []func(*Command){
		func(c *Command) { c.ClientID[0] ^= 1 },
		func(c *Command) { c.ClientEpoch = 1 },
		func(c *Command) { c.ClientSequence++ },
		func(c *Command) { c.AckThrough = 1 },
		func(c *Command) { c.ExpectedDeadlineUnixNano = 1 },
	} {
		bad := valid
		mutate(&bad)
		if _, err := AppendCommand(nil, bad); !errors.Is(err, ErrEnvelopeSemantic) {
			t.Fatalf("accepted noncanonical transaction identity: %v", err)
		}
	}

	encoded := encodeCommand(t, valid)
	for _, offset := range []int{168, 184, 192, 248} {
		bad := bytes.Clone(encoded)
		bad[offset] ^= 1
		sealEnvelope(bad)
		if _, err := OpenCommand(bad); !errors.Is(err, ErrEnvelopeSemantic) {
			t.Fatalf("open accepted noncanonical transaction identity at %d: %v", offset, err)
		}
	}
}

func TestFusedTransactionClientSequencesAreCanonicalAndNonAliasing(t *testing.T) {
	id := transactionControlID(0xc3)
	fused := testFusedParticipantStageCommand(t)
	fusedView, err := distributedtxn.OpenReplicatedCommand(fused.Transaction)
	if err != nil {
		t.Fatal(err)
	}
	if fused.ClientEpoch != transactionParticipantEpoch || fused.ClientSequence != 1 {
		t.Fatalf("fused participant tuple=(%d,%d)", fused.ClientEpoch, fused.ClientSequence)
	}

	split := fusedView.Command()
	split.Operation = distributedtxn.ReplicatedStageParticipant
	split.Participant.ParticipantOrdinal = 0
	splitBytes := encodeTransactionControl(t, split)
	splitSequence, err := TransactionClientSequence(splitBytes)
	if err != nil || splitSequence != fused.ClientSequence || bytes.Equal(splitBytes, fused.Transaction) {
		t.Fatalf("split/fused identity sequence=%d/%d equal=%t err=%v",
			splitSequence, fused.ClientSequence, bytes.Equal(splitBytes, fused.Transaction), err)
	}
	// The shared creation sequence deliberately makes an old split command and
	// a fused command competing work under one participant retry identity. Their
	// fresh operation codes and exact envelope digests prevent either command
	// from being mistaken for the other's retry.
	splitOuter := fused
	splitOuter.Transaction = splitBytes
	fusedEncoded := encodeCommand(t, fused)
	splitEncoded := encodeCommand(t, splitOuter)
	if bytes.Equal(splitEncoded, fusedEncoded) {
		t.Fatal("split and fused participant commands aliased exact retry bytes")
	}
	fusedOpened, err := OpenCommand(fusedEncoded)
	if err != nil {
		t.Fatal(err)
	}
	splitOpened, err := OpenCommand(splitEncoded)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(fusedOpened.Bytes()) == sha256.Sum256(splitOpened.Bytes()) {
		t.Fatal("split and fused participant commands aliased logical command digest")
	}

	otherOrdinal := fusedView.Command()
	otherOrdinal.Participant.ParticipantOrdinal++
	otherOrdinalBytes := encodeTransactionControl(t, otherOrdinal)
	otherOrdinalSequence, err := TransactionClientSequence(otherOrdinalBytes)
	if err != nil || otherOrdinalSequence != fused.ClientSequence ||
		bytes.Equal(otherOrdinalBytes, fused.Transaction) {
		t.Fatalf("participant ordinal was not bound: sequence=%d err=%v equal=%t",
			otherOrdinalSequence, err, bytes.Equal(otherOrdinalBytes, fused.Transaction))
	}
	otherOuter := fused
	otherOuter.Transaction = otherOrdinalBytes
	otherEncoded := encodeCommand(t, otherOuter)
	otherOpened, err := OpenCommand(otherEncoded)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(fusedOpened.Bytes()) == sha256.Sum256(otherOpened.Bytes()) {
		t.Fatal("participant ordinal did not change logical command digest")
	}

	for _, operation := range []distributedtxn.ReplicatedOperation{
		distributedtxn.ReplicatedApplyReleaseParticipant,
		distributedtxn.ReplicatedAbortReleaseParticipant,
	} {
		control := distributedtxn.ReplicatedCommand{
			Role: distributedtxn.ReplicatedRoleParticipant, Operation: operation,
			ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
		}
		raw := encodeTransactionControl(t, control)
		sequence, sequenceErr := TransactionClientSequence(raw)
		if sequenceErr != nil || sequence != 3 {
			t.Fatalf("operation %d sequence=%d err=%v", operation, sequence, sequenceErr)
		}
	}
	direct := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedApplySingleParticipant,
		ID:        id, ExpectedRevision: 9,
		PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		Participant: fusedView.Participant,
	}
	directBytes := encodeTransactionControl(t, direct)
	directSequence, err := TransactionClientSequence(directBytes)
	if err != nil || directSequence != 9 {
		t.Fatalf("direct issuer sequence=%d err=%v", directSequence, err)
	}
	fence := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedAbortReleaseParticipant,
		ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID{1},
			CoordinatorShardIncarnation: distributedtxn.ID{2},
			CoordinatorAllocation:       3,
			MutationDigest:              distributedtxn.Digest{4},
			ParticipantOrdinal:          4096,
		},
	}
	fenceRaw := encodeTransactionControl(t, fence)
	fenceSequence, err := TransactionClientSequence(fenceRaw)
	if err != nil || fenceSequence != 1 || fenceSequence != fused.ClientSequence {
		t.Fatalf("abort fence sequence=%d prepare=%d err=%v",
			fenceSequence, fused.ClientSequence, err)
	}
	if bytes.Equal(fenceRaw, fused.Transaction) {
		t.Fatal("abort fence aliased stage+prepare bytes")
	}
	fenceOuter := testTransactionTransitionCommand(t, fence)
	if fenceOuter.ClientSequence != 1 || len(fenceOuter.Batches) != 0 {
		t.Fatalf("abort fence outer sequence=%d batches=%d",
			fenceOuter.ClientSequence, len(fenceOuter.Batches))
	}
	withBatches := fenceOuter
	withBatches.Batches = testMultiRelationCommand().Batches
	if _, err := AppendCommand(nil, withBatches); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("abort fence accepted relation batches: %v", err)
	}
}

func TestFusedFinishCommandsDoNotAliasSplitFinishCommands(t *testing.T) {
	id := transactionControlID(0xc9)
	for _, test := range []struct {
		name  string
		split distributedtxn.ReplicatedOperation
		fused distributedtxn.ReplicatedOperation
	}{
		{name: "apply", split: distributedtxn.ReplicatedApplyParticipant,
			fused: distributedtxn.ReplicatedApplyReleaseParticipant},
		{name: "abort", split: distributedtxn.ReplicatedAbortParticipant,
			fused: distributedtxn.ReplicatedAbortReleaseParticipant},
	} {
		t.Run(test.name, func(t *testing.T) {
			split := testTransactionTransitionCommand(t, distributedtxn.ReplicatedCommand{
				Role: distributedtxn.ReplicatedRoleParticipant, Operation: test.split,
				ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
			})
			fused := testTransactionTransitionCommand(t, distributedtxn.ReplicatedCommand{
				Role: distributedtxn.ReplicatedRoleParticipant, Operation: test.fused,
				ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
			})
			if split.ClientSequence != fused.ClientSequence {
				t.Fatalf("competing retry sequence=%d/%d", split.ClientSequence, fused.ClientSequence)
			}
			if bytes.Equal(split.Transaction, fused.Transaction) {
				t.Fatal("split and fused control bytes aliased")
			}
			splitEnvelope := encodeCommand(t, split)
			fusedEnvelope := encodeCommand(t, fused)
			if bytes.Equal(splitEnvelope, fusedEnvelope) ||
				sha256.Sum256(splitEnvelope) == sha256.Sum256(fusedEnvelope) {
				t.Fatal("split and fused finish envelopes aliased")
			}
		})
	}
}

func TestPackedManifestClientSequenceBindsFirstPageOrdinal(t *testing.T) {
	digest := distributedtxn.Digest{1}
	refs := transactionPerformanceParticipants(2048, digest)
	pageScratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	var pages [][]byte
	builder, err := distributedtxn.NewManifestBuilder(
		pageScratch,
		func(segment distributedtxn.ManifestSegment) error {
			pages = append(pages, bytes.Clone(segment.Raw))
			return nil
		},
	)
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
	if len(pages) < 2 {
		t.Fatalf("manifest pages=%d, want at least two", len(pages))
	}

	id := transactionControlID(0xd7)
	packed := bytes.Join(pages[1:], nil)
	control := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedAppendManifestSegments,
		ID:        id, ExpectedRevision: 1,
		PayloadKind: distributedtxn.ReplicatedPayloadManifestSegments,
		Payload:     packed,
	}
	packedBytes := encodeTransactionControl(t, control)
	sequence, err := TransactionClientSequence(packedBytes)
	if err != nil || sequence != 3 {
		t.Fatalf("packed first-page sequence=%d err=%v, want 3", sequence, err)
	}
	control.Payload = pages[1]
	singlePackedBytes := encodeTransactionControl(t, control)
	singleSequence, err := TransactionClientSequence(singlePackedBytes)
	if err != nil || singleSequence != sequence || bytes.Equal(singlePackedBytes, packedBytes) {
		t.Fatalf("packed retry namespace sequence=%d/%d equal=%t err=%v",
			singleSequence, sequence, bytes.Equal(singlePackedBytes, packedBytes), err)
	}

	legacy := control
	legacy.Operation = distributedtxn.ReplicatedStageManifestSegment
	legacy.PayloadKind = distributedtxn.ReplicatedPayloadManifestSegment
	legacyBytes := encodeTransactionControl(t, legacy)
	legacySequence, err := TransactionClientSequence(legacyBytes)
	if err != nil || legacySequence != sequence || bytes.Equal(legacyBytes, singlePackedBytes) {
		t.Fatalf("legacy/packed sequence=%d/%d equal=%t err=%v",
			legacySequence, sequence, bytes.Equal(legacyBytes, singlePackedBytes), err)
	}
}

func TestTransactionControlLengthAndAliasAreStrict(t *testing.T) {
	command := testParticipantStageCommand(t)
	encoded := encodeCommand(t, command)
	bodyStart := commandHeaderBytes + len(command.Tenant) + len(command.Distribution) + len(command.Shard)
	for _, length := range []uint32{0, uint32(len(command.Transaction) - 1), uint32(len(encoded))} {
		bad := bytes.Clone(encoded)
		appendU32(bad, bodyStart, length)
		sealEnvelope(bad)
		if _, err := OpenCommand(bad); err == nil {
			t.Fatalf("accepted transaction control length %d", length)
		}
	}

	total, err := CommandSize(command)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, total+len(command.Transaction))
	alias := dst[:cap(dst)][total-len(command.Transaction) : total]
	copy(alias, command.Transaction)
	command.Transaction = alias
	before := len(dst)
	if out, err := AppendCommand(dst, command); !errors.Is(err, ErrEnvelopeSemantic) || len(out) != before {
		t.Fatalf("transaction append alias: len=%d err=%v", len(out), err)
	}
}

func TestTransactionCommandSizePreflightIsExactAllocationFreeAndStrictlyOuter(t *testing.T) {
	commands := []Command{
		testFusedParticipantStageCommand(t),
		testTransactionTransitionCommand(t, distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleCoordinator,
			Operation: distributedtxn.ReplicatedCommitCoordinator,
			ID:        transactionControlID(0xe1), ExpectedRevision: 2,
			PayloadKind: distributedtxn.ReplicatedPayloadNone,
		}),
	}
	for index := range commands {
		command := commands[index]
		want, err := CommandSize(command)
		if err != nil {
			t.Fatal(err)
		}
		controlBytes := len(command.Transaction)
		preflight := command
		preflight.Transaction = nil
		preflight.Fingerprint = Digest{}
		got, err := TransactionCommandSize(preflight, controlBytes)
		if err != nil || got != want {
			t.Fatalf("case %d preflight=%d final=%d err=%v", index, got, want, err)
		}
		encoded, err := AppendCommand(make([]byte, 0, got), command)
		if err != nil || len(encoded) != got {
			t.Fatalf("case %d encoded=%d preflight=%d err=%v", index, len(encoded), got, err)
		}
		if allocations := testing.AllocsPerRun(1000, func() {
			size, sizeErr := TransactionCommandSize(preflight, controlBytes)
			if sizeErr != nil || size != want {
				panic(sizeErr)
			}
		}); allocations != 0 {
			t.Fatalf("case %d preflight allocations=%v, want 0", index, allocations)
		}
	}
}

func TestTransactionCommandSizePreflightRejectsBoundsWithoutClaimingControlSemantics(t *testing.T) {
	full := testFusedParticipantStageCommand(t)
	controlBytes := len(full.Transaction)
	preflight := full
	preflight.Transaction = nil
	preflight.Fingerprint = Digest{}

	if _, err := TransactionCommandSize(full, controlBytes); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("present control error=%v", err)
	}
	ordinary := testCommand()
	ordinary.Transaction = nil
	if _, err := TransactionCommandSize(ordinary, controlBytes); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("ordinary command error=%v", err)
	}
	for _, bytes := range []int{-1, 0} {
		if _, err := TransactionCommandSize(preflight, bytes); !errors.Is(err, ErrEnvelopeSemantic) {
			t.Fatalf("control bytes %d error=%v", bytes, err)
		}
	}
	if _, err := TransactionCommandSize(
		preflight, distributedtxn.MaxReplicatedCommandBytes+1,
	); !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("oversized control error=%v", err)
	}

	badIdentity := preflight
	badIdentity.ClientID = ID128{}
	if _, err := TransactionCommandSize(badIdentity, controlBytes); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("zero client identity error=%v", err)
	}
	badBatches := preflight
	badBatches.Batches = append([]RelationMutationBatch(nil), preflight.Batches...)
	badBatches.Batches[0].Relation = 0
	if _, err := TransactionCommandSize(badBatches, controlBytes); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("invalid relation batches error=%v", err)
	}
	badMutation := preflight
	badMutation.Batches = append([]RelationMutationBatch(nil), preflight.Batches...)
	badMutation.Batches[0].Mutations = append([]Mutation(nil), preflight.Batches[0].Mutations...)
	badMutation.Batches[0].Mutations[0].Key = nil
	if _, err := TransactionCommandSize(badMutation, controlBytes); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("invalid mutation error=%v", err)
	}

	terminal := testTransactionTransitionCommand(t, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator,
		ID:        transactionControlID(0xe2), ExpectedRevision: 2,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	})
	terminalBytes := bytes.Clone(terminal.Transaction)
	terminalFingerprint := terminal.Fingerprint
	terminalControlBytes := len(terminal.Transaction)
	terminal.Transaction = nil
	terminal.Fingerprint = Digest{}
	terminal.Batches = preflight.Batches
	if _, err := TransactionCommandSize(terminal, terminalControlBytes); err != nil {
		t.Fatalf("outer-only preflight inferred absent control semantics: %v", err)
	}
	terminal.Transaction = terminalBytes
	terminal.Fingerprint = terminalFingerprint
	if _, err := CommandSize(terminal); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("final semantic validation accepted mismatched control/batches: %v", err)
	}
}

func TestTransactionGrammarLeavesOrdinaryFramingByteIdentical(t *testing.T) {
	command, payload := transactionPerfCommand(1024)
	encoded := encodeCommand(t, command)
	if framing := len(encoded) - len(payload); framing != 297 {
		t.Fatalf("ordinary command framing = %dB, want unchanged 297B", framing)
	}
}

func FuzzOpenTransactionCommand(f *testing.F) {
	f.Add(encodeCommand(f, testParticipantStageCommand(f)))
	f.Add(encodeCommand(f, testTransactionTransitionCommand(f, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant,
		ID:        transactionControlID(0xb1), ExpectedRevision: 1,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	})))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxCommandBytes {
			return
		}
		view, err := OpenCommand(raw)
		if err != nil || view.Kind() != CommandTransaction {
			return
		}
		control := view.TransactionBytes()
		if len(control) == 0 || cap(control) != len(control) ||
			distributedtxn.ValidateReplicatedCommand(control) != nil {
			t.Fatal("successful transaction decode did not retain exact validated control")
		}
		if view.RelationCount() == 0 && view.MutationCount() != 0 {
			t.Fatal("zero relation transaction retained mutations")
		}
		relations := view.RelationBatches()
		seen := 0
		for relations.Next() {
			mutations := relations.Batch().Mutations()
			for mutations.Next() {
				seen++
			}
		}
		if seen != view.MutationCount() {
			t.Fatalf("iterated mutations = %d, header = %d", seen, view.MutationCount())
		}
	})
}
