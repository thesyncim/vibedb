package replication

import (
	"bytes"
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
	encoded, err := distributedtxn.AppendReplicatedCommand(nil, command)
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
		if openErr != nil || len(opened.TransactionBytes()) == 0 {
			panic(openErr)
		}
	}); got != 0 {
		t.Fatalf("OpenCommand allocations = %v, want 0", got)
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
			ExpectedRevision: 10, PayloadKind: distributedtxn.ReplicatedPayloadNone},
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
