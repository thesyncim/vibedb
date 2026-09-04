package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func testRequestLedgerCommand() Command {
	command := testSessionRetireCommand()
	command.Kind = CommandRequestLedger
	command.AuthorityClass = CommandAuthorityRequestLedger
	key := requestledger.RequestKey{
		Scope:     requestledger.ScopeAuthenticated,
		Principal: requestledger.PrincipalID{1}, Request: requestledger.RequestID{2},
		TenantDigest: requestledger.Digest{3},
	}
	plan, err := requestledger.AppendPlan(nil, []byte("canonical recipe"))
	if err != nil {
		panic(err)
	}
	head, err := requestledger.NewHeadWithContract(
		key, requestledger.Digest{4}, requestledger.Digest{5}, plan,
	)
	if err != nil {
		panic(err)
	}
	home, err := requestledger.Home(key)
	if err != nil {
		panic(err)
	}
	payload, err := requestledger.AppendHead(nil, head)
	if err != nil {
		panic(err)
	}
	body, err := requestledger.AppendCommand(nil, requestledger.Command{
		Operation: requestledger.OperationCreate, Revision: head.Revision,
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest,
		PlanRoot: head.PlanRoot, SubjectDigest: head.TerminalContractDigest,
		ExpectedRangeIdentity: requestledger.Digest{6}, Payload: payload,
		Home: home,
	})
	if err != nil {
		panic(err)
	}
	command.RequestLedger = body
	return command
}

func TestRequestLedgerCommandRoundTripBorrowed(t *testing.T) {
	command := testRequestLedgerCommand()
	encoded := encodeCommand(t, command)
	if encoded[10] != 9 {
		t.Fatalf("request-ledger wire kind = %d, want 9", encoded[10])
	}
	view, err := OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind() != CommandRequestLedger || view.RelationCount() != 0 ||
		view.MutationCount() != 0 ||
		!bytes.Equal(view.RequestLedgerBytes(), command.RequestLedger) {
		t.Fatalf("request ledger view = kind:%d relations:%d mutations:%d body:%x",
			view.Kind(), view.RelationCount(), view.MutationCount(), view.RequestLedgerBytes())
	}
	if cap(view.RequestLedgerBytes()) != len(view.RequestLedgerBytes()) {
		t.Fatal("request ledger bytes are not capacity clamped")
	}
	var steps [requestledger.MaxPendingWaveSteps]requestledger.StepRef
	inner, err := view.OpenRequestLedgerInto(steps[:])
	if err != nil || inner.Operation != requestledger.OperationCreate || inner.KeyDigest == (requestledger.Digest{}) {
		t.Fatalf("open inner ledger command = %+v, %v", inner.Command, err)
	}
	identity, ok := view.RequestLedgerIdentity()
	if !ok || identity.Operation != inner.Operation || identity.KeyDigest != inner.KeyDigest ||
		identity.RequestDigest != inner.RequestDigest || identity.PlanRoot != inner.PlanRoot ||
		identity.RangeIdentity != inner.ExpectedRangeIdentity {
		t.Fatalf("cached ledger identity = %+v, valid=%t", identity, ok)
	}
	if _, ok := (CommandView{}).RequestLedgerIdentity(); ok {
		t.Fatal("empty command advertised a ledger identity")
	}
	if got := testing.AllocsPerRun(1000, func() {
		opened, openErr := OpenCommand(encoded)
		if openErr != nil || opened.Kind() != CommandRequestLedger ||
			len(opened.RequestLedgerBytes()) != len(command.RequestLedger) {
			panic(openErr)
		}
	}); got != 0 {
		t.Fatalf("OpenCommand allocations = %v, want 0", got)
	}
}

func TestRequestLedgerRejectsRouteGateWireKind(t *testing.T) {
	encoded := encodeCommand(t, testRequestLedgerCommand())
	encoded[10] = 8
	sealEnvelope(encoded)
	if _, err := OpenCommand(encoded); !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("wire kind 8 error = %v, want %v", err, ErrEnvelopeSemantic)
	}
}

func TestRequestLedgerRejectsMalformedInnerCommand(t *testing.T) {
	command := testRequestLedgerCommand()
	valid := append([]byte(nil), command.RequestLedger...)
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"truncated", func(raw []byte) { raw[len(raw)-1] ^= 1 }},
		{"unknown_operation", func(raw []byte) { raw[8] = 0xff }},
		{"zero_range_identity", func(raw []byte) { clear(raw[160:192]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := append([]byte(nil), valid...)
			tc.mutate(candidate)
			command.RequestLedger = candidate
			if _, err := CommandSize(command); !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("CommandSize error = %v, want %v", err, ErrEnvelopeSemantic)
			}
		})
	}
}

func TestRequestLedgerRejectsRelationCountFields(t *testing.T) {
	valid := encodeCommand(t, testRequestLedgerCommand())
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"mutation_count", func(raw []byte) { binary.LittleEndian.PutUint32(raw[24:28], 1) }},
		{"relation_count", func(raw []byte) { binary.LittleEndian.PutUint16(raw[28:30], 1) }},
		{"inline_relation", func(raw []byte) { binary.LittleEndian.PutUint16(raw[30:32], 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := append([]byte(nil), valid...)
			tc.mutate(candidate)
			sealEnvelope(candidate)
			if _, err := OpenCommand(candidate); !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("OpenCommand error = %v, want %v", err, ErrEnvelopeSemantic)
			}
		})
	}
}

func TestTransactionRelationCountMatrixRemainsClosed(t *testing.T) {
	withRelations := encodeCommand(t, testTargetStageCommand(t))
	withoutRelationsCommand := testTransactionTransitionCommand(t, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleTarget,
		Operation: distributedtxn.ReplicatedPrepareTarget,
		ID:        transactionControlID(0xd7), ExpectedRevision: 1,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	})
	withoutRelations := encodeCommand(t, withoutRelationsCommand)

	for _, tc := range []struct {
		name   string
		base   []byte
		mutate func([]byte)
	}{
		{"mutation_without_relation", withoutRelations, func(raw []byte) {
			binary.LittleEndian.PutUint32(raw[24:28], 1)
		}},
		{"relation_without_mutation", withoutRelations, func(raw []byte) {
			binary.LittleEndian.PutUint16(raw[28:30], 1)
			binary.LittleEndian.PutUint16(raw[30:32], 1)
		}},
		{"multi_relation_with_inline", withRelations, func(raw []byte) {
			binary.LittleEndian.PutUint16(raw[30:32], 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := append([]byte(nil), tc.base...)
			tc.mutate(candidate)
			sealEnvelope(candidate)
			if _, err := OpenCommand(candidate); !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("OpenCommand error = %v, want %v", err, ErrEnvelopeSemantic)
			}
		})
	}
}
