package gateway

import (
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// TestDurableSQLGatewaySharedTypedLedgerRecoversLostTerminalAndAck is the
// gateway protocol + typed durable-service integration boundary. It uses two
// independently constructed gateway services and only shares the typed ledger
// oracle. It deliberately does not claim RF3 transport or multiraft failover;
// TestDurableRequestLedgerRF3AppliesTypedCASAndReopensAuthoritativeAck covers
// the production RF3 codec/reopen boundary separately.
func TestDurableSQLGatewaySharedTypedLedgerRecoversLostTerminalAndAck(t *testing.T) {
	plan, head, continuation, pin := terminalCoordinatorFixture(t)
	result, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed: true, AffectedRows: plan.AffectedRows,
		Transaction:             replication.ID128(plan.Key.Request),
		CatalogGeneration:       head.CatalogGeneration,
		ShardsFanned:            1,
		TransitionTag:           head.TerminalTransitionTag,
		TerminalStateDigest:     replication.Digest(head.TerminalStateDigest),
		TerminalContractDigest:  replication.Digest(head.TerminalContractDigest),
		RetirementWitnessDigest: replication.Digest(plan.RetirementWitness),
		Payload:                 []byte("shared-ledger-terminal"),
	})
	if err != nil {
		t.Fatal(err)
	}
	plan.Result = result
	terminalLedger := &terminalCoordinatorLedger{head: head, continuation: continuation}
	coordinator, err := newDurableRequestTerminalCoordinator(terminalLedger, pin)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := coordinator.Complete(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	shared := &ackCollectorLedger{
		head: terminalLedger.head, terminal: terminal.Terminal,
		faultAck: true, faultGC: true,
	}
	participants := durableFaultParticipants(t)
	serviceA, err := newDurableRequestService(
		durableFaultTopology(t, participants), shared,
		typedServiceRunnerStop{}, new(typedServicePinStop),
	)
	if err != nil {
		t.Fatal(err)
	}
	serviceB, err := newDurableRequestService(
		durableFaultTopology(t, participants), shared,
		typedServiceRunnerStop{}, new(typedServicePinStop),
	)
	if err != nil {
		t.Fatal(err)
	}
	gatewayA := newSharedTypedLedgerSQLExecutor(t, serviceA)
	gatewayB := newSharedTypedLedgerSQLExecutor(t, serviceB)
	key, err := NewDurableRequestLedgerKey(
		plan.Key, replication.Digest(terminalLedger.head.RequestDigest),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Gateway A reopens the committed terminal, but its response is lost. A
	// replacement gateway must recover the same witnesses without invoking the
	// typed runner or relying on gateway-local memory.
	lost, found, err := gatewayA.Replay(t.Context(), key)
	if err != nil || !found {
		t.Fatalf("gateway A terminal reopen found=%v err=%v", found, err)
	}
	recovered, found, err := gatewayB.Replay(t.Context(), key)
	if err != nil || !found {
		t.Fatalf("gateway B terminal reopen found=%v err=%v", found, err)
	}
	if recovered.Key != lost.Key || recovered.TerminalRevision != lost.TerminalRevision ||
		recovered.ResultDigest != lost.ResultDigest || recovered.AckToken != lost.AckToken ||
		recovered.Result == nil || lost.Result == nil ||
		recovered.Result.TransactionID != lost.Result.TransactionID ||
		recovered.Result.RowsAffected != lost.Result.RowsAffected ||
		recovered.Result.Generation != lost.Result.Generation ||
		recovered.Result.ShardsFanned != lost.Result.ShardsFanned {
		t.Fatalf("replacement terminal drifted: lost=%+v recovered=%+v", lost, recovered)
	}

	wrong := recovered.AckToken
	wrong[0] ^= 0xff
	if _, err = gatewayB.Acknowledge(t.Context(), recovered.Key,
		recovered.TerminalRevision, recovered.ResultDigest, wrong,
	); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("wrong ACK capability error=%v", err)
	}

	// The shared oracle applies ACK and collection before returning injected
	// outcome-unknown errors. The production collector must reopen both states,
	// finish collection, and make the exact retry on gateway A write-free.
	acknowledged, err := gatewayB.Acknowledge(t.Context(), recovered.Key,
		recovered.TerminalRevision, recovered.ResultDigest, recovered.AckToken,
	)
	if err != nil || acknowledged.Ack.GCPhase != requestledger.AckGCComplete ||
		acknowledged.Rounds != 1 {
		t.Fatalf("gateway B ACK=%+v err=%v", acknowledged, err)
	}
	operations := len(shared.operations)
	replayed, err := gatewayA.Acknowledge(t.Context(), recovered.Key,
		recovered.TerminalRevision, recovered.ResultDigest, recovered.AckToken,
	)
	if err != nil || replayed.Ack.AckDigest != acknowledged.Ack.AckDigest ||
		len(shared.operations) != operations {
		t.Fatalf("gateway A ACK replay=%+v operations=%v err=%v",
			replayed, shared.operations, err)
	}
}

func newSharedTypedLedgerSQLExecutor(
	t *testing.T,
	service *DurableRequestService,
) *DurableSQLRequestExecutor {
	t.Helper()
	_, planner := replicatedSQLTransactionFixture(t, true)
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: data, Requests: service,
		RecoveryPulseLimit: 1, PlanningLeaseSpan: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}
