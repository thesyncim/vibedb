package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

var errTypedServicePin = errors.New("typed service pin stop")

type typedServiceLedger struct {
	head       requestledger.HeadRecord
	terminal   requestledger.TerminalRecord
	applies    int
	reads      int
	operations []string
}

func (ledger *typedServiceLedger) ApplyCAS(
	_ context.Context,
	_ DurableRequestLedgerHome,
	key requestledger.RequestKey,
	cas DurableRequestLifecycleCAS,
) (DurableRequestLifecycleCASResult, error) {
	ledger.operations = append(ledger.operations, "apply")
	if cas.Operation != requestledger.OperationCreate || ledger.head.Revision != 0 ||
		cas.Head.Key != key || cas.Revision != 1 {
		return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
	}
	materialized, err := requestledger.MaterializeCreate(cas.Head, 2)
	if err != nil {
		return DurableRequestLifecycleCASResult{}, err
	}
	ledger.head = materialized
	ledger.applies++
	keyDigest, _ := requestledger.KeyDigest(key)
	return DurableRequestLifecycleCASResult{
		Ledger: replicatedstate.RequestLedgerCompletionResult{
			ResultCode: replicatedstate.ResultApplied, Operation: requestledger.OperationCreate,
			KeyDigest: keyDigest, RequestDigest: cas.Head.RequestDigest,
			PlanRoot: cas.Head.PlanRoot, PlanningLeaseExpiryIndex: materialized.PlanningLeaseExpiryIndex,
		},
		Applied: 2,
	}, nil
}

func (ledger *typedServiceLedger) ReadRow(
	_ context.Context,
	_ DurableRequestLedgerHome,
	read DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	ledger.reads++
	ledger.operations = append(ledger.operations, "read")
	switch read.Kind {
	case replicatedstate.RequestLedgerReadHead:
		if ledger.head.Revision == 0 {
			return DurableRequestLifecycleRow{Applied: 1}, nil
		}
		return DurableRequestLifecycleRow{Applied: 2, Found: true,
			Kind: replicatedstate.RequestLedgerReadHead, Head: ledger.head}, nil
	case replicatedstate.RequestLedgerReadTerminal:
		if ledger.terminal.Revision == 0 {
			return DurableRequestLifecycleRow{Applied: 2}, nil
		}
		return DurableRequestLifecycleRow{Applied: 3, Found: true,
			Kind: replicatedstate.RequestLedgerReadTerminal, Terminal: ledger.terminal}, nil
	default:
		return DurableRequestLifecycleRow{}, ErrDurableRequest
	}
}

func TestDurableRequestBeginFusesCreateWithoutPreflightRead(t *testing.T) {
	participants := durableFaultParticipants(t)
	request := durableFaultRequest(t, participants)
	ledger := new(typedServiceLedger)
	service, err := newDurableRequestService(
		durableFaultTopology(t, participants), ledger, typedServiceRunnerStop{}, new(typedServicePinStop),
	)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := service.Begin(t.Context(), request)
	if err != nil || !begin.Created || !begin.ProgramMatches || begin.Applied != 2 ||
		begin.PlanningLeaseExpiryIndex != 2+request.Program.Contract.PlanningLeaseSpan ||
		ledger.reads != 0 || len(ledger.operations) != 1 || ledger.operations[0] != "apply" {
		t.Fatalf("begin=%+v reads=%d operations=%v: %v", begin, ledger.reads, ledger.operations, err)
	}
	retry, err := service.Begin(t.Context(), request)
	if err != nil || retry.Created || !retry.ProgramMatches ||
		retry.PlanningLeaseExpiryIndex != begin.PlanningLeaseExpiryIndex || ledger.reads != 1 {
		t.Fatalf("retry=%+v reads=%d: %v", retry, ledger.reads, err)
	}
}

type typedServicePinStop struct {
	called int
}

func (pins *typedServicePinStop) AcquireOrRecover(
	_ context.Context,
	execution DurableRequestTypedExecutionContext,
) (ReplicatedRoute, executionpin.AcquireCertificate, executionpin.LeaseCertificate, error) {
	if execution.Key.RequestKey.Valid() && execution.Participants != nil {
		pins.called++
	}
	return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, errTypedServicePin
}

type typedServiceRunnerStop struct{}

func (typedServiceRunnerStop) RunTyped(
	context.Context,
	DurableRequestTypedExecutionContext,
) (DurableRequestTerminalResult, error) {
	return DurableRequestTerminalResult{}, errors.New("runner must not run")
}

func TestDurableRequestServiceAdmitsSealsAndReopensBeforePin(t *testing.T) {
	participants := durableFaultParticipants(t)
	request := durableFaultRequest(t, participants)
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	service, err := newDurableRequestService(
		durableFaultTopology(t, participants), ledger, typedServiceRunnerStop{}, pins,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Execute(t.Context(), request); !errors.Is(err, errTypedServicePin) {
		t.Fatalf("execute error=%v", err)
	}
	if ledger.applies != 1 || ledger.head.Phase != requestledger.PhaseSealed || pins.called != 1 ||
		ledger.head.Key != request.Key.RequestKey ||
		ledger.head.RequestDigest != requestledger.Digest(request.Key.Digest) {
		t.Fatalf("applies=%d phase=%d pins=%d head=%+v", ledger.applies, ledger.head.Phase, pins.called, ledger.head)
	}
}

func TestDurableRequestServiceReplaysTerminalFromReplicatedStateOnly(t *testing.T) {
	participants := durableFaultParticipants(t)
	request := durableFaultRequest(t, participants)
	resultRaw, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed: true, AffectedRows: 2, Transaction: request.Program.Identity.ID,
		CatalogGeneration:       request.Program.Identity.CatalogGeneration,
		ShardsFanned:            uint64(len(request.Program.Participants)),
		TransitionTag:           request.Program.Contract.CommitTransitionTag,
		TerminalStateDigest:     request.Program.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  request.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: request.Program.Contract.RetirementWitnessDigest,
		Payload:                 []byte("served-from-rf3-ledger"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger := &typedServiceLedger{
		head: requestledger.HeadRecord{
			Key: request.Key.RequestKey, RequestDigest: requestledger.Digest(request.Key.Digest),
			PlanRoot: requestledger.Digest{2}, Revision: 9, Phase: requestledger.PhaseTerminal,
		},
		terminal: requestledger.TerminalRecord{
			Revision: 9, Result: resultRaw, ResultDigest: requestledger.ResultDigest(resultRaw),
			RequestDigest:          requestledger.Digest(request.Key.Digest),
			PlanRoot:               requestledger.Digest{2},
			CatalogGeneration:      request.Program.Identity.CatalogGeneration,
			TerminalContractDigest: requestledger.Digest(request.Program.Contract.TerminalContractDigest),
			AckToken:               requestledger.AckToken{1},
		},
	}
	ledger.terminal.KeyDigest, err = requestledger.KeyDigest(request.Key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(
		durableFaultTopology(t, participants), ledger, typedServiceRunnerStop{}, new(typedServicePinStop),
	)
	if err != nil {
		t.Fatal(err)
	}
	outcome, found, err := service.Replay(t.Context(), request.Key)
	if err != nil || !found || !outcome.Committed || outcome.AffectedRows != 2 ||
		outcome.ID != request.Program.Identity.ID ||
		string(outcome.Result) != "served-from-rf3-ledger" || outcome.TerminalRevision != 9 ||
		outcome.ResultDigest != replication.Digest(requestledger.ResultDigest(resultRaw)) ||
		outcome.AckToken == (DurableRequestAckToken{}) {
		t.Fatalf("outcome=%+v found=%v err=%v", outcome, found, err)
	}
	if _, err = service.Acknowledge(t.Context(), request.Key, outcome.TerminalRevision+1,
		outcome.ResultDigest, outcome.AckToken); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("wrong terminal revision ACK error=%v", err)
	}
	wrongDigest := outcome.ResultDigest
	wrongDigest[0]++
	if _, err = service.Acknowledge(t.Context(), request.Key, outcome.TerminalRevision,
		wrongDigest, outcome.AckToken); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("wrong result digest ACK error=%v", err)
	}
}

var _ DurableRequestLedger = (*typedServiceLedger)(nil)
var _ DurableRequestExecutionPinAuthority = (*typedServicePinStop)(nil)
var _ DurableRequestTypedRunner = typedServiceRunnerStop{}
