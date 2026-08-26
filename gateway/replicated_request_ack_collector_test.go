package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

type ackCollectorLedger struct {
	head       requestledger.HeadRecord
	terminal   requestledger.TerminalRecord
	ack        requestledger.AckRecord
	faultAck   bool
	faultGC    bool
	operations []requestledger.Operation
}

func (ledger *ackCollectorLedger) ApplyCAS(
	_ context.Context,
	_ DurableRequestLedgerHome,
	_ requestledger.RequestKey,
	cas DurableRequestLifecycleCAS,
) (DurableRequestLifecycleCASResult, error) {
	ledger.operations = append(ledger.operations, cas.Operation)
	var err error
	switch cas.Operation {
	case requestledger.OperationAck:
		if ledger.ack.Revision != 0 || cas.ExpectedRevision != ledger.head.Revision ||
			cas.Revision != ledger.head.Revision+1 ||
			cas.Ack.TerminalRevision != ledger.terminal.Revision ||
			cas.Ack.ResultDigest != ledger.terminal.ResultDigest ||
			cas.Ack.AckToken != ledger.terminal.AckToken {
			return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
		}
		ledger.ack, err = requestledger.NewAck(
			ledger.head, ledger.terminal, cas.Revision, 4096,
		)
		if err == nil && ledger.faultAck {
			ledger.faultAck = false
			return DurableRequestLifecycleCASResult{}, errLifecycleRunnerFault
		}
	case requestledger.OperationGC:
		if ledger.ack.Revision == 0 || cas.ExpectedRevision != ledger.ack.Revision ||
			cas.Revision != ledger.ack.Revision+1 ||
			cas.GC.ExpectedAckDigest != ledger.ack.AckDigest {
			return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
		}
		remaining := ledger.ack.PriorEncodedBytes - ledger.ack.ReclaimedBytes
		ledger.ack, err = requestledger.AdvanceAckGC(
			ledger.ack, cas.GC, cas.Revision,
			ledger.ack.GCCursor+1, remaining, true,
		)
		if err == nil && ledger.faultGC {
			ledger.faultGC = false
			return DurableRequestLifecycleCASResult{}, errLifecycleRunnerFault
		}
	default:
		return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
	}
	if err != nil {
		return DurableRequestLifecycleCASResult{}, err
	}
	return DurableRequestLifecycleCASResult{
		Ledger: replicatedstate.RequestLedgerCompletionResult{
			ResultCode: replicatedstate.ResultApplied,
		},
		Applied: cas.Revision + 100,
	}, nil
}

func (ledger *ackCollectorLedger) ReadRow(
	_ context.Context,
	_ DurableRequestLedgerHome,
	read DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	if read.MinimumApplied == 0 {
		return DurableRequestLifecycleRow{}, ErrDurableRequestConflict
	}
	if ledger.ack.Revision != 0 {
		return DurableRequestLifecycleRow{
			Applied: ledger.ack.Revision + 100, Found: true,
			Kind: replicatedstate.RequestLedgerReadAck, Ack: ledger.ack,
		}, nil
	}
	switch read.Kind {
	case replicatedstate.RequestLedgerReadHead:
		return DurableRequestLifecycleRow{
			Applied: ledger.head.Revision + 100, Found: true,
			Kind: read.Kind, Head: ledger.head,
		}, nil
	case replicatedstate.RequestLedgerReadTerminal:
		return DurableRequestLifecycleRow{
			Applied: ledger.head.Revision + 100, Found: true,
			Kind: read.Kind, Terminal: ledger.terminal,
		}, nil
	default:
		return DurableRequestLifecycleRow{}, ErrDurableRequestConflict
	}
}

func TestDurableRequestAckCollectorResumesAmbiguousAckAndCollection(t *testing.T) {
	terminalPlan, head, continuation, pin := terminalCoordinatorFixture(t)
	terminalLedger := &terminalCoordinatorLedger{head: head, continuation: continuation}
	coordinator, err := newDurableRequestTerminalCoordinator(terminalLedger, pin)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := coordinator.Complete(t.Context(), terminalPlan)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &ackCollectorLedger{
		head: terminalLedger.head, terminal: terminal.Terminal,
		faultAck: true, faultGC: true,
	}
	collector, err := NewDurableRequestAckCollector(ledger)
	if err != nil {
		t.Fatal(err)
	}
	result, err := collector.AcknowledgeAndCollect(t.Context(), DurableRequestAckPlan{
		Home: terminalPlan.Home, Key: terminalPlan.Key,
		TerminalRevision: terminal.Terminal.Revision,
		ResultDigest:     terminal.Terminal.ResultDigest,
		AckToken:         terminal.Terminal.AckToken,
	})
	if err != nil || result.Ack.GCPhase != requestledger.AckGCComplete ||
		result.Ack.ReclaimedBytes != result.Ack.PriorEncodedBytes || result.Rounds != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	want := []requestledger.Operation{requestledger.OperationAck, requestledger.OperationGC}
	if len(ledger.operations) != len(want) {
		t.Fatalf("operations=%v", ledger.operations)
	}
	for i := range want {
		if ledger.operations[i] != want[i] {
			t.Fatalf("operations=%v", ledger.operations)
		}
	}

	// A replay with the same possession witness performs no writes and returns
	// the permanent compact tombstone.
	before := len(ledger.operations)
	replayed, err := collector.AcknowledgeAndCollect(t.Context(), DurableRequestAckPlan{
		Home: terminalPlan.Home, Key: terminalPlan.Key,
		TerminalRevision: terminal.Terminal.Revision,
		ResultDigest:     terminal.Terminal.ResultDigest,
		AckToken:         terminal.Terminal.AckToken,
	})
	if err != nil || replayed.Ack.AckDigest != result.Ack.AckDigest ||
		len(ledger.operations) != before {
		t.Fatalf("replayed=%+v operations=%v err=%v", replayed, ledger.operations, err)
	}

	wrong := terminal.Terminal.AckToken
	wrong[0] ^= 0xff
	_, err = collector.AcknowledgeAndCollect(t.Context(), DurableRequestAckPlan{
		Home: terminalPlan.Home, Key: terminalPlan.Key,
		TerminalRevision: terminal.Terminal.Revision,
		ResultDigest:     terminal.Terminal.ResultDigest, AckToken: wrong,
	})
	if !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("wrong token err=%v", err)
	}
}
