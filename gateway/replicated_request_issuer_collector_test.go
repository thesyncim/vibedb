package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

type issuerCollectorLedger struct {
	highwater      requestledger.IssuerHighwaterRecord
	rangeIdentity  requestledger.Digest
	sequence       requestledger.IssuerSequenceRecord
	ack            requestledger.AckRecord
	applied        uint64
	applyFault     bool
	reads, applies int
}

func (ledger *issuerCollectorLedger) ReadRow(
	_ context.Context,
	_ DurableRequestLedgerHome,
	read DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	ledger.reads++
	if read.Kind != replicatedstate.RequestLedgerReadIssuerStatus ||
		read.MinimumApplied == 0 || read.MinimumApplied > ledger.applied {
		return DurableRequestLifecycleRow{}, ErrDurableRequestConflict
	}
	identity, _ := requestledger.IssuerIdentityFor(read.Key)
	if identity != ledger.highwater.Identity {
		return DurableRequestLifecycleRow{Applied: ledger.applied, Kind: read.Kind}, nil
	}
	var sequence *requestledger.IssuerSequenceRecord
	var ack *requestledger.AckRecord
	if ledger.sequence.Sequence == ledger.highwater.HighwaterSequence+1 {
		sequence = &ledger.sequence
		if ledger.sequence.Phase == requestledger.IssuerSequenceGCComplete {
			ack = &ledger.ack
		}
	}
	status, err := requestledger.NewIssuerLaneStatus(ledger.rangeIdentity, ledger.highwater, sequence, ack)
	if err != nil {
		return DurableRequestLifecycleRow{}, err
	}
	return DurableRequestLifecycleRow{
		Applied: ledger.applied, Found: true, Kind: read.Kind, IssuerStatus: status,
	}, nil
}

func (ledger *issuerCollectorLedger) ApplyCAS(
	_ context.Context,
	_ DurableRequestLedgerHome,
	key requestledger.RequestKey,
	cas DurableRequestLifecycleCAS,
) (DurableRequestLifecycleCASResult, error) {
	ledger.applies++
	if cas.Operation != requestledger.OperationAdvanceIssuerHighwater || key != ledger.ack.Key ||
		cas.ExpectedRevision != ledger.highwater.Revision || cas.Revision != ledger.highwater.Revision+1 {
		return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
	}
	next, err := requestledger.AdvanceIssuerHighwater(
		ledger.highwater, ledger.sequence, ledger.ack, cas.IssuerAdvance, cas.Revision,
	)
	if err != nil {
		return DurableRequestLifecycleCASResult{}, err
	}
	ledger.highwater = next
	ledger.sequence = requestledger.IssuerSequenceRecord{}
	ledger.ack = requestledger.AckRecord{}
	ledger.applied++
	if ledger.applyFault {
		ledger.applyFault = false
		return DurableRequestLifecycleCASResult{}, errLifecycleRunnerFault
	}
	return DurableRequestLifecycleCASResult{
		Ledger:  replicatedstate.RequestLedgerCompletionResult{ResultCode: replicatedstate.ResultApplied},
		Applied: ledger.applied,
	}, nil
}

func issuerCollectorFixture(t *testing.T) (
	DurableRequestLedgerHome,
	requestledger.RequestKey,
	requestledger.IssuerHighwaterRecord,
	requestledger.IssuerSequenceRecord,
	requestledger.AckRecord,
) {
	t.Helper()
	key := lifecycleKey()
	key.IssuerEpoch, key.IssuerSequence = 7, 1
	copy(key.IssuerLane[:], []byte("lane0001"))
	ack, sequence := issuerCollectorGCComplete(t, key)
	highwater, err := requestledger.NewIssuerHighwater(key)
	if err != nil {
		t.Fatal(err)
	}
	highwater, err = requestledger.AdmitIssuerSequence(
		highwater, key, ack.RequestDigest, highwater.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	homePoint, _ := requestledger.Home(key)
	return DurableRequestLedgerHome{Identity: replication.Digest(lifecycleDigest("issuer-range")), Point: homePoint},
		key, highwater, sequence, ack
}

func issuerCollectorGCComplete(
	t testing.TB,
	key requestledger.RequestKey,
) (requestledger.AckRecord, requestledger.IssuerSequenceRecord) {
	t.Helper()
	plan, err := requestledger.AppendPlan(nil, bytes.Repeat([]byte{0x5a}, 256))
	if err != nil {
		t.Fatal(err)
	}
	cursor := []byte("terminal-cursor")
	contract := requestledger.ExecutionContract{
		CatalogGeneration: 7, PinID: requestledger.PinID{1},
		PinDigest:                    lifecycleDigest("issuer-pin"),
		RouteSchemaCertificateDigest: lifecycleDigest("issuer-route-cert"),
		MaxPendingWaveBytes:          requestledger.MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         requestledger.MaxContinuationRecordBytes,
		MaxTerminalBytes:             requestledger.MaxLifecyclePayloadBytes,
		MaxActivePayloadBytes:        2 * requestledger.MaxPlanPageBytes,
		MaxActivePayloadChunks:       2,
		PlanBuildID:                  lifecycleDigest("issuer-plan-build"), PlanBuildGeneration: 1,
		PlanningLeaseSpan: requestledger.MaxPlanningLeaseSpan, PlanningLeaseGeneration: 1,
		TerminalTransitionTag: 9, FinalWaveCount: 1,
		TerminalStateDigest:        requestledger.NextStateDigest(9, cursor),
		TerminalSummaryDigest:      lifecycleDigest("issuer-retirement"),
		AbortTerminalTransitionTag: 10, AbortFinalWaveCount: 1,
		AbortTerminalStateDigest: requestledger.NextStateDigest(10, cursor),
	}
	head, err := requestledger.NewHeadWithExecutionContract(
		key, lifecycleDigest("issuer-request"), lifecycleDigest("issuer-contract"), contract, plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := requestledger.NewRoutePinAcquiring(
		head, requestledger.PinID{2}, lifecycleDigest("issuer-route-binding"),
		lifecycleDigest("issuer-physical-route"), []byte("acquire"),
	)
	if err != nil {
		t.Fatal(err)
	}
	pin, err = requestledger.RecordVerifiedRoutePinAcquired(pin, pin.Revision+1, []byte("acquired"))
	if err != nil {
		t.Fatal(err)
	}
	step := requestledger.StepRef{
		TargetSource: requestledger.PayloadSourcePlan, CommandSource: requestledger.PayloadSourcePlan,
		TargetOffset: 0, TargetLength: 8, CommandOffset: 8, CommandLength: 16,
		TargetDigest: lifecycleDigest("issuer-target"), CommandDigest: lifecycleDigest("issuer-command"),
	}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		head, requestledger.PayloadBuildRecord{}, head.Revision+1, pin, []requestledger.StepRef{step},
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallPendingWave(head, pending, requestledger.PayloadBuildRecord{}, pin)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := requestledger.NewContinuation(
		head, pending, pin, head.Revision+1, 9, cursor, []byte("settled"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvancePending(head, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	pin, err = requestledger.BeginRoutePinRelease(pin, pin.Revision+1, []byte("release"))
	if err != nil {
		t.Fatal(err)
	}
	pin, err = requestledger.RecordVerifiedRoutePinReleased(pin, pin.Revision+1, []byte("released"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkRoutePinReleased(head, pin, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	var token requestledger.AckToken
	token[0] = 0x61
	prepared, err := requestledger.NewPreparedTerminal(
		head, continuation, head.Revision+1, requestledger.OutcomeCommitted, 1, true,
		[]byte("result"), head.TerminalSummaryDigest, token,
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkTerminalPrepared(head, continuation, prepared)
	if err != nil {
		t.Fatal(err)
	}
	release, err := requestledger.NewSchemaPinRelease(head, prepared, head.Revision+1, []byte("schema-release"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallSchemaPinRelease(head, prepared, release)
	if err != nil {
		t.Fatal(err)
	}
	intent := release
	release, err = requestledger.RecordVerifiedSchemaPinReleased(release, release.Revision+1, []byte("released"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkSchemaPinReleased(head, prepared, intent, release)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := requestledger.NewTerminal(head, prepared, release, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkTerminal(head, prepared, release, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := requestledger.NewAck(head, terminal, head.Revision+1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	collect, err := requestledger.NewCollectRequest(ack.AckDigest, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	ack, err = requestledger.AdvanceAckGC(ack, collect, ack.Revision+1, 1, ack.PriorEncodedBytes, true)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := requestledger.NewIssuerSequence(key, ack.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err = requestledger.MarkIssuerSequenceGCComplete(sequence, ack, sequence.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	return ack, sequence
}

func TestDurableIssuerHighwaterCollectorResolvesOutcomeUnknownAndRestart(t *testing.T) {
	home, key, highwater, sequence, ack := issuerCollectorFixture(t)
	ledger := &issuerCollectorLedger{
		highwater: highwater, rangeIdentity: requestledger.Digest(home.Identity),
		sequence: sequence, ack: ack, applied: 100, applyFault: true,
	}
	collector, err := NewDurableIssuerHighwaterCollector(ledger)
	if err != nil {
		t.Fatal(err)
	}
	result, err := collector.Collect(t.Context(), DurableIssuerHighwaterCollectPlan{
		Home: home, Key: key, MaxAdvances: 8,
	})
	if err != nil || result.Highwater.HighwaterSequence != 1 || result.Advances != 1 ||
		!result.StoppedAtGap || ledger.applies != 1 || ledger.reads < 3 {
		t.Fatalf("result=%+v reads=%d applies=%d err=%v", result, ledger.reads, ledger.applies, err)
	}

	// Reconstructing the collector after the outcome-unknown cut observes only
	// the retained highwater. The deleted ACK/sequence cannot resurrect.
	restarted, _ := NewDurableIssuerHighwaterCollector(ledger)
	before := ledger.applies
	replayed, err := restarted.Collect(t.Context(), DurableIssuerHighwaterCollectPlan{
		Home: home, Key: key, MaxAdvances: 8,
	})
	if err != nil || replayed.Highwater.HighwaterSequence != 1 || ledger.applies != before {
		t.Fatalf("replayed=%+v applies=%d err=%v", replayed, ledger.applies, err)
	}
}

func TestDurableIssuerHighwaterCollectorStopsAtNonGCCompleteAndBoundsWork(t *testing.T) {
	home, key, highwater, complete, _ := issuerCollectorFixture(t)
	active, err := requestledger.NewIssuerSequence(key, complete.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &issuerCollectorLedger{
		highwater: highwater, rangeIdentity: requestledger.Digest(home.Identity),
		sequence: active, applied: 50,
	}
	collector, _ := NewDurableIssuerHighwaterCollector(ledger)
	result, err := collector.Collect(t.Context(), DurableIssuerHighwaterCollectPlan{
		Home: home, Key: key, MaxAdvances: 1,
	})
	if err != nil || !result.StoppedAtGap || result.Advances != 0 || ledger.applies != 0 {
		t.Fatalf("result=%+v applies=%d err=%v", result, ledger.applies, err)
	}
	_, err = collector.Collect(t.Context(), DurableIssuerHighwaterCollectPlan{
		Home: home, Key: key, MaxAdvances: MaxDurableIssuerHighwaterAdvances + 1,
	})
	if !errors.Is(err, ErrDurableRequest) {
		t.Fatalf("unbounded collector err=%v", err)
	}
}
