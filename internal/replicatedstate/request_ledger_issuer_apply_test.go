package replicatedstate

import (
	"bytes"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/requestledger"
)

func issuerPlannerDigest(value byte) requestledger.Digest {
	return requestledger.Digest{value}
}

func issuerPlannerKey(sequence uint64, requestByte byte) requestledger.RequestKey {
	return requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, TenantDigest: issuerPlannerDigest(0x11),
		Principal: requestledger.PrincipalID{0x21}, Request: requestledger.RequestID{requestByte},
		IssuerEpoch: 7, IssuerSequence: sequence, IssuerLane: requestledger.IssuerLane{0x31},
	}
}

func issuerPlannerHead(t testing.TB, key requestledger.RequestKey) requestledger.HeadRecord {
	t.Helper()
	plan, err := requestledger.AppendPlan(nil, []byte("canonical issuer planner recipe bytes"))
	if err != nil {
		t.Fatal(err)
	}
	cursor := []byte("terminal-cursor")
	contract := requestledger.ExecutionContract{
		CatalogGeneration: 9, PinID: requestledger.PinID{0x41}, PinDigest: issuerPlannerDigest(0x42),
		RouteSchemaCertificateDigest: issuerPlannerDigest(0x43),
		MaxPendingWaveBytes:          requestledger.MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         requestledger.MaxContinuationRecordBytes,
		MaxTerminalBytes:             requestledger.MaxLifecyclePayloadBytes,
		TerminalTransitionTag:        9, FinalWaveCount: 1,
		TerminalStateDigest:        requestledger.NextStateDigest(9, cursor),
		TerminalSummaryDigest:      issuerPlannerDigest(0x44),
		AbortTerminalTransitionTag: 10, AbortFinalWaveCount: 1,
		AbortTerminalStateDigest: requestledger.NextStateDigest(10, cursor),
		PlanBuildID:              issuerPlannerDigest(0x45), PlanBuildGeneration: 1,
		PlanningLeaseExpiryIndex: math.MaxUint64, PlanningLeaseGeneration: 1,
	}
	head, err := requestledger.NewHeadWithExecutionContract(
		key, issuerPlannerDigest(0x46), issuerPlannerDigest(0x47), contract, plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func issuerPlannerGCComplete(t testing.TB, key requestledger.RequestKey) (
	requestledger.AckRecord,
	requestledger.IssuerSequenceRecord,
) {
	t.Helper()
	head := issuerPlannerHead(t, key)
	pin, err := requestledger.NewRoutePinAcquiring(
		head, requestledger.PinID{0x51}, issuerPlannerDigest(0x52), issuerPlannerDigest(0x53),
		[]byte("exact-acquire-command"),
	)
	if err != nil {
		t.Fatal(err)
	}
	pin, err = requestledger.RecordVerifiedRoutePinAcquired(pin, pin.Revision+1, []byte("exact-acquire-completion"))
	if err != nil {
		t.Fatal(err)
	}
	steps := []requestledger.StepRef{{
		TargetSource: requestledger.PayloadSourcePlan, CommandSource: requestledger.PayloadSourcePlan,
		TargetOffset: 0, TargetLength: 8, CommandOffset: 8, CommandLength: 16,
		TargetDigest: issuerPlannerDigest(0x54), CommandDigest: issuerPlannerDigest(0x55),
	}}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		head, requestledger.PayloadBuildRecord{}, 2, pin, steps,
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallPendingWave(head, pending, requestledger.PayloadBuildRecord{}, pin)
	if err != nil {
		t.Fatal(err)
	}
	cursor := []byte("terminal-cursor")
	continuation, err := requestledger.NewContinuation(
		head, pending, pin, 3, head.TerminalTransitionTag, cursor, []byte("observation"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvancePending(head, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	pin, err = requestledger.BeginRoutePinRelease(pin, pin.Revision+1, []byte("exact-release-command"))
	if err != nil {
		t.Fatal(err)
	}
	pin, err = requestledger.RecordVerifiedRoutePinReleased(pin, pin.Revision+1, []byte("exact-release-completion"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkRoutePinReleased(head, pin, 4)
	if err != nil {
		t.Fatal(err)
	}
	var token requestledger.AckToken
	token[0] = 0x61
	prepared, err := requestledger.NewPreparedTerminal(
		head, continuation, 5, requestledger.OutcomeCommitted, 1, true,
		[]byte("result"), head.TerminalSummaryDigest, token,
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkTerminalPrepared(head, continuation, prepared)
	if err != nil {
		t.Fatal(err)
	}
	release, err := requestledger.NewSchemaPinRelease(head, prepared, 6, []byte("schema-release-command"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallSchemaPinRelease(head, prepared, release)
	if err != nil {
		t.Fatal(err)
	}
	intent := release
	release, err = requestledger.RecordVerifiedSchemaPinReleased(release, 7, []byte("schema-release-completion"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkSchemaPinReleased(head, prepared, intent, release)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := requestledger.NewTerminal(head, prepared, release, 8)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkTerminal(head, prepared, release, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := requestledger.NewAck(head, terminal, 9, 10000)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := requestledger.NewIssuerSequence(key, ack.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	collect, err := requestledger.NewCollectRequest(ack.AckDigest, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	ack, err = requestledger.AdvanceAckGC(ack, collect, 10, 1, ack.PriorEncodedBytes, true)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err = requestledger.MarkIssuerSequenceGCComplete(sequence, ack, 2)
	if err != nil {
		t.Fatal(err)
	}
	return ack, sequence
}

func issuerPlannerCommandView(t testing.TB, command requestledger.Command) requestledger.CommandView {
	t.Helper()
	raw, err := requestledger.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	view, err := requestledger.OpenCommandInto(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func TestRequestLedgerSequencedCreateConvertsReservation(t *testing.T) {
	key := issuerPlannerKey(1, 0x71)
	head := issuerPlannerHead(t, key)
	home, _ := requestledger.Home(key)
	headRaw, _ := requestledger.AppendHead(nil, head)
	command := issuerPlannerCommandView(t, requestledger.Command{
		Operation: requestledger.OperationCreate, Revision: 1,
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		SubjectDigest: head.TerminalContractDigest, ExpectedRangeIdentity: issuerPlannerDigest(0x72),
		Home: home, Payload: headRaw,
	})
	headKey := requestledger.AppendHeadKey(nil, home, head.KeyDigest)
	_, reserved, err := requestledger.Reservation(head)
	if err != nil {
		t.Fatal(err)
	}
	plan := requestLedgerCommandPlan{
		rows: []transactionRowMutation{newTransactionPut(headKey, headRaw)},
		delta: requestLedgerStateDelta{rows: 1, residentBytes: int64(len(headKey) + len(headRaw)),
			reservedBytes: int64(reserved)},
		completion: RequestLedgerCompletionResult{Operation: requestledger.OperationCreate,
			ResultCode: ResultApplied, KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest,
			PlanRoot: head.PlanRoot, RangeIdentity: issuerPlannerDigest(0x72)},
	}
	base := plan
	base.rows = append([]transactionRowMutation(nil), plan.rows...)
	admission := uint64(plan.delta.residentBytes + plan.delta.reservedBytes)
	lowCapacity := &Machine{options: Options{
		RequestLedgerCapacityBytes: 1<<20 + admission, RequestLedgerCleanupReserveBytes: 1 << 20,
	}}
	refused, err := lowCapacity.planRequestLedgerSequencedCreate(
		base, command, head, State{}, pointSnapshot{overlay: &logicalOverlay{}},
	)
	if err != nil || refused.completion.ResultCode != ResultRequestLedgerCapacity ||
		len(refused.rows) != 0 || refused.delta != (requestLedgerStateDelta{}) {
		t.Fatalf("first-lane highwater bypassed capacity: completion=%+v delta=%+v: %v",
			refused.completion, refused.delta, err)
	}
	machine := &Machine{options: Options{RequestLedgerCapacityBytes: 64 << 20,
		RequestLedgerCleanupReserveBytes: 1 << 20}}
	overlay := logicalOverlay{}
	plan, err = machine.planRequestLedgerSequencedCreate(
		plan, command, head, State{}, pointSnapshot{overlay: &overlay},
	)
	if err != nil || len(plan.rows) != 3 || plan.delta.rows != 3 ||
		plan.delta.reservedBytes != int64(reserved-requestledger.IssuerSequenceReservationBytes) {
		t.Fatalf("sequenced create plan rows=%d delta=%+v: %v", len(plan.rows), plan.delta, err)
	}
	shared := int64(requestledger.IssuerHighwaterResidentBytes)
	wantResident := int64(len(headKey)+len(headRaw)) +
		int64(requestledger.IssuerSequenceReservationBytes) + shared
	if plan.delta.residentBytes != wantResident {
		t.Fatalf("resident=%d want=%d", plan.delta.residentBytes, wantResident)
	}
	issuer, err := requestledger.IssuerDigest(requestledger.IssuerIdentity{
		Scope: key.Scope, TenantDigest: key.TenantDigest, Principal: key.Principal,
		IssuerEpoch: key.IssuerEpoch, IssuerLane: key.IssuerLane,
	})
	if err != nil {
		t.Fatal(err)
	}
	highwaterKey := requestledger.AppendIssuerHighwaterKey(nil, home, issuer)
	var highwater requestledger.IssuerHighwaterRecord
	found := false
	for _, row := range plan.rows {
		if !bytes.Equal(row.key, highwaterKey) {
			continue
		}
		highwater, err = requestledger.OpenIssuerHighwater(row.value)
		found = true
	}
	if err != nil || !found || highwater.AdmittedSequence != 1 || highwater.HighwaterSequence != 0 ||
		highwater.LastAdmissionKeyDigest != head.KeyDigest ||
		highwater.LastAdmissionRequestDigest != head.RequestDigest {
		t.Fatalf("admission highwater=%+v found=%v: %v", highwater, found, err)
	}
}

func TestRequestLedgerIssuerOpenIsPersistedIdempotentCAS(t *testing.T) {
	key := issuerPlannerKey(1, 0x61)
	highwater, err := requestledger.NewIssuerHighwater(key)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := requestledger.AppendIssuerHighwater(nil, highwater)
	command := issuerPlannerCommandView(t, requestledger.Command{
		Operation: requestledger.OperationOpenIssuerLane, Revision: 1,
		KeyDigest: highwater.IssuerDigest, RequestDigest: highwater.HighwaterDigest,
		PlanRoot: highwater.HighwaterDigest, SubjectDigest: highwater.HighwaterDigest,
		ExpectedRangeIdentity: issuerPlannerDigest(0x62), Home: highwater.Home, Payload: payload,
	})
	machine := &Machine{options: Options{
		RequestLedgerCapacityBytes: 64 << 20, RequestLedgerCleanupReserveBytes: 1 << 20,
	}}
	overlay := logicalOverlay{}
	plan := requestLedgerCommandPlan{completion: RequestLedgerCompletionResult{
		Operation: command.Operation, ResultCode: ResultRequestLedgerConflict,
		KeyDigest: command.KeyDigest, RequestDigest: command.RequestDigest,
		PlanRoot: command.PlanRoot, RangeIdentity: command.ExpectedRangeIdentity,
	}}
	plan, err = machine.planRequestLedgerIssuerOpen(
		plan, command, State{}, pointSnapshot{overlay: &overlay},
	)
	if err != nil || len(plan.rows) != 1 || plan.rows[0].delete || plan.delta.rows != 1 ||
		plan.delta.residentBytes != int64(requestledger.IssuerHighwaterResidentBytes) ||
		plan.completion.ResultCode != ResultApplied || plan.completion.ExactDuplicate {
		t.Fatalf("open plan=%+v rows=%d delta=%+v err=%v", plan.completion, len(plan.rows), plan.delta, err)
	}
	if err = overlay.record(plan.rows[0].key, plan.rows[0].value, false); err != nil {
		t.Fatal(err)
	}
	retry := requestLedgerCommandPlan{completion: plan.completion}
	retry.completion.ExactDuplicate = false
	retry, err = machine.planRequestLedgerIssuerOpen(
		retry, command, State{RequestLedgerResidentBytes: uint64(requestledger.IssuerHighwaterResidentBytes)},
		pointSnapshot{overlay: &overlay},
	)
	if err != nil || len(retry.rows) != 0 || !retry.completion.ExactDuplicate ||
		retry.completion.StateDigest != plan.completion.StateDigest {
		t.Fatalf("retry=%+v rows=%d err=%v", retry.completion, len(retry.rows), err)
	}
}

func TestRequestLedgerIssuerAdvanceDeletesOnlyAfterHighwater(t *testing.T) {
	key := issuerPlannerKey(1, 0x81)
	ack, sequence := issuerPlannerGCComplete(t, key)
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
	request, err := requestledger.NewIssuerAdvanceRequest(highwater, sequence, ack)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := requestledger.AppendIssuerAdvanceRequest(nil, request)
	command := issuerPlannerCommandView(t, requestledger.Command{
		Operation:        requestledger.OperationAdvanceIssuerHighwater,
		ExpectedRevision: highwater.Revision, Revision: highwater.Revision + 1,
		KeyDigest: ack.KeyDigest, RequestDigest: ack.RequestDigest, PlanRoot: ack.PlanRoot,
		SubjectDigest: request.ExpectedHighwaterDigest, ExpectedRangeIdentity: issuerPlannerDigest(0x82),
		Home: highwater.Home, Payload: payload,
	})
	highwaterRaw, _ := requestledger.AppendIssuerHighwater(nil, highwater)
	sequenceRaw, _ := requestledger.AppendIssuerSequence(nil, sequence)
	ackRaw, _ := requestledger.AppendAck(nil, ack)
	highwaterKey := requestledger.AppendIssuerHighwaterKey(nil, highwater.Home, highwater.IssuerDigest)
	sequenceKey := requestledger.AppendIssuerSequenceKey(nil, highwater.Home, highwater.IssuerDigest, 1)
	ackKey := requestledger.AppendAckKey(nil, highwater.Home, ack.KeyDigest)
	overlay := logicalOverlay{}
	for _, row := range []struct{ key, value []byte }{
		{highwaterKey, highwaterRaw}, {sequenceKey, sequenceRaw}, {ackKey, ackRaw},
	} {
		if err = overlay.record(row.key, row.value, false); err != nil {
			t.Fatal(err)
		}
	}
	plan := requestLedgerCommandPlan{completion: RequestLedgerCompletionResult{
		Operation: command.Operation, ResultCode: ResultRequestLedgerConflict,
		KeyDigest: command.KeyDigest, RequestDigest: command.RequestDigest,
		PlanRoot: command.PlanRoot, RangeIdentity: command.ExpectedRangeIdentity,
	}}
	plan, err = planRequestLedgerIssuerAdvance(plan, command, pointSnapshot{overlay: &overlay})
	if err != nil || len(plan.rows) != 3 || plan.delta.rows != -2 || plan.delta.ackRows != -1 ||
		plan.completion.ResultCode != ResultApplied || plan.completion.ExactDuplicate {
		t.Fatalf("issuer advance plan rows=%d delta=%+v completion=%+v: %v",
			len(plan.rows), plan.delta, plan.completion, err)
	}
	if plan.rows[0].delete || !plan.rows[1].delete || !plan.rows[2].delete {
		t.Fatal("highwater was not installed before sequence/ACK deletion")
	}
	for _, mutation := range plan.rows {
		if err = overlay.record(mutation.key, mutation.value, mutation.delete); err != nil {
			t.Fatal(err)
		}
	}
	retry := requestLedgerCommandPlan{completion: plan.completion}
	retry.completion.ResultCode = ResultRequestLedgerConflict
	retry.completion.ExactDuplicate = false
	retry, err = planRequestLedgerIssuerAdvance(retry, command, pointSnapshot{overlay: &overlay})
	if err != nil || len(retry.rows) != 0 || !retry.completion.ExactDuplicate ||
		retry.completion.StateDigest != plan.completion.StateDigest {
		t.Fatalf("issuer advance retry=%+v rows=%d: %v", retry.completion, len(retry.rows), err)
	}
}

func TestRequestLedgerFinalAckGCMarksIssuerSequence(t *testing.T) {
	key := issuerPlannerKey(1, 0x91)
	ack, _ := issuerPlannerGCComplete(t, key)
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
	sequence, err := requestledger.NewIssuerSequence(key, ack.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	highwaterRaw, _ := requestledger.AppendIssuerHighwater(nil, highwater)
	sequenceRaw, _ := requestledger.AppendIssuerSequence(nil, sequence)
	highwaterKey := requestledger.AppendIssuerHighwaterKey(nil, highwater.Home, highwater.IssuerDigest)
	sequenceKey := requestledger.AppendIssuerSequenceKey(nil, highwater.Home, highwater.IssuerDigest, 1)
	overlay := logicalOverlay{}
	if err = overlay.record(highwaterKey, highwaterRaw, false); err != nil {
		t.Fatal(err)
	}
	if err = overlay.record(sequenceKey, sequenceRaw, false); err != nil {
		t.Fatal(err)
	}
	plan, err := planRequestLedgerSequencedAckGCComplete(
		requestLedgerCommandPlan{}, requestLedgerRows{home: highwater.Home}, ack,
		pointSnapshot{overlay: &overlay},
	)
	if err != nil || len(plan.rows) != 1 || plan.rows[0].delete || plan.delta != (requestLedgerStateDelta{}) {
		t.Fatalf("mark sequence plan rows=%d delta=%+v: %v", len(plan.rows), plan.delta, err)
	}
	opened, err := requestledger.OpenIssuerSequence(plan.rows[0].value)
	if err != nil || opened.Phase != requestledger.IssuerSequenceGCComplete ||
		opened.AckDigest != ack.AckDigest {
		t.Fatalf("marked sequence=%+v: %v", opened, err)
	}
}
