package replicatedstate

import (
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
)

func requestLedgerPreparedForExecutionPin(t testing.TB) (
	requestledger.HeadRecord,
	requestledger.PreparedTerminalRecord,
	executionpin.Binding,
) {
	t.Helper()
	plan, err := requestledger.AppendPlan(nil, []byte("execution-pin ledger recipe"))
	if err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestLedgerStateTestDigest("execution-pin tenant"),
		Principal:    requestledger.PrincipalID{0x21}, Request: requestledger.RequestID{0x31},
	}
	keyDigest, err := requestledger.KeyDigest(key)
	if err != nil {
		t.Fatal(err)
	}
	requestDigest := requestLedgerStateTestDigest("execution-pin request")
	routeCertificate := requestLedgerStateTestDigest("execution-pin route schema certificate")
	binding := executionpin.Binding{
		RequestKeyDigest:          executionpin.Digest(keyDigest),
		RequestDigest:             executionpin.Digest(requestDigest),
		CatalogGeneration:         7,
		SchemaManifestDigest:      executionpin.Digest(routeCertificate),
		TransactionManifestDigest: executionpin.Digest(requestLedgerStateTestDigest("transaction manifest")),
		ParticipantAuthorityRoot:  executionpin.Digest(requestLedgerStateTestDigest("participant authority")),
		ParticipantCount:          2,
		ExecutionContractDigest:   executionpin.Digest(requestLedgerStateTestDigest("execution contract")),
		LedgerHomeGroup:           executionpin.ID{0x41},
	}
	pinDigest, err := executionpin.BindingDigest(binding)
	if err != nil {
		t.Fatal(err)
	}
	cursor := []byte("execution-pin terminal cursor")
	contract := requestledger.ExecutionContract{
		CatalogGeneration: 7, PinID: requestledger.PinID{0xa1, 0xa2},
		PinDigest:                    requestledger.Digest(pinDigest),
		RouteSchemaCertificateDigest: routeCertificate,
		MaxPendingWaveBytes:          requestledger.MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         requestledger.MaxContinuationRecordBytes,
		MaxTerminalBytes:             requestledger.MaxLifecyclePayloadBytes,
		PlanBuildID:                  requestLedgerStateTestDigest("execution-pin plan build"),
		PlanBuildGeneration:          1,
		PlanningLeaseSpan:            requestledger.MaxPlanningLeaseSpan,
		PlanningLeaseGeneration:      1,
		TerminalTransitionTag:        11,
		FinalWaveCount:               1,
		TerminalStateDigest:          requestledger.NextStateDigest(11, cursor),
		TerminalSummaryDigest:        requestLedgerStateTestDigest("execution-pin terminal summary"),
		AbortTerminalTransitionTag:   12,
		AbortFinalWaveCount:          1,
		AbortTerminalStateDigest:     requestledger.NextStateDigest(12, cursor),
	}
	head, err := requestledger.NewHeadWithExecutionContract(
		key, requestDigest, requestLedgerStateTestDigest("execution-pin terminal contract"),
		contract, plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	acquiring, err := requestledger.NewRoutePinAcquiring(
		head, requestledger.PinID{0xb1}, requestLedgerStateTestDigest("route binding"),
		requestLedgerStateTestDigest("physical witness"), []byte("acquire route pin"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(
		head, requestledger.RoutePinRecord{}, acquiring, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := requestledger.RecordVerifiedRoutePinAcquired(
		acquiring, 2, []byte("acquired route pin"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, acquiring, acquired, 3)
	if err != nil {
		t.Fatal(err)
	}
	step := requestledger.StepRef{
		TargetSource:  requestledger.PayloadSourcePlan,
		CommandSource: requestledger.PayloadSourcePlan,
		TargetOffset:  0, TargetLength: 1, CommandOffset: 1, CommandLength: 1,
		TargetDigest:  requestLedgerStateTestDigest("target"),
		CommandDigest: requestLedgerStateTestDigest("command"),
	}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		head, requestledger.PayloadBuildRecord{}, 4, acquired, []requestledger.StepRef{step},
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallPendingWave(
		head, pending, requestledger.PayloadBuildRecord{}, acquired,
	)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := requestledger.NewContinuation(
		head, pending, acquired, 5, contract.TerminalTransitionTag,
		cursor, []byte("execution-pin observation"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvancePending(head, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	releasing, err := requestledger.BeginRoutePinRelease(
		acquired, 3, []byte("release route pin"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, acquired, releasing, 6)
	if err != nil {
		t.Fatal(err)
	}
	released, err := requestledger.RecordVerifiedRoutePinReleased(
		releasing, 4, []byte("released route pin"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkRoutePinReleased(head, released, 7)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := requestledger.NewPreparedTerminal(
		head, continuation, 8, requestledger.OutcomeCommitted, 1, true,
		[]byte("execution-pin result"), head.TerminalSummaryDigest, requestledger.AckToken{0xc1},
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkTerminalPrepared(head, continuation, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return head, prepared, binding
}

func requestLedgerRouteGateOuter(
	t testing.TB,
	binding Binding,
	sequence uint64,
	command routegate.Command,
) []byte {
	t.Helper()
	nested, err := routegate.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	outer := commandValue(binding, sequence)
	outer.Kind, outer.Batches, outer.RouteGate = replication.CommandRouteGate, nil, nested
	outer.Fingerprint = sha256.Sum256(append([]byte("request-ledger/route-gate/"), nested...))
	return encodeCommand(t, outer)
}

func TestRequestLedgerRouteEvidenceBindsRealSettlementAndPhysicalAuthority(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))

	head, _ := requestLedgerStateTestHead(t, false)
	pin := requestledger.PinID{0x31, 0x32}
	logical := requestLedgerStateTestDigest("exact logical command binding")
	identityDigest, err := requestledger.DeriveRouteGateIdentity(
		head.KeyDigest, head.RequestDigest, head.PlanRoot, head.ContinuationDigest,
		pin, head.NextStepOrdinal,
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := routegate.Identity(identityDigest)

	// PhysicalWitness deliberately excludes the nested operation, so builders
	// can compute it from the route envelope before sealing the final binding.
	provisional := requestLedgerRouteGateOuter(t, fixture.binding, 1, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: routegate.Binding{1},
	})
	provisionalView, err := replication.OpenCommand(provisional)
	if err != nil {
		t.Fatal(err)
	}
	physical, ok := replication.RouteGatePhysicalWitness(provisionalView)
	if !ok {
		t.Fatal("physical route witness unavailable")
	}
	gateBindingDigest, err := requestledger.DeriveRouteGateBinding(
		identityDigest, logical, requestledger.Digest(physical), 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	gateBinding := routegate.Binding(gateBindingDigest)
	acquireBytes := requestLedgerRouteGateOuter(t, fixture.binding, 1, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: gateBinding,
	})
	acquireView, err := replication.OpenCommand(acquireBytes)
	if err != nil {
		t.Fatal(err)
	}
	finalPhysical, ok := replication.RouteGatePhysicalWitness(acquireView)
	if !ok || finalPhysical != physical {
		t.Fatal("nested command changed the physical authority witness")
	}

	acquiring, err := requestledger.NewRoutePinAcquiring(
		head, pin, logical, requestledger.Digest(physical), acquireBytes,
	)
	if err != nil || !requestLedgerRouteCommandEvidenceAvailable(
		requestledger.RoutePinRecord{}, acquiring,
	) {
		t.Fatalf("acquire intent evidence = %v", err)
	}
	if _, err = fixture.machine.ApplyNormal(normalMeta(3), acquireBytes); err != nil {
		t.Fatal(err)
	}
	acquireSettlement, err := fixture.machine.LookupCompletion(acquireBytes)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := requestledger.RecordVerifiedRoutePinAcquired(
		acquiring, acquiring.Revision+1, acquireSettlement.Bytes,
	)
	if err != nil || !requestLedgerRouteCompletionEvidenceAvailable(acquiring, acquired) {
		t.Fatalf("acquire completion evidence = %v", err)
	}

	releaseBytes := requestLedgerRouteGateOuter(t, fixture.binding, 2, routegate.Command{
		Operation: routegate.OperationReleaseShared, Epoch: 1,
		Identity: identity, Binding: gateBinding,
	})
	releasing, err := requestledger.BeginRoutePinRelease(
		acquired, acquired.Revision+1, releaseBytes,
	)
	if err != nil || !requestLedgerRouteCommandEvidenceAvailable(acquired, releasing) {
		t.Fatalf("release intent evidence = %v", err)
	}
	if _, err = fixture.machine.ApplyNormal(normalMeta(4), releaseBytes); err != nil {
		t.Fatal(err)
	}
	releaseSettlement, err := fixture.machine.LookupCompletion(releaseBytes)
	if err != nil {
		t.Fatal(err)
	}
	released, err := requestledger.RecordVerifiedRoutePinReleased(
		releasing, releasing.Revision+1, releaseSettlement.Bytes,
	)
	if err != nil || !requestLedgerRouteCompletionEvidenceAvailable(releasing, released) {
		t.Fatalf("release completion evidence = %v", err)
	}

	wrongWitness := acquiring
	wrongWitness.PhysicalWitnessDigest[0] ^= 0x80
	if requestLedgerRouteCommandEvidenceAvailable(requestledger.RoutePinRecord{}, wrongWitness) {
		t.Fatal("acquire accepted a different physical authority witness")
	}
	wrongCompletion := acquired
	wrongCompletion.Completion = releaseSettlement.Bytes
	if requestLedgerRouteCompletionEvidenceAvailable(acquiring, wrongCompletion) {
		t.Fatal("acquire accepted another command's settled completion")
	}
	movedOuter := commandValue(fixture.binding, 3)
	movedOuter.RouteGeneration++
	movedOuter.Kind, movedOuter.Batches = replication.CommandRouteGate, nil
	movedOuter.RouteGate, _ = routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: gateBinding,
	})
	movedOuter.Fingerprint = sha256.Sum256(movedOuter.RouteGate)
	moved, err := requestledger.NewRoutePinAcquiring(
		head, pin, logical, requestledger.Digest(physical), encodeCommand(t, movedOuter),
	)
	if err != nil {
		t.Fatal(err)
	}
	if requestLedgerRouteCommandEvidenceAvailable(requestledger.RoutePinRecord{}, moved) {
		t.Fatal("route intent accepted a command addressed to another route generation")
	}
}

func TestRequestLedgerSchemaReleaseEvidenceBindsFullExecutionPinProof(t *testing.T) {
	head, prepared, pinBinding := requestLedgerPreparedForExecutionPin(t)
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	client := id128(0xd1)
	applySessionOpen(
		t, fixture.machine, 2, executionPinSessionPrototype(fixture.binding, client),
	)
	fullPin, err := executionpin.DerivePinID(pinBinding)
	if err != nil {
		t.Fatal(err)
	}
	var truncated requestledger.PinID
	copy(truncated[:], fullPin[:])
	if truncated == prepared.PinID {
		t.Fatal("test requires distinct compact ledger and full execution-pin identities")
	}
	controller := executionpin.ID(id128(0xe1))
	acquire := executionpin.Command{
		Operation: executionpin.OperationAcquire, Binding: pinBinding, PinID: fullPin,
		AuthorityNode: executionpin.ID(client), AuthorityGeneration: 7,
		NextController: controller, NextControllerEpoch: 1, NextLeaseSpan: 97,
	}
	acquireBytes := executionPinCommand(fixture.binding, client, 2, 2, acquire)
	if _, err = fixture.machine.ApplyNormal(normalMeta(3), acquireBytes); err != nil {
		t.Fatal(err)
	}
	_, acquireProof := openExecutionPinProof(t, fixture.machine, acquireBytes)
	acquireDigest, err := executionpin.AcquireCertificateDigest(acquireProof.Acquire)
	if err != nil {
		t.Fatal(err)
	}
	release := executionpin.Command{
		Operation: executionpin.OperationRelease, Binding: pinBinding, PinID: fullPin,
		AuthorityNode: executionpin.ID(client), AuthorityGeneration: 7,
		ExpectedController: controller, ExpectedControllerEpoch: 1,
		ExpectedLeaseAppliedThrough: 100, ExpectedLeaseRevision: 1,
		PrepareTerminalDigest:    executionpin.Digest(prepared.PreparedDigest),
		AcquireCertificateDigest: acquireDigest,
	}
	releaseBytes := executionPinCommand(fixture.binding, client, 2, 3, release)
	intent, err := requestledger.NewSchemaPinRelease(
		head, prepared, head.Revision+1, releaseBytes,
	)
	if err != nil || !requestLedgerSchemaReleaseCommandAvailable(intent) {
		t.Fatalf("schema release intent evidence = %v", err)
	}
	if _, err = fixture.machine.ApplyNormal(normalMeta(4), releaseBytes); err != nil {
		t.Fatal(err)
	}
	settlement, err := fixture.machine.LookupCompletion(releaseBytes)
	if err != nil {
		t.Fatal(err)
	}
	released, err := requestledger.RecordVerifiedSchemaPinReleased(
		intent, intent.Revision+1, settlement.Bytes,
	)
	if err != nil || !requestLedgerSchemaReleaseEvidenceAvailable(released) {
		t.Fatalf("schema release completion evidence = %v", err)
	}
	wrongBinding := intent
	wrongBinding.PinDigest[0] ^= 0x80
	if requestLedgerSchemaReleaseCommandAvailable(wrongBinding) {
		t.Fatal("schema release accepted another execution binding digest")
	}
	wrongCompletion := released
	acquireSettlement, err := fixture.machine.LookupCompletion(acquireBytes)
	if err != nil {
		t.Fatal(err)
	}
	wrongCompletion.Completion = acquireSettlement.Bytes
	if requestLedgerSchemaReleaseEvidenceAvailable(wrongCompletion) {
		t.Fatal("schema release accepted an acquire proof as terminal release evidence")
	}
}
