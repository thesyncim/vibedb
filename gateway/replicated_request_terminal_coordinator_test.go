package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

type terminalCoordinatorLedger struct {
	head         requestledger.HeadRecord
	continuation requestledger.ContinuationRecord
	prepared     requestledger.PreparedTerminalRecord
	release      requestledger.SchemaPinReleaseRecord
	terminal     requestledger.TerminalRecord
	fault        requestledger.Operation
	operations   []requestledger.Operation
}

func (ledger *terminalCoordinatorLedger) ApplyCAS(
	_ context.Context,
	_ DurableRequestLedgerHome,
	_ requestledger.RequestKey,
	cas DurableRequestLifecycleCAS,
) (DurableRequestLifecycleCASResult, error) {
	ledger.operations = append(ledger.operations, cas.Operation)
	if cas.ExpectedRevision != ledger.head.Revision || cas.Revision != ledger.head.Revision+1 {
		return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
	}
	var err error
	switch cas.Operation {
	case requestledger.OperationPrepareTerminal:
		ledger.head, err = requestledger.MarkTerminalPrepared(
			ledger.head, ledger.continuation, cas.Prepared,
		)
		ledger.prepared = cas.Prepared
	case requestledger.OperationBeginSchemaPinRelease:
		ledger.head, err = requestledger.InstallSchemaPinRelease(
			ledger.head, ledger.prepared, cas.SchemaPin,
		)
		ledger.release = cas.SchemaPin
	case requestledger.OperationRecordSchemaPinReleased:
		ledger.head, err = requestledger.MarkSchemaPinReleased(
			ledger.head, ledger.prepared, ledger.release, cas.SchemaPin,
		)
		ledger.release = cas.SchemaPin
	case requestledger.OperationComplete:
		ledger.head, err = requestledger.MarkTerminal(
			ledger.head, ledger.prepared, ledger.release, cas.Terminal,
		)
		ledger.terminal = cas.Terminal
	default:
		err = ErrDurableRequestConflict
	}
	if err != nil {
		return DurableRequestLifecycleCASResult{}, err
	}
	if ledger.fault == cas.Operation {
		ledger.fault = requestledger.OperationInvalid
		return DurableRequestLifecycleCASResult{}, errLifecycleRunnerFault
	}
	return DurableRequestLifecycleCASResult{
		Ledger: replicatedstate.RequestLedgerCompletionResult{
			ResultCode: replicatedstate.ResultApplied,
		},
		Applied: ledger.head.Revision + 100,
	}, nil
}

func (ledger *terminalCoordinatorLedger) ReadRow(
	_ context.Context,
	_ DurableRequestLedgerHome,
	read DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	row := DurableRequestLifecycleRow{Applied: ledger.head.Revision + 100, Kind: read.Kind}
	switch read.Kind {
	case replicatedstate.RequestLedgerReadHead:
		row.Found, row.Head = true, ledger.head
	case replicatedstate.RequestLedgerReadContinuation:
		row.Found, row.Continuation = true, ledger.continuation
	case replicatedstate.RequestLedgerReadPrepared:
		row.Found, row.Prepared = ledger.prepared.Revision != 0, ledger.prepared
	case replicatedstate.RequestLedgerReadSchemaPin:
		row.Found, row.SchemaPin = ledger.release.Revision != 0, ledger.release
	case replicatedstate.RequestLedgerReadTerminal:
		row.Found, row.Terminal = ledger.terminal.Revision != 0, ledger.terminal
	default:
		return DurableRequestLifecycleRow{}, ErrDurableRequestConflict
	}
	return row, nil
}

type terminalCoordinatorPin struct {
	t            testing.TB
	route        ReplicatedRoute
	tenant       []byte
	retryHome    replication.RetryHome
	clientID     replication.ID128
	epoch        uint64
	sequence     uint64
	record       executionpin.Record
	fault        bool
	attempts     [][]byte
	fenceCalls   int
	fenceFaultAt int
}

func (pin *terminalCoordinatorPin) ValidateFence(
	_ context.Context,
	lease executionpin.LeaseCertificate,
) error {
	pin.fenceCalls++
	if pin.fenceFaultAt == pin.fenceCalls {
		return ErrDurableRequestConflict
	}
	return executionpin.ValidateSideEffectFence(lease, pin.record, pin.record.LastApplied)
}

func (pin *terminalCoordinatorPin) BuildRelease(
	transition executionpin.Command,
) ([]byte, error) {
	var storage [executionpin.CommandBytes]byte
	nested, err := executionpin.AppendCommand(storage[:0], transition)
	if err != nil {
		return nil, err
	}
	outer := replicatedTransactionCommandHeader(
		pin.route, pin.tenant, pin.retryHome, pin.clientID, pin.epoch, pin.sequence,
	)
	outer.Kind = replication.CommandExecutionPin
	outer.AuthorityClass = replication.CommandAuthorityExecutionPin
	outer.ExecutionPin = nested
	outer.Fingerprint = nativeCommandFingerprint(outer)
	return replication.AppendCommand(nil, outer)
}

func (pin *terminalCoordinatorPin) ProposeNew(
	_ context.Context,
	transition executionpin.Command,
	exact []byte,
) (ReplicatedResult, error) {
	built, err := pin.BuildRelease(transition)
	if err != nil || !bytes.Equal(built, exact) {
		return ReplicatedResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	return pin.propose(exact)
}

func (pin *terminalCoordinatorPin) RetryExact(
	_ context.Context,
	exact []byte,
) (ReplicatedResult, error) {
	return pin.propose(exact)
}

func (pin *terminalCoordinatorPin) propose(exact []byte) (ReplicatedResult, error) {
	pin.attempts = append(pin.attempts, bytes.Clone(exact))
	if pin.fault {
		pin.fault = false
		return ReplicatedResult{}, errLifecycleRunnerFault
	}
	outer, err := replication.OpenCommand(exact)
	if err != nil {
		return ReplicatedResult{}, err
	}
	command, err := outer.OpenExecutionPin()
	if err != nil {
		return ReplicatedResult{}, err
	}
	authority, ok := replication.ExecutionPinAuthorityDigest(outer)
	if !ok {
		return ReplicatedResult{}, ErrDurableRequestConflict
	}
	transition := executionpin.Apply(
		pin.record, true, command, 77, executionpin.Digest(authority), executionpin.Digest{9},
	)
	if transition.Reason != executionpin.ReasonApplied {
		return ReplicatedResult{}, ErrDurableRequestConflict
	}
	pin.record = transition.Record
	proof, err := executionpin.CompletionFromRecord(executionpin.OperationRelease, pin.record)
	if err != nil {
		return ReplicatedResult{}, err
	}
	result, err := executionpin.AppendCompletion(nil, proof)
	if err != nil {
		return ReplicatedResult{}, err
	}
	digest := replication.CompletionResultDigest(
		replicatedstate.ResultApplied, replicatedstate.ResultFormatExecutionPin, result,
	)
	completion, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: outer.ClusterID, ClusterIncarnation: outer.ClusterIncarnation,
		TopologyRecoveryEpoch: outer.TopologyRecoveryEpoch,
		Distribution:          outer.Distribution, Shard: outer.Shard,
		AllocationGeneration: outer.AllocationGeneration,
		ShardIncarnation:     outer.ShardIncarnation, GroupID: outer.GroupID,
		ReplicaSetVersion:      outer.ReplicaSetVersion,
		ActivePolicyGeneration: outer.ActivePolicyGeneration,
		ProtectionEpoch:        outer.ProtectionEpoch,
		RoutingVersion:         outer.RoutingVersion, RouteGeneration: outer.RouteGeneration,
		Tenant: outer.Tenant, ClientID: outer.ClientID, ClientEpoch: outer.ClientEpoch,
		ClientSequence: outer.ClientSequence, Fingerprint: outer.Fingerprint,
		RetryHome: outer.RetryHome, AppliedSequence: 77,
		ResultCode:   replicatedstate.ResultApplied,
		ResultFormat: replicatedstate.ResultFormatExecutionPin,
		Storage:      replication.CompletionInline, ResultLength: uint64(len(result)),
		ResultDigest: digest, InlineResult: result,
	})
	if err != nil {
		return ReplicatedResult{}, err
	}
	return ReplicatedResult{
		Outcome:    raftserve.Outcome{Code: raftserve.OutcomeCompletion, AppliedIndex: 77},
		Completion: completion,
	}, nil
}

func terminalCoordinatorFixture(t testing.TB) (
	DurableRequestTerminalPlan,
	requestledger.HeadRecord,
	requestledger.ContinuationRecord,
	*terminalCoordinatorPin,
) {
	t.Helper()
	wave, _, route := lifecycleRunnerFixture(t)
	key := lifecycleKey()
	keyDigest, _ := requestledger.KeyDigest(key)
	binding := executionpin.Binding{
		RequestKeyDigest:          executionpin.Digest(keyDigest),
		RequestDigest:             executionpin.Digest(lifecycleDigest("terminal-request")),
		CatalogGeneration:         7,
		SchemaManifestDigest:      executionpin.Digest(lifecycleDigest("schema-certificate")),
		TransactionManifestDigest: executionpin.Digest{6},
		ParticipantAuthorityRoot:  executionpin.Digest{7}, ParticipantCount: 1,
		ExecutionContractDigest: executionpin.Digest{9},
		LedgerHomeGroup:         executionpin.ID(route.Group.GroupID),
	}
	executionPinID, err := executionpin.DerivePinID(binding)
	if err != nil {
		t.Fatal(err)
	}
	bindingDigest, err := executionpin.BindingDigest(binding)
	if err != nil {
		t.Fatal(err)
	}
	acquire := executionpin.Command{
		Operation: executionpin.OperationAcquire, Binding: binding, PinID: executionPinID,
		AuthorityNode: executionpin.ID{4}, AuthorityGeneration: 5,
		NextController: executionpin.ID{6}, NextControllerEpoch: 7,
		NextLeaseSpan: 1_000,
	}
	acquired := executionpin.Apply(
		executionpin.Record{}, false, acquire, 10, executionpin.Digest{11}, executionpin.Digest{12},
	)
	if acquired.Reason != executionpin.ReasonApplied {
		t.Fatal("execution pin acquire fixture failed")
	}
	certificate, ok := acquired.Record.AcquireCertificate()
	if !ok {
		t.Fatal("missing acquire certificate")
	}
	certificateDigest, err := executionpin.AcquireCertificateDigest(certificate)
	if err != nil {
		t.Fatal(err)
	}
	release := acquire
	release.Operation = executionpin.OperationRelease
	release.ExpectedController = acquired.Record.Controller
	release.ExpectedControllerEpoch = acquired.Record.ControllerEpoch
	release.ExpectedLeaseAppliedThrough = acquired.Record.LeaseAppliedThrough
	release.ExpectedLeaseRevision = acquired.Record.LeaseRevision
	release.NextController = executionpin.ID{}
	release.NextControllerEpoch, release.NextLeaseSpan = 0, 0
	release.AcquireCertificateDigest = certificateDigest

	planBytes, err := requestledger.AppendPlan(nil, bytes.Repeat([]byte{0x44}, 4096))
	if err != nil {
		t.Fatal(err)
	}
	cursor := []byte("terminal-cursor")
	contract := requestledger.ExecutionContract{
		CatalogGeneration: 7, PinID: requestledger.PinID{1},
		PinDigest:                    requestledger.Digest(bindingDigest),
		RouteSchemaCertificateDigest: requestledger.Digest(binding.SchemaManifestDigest),
		MaxPendingWaveBytes:          requestledger.MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         requestledger.MaxContinuationRecordBytes,
		MaxTerminalBytes:             requestledger.MaxLifecyclePayloadBytes,
		MaxActivePayloadBytes:        2 * requestledger.MaxPlanPageBytes,
		MaxActivePayloadChunks:       2,
		PlanBuildID:                  lifecycleDigest("terminal-plan-build"), PlanBuildGeneration: 1,
		PlanningLeaseExpiryIndex: math.MaxUint64, PlanningLeaseGeneration: 1,
		TerminalTransitionTag: 9, FinalWaveCount: 1,
		TerminalStateDigest:        requestledger.NextStateDigest(9, cursor),
		TerminalSummaryDigest:      lifecycleDigest("terminal-retirement"),
		AbortTerminalTransitionTag: 10, AbortFinalWaveCount: 1,
		AbortTerminalStateDigest: requestledger.NextStateDigest(10, cursor),
	}
	head, err := requestledger.NewHeadWithExecutionContract(
		key, requestledger.Digest(binding.RequestDigest), lifecycleDigest("terminal-contract"),
		contract, planBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	routePin, err := requestledger.NewRoutePinAcquiring(
		head, head.PinID, lifecycleDigest("route-binding"), lifecycleDigest("physical"),
		[]byte("acquire"),
	)
	if err != nil {
		t.Fatal(err)
	}
	routePin, err = requestledger.RecordVerifiedRoutePinAcquired(routePin, 2, []byte("acquired"))
	if err != nil {
		t.Fatal(err)
	}
	step := requestledger.StepRef{
		TargetSource: requestledger.PayloadSourcePlan, CommandSource: requestledger.PayloadSourcePlan,
		TargetOffset: 0, TargetLength: 8, CommandOffset: 8, CommandLength: 16,
		TargetDigest: lifecycleDigest("target"), CommandDigest: lifecycleDigest("command"),
	}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		head, requestledger.PayloadBuildRecord{}, head.Revision+1, routePin,
		[]requestledger.StepRef{step},
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallPendingWave(head, pending, requestledger.PayloadBuildRecord{}, routePin)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := requestledger.NewContinuation(
		head, pending, routePin, head.Revision+1, 9, cursor, []byte("settled"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvancePending(head, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	intent := routePin
	routePin, err = requestledger.BeginRoutePinRelease(routePin, 3, []byte("release"))
	if err != nil {
		t.Fatal(err)
	}
	intent = routePin
	routePin, err = requestledger.RecordVerifiedRoutePinReleased(routePin, 4, []byte("released"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkRoutePinReleased(head, routePin, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	_ = intent
	homePoint, _ := requestledger.Home(key)
	var ack requestledger.AckToken
	copy(ack[:], []byte("terminal-ack-capability-00000001"))
	pin := &terminalCoordinatorPin{
		t: t, route: route, tenant: wave.Tenant, retryHome: wave.Identity.RetryHome,
		clientID: replication.ID128{3}, epoch: 2, sequence: 2, record: acquired.Record,
	}
	lease, ok := acquired.Record.LeaseCertificate()
	if !ok {
		t.Fatal("missing acquired lease certificate")
	}
	return DurableRequestTerminalPlan{
		Home: DurableRequestLedgerHome{
			Identity: replication.Digest(lifecycleDigest("terminal-home")), Point: homePoint,
		},
		Key: key, Outcome: requestledger.OutcomeCommitted,
		AffectedRows: 12, AffectedRowsValid: true, Result: []byte("committed-result"),
		RetirementWitness: head.TerminalSummaryDigest, AckToken: ack, Release: release,
		Lease: lease,
	}, head, continuation, pin
}

func TestDurableRequestTerminalCoordinatorResumesEveryBoundary(t *testing.T) {
	faults := []struct {
		name      string
		operation requestledger.Operation
		proposal  bool
	}{
		{"prepare", requestledger.OperationPrepareTerminal, false},
		{"release_intent", requestledger.OperationBeginSchemaPinRelease, false},
		{"release_proposal", requestledger.OperationInvalid, true},
		{"release_proof", requestledger.OperationRecordSchemaPinReleased, false},
		{"complete", requestledger.OperationComplete, false},
	}
	for _, testCase := range faults {
		t.Run(testCase.name, func(t *testing.T) {
			plan, head, continuation, pin := terminalCoordinatorFixture(t)
			ledger := &terminalCoordinatorLedger{
				head: head, continuation: continuation, fault: testCase.operation,
			}
			pin.fault = testCase.proposal
			coordinator, err := newDurableRequestTerminalCoordinator(ledger, pin)
			if err != nil {
				t.Fatal(err)
			}
			var result DurableRequestTerminalResult
			for attempt := 0; attempt < 3; attempt++ {
				result, err = coordinator.Complete(t.Context(), plan)
				if err == nil {
					break
				}
				if !errors.Is(err, errLifecycleRunnerFault) {
					t.Fatalf("attempt %d: %v", attempt, err)
				}
			}
			if err != nil || result.Terminal.Revision == 0 ||
				ledger.head.Phase != requestledger.PhaseTerminal ||
				!bytes.Equal(result.Terminal.Result, plan.Result) {
				t.Fatalf("result=%+v head=%+v err=%v", result, ledger.head, err)
			}
			for index := 1; index < len(pin.attempts); index++ {
				if !bytes.Equal(pin.attempts[0], pin.attempts[index]) {
					t.Fatal("execution-pin retry changed exact command bytes")
				}
			}
		})
	}
}

func TestDurableRequestTerminalCoordinatorFencesPreparedAndReleaseSideEffects(t *testing.T) {
	plan, head, continuation, pin := terminalCoordinatorFixture(t)
	pin.fenceFaultAt = 1
	ledger := &terminalCoordinatorLedger{head: head, continuation: continuation}
	coordinator, err := newDurableRequestTerminalCoordinator(ledger, pin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = coordinator.Complete(t.Context(), plan); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("changed/released pin at terminal admission = %v", err)
	}
	if pin.fenceCalls != 1 || len(pin.attempts) != 0 {
		t.Fatalf("fences=%d release proposals=%d, want 1/0", pin.fenceCalls, len(pin.attempts))
	}
}

func TestDurableRequestTerminalCoordinatorRejectsChangedResultAfterPrepare(t *testing.T) {
	plan, head, continuation, pin := terminalCoordinatorFixture(t)
	ledger := &terminalCoordinatorLedger{
		head: head, continuation: continuation,
		fault: requestledger.OperationPrepareTerminal,
	}
	coordinator, err := newDurableRequestTerminalCoordinator(ledger, pin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = coordinator.Complete(t.Context(), plan); !errors.Is(err, errLifecycleRunnerFault) {
		t.Fatalf("prepare fault = %v", err)
	}
	plan.Result = []byte("changed-result")
	if _, err = coordinator.Complete(t.Context(), plan); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("changed result error = %v", err)
	}
	if len(pin.attempts) != 0 {
		t.Fatal("changed prepared result reached execution pin")
	}
}

func TestDurableRequestTerminalCoordinatorExactCommandDigestStable(t *testing.T) {
	plan, head, continuation, pin := terminalCoordinatorFixture(t)
	ledger := &terminalCoordinatorLedger{head: head, continuation: continuation}
	coordinator, _ := newDurableRequestTerminalCoordinator(ledger, pin)
	result, err := coordinator.Complete(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.Terminal.SchemaPinReleaseCertificateDigest == (requestledger.Digest{}) ||
		len(pin.attempts) != 1 || sha256.Sum256(pin.attempts[0]) == ([32]byte{}) {
		t.Fatal("terminal release lacks exact command certificate")
	}
}
