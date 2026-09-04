package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type recoveryAuthorityClient struct{ seen []serviceauthz.Authority }

func (c *recoveryAuthorityClient) DoReplicated(_ context.Context, _ ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	c.seen = append(c.seen, request.Authority)
	return nil, ErrReplicatedUnauthorized
}

func TestDurableRecoveryUsesServiceAuthorityNotCaller(t *testing.T) {
	_, _, route := lifecycleRunnerFixture(t)
	service := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	caller := serviceauthz.Authority{Node: [16]byte{2}, Generation: 1}
	client := &recoveryAuthorityClient{}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	waves := &DurableRequestLifecycleRunner{proposer: executor, pinAuthority: service}
	runner, err := NewDurableRequestDistributedRunner(&distributedRunnerLedger{}, distributedRunnerResolver{base: route}, waves,
		&DurableRequestDynamicPayloadStore{}, &DurableRequestTerminalCoordinator{}, distributedRunnerAuthority{}, &NativeDurableRequestExecutionPinAuthority{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(t.Context(), caller)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = runner.recovery.ReadTransactionRecovery(ctx, route, replicatedstate.TransactionRecoveryReadRequest{
		Kind: replicatedstate.TransactionRecoveryLookupCoordinator, ID: distributedtxn.ID{1}, MinimumApplied: 1, MaxRows: 1,
		MaxBytes: uint32(replicatedstate.TransactionRecoverySummaryBytes + distributedtxn.MaxCoordinatorRecordBytes),
	})
	if len(client.seen) == 0 {
		t.Fatal("no recovery probe")
	}
	for _, got := range client.seen {
		if got != service {
			t.Fatalf("recovery forwarded caller authority: got=%+v want=%+v", got, service)
		}
	}
	if got, _ := serviceauthz.FromContext(ctx); got != caller {
		t.Fatal("modified caller context")
	}
}

type distributedRunnerLedger struct {
	head         requestledger.HeadRecord
	continuation requestledger.ContinuationRecord
}

func (ledger *distributedRunnerLedger) ApplyCAS(context.Context, DurableRequestLedgerHome, requestledger.RequestKey, DurableRequestLifecycleCAS) (DurableRequestLifecycleCASResult, error) {
	return DurableRequestLifecycleCASResult{}, ErrDurableRequest
}
func (ledger *distributedRunnerLedger) ReadRow(_ context.Context, _ DurableRequestLedgerHome, read DurableRequestLifecycleRead) (DurableRequestLifecycleRow, error) {
	switch read.Kind {
	case replicatedstate.RequestLedgerReadHead:
		return DurableRequestLifecycleRow{Applied: max(uint64(1), ledger.head.Revision), Found: true, Kind: read.Kind, Head: ledger.head}, nil
	case replicatedstate.RequestLedgerReadContinuation:
		return DurableRequestLifecycleRow{Applied: max(uint64(1), ledger.head.Revision), Found: ledger.continuation.Revision != 0, Kind: read.Kind, Continuation: ledger.continuation}, nil
	default:
		return DurableRequestLifecycleRow{}, ErrDurableRequest
	}
}

type distributedRunnerPayloads struct{}

type advancedRecoveryOrderWaves struct {
	*distributedRunnerWaves
	calls   int
	failure error
}

func (waves *advancedRecoveryOrderWaves) ResumeAdvancedWave(context.Context, DurableRequestTypedExecutionContext) error {
	waves.calls++
	if waves.failure != nil {
		return waves.failure
	}
	waves.ledger.head.OutstandingRoutePinDigest = requestledger.Digest{}
	return nil
}

type advancedRecoveryOrderCleanup struct {
	ledger *distributedRunnerLedger
	calls  int
}

func (cleanup *advancedRecoveryOrderCleanup) Cleanup(context.Context, DurableRequestLedgerHome, requestledger.RequestKey) (uint64, error) {
	cleanup.calls++
	if cleanup.ledger.head.OutstandingRoutePinDigest != (requestledger.Digest{}) {
		return 0, ErrDurableRequestConflict
	}
	// Stop after observing ordering; transaction execution is covered separately.
	return 0, errDynamicPayloadFault
}

func TestDurableRequestDistributedRunnerRecoversRouteBeforePayloadCleanup(t *testing.T) {
	for _, state := range []string{"outstanding", "released", "partly-cleaned", "release-unknown"} {
		t.Run(state, func(t *testing.T) {
			execution := typedExecutionFixture(t)
			_, head, route := lifecycleRunnerFixture(t)
			head.NextStepOrdinal = 1
			head.CleanupBuildDigest = requestledger.Digest{1}
			if state == "outstanding" || state == "release-unknown" {
				head.OutstandingRoutePinDigest = requestledger.Digest{2}
			}
			if state == "partly-cleaned" {
				head.CleanupNextChunk = 1
			}
			ledger := &distributedRunnerLedger{head: head}
			waves := &advancedRecoveryOrderWaves{distributedRunnerWaves: &distributedRunnerWaves{ledger: ledger}}
			if state == "release-unknown" {
				waves.failure = errLifecycleRunnerFault
			}
			cleanup := &advancedRecoveryOrderCleanup{ledger: ledger}
			runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, cleanup,
				&distributedRunnerTerminal{}, distributedRunnerAuthority{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = runner.RunTyped(t.Context(), execution)
			if state == "release-unknown" {
				if !errors.Is(err, errLifecycleRunnerFault) || waves.calls != 1 || cleanup.calls != 0 {
					t.Fatalf("recovery=%d cleanup=%d err=%v", waves.calls, cleanup.calls, err)
				}
				return
			}
			wantRecovery := 1
			if state == "partly-cleaned" {
				wantRecovery = 0
			}
			if !errors.Is(err, errDynamicPayloadFault) || waves.calls != wantRecovery || cleanup.calls != 1 {
				t.Fatalf("recovery=%d cleanup=%d err=%v", waves.calls, cleanup.calls, err)
			}
		})
	}
}

func (distributedRunnerPayloads) Stage(_ context.Context, _ DurableRequestLedgerHome, _ requestledger.RequestKey, _ uint64, target, command []byte) (DurableRequestDynamicPayload, error) {
	step := requestledger.StepRef{TargetSource: requestledger.PayloadSourceDynamic, CommandSource: requestledger.PayloadSourceDynamic,
		TargetLength: uint64(len(target)), CommandOffset: uint64(len(target)), CommandLength: uint64(len(command)),
		TargetDigest:  requestledger.Digest(replication.CompletionResultDigest(1, 1, target)),
		CommandDigest: requestledger.Digest(replication.CompletionResultDigest(1, 1, command))}
	return DurableRequestDynamicPayload{Step: step, Target: bytes.Clone(target), Command: bytes.Clone(command)}, nil
}
func (distributedRunnerPayloads) Cleanup(context.Context, DurableRequestLedgerHome, requestledger.RequestKey) (uint64, error) {
	return 1, nil
}

type distributedRunnerWaves struct {
	ledger               *distributedRunnerLedger
	fault                distributedtxn.ReplicatedOperation
	prepareConflict      bool
	manifestPayload      []byte
	decisionSettled      bool
	admissionFailures    int
	admissionError       error
	admissionCommands    [][]byte
	wantMembershipStable bool
}

func (waves *distributedRunnerWaves) ReadTransactionRecovery(_ context.Context, route ReplicatedRoute, read replicatedstate.TransactionRecoveryReadRequest) (ReplicatedTransactionRecoveryResult, error) {
	if route.membershipStable != waves.wantMembershipStable {
		return ReplicatedTransactionRecoveryResult{}, errors.New("recovery lost sealed membership mode")
	}
	if read.Kind == replicatedstate.TransactionRecoveryLookupTarget {
		if waves.decisionSettled {
			// Once the decision is durable, participants may already have
			// applied/released and retired their prepare records.
			return ReplicatedTransactionRecoveryResult{}, errors.New("prepared state read after durable decision")
		}
		return ReplicatedTransactionRecoveryResult{Complete: true, Records: []replicatedstate.TransactionRecoveryRecord{{
			ID: read.ID, Role: distributedtxn.ReplicatedRoleTarget,
			State: uint8(distributedtxn.TargetPrepared),
		}}}, nil
	}
	if len(waves.manifestPayload) == 0 {
		return ReplicatedTransactionRecoveryResult{}, ErrDurableRequestUnresolved
	}
	control, err := distributedtxn.OpenManifestCoordinator(waves.manifestPayload)
	if err != nil {
		return ReplicatedTransactionRecoveryResult{}, err
	}
	return ReplicatedTransactionRecoveryResult{Complete: true, Records: []replicatedstate.TransactionRecoveryRecord{{
		ID: control.ID, Role: distributedtxn.ReplicatedRoleCoordinator,
		PayloadKind: distributedtxn.ReplicatedPayloadManifestCoordinator,
		Payload:     bytes.Clone(waves.manifestPayload),
	}}}, nil
}

func (waves *distributedRunnerWaves) RunStagedWave(_ context.Context, wave DurableRequestWave) (DurableRequestWaveResult, error) {
	if waves.admissionError != nil {
		waves.admissionCommands = append(waves.admissionCommands, bytes.Clone(wave.Command))
		if waves.admissionFailures > 0 {
			waves.admissionFailures--
			return DurableRequestWaveResult{}, waves.admissionError
		}
	}
	command, err := replication.OpenCommand(wave.Command)
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	if replication.IsMembershipStableAuthority(command.AuthorityClass) != waves.wantMembershipStable {
		return DurableRequestWaveResult{}, errors.New("wave lost sealed membership mode")
	}
	controlView, err := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	control := controlView.Command()
	if control.ControllerEpoch != wave.CommandEpoch || wave.CommandEpoch == 0 || wave.CommandEpoch > wave.ExecutionPinLease.ControllerEpoch ||
		control.ExecutionPinDigest != distributedtxn.Digest(wave.Binding) ||
		wave.GateEpoch != wave.ExecutionPinLease.ControllerEpoch {
		return DurableRequestWaveResult{}, fmt.Errorf("transaction command lacks exact execution-pin fence")
	}
	if control.Operation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator {
		coordinator, _, openErr := distributedtxn.OpenReplicatedManifestStart(control.Payload)
		if openErr != nil {
			return DurableRequestWaveResult{}, openErr
		}
		waves.manifestPayload = bytes.Clone(coordinator)
	}
	affected := int64(-1)
	if control.Operation == distributedtxn.ReplicatedApplyReleaseTarget {
		affected = 1
	}
	if control.Operation == distributedtxn.ReplicatedRetireCoordinator {
		summary, openErr := distributedtxn.OpenReplicatedRetirementSummary(control.Payload)
		if openErr != nil {
			return DurableRequestWaveResult{}, openErr
		}
		if summary.AffectedRowsValid {
			affected = summary.AffectedRows
		}
	}
	code := uint32(replicatedstate.ResultApplied)
	if waves.prepareConflict && control.Operation == distributedtxn.ReplicatedStagePrepareTarget {
		waves.prepareConflict = false
		code = replicatedstate.ResultIndexConflict
	}
	result := transactionOrchestratorResult(control.Role, control.Operation, max(uint64(1), control.ExpectedRevision+1), affected)
	observation := appendTransactionOrchestratorCompletion(command, code, result[:], wave.Ordinal+1)
	transition, cursor, err := wave.Settle(observation)
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	waves.ledger.head.NextStepOrdinal = wave.Ordinal + 1
	waves.ledger.head.Revision++
	waves.ledger.continuation = requestledger.ContinuationRecord{
		Revision: waves.ledger.head.Revision, SettledOrdinal: wave.Ordinal,
		TransitionTag: transition, Cursor: bytes.Clone(cursor), Observation: bytes.Clone(observation),
	}
	if control.Operation == distributedtxn.ReplicatedCommitCoordinator || control.Operation == distributedtxn.ReplicatedAbortCoordinator {
		waves.decisionSettled = true
	}
	if waves.fault == control.Operation {
		waves.fault = distributedtxn.ReplicatedOperationInvalid
		return DurableRequestWaveResult{}, errLifecycleRunnerFault
	}
	return DurableRequestWaveResult{Observation: observation, Revision: waves.ledger.head.Revision}, nil
}

type refreshingRunnerLedger struct{ *distributedRunnerLedger }

func (l refreshingRunnerLedger) ReadTerminalCut(context.Context, DurableRequestLedgerHome, requestledger.RequestKey) (durableRequestTerminalReadCut, error) {
	return durableRequestTerminalReadCut{Head: l.head, Continuation: l.continuation, Applied: max(uint64(1), l.head.Revision)}, nil
}

type refreshingRunnerPins struct {
	record executionpin.Record
	calls  int
}

func (p *refreshingRunnerPins) AcquireOrRecover(_ context.Context, e DurableRequestTypedExecutionContext) (ReplicatedRoute, executionpin.AcquireCertificate, executionpin.LeaseCertificate, error) {
	if p.calls == 0 {
		a, l := e.ExecutionPinAcquire, e.ExecutionPinLease
		p.record = executionpin.Record{Status: executionpin.StatusActive, LastOperation: executionpin.OperationAcquire,
			PinID: a.PinID, Binding: a.Binding, AcquireAuthorityDigest: a.AuthorityDigest, CurrentAuthorityDigest: l.AuthorityDigest,
			AcquireApplied: a.Applied, AcquireController: a.Controller, AcquireControllerEpoch: a.ControllerEpoch, AcquireLeaseAppliedThrough: a.LeaseAppliedThrough,
			Controller: l.Controller, ControllerEpoch: l.ControllerEpoch, LeaseAppliedThrough: l.LeaseAppliedThrough, LeaseRevision: l.Revision, LeaseApplied: l.Applied,
			LastApplied: a.Applied, LastCommandDigest: executionpin.Digest{1}}
	}
	a, _ := p.record.AcquireCertificate()
	digest, _ := executionpin.AcquireCertificateDigest(a)
	c := executionpin.Command{Operation: executionpin.OperationRecover, Binding: p.record.Binding, PinID: p.record.PinID,
		AuthorityNode: p.record.Controller, AuthorityGeneration: 1, ExpectedController: p.record.Controller, ExpectedControllerEpoch: p.record.ControllerEpoch,
		ExpectedLeaseAppliedThrough: p.record.LeaseAppliedThrough, ExpectedLeaseRevision: p.record.LeaseRevision,
		NextController: p.record.Controller, NextControllerEpoch: p.record.ControllerEpoch + 1, NextLeaseSpan: 1, AcquireCertificateDigest: digest}
	transition := executionpin.Apply(p.record, true, c, p.record.LeaseAppliedThrough+1, executionpin.Digest{3}, executionpin.Digest{byte(p.calls + 2)})
	if transition.Reason != executionpin.ReasonApplied || !transition.Mutated {
		return ReplicatedRoute{}, a, executionpin.LeaseCertificate{}, ErrDurableRequestConflict
	}
	p.record = transition.Record
	p.calls++
	l, _ := p.record.LeaseCertificate()
	return e.Home.borrowedRoute(), a, l, nil
}

type retainedEpochRunnerPayloads struct{ distributedRunnerPayloads }

func (retainedEpochRunnerPayloads) ExistingCommandEpoch(_ context.Context, _ DurableRequestLedgerHome, _ requestledger.RequestKey, ordinal uint64) (uint64, error) {
	if ordinal == 0 {
		return 1, nil
	}
	return 0, nil
}

type refreshingRunnerAuthority struct {
	distributedRunnerAuthority
	lastEpoch uint64
}

func (a *refreshingRunnerAuthority) TerminalAuthority(_ context.Context, e DurableRequestTypedExecutionContext) (DurableRequestTerminalAuthority, error) {
	a.lastEpoch = e.ExecutionPinLease.ControllerEpoch
	value := a.value
	value.AckToken[0] = byte(a.lastEpoch)
	value.Release.ExpectedController = e.ExecutionPinLease.Controller
	value.Release.ExpectedControllerEpoch = a.lastEpoch
	value.Release.ExpectedLeaseAppliedThrough = e.ExecutionPinLease.LeaseAppliedThrough
	value.Release.ExpectedLeaseRevision = e.ExecutionPinLease.Revision
	return value, nil
}

func TestDurableRequestDistributedRunnerRefreshesOnlyUnfinishedWavesAndTerminal(t *testing.T) {
	execution := typedExecutionFixture(t)
	execution.Recipe.Contract.CommitFinalWaveCount, execution.Recipe.Contract.AbortFinalWaveCount = 8, 11
	commit, abort := []byte("terminal-commit"), []byte("terminal-abort")
	execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commit))
	execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abort))
	_, head, route := lifecycleRunnerFixture(t)
	execution, release := bindTypedExecutionPin(t, execution, route)
	head.NextStepOrdinal, head.Revision = 0, 1
	base := &distributedRunnerLedger{head: head}
	ledger := refreshingRunnerLedger{base}
	waves := &distributedRunnerWaves{ledger: base, fault: distributedtxn.ReplicatedBeginPrepareManifestCoordinator}
	terminal := &distributedRunnerTerminal{}
	authority := &refreshingRunnerAuthority{distributedRunnerAuthority: distributedRunnerAuthority{value: DurableRequestTerminalAuthority{CommitCursor: commit, AbortCursor: abort, Release: release}}}
	runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, retainedEpochRunnerPayloads{}, terminal, authority)
	if err != nil {
		t.Fatal(err)
	}
	pins := &refreshingRunnerPins{}
	runner.pins = pins
	if _, err = runner.RunTyped(t.Context(), execution); !errors.Is(err, errLifecycleRunnerFault) {
		t.Fatal("expected committed first-wave cut", err)
	}
	if pins.calls != 1 {
		t.Fatalf("first wave acquisitions=%d", pins.calls)
	}
	if err = execution.Targets.Reset(); err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunTyped(t.Context(), execution); err != nil {
		t.Fatal(err)
	}
	if pins.calls != 9 || terminal.plan.Lease.ControllerEpoch != 10 || authority.lastEpoch != 10 || terminal.plan.AckToken[0] != 10 || terminal.plan.Release.ExpectedControllerEpoch != 10 {
		t.Fatalf("refreshes=%d terminalepoch=%d authority=%d ack=%d release=%d", pins.calls, terminal.plan.Lease.ControllerEpoch, authority.lastEpoch, terminal.plan.AckToken[0], terminal.plan.Release.ExpectedControllerEpoch)
	}
	old := execution.ExecutionPinLease
	if executionpin.ValidateSideEffectFence(old, pins.record, pins.record.LeaseApplied) == nil {
		t.Fatal("old controller retained side-effect authority")
	}
}

func TestDurableRequestWaveRetriesOnlyExpiredAdmissionWithinBound(t *testing.T) {
	for _, test := range []struct {
		name               string
		failures, attempts int
		fault              error
		admitted           bool
	}{
		{"expired", 1, 2, &durableExecutionPinAdvancedError{}, true},
		{"bounded", 4, 4, &durableExecutionPinAdvancedError{}, false},
		{"identity-conflict", 1, 1, ErrDurableRequestConflict, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			execution := typedExecutionFixture(t)
			execution.Recipe.Contract.CommitFinalWaveCount, execution.Recipe.Contract.AbortFinalWaveCount = 8, 11
			commit, abort := []byte("terminal-commit"), []byte("terminal-abort")
			execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commit))
			execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abort))
			_, head, route := lifecycleRunnerFixture(t)
			execution, release := bindTypedExecutionPin(t, execution, route)
			head.NextStepOrdinal, head.Revision = 0, 1
			base := &distributedRunnerLedger{head: head}
			waves := &distributedRunnerWaves{ledger: base, fault: distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
				admissionFailures: test.failures, admissionError: test.fault}
			authority := &refreshingRunnerAuthority{distributedRunnerAuthority: distributedRunnerAuthority{value: DurableRequestTerminalAuthority{CommitCursor: commit, AbortCursor: abort, Release: release}}}
			runner, err := newDurableRequestDistributedRunner(refreshingRunnerLedger{base}, distributedRunnerResolver{base: route}, waves, retainedEpochRunnerPayloads{}, &distributedRunnerTerminal{}, authority)
			if err != nil {
				t.Fatal(err)
			}
			pins := &refreshingRunnerPins{}
			runner.pins = pins
			_, err = runner.RunTyped(t.Context(), execution)
			want := test.fault
			if test.admitted {
				want = errLifecycleRunnerFault
			}
			if !errors.Is(err, want) || pins.calls != test.attempts || len(waves.admissionCommands) != test.attempts {
				t.Fatalf("calls=%d commands=%d err=%v", pins.calls, len(waves.admissionCommands), err)
			}
			for _, command := range waves.admissionCommands {
				if !bytes.Equal(command, waves.admissionCommands[0]) {
					t.Fatal("lease refresh rebuilt exact participant command")
				}
			}
		})
	}
}

type distributedRunnerTerminal struct {
	plan              DurableRequestTerminalPlan
	fault             bool
	admissionFailures int
	calls             int
}

func (terminal *distributedRunnerTerminal) Complete(_ context.Context, plan DurableRequestTerminalPlan) (DurableRequestTerminalResult, error) {
	terminal.plan = plan
	terminal.calls++
	if terminal.admissionFailures > 0 {
		terminal.admissionFailures--
		return DurableRequestTerminalResult{}, &durableExecutionPinAdvancedError{}
	}
	if terminal.fault {
		terminal.fault = false
		return DurableRequestTerminalResult{}, errLifecycleRunnerFault
	}
	return DurableRequestTerminalResult{Revision: 1, Applied: 1}, nil
}

type distributedRunnerAuthority struct {
	value DurableRequestTerminalAuthority
}

func (authority distributedRunnerAuthority) TerminalAuthority(context.Context, DurableRequestTypedExecutionContext) (DurableRequestTerminalAuthority, error) {
	return authority.value, nil
}

type distributedRunnerResolver struct{ base ReplicatedRoute }

func (resolver distributedRunnerResolver) ResolveDurableRequestTarget(_ context.Context, logical DurableRequestLogicalTarget) (ReplicatedRoute, error) {
	route := cloneDurableRequestRoute(resolver.base)
	route.Distribution, route.Shard, route.Group = logical.Distribution, logical.Shard, logical.Group
	route.RangeIdentity = logical.RangeIdentity
	route.LineageDigest = logical.LineageDigest
	route.ForwardingRuleDigest = logical.ForwardingRuleDigest
	route.AllocationGeneration = 1
	route.Command.SchemaGeneration = logical.SchemaGeneration
	route.Command.RelationManifestDigest = replication.Digest(logical.RelationManifestDigest)
	return route, nil
}

func TestDurableRequestDistributedRunnerResumesProtocolCuts(t *testing.T) {
	for _, stable := range []bool{false, true} {
		t.Run(fmt.Sprintf("membership_stable_%t", stable), func(t *testing.T) {
			testDurableRequestDistributedRunnerResumesProtocolCuts(t, stable)
		})
	}
}

func testDurableRequestDistributedRunnerResumesProtocolCuts(t *testing.T, stable bool) {
	for _, fault := range []distributedtxn.ReplicatedOperation{
		distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		distributedtxn.ReplicatedStagePrepareTarget,
		distributedtxn.ReplicatedCommitCoordinator,
		distributedtxn.ReplicatedApplyReleaseTarget,
		distributedtxn.ReplicatedRetireCoordinator,
		distributedtxn.ReplicatedAbortCoordinator,
		distributedtxn.ReplicatedAbortReleaseTarget,
	} {
		t.Run(fmt.Sprintf("operation_%d", fault), func(t *testing.T) {
			aborted := fault == distributedtxn.ReplicatedAbortCoordinator || fault == distributedtxn.ReplicatedAbortReleaseTarget
			execution := typedExecutionFixture(t)
			execution.Recipe.Contract.CommitFinalWaveCount = 8
			execution.Recipe.Contract.AbortFinalWaveCount = 11
			// Production terminal cursors share the decision-state encoding.
			commitCursor := appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedCommitted})
			abortCursor := appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedAborted})
			execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commitCursor))
			execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abortCursor))
			if stable {
				execution.Recipe.Contract.ProtocolProgramDigest = durableRequestMembershipStableProgramDigest(execution.Recipe.Contract)
			}
			wave, head, route := lifecycleRunnerFixture(t)
			execution, release := bindTypedExecutionPin(t, execution, route)
			head.NextStepOrdinal, head.Revision = 0, 1
			ledger := &distributedRunnerLedger{head: head}
			waves := &distributedRunnerWaves{ledger: ledger, fault: fault, prepareConflict: aborted, wantMembershipStable: stable}
			terminal := &distributedRunnerTerminal{}
			authority := DurableRequestTerminalAuthority{CommitCursor: commitCursor, AbortCursor: abortCursor, AckToken: requestledger.AckToken{1}, Release: release}
			runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, distributedRunnerPayloads{}, terminal, distributedRunnerAuthority{value: authority})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = runner.RunTyped(context.Background(), execution); !errors.Is(err, errLifecycleRunnerFault) {
				t.Fatalf("first run err=%v", err)
			}
			if err = execution.Targets.Reset(); err != nil {
				t.Fatal(err)
			}
			if _, err = runner.RunTyped(context.Background(), execution); err != nil {
				t.Fatal(err)
			}
			wantOutcome, wantRows := requestledger.OutcomeCommitted, int64(3)
			if aborted {
				wantOutcome, wantRows = requestledger.OutcomeAborted, 0
			}
			if terminal.plan.AffectedRowsValid == aborted || terminal.plan.AffectedRows != wantRows || terminal.plan.Outcome != wantOutcome || wave.Identity.ID == ([16]byte{}) {
				t.Fatalf("terminal=%+v", terminal.plan)
			}
		})
	}
}

func TestDurableTerminalAdmissionRetryIsBounded(t *testing.T) {
	for _, failures := range []int{1, 4} {
		execution := typedExecutionFixture(t)
		_, _, route := lifecycleRunnerFixture(t)
		execution, _ = bindTypedExecutionPin(t, execution, route)
		terminal := &distributedRunnerTerminal{admissionFailures: failures}
		runner := &DurableRequestDistributedRunner{terminal: terminal}
		_, err := runner.completeTerminal(t.Context(), execution, DurableRequestTerminalAuthority{}, durableDistributedState{branch: durableDistributedCommitted})
		if terminal.calls != min(failures+1, 4) || (err != nil) != (failures == 4) {
			t.Fatalf("failures=%d calls=%d err=%v", failures, terminal.calls, err)
		}
	}
}

func TestDurableDistributedStateCanonical(t *testing.T) {
	want := durableDistributedState{branch: durableDistributedCommitted, conflict: true, affected: 19}
	raw := appendDurableDistributedState(nil, want)
	got, err := openDurableDistributedState(raw)
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	raw[24] = 1
	if _, err = openDurableDistributedState(raw); err == nil {
		t.Fatal("noncanonical cursor accepted")
	}
}

func TestDurableRequestDistributedRunnerDurablyAbortsPrepareConflict(t *testing.T) {
	execution := typedExecutionFixture(t)
	execution.Recipe.Contract.CommitFinalWaveCount, execution.Recipe.Contract.AbortFinalWaveCount = 8, 11
	commitCursor, abortCursor := []byte("terminal-commit"), []byte("terminal-abort")
	execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commitCursor))
	execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abortCursor))
	_, head, route := lifecycleRunnerFixture(t)
	execution, release := bindTypedExecutionPin(t, execution, route)
	head.NextStepOrdinal, head.Revision = 0, 1
	ledger := &distributedRunnerLedger{head: head}
	waves := &distributedRunnerWaves{ledger: ledger, prepareConflict: true}
	terminal := &distributedRunnerTerminal{}
	authority := DurableRequestTerminalAuthority{CommitCursor: commitCursor, AbortCursor: abortCursor, AckToken: requestledger.AckToken{1}, Release: release}
	runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, distributedRunnerPayloads{}, terminal, distributedRunnerAuthority{value: authority})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunTyped(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if terminal.plan.Outcome != requestledger.OutcomeAborted || terminal.plan.AffectedRowsValid ||
		terminal.plan.AffectedRows != 0 || ledger.head.NextStepOrdinal != 11 {
		t.Fatalf("terminal=%+v waves=%d", terminal.plan, ledger.head.NextStepOrdinal)
	}
}

func TestDurableRequestDistributedRunnerResumesTerminalHandoff(t *testing.T) {
	execution := typedExecutionFixture(t)
	execution.Recipe.Contract.CommitFinalWaveCount, execution.Recipe.Contract.AbortFinalWaveCount = 8, 11
	commitCursor, abortCursor := []byte("terminal-commit"), []byte("terminal-abort")
	execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commitCursor))
	execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abortCursor))
	_, head, route := lifecycleRunnerFixture(t)
	execution, release := bindTypedExecutionPin(t, execution, route)
	head.NextStepOrdinal, head.Revision = 0, 1
	ledger := &distributedRunnerLedger{head: head}
	waves := &distributedRunnerWaves{ledger: ledger}
	terminal := &distributedRunnerTerminal{fault: true}
	authority := DurableRequestTerminalAuthority{CommitCursor: commitCursor, AbortCursor: abortCursor, AckToken: requestledger.AckToken{1}, Release: release}
	runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, distributedRunnerPayloads{}, terminal, distributedRunnerAuthority{value: authority})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunTyped(context.Background(), execution); !errors.Is(err, errLifecycleRunnerFault) {
		t.Fatalf("first terminal err=%v", err)
	}
	if ledger.head.NextStepOrdinal != 8 {
		t.Fatalf("waves=%d", ledger.head.NextStepOrdinal)
	}
	if err = execution.Targets.Reset(); err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunTyped(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if terminal.plan.Outcome != requestledger.OutcomeCommitted || terminal.plan.AffectedRows != 3 {
		t.Fatalf("terminal=%+v", terminal.plan)
	}
}

func BenchmarkDurableRequestDistributedRunnerBoundedStream(b *testing.B) {
	for _, count := range []int{3, 129, 1025} {
		b.Run(fmt.Sprintf("participants_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				execution := typedExecutionFixtureCount(b, count)
				// These fixtures fit one manifest command. The schedule has no
				// participant cap and is derived from the sealed count.
				execution.Recipe.Contract.CommitFinalWaveCount = uint64(2*count + 2)
				execution.Recipe.Contract.AbortFinalWaveCount = uint64(3*count + 2)
				commitCursor, abortCursor := []byte("terminal-commit"), []byte("terminal-abort")
				execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commitCursor))
				execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abortCursor))
				_, head, route := lifecycleRunnerFixture(b)
				execution, release := bindTypedExecutionPin(b, execution, route)
				head.NextStepOrdinal, head.Revision = 0, 1
				ledger := &distributedRunnerLedger{head: head}
				waves := &distributedRunnerWaves{ledger: ledger}
				terminal := &distributedRunnerTerminal{}
				authority := DurableRequestTerminalAuthority{CommitCursor: commitCursor, AbortCursor: abortCursor, AckToken: requestledger.AckToken{1}, Release: release}
				runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, distributedRunnerPayloads{}, terminal, distributedRunnerAuthority{value: authority})
				if err != nil {
					b.Fatal(err)
				}
				if _, err = runner.RunTyped(context.Background(), execution); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestDurableRequestDistributedRunnerStreamsMoreThan64Targets(t *testing.T) {
	execution := typedExecutionFixtureCount(t, 129)
	// One manifest command + (P-1) prepare + decision + P finish + retire.
	execution.Recipe.Contract.CommitFinalWaveCount = 260
	execution.Recipe.Contract.AbortFinalWaveCount = 389
	commitCursor, abortCursor := []byte("terminal-commit"), []byte("terminal-abort")
	execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commitCursor))
	execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abortCursor))
	_, head, route := lifecycleRunnerFixture(t)
	execution, release := bindTypedExecutionPin(t, execution, route)
	head.NextStepOrdinal, head.Revision = 0, 1
	ledger := &distributedRunnerLedger{head: head}
	waves := &distributedRunnerWaves{ledger: ledger}
	terminal := &distributedRunnerTerminal{}
	authority := DurableRequestTerminalAuthority{CommitCursor: commitCursor, AbortCursor: abortCursor, AckToken: requestledger.AckToken{1}, Release: release}
	runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, distributedRunnerPayloads{}, terminal, distributedRunnerAuthority{value: authority})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunTyped(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if ledger.head.NextStepOrdinal != 260 || terminal.plan.AffectedRows != 129 ||
		execution.Targets.BufferedBytes() > durableRequestReaderMaxLiveBytes {
		t.Fatalf("waves=%d rows=%d buffered=%d", ledger.head.NextStepOrdinal, terminal.plan.AffectedRows, execution.Targets.BufferedBytes())
	}
}
