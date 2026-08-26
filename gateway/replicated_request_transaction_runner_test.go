package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

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

func (distributedRunnerPayloads) Stage(_ context.Context, _ DurableRequestLedgerHome, _ requestledger.RequestKey, target, command []byte) (DurableRequestDynamicPayload, error) {
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
	ledger          *distributedRunnerLedger
	fault           distributedtxn.ReplicatedOperation
	prepareConflict bool
	manifestPayload []byte
}

func (waves *distributedRunnerWaves) ReadTransactionRecovery(_ context.Context, _ ReplicatedRoute, read replicatedstate.TransactionRecoveryReadRequest) (ReplicatedTransactionRecoveryResult, error) {
	if read.Kind == replicatedstate.TransactionRecoveryLookupParticipant {
		return ReplicatedTransactionRecoveryResult{Complete: true, Records: []replicatedstate.TransactionRecoveryRecord{{
			ID: read.ID, Role: distributedtxn.ReplicatedRoleParticipant,
			State: uint8(distributedtxn.ParticipantPrepared),
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

func (waves *distributedRunnerWaves) RunWave(_ context.Context, wave DurableRequestWave) (DurableRequestWaveResult, error) {
	command, err := replication.OpenCommand(wave.Command)
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	controlView, err := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	control := controlView.Command()
	if control.Operation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator {
		coordinator, _, openErr := distributedtxn.OpenReplicatedManifestStart(control.Payload)
		if openErr != nil {
			return DurableRequestWaveResult{}, openErr
		}
		waves.manifestPayload = bytes.Clone(coordinator)
	}
	affected := int64(-1)
	if control.Operation == distributedtxn.ReplicatedApplyReleaseParticipant {
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
	if waves.prepareConflict && control.Operation == distributedtxn.ReplicatedStagePrepareParticipant {
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
	if waves.fault == control.Operation {
		waves.fault = distributedtxn.ReplicatedOperationInvalid
		return DurableRequestWaveResult{}, errLifecycleRunnerFault
	}
	return DurableRequestWaveResult{Observation: observation, Revision: waves.ledger.head.Revision}, nil
}

type distributedRunnerTerminal struct{ plan DurableRequestTerminalPlan }

func (terminal *distributedRunnerTerminal) Complete(_ context.Context, plan DurableRequestTerminalPlan) (DurableRequestTerminalResult, error) {
	terminal.plan = plan
	return DurableRequestTerminalResult{Revision: 1, Applied: 1}, nil
}

type distributedRunnerAuthority struct {
	value DurableRequestTerminalAuthority
}

func (authority distributedRunnerAuthority) TerminalAuthority(context.Context, DurableRequestTypedExecutionContext) (DurableRequestTerminalAuthority, error) {
	return authority.value, nil
}

type distributedRunnerResolver struct{ base ReplicatedRoute }

func (resolver distributedRunnerResolver) ResolveDurableRequestParticipant(_ context.Context, logical DurableRequestLogicalParticipant) (ReplicatedRoute, error) {
	route := cloneDurableRequestRoute(resolver.base)
	route.Distribution, route.Shard, route.Group = logical.Distribution, logical.Shard, logical.Group
	route.AllocationGeneration = 1
	route.Command.SchemaGeneration = logical.SchemaGeneration
	route.Command.RelationManifestDigest = replication.Digest(logical.RelationManifestDigest)
	return route, nil
}

func TestDurableRequestDistributedRunnerResumesProtocolCuts(t *testing.T) {
	for _, fault := range []distributedtxn.ReplicatedOperation{
		distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		distributedtxn.ReplicatedStagePrepareParticipant,
		distributedtxn.ReplicatedCommitCoordinator,
		distributedtxn.ReplicatedApplyReleaseParticipant,
		distributedtxn.ReplicatedRetireCoordinator,
	} {
		t.Run(fmt.Sprintf("operation_%d", fault), func(t *testing.T) {
			execution := typedExecutionFixture(t)
			execution.Recipe.Contract.CommitFinalWaveCount = 8
			execution.Recipe.Contract.AbortFinalWaveCount = 11
			commitCursor, abortCursor := []byte("terminal-commit"), []byte("terminal-abort")
			execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commitCursor))
			execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abortCursor))
			wave, head, route := lifecycleRunnerFixture(t)
			head.NextStepOrdinal, head.Revision = 0, 1
			ledger := &distributedRunnerLedger{head: head}
			waves := &distributedRunnerWaves{ledger: ledger, fault: fault}
			terminal := &distributedRunnerTerminal{}
			authority := DurableRequestTerminalAuthority{CommitCursor: commitCursor, AbortCursor: abortCursor, AckToken: requestledger.AckToken{1}, Release: terminalAuthorityRelease(t, execution)}
			runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, distributedRunnerPayloads{}, terminal, distributedRunnerAuthority{value: authority})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = runner.RunTyped(context.Background(), execution); !errors.Is(err, errLifecycleRunnerFault) {
				t.Fatalf("first run err=%v", err)
			}
			if err = execution.Participants.Reset(); err != nil {
				t.Fatal(err)
			}
			if _, err = runner.RunTyped(context.Background(), execution); err != nil {
				t.Fatal(err)
			}
			if !terminal.plan.AffectedRowsValid || terminal.plan.AffectedRows != 3 || terminal.plan.Outcome != requestledger.OutcomeCommitted || wave.Identity.ID == ([16]byte{}) {
				t.Fatalf("terminal=%+v", terminal.plan)
			}
		})
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
	head.NextStepOrdinal, head.Revision = 0, 1
	ledger := &distributedRunnerLedger{head: head}
	waves := &distributedRunnerWaves{ledger: ledger, prepareConflict: true}
	terminal := &distributedRunnerTerminal{}
	authority := DurableRequestTerminalAuthority{CommitCursor: commitCursor, AbortCursor: abortCursor, AckToken: requestledger.AckToken{1}, Release: terminalAuthorityRelease(t, execution)}
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

func TestDurableRequestDistributedRunnerStreamsMoreThan64Participants(t *testing.T) {
	execution := typedExecutionFixtureCount(t, 129)
	// One manifest command + (P-1) prepare + decision + P finish + retire.
	execution.Recipe.Contract.CommitFinalWaveCount = 260
	execution.Recipe.Contract.AbortFinalWaveCount = 389
	commitCursor, abortCursor := []byte("terminal-commit"), []byte("terminal-abort")
	execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commitCursor))
	execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abortCursor))
	_, head, route := lifecycleRunnerFixture(t)
	head.NextStepOrdinal, head.Revision = 0, 1
	ledger := &distributedRunnerLedger{head: head}
	waves := &distributedRunnerWaves{ledger: ledger}
	terminal := &distributedRunnerTerminal{}
	authority := DurableRequestTerminalAuthority{CommitCursor: commitCursor, AbortCursor: abortCursor, AckToken: requestledger.AckToken{1}, Release: terminalAuthorityRelease(t, execution)}
	runner, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves, distributedRunnerPayloads{}, terminal, distributedRunnerAuthority{value: authority})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunTyped(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if ledger.head.NextStepOrdinal != 260 || terminal.plan.AffectedRows != 129 ||
		execution.Participants.BufferedBytes() > durableRequestReaderMaxLiveBytes {
		t.Fatalf("waves=%d rows=%d buffered=%d", ledger.head.NextStepOrdinal, terminal.plan.AffectedRows, execution.Participants.BufferedBytes())
	}
}
