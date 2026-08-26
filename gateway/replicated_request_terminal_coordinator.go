package gateway

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// DurableRequestTerminalPlan contains the final branch result and the exact
// execution-pin lease authority acquired at planning time. Release is a
// template: PrepareTerminalDigest must be zero and is filled only after the
// prepared result has become durable.
type DurableRequestTerminalPlan struct {
	Home              DurableRequestLedgerHome
	Key               requestledger.RequestKey
	Outcome           requestledger.Outcome
	AffectedRows      int64
	AffectedRowsValid bool
	Result            []byte
	RetirementWitness requestledger.Digest
	AckToken          requestledger.AckToken
	Release           executionpin.Command
}

type DurableRequestTerminalResult struct {
	Terminal requestledger.TerminalRecord
	Revision uint64
	Applied  uint64
}

type durableExecutionPinClient interface {
	BuildRelease(executionpin.Command) ([]byte, error)
	ProposeNew(context.Context, executionpin.Command, []byte) (ReplicatedResult, error)
	RetryExact(context.Context, []byte) (ReplicatedResult, error)
}

type nativeDurableExecutionPinClient struct {
	session  *NativeSession
	executor *ReplicatedExecutor
}

func newNativeDurableExecutionPinClient(
	session *NativeSession,
	executor *ReplicatedExecutor,
) (*nativeDurableExecutionPinClient, error) {
	if session == nil || executor == nil || session.executor != executor ||
		session.proposalCapability != serviceauthz.CapabilityExecutionPin {
		return nil, ErrDurableRequest
	}
	return &nativeDurableExecutionPinClient{session: session, executor: executor}, nil
}

// BuildRelease creates the byte-identical command which ExecutionPin will
// submit, without changing session sequence or pending state. This permits the
// request ledger to own the exact bytes before any network admission.
func (client *nativeDurableExecutionPinClient) BuildRelease(
	transition executionpin.Command,
) ([]byte, error) {
	session := client.session
	if client == nil || session == nil || session.phase != nativeSessionActive ||
		session.pending || session.nextSequence == 0 ||
		session.nextSequence == math.MaxUint64 || !transition.Valid() ||
		transition.Operation != executionpin.OperationRelease {
		return nil, ErrDurableRequest
	}
	var nestedStorage [executionpin.CommandBytes]byte
	nested, err := executionpin.AppendCommand(nestedStorage[:0], transition)
	if err != nil {
		return nil, err
	}
	command := session.commandHeader(
		replication.CommandExecutionPin, session.epoch,
		session.nextSequence, session.ackThrough,
	)
	command.ExecutionPin = nested
	command.Fingerprint = nativeCommandFingerprint(command)
	return replication.AppendCommand(nil, command)
}

func (client *nativeDurableExecutionPinClient) ProposeNew(
	ctx context.Context,
	transition executionpin.Command,
	exact []byte,
) (ReplicatedResult, error) {
	if client == nil || client.session == nil || ctx == nil {
		return ReplicatedResult{}, ErrDurableRequest
	}
	built, err := client.BuildRelease(transition)
	if err != nil || !bytes.Equal(built, exact) {
		return ReplicatedResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	result, err := client.session.ExecutionPin(ctx, transition)
	if err != nil {
		return ReplicatedResult{}, err
	}
	return ReplicatedResult{
		Outcome: result.Outcome, Completion: bytes.Clone(result.Completion.Bytes()),
	}, nil
}

func (client *nativeDurableExecutionPinClient) RetryExact(
	ctx context.Context,
	exact []byte,
) (ReplicatedResult, error) {
	if client == nil || client.executor == nil || client.session == nil || ctx == nil ||
		!commandMatchesRoute(exact, client.session.route) {
		return ReplicatedResult{}, ErrDurableRequestConflict
	}
	if client.session.pending {
		if !bytes.Equal(client.session.command, exact) {
			return ReplicatedResult{}, ErrDurableRequestConflict
		}
		result, err := client.session.RetryPending(ctx)
		if err != nil {
			return ReplicatedResult{}, err
		}
		return ReplicatedResult{
			Outcome: result.Outcome, Completion: bytes.Clone(result.Completion.Bytes()),
		}, nil
	}
	return client.executor.propose(
		ctx, client.session.route, exact, nil, true,
		serviceauthz.CapabilityExecutionPin, replicatedUnknownCommandClone,
	)
}

// DurableRequestTerminalCoordinator publishes a terminal result only after
// its complete client result is durable and the catalog execution pin has an
// authenticated release certificate.
type DurableRequestTerminalCoordinator struct {
	ledger DurableRequestLedger
	pin    durableExecutionPinClient
}

func NewDurableRequestTerminalCoordinator(
	ledger DurableRequestLedger,
	session *NativeSession,
	executor *ReplicatedExecutor,
) (*DurableRequestTerminalCoordinator, error) {
	pin, err := newNativeDurableExecutionPinClient(session, executor)
	if err != nil || ledger == nil {
		return nil, errors.Join(err, ErrDurableRequest)
	}
	return &DurableRequestTerminalCoordinator{ledger: ledger, pin: pin}, nil
}

func newDurableRequestTerminalCoordinator(
	ledger DurableRequestLedger,
	pin durableExecutionPinClient,
) (*DurableRequestTerminalCoordinator, error) {
	if ledger == nil || pin == nil {
		return nil, ErrDurableRequest
	}
	return &DurableRequestTerminalCoordinator{ledger: ledger, pin: pin}, nil
}

func (coordinator *DurableRequestTerminalCoordinator) Complete(
	ctx context.Context,
	plan DurableRequestTerminalPlan,
) (DurableRequestTerminalResult, error) {
	if coordinator == nil || coordinator.ledger == nil || coordinator.pin == nil ||
		ctx == nil || !validDurableRequestTerminalPlan(plan) {
		return DurableRequestTerminalResult{}, ErrDurableRequest
	}
	head, continuation, prepared, release, terminal, applied, err :=
		coordinator.openTerminalRows(ctx, plan)
	if err != nil {
		return DurableRequestTerminalResult{}, err
	}
	if terminal.Revision != 0 {
		if terminal.Outcome != plan.Outcome || terminal.AffectedRows != plan.AffectedRows ||
			terminal.AffectedRowsValid != plan.AffectedRowsValid ||
			terminal.RetirementWitnessDigest != plan.RetirementWitness ||
			terminal.AckToken != plan.AckToken || !bytes.Equal(terminal.Result, plan.Result) {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
		return DurableRequestTerminalResult{
			Terminal: terminal, Revision: head.Revision, Applied: applied,
		}, nil
	}

	if prepared.Revision == 0 {
		prepared, err = requestledger.NewPreparedTerminal(
			head, continuation, head.Revision+1, plan.Outcome,
			plan.AffectedRows, plan.AffectedRowsValid, plan.Result,
			plan.RetirementWitness, plan.AckToken,
		)
		if err != nil {
			return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		cas, applyErr := coordinator.ledger.ApplyCAS(ctx, plan.Home, plan.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationPrepareTerminal,
				ExpectedRevision: head.Revision, Revision: prepared.Revision,
				Prepared: prepared,
			})
		if applyErr != nil {
			return DurableRequestTerminalResult{}, applyErr
		}
		if cas.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
		applied = cas.Applied
		head, err = requestledger.MarkTerminalPrepared(head, continuation, prepared)
		if err != nil {
			return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
	}
	if prepared.Outcome != plan.Outcome || prepared.AffectedRows != plan.AffectedRows ||
		prepared.AffectedRowsValid != plan.AffectedRowsValid ||
		prepared.RetirementWitnessDigest != plan.RetirementWitness ||
		prepared.AckToken != plan.AckToken || !bytes.Equal(prepared.Result, plan.Result) {
		return DurableRequestTerminalResult{}, ErrDurableRequestConflict
	}

	createdRelease := false
	transition := plan.Release
	transition.PrepareTerminalDigest = executionpin.Digest(prepared.PreparedDigest)
	if !transition.Valid() || transition.Operation != executionpin.OperationRelease {
		return DurableRequestTerminalResult{}, ErrDurableRequestConflict
	}
	if release.Phase != requestledger.SchemaPinReleaseInvalid {
		outer, openErr := replication.OpenCommand(release.Command)
		persisted, nestedErr := outer.OpenExecutionPin()
		if openErr != nil || nestedErr != nil || persisted != transition {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
	}
	if release.Phase == requestledger.SchemaPinReleaseInvalid {
		exact, buildErr := coordinator.pin.BuildRelease(transition)
		if buildErr != nil {
			return DurableRequestTerminalResult{}, buildErr
		}
		release, err = requestledger.NewSchemaPinRelease(
			head, prepared, head.Revision+1, exact,
		)
		if err != nil {
			return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		cas, applyErr := coordinator.ledger.ApplyCAS(ctx, plan.Home, plan.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationBeginSchemaPinRelease,
				ExpectedRevision: head.Revision, Revision: release.Revision,
				SchemaPin: release,
			})
		if applyErr != nil {
			return DurableRequestTerminalResult{}, applyErr
		}
		if cas.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
		applied = cas.Applied
		head, err = requestledger.InstallSchemaPinRelease(head, prepared, release)
		if err != nil {
			return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		createdRelease = true
	}

	if release.Phase == requestledger.SchemaPinReleasing {
		var settled ReplicatedResult
		if createdRelease {
			settled, err = coordinator.pin.ProposeNew(ctx, transition, release.Command)
		} else {
			settled, err = coordinator.pin.RetryExact(ctx, release.Command)
		}
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
		if !validDurableRequestSettlement(release.Command, settled) {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
		next, recordErr := requestledger.RecordVerifiedSchemaPinReleased(
			release, release.Revision+1, settled.Completion,
		)
		if recordErr != nil {
			return DurableRequestTerminalResult{}, errors.Join(recordErr, ErrDurableRequestConflict)
		}
		cas, applyErr := coordinator.ledger.ApplyCAS(ctx, plan.Home, plan.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationRecordSchemaPinReleased,
				ExpectedRevision: head.Revision, Revision: next.Revision,
				SchemaPin: next,
			})
		if applyErr != nil {
			return DurableRequestTerminalResult{}, applyErr
		}
		if cas.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
		applied = cas.Applied
		head, err = requestledger.MarkSchemaPinReleased(head, prepared, release, next)
		if err != nil {
			return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		release = next
	}

	terminal, err = requestledger.NewTerminal(head, prepared, release, head.Revision+1)
	if err != nil {
		return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	cas, err := coordinator.ledger.ApplyCAS(ctx, plan.Home, plan.Key,
		DurableRequestLifecycleCAS{
			Operation:        requestledger.OperationComplete,
			ExpectedRevision: head.Revision, Revision: terminal.Revision,
			Terminal: terminal,
		})
	if err != nil {
		return DurableRequestTerminalResult{}, err
	}
	if cas.Ledger.ResultCode != replicatedstate.ResultApplied {
		return DurableRequestTerminalResult{}, ErrDurableRequestConflict
	}
	return DurableRequestTerminalResult{
		Terminal: terminal, Revision: terminal.Revision, Applied: cas.Applied,
	}, nil
}

func validDurableRequestTerminalPlan(plan DurableRequestTerminalPlan) bool {
	return plan.Key.Valid() && plan.Home.Identity != (replication.Digest{}) &&
		plan.Outcome.Valid() && plan.AffectedRows >= 0 &&
		(plan.Outcome == requestledger.OutcomeCommitted) == plan.AffectedRowsValid &&
		(plan.Outcome != requestledger.OutcomeAborted || plan.AffectedRows == 0) &&
		len(plan.Result) <= requestledger.MaxPreparedTerminalResultBytes &&
		plan.RetirementWitness != (requestledger.Digest{}) &&
		plan.AckToken != (requestledger.AckToken{}) &&
		plan.Release.Operation == executionpin.OperationRelease &&
		plan.Release.PrepareTerminalDigest == (executionpin.Digest{})
}

func (coordinator *DurableRequestTerminalCoordinator) openTerminalRows(
	ctx context.Context,
	plan DurableRequestTerminalPlan,
) (requestledger.HeadRecord, requestledger.ContinuationRecord,
	requestledger.PreparedTerminalRecord, requestledger.SchemaPinReleaseRecord,
	requestledger.TerminalRecord, uint64, error,
) {
	headRow, err := coordinator.ledger.ReadRow(ctx, plan.Home, DurableRequestLifecycleRead{
		Key: plan.Key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 1,
	})
	if err != nil || !headRow.Found || headRow.Kind != replicatedstate.RequestLedgerReadHead {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
			requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
			requestledger.TerminalRecord{}, 0, errors.Join(err, ErrDurableRequestConflict)
	}
	read := func(kind replicatedstate.RequestLedgerReadKind) (DurableRequestLifecycleRow, error) {
		return coordinator.ledger.ReadRow(ctx, plan.Home, DurableRequestLifecycleRead{
			Key: plan.Key, Kind: kind, MinimumApplied: headRow.Applied,
		})
	}
	terminalRow, err := read(replicatedstate.RequestLedgerReadTerminal)
	if err != nil {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
			requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
			requestledger.TerminalRecord{}, 0, err
	}
	if terminalRow.Found {
		if terminalRow.Kind == replicatedstate.RequestLedgerReadAck {
			return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
				requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
				requestledger.TerminalRecord{}, 0, ErrDurableRequestAcknowledged
		}
		if terminalRow.Kind != replicatedstate.RequestLedgerReadTerminal {
			return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
				requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
				requestledger.TerminalRecord{}, 0, ErrDurableRequestConflict
		}
		return headRow.Head, requestledger.ContinuationRecord{},
			requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
			terminalRow.Terminal, terminalRow.Applied, nil
	}
	continuationRow, err := read(replicatedstate.RequestLedgerReadContinuation)
	if err != nil || !continuationRow.Found ||
		continuationRow.Kind != replicatedstate.RequestLedgerReadContinuation {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
			requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
			requestledger.TerminalRecord{}, 0, errors.Join(err, ErrDurableRequestConflict)
	}
	preparedRow, err := read(replicatedstate.RequestLedgerReadPrepared)
	if err != nil {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
			requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
			requestledger.TerminalRecord{}, 0, err
	}
	schemaRow, err := read(replicatedstate.RequestLedgerReadSchemaPin)
	if err != nil {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
			requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
			requestledger.TerminalRecord{}, 0, err
	}
	if preparedRow.Found && preparedRow.Kind != replicatedstate.RequestLedgerReadPrepared ||
		schemaRow.Found && schemaRow.Kind != replicatedstate.RequestLedgerReadSchemaPin {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
			requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
			requestledger.TerminalRecord{}, 0, ErrDurableRequestConflict
	}
	return headRow.Head, continuationRow.Continuation,
		preparedRow.Prepared, schemaRow.SchemaPin, requestledger.TerminalRecord{},
		headRow.Applied, nil
}
