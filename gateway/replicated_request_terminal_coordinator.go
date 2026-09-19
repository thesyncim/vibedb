package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"

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
	Execution         DurableRequestTypedExecutionContext
	Home              DurableRequestLedgerHome
	Key               requestledger.RequestKey
	Outcome           requestledger.Outcome
	AffectedRows      int64
	AffectedRowsValid bool
	Result            []byte
	RetirementWitness requestledger.Digest
	AckToken          requestledger.AckToken
	Release           executionpin.Command
	Lease             executionpin.LeaseCertificate
}

type DurableRequestTerminalResult struct {
	Terminal requestledger.TerminalRecord
	Revision uint64
	Applied  uint64
}

type durableExecutionPinClient interface {
	ValidateFence(context.Context, ReplicatedRoute, executionpin.LeaseCertificate) error
}

type nativeDurableExecutionPinClient struct {
	executor  *ReplicatedExecutor
	principal serviceauthz.Authority
}

func (client nativeDurableExecutionPinClient) ValidateFence(ctx context.Context, route ReplicatedRoute, lease executionpin.LeaseCertificate) error {
	ctx, err := serviceauthz.WithAuthority(ctx, client.principal)
	if err != nil {
		return err
	}
	_, err = client.executor.ValidateExecutionPinFence(ctx, route, lease, lease.Applied)
	return err
}

// DurableRequestTerminalCoordinator seals a result and releases its co-located
// execution pin through the authenticated request ledger. It owns no native
// session or process-local retry state.
type DurableRequestTerminalCoordinator struct {
	ledger DurableRequestLedger
	pin    durableExecutionPinClient
}

func NewDurableRequestTerminalCoordinator(ledger DurableRequestLedger, executor *ReplicatedExecutor, principal serviceauthz.Authority) (*DurableRequestTerminalCoordinator, error) {
	if executor == nil || !principal.Valid() {
		return nil, ErrDurableRequest
	}
	return newDurableRequestTerminalCoordinator(ledger, nativeDurableExecutionPinClient{executor: executor, principal: principal})
}

func newDurableRequestTerminalCoordinator(ledger DurableRequestLedger, pin durableExecutionPinClient) (*DurableRequestTerminalCoordinator, error) {
	if ledger == nil || pin == nil {
		return nil, ErrDurableRequest
	}
	return &DurableRequestTerminalCoordinator{ledger: ledger, pin: pin}, nil
}

func (coordinator *DurableRequestTerminalCoordinator) Complete(
	ctx context.Context,
	plan DurableRequestTerminalPlan,
) (_ DurableRequestTerminalResult, failure error) {
	stage := "validate"
	defer func() {
		if failure != nil {
			failure = fmt.Errorf("gateway: terminal %s: %w", stage, failure)
		}
	}()
	if coordinator == nil || coordinator.ledger == nil || coordinator.pin == nil ||
		ctx == nil || !validDurableRequestTerminalPlan(plan) {
		return DurableRequestTerminalResult{}, ErrDurableRequest
	}
	stage = "read cut"
	head, continuation, prepared, release, terminal, applied, err :=
		coordinator.openTerminalRows(ctx, plan)
	if err != nil {
		return DurableRequestTerminalResult{}, err
	}
	if terminal.Revision != 0 {
		if terminal.Outcome != plan.Outcome || terminal.AffectedRows != plan.AffectedRows ||
			terminal.AffectedRowsValid != plan.AffectedRowsValid ||
			terminal.RetirementWitnessDigest != plan.RetirementWitness ||
			!bytes.Equal(terminal.Result, plan.Result) {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
		return DurableRequestTerminalResult{
			Terminal: terminal, Revision: head.Revision, Applied: applied,
		}, nil
	}
	if prepared.Revision == 0 || release.Phase == requestledger.SchemaPinReleaseInvalid {
		stage = "pin fence"
		if err = coordinator.pin.ValidateFence(ctx, plan.Execution.ExecutionPinRoute, plan.Lease); err != nil {
			return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
	}

	if prepared.Revision == 0 {
		stage = "prepare result"
		prepared, err = requestledger.NewPreparedTerminal(
			head, continuation, head.Revision+1, plan.Outcome,
			plan.AffectedRows, plan.AffectedRowsValid, plan.Result,
			plan.RetirementWitness, plan.AckToken,
		)
		if err != nil {
			return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		stage = "prepare CAS"
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
	stage = "prepared identity"
	if prepared.Outcome != plan.Outcome || prepared.AffectedRows != plan.AffectedRows ||
		prepared.AffectedRowsValid != plan.AffectedRowsValid ||
		prepared.RetirementWitnessDigest != plan.RetirementWitness ||
		!bytes.Equal(prepared.Result, plan.Result) {
		return DurableRequestTerminalResult{}, ErrDurableRequestConflict
	}

	if release.Phase == requestledger.SchemaPinReleaseInvalid {
		stage = "release proposal"
		transition := plan.Release
		transition.PrepareTerminalDigest = executionpin.Digest(prepared.PreparedDigest)
		exact, buildErr := executionpin.AppendCommand(nil, transition)
		if buildErr != nil {
			return DurableRequestTerminalResult{}, buildErr
		}
		intent, buildErr := requestledger.NewSchemaPinRelease(head, prepared, head.Revision+1, exact)
		if buildErr != nil {
			return DurableRequestTerminalResult{}, buildErr
		}
		stage = "atomic pin release"
		cas, applyErr := coordinator.ledger.ApplyCAS(ctx, plan.Home, plan.Key,
			DurableRequestLifecycleCAS{Operation: requestledger.OperationReleaseSchemaPin,
				ExpectedRevision: head.Revision, Revision: intent.Revision, SchemaPin: intent})
		if applyErr != nil {
			return DurableRequestTerminalResult{}, applyErr
		}
		if cas.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
		// The RF3 apply releases the co-located pin and seals its proof into
		// this ledger row. Reopen that committed cut; a local proposal payload
		// is not release evidence, and no old gateway identity is replayed.
		refreshPlan := plan
		refreshPlan.Execution.terminalCut = nil
		head, continuation, prepared, release, terminal, applied, err = coordinator.openTerminalRows(ctx, refreshPlan)
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
		if terminal.Revision != 0 {
			if terminal.Outcome != plan.Outcome || terminal.AffectedRows != plan.AffectedRows ||
				terminal.AffectedRowsValid != plan.AffectedRowsValid || terminal.RetirementWitnessDigest != plan.RetirementWitness ||
				!bytes.Equal(terminal.Result, plan.Result) {
				return DurableRequestTerminalResult{}, ErrDurableRequestConflict
			}
			return DurableRequestTerminalResult{Terminal: terminal, Revision: head.Revision, Applied: applied}, nil
		}
	}
	if release.Phase != requestledger.SchemaPinReleased {
		return DurableRequestTerminalResult{}, ErrDurableRequestConflict
	}

	stage = "terminal result"
	terminal, err = requestledger.NewTerminal(head, prepared, release, head.Revision+1)
	if err != nil {
		return DurableRequestTerminalResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	stage = "terminal CAS"
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
		plan.Release.PrepareTerminalDigest == (executionpin.Digest{}) &&
		plan.Lease.Valid() && plan.Lease.PinID == plan.Release.PinID &&
		plan.Lease.AcquireCertificateDigest == plan.Release.AcquireCertificateDigest &&
		plan.Lease.Controller == plan.Release.ExpectedController &&
		plan.Lease.ControllerEpoch == plan.Release.ExpectedControllerEpoch &&
		plan.Lease.LeaseAppliedThrough == plan.Release.ExpectedLeaseAppliedThrough &&
		plan.Lease.Revision == plan.Release.ExpectedLeaseRevision
}

func (coordinator *DurableRequestTerminalCoordinator) openTerminalRows(
	ctx context.Context,
	plan DurableRequestTerminalPlan,
) (requestledger.HeadRecord, requestledger.ContinuationRecord,
	requestledger.PreparedTerminalRecord, requestledger.SchemaPinReleaseRecord,
	requestledger.TerminalRecord, uint64, error,
) {
	if cut := plan.Execution.terminalCut; cut != nil {
		err := validateDurableRequestPreparedCut(plan.Execution, *cut)
		if err == nil && cut.SchemaPin.Revision != 0 {
			_, err = durableRequestTerminalReleaseCommand(plan.Execution, *cut)
		}
		if err != nil {
			return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
				requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
				requestledger.TerminalRecord{}, 0, err
		}
		return cut.Head, cut.Continuation, cut.Prepared, cut.SchemaPin, cut.Terminal, cut.Applied, nil
	}
	if reader, ok := coordinator.ledger.(durableRequestTerminalCutReader); ok {
		cut, err := reader.ReadTerminalCut(ctx, plan.Home, plan.Key)
		if err != nil {
			return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
				requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
				requestledger.TerminalRecord{}, 0, err
		}
		if cut.Terminal.Revision == 0 && cut.Continuation.Revision == 0 {
			return requestledger.HeadRecord{}, requestledger.ContinuationRecord{},
				requestledger.PreparedTerminalRecord{}, requestledger.SchemaPinReleaseRecord{},
				requestledger.TerminalRecord{}, 0, ErrDurableRequestConflict
		}
		return cut.Head, cut.Continuation, cut.Prepared, cut.SchemaPin,
			cut.Terminal, cut.Applied, nil
	}
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
