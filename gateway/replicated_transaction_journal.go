package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

// ReplicatedTransactionJournalCommand is one exact outer command in a durable
// execution wave. Route and Command are borrowed for StageWave only.
type ReplicatedTransactionJournalCommand struct {
	Route   ReplicatedRoute
	Ordinal uint32
	Command []byte
}

// ReplicatedTransactionJournalWave is one complete canonical network wave.
// StageWave must make every command recoverable atomically before it returns;
// the orchestrator sends no command from a wave whose staging failed.
type ReplicatedTransactionJournalWave struct {
	Identity ReplicatedTransactionIdentity
	Phase    ReplicatedTransactionPhase
	Commands []ReplicatedTransactionJournalCommand
}

// ReplicatedTransactionJournalAdvance settles exactly one staged command only
// after its native completion was authenticated and decoded.
type ReplicatedTransactionJournalAdvance struct {
	Identity     ReplicatedTransactionIdentity
	Phase        ReplicatedTransactionPhase
	Ordinal      uint32
	Command      []byte
	ResultCode   uint32
	AppliedIndex uint64
}

// ReplicatedTransactionJournal has no proposal authority. It persists exact
// retry material and monotone settlement evidence for the request ledger.
type ReplicatedTransactionJournal interface {
	StageWave(context.Context, ReplicatedTransactionJournalWave) error
	Advance(context.Context, ReplicatedTransactionJournalAdvance) error
}

type replicatedTransactionJournalCommandPlan struct {
	route   ReplicatedRoute
	ordinal uint32
	control distributedtxn.ReplicatedCommand
	batches []replication.RelationMutationBatch
}

func (orchestrator *ReplicatedTransactionOrchestrator) appendExactTransactionCommand(
	dst []byte,
	retryHome replication.RetryHome,
	route ReplicatedRoute,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
) ([]byte, error) {
	controlSize, err := distributedtxn.ReplicatedCommandSize(control)
	if err != nil {
		return dst, err
	}
	controlBytes := make([]byte, 0, controlSize)
	controlBytes, err = distributedtxn.AppendReplicatedCommand(controlBytes, control)
	if err != nil {
		return dst, err
	}
	sequence, err := replication.TransactionClientSequence(controlBytes)
	if err != nil {
		return dst, err
	}
	command := replicatedTransactionCommandHeader(
		route, orchestrator.tenant, retryHome, replication.ID128(control.ID),
		uint64(control.Role), sequence,
	)
	command.Kind = replication.CommandTransaction
	command.Transaction = controlBytes
	command.Batches = batches
	command.Fingerprint = nativeCommandFingerprint(command)
	commandSize, err := replication.CommandSize(command)
	if err != nil {
		return dst, err
	}
	if cap(dst)-len(dst) < commandSize {
		dst = make([]byte, 0, commandSize)
	}
	start := len(dst)
	dst, err = replication.AppendCommand(dst, command)
	if err != nil || len(dst)-start != commandSize {
		return dst[:start], errors.Join(err, ErrReplicatedTransaction)
	}
	return dst, nil
}

func (orchestrator *ReplicatedTransactionOrchestrator) stageTransactionWave(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	phase ReplicatedTransactionPhase,
	plans []replicatedTransactionJournalCommandPlan,
) ([][]byte, *replicatedTransactionPendingReservation, error) {
	if handle == nil || handle.journal == nil || len(plans) == 0 {
		return nil, nil, ErrReplicatedTransaction
	}
	exact := make([][]byte, len(plans))
	wave := ReplicatedTransactionJournalWave{
		Identity: replicatedTransactionHandleIdentity(handle), Phase: phase,
		Commands: make([]ReplicatedTransactionJournalCommand, len(plans)),
	}
	var stagedBytes uint64
	for index := range plans {
		plan := &plans[index]
		if index != 0 && plan.ordinal <= plans[index-1].ordinal {
			return nil, nil, ErrReplicatedTransaction
		}
		encoded, err := orchestrator.appendExactTransactionCommand(
			nil, handle.RetryHome, plan.route, plan.control, plan.batches,
		)
		if err != nil {
			return nil, nil, err
		}
		exact[index] = encoded
		wave.Commands[index] = ReplicatedTransactionJournalCommand{
			Route: plan.route, Ordinal: plan.ordinal, Command: encoded,
		}
		stagedBytes, err = checkedReplicatedTransactionLogicalSum(
			stagedBytes, uint64(cap(encoded)),
		)
		if err != nil {
			return nil, nil, err
		}
	}
	if stagedBytes == 0 || !orchestrator.byteBudget.tryAcquire(stagedBytes) {
		return nil, nil, ErrReplicatedTransactionBound
	}
	reservation := newReplicatedTransactionPendingReservation(
		&orchestrator.byteBudget, stagedBytes,
	)
	if err := handle.journal.StageWave(ctx, wave); err != nil {
		reservation.release()
		return nil, nil, err
	}
	return exact, reservation, nil
}

func (orchestrator *ReplicatedTransactionOrchestrator) stageTransactionSingleton(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	phase ReplicatedTransactionPhase,
	route ReplicatedRoute,
	ordinal uint32,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
) ([]byte, *replicatedTransactionPendingReservation, error) {
	if handle == nil || handle.journal == nil {
		return nil, nil, ErrReplicatedTransaction
	}
	exact, reservation, err := orchestrator.stageTransactionWave(ctx, handle, phase,
		[]replicatedTransactionJournalCommandPlan{{
			route: route, ordinal: ordinal, control: control, batches: batches,
		}})
	if err != nil {
		return nil, nil, err
	}
	return exact[0], reservation, nil
}

func replicatedTransactionHandleIdentity(
	handle *ReplicatedTransactionRecoveryHandle,
) ReplicatedTransactionIdentity {
	if handle == nil {
		return ReplicatedTransactionIdentity{}
	}
	return ReplicatedTransactionIdentity{
		ID: handle.ID, RetryHome: handle.RetryHome,
		CatalogGeneration:  handle.CatalogGeneration,
		RecoveryDeadline:   handle.RecoveryDeadline,
		CoordinatorOrdinal: handle.CoordinatorOrdinal,
	}
}
