package rebalanceexec

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rebalance"
)

var ErrControllerConfig = errors.New("rebalanceexec: invalid replica move controller configuration")

// MoveDirectory is the bounded replicated catalog work directory. It contains
// multiple operation kinds; Controller ignores records it does not own.
type MoveDirectory interface {
	ReadOperationIDs(context.Context) ([][32]byte, error)
	ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error)
}

// Controller composes durable move discovery with the exact one-step
// reconciler. It intentionally has no process-local queue or progress cursor:
// every restart rediscovers work and resumes from the replicated record.
type Controller struct {
	directory         MoveDirectory
	journal           rebalance.ReplicatedOperationJournal
	observer          rebalance.ReplicatedMoveObserver
	executor          rebalance.ReplicatedMoveActionExecutor
	abandonment       *AbandonmentScheduler
	abandonmentCursor AbandonmentSchedulerCursor
}

func (controller *Controller) InstallAbandonmentScheduler(scheduler *AbandonmentScheduler) bool {
	if controller == nil || scheduler == nil || controller.abandonment != nil {
		return false
	}
	controller.abandonment = scheduler
	return true
}

func NewController(
	directory MoveDirectory,
	journal rebalance.ReplicatedOperationJournal,
	observer rebalance.ReplicatedMoveObserver,
	executor rebalance.ReplicatedMoveActionExecutor,
) (*Controller, error) {
	if directory == nil || journal == nil || observer == nil || executor == nil {
		return nil, ErrControllerConfig
	}
	return &Controller{
		directory: directory, journal: journal, observer: observer, executor: executor,
	}, nil
}

// Submit executes the first journaled step for a newly planned move. The plan
// is used only if its operation record is absent; subsequent calls recover the
// immutable intent from the replicated journal.
func (controller *Controller) Submit(
	ctx context.Context, plan *rebalance.Plan,
) (rebalance.Action, error) {
	if controller == nil || ctx == nil || plan == nil ||
		plan.OperationID() == (rebalance.OperationID{}) {
		return rebalance.Action{}, ErrControllerConfig
	}
	return rebalance.ExecuteReplicatedMoveStep(
		ctx, plan.OperationID(), plan, controller.journal, controller.observer,
		controller.executor,
	)
}

// Resume executes one journaled step for an already submitted move.
func (controller *Controller) Resume(
	ctx context.Context, operation rebalance.OperationID,
) (rebalance.Action, error) {
	if controller == nil || ctx == nil || operation == (rebalance.OperationID{}) {
		return rebalance.Action{}, ErrControllerConfig
	}
	return rebalance.ExecuteReplicatedMoveStep(
		ctx, operation, nil, controller.journal, controller.observer,
		controller.executor,
	)
}

type ControllerPass struct {
	Discovered           uint32
	Moves                uint32
	Advanced             uint32
	Completed            uint32
	AbandonmentScanned   uint32
	AbandonmentWitnessed uint32
	AbandonmentDeleted   uint32
	AbandonmentBytes     uint64
}

// RunPass discovers the complete catalog-bounded work directory and advances
// every move by at most one durable step. A failed move does not starve an
// unrelated shard: errors are joined after all discoverable records have had
// their turn. Catalog CAS fences still serialize conflicting topology changes.
func (controller *Controller) RunPass(ctx context.Context) (ControllerPass, error) {
	if controller == nil || ctx == nil {
		return ControllerPass{}, ErrControllerConfig
	}
	ids, err := controller.directory.ReadOperationIDs(ctx)
	if err != nil {
		return ControllerPass{}, err
	}
	pass := ControllerPass{Discovered: uint32(len(ids))}
	var failures error
	if controller.abandonment != nil {
		abandoned, abandonErr := controller.abandonment.RunPass(ctx, controller.abandonmentCursor)
		controller.abandonmentCursor = abandoned.Cursor
		if abandoned.Done {
			controller.abandonmentCursor = AbandonmentSchedulerCursor{}
		}
		pass.AbandonmentScanned, pass.AbandonmentWitnessed, pass.AbandonmentDeleted =
			abandoned.Scanned, abandoned.Witnessed, abandoned.Deleted
		pass.AbandonmentBytes = abandoned.ScheduledBytes
		failures = errors.Join(failures, abandonErr)
	}
	for index := range ids {
		if err = ctx.Err(); err != nil {
			return pass, errors.Join(failures, err)
		}
		record, readErr := controller.directory.ReadOperation(ctx, ids[index])
		if errors.Is(readErr, gateway.ErrReplicatedOperationMissing) {
			continue
		}
		if readErr != nil {
			failures = errors.Join(failures, readErr)
			continue
		}
		if record.Kind != gateway.ReplicatedOperationMove {
			continue
		}
		if record.State == gateway.ReplicatedOperationCancelled {
			continue
		}
		pass.Moves++
		action, stepErr := controller.Resume(ctx, rebalance.OperationID(record.ID))
		if stepErr != nil {
			failures = errors.Join(failures, stepErr)
			continue
		}
		pass.Advanced++
		if action.Kind == rebalance.ActionComplete {
			pass.Completed++
		}
	}
	return pass, failures
}
