package hotshard

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

// SplitPlanFactory owns the topology allocator needed to turn an admitted
// source recommendation into exact child allocation, WAL, SQL, and RF3
// identities. Repeating one Admission ID must return the same immutable plan.
type SplitPlanFactory interface {
	BuildHotSplitPlan(context.Context, *gateway.Snapshot, [32]byte, SplitWork) (*splitcontroller.Plan, error)
}

// MovePlanFactory binds a selected source/target endpoint to exact current
// publication, retiring member, donor, learner member, store, and incarnation.
type MovePlanFactory interface {
	BuildHotReplicaMove(context.Context, *gateway.Snapshot, [32]byte, MoveWork) (*rebalance.Plan, error)
}

type MoveSubmitter interface {
	Submit(context.Context, *rebalance.Plan) (rebalance.Action, error)
}

// OperationSink adapts one selected pressure cut to the already-shipped split
// and replica-move journals. The catalog's current global generation is one
// serial publication point, so this adapter admits exactly one operation and
// requires the controller's conservative default batch policy.
type OperationSink struct {
	Catalog *gateway.Snapshot
	Splits  SplitPlanFactory
	Journal splitcontroller.ReplicatedOperationJournal
	Moves   MovePlanFactory
	MoveRun MoveSubmitter
}

func (sink OperationSink) SubmitHotShardAdmission(
	ctx context.Context, admission Admission,
) error {
	if ctx == nil || sink.Catalog == nil || admission.ID == ([32]byte{}) ||
		admission.CatalogGeneration != sink.Catalog.Generation() ||
		int(admission.SplitCount)+int(admission.MoveCount) != 1 {
		return ErrInvalidPressureCut
	}
	if admission.SplitCount == 1 {
		if sink.Splits == nil || sink.Journal == nil {
			return ErrInvalidPressureCut
		}
		plan, err := sink.Splits.BuildHotSplitPlan(
			ctx, sink.Catalog, admission.ID, admission.Splits[0],
		)
		if err != nil || plan == nil {
			return errors.Join(err, ErrInvalidPressureCut)
		}
		_, err = splitcontroller.AdmitReplicatedPlan(ctx, sink.Journal, sink.Catalog, plan)
		return err
	}
	if sink.Moves == nil || sink.MoveRun == nil {
		return ErrInvalidPressureCut
	}
	plan, err := sink.Moves.BuildHotReplicaMove(
		ctx, sink.Catalog, admission.ID, admission.Moves[0],
	)
	if err != nil || plan == nil {
		return errors.Join(err, ErrInvalidPressureCut)
	}
	_, err = sink.MoveRun.Submit(ctx, plan)
	return err
}
