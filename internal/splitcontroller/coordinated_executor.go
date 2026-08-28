package splitcontroller

import (
	"context"

	"github.com/thesyncim/vibedb/gateway"
)

type PlanAdmissionCoordinator interface {
	AdmitPlan(context.Context, *gateway.Snapshot, *Plan, PlanAdmission) error
}

type GatewaySplitActionExecutor interface {
	ExecuteGatewaySplitAction(context.Context, *Plan, Observation, Action) error
}

// ExecuteCoordinatedReplicatedStep keeps the authority split explicit. Pure
// waits settle without a network hop, catalog publication executes only in the
// gateway, and shard RPCs carry only fenced data-plane mutations.
func ExecuteCoordinatedReplicatedStep(
	ctx context.Context,
	journal ReplicatedOperationJournal,
	plan *Plan,
	observed Observation,
	router ShardControlRouter,
	gatewayExecutor GatewaySplitActionExecutor,
) (Action, error) {
	if router == nil || gatewayExecutor == nil {
		return Action{}, ErrRemoteExecution
	}
	return ExecuteReplicatedStep(ctx, journal, plan, observed,
		func(ctx context.Context, operation OperationID, action Action) error {
			if operation != plan.OperationID() {
				return ErrReplicatedExecution
			}
			switch action.Kind {
			case ActionAwaitSourceLeader, ActionAwaitChildReady, ActionAwaitCatalogDrain:
				return nil
			case ActionPublishCatalog:
				return gatewayExecutor.ExecuteGatewaySplitAction(ctx, plan, observed, action)
			case ActionComplete:
				return gatewayExecutor.ExecuteGatewaySplitAction(ctx, plan, observed, action)
			default:
				return executeRemoteActionWave(ctx, journal, plan, observed, action, router, true)
			}
		})
}
