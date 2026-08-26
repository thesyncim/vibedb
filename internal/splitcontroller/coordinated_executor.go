package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/shardcontrol"
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
				return nil
			default:
				if remoteActionTargetsChild(action.Kind) {
					target, ok := plan.Target(action.Child)
					if !ok || len(target.Replicas) != gateway.ServingReplicaCount {
						return ErrRemoteExecution
					}
					for _, replica := range target.Replicas {
						destination, targetErr := remoteActionTargetForChildReplica(plan, action.Child, replica)
						if targetErr != nil {
							return targetErr
						}
						request, requestErr := appendRemoteStepRequestForTarget(
							nil, plan, observed, action, destination,
						)
						if requestErr != nil {
							return errors.Join(ErrRemoteExecution, requestErr)
						}
						response, requestErr := router.ExecuteShardControl(ctx, action, request)
						if requestErr != nil {
							return requestErr
						}
						if response.Code != shardcontrol.ResultAccepted || response.Operation != request.Operation ||
							response.Step != request.Step {
							return ErrRemoteExecution
						}
					}
					return nil
				}
				request, err := appendRemoteStepRequest(nil, plan, observed, action)
				if err != nil {
					return errors.Join(ErrRemoteExecution, err)
				}
				response, err := router.ExecuteShardControl(ctx, action, request)
				if err != nil {
					return err
				}
				if response.Code != shardcontrol.ResultAccepted || response.Operation != request.Operation ||
					response.Step != request.Step {
					return ErrRemoteExecution
				}
				return nil
			}
		})
}
