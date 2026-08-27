package splitcontroller

import (
	"context"
	"errors"
	"sync"

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
				return gatewayExecutor.ExecuteGatewaySplitAction(ctx, plan, observed, action)
			default:
				if remoteActionTargetsChild(action.Kind) {
					target, ok := plan.Target(action.Child)
					if !ok || len(target.Replicas) != gateway.ServingReplicaCount {
						return ErrRemoteExecution
					}
					type childRequest struct {
						request shardcontrol.Request
						err     error
					}
					requests := make([]childRequest, len(target.Replicas))
					for index, replica := range target.Replicas {
						destination, targetErr := remoteActionTargetForChildReplica(plan, action.Child, replica)
						if targetErr != nil {
							return targetErr
						}
						requests[index].request, requests[index].err = appendRemoteStepRequestForTarget(
							nil, plan, observed, action, destination,
						)
						if requests[index].err != nil {
							return errors.Join(ErrRemoteExecution, requests[index].err)
						}
					}
					var group sync.WaitGroup
					group.Add(len(requests))
					for index := range requests {
						go func(index int) {
							defer group.Done()
							request := requests[index].request
							response, requestErr := router.ExecuteShardControl(ctx, action, request)
							if requestErr != nil {
								requests[index].err = requestErr
								return
							}
							if response.Code != shardcontrol.ResultAccepted || response.Operation != request.Operation ||
								response.Step != request.Step {
								requests[index].err = ErrRemoteExecution
							}
						}(index)
					}
					group.Wait()
					var joined error
					for index := range requests {
						joined = errors.Join(joined, requests[index].err)
					}
					if joined != nil {
						return joined
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
