package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/shardcontrol"
)

type ControllerDirectory interface {
	Read(context.Context) (*gateway.Snapshot, error)
	ReadOperationIDs(context.Context) ([][32]byte, error)
	ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error)
}

type ControllerTriggerClient interface {
	TriggerSplitController(
		context.Context, gateway.ReplicatedRoute, shardcontrol.Request,
	) (shardcontrol.Response, error)
}

type ControllerPass struct {
	Discovered uint16
	Triggered  uint16
	Completed  uint16
}

// RunDirectControllerPass is the production authority composition. The
// gateway reads the bounded operation directory and executes each operation
// locally; it never asks a shard to host catalog/controller authority.
func RunDirectControllerPass(
	ctx context.Context,
	directory ControllerDirectory,
	controller *ControllerService,
) (ControllerPass, error) {
	if ctx == nil || directory == nil || controller == nil {
		return ControllerPass{}, ErrControllerTrigger
	}
	ids, err := directory.ReadOperationIDs(ctx)
	if err != nil || len(ids) > maxControllerPassOperations {
		return ControllerPass{}, errors.Join(err, ErrControllerTrigger)
	}
	pass := ControllerPass{Discovered: uint16(len(ids))}
	for _, id := range ids {
		record, readErr := directory.ReadOperation(ctx, id)
		if errors.Is(readErr, gateway.ErrReplicatedOperationMissing) {
			continue
		}
		if readErr != nil || record.ID != id || record.Kind != gateway.ReplicatedOperationSplit {
			return pass, errors.Join(readErr, ErrControllerTrigger)
		}
		action, executeErr := controller.ExecuteReplicatedOperation(ctx, id)
		if executeErr != nil {
			return pass, executeErr
		}
		pass.Triggered++
		if action.Kind == ActionComplete {
			pass.Completed++
		}
	}
	return pass, nil
}

// RunControllerPass reads the bounded RF3 directory and triggers at most one
// exact step per operation. It stores no local queue: a crash before, during,
// or after a trigger resumes from the directory, operation revision, and the
// shard-control result journal.
func RunControllerPass(
	ctx context.Context, directory ControllerDirectory, client ControllerTriggerClient,
) (ControllerPass, error) {
	if ctx == nil || directory == nil || client == nil {
		return ControllerPass{}, ErrControllerTrigger
	}
	catalog, err := directory.Read(ctx)
	if err != nil {
		return ControllerPass{}, err
	}
	ids, err := directory.ReadOperationIDs(ctx)
	if err != nil || len(ids) > maxControllerPassOperations {
		return ControllerPass{}, errors.Join(err, ErrControllerTrigger)
	}
	pass := ControllerPass{Discovered: uint16(len(ids))}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	for index := range ids {
		record, readErr := directory.ReadOperation(ctx, ids[index])
		if errors.Is(readErr, gateway.ErrReplicatedOperationMissing) {
			continue
		}
		if readErr != nil {
			return pass, readErr
		}
		plan, openErr := OpenPlanIntent(record.Intent, catalog)
		if openErr != nil || [32]byte(plan.OperationID()) != record.ID {
			return pass, errors.Join(openErr, ErrControllerTrigger)
		}
		route, ok := catalog.ResolveReplicatedRoute(
			plan.source.Distribution, plan.source.Shard, replicas[:0],
		)
		if !ok || len(route.Replicas) != gateway.ServingReplicaCount {
			return pass, ErrControllerTrigger
		}
		for replica := range route.Replicas {
			if route.Replicas[replica].ControlEndpoint == "" ||
				route.Replicas[replica].ControlAddress == "" {
				return pass, ErrControllerTrigger
			}
		}
		request, requestErr := AppendReconcileTrigger(nil, record)
		if requestErr != nil {
			return pass, requestErr
		}
		response, triggerErr := client.TriggerSplitController(ctx, route, request)
		if triggerErr != nil {
			return pass, triggerErr
		}
		if response.Code != shardcontrol.ResultAccepted || response.Operation != request.Operation ||
			response.Step != request.Step {
			return pass, ErrControllerTrigger
		}
		pass.Triggered++
		if record.State == gateway.ReplicatedOperationComplete {
			pass.Completed++
		}
	}
	return pass, nil
}

const maxControllerPassOperations = 64
