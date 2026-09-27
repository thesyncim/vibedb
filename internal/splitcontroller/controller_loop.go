package splitcontroller

import (
	"bytes"
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
	// Triggered counts successfully attempted direct actions or accepted remote
	// trigger requests. It is not proof that the durable operation advanced.
	Triggered uint16
	// Progressed counts direct operations whose authoritative durable record
	// changed after execution. Remote trigger passes cannot observe this value.
	Progressed uint16
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
	if cause := context.Cause(ctx); cause != nil {
		return ControllerPass{}, cause
	}
	ids, err := directory.ReadOperationIDs(ctx)
	if err != nil || len(ids) > maxControllerPassOperations {
		return ControllerPass{}, errors.Join(err, ErrControllerTrigger)
	}
	pass := ControllerPass{Discovered: uint16(len(ids))}
	for _, id := range ids {
		if cause := context.Cause(ctx); cause != nil {
			return pass, cause
		}
		record, readErr := directory.ReadOperation(ctx, id)
		if errors.Is(readErr, gateway.ErrReplicatedOperationMissing) {
			continue
		}
		if readErr != nil || record.ID != id || !record.Valid() {
			return pass, errors.Join(readErr, ErrControllerTrigger)
		}
		// The shared directory also carries schema, move, and backup witnesses.
		// Direct split execution is intentionally limited to split records; the
		// other valid kinds are owned by their respective controllers and must
		// not poison this pass.
		if record.Kind != gateway.ReplicatedOperationSplit {
			continue
		}
		// The controller may publish a new operation record while it executes.
		// Keep an owned snapshot because some directory implementations may
		// reuse decoded byte storage on their next read.
		before := record
		before.Intent = bytes.Clone(record.Intent)
		before.Execution = bytes.Clone(record.Execution)
		action, executeErr := controller.ExecuteReplicatedOperation(ctx, id)
		if executeErr != nil {
			return pass, executeErr
		}
		pass.Triggered++
		if action.Kind == ActionComplete {
			pass.Completed++
		}
		if cause := context.Cause(ctx); cause != nil {
			return pass, cause
		}
		after, postReadErr := directory.ReadOperation(ctx, id)
		if cause := context.Cause(ctx); cause != nil {
			return pass, cause
		}
		progressed, progressErr := directOperationProgress(id, before, after, postReadErr)
		if progressErr != nil {
			return pass, progressErr
		}
		if progressed {
			pass.Progressed++
		}
	}
	return pass, nil
}

func directOperationProgress(
	id [32]byte,
	before, after gateway.ReplicatedOperationRecord,
	readErr error,
) (bool, error) {
	if id == ([32]byte{}) || !before.Valid() || before.ID != id {
		return false, ErrReplicatedExecution
	}
	if errors.Is(readErr, gateway.ErrReplicatedOperationMissing) {
		return true, nil
	}
	if readErr != nil {
		return false, errors.Join(readErr, ErrControllerTrigger)
	}
	if !after.Valid() || after.ID != id || after.Kind != before.Kind ||
		after.IntentDigest != before.IntentDigest || !bytes.Equal(after.Intent, before.Intent) ||
		after.CatalogGeneration < before.CatalogGeneration {
		return false, ErrReplicatedExecution
	}
	if after.Revision < before.Revision {
		return false, ErrReplicatedExecution
	}
	if after.Revision > before.Revision {
		return true, nil
	}
	if !after.Equal(before) {
		return false, ErrReplicatedExecution
	}
	return false, nil
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
		if record.ID != ids[index] || !record.Valid() {
			return pass, ErrControllerTrigger
		}
		// Schema, move, and backup witnesses share the bounded directory but
		// are dispatched by their own controllers. A split trigger must never
		// try to decode their intent as a split plan.
		if record.Kind != gateway.ReplicatedOperationSplit {
			continue
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
