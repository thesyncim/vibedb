package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/shardcontrol"
	vibejson "github.com/thesyncim/vibejson"
)

// One bounded wave occupies the existing catalog operation row. No local
// controller journal, growing per-wave catalog directory, or larger document
// quota is introduced. An oversized wave fails catalog admission before RPC.
type remoteExecutionImage struct {
	Requests [][]byte `json:"requests"`
}

// The old witness sequence occupied the action-kind/child tuple namespace.
// Revision-ordered waves start strictly above it so existing admitted plans
// can resume without a sequence regression after upgrading the controller.
const remoteExecutionSequenceBase = uint64(ActionComplete+1) << 8

func pendingRemoteAction(record gateway.ReplicatedOperationRecord) (Action, error) {
	action := Action{Kind: ActionKind(record.Cursor[0]), Child: uint8(record.Cursor[1]), CatalogGeneration: record.Cursor[2]}
	if !record.Valid() || record.State != gateway.ReplicatedOperationRunning || len(record.Execution) == 0 || record.ExecutionSettled ||
		action.Kind < ActionAwaitSourceLeader || action.Kind >= ActionComplete || replicatedActionCursor(action) != record.Cursor ||
		replicatedActionProof(record.ID, record.Cursor) != record.Proof {
		return Action{}, ErrReplicatedExecution
	}
	return action, nil
}

func executeRemoteActionWave(ctx context.Context, journal ReplicatedOperationJournal, plan *Plan, observed Observation, action Action, router ShardControlRouter, allChildren bool) error {
	record, err := journal.ReadOperation(ctx, [32]byte(plan.OperationID()))
	if err != nil || record.State != gateway.ReplicatedOperationRunning || record.Cursor != replicatedActionCursor(action) || record.Revision == math.MaxUint64 {
		return errors.Join(ErrReplicatedExecution, err)
	}
	var requests []shardcontrol.Request
	if len(record.Execution) != 0 {
		requests, err = openRemoteExecution(record, action)
		if err != nil {
			return err
		}
		if record.ExecutionSettled {
			_, candidate, err := buildRemoteExecution(plan, observed, action, record.ExecutionRevision, allChildren)
			if err != nil {
				return err
			}
			if bytes.Equal(candidate, record.Execution) {
				return nil
			}
		}
	}
	if len(record.Execution) == 0 || record.ExecutionSettled {
		if record.Revision > math.MaxUint64-remoteExecutionSequenceBase-2 {
			return ErrReplicatedExecution
		}
		next := record
		next.Revision++
		next.ExecutionRevision, next.ExecutionSettled = next.Revision, false
		requests, next.Execution, err = buildRemoteExecution(plan, observed, action, next.ExecutionRevision, allChildren)
		if err != nil {
			return err
		}
		if err = settleReplicatedOperationPublish(ctx, journal, record.Revision, next); err != nil {
			return err
		}
		record = next
	}
	// Every request is already in catalog Raft before any destination sees it.
	// A partial fanout or lost reply replays these exact frames after restart.
	failures := make([]error, len(requests))
	var group sync.WaitGroup
	for index := range requests {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			request := requests[index]
			response, err := router.ExecuteShardControl(ctx, action, request)
			if err != nil {
				failures[index] = err
				return
			}
			if response.Code != shardcontrol.ResultAccepted || response.Operation != request.Operation || response.Step != request.Step {
				failures[index] = ErrRemoteExecution
			}
		}(index)
	}
	group.Wait()
	if err := errors.Join(failures...); err != nil {
		return err
	}
	next := record
	next.Revision++
	next.ExecutionSettled = true
	return settleReplicatedOperationPublish(ctx, journal, record.Revision, next)
}

func buildRemoteExecution(plan *Plan, observed Observation, action Action, revision uint64, allChildren bool) ([]shardcontrol.Request, []byte, error) {
	var targets []ShardActionTarget
	if allChildren && remoteActionTargetsChild(action.Kind) {
		target, found := plan.Target(action.Child)
		if !found || len(target.Replicas) != gateway.ServingReplicaCount {
			return nil, nil, ErrRemoteExecution
		}
		for _, replica := range target.Replicas {
			destination, err := remoteActionTargetForChildReplica(plan, action.Child, replica)
			if err != nil {
				return nil, nil, err
			}
			targets = append(targets, destination)
		}
	} else {
		target, err := remoteActionTarget(plan, observed, action)
		if err != nil {
			return nil, nil, err
		}
		targets = []ShardActionTarget{target}
	}
	requests := make([]shardcontrol.Request, len(targets))
	image := remoteExecutionImage{Requests: make([][]byte, len(targets))}
	for index, target := range targets {
		request, err := appendRemoteStepRequestForTarget(nil, plan, observed, action, target)
		if err != nil {
			return nil, nil, err
		}
		request, err = bindRemoteExecutionRevision(request, action, revision)
		if err != nil {
			return nil, nil, err
		}
		image.Requests[index], err = shardcontrol.AppendRequest(nil, &request)
		if err != nil {
			return nil, nil, err
		}
		requests[index] = request
	}
	raw, err := appendCanonicalVibeJSON(nil, &image)
	if err != nil || len(raw) > gateway.MaxReplicatedOperationBytes {
		return nil, nil, errors.Join(ErrRemoteExecution, err)
	}
	return requests, raw, nil
}

func openRemoteExecution(record gateway.ReplicatedOperationRecord, action Action) ([]shardcontrol.Request, error) {
	if !record.Valid() || record.ExecutionRevision == 0 || len(record.Execution) == 0 {
		return nil, ErrReplicatedExecution
	}
	var image remoteExecutionImage
	if err := vibejson.Unmarshal(record.Execution, &image); err != nil || len(image.Requests) == 0 || len(image.Requests) > gateway.ServingReplicaCount {
		return nil, errors.Join(ErrReplicatedExecution, err)
	}
	canonical, err := appendCanonicalVibeJSON(nil, &image)
	if err != nil || !bytes.Equal(canonical, record.Execution) {
		return nil, errors.Join(ErrReplicatedExecution, err)
	}
	requests := make([]shardcontrol.Request, len(image.Requests))
	for index, raw := range image.Requests {
		request, err := shardcontrol.OpenRequest(raw)
		if err != nil || request.Operation != record.ID || request.PlanDigest != record.IntentDigest ||
			request.Action != shardcontrol.Action(action.Kind) || request.Child != action.Child ||
			request.Step != remoteExecutionStep(record.ID, action, record.ExecutionRevision) {
			return nil, errors.Join(ErrReplicatedExecution, err)
		}
		payload, err := openRemoteStepPayload(request)
		if err != nil || payload.ExecutionRevision != record.ExecutionRevision {
			return nil, errors.Join(ErrReplicatedExecution, err)
		}
		requests[index] = request
	}
	return requests, nil
}

func bindRemoteExecutionRevision(request shardcontrol.Request, action Action, revision uint64) (shardcontrol.Request, error) {
	payload, err := openRemoteStepPayload(request)
	if err != nil || revision == 0 || remoteExecutionSequence(action, revision) == 0 {
		return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
	}
	payload.ExecutionRevision, payload.Sequence = revision, remoteExecutionSequence(action, revision)
	request.Payload, err = appendCanonicalVibeJSON(nil, &payload)
	request.Step = remoteExecutionStep(request.Operation, action, revision)
	return request, err
}

func remoteExecutionSequence(action Action, revision uint64) uint64 {
	if revision == 0 {
		return remoteActionWitnessSequence(action)
	}
	if revision > math.MaxUint64-remoteExecutionSequenceBase {
		return 0
	}
	return remoteExecutionSequenceBase + revision
}

func remoteExecutionStep(operation [32]byte, action Action, revision uint64) [32]byte {
	proof := replicatedActionProof(operation, replicatedActionCursor(action))
	if revision == 0 {
		return proof
	}
	var raw [40]byte
	copy(raw[:32], proof[:])
	binary.LittleEndian.PutUint64(raw[32:], revision)
	return sha256.Sum256(raw[:])
}
