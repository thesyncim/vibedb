package splitcontroller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/shardcontrol"
	vibejson "github.com/thesyncim/vibejson"
)

var ErrRemoteExecution = errors.New("splitcontroller: remote shard action was not durably accepted")

// ShardControlRouter selects the exact authenticated shard-control endpoint
// for one already-reconciled action. It must not rebuild or reinterpret request.
type ShardControlRouter interface {
	ExecuteShardControl(context.Context, Action, shardcontrol.Request) (shardcontrol.Response, error)
}

type remoteStepPayload struct {
	Action          uint8    `json:"action"`
	Child           uint8    `json:"child"`
	Catalog         uint64   `json:"catalog"`
	StateDigest     [32]byte `json:"state_digest"`
	DataChainDigest [32]byte `json:"data_chain_digest"`
	EntryDigest     [32]byte `json:"entry_digest"`
}

// ExecuteRemoteReplicatedStep is the serving controller composition: intent
// and running state settle in catalog RF3 before one authenticated shard RPC.
// Exact request identity is derived only from the immutable Plan and coherent
// Observation used by Reconcile.
func ExecuteRemoteReplicatedStep(
	ctx context.Context,
	journal ReplicatedOperationJournal,
	plan *Plan,
	observed Observation,
	router ShardControlRouter,
) (Action, error) {
	if router == nil {
		return Action{}, ErrRemoteExecution
	}
	return ExecuteReplicatedStep(ctx, journal, plan, observed,
		func(ctx context.Context, operation OperationID, action Action) error {
			request, err := appendRemoteStepRequest(nil, plan, observed, action)
			if err != nil || request.Operation != [32]byte(operation) {
				return errors.Join(ErrRemoteExecution, err)
			}
			response, err := router.ExecuteShardControl(ctx, action, request)
			if err != nil {
				return err
			}
			if response.Code != shardcontrol.ResultAccepted ||
				response.Operation != request.Operation || response.Step != request.Step {
				return ErrRemoteExecution
			}
			return nil
		})
}

func appendRemoteStepRequest(
	dst []byte, plan *Plan, observed Observation, action Action,
) (shardcontrol.Request, error) {
	if plan == nil || observed.Catalog == nil || action.Kind < ActionAwaitSourceLeader ||
		action.Kind > ActionComplete {
		return shardcontrol.Request{}, ErrRemoteExecution
	}
	payload := remoteStepPayload{
		Action: uint8(action.Kind), Child: action.Child,
		Catalog:         action.CatalogGeneration,
		StateDigest:     remoteObservationStateDigest(observed.SourceState),
		DataChainDigest: observed.SourceState.DataChainDigest,
		EntryDigest:     observed.SourceState.LastEntryDigest,
	}
	raw, err := vibejson.Marshal(&payload)
	if err != nil {
		return shardcontrol.Request{}, err
	}
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst) > shardcontrol.MaxPayloadBytes {
		return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
	}
	id := [32]byte(plan.OperationID())
	cursor := replicatedActionCursor(action)
	binding := observed.SourceState.Binding
	return shardcontrol.Request{
		Action: shardcontrol.Action(action.Kind), Child: action.Child,
		Operation: id, Step: replicatedActionProof(id, cursor), PlanDigest: id,
		Fence: shardcontrol.Fence{
			CatalogGeneration: observed.Catalog.Generation(),
			Allocation:        binding.AllocationGeneration, OwnershipEpoch: binding.OwnershipEpoch,
			SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
			RouteGeneration:   binding.RouteGeneration,
			ReplicaSetVersion: observed.SourceState.ReplicaSetVersion,
			Applied:           observed.SourceState.Applied,
		},
		Payload: dst,
	}, nil
}

func remoteObservationStateDigest(state replicatedstate.State) [32]byte {
	hash := sha256.New()
	binding := state.Binding
	_, _ = hash.Write(binding.ClusterID[:])
	_, _ = hash.Write(binding.ClusterIncarnation[:])
	_, _ = hash.Write(binding.ShardIncarnation[:])
	_, _ = hash.Write(binding.GroupID[:])
	_, _ = hash.Write(state.LastEntryDigest[:])
	_, _ = hash.Write(state.DataChainDigest[:])
	_, _ = hash.Write(state.SnapshotBaseDigest[:])
	var scalars [11 * 8]byte
	for index, value := range [...]uint64{
		state.Applied, state.LastTerm, state.ReplicaSetVersion,
		binding.TopologyRecoveryEpoch, binding.AllocationGeneration,
		binding.ActivePolicyGeneration, binding.ProtectionEpoch, binding.OwnershipEpoch,
		binding.SchemaGeneration, binding.RoutingVersion, binding.RouteGeneration,
	} {
		binary.LittleEndian.PutUint64(scalars[index*8:], value)
	}
	_, _ = hash.Write(scalars[:])
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(binding.Distribution)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(binding.Distribution))
	binary.LittleEndian.PutUint64(length[:], uint64(len(binding.Shard)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(binding.Shard))
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}
