package shardservice

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func (server *ReplicatedServer) readRouteGate(ctx context.Context, request *ReplicatedRequest, wireState ReplicatedMemberState) *ReplicatedResponse {
	result, readLease, readErr := server.owner.ReadRouteGate(ctx,
		raftservice.RouteGateReadRequest{
			Fence: raftservice.ServingFence{
				Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
				Command: request.Fence.Command, MemberID: request.Fence.MemberID,
				StoreID: request.Fence.StoreID, NodeIncarnation: request.Fence.NodeIncarnation,
				Term: request.Fence.Term,
			},
			Capability:     request.Capability,
			MinimumApplied: request.MinimumApplied,
		})
	if readErr != nil && readLease != nil {
		readLease.Release()
		readLease = nil
	}
	if readErr == nil {
		wireState = replicatedReadState(wireState, request.Fence, result.Applied)
		value, encodeErr := AppendReplicatedRouteGateReadValue(nil,
			result.Status)
		response := &ReplicatedResponse{
			Kind: ReplicatedRouteGateReadResult, HasState: true, State: wireState,
			ReadApplied: result.Applied, Value: value, readLease: readLease,
		}
		if encodeErr == nil && result.Applied >= request.MinimumApplied &&
			validReplicatedResponse(response) {
			return response
		}
		if readLease != nil {
			readLease.Release()
		}
		return &ReplicatedResponse{Kind: ReplicatedRefusal,
			Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
	}
	if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
		wireState = replicatedWireState(refreshed)
	}
	switch {
	case errors.Is(readErr, raftmodel.ErrNotLeader),
		errors.Is(readErr, raftmodel.ErrReadLeadershipLost):
		return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
	case errors.Is(readErr, raftservice.ErrServingFence):
		return &ReplicatedResponse{Kind: ReplicatedRefusal,
			Refusal: ReplicatedRefusalStaleFence, HasState: true, State: wireState}
	case errors.Is(readErr, replicatedstate.ErrReadBehind):
		return &ReplicatedResponse{Kind: ReplicatedRefusal,
			Refusal: ReplicatedRefusalReadBehind, HasState: true, State: wireState}
	case errors.Is(readErr, raftservice.ErrRouteGateUnauthorized):
		return &ReplicatedResponse{Kind: ReplicatedRefusal,
			Refusal: ReplicatedRefusalUnauthorized, HasState: true, State: wireState}
	case errors.Is(readErr, raftservice.ErrIngressFull),
		errors.Is(readErr, raftservice.ErrPendingReadsFull):
		return &ReplicatedResponse{Kind: ReplicatedRefusal,
			Refusal: ReplicatedRefusalAdmissionBound, HasState: true, State: wireState}
	default:
		return &ReplicatedResponse{Kind: ReplicatedRefusal,
			Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
	}
}
