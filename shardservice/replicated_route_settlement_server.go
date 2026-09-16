package shardservice

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

type replicatedRouteSettlementOwner interface {
	ReadRouteReleaseReceipt(
		context.Context,
		raftservice.RouteReleaseReceiptReadRequest,
	) (raftservice.RouteReleaseReceiptReadResult, raftservice.RouteReleaseReceiptReadLease, error)
}

func (server *ReplicatedServer) readRouteReleaseReceipt(
	ctx context.Context,
	request *ReplicatedRequest,
	wireState ReplicatedMemberState,
	authorize raftservice.ProposalAuthorization,
) *ReplicatedResponse {
	if request.RouteSettlement.Mode != ReplicatedRouteSettlementReadReleaseReceipt {
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
			HasState: true, State: wireState,
		}
	}
	owner, ok := server.owner.(replicatedRouteSettlementOwner)
	if !ok {
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
			HasState: true, State: wireState,
		}
	}
	result, readLease, readErr := owner.ReadRouteReleaseReceipt(ctx,
		raftservice.RouteReleaseReceiptReadRequest{
			Fence: raftservice.ServingFence{
				Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
				Command: request.Fence.Command, MemberID: request.Fence.MemberID,
				StoreID: request.Fence.StoreID, NodeIncarnation: request.Fence.NodeIncarnation,
				Term: request.Fence.Term,
			},
			Capability: request.Capability, Command: request.RouteSettlement.Command,
			MinimumApplied: request.RouteSettlement.MinimumApplied, Authorize: authorize,
		})
	if readErr != nil && readLease != nil {
		readLease.Release()
		readLease = nil
	}
	if readErr == nil {
		wireState = replicatedWireState(result.State)
		wireState = replicatedReadState(wireState, request.Fence, result.Applied)
		value, encodeErr := AppendReplicatedRouteSettlementValue(nil,
			ReplicatedRouteSettlementValue{
				Mode:                      ReplicatedRouteSettlementReadReleaseReceipt,
				CompletionAppliedSequence: result.AppliedSequence,
				Completion:                result.Completion,
			})
		response := &ReplicatedResponse{
			Kind: ReplicatedRouteSettlementResult, HasState: true, State: wireState,
			ReadApplied: result.Applied, Value: value, readLease: readLease,
		}
		if encodeErr == nil && result.Applied >= request.RouteSettlement.MinimumApplied &&
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
	case errors.Is(readErr, raftservice.ErrServingAuthorization):
		return &ReplicatedResponse{Kind: ReplicatedRefusal,
			Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
	case errors.Is(readErr, replicatedstate.ErrReadBehind):
		return &ReplicatedResponse{Kind: ReplicatedRefusal,
			Refusal: ReplicatedRefusalReadBehind, HasState: true, State: wireState}
	case errors.Is(readErr, raftservice.ErrRouteReleaseReceiptUnauthorized):
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
