package raftservice

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var ErrRouteGateUnauthorized = errors.New("raftservice: route-gate read requires data-write authority")

type RouteGateSource interface {
	RouteGateRead(uint64) (replicatedstate.RouteGateReadResult, error)
}

type RouteGateReadRequest struct {
	Fence          ServingFence
	Capability     serviceauthz.Capability
	MinimumApplied uint64
	Authorize      ProposalAuthorization
}

type RouteGateReadResult struct {
	Applied uint64
	Status  routegate.Status
	State   ServingState
}

type RouteGateReadLease interface{ Release() }

func (owners *ExecutionOwners) ReadRouteGate(ctx context.Context, request RouteGateReadRequest) (RouteGateReadResult, RouteGateReadLease, error) {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return RouteGateReadResult{}, nil, err
	}
	return owner.ReadRouteGate(ctx, request)
}

// ReadRouteGate observes shared-pin lifecycle state at a leader quorum barrier.
// It is not a topology action and does not accept follower observations.
func (owner *Owner) ReadRouteGate(
	ctx context.Context,
	request RouteGateReadRequest,
) (RouteGateReadResult, RouteGateReadLease, error) {
	if owner == nil || ctx == nil || request.MinimumApplied == 0 {
		return RouteGateReadResult{}, nil, ErrInvalidOwner
	}
	if request.Capability != serviceauthz.CapabilityDataWrite {
		return RouteGateReadResult{}, nil, ErrRouteGateUnauthorized
	}
	responseCharge, ok := pointReadResponseCharge(routegate.StatusBytes)
	if !ok {
		return RouteGateReadResult{}, nil, ErrInvalidOwner
	}
	if err := owner.reservePendingRead(responseCharge); err != nil {
		return RouteGateReadResult{}, nil, err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			owner.releasePendingRead(responseCharge)
		}
	}()
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	reply, err := owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadRouteGate, group: request.Fence.Group, reply: delivery.reply,
		read: readRequest{
			fence: request.Fence, minimumApplied: request.MinimumApplied, delivery: delivery,
			authorize: request.Authorize,
		},
	}, delivery)
	if err != nil {
		return RouteGateReadResult{}, nil, err
	}
	defer reply.read.generation.release()
	value, err := reply.read.routeGate.RouteGateRead(reply.read.minimumApplied)
	if err != nil {
		return RouteGateReadResult{}, nil, err
	}
	if !pointReadFenceMatches(value.Fence, request.Fence) ||
		value.Fence.Applied < reply.read.minimumApplied {
		return RouteGateReadResult{}, nil, ErrServingFence
	}
	releaseReservation = false
	return RouteGateReadResult{
		Applied: value.Fence.Applied, Status: value.Status, State: reply.read.state,
	}, &pointReadLease{owner: owner, bytes: responseCharge}, nil
}
