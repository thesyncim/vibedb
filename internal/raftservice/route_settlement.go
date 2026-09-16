package raftservice

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// ErrRouteReleaseReceiptUnauthorized keeps the exact ledger-recovery lane
// separate from ordinary data and route-gate reads. A receipt is admitted
// only with the authenticated request-ledger capability.
var ErrRouteReleaseReceiptUnauthorized = errors.New(
	"raftservice: route-release receipt read is not authorized",
)

// RouteReleaseReceiptSource exposes the one retained route-session release
// completion needed after a lost proposal response. Implementations resolve
// the exact command in the replicated source session completion ring; this
// interface does not accept a digest or an arbitrary collection key.
type RouteReleaseReceiptSource interface {
	RouteReleaseReceiptReadInto([]byte, []byte) (replicatedstate.CompletionLookup, error)
}

type RouteReleaseReceiptReadRequest struct {
	Fence          ServingFence
	Capability     serviceauthz.Capability
	Command        []byte
	MinimumApplied uint64
	Authorize      ProposalAuthorization
}

type RouteReleaseReceiptReadResult struct {
	Applied         uint64
	Completion      []byte
	AppliedSequence uint64
	State           ServingState
}

type RouteReleaseReceiptReadLease interface{ Release() }

func (owners *ExecutionOwners) ReadRouteReleaseReceipt(
	ctx context.Context,
	request RouteReleaseReceiptReadRequest,
) (RouteReleaseReceiptReadResult, RouteReleaseReceiptReadLease, error) {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return RouteReleaseReceiptReadResult{}, nil, err
	}
	return owner.ReadRouteReleaseReceipt(ctx, request)
}

// ReadRouteReleaseReceipt resolves one exact committed ReleaseShared
// completion after a leader-only quorum barrier. The caller receives bytes
// only when the authenticated source returns a nil lookup error.
func (owner *Owner) ReadRouteReleaseReceipt(
	ctx context.Context,
	request RouteReleaseReceiptReadRequest,
) (RouteReleaseReceiptReadResult, RouteReleaseReceiptReadLease, error) {
	if owner == nil || ctx == nil || request.MinimumApplied == 0 {
		return RouteReleaseReceiptReadResult{}, nil, ErrInvalidOwner
	}
	if request.Capability != serviceauthz.CapabilityRequestLedger {
		return RouteReleaseReceiptReadResult{}, nil, ErrRouteReleaseReceiptUnauthorized
	}
	if _, err := replicatedstate.ValidateRouteReleaseReceiptCommand(request.Command); err != nil {
		return RouteReleaseReceiptReadResult{}, nil, ErrInvalidOwner
	}
	responseCharge, ok := pointReadResponseCharge(replicatedstate.MaxRouteGateCompletionEnvelopeBytes)
	if !ok {
		return RouteReleaseReceiptReadResult{}, nil, ErrInvalidOwner
	}
	if err := owner.reservePendingRead(responseCharge); err != nil {
		return RouteReleaseReceiptReadResult{}, nil, err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			owner.releasePendingRead(responseCharge)
		}
	}()
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	command := bytes.Clone(request.Command)
	reply, err := owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadRouteReleaseReceipt, group: request.Fence.Group, reply: delivery.reply,
		read: readRequest{
			fence: request.Fence, minimumApplied: request.MinimumApplied,
			delivery: delivery, authorize: request.Authorize,
			routeReleaseCommand: command,
		},
	}, delivery)
	if err != nil {
		return RouteReleaseReceiptReadResult{}, nil, err
	}
	defer reply.read.generation.release()
	if reply.read.routeReleaseReceipt == nil {
		return RouteReleaseReceiptReadResult{}, nil, ErrServingFence
	}
	dst := make([]byte, 0, replicatedstate.MaxRouteGateCompletionEnvelopeBytes)
	lookup, err := reply.read.routeReleaseReceipt.RouteReleaseReceiptReadInto(reply.read.routeReleaseCommand, dst)
	if err != nil || len(lookup.Bytes) == 0 || lookup.AppliedSequence == 0 {
		if err == nil {
			err = ErrServingFence
		}
		return RouteReleaseReceiptReadResult{}, nil, err
	}
	lookup.Bytes = lookup.Bytes[:len(lookup.Bytes):len(lookup.Bytes)]
	releaseReservation = false
	return RouteReleaseReceiptReadResult{
		Applied: reply.read.minimumApplied, Completion: lookup.Bytes,
		AppliedSequence: lookup.AppliedSequence, State: reply.read.state,
	}, &pointReadLease{owner: owner, bytes: responseCharge}, nil
}
