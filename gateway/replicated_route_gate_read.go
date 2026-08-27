package gateway

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type ReplicatedRouteGateReadResult struct {
	Applied uint64
	Status  routegate.Status
	State   shardservice.ReplicatedMemberState
	Retries int
}

// ReadRouteGate observes the participant's independent gate epoch and pin
// counts through its authenticated leader, never a follower or lease epoch.
func (executor *ReplicatedExecutor) ReadRouteGate(
	ctx context.Context,
	route ReplicatedRoute,
	minimumApplied uint64,
) (ReplicatedRouteGateReadResult, error) {
	if executor == nil || executor.client == nil || ctx == nil || !validReplicatedRoute(route) ||
		minimumApplied == 0 {
		return ReplicatedRouteGateReadResult{}, ErrReplicatedRoute
	}
	preferred := route.Replicas[0].Member
	var joined error
	for attempt := 0; attempt < executor.maxAttempts; attempt++ {
		endpoint, state, err := executor.discoverLeader(
			ctx, route, preferred, serviceauthz.CapabilityDataWrite,
		)
		if err != nil {
			joined = errors.Join(joined, err)
			preferred = 0
			continue
		}
		response, err := executor.doReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
			Operation:  shardservice.ReplicatedRouteGateRead,
			Capability: serviceauthz.CapabilityDataWrite, Fence: state.Fence,
			MinimumApplied: minimumApplied,
		})
		if err != nil {
			executor.leaderHints.invalidate(route, endpoint, state)
			joined = errors.Join(joined, err)
			preferred = 0
			continue
		}
		if validReplicatedUnauthorizedWithoutState(response) {
			return ReplicatedRouteGateReadResult{}, &ReplicatedRefusalError{Code: response.Refusal}
		}
		if !validReplicatedResponseState(response) || response.State.Fence.Group != route.Group ||
			response.State.Fence.AllocationGeneration != route.AllocationGeneration ||
			response.State.Fence.MemberID != endpoint.Member ||
			response.State.Fence.Command != route.Command {
			executor.leaderHints.invalidate(route, endpoint, state)
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
			continue
		}
		switch response.Kind {
		case shardservice.ReplicatedRouteGateReadResult:
			value, openErr := shardservice.OpenReplicatedRouteGateReadValue(response.Value)
			if openErr != nil || response.Refusal != shardservice.ReplicatedRefusalNone ||
				response.RequestDigest != ([sha256.Size]byte{}) || response.Outcome != (raftserve.Outcome{}) ||
				len(response.Completion) != 0 || response.ReadApplied < minimumApplied ||
				response.State.Applied < response.ReadApplied {
				joined = errors.Join(joined, openErr, ErrReplicatedRoute)
				continue
			}
			executor.leaderHints.publish(route, endpoint, response.State)
			return ReplicatedRouteGateReadResult{
				Applied: response.ReadApplied, Status: value,
				State: response.State, Retries: attempt,
			}, nil
		case shardservice.ReplicatedNotLeader:
			if !validReplicatedNonterminalResponse(response) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				continue
			}
			executor.leaderHints.invalidate(route, endpoint, state)
			preferred = response.State.LeaderID
			joined = errors.Join(joined, raftmodel.ErrNotLeader)
		case shardservice.ReplicatedRefusal:
			if !validReplicatedReadRefusal(response, response.Refusal) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				continue
			}
			if response.Refusal == shardservice.ReplicatedRefusalStaleFence ||
				response.Refusal == shardservice.ReplicatedRefusalReadBehind {
				return ReplicatedRouteGateReadResult{}, &ReplicatedRefusalError{Code: response.Refusal}
			}
			joined = errors.Join(joined, &ReplicatedRefusalError{Code: response.Refusal})
			preferred = 0
		default:
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
		}
	}
	return ReplicatedRouteGateReadResult{}, errors.Join(ErrReplicatedLeader, joined)
}
