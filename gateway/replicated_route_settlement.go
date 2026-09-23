package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// ReadRouteReleaseReceipt reads the exact committed ReleaseShared completion
// retained by one physical source. The route is the current serving route used
// only for its endpoint, fence, and service-directory scope; exactCommand is
// never rewritten and is the sole receipt identity sent to the source.
func (executor *ReplicatedExecutor) ReadRouteReleaseReceipt(
	ctx context.Context,
	currentSource ReplicatedRoute,
	exactCommand []byte,
	minimumApplied uint64,
) (ReplicatedResult, error) {
	if executor == nil || executor.client == nil || ctx == nil ||
		!validReplicatedRoute(currentSource) || minimumApplied == 0 {
		return ReplicatedResult{}, ErrReplicatedRoute
	}
	command, err := replicatedstate.ValidateRouteReleaseReceiptCommand(exactCommand)
	if err != nil || command.Kind() != replication.CommandRouteGate {
		return ReplicatedResult{}, ErrReplicatedRoute
	}
	original := exactCommand[:len(exactCommand):len(exactCommand)]
	preferred := currentSource.Replicas[0].Member
	if endpoint, _, ok := executor.leaderHints.lookup(currentSource); ok {
		preferred = endpoint.Member
	}
	var joined error
	for attempt := 0; attempt < executor.maxAttempts; attempt++ {
		endpoint, state, discoverErr := executor.discoverLeaderFresh(
			ctx, currentSource, preferred, serviceauthz.CapabilityTopology,
		)
		if discoverErr != nil {
			joined = errors.Join(joined, discoverErr)
			if terminalReplicatedDiscoveryError(discoverErr) {
				return ReplicatedResult{}, joined
			}
			preferred = 0
			continue
		}
		preferred = state.LeaderID
		response, readErr := executor.doReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
			Operation:  shardservice.ReplicatedRouteSettlement,
			Capability: serviceauthz.CapabilityRequestLedger,
			Fence: shardservice.ReplicatedFence{
				Group: currentSource.Group, AllocationGeneration: currentSource.AllocationGeneration,
				Command: state.Fence.Command, MemberID: endpoint.Member,
				StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation,
				Term: state.Fence.Term,
			},
			RouteSettlement: shardservice.ReplicatedRouteSettlementRequest{
				Mode:           shardservice.ReplicatedRouteSettlementReadReleaseReceipt,
				MinimumApplied: minimumApplied, Command: original,
			},
		})
		if readErr != nil {
			executor.leaderHints.invalidate(currentSource, endpoint, state)
			joined = errors.Join(joined, readErr)
			preferred = nextReplicatedMember(currentSource, endpoint.Member)
			if attempt+1 < executor.maxAttempts {
				if waitErr := waitReplicatedFailoverRetry(ctx, attempt); waitErr != nil {
					return ReplicatedResult{}, errors.Join(joined, waitErr)
				}
			}
			continue
		}
		if validReplicatedUnauthorizedWithoutState(response) {
			return ReplicatedResult{}, &ReplicatedRefusalError{Code: response.Refusal}
		}
		if !validReplicatedResponseState(response) || response.State.Fence.Group != currentSource.Group ||
			response.State.Fence.AllocationGeneration != currentSource.AllocationGeneration ||
			response.State.Fence.MemberID != endpoint.Member ||
			!replicatedObservedCommandMatches(currentSource, response.State.Fence.Command) {
			executor.leaderHints.invalidate(currentSource, endpoint, state)
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
			continue
		}
		switch response.Kind {
		case shardservice.ReplicatedRouteSettlementResult:
			value, valueErr := shardservice.OpenReplicatedRouteSettlementValue(response.Value)
			completion, completionErr := replication.OpenCompletion(value.Completion)
			outcome := raftserve.Outcome{
				Code: raftserve.OutcomeCompletion, AppliedIndex: response.ReadApplied,
				CompletionBytes: len(value.Completion), CompletionAppliedSequence: value.CompletionAppliedSequence,
			}
			if valueErr != nil || completionErr != nil || response.Refusal != shardservice.ReplicatedRefusalNone ||
				response.RequestDigest != ([sha256.Size]byte{}) || response.Outcome != (raftserve.Outcome{}) ||
				len(response.Completion) != 0 || response.ReadApplied < minimumApplied ||
				response.State.Applied < response.ReadApplied ||
				completion.AppliedSequence != value.CompletionAppliedSequence ||
				!validDurableRequestSettlement(original, ReplicatedResult{Outcome: outcome, Completion: value.Completion}) {
				joined = errors.Join(joined, valueErr, completionErr, ErrReplicatedRoute)
				continue
			}
			executor.leaderHints.publish(currentSource, endpoint, response.State)
			return ReplicatedResult{
				Outcome: outcome, Completion: bytes.Clone(value.Completion),
				State: response.State, Retries: attempt,
			}, nil
		case shardservice.ReplicatedNotLeader:
			if !validReplicatedNonterminalResponse(response) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				continue
			}
			executor.leaderHints.invalidate(currentSource, endpoint, state)
			preferred = response.State.LeaderID
			joined = errors.Join(joined, raftmodel.ErrNotLeader)
		case shardservice.ReplicatedRefusal:
			if !validReplicatedReadRefusal(response, response.Refusal) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				continue
			}
			// A missing, retired, or unauthorized receipt is a definite read
			// result. The lifecycle runner rereads the canonical ledger cut and
			// decides whether this is a concurrent Released record or a refusal;
			// this method never proposes the old command as a fallback.
			return ReplicatedResult{}, &ReplicatedRefusalError{Code: response.Refusal}
		default:
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
		}
	}
	if joined == nil {
		joined = ErrReplicatedLeader
	}
	return ReplicatedResult{}, errors.Join(ErrReplicatedLeader, joined)
}
