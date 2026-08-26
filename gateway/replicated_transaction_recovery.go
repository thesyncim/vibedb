package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// ReplicatedTransactionRecoveryResult is one leader-only, ReadIndex-fenced
// page from the hidden replicated transaction state. Record payloads are
// canonical VTC1, VTCM, or VTM1 bytes owned by the detached response frame.
type ReplicatedTransactionRecoveryResult struct {
	Applied  uint64
	Complete bool
	Records  []replicatedstate.TransactionRecoveryRecord
	State    shardservice.ReplicatedMemberState
	Retries  int
}

// ReadTransactionRecovery reads only the closed replicated transaction
// recovery vocabulary. It never falls back to a follower, a leader lease, or
// the legacy process-local transaction journal.
func (executor *ReplicatedExecutor) ReadTransactionRecovery(
	ctx context.Context,
	route ReplicatedRoute,
	read replicatedstate.TransactionRecoveryReadRequest,
) (ReplicatedTransactionRecoveryResult, error) {
	if executor == nil || executor.client == nil || ctx == nil ||
		!validReplicatedRoute(route) ||
		replicatedstate.ValidateTransactionRecoveryReadRequest(read) != nil {
		return ReplicatedTransactionRecoveryResult{}, ErrReplicatedRoute
	}
	wireRead, ok := replicatedTransactionRecoveryWireRead(read)
	if !ok {
		return ReplicatedTransactionRecoveryResult{}, ErrReplicatedRoute
	}
	preferred := route.Replicas[0].Member
	var joined error
	for attempt := 0; attempt < executor.maxAttempts; attempt++ {
		endpoint, state, err := executor.discoverLeader(
			ctx, route, preferred, serviceauthz.CapabilityTransactionRecovery,
		)
		if err != nil {
			joined = errors.Join(joined, err)
			preferred = 0
			continue
		}
		response, err := executor.doReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
			Operation:       shardservice.ReplicatedTransactionRead,
			Capability:      serviceauthz.CapabilityTransactionRecovery,
			Fence:           state.Fence,
			TransactionRead: wireRead,
		})
		if err != nil {
			executor.leaderHints.invalidate(route, endpoint, state)
			joined = errors.Join(joined, err)
			preferred = 0
			continue
		}
		if validReplicatedUnavailableWithoutState(response) {
			joined = errors.Join(joined, &ReplicatedRefusalError{Code: response.Refusal})
			preferred = 0
			continue
		}
		if validReplicatedUnauthorizedWithoutState(response) {
			return ReplicatedTransactionRecoveryResult{},
				&ReplicatedRefusalError{Code: response.Refusal}
		}
		if !validReplicatedResponseState(response) ||
			response.State.Fence.Group != route.Group ||
			response.State.Fence.AllocationGeneration != route.AllocationGeneration ||
			response.State.Fence.MemberID != endpoint.Member {
			executor.leaderHints.invalidate(route, endpoint, state)
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
			continue
		}
		if response.State.Fence.Command != route.Command {
			executor.leaderHints.invalidate(route, endpoint, state)
			if validReplicatedReadRefusal(response, shardservice.ReplicatedRefusalStaleFence) {
				return ReplicatedTransactionRecoveryResult{},
					&ReplicatedRefusalError{Code: response.Refusal}
			}
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
			continue
		}
		switch response.Kind {
		case shardservice.ReplicatedTransactionReadResult:
			if response.Refusal != shardservice.ReplicatedRefusalNone ||
				response.RequestDigest != ([sha256.Size]byte{}) ||
				response.Outcome != (raftserve.Outcome{}) || len(response.Completion) != 0 ||
				response.ReadApplied < read.MinimumApplied ||
				response.State.Applied < response.ReadApplied ||
				len(response.Value) < shardservice.ReplicatedTransactionReadValueHeaderBytes ||
				len(response.Value)-shardservice.ReplicatedTransactionReadValueHeaderBytes > int(read.MaxBytes) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				preferred = 0
				continue
			}
			records := make([]replicatedstate.TransactionRecoveryRecord, 0, int(read.MaxRows))
			value, openErr := shardservice.OpenReplicatedTransactionReadValueInto(
				response.Value, records,
			)
			if openErr != nil || value.Kind != wireRead.Kind || len(value.Records) > int(read.MaxRows) {
				joined = errors.Join(joined, openErr, ErrReplicatedRoute)
				preferred = 0
				continue
			}
			if !replicatedTransactionRecoveryResultMatches(read, value) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				preferred = 0
				continue
			}
			executor.leaderHints.publish(route, endpoint, response.State)
			return ReplicatedTransactionRecoveryResult{
				Applied: response.ReadApplied, Complete: value.Complete,
				Records: value.Records, State: response.State, Retries: attempt,
			}, nil
		case shardservice.ReplicatedNotLeader:
			executor.leaderHints.invalidate(route, endpoint, state)
			if !validReplicatedNonterminalResponse(response) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				preferred = 0
				continue
			}
			preferred = response.State.LeaderID
			joined = errors.Join(joined, raftmodel.ErrNotLeader)
		case shardservice.ReplicatedRefusal:
			if !validReplicatedTransactionReadRefusal(response, response.Refusal) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				preferred = 0
				continue
			}
			if response.Refusal == shardservice.ReplicatedRefusalStaleFence {
				executor.leaderHints.invalidate(route, endpoint, state)
				joined = errors.Join(joined, &ReplicatedRefusalError{Code: response.Refusal})
				preferred = response.State.LeaderID
				if attempt+1 < executor.maxAttempts {
					if err := waitReplicatedFailoverRetry(ctx, attempt); err != nil {
						return ReplicatedTransactionRecoveryResult{},
							errors.Join(ErrReplicatedLeader, err)
					}
				}
				continue
			}
			if response.Refusal == shardservice.ReplicatedRefusalReadBehind ||
				response.Refusal == shardservice.ReplicatedRefusalReadBufferBound ||
				response.Refusal == shardservice.ReplicatedRefusalUnauthorized ||
				response.Refusal == shardservice.ReplicatedRefusalTransactionReadMalformed {
				return ReplicatedTransactionRecoveryResult{},
					&ReplicatedRefusalError{Code: response.Refusal}
			}
			joined = errors.Join(joined, &ReplicatedRefusalError{Code: response.Refusal})
			preferred = 0
		default:
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
		}
	}
	return ReplicatedTransactionRecoveryResult{}, errors.Join(ErrReplicatedLeader, joined)
}

func validReplicatedTransactionReadRefusal(
	response *shardservice.ReplicatedResponse,
	code shardservice.ReplicatedRefusalCode,
) bool {
	if code != shardservice.ReplicatedRefusalTransactionReadMalformed {
		return validReplicatedReadRefusal(response, code)
	}
	return response != nil && response.Kind == shardservice.ReplicatedRefusal &&
		response.Refusal == code && response.HasState &&
		response.RequestDigest == ([sha256.Size]byte{}) &&
		response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
		response.ReadApplied == 0 && len(response.Value) == 0
}

func replicatedTransactionRecoveryResultMatches(
	read replicatedstate.TransactionRecoveryReadRequest,
	value shardservice.ReplicatedTransactionReadValue,
) bool {
	if read.Kind == replicatedstate.TransactionRecoveryScanCoordinator {
		for index := range value.Records {
			if index == 0 {
				if !read.ID.IsZero() && bytes.Compare(value.Records[index].ID[:], read.ID[:]) <= 0 {
					return false
				}
			} else if bytes.Compare(value.Records[index-1].ID[:], value.Records[index].ID[:]) >= 0 {
				return false
			}
		}
		return true
	}
	if len(value.Records) == 0 {
		return true
	}
	return len(value.Records) == 1 && value.Records[0].ID == read.ID &&
		(read.Kind != replicatedstate.TransactionRecoveryReadManifestPage ||
			value.Records[0].ManifestPage == read.ManifestPage)
}

func replicatedTransactionRecoveryWireRead(
	read replicatedstate.TransactionRecoveryReadRequest,
) (shardservice.ReplicatedTransactionReadRequest, bool) {
	var kind shardservice.ReplicatedTransactionReadKind
	switch read.Kind {
	case replicatedstate.TransactionRecoveryLookupCoordinator:
		kind = shardservice.ReplicatedTransactionLookupCoordinator
	case replicatedstate.TransactionRecoveryLookupParticipant:
		kind = shardservice.ReplicatedTransactionLookupParticipant
	case replicatedstate.TransactionRecoveryReadManifestPage:
		kind = shardservice.ReplicatedTransactionReadManifestPage
	case replicatedstate.TransactionRecoveryScanCoordinator:
		kind = shardservice.ReplicatedTransactionScanCoordinators
	default:
		return shardservice.ReplicatedTransactionReadRequest{}, false
	}
	return shardservice.ReplicatedTransactionReadRequest{
		Kind: kind, ID: read.ID, MinimumApplied: read.MinimumApplied,
		SegmentIndex: read.ManifestPage, MaxRows: read.MaxRows, MaxBytes: read.MaxBytes,
	}, true
}
