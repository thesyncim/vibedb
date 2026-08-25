package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	AbsoluteMaxReplicatedRouteMembers   = 64
	AbsoluteMaxReplicatedAttempts       = 16
	AbsoluteMaxReplicatedAttemptTimeout = 5 * time.Minute
)

var (
	ErrReplicatedRoute           = errors.New("gateway: invalid replicated shard route")
	ErrReplicatedLeader          = errors.New("gateway: replicated shard has no reachable leader")
	ErrReplicatedDial            = errors.New("gateway: replicated shard dial is not configured")
	ErrReplicatedReadBehind      = errors.New("gateway: replica is below the requested applied index")
	ErrReplicatedReadBufferBound = errors.New("gateway: point-read response bound is below the relation limit")
	ErrReplicatedUnauthorized    = errors.New("gateway: replicated authorization denied")
)

// ReplicatedEndpoint binds one Raft member to its cold network address. Member
// is fixed-width; Address is interpreted only by the dial boundary.
type ReplicatedEndpoint struct {
	Member          uint64
	Node            rafttransport.NodeID
	StoreID         [16]byte
	NodeIncarnation uint64
	NativeEndpoint  string
	Address         string
	ControlEndpoint string
	ControlAddress  string
}

// ReplicatedRoute is one exact catalog allocation and its bounded replica set.
type ReplicatedRoute struct {
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	Group                raftmember.GroupKey
	AllocationGeneration uint64
	Command              raftservice.CommandFence
	Replicas             []ReplicatedEndpoint
}

// ReplicatedRoundTripper performs one native request. Implementations must not
// reinterpret or rebuild request.Command.
type ReplicatedRoundTripper interface {
	DoReplicated(
		context.Context,
		ReplicatedEndpoint,
		*shardservice.ReplicatedRequest,
	) (*shardservice.ReplicatedResponse, error)
}

// ReplicatedDial opens one native shard connection.
type ReplicatedDial func(context.Context, string) (net.Conn, error)

// TCPReplicatedClient is the minimal connection boundary. Dial is required:
// authentication and endpoint authorization belong to the caller's explicit
// transport policy, so this boundary never silently falls back to raw TCP.
type TCPReplicatedClient struct {
	Dial ReplicatedDial
}

func (client TCPReplicatedClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if client.Dial == nil {
		return nil, ErrReplicatedDial
	}
	connection, err := client.Dial(ctx, endpoint.Address)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return shardservice.RoundTripReplicated(ctx, connection, request)
}

// ReplicatedExecutor performs leader discovery and retries only the exact
// original command bytes. MaxAttempts includes the first proposal and is
// required, so no implicit unbounded retry policy exists.
type ReplicatedExecutor struct {
	client         ReplicatedRoundTripper
	maxAttempts    int
	attemptTimeout time.Duration
}

func NewReplicatedExecutor(
	client ReplicatedRoundTripper,
	maxAttempts int,
	attemptTimeout time.Duration,
) (*ReplicatedExecutor, error) {
	if client == nil || maxAttempts <= 0 || maxAttempts > AbsoluteMaxReplicatedAttempts ||
		attemptTimeout <= 0 || attemptTimeout > AbsoluteMaxReplicatedAttemptTimeout {
		return nil, ErrReplicatedRoute
	}
	return &ReplicatedExecutor{
		client: client, maxAttempts: maxAttempts, attemptTimeout: attemptTimeout,
	}, nil
}

// ReplicatedResult is the deterministic native completion plus routing facts.
type ReplicatedResult struct {
	Outcome    raftserve.Outcome
	Completion []byte
	State      shardservice.ReplicatedMemberState
	Retries    int
}

func replicatedRequestDigest(command []byte) [sha256.Size]byte {
	return sha256.Sum256(command)
}

// ReplicatedPointRead selects one explicit consistency contract. A
// linearizable read is leader-only and ReadIndex-fenced. A follower read is
// bounded solely by MinimumApplied and may fall back to any replica that has
// reached that index.
type ReplicatedPointRead struct {
	Relation       replication.RelationID
	Key            []byte
	MinimumApplied uint64
	MaxValueBytes  uint32
	Linearizable   bool
}

type ReplicatedPointResult struct {
	Applied uint64
	Found   bool
	Value   []byte
	State   shardservice.ReplicatedMemberState
	Retries int
}

func (executor *ReplicatedExecutor) ReadPoint(
	ctx context.Context,
	route ReplicatedRoute,
	read ReplicatedPointRead,
) (ReplicatedPointResult, error) {
	return executor.readPoint(ctx, route, read, serviceauthz.CapabilityDataRead)
}

// ReadTopologyPoint performs the same bounded point-read protocol under the
// distinct topology capability. Catalog bytes never borrow ordinary data-read
// authority merely because they occupy a JSON relation.
func (executor *ReplicatedExecutor) ReadTopologyPoint(
	ctx context.Context,
	route ReplicatedRoute,
	read ReplicatedPointRead,
) (ReplicatedPointResult, error) {
	return executor.readPoint(ctx, route, read, serviceauthz.CapabilityTopology)
}

func (executor *ReplicatedExecutor) readPoint(
	ctx context.Context,
	route ReplicatedRoute,
	read ReplicatedPointRead,
	capability serviceauthz.Capability,
) (ReplicatedPointResult, error) {
	if executor == nil || executor.client == nil || ctx == nil || !validReplicatedRoute(route) ||
		read.Relation == 0 || read.Relation > replication.MaxRelationID ||
		len(read.Key) == 0 || len(read.Key) > replication.MaxMutationKeyBytes ||
		read.MinimumApplied == 0 || read.MaxValueBytes == 0 ||
		read.MaxValueBytes > replication.MaxMutationValueBytes {
		return ReplicatedPointResult{}, ErrReplicatedRoute
	}
	preferred := route.Replicas[0].Member
	var joined error
	for attempt := 0; attempt < executor.maxAttempts; attempt++ {
		endpoint, state, err := executor.readEndpoint(ctx, route, preferred, read, capability)
		if err != nil {
			joined = errors.Join(joined, err)
			preferred = 0
			continue
		}
		operation := shardservice.ReplicatedReadFollower
		if read.Linearizable {
			operation = shardservice.ReplicatedReadLeader
		}
		response, err := executor.doReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
			Operation: operation, Capability: capability,
			Fence: state.Fence, Relation: read.Relation,
			Key: read.Key, MinimumApplied: read.MinimumApplied, MaxValueBytes: read.MaxValueBytes,
		})
		if err != nil {
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
			return ReplicatedPointResult{}, &ReplicatedRefusalError{Code: response.Refusal}
		}
		if !validReplicatedResponseState(response) ||
			response.State.Fence.Group != route.Group ||
			response.State.Fence.AllocationGeneration != route.AllocationGeneration ||
			response.State.Fence.MemberID != endpoint.Member {
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
			continue
		}
		if response.State.Fence.Command != route.Command {
			if validReplicatedReadRefusal(
				response, shardservice.ReplicatedRefusalStaleFence,
			) {
				return ReplicatedPointResult{}, &ReplicatedRefusalError{Code: response.Refusal}
			}
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
			continue
		}
		switch response.Kind {
		case shardservice.ReplicatedReadFound, shardservice.ReplicatedReadMissing:
			if response.Refusal != shardservice.ReplicatedRefusalNone ||
				response.RequestDigest != ([sha256.Size]byte{}) ||
				response.Outcome != (raftserve.Outcome{}) || len(response.Completion) != 0 ||
				response.ReadApplied < read.MinimumApplied ||
				response.State.Applied < response.ReadApplied ||
				len(response.Value) > int(read.MaxValueBytes) ||
				(response.Kind == shardservice.ReplicatedReadMissing && len(response.Value) != 0) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				continue
			}
			return ReplicatedPointResult{Applied: response.ReadApplied,
				Found: response.Kind == shardservice.ReplicatedReadFound,
				Value: response.Value, State: response.State, Retries: attempt}, nil
		case shardservice.ReplicatedNotLeader:
			if !validReplicatedNonterminalResponse(response) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				preferred = 0
				continue
			}
			preferred = response.State.LeaderID
			joined = errors.Join(joined, raftmodel.ErrNotLeader)
		case shardservice.ReplicatedRefusal:
			if !validReplicatedReadRefusal(response, response.Refusal) {
				joined = errors.Join(joined, ErrReplicatedRoute)
				preferred = 0
				continue
			}
			if response.Refusal == shardservice.ReplicatedRefusalStaleFence {
				// A command-fence change was handled above and is a definite
				// catalog refusal. With the same command fence this is only a
				// member/term incarnation race between probe and read admission;
				// refresh the handshake within the caller's bounded attempt budget.
				joined = errors.Join(joined, &ReplicatedRefusalError{Code: response.Refusal})
				if read.Linearizable {
					preferred = response.State.LeaderID
				} else {
					preferred = endpoint.Member
				}
				if attempt+1 < executor.maxAttempts {
					if err := waitReplicatedFailoverRetry(ctx, attempt); err != nil {
						return ReplicatedPointResult{}, errors.Join(ErrReplicatedLeader, err)
					}
				}
				continue
			}
			if response.Refusal == shardservice.ReplicatedRefusalReadBehind ||
				response.Refusal == shardservice.ReplicatedRefusalReadBufferBound {
				return ReplicatedPointResult{}, &ReplicatedRefusalError{Code: response.Refusal}
			}
			joined = errors.Join(joined, &ReplicatedRefusalError{Code: response.Refusal})
			preferred = 0
		default:
			joined = errors.Join(joined, ErrReplicatedRoute)
			preferred = 0
		}
	}
	return ReplicatedPointResult{}, errors.Join(ErrReplicatedLeader, joined)
}

func (executor *ReplicatedExecutor) readEndpoint(
	ctx context.Context,
	route ReplicatedRoute,
	preferred uint64,
	read ReplicatedPointRead,
	capability serviceauthz.Capability,
) (ReplicatedEndpoint, shardservice.ReplicatedMemberState, error) {
	if read.Linearizable {
		return executor.discoverLeader(ctx, route, preferred, capability)
	}
	// Applied-bounded reads deliberately prefer followers to preserve leader
	// capacity. Every candidate is authenticated by a fresh exact probe.
	var joined error
	for _, endpoint := range route.Replicas {
		response, err := executor.doReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
			Operation: shardservice.ReplicatedProbe, Capability: capability,
			Fence: shardservice.ReplicatedFence{Group: route.Group,
				AllocationGeneration: route.AllocationGeneration},
		})
		if err != nil || response == nil || response.Kind != shardservice.ReplicatedHandshake ||
			!validReplicatedResponseState(response) ||
			!validReplicatedNonterminalResponse(response) ||
			response.State.Fence.MemberID != endpoint.Member ||
			response.State.Fence.Group != route.Group ||
			response.State.Fence.AllocationGeneration != route.AllocationGeneration ||
			response.State.Fence.Command != route.Command {
			joined = errors.Join(joined, err, ErrReplicatedRoute)
			continue
		}
		if response.State.Applied >= read.MinimumApplied &&
			response.State.Fence.MemberID != response.State.LeaderID {
			return endpoint, response.State, nil
		}
	}
	// A leader is also a valid index-bounded replica.
	return executor.discoverLeader(ctx, route, preferred, capability)
}

// ReplicatedRefusalError preserves one typed shard refusal.
type ReplicatedRefusalError struct {
	Code    shardservice.ReplicatedRefusalCode
	Outcome raftserve.Outcome
}

func (e *ReplicatedRefusalError) Error() string {
	return fmt.Sprintf("gateway: replicated shard refusal %d", e.Code)
}

func (e *ReplicatedRefusalError) Unwrap() error {
	if e.Code == shardservice.ReplicatedRefusalDeterministic {
		return e.Outcome.Err()
	}
	if e.Code == shardservice.ReplicatedRefusalAdmissionBound {
		return raftmodel.ErrAdmissionBound
	}
	if e.Code == shardservice.ReplicatedRefusalStaleFence {
		return raftservice.ErrServingFence
	}
	if e.Code == shardservice.ReplicatedRefusalProposalRefused {
		return raftserve.ErrProposalRefused
	}
	if e.Code == shardservice.ReplicatedRefusalMembershipUnauthorized {
		return raftservice.ErrMembershipUnauthorized
	}
	if e.Code == shardservice.ReplicatedRefusalMembershipStale {
		return raftservice.ErrMembershipStale
	}
	if e.Code == shardservice.ReplicatedRefusalMembershipMalformed {
		return raftservice.ErrMembershipMalformed
	}
	if e.Code == shardservice.ReplicatedRefusalMembershipNotCaughtUp {
		return raftservice.ErrMembershipNotCaughtUp
	}
	if e.Code == shardservice.ReplicatedRefusalReadBehind {
		return ErrReplicatedReadBehind
	}
	if e.Code == shardservice.ReplicatedRefusalReadBufferBound {
		return ErrReplicatedReadBufferBound
	}
	if e.Code == shardservice.ReplicatedRefusalUnauthorized {
		return ErrReplicatedUnauthorized
	}
	return ErrReplicatedLeader
}

// ReplicatedMembershipResult means the leader accepted one exact control
// request. Applied membership remains an observation barrier: the controller
// must wait for ExpectedReplicaSetVersion to advance before the next step.
type ReplicatedMembershipResult struct {
	State           shardservice.ReplicatedMemberState
	Retries         int
	TransferWitness MembershipTransferWitness
}

type MembershipTransferWitness struct {
	TargetMember uint64
	Term         uint64
}

// ObserveMembershipTransfer resolves a prior outcome-unknown transfer without
// resending the transfer command. Success is an exact barrier: target is the
// observed leader and its term is newer than the source term observed before
// the original request. A controller must carry the returned term into the
// subsequent remove-voter request.
func (executor *ReplicatedExecutor) ObserveMembershipTransfer(
	ctx context.Context,
	route ReplicatedRoute,
	target, afterTerm uint64,
) (ReplicatedMembershipResult, error) {
	if executor == nil || executor.client == nil || ctx == nil ||
		!validReplicatedRoute(route) || target == 0 || afterTerm == 0 {
		return ReplicatedMembershipResult{}, ErrReplicatedRoute
	}
	result, witnessed := executor.observeMembershipTransfer(ctx, route, target, afterTerm)
	if !witnessed {
		return ReplicatedMembershipResult{}, raftservice.ErrOutcomeUnknown
	}
	result.TransferWitness = MembershipTransferWitness{TargetMember: target,
		Term: result.State.Fence.Term}
	return result, nil
}

// ApplyMembership routes one fixed-width metadata-authorized transition. It
// retries only definite NotLeader responses. Any transport failure after the
// write begins remains outcome-unknown and is returned immediately for the
// controller to resolve from replicated membership state.
func (executor *ReplicatedExecutor) ApplyMembership(
	ctx context.Context,
	route ReplicatedRoute,
	membership shardservice.ReplicatedMembershipRequest,
) (ReplicatedMembershipResult, error) {
	if executor == nil || executor.client == nil || ctx == nil || !validReplicatedRoute(route) {
		return ReplicatedMembershipResult{}, ErrReplicatedRoute
	}
	if err := raftservice.ValidateMembershipFields(
		membership.Kind, membership.TransitionID, membership.MetadataEpoch,
		membership.CatalogGeneration, membership.ExpectedReplicaSetVersion,
		membership.SourceMember, membership.TargetMember, membership.TransferTerm,
	); err != nil {
		return ReplicatedMembershipResult{}, err
	}
	if membership.ExpectedReplicaSetVersion != route.Command.ReplicaSetVersion {
		return ReplicatedMembershipResult{}, ErrReplicatedRoute
	}
	preferred := route.Replicas[0].Member
	for attempt := 0; attempt < executor.maxAttempts; attempt++ {
		endpoint, state, err := executor.discoverLeader(ctx, route, preferred,
			serviceauthz.CapabilityMembership)
		if err != nil {
			return ReplicatedMembershipResult{}, err
		}
		response, err := executor.doReplicated(ctx, endpoint,
			&shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedMembership,
				Capability: serviceauthz.CapabilityMembership,
				Fence:      state.Fence, Membership: membership})
		if err != nil {
			if membership.Kind == raftservice.MembershipTransferLeader {
				if witness, observeErr := executor.ObserveMembershipTransfer(
					ctx, route, membership.TargetMember, state.Fence.Term,
				); observeErr == nil {
					witness.Retries += attempt
					return witness, nil
				}
			}
			return ReplicatedMembershipResult{}, errors.Join(raftservice.ErrOutcomeUnknown, err)
		}
		// Authorization runs before proposal admission and deliberately returns
		// no member state. It is therefore a definite refusal, not an unknown
		// membership outcome and not a route-integrity failure.
		if validReplicatedUnauthorizedWithoutState(response) {
			return ReplicatedMembershipResult{}, &ReplicatedRefusalError{
				Code: shardservice.ReplicatedRefusalUnauthorized,
			}
		}
		if !validReplicatedResponseState(response) ||
			response.State.Fence.Group != route.Group ||
			response.State.Fence.AllocationGeneration != route.AllocationGeneration ||
			response.State.Fence.MemberID != endpoint.Member {
			return ReplicatedMembershipResult{}, errors.Join(raftservice.ErrOutcomeUnknown,
				ErrReplicatedRoute)
		}
		switch response.Kind {
		case shardservice.ReplicatedMembershipAccepted:
			if !validReplicatedNonterminalResponse(response) {
				return ReplicatedMembershipResult{}, errors.Join(raftservice.ErrOutcomeUnknown,
					ErrReplicatedRoute)
			}
			result := ReplicatedMembershipResult{State: response.State, Retries: attempt}
			if membership.Kind == raftservice.MembershipTransferLeader {
				witness, observeErr := executor.ObserveMembershipTransfer(
					ctx, route, membership.TargetMember, state.Fence.Term,
				)
				if observeErr != nil {
					return ReplicatedMembershipResult{}, raftservice.ErrOutcomeUnknown
				}
				result.State = witness.State
				result.TransferWitness = witness.TransferWitness
			}
			return result, nil
		case shardservice.ReplicatedNotLeader:
			if !validReplicatedNonterminalResponse(response) {
				return ReplicatedMembershipResult{}, errors.Join(raftservice.ErrOutcomeUnknown,
					ErrReplicatedRoute)
			}
			preferred = response.State.LeaderID
			continue
		case shardservice.ReplicatedOutcomeUnknown:
			if membership.Kind == raftservice.MembershipTransferLeader {
				if witness, observeErr := executor.ObserveMembershipTransfer(
					ctx, route, membership.TargetMember, state.Fence.Term,
				); observeErr == nil {
					witness.Retries += attempt
					return witness, nil
				}
			}
			return ReplicatedMembershipResult{}, raftservice.ErrOutcomeUnknown
		case shardservice.ReplicatedRefusal:
			if !validReplicatedWritePreAdmissionRefusal(response, response.Refusal, true) {
				return ReplicatedMembershipResult{}, errors.Join(raftservice.ErrOutcomeUnknown,
					ErrReplicatedRoute)
			}
			return ReplicatedMembershipResult{}, &ReplicatedRefusalError{Code: response.Refusal}
		default:
			return ReplicatedMembershipResult{}, errors.Join(raftservice.ErrOutcomeUnknown,
				ErrReplicatedRoute)
		}
	}
	return ReplicatedMembershipResult{}, ErrReplicatedLeader
}

func (executor *ReplicatedExecutor) observeMembershipTransfer(
	ctx context.Context,
	route ReplicatedRoute,
	target, afterTerm uint64,
) (ReplicatedMembershipResult, bool) {
	preferred := target
	for attempt := 0; attempt < executor.maxAttempts; attempt++ {
		endpoint, state, err := executor.discoverLeader(ctx, route, preferred,
			serviceauthz.CapabilityMembership)
		if err == nil && endpoint.Member == target && state.LeaderID == target &&
			state.Fence.MemberID == target && state.Fence.Term > afterTerm {
			return ReplicatedMembershipResult{State: state, Retries: attempt}, true
		}
		preferred = target
	}
	return ReplicatedMembershipResult{}, false
}

// Propose discovers the live leader, submits the canonical command, and uses
// at most MaxAttempts proposals. NotLeader and outcome-unknown retries always
// resend the byte-identical envelope; deterministic refusals are never retried.
func (executor *ReplicatedExecutor) Propose(
	ctx context.Context,
	route ReplicatedRoute,
	command []byte,
) (ReplicatedResult, error) {
	return executor.propose(ctx, route, command, nil, false, serviceauthz.CapabilityDataWrite)
}

// ProposeTopology submits an exact replicated catalog or controller-journal
// command under the topology capability. It shares the byte-identical retry
// protocol with ordinary proposals but cannot borrow DataWrite authority.
func (executor *ReplicatedExecutor) ProposeTopology(
	ctx context.Context,
	route ReplicatedRoute,
	command []byte,
) (ReplicatedResult, error) {
	return executor.propose(ctx, route, command, nil, false, serviceauthz.CapabilityTopology)
}

// RetryUnknown retries exact bytes retained from an earlier UnknownOutcomeError.
// Pre-admission refusals cannot resolve that earlier attempt; only a validated
// completion or an applied deterministic refusal may settle the command.
func (executor *ReplicatedExecutor) RetryUnknown(
	ctx context.Context,
	route ReplicatedRoute,
	command []byte,
) (ReplicatedResult, error) {
	return executor.propose(ctx, route, command, nil, true, serviceauthz.CapabilityDataWrite)
}

// RetryTopologyUnknown retries the exact topology command bytes retained after
// an outcome-unknown attempt without changing the requested capability.
func (executor *ReplicatedExecutor) RetryTopologyUnknown(
	ctx context.Context,
	route ReplicatedRoute,
	command []byte,
) (ReplicatedResult, error) {
	return executor.propose(ctx, route, command, nil, true, serviceauthz.CapabilityTopology)
}

func (executor *ReplicatedExecutor) propose(
	ctx context.Context,
	route ReplicatedRoute,
	command []byte,
	hint *shardservice.ReplicatedMemberState,
	priorUnknown bool,
	capability serviceauthz.Capability,
) (ReplicatedResult, error) {
	if executor == nil || executor.client == nil || ctx == nil ||
		!validReplicatedRoute(route) || len(command) == 0 ||
		len(command) > replication.MaxCommandBytes || !commandMatchesRoute(command, route) {
		return ReplicatedResult{}, ErrReplicatedRoute
	}
	original := command[:len(command):len(command)]
	requestDigest := replicatedRequestDigest(original)
	preferred := route.Replicas[0].Member
	var lastUnknown error
	if priorUnknown {
		lastUnknown = raftservice.ErrOutcomeUnknown
	}
	hintPending := hint != nil
	for attempt := 0; attempt < executor.maxAttempts; attempt++ {
		var endpoint ReplicatedEndpoint
		var state shardservice.ReplicatedMemberState
		var err error
		if hintPending {
			hintPending = false
			state = *hint
			endpoint, _, _ = replicatedEndpoint(route, state.Fence.MemberID)
			if !validReplicatedLeaderHint(route, endpoint, state) {
				state = shardservice.ReplicatedMemberState{}
			}
		}
		if state == (shardservice.ReplicatedMemberState{}) {
			endpoint, state, err = executor.discoverLeader(ctx, route, preferred, capability)
			if err != nil {
				if lastUnknown != nil {
					lastUnknown = errors.Join(lastUnknown, err)
					if attempt+1 == executor.maxAttempts || context.Cause(ctx) != nil {
						return ReplicatedResult{}, &raftservice.UnknownOutcomeError{
							Command: append([]byte(nil), original...), Cause: lastUnknown,
						}
					}
					preferred = nextReplicatedMember(route, preferred)
					if waitErr := waitReplicatedFailoverRetry(ctx, attempt); waitErr != nil {
						return ReplicatedResult{}, &raftservice.UnknownOutcomeError{
							Command: append([]byte(nil), original...),
							Cause:   errors.Join(lastUnknown, waitErr),
						}
					}
					continue
				}
				return ReplicatedResult{}, err
			}
		}
		preferred = state.LeaderID
		response, err := executor.doReplicated(ctx, endpoint,
			&shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedPropose, Capability: capability,
				Fence:   state.Fence,
				Command: original,
			})
		if err != nil {
			// A transport implementation might not wrap a proposal failure;
			// fail closed because the complete command could have reached admission.
			lastUnknown = errors.Join(lastUnknown, err)
			preferred = nextReplicatedMember(route, endpoint.Member)
			if attempt+1 != executor.maxAttempts {
				if waitErr := waitReplicatedFailoverRetry(ctx, attempt); waitErr != nil {
					return ReplicatedResult{}, &raftservice.UnknownOutcomeError{
						Command: append([]byte(nil), original...),
						Cause:   errors.Join(lastUnknown, waitErr),
					}
				}
			}
			continue
		}
		if !validReplicatedWriteResponseFields(response) {
			lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
			continue
		}
		// Unavailable without a member state is a definite owner-probe failure
		// only when no earlier attempt could have been admitted. Once an outcome
		// is unknown, no later pre-admission response can resolve it.
		if validReplicatedUnavailableWithoutState(response) {
			if lastUnknown != nil {
				continue
			}
			return ReplicatedResult{}, &ReplicatedRefusalError{
				Code: response.Refusal, Outcome: response.Outcome,
			}
		}
		if validReplicatedUnauthorizedWithoutState(response) {
			if lastUnknown != nil {
				continue
			}
			return ReplicatedResult{}, &ReplicatedRefusalError{Code: response.Refusal}
		}
		if !validReplicatedResponseState(response) ||
			response.State.Fence.Group != route.Group ||
			response.State.Fence.AllocationGeneration != route.AllocationGeneration ||
			response.State.Fence.MemberID != endpoint.Member {
			lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
			continue
		}
		if response.State.Fence.Command != route.Command {
			// A typed stale-fence refusal is definite pre-admission and may
			// legitimately carry the newly installed command contract. Every
			// other mismatched post-proposal response remains outcome-unknown.
			if validReplicatedWritePreAdmissionRefusal(
				response, shardservice.ReplicatedRefusalStaleFence, false,
			) && lastUnknown == nil {
				return ReplicatedResult{}, &ReplicatedRefusalError{
					Code: response.Refusal, Outcome: response.Outcome,
				}
			}
			if lastUnknown == nil {
				lastUnknown = ErrReplicatedRoute
			}
			continue
		}
		switch response.Kind {
		case shardservice.ReplicatedCompletion:
			commandView, commandErr := replication.OpenCommand(original)
			completion, completionErr := replication.OpenCompletion(response.Completion)
			if commandErr != nil || completionErr != nil ||
				response.RequestDigest != requestDigest ||
				response.Refusal != shardservice.ReplicatedRefusalNone ||
				response.Outcome.Code != raftserve.OutcomeCompletion ||
				response.Outcome.AppliedIndex == 0 ||
				response.Outcome.CompletionBytes != len(response.Completion) ||
				!nativeCompletionMatches(commandView, completion) ||
				completion.AppliedSequence != response.Outcome.CompletionAppliedSequence ||
				response.State.Applied < response.Outcome.AppliedIndex {
				lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
				continue
			}
			return ReplicatedResult{
				Outcome: response.Outcome, Completion: response.Completion,
				State: response.State, Retries: attempt,
			}, nil
		case shardservice.ReplicatedNotLeader:
			if !validReplicatedNonterminalResponse(response) {
				lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
				continue
			}
			preferred = response.State.LeaderID
			if preferred != 0 && preferred != endpoint.Member {
				continue
			}
			preferred = nextReplicatedMember(route, endpoint.Member)
			if attempt+1 != executor.maxAttempts {
				if err := waitReplicatedFailoverRetry(ctx, attempt); err != nil {
					if lastUnknown != nil {
						return ReplicatedResult{}, &raftservice.UnknownOutcomeError{
							Command: append([]byte(nil), original...),
							Cause:   errors.Join(lastUnknown, err),
						}
					}
					return ReplicatedResult{}, errors.Join(ErrReplicatedLeader, err)
				}
			}
			continue
		case shardservice.ReplicatedOutcomeUnknown:
			if !validReplicatedNonterminalResponse(response) {
				lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
				continue
			}
			lastUnknown = errors.Join(lastUnknown, raftservice.ErrOutcomeUnknown)
			preferred = response.State.LeaderID
			if preferred != 0 && preferred != endpoint.Member {
				continue
			}
			preferred = nextReplicatedMember(route, endpoint.Member)
			if attempt+1 != executor.maxAttempts {
				if err := waitReplicatedFailoverRetry(ctx, attempt); err != nil {
					return ReplicatedResult{}, &raftservice.UnknownOutcomeError{
						Command: append([]byte(nil), original...),
						Cause:   errors.Join(lastUnknown, err),
					}
				}
			}
			continue
		case shardservice.ReplicatedRefusal:
			if response.Refusal == shardservice.ReplicatedRefusalDeterministic {
				if response.RequestDigest == requestDigest &&
					validReplicatedAppliedRefusal(response) {
					return ReplicatedResult{}, &ReplicatedRefusalError{
						Code: response.Refusal, Outcome: response.Outcome,
					}
				}
				lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
				continue
			}
			if response.Refusal == shardservice.ReplicatedRefusalStaleFence {
				// The command contract was checked above. With the same contract,
				// this is a definite pre-admission term/member incarnation race
				// between leader discovery and proposal admission. Refresh the
				// leader and resend the byte-identical command within the bounded
				// attempt budget. A changed command fence remains a definite
				// serving-fence refusal in the earlier branch.
				if !validReplicatedWritePreAdmissionRefusal(
					response, shardservice.ReplicatedRefusalStaleFence, false,
				) {
					lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
					continue
				}
				preferred = response.State.LeaderID
				if preferred != 0 && preferred != endpoint.Member {
					continue
				}
				preferred = nextReplicatedMember(route, endpoint.Member)
				if attempt+1 == executor.maxAttempts {
					continue
				}
				if err := waitReplicatedFailoverRetry(ctx, attempt); err != nil {
					if lastUnknown != nil {
						return ReplicatedResult{}, &raftservice.UnknownOutcomeError{
							Command: append([]byte(nil), original...),
							Cause:   errors.Join(lastUnknown, err),
						}
					}
					return ReplicatedResult{}, errors.Join(ErrReplicatedLeader, err)
				}
				continue
			}
			if !validReplicatedWritePreAdmissionRefusal(response, response.Refusal, false) {
				lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
				continue
			}
			if lastUnknown != nil {
				continue
			}
			return ReplicatedResult{}, &ReplicatedRefusalError{
				Code: response.Refusal, Outcome: response.Outcome,
			}
		default:
			lastUnknown = errors.Join(lastUnknown, ErrReplicatedRoute)
			continue
		}
	}
	if lastUnknown != nil {
		return ReplicatedResult{}, &raftservice.UnknownOutcomeError{
			Command: append([]byte(nil), original...), Cause: lastUnknown,
		}
	}
	return ReplicatedResult{}, ErrReplicatedLeader
}

func waitReplicatedFailoverRetry(ctx context.Context, attempt int) error {
	// Failover is exceptional, so spend a small bounded wall-clock budget to
	// avoid burning every retry while the replacement term is still settling.
	// The normal proposal path never creates a timer. Twenty milliseconds keeps a
	// one-race retry responsive. The shipped eight-attempt executor waits at
	// most 1.26 s, spanning the harness's 950 ms maximum randomized election
	// timeout with scheduler margin; the absolute sixteen-attempt configuration
	// remains below 4 s.
	delay := replicatedFailoverRetryDelay(attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func replicatedFailoverRetryDelay(attempt int) time.Duration {
	shift := attempt
	if shift < 0 {
		shift = 0
	} else if shift > 4 {
		shift = 4
	}
	return 20 * time.Millisecond * time.Duration(uint64(1)<<uint(shift))
}

func (executor *ReplicatedExecutor) doReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, executor.attemptTimeout)
	defer cancel()
	forwarded := request
	if authority, ok := serviceauthz.FromContext(ctx); ok {
		copy := *request
		copy.Authority = authority
		forwarded = &copy
	}
	response, err := executor.client.DoReplicated(attemptCtx, endpoint, forwarded)
	if err == nil && response != nil && response.HasState &&
		(response.State.Fence.MemberID != endpoint.Member ||
			response.State.Fence.StoreID != endpoint.StoreID ||
			response.State.Fence.NodeIncarnation != endpoint.NodeIncarnation) {
		return nil, ErrReplicatedRoute
	}
	return response, err
}

func validReplicatedLeaderHint(
	route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
	state shardservice.ReplicatedMemberState,
) bool {
	return endpoint.Member != 0 && endpoint.Member == state.Fence.MemberID &&
		validReplicatedMemberState(state) &&
		state.LeaderID == state.Fence.MemberID &&
		state.Fence.Group == route.Group &&
		state.Fence.AllocationGeneration == route.AllocationGeneration &&
		state.Fence.Command == route.Command && state.Fence.Term != 0
}

func (executor *ReplicatedExecutor) discoverLeader(
	ctx context.Context,
	route ReplicatedRoute,
	preferred uint64,
	capability serviceauthz.Capability,
) (ReplicatedEndpoint, shardservice.ReplicatedMemberState, error) {
	visited := uint64(0)
	member := preferred
	var joined error
	for probes := 0; probes < len(route.Replicas); probes++ {
		endpoint, ordinal, ok := replicatedEndpoint(route, member)
		if !ok || visited&(uint64(1)<<ordinal) != 0 {
			endpoint, ordinal, ok = firstUnvisitedReplicatedEndpoint(route, visited)
			if !ok {
				break
			}
		}
		visited |= uint64(1) << ordinal
		response, err := executor.doReplicated(ctx, endpoint,
			&shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedProbe, Capability: capability,
				Fence: shardservice.ReplicatedFence{
					Group: route.Group, AllocationGeneration: route.AllocationGeneration,
				},
			})
		if err != nil {
			joined = errors.Join(joined, err)
			member = 0
			continue
		}
		if validReplicatedUnauthorizedWithoutState(response) {
			return ReplicatedEndpoint{}, shardservice.ReplicatedMemberState{},
				&ReplicatedRefusalError{Code: response.Refusal}
		}
		if response == nil || response.Kind != shardservice.ReplicatedHandshake ||
			!validReplicatedResponseState(response) ||
			!validReplicatedNonterminalResponse(response) ||
			response.State.Fence.Group != route.Group ||
			response.State.Fence.AllocationGeneration != route.AllocationGeneration ||
			response.State.Fence.Command != route.Command ||
			response.State.Fence.MemberID != endpoint.Member {
			joined = errors.Join(joined, ErrReplicatedRoute)
			member = 0
			continue
		}
		if response.State.LeaderID == response.State.Fence.MemberID {
			return endpoint, response.State, nil
		}
		member = response.State.LeaderID
	}
	return ReplicatedEndpoint{}, shardservice.ReplicatedMemberState{},
		errors.Join(ErrReplicatedLeader, joined)
}

func validReplicatedResponseState(response *shardservice.ReplicatedResponse) bool {
	return response != nil && response.HasState &&
		validReplicatedMemberState(response.State)
}

func validReplicatedMemberState(state shardservice.ReplicatedMemberState) bool {
	return validReplicatedCatalogGroup(state.Fence.Group) &&
		state.Fence.AllocationGeneration != 0 && state.Fence.Command.Valid() &&
		state.Fence.MemberID != 0 && state.Fence.StoreID != ([16]byte{}) &&
		state.Fence.NodeIncarnation != 0 && state.Fence.Term != 0 &&
		state.Commit >= state.Applied && state.Applied >= state.CheckpointApplied
}

func validReplicatedNonterminalResponse(response *shardservice.ReplicatedResponse) bool {
	return response != nil && response.Refusal == shardservice.ReplicatedRefusalNone &&
		response.RequestDigest == ([sha256.Size]byte{}) &&
		response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
		response.ReadApplied == 0 && len(response.Value) == 0
}

func validReplicatedWriteResponseFields(response *shardservice.ReplicatedResponse) bool {
	return response != nil && response.ReadApplied == 0 && len(response.Value) == 0
}

func validReplicatedUnavailableWithoutState(
	response *shardservice.ReplicatedResponse,
) bool {
	return response != nil && response.Kind == shardservice.ReplicatedRefusal &&
		response.Refusal == shardservice.ReplicatedRefusalUnavailable &&
		!response.HasState && response.State == (shardservice.ReplicatedMemberState{}) &&
		response.RequestDigest == ([sha256.Size]byte{}) &&
		response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
		response.ReadApplied == 0 && len(response.Value) == 0
}

func validReplicatedUnauthorizedWithoutState(
	response *shardservice.ReplicatedResponse,
) bool {
	return response != nil && response.Kind == shardservice.ReplicatedRefusal &&
		response.Refusal == shardservice.ReplicatedRefusalUnauthorized &&
		!response.HasState && response.State == (shardservice.ReplicatedMemberState{}) &&
		response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
		response.ReadApplied == 0 && len(response.Value) == 0
}

func validReplicatedWritePreAdmissionRefusal(
	response *shardservice.ReplicatedResponse,
	code shardservice.ReplicatedRefusalCode,
	membership bool,
) bool {
	if response == nil || response.Kind != shardservice.ReplicatedRefusal ||
		response.Refusal != code || response.Outcome != (raftserve.Outcome{}) ||
		response.RequestDigest != ([sha256.Size]byte{}) ||
		len(response.Completion) != 0 || response.ReadApplied != 0 || len(response.Value) != 0 {
		return false
	}
	if membership {
		switch code {
		case shardservice.ReplicatedRefusalAdmissionBound,
			shardservice.ReplicatedRefusalUnavailable,
			shardservice.ReplicatedRefusalMembershipUnauthorized,
			shardservice.ReplicatedRefusalMembershipStale,
			shardservice.ReplicatedRefusalMembershipMalformed,
			shardservice.ReplicatedRefusalMembershipNotCaughtUp:
			return true
		default:
			return false
		}
	}
	switch code {
	case shardservice.ReplicatedRefusalStaleFence,
		shardservice.ReplicatedRefusalAdmissionBound,
		shardservice.ReplicatedRefusalProposalRefused,
		shardservice.ReplicatedRefusalUnavailable,
		shardservice.ReplicatedRefusalUnauthorized:
		return true
	default:
		return false
	}
}

func validReplicatedReadRefusal(
	response *shardservice.ReplicatedResponse,
	code shardservice.ReplicatedRefusalCode,
) bool {
	if response == nil || response.Kind != shardservice.ReplicatedRefusal ||
		response.Refusal != code || response.Outcome != (raftserve.Outcome{}) ||
		response.RequestDigest != ([sha256.Size]byte{}) ||
		len(response.Completion) != 0 || response.ReadApplied != 0 || len(response.Value) != 0 {
		return false
	}
	switch code {
	case shardservice.ReplicatedRefusalStaleFence,
		shardservice.ReplicatedRefusalAdmissionBound,
		shardservice.ReplicatedRefusalUnavailable,
		shardservice.ReplicatedRefusalReadBehind,
		shardservice.ReplicatedRefusalReadBufferBound,
		shardservice.ReplicatedRefusalUnauthorized:
		return true
	default:
		return false
	}
}

func validReplicatedAppliedRefusal(response *shardservice.ReplicatedResponse) bool {
	if response == nil || response.Kind != shardservice.ReplicatedRefusal ||
		response.Refusal != shardservice.ReplicatedRefusalDeterministic ||
		len(response.Completion) != 0 || response.Outcome.AppliedIndex == 0 ||
		response.Outcome.CompletionAppliedSequence != 0 ||
		response.Outcome.CompletionBytes != 0 ||
		response.ReadApplied != 0 || len(response.Value) != 0 ||
		response.State.Applied < response.Outcome.AppliedIndex {
		return false
	}
	return response.Outcome.Code > raftserve.OutcomeCompletion &&
		response.Outcome.Code < raftserve.OutcomeProposalRefused
}

func validReplicatedRoute(route ReplicatedRoute) bool {
	if route.Distribution == "" || route.Shard == "" ||
		!validReplicatedCatalogGroup(route.Group) || route.AllocationGeneration == 0 ||
		!route.Command.Valid() ||
		len(route.Replicas) == 0 || len(route.Replicas) > AbsoluteMaxReplicatedRouteMembers {
		return false
	}
	for index, endpoint := range route.Replicas {
		if endpoint.Member == 0 || endpoint.Node == (rafttransport.NodeID{}) ||
			endpoint.StoreID == ([16]byte{}) || endpoint.NodeIncarnation == 0 ||
			endpoint.NativeEndpoint == "" || endpoint.Address == "" {
			return false
		}
		for prior := 0; prior < index; prior++ {
			if route.Replicas[prior].Member == endpoint.Member ||
				route.Replicas[prior].Node == endpoint.Node ||
				route.Replicas[prior].StoreID == endpoint.StoreID ||
				route.Replicas[prior].NativeEndpoint == endpoint.NativeEndpoint ||
				route.Replicas[prior].Address == endpoint.Address {
				return false
			}
		}
	}
	return true
}

func commandMatchesRoute(command []byte, route ReplicatedRoute) bool {
	view, err := replication.OpenCommand(command)
	return err == nil && view.ClusterID == route.Group.ClusterID &&
		view.ClusterIncarnation == route.Group.ClusterIncarnation &&
		view.TopologyRecoveryEpoch == route.Group.TopologyRecoveryEpoch &&
		vibejson.BytesEqualString(view.Distribution, string(route.Distribution)) &&
		vibejson.BytesEqualString(view.Shard, string(route.Shard)) &&
		view.ShardIncarnation == route.Group.ShardIncarnation &&
		view.GroupID == route.Group.GroupID &&
		view.AllocationGeneration == route.AllocationGeneration &&
		view.ReplicaSetVersion == route.Command.ReplicaSetVersion &&
		view.ActivePolicyGeneration == route.Command.ActivePolicyGeneration &&
		view.ProtectionEpoch == route.Command.ProtectionEpoch &&
		view.OwnershipEpoch == route.Command.OwnershipEpoch &&
		view.SchemaGeneration == route.Command.SchemaGeneration &&
		view.RoutingVersion == route.Command.RoutingVersion &&
		view.RouteGeneration == route.Command.RouteGeneration
}

func replicatedEndpoint(
	route ReplicatedRoute,
	member uint64,
) (ReplicatedEndpoint, int, bool) {
	for index, endpoint := range route.Replicas {
		if endpoint.Member == member {
			return endpoint, index, true
		}
	}
	return ReplicatedEndpoint{}, 0, false
}

func nextReplicatedMember(route ReplicatedRoute, member uint64) uint64 {
	_, ordinal, found := replicatedEndpoint(route, member)
	if !found || len(route.Replicas) < 2 {
		return 0
	}
	return route.Replicas[(ordinal+1)%len(route.Replicas)].Member
}

func firstUnvisitedReplicatedEndpoint(
	route ReplicatedRoute,
	visited uint64,
) (ReplicatedEndpoint, int, bool) {
	for index, endpoint := range route.Replicas {
		if visited&(uint64(1)<<index) == 0 {
			return endpoint, index, true
		}
	}
	return ReplicatedEndpoint{}, 0, false
}
