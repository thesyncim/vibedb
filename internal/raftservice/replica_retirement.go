package raftservice

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// retirementFenceFailure keeps the public stale-fence sentinel intact while
// retaining the first exact local mismatch in control-service diagnostics.
// A retirement proof is security-sensitive, so callers must continue to
// reject every mismatch; the context is only evidence for operators and
// tests, never an authorization decision.
func retirementFenceFailure(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrServingFence}, args...)...)
}

// ReplicaRetirementProof is detached evidence fetched independently by the
// source control service from a mutually authenticated surviving member. No
// proof fields are accepted in the gateway's retirement request. Applying a
// removal stops Raft replication to its old member, so that member need not
// learn its own removal before it can safely acknowledge local shutdown.
type ReplicaRetirementProof struct {
	grant                   membershipgrant.Grant
	fence                   ServingFence
	binding                 replicatedstate.Binding
	observer, applied, term uint64
	voters                  [3]uint64
}

func NewReplicaRetirementProof(request ReplicaRetirementRequest, grant membershipgrant.Grant,
	observation ReplicaObservation,
) (ReplicaRetirementProof, error) {
	state, publication, status := observation.State, observation.Publication, observation.Status
	if !grant.Valid() || grant.Group != request.Fence.Group || grant.SourceMember != request.SourceMember ||
		grant.TargetMember != request.TargetMember || request.Fence.MemberID != request.SourceMember ||
		!request.Fence.Command.Valid() || request.Fence.Term == 0 ||
		status.MemberID == request.SourceMember || status.Term < request.Fence.Term ||
		publication.Applied == 0 || publication.Applied < publication.ReplicaSetVersion || publication.Applied != state.Applied ||
		status.Applied != publication.Applied || status.Commit < publication.Applied ||
		publication.ReplicaSetVersion != state.ReplicaSetVersion || publication.ReplicaSetVersion <= grant.InitialReplicaSetVersion ||
		publication.ConfState == nil || state.ConfState == nil || publication.ConfState.Equivalent(state.ConfState) != nil ||
		!retirementStateMatches(state, request.Fence, request.SourceMember, request.TargetMember) ||
		len(state.ConfState.GetLearners()) != 0 || len(state.ConfState.GetVoters()) != 3 {
		return ReplicaRetirementProof{}, ErrServingFence
	}
	want := grant.InitialVoters
	for index := range want {
		if want[index] == grant.SourceMember {
			want[index] = grant.TargetMember
		}
	}
	slices.Sort(want[:])
	if !slices.Equal(want[:], state.ConfState.GetVoters()) || !slices.Contains(want[:], status.MemberID) {
		return ReplicaRetirementProof{}, ErrServingFence
	}
	return ReplicaRetirementProof{grant: grant, fence: request.Fence, binding: state.Binding,
		observer: status.MemberID, applied: publication.Applied, term: status.Term, voters: want}, nil
}

func (owner *Owner) validateProvenReplicaRetirement(request ownerRequest, member ownerMember) error {
	fence := request.fence
	switch {
	case request.operation == ([32]byte{}):
		return retirementFenceFailure("operation is zero")
	case request.step == ([32]byte{}):
		return retirementFenceFailure("step is zero")
	case request.sourceMember == 0 || request.targetMember == 0 || request.sourceMember == request.targetMember:
		return retirementFenceFailure("source/target member identity is invalid")
	case member.identity.Group != fence.Group:
		return retirementFenceFailure("source group identity differs from fence")
	case member.identity.AllocationGeneration != fence.AllocationGeneration:
		return retirementFenceFailure("source allocation differs from fence")
	case member.identity.MemberID != request.sourceMember || fence.MemberID != request.sourceMember:
		return retirementFenceFailure("source member differs from fence")
	case member.identity.StoreID != fence.StoreID:
		return retirementFenceFailure("source store differs from fence")
	case member.identity.NodeIncarnation != fence.NodeIncarnation:
		return retirementFenceFailure("source node incarnation differs from fence")
	case member.command.SchemaGeneration == fence.Command.SchemaGeneration && member.command.RelationManifestDigest != fence.Command.RelationManifestDigest:
		return retirementFenceFailure("source relation manifest differs from fence at schema %d", fence.Command.SchemaGeneration)
	case !fence.Command.Valid():
		return retirementFenceFailure("command fence is invalid")
	}
	publication, err := owner.host.Publication(request.group)
	if err != nil || publication.ReplicaSetVersion > fence.Command.ReplicaSetVersion {
		return errors.Join(err, retirementFenceFailure("publication version=%d exceeds fence version=%d", publication.ReplicaSetVersion, fence.Command.ReplicaSetVersion))
	}
	state, err := owner.host.SnapshotState(request.group)
	if err != nil {
		return err
	}
	b := state.Binding
	switch {
	case b.ClusterID != fence.Group.ClusterID || b.ClusterIncarnation != fence.Group.ClusterIncarnation:
		return retirementFenceFailure("source snapshot cluster identity differs from fence")
	case b.TopologyRecoveryEpoch != fence.Group.TopologyRecoveryEpoch:
		return retirementFenceFailure("source snapshot topology epoch differs from fence")
	case b.ShardIncarnation != fence.Group.ShardIncarnation || b.GroupID != fence.Group.GroupID:
		return retirementFenceFailure("source snapshot shard/group identity differs from fence")
	case b.AllocationGeneration != fence.AllocationGeneration:
		return retirementFenceFailure("source snapshot allocation differs from fence")
	case b.Distribution != string(member.identity.Distribution) || b.Shard != string(member.identity.Shard):
		return retirementFenceFailure("source snapshot distribution/shard differs from identity")
	case b.ActivePolicyGeneration > fence.Command.ActivePolicyGeneration:
		return retirementFenceFailure("source snapshot policy=%d exceeds fence=%d", b.ActivePolicyGeneration, fence.Command.ActivePolicyGeneration)
	case b.ProtectionEpoch > fence.Command.ProtectionEpoch:
		return retirementFenceFailure("source snapshot protection=%d exceeds fence=%d", b.ProtectionEpoch, fence.Command.ProtectionEpoch)
	case b.SchemaGeneration > fence.Command.SchemaGeneration:
		return retirementFenceFailure("source snapshot schema=%d exceeds fence=%d", b.SchemaGeneration, fence.Command.SchemaGeneration)
	case b.OwnershipEpoch > fence.Command.OwnershipEpoch:
		return retirementFenceFailure("source snapshot ownership=%d exceeds fence=%d", b.OwnershipEpoch, fence.Command.OwnershipEpoch)
	case b.RoutingVersion > fence.Command.RoutingVersion:
		return retirementFenceFailure("source snapshot routing=%d exceeds fence=%d", b.RoutingVersion, fence.Command.RoutingVersion)
	case b.RouteGeneration > fence.Command.RouteGeneration:
		return retirementFenceFailure("source snapshot route=%d exceeds fence=%d", b.RouteGeneration, fence.Command.RouteGeneration)
	case state.ReplicaSetVersion > fence.Command.ReplicaSetVersion:
		return retirementFenceFailure("source snapshot replica-set=%d exceeds fence=%d", state.ReplicaSetVersion, fence.Command.ReplicaSetVersion)
	}
	if !request.retirementAuthorized {
		proof := request.retirementProof
		switch {
		case proof == nil:
			return retirementFenceFailure("retirement proof is missing")
		case proof.fence != fence:
			return retirementFenceFailure("retirement proof fence differs from source fence")
		case proof.applied < fence.Command.ReplicaSetVersion:
			return retirementFenceFailure("retirement proof applied=%d precedes fence replica-set=%d", proof.applied, fence.Command.ReplicaSetVersion)
		case proof.term < fence.Term:
			return retirementFenceFailure("retirement proof term=%d precedes fence term=%d", proof.term, fence.Term)
		case proof.grant.SourceMember != request.sourceMember || proof.grant.TargetMember != request.targetMember:
			return retirementFenceFailure("retirement proof source/target differs from request")
		case proof.binding.Distribution != b.Distribution || proof.binding.Shard != b.Shard:
			return retirementFenceFailure("retirement proof distribution/shard differs from source")
		case proof.binding.OwnedRange != b.OwnedRange:
			return retirementFenceFailure("retirement proof owned range differs from source")
		case owner.authority == nil:
			return retirementFenceFailure("retirement proof has no membership authority")
		}
		grant, found, grantErr := owner.authority.CurrentTransitionGrant(request.group)
		if grantErr != nil || !found || grant != proof.grant {
			return errors.Join(grantErr, retirementFenceFailure("retirement proof grant is not current"), ErrMembershipUnauthorized)
		}
	}
	// A removed replica can campaign into a higher isolated term. That cannot
	// reverse a committed removal and must not block its durable local fence.
	if owner.pendingTransfers[request.group] != nil {
		return multiraft.ErrGroupBusy
	}
	return nil
}

// ValidateReplicaRetirement checks a proof and exact installed source through
// the serialized lane before the source service persists its tombstone.
func (owner *Owner) ValidateReplicaRetirement(ctx context.Context, request ReplicaRetirementRequest) error {
	if owner == nil || ctx == nil || request.Authorized {
		return ErrInvalidOwner
	}
	var proof *ReplicaRetirementProof
	if request.Proof != nil {
		copied := *request.Proof
		proof = &copied
	}
	_, err := owner.enqueue(ctx, ownerRequest{kind: requestValidateReplicaRetirement,
		group: request.Fence.Group, fence: request.Fence, operation: request.Operation, step: request.Step,
		sourceMember: request.SourceMember, targetMember: request.TargetMember, retirementProof: proof,
		reply: make(chan ownerReply, 1)})
	return err
}

func (owners *ExecutionOwners) ValidateReplicaRetirement(ctx context.Context, request ReplicaRetirementRequest) error {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return err
	}
	return owner.ValidateReplicaRetirement(ctx, request)
}
