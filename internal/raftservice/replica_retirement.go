package raftservice

import (
	"context"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

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
	if request.operation == ([32]byte{}) || request.step == ([32]byte{}) ||
		request.sourceMember == 0 || request.targetMember == 0 || request.sourceMember == request.targetMember ||
		member.identity.Group != fence.Group || member.identity.AllocationGeneration != fence.AllocationGeneration ||
		member.identity.MemberID != request.sourceMember || fence.MemberID != request.sourceMember ||
		member.identity.StoreID != fence.StoreID || member.identity.NodeIncarnation != fence.NodeIncarnation ||
		(member.command.SchemaGeneration == fence.Command.SchemaGeneration && member.command.RelationManifestDigest != fence.Command.RelationManifestDigest) || !fence.Command.Valid() {
		return ErrServingFence
	}
	publication, err := owner.host.Publication(request.group)
	if err != nil || publication.ReplicaSetVersion > fence.Command.ReplicaSetVersion {
		return errors.Join(err, ErrServingFence)
	}
	state, err := owner.host.SnapshotState(request.group)
	if err != nil {
		return err
	}
	b := state.Binding
	if b.ClusterID != fence.Group.ClusterID || b.ClusterIncarnation != fence.Group.ClusterIncarnation ||
		b.TopologyRecoveryEpoch != fence.Group.TopologyRecoveryEpoch || b.ShardIncarnation != fence.Group.ShardIncarnation ||
		b.GroupID != fence.Group.GroupID || b.AllocationGeneration != fence.AllocationGeneration ||
		b.Distribution != string(member.identity.Distribution) || b.Shard != string(member.identity.Shard) ||
		b.ActivePolicyGeneration > fence.Command.ActivePolicyGeneration || b.ProtectionEpoch > fence.Command.ProtectionEpoch ||
		b.SchemaGeneration > fence.Command.SchemaGeneration || b.OwnershipEpoch > fence.Command.OwnershipEpoch ||
		b.RoutingVersion > fence.Command.RoutingVersion || b.RouteGeneration > fence.Command.RouteGeneration ||
		state.ReplicaSetVersion > fence.Command.ReplicaSetVersion {
		return ErrServingFence
	}
	if !request.retirementAuthorized {
		proof := request.retirementProof
		if proof == nil || proof.fence != fence || proof.applied < fence.Command.ReplicaSetVersion ||
			proof.term < fence.Term || proof.grant.SourceMember != request.sourceMember || proof.grant.TargetMember != request.targetMember ||
			proof.binding.Distribution != b.Distribution || proof.binding.Shard != b.Shard || proof.binding.OwnedRange != b.OwnedRange ||
			owner.authority == nil {
			return ErrServingFence
		}
		grant, found, grantErr := owner.authority.CurrentTransitionGrant(request.group)
		if grantErr != nil || !found || grant != proof.grant {
			return errors.Join(grantErr, ErrMembershipUnauthorized)
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
