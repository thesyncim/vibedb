package splitcontroller

import (
	"context"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
)

// ExecutionGroupRegistrar is the exact live Multi-Raft capability used by a
// child lifecycle. AuthenticatedExecutionPeerRuntime implements it.
type ExecutionGroupRegistrar interface {
	RegisterExecutionGroup([]rafttransport.Member, raftservice.ExecutionGroup) error
}

// PreboundChildRuntimeAdopter freezes every metadata input that must not be
// reconstructed from process state after child activation. The activated
// apply object itself arrives in PreparedChildRuntime because it is minted by
// the certified child handoff and cannot be opened independently.
type PreboundChildRuntimeAdopter struct {
	registrar ExecutionGroupRegistrar
	operation OperationID
	child     uint8
	target    ChildTarget
	roster    []rafttransport.Member
	command   raftservice.CommandFence
}

func NewPreboundChildRuntimeAdopter(
	registrar ExecutionGroupRegistrar,
	operation OperationID,
	child uint8,
	target ChildTarget,
	roster []rafttransport.Member,
) (*PreboundChildRuntimeAdopter, error) {
	command, err := validateChildExecutionRoster(target, roster)
	if registrar == nil || operation == (OperationID{}) || child != target.Child || err != nil {
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	detached := slices.Clone(roster)
	slices.SortFunc(detached, compareChildExecutionMembers)
	return &PreboundChildRuntimeAdopter{
		registrar: registrar, operation: operation, child: child,
		target: cloneChildTarget(target), roster: detached, command: command,
	}, nil
}

func (adopter *PreboundChildRuntimeAdopter) AdoptSplitChild(
	ctx context.Context,
	operation OperationID,
	child uint8,
	prepared PreparedChildRuntime,
) error {
	if adopter == nil || ctx == nil || operation != adopter.operation || child != adopter.child ||
		prepared.Runtime == nil || prepared.Apply == nil {
		return ErrRuntimeStore
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	identity := prepared.Runtime.Identity()
	profile, profileErr := prepared.Apply.CapacityQualificationProfile()
	publication, publicationErr := prepared.Runtime.Publication()
	if profileErr != nil || !profile.Initialized || profile.Binding != adopter.target.SQL.Binding ||
		profile.RelationManifestDigest != adopter.command.RelationManifestDigest ||
		publicationErr != nil || !childPublicationMatchesRoster(
		publication.ReplicaSetVersion, publication.ConfState, adopter.roster,
	) ||
		!runtimeIdentityMatches(adopter.target, identity) ||
		identity.RelationManifestDigest != adopter.command.RelationManifestDigest {
		return errors.Join(ErrTopologyConflict, profileErr, publicationErr)
	}
	return adopter.registrar.RegisterExecutionGroup(adopter.roster, raftservice.ExecutionGroup{
		Runtime: prepared.Runtime, Identity: identity, Command: adopter.command,
		Read: prepared.Apply, Recovery: prepared.Apply,
	})
}

func childPublicationMatchesRoster(
	version uint64,
	conf *pb.ConfState,
	roster []rafttransport.Member,
) bool {
	if version == 0 || conf == nil || len(roster) != gateway.ServingReplicaCount ||
		len(conf.GetVoters()) != len(roster) || len(conf.GetLearners()) != 0 ||
		len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 ||
		conf.GetAutoLeave() {
		return false
	}
	for _, member := range roster {
		if member.ReplicaSetVersion != version ||
			!slices.Contains(conf.GetVoters(), member.MemberID) {
			return false
		}
	}
	return true
}

func validateChildExecutionRoster(
	target ChildTarget,
	roster []rafttransport.Member,
) (raftservice.CommandFence, error) {
	group := raftmember.GroupKey{
		ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
		TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
		ShardIncarnation:      target.WAL.ShardIncarnation, GroupID: target.WAL.GroupID,
	}
	if target.Child >= autosplit.MaxSplitChildren || group == (raftmember.GroupKey{}) ||
		target.WAL.MemberID == 0 || target.WAL.StoreID == ([16]byte{}) ||
		len(roster) != gateway.ServingReplicaCount ||
		target.SQL.RelationManifestDigest == ([32]byte{}) {
		return raftservice.CommandFence{}, ErrRuntimeStore
	}
	version := roster[0].ReplicaSetVersion
	if version != target.ReplicaSetVersion {
		return raftservice.CommandFence{}, ErrRuntimeStore
	}
	foundLocal := false
	for index := range roster {
		member := roster[index]
		if member.Group != group || member.ReplicaSetVersion != version || version == 0 ||
			member.MemberID == 0 || member.Node == (rafttransport.NodeID{}) ||
			member.Role != rafttransport.MemberVoter {
			return raftservice.CommandFence{}, ErrRuntimeStore
		}
		if member.MemberID == target.WAL.MemberID {
			if foundLocal {
				return raftservice.CommandFence{}, ErrRuntimeStore
			}
			foundLocal = true
		}
		matchedTarget := false
		for _, replica := range target.Replicas {
			if replica.Member == member.MemberID && replica.Node == member.Node {
				matchedTarget = true
				break
			}
		}
		if !matchedTarget {
			return raftservice.CommandFence{}, ErrRuntimeStore
		}
		for prior := 0; prior < index; prior++ {
			if roster[prior].MemberID == member.MemberID || roster[prior].Node == member.Node {
				return raftservice.CommandFence{}, ErrRuntimeStore
			}
		}
	}
	command := raftservice.CommandFence{
		ReplicaSetVersion:      version,
		ActivePolicyGeneration: target.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        target.Authority.ProtectionEpoch,
		OwnershipEpoch:         target.Authority.OwnershipEpoch,
		SchemaGeneration:       target.Authority.SchemaGeneration,
		RelationManifestDigest: target.SQL.RelationManifestDigest,
		RoutingVersion:         target.Authority.RoutingVersion,
		RouteGeneration:        target.Authority.RouteGeneration,
	}
	if !foundLocal || len(target.Replicas) != len(roster) || !command.Valid() {
		return raftservice.CommandFence{}, ErrRuntimeStore
	}
	return command, nil
}

func compareChildExecutionMembers(left, right rafttransport.Member) int {
	if left.MemberID < right.MemberID {
		return -1
	}
	if left.MemberID > right.MemberID {
		return 1
	}
	return 0
}
