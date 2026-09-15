package main

import (
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Native predicates retain the source-certified initial binding, not the
// latest schema binding, so a recovered target must still prove the original
// ownership handoff. Published maps are immutable on the concurrent read path.
func (authority *rf3NativeAuthorities) registerEnrolled(identity raftmember.RuntimeIdentity,
	spec nodecontrol.PreparationSpec, base sqldriver.ReplicatedShardStoreIdentity,
) error {
	if authority == nil || authority.registry == nil || spec.Group != identity.Group ||
		groupFromBinding(base.Binding) != identity.Group || base.Binding.MemberID != identity.MemberID ||
		base.Binding.StoreID != identity.StoreID || base.Binding.AllocationGeneration != identity.AllocationGeneration ||
		spec.Target.MemberID != identity.MemberID || spec.TargetStoreID != identity.StoreID ||
		spec.TargetNodeIncarnation != identity.NodeIncarnation || !spec.SourceCommand.Valid() {
		return raftservice.ErrServingFence
	}
	manifest := rf3Manifest{EnrolledTarget: &rf3ManifestEnrolledTarget{MemberID: spec.Target.MemberID,
		NodeID: spec.Target.Node, StoreID: spec.TargetStoreID, NodeIncarnation: spec.TargetNodeIncarnation},
		MemberCount: rf3ManifestMembers}
	for index, member := range spec.InitialVoters {
		manifest.Members[index] = rf3ManifestMember{MemberID: member.MemberID, NodeID: member.Node}
	}
	entry := rf3NativeDynamicGroup{identity: identity, specDigest: spec.Digest(), authority: rf3NativeGroupAuthority{
		baseServing: rf3NativeServingAuthority(authority.registry, manifest, identity.Group, base),
		move:        rf3NativeMoveAuthority(authority.registry, manifest, identity.Group, base),
	}}
	authority.dynamicMu.Lock()
	defer authority.dynamicMu.Unlock()
	if _, exists := authority.groups[identity.Group]; exists {
		return raftservice.ErrServingFence
	}
	current := authority.dynamic.Load()
	count := 0
	if current != nil {
		count = len(*current)
		if prior, found := (*current)[identity.Group]; found {
			if sameRF3DonorIdentity(prior.identity, identity) && prior.specDigest == entry.specDigest {
				return nil
			}
			return raftservice.ErrServingFence
		}
	}
	if count+len(authority.groups) >= maxRF3ManifestGroups {
		return errRF3Serving
	}
	next := make(rf3NativeDynamicGroups, count+1)
	if current != nil {
		for group, prior := range *current {
			next[group] = prior
		}
	}
	next[identity.Group] = entry
	authority.dynamic.Store(&next)
	return nil
}
