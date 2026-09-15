package main

import (
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// The shared native listener resolves its authority by full group identity,
// just like the execution owners. Each entry retains the original exact
// membership and restore predicates; a primary-group predicate must never be
// reused for a different group's request.
type rf3NativeAuthorities struct {
	groups    map[raftmember.GroupKey]rf3NativeGroupAuthority
	registry  *rafttransport.StaticRegistry
	adopted   *rf3AdoptedGroupInventory
	dynamicMu sync.Mutex
	dynamic   atomic.Pointer[rf3NativeDynamicGroups]
}

type rf3NativeDynamicGroups map[raftmember.GroupKey]rf3NativeDynamicGroup
type rf3NativeDynamicGroup struct {
	identity   raftmember.RuntimeIdentity
	specDigest [32]byte
	authority  rf3NativeGroupAuthority
}

type rf3NativeGroupAuthority struct {
	baseServing      func(raftservice.ServingState) bool
	move             func(raftservice.ServingState, *shardservice.ReplicatedRequest) bool
	restorePreparing func(raftservice.ServingState, *shardservice.ReplicatedRequest) bool
	restoreGate      *shardservice.RestoreServingGate
}

func newRF3NativeAuthorities(registry *rafttransport.StaticRegistry, authorization *serviceauthz.Gate,
	prepared []preparedRF3Group, restoreGates map[raftmember.GroupKey]*shardservice.RestoreServingGate,
	restoreOperations map[raftmember.GroupKey][32]byte,
) (*rf3NativeAuthorities, error) {
	if registry == nil || authorization == nil || len(prepared) > maxRF3ManifestGroups {
		return nil, errRF3Serving
	}
	result := &rf3NativeAuthorities{groups: make(map[raftmember.GroupKey]rf3NativeGroupAuthority, len(prepared)), registry: registry}
	for _, item := range prepared {
		group := groupFromBinding(item.base.Binding)
		if _, duplicate := result.groups[group]; duplicate {
			return nil, errRF3Serving
		}
		local, err := registry.LocalMember(group)
		if err != nil || local != item.base.Binding.MemberID {
			return nil, errRF3Serving
		}
		entry := rf3NativeGroupAuthority{
			baseServing: rf3NativeServingAuthority(registry, item.manifest, group, item.base),
			restoreGate: restoreGates[group],
		}
		if target := item.manifest.EnrolledTarget; target != nil && target.MemberID == item.base.Binding.MemberID {
			entry.move = rf3NativeMoveAuthority(registry, item.manifest, group, item.base)
		}
		entry.restorePreparing = rf3RestoreCatalogPreparingAuthority(authorization, restoreOperations[group], group, item.base, entry.baseServing)
		result.groups[group] = entry
	}
	return result, nil
}

func (authority *rf3NativeAuthorities) serving(state raftservice.ServingState) bool {
	if authority == nil {
		return false
	}
	entry, found := authority.group(state.Identity.Group)
	if !found {
		return authority.adopted.nativeServing(authority.registry, state)
	}
	return entry.baseServing(state) && (entry.restoreGate == nil || entry.restoreGate.Allows(state))
}

func (authority *rf3NativeAuthorities) transitional(state raftservice.ServingState, request *shardservice.ReplicatedRequest) bool {
	if authority == nil {
		return false
	}
	entry, found := authority.group(state.Identity.Group)
	return found && (entry.restorePreparing != nil && entry.restorePreparing(state, request) || entry.move != nil && entry.move(state, request) &&
		(request.Capability != serviceauthz.CapabilityTopology || entry.restoreGate == nil || entry.restoreGate.Allows(state)))
}

func (authority *rf3NativeAuthorities) group(group raftmember.GroupKey) (rf3NativeGroupAuthority, bool) {
	if entry, found := authority.groups[group]; found {
		return entry, true
	}
	if dynamic := authority.dynamic.Load(); dynamic != nil {
		entry, found := (*dynamic)[group]
		return entry.authority, found
	}
	return rf3NativeGroupAuthority{}, false
}

func (authority *rf3NativeAuthorities) unregisterDynamic(identity raftmember.RuntimeIdentity) error {
	if authority == nil {
		return nil
	}
	authority.dynamicMu.Lock()
	defer authority.dynamicMu.Unlock()
	current := authority.dynamic.Load()
	if current == nil {
		return nil
	}
	entry, found := (*current)[identity.Group]
	if !found {
		return nil
	}
	if !sameRF3DonorIdentity(entry.identity, identity) {
		return raftservice.ErrServingFence
	}
	next := make(rf3NativeDynamicGroups, len(*current)-1)
	for group, entry := range *current {
		if group != identity.Group {
			next[group] = entry
		}
	}
	authority.dynamic.Store(&next)
	return nil
}
