package raftservice

import (
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// servingFencePermit is the immutable owner-side capability carried by an
// authority-backed read cut. Its fence and generation never change after
// construction; revocation is a one-way atomic edge owned by Owner. Keeping
// the generation pointer in the permit distinguishes a same-fence generation
// replacement and prevents a stale cut from borrowing a later source.
type servingFencePermit struct {
	fence      ServingFence
	generation *ownerGeneration
	revoked    atomic.Uint32
}

func (permit *servingFencePermit) valid(
	fence ServingFence, generation *ownerGeneration,
) bool {
	return permit != nil && permit.revoked.Load() == 0 &&
		permit.fence == fence && permit.generation == generation &&
		generation != nil && !generation.transitionFenced.Load() &&
		!generation.quiescing.Load()
}

func (permit *servingFencePermit) revoke() {
	if permit != nil {
		permit.revoked.Store(1)
	}
}

// ensureServingFencePermit returns the one immutable permit for the current
// owner serving epoch. It runs only on the serialized Owner and allocates on a
// new term or generation, never once per read. A stale cached permit is
// revoked before it is replaced, so a concurrent cut can only fail closed.
func (owner *Owner) ensureServingFencePermit(
	group ServingState,
) *servingFencePermit {
	if owner == nil {
		return nil
	}
	key := group.Identity.Group
	member, found := owner.members[key]
	if !found || member.generation == nil {
		return nil
	}
	if member.generation.transitionFenced.Load() || member.generation.quiescing.Load() {
		return nil
	}
	fence := group.Fence()
	if member.permit != nil && member.permit.valid(fence, member.generation) {
		return member.permit
	}
	if member.permit != nil {
		member.permit.revoke()
	}
	member.permit = &servingFencePermit{fence: fence, generation: member.generation}
	owner.members[key] = member
	return member.permit
}

// revokeServingFencePermit fences the current serving epoch before a caller
// mutates its command, generation, retirement, or removal state. Clearing the
// cached pointer ensures a later resumed epoch mints a fresh capability.
func (owner *Owner) revokeServingFencePermit(group raftmember.GroupKey) {
	if owner == nil {
		return
	}
	member, found := owner.members[group]
	if !found {
		return
	}
	if member.permit != nil {
		member.permit.revoke()
		member.permit = nil
		owner.members[group] = member
	}
}

// revokeAllServingFencePermits is the first shutdown/failure action. Runtime
// teardown and pending transfer cleanup may release or replace lower-level
// state, but no read cut may remain eligible once Owner begins stopping.
func (owner *Owner) revokeAllServingFencePermits() {
	if owner == nil {
		return
	}
	for group, member := range owner.members {
		if member.permit == nil {
			continue
		}
		member.permit.revoke()
		member.permit = nil
		owner.members[group] = member
	}
}

// storeOwnerMember publishes a new owner member cut while revoking the old
// serving capability first whenever any fence or generation identity changes.
// Owner owns the map, so this helper keeps all production metadata publication
// edges auditable and leaves manually assembled test owners compatible.
func (owner *Owner) storeOwnerMember(group raftmember.GroupKey, next ownerMember) {
	if owner == nil {
		return
	}
	if current, found := owner.members[group]; found {
		sameEpoch := current.identity == next.identity &&
			current.command == next.command &&
			current.generation == next.generation && current.retiring == next.retiring
		if sameEpoch {
			// A stale local ownerMember copy must not restore a capability
			// which an earlier explicit revoke cleared from the map.
			next.permit = current.permit
		} else {
			current.permit.revoke()
			if next.permit != nil {
				next.permit.revoke()
			}
			next.permit = nil
		}
	}
	owner.members[group] = next
}
