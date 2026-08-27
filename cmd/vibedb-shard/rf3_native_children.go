package main

import (
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// This immutable reader snapshot contains only identities already authenticated
// by a successful durable child-adoption checkpoint. It grants nothing without
// an installed execution owner and matching current transport membership.
type rf3NativeChildren map[raftmember.GroupKey]raftmember.RuntimeIdentity

// The caller holds the inventory mutex and has verified the certified child
// receipt and runtime. Durable publication must precede both runtime discovery
// and native authority; uncertain publication exposes neither new entry.
func (inventory *rf3AdoptedGroupInventory) recordNativeChild(entry rf3AdoptedGroupEntry, runtime rf3AdoptedRuntime) error {
	if !inventory.nativeChildCapacity(runtime.identity) {
		return errRF3SplitChildRegistryBound
	}
	if err := inventory.record(entry); err != nil {
		return err
	}
	if inventory.runtimes == nil {
		inventory.runtimes = make(map[raftmember.GroupKey]rf3AdoptedRuntime)
	}
	inventory.runtimes[runtime.identity.Group] = runtime
	inventory.publishNativeChild(runtime.identity)
	return nil
}

// Called with the inventory mutex held, before recording any new authority.
func (inventory *rf3AdoptedGroupInventory) nativeChildCapacity(identity raftmember.RuntimeIdentity) bool {
	current := inventory.nativeChildren.Load()
	count := 0
	if current != nil {
		if previous, found := (*current)[identity.Group]; found {
			return previous == identity
		}
		count = len(*current)
	}
	return count+len(inventory.manifest.groupBundles()) < maxRF3ManifestGroups
}

// Called only after record has synced the exact CertifiedChildAdoption. Readers
// never lock the durable inventory or touch its files on the serving hot path.
func (inventory *rf3AdoptedGroupInventory) publishNativeChild(identity raftmember.RuntimeIdentity) {
	current := inventory.nativeChildren.Load()
	if current != nil {
		if previous, found := (*current)[identity.Group]; found && previous == identity {
			return
		}
	}
	next := make(rf3NativeChildren, len(inventory.runtimes))
	if current != nil {
		for group, previous := range *current {
			next[group] = previous
		}
	}
	next[identity.Group] = identity
	inventory.nativeChildren.Store(&next)
}

func (inventory *rf3AdoptedGroupInventory) nativeServing(registry *rafttransport.StaticRegistry, state raftservice.ServingState) bool {
	if inventory == nil || registry == nil || !state.Command.Valid() {
		return false
	}
	current := inventory.nativeChildren.Load()
	if current == nil {
		return false
	}
	identity, found := (*current)[state.Identity.Group]
	// ServingState comes from the registered serialized execution owner, never
	// the request. Its installed SQL generation advances both digest fields
	// together. Retain every certified physical identity field while admitting
	// that authenticated schema evolution without a second authority cache.
	identity.RelationManifestDigest = state.Command.RelationManifestDigest
	if !found || state.Identity != identity {
		return false
	}
	version, found := registry.ReplicaSetVersion(identity.Group)
	if !found || version != state.Command.ReplicaSetVersion {
		return false
	}
	role, err := registry.Role(identity.Group, identity.MemberID)
	return err == nil && role == rafttransport.MemberVoter
}
