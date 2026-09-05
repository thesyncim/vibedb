package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Reload is append-only and local-operator authorized. It consumes already
// prepared immutable artifacts, never creates storage or replaces a live
// group. Existing query routes and execution lanes are unchanged.
func validateRF3GroupAppend(current, next rf3Manifest) error {
	old, all := current.groupBundles(), next.groupBundles()
	if len(old) == 0 || len(all) < len(old) || len(all) > maxRF3ManifestGroups || current.DevelopmentOnly || next.DevelopmentOnly {
		return errInvalidRF3Manifest
	}
	if current.ReadAuthority != nil && len(all) > len(old) {
		return fmt.Errorf("%w: read-authority groups require a restart before append", errInvalidRF3Manifest)
	}
	if err := validateRF3GroupRosterUnion(all); err != nil {
		return err
	}
	if next.Gateway != nil {
		if _, err := rf3EmbeddedGatewayPeers(next, next.Gateway.ShardPeers); err != nil {
			return err
		}
	}
	a, b := current.withGroup(old[0]), next.withGroup(all[0])
	a.Digest, b.Digest = [32]byte{}, [32]byte{}
	a.SplitControl.MaxOperations = a.SplitControl.operationLimit()
	b.SplitControl.MaxOperations = b.SplitControl.operationLimit()
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("%w: reload changes process configuration", errInvalidRF3Manifest)
	}
	for i := range old {
		if !reflect.DeepEqual(old[i], all[i]) {
			return fmt.Errorf("%w: reload replaces a retained group", errInvalidRF3Manifest)
		}
	}
	groups := make(map[raftmember.GroupKey]bool, len(all))
	paths := make(map[string]bool, 2*len(all))
	for _, group := range all {
		if groups[group.Route.Group] || paths[group.WAL.Path] || paths[group.SQL.Path] || group.WAL.Path == group.SQL.Path ||
			group.EnrolledTarget != nil {
			return errInvalidRF3Manifest
		}
		groups[group.Route.Group], paths[group.WAL.Path], paths[group.SQL.Path] = true, true, true
	}
	return nil
}

func validateRF3GroupTransition(current, next rf3Manifest) error {
	old, all := current.groupBundles(), next.groupBundles()
	if len(old) == 0 || len(all) == 0 || len(all) > maxRF3ManifestGroups ||
		current.DevelopmentOnly || next.DevelopmentOnly {
		return errInvalidRF3Manifest
	}
	if current.ReadAuthority != nil && len(all) > len(old) {
		return fmt.Errorf("%w: read-authority groups require a restart before append", errInvalidRF3Manifest)
	}
	if err := validateRF3GroupRosterUnion(all); err != nil {
		return err
	}
	if next.Gateway != nil {
		if _, err := rf3EmbeddedGatewayPeers(next, next.Gateway.ShardPeers); err != nil {
			return err
		}
	}
	a, b := current.withGroup(old[0]), next.withGroup(all[0])
	a.Digest, b.Digest = [32]byte{}, [32]byte{}
	a.SplitControl.MaxOperations = a.SplitControl.operationLimit()
	b.SplitControl.MaxOperations = b.SplitControl.operationLimit()
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("%w: reload changes process configuration", errInvalidRF3Manifest)
	}
	oldByGroup := make(map[raftmember.GroupKey]rf3ManifestGroup, len(old))
	for _, bundle := range old {
		oldByGroup[bundle.Route.Group] = bundle
	}
	added, retained := 0, 0
	groups := make(map[raftmember.GroupKey]bool, len(all))
	paths := make(map[string]bool, 2*len(all))
	for _, bundle := range all {
		if groups[bundle.Route.Group] || paths[bundle.WAL.Path] || paths[bundle.SQL.Path] || bundle.WAL.Path == bundle.SQL.Path ||
			bundle.EnrolledTarget != nil {
			return errInvalidRF3Manifest
		}
		groups[bundle.Route.Group], paths[bundle.WAL.Path], paths[bundle.SQL.Path] = true, true, true
		if previous, found := oldByGroup[bundle.Route.Group]; found {
			if !reflect.DeepEqual(previous, bundle) {
				return fmt.Errorf("%w: reload replaces a retained group", errInvalidRF3Manifest)
			}
			retained++
		} else {
			added++
		}
	}
	removed := len(old) - retained
	if added != 0 && removed != 0 || removed != 0 && all[0].Route.Group != old[0].Route.Group {
		return errInvalidRF3Manifest
	}
	// Group ordinals are durable child-admission authority. Additions must be
	// a suffix; retained groups must preserve order during retirement too.
	if removed == 0 {
		for index := range old {
			if old[index].Route.Group != all[index].Route.Group {
				return fmt.Errorf("%w: reload reorders retained groups", errInvalidRF3Manifest)
			}
		}
	} else {
		index := 0
		for _, previous := range old {
			if index < len(all) && previous.Route.Group == all[index].Route.Group {
				index++
			}
		}
		if index != len(all) {
			return fmt.Errorf("%w: reload reorders retained groups", errInvalidRF3Manifest)
		}
	}
	return nil
}

// validateRF3GroupRosterUnion qualifies reloads for physical nodes whose
// groups have different RF3 placements. Every group remains independently
// three replica, while a NodeID/address pair has one stable spelling across
// the complete hosted roster. The parser performs the same check for files;
// keeping it here protects in-memory callers of the append validators too.
func validateRF3GroupRosterUnion(groups []rf3ManifestGroup) error {
	nodes := make(map[rafttransport.NodeID]string, len(groups)*rf3ManifestMembers)
	addresses := make(map[string]rafttransport.NodeID, len(groups)*rf3ManifestMembers)
	for _, group := range groups {
		count := group.MemberCount
		if count == 0 {
			count = rf3ManifestMembers
		}
		if count != rf3ManifestMembers {
			return errInvalidRF3Manifest
		}
		members := make(map[uint64]bool, count)
		groupNodes := make(map[rafttransport.NodeID]bool, count)
		for _, member := range group.Members[:count] {
			if member.MemberID == 0 || member.NodeID == (rafttransport.NodeID{}) ||
				validateRF3Address(member.PeerAddress, false) != nil || members[member.MemberID] || groupNodes[member.NodeID] {
				return errInvalidRF3Manifest
			}
			members[member.MemberID], groupNodes[member.NodeID] = true, true
			if prior, found := nodes[member.NodeID]; found && prior != member.PeerAddress {
				return errInvalidRF3Manifest
			}
			if prior, found := addresses[member.PeerAddress]; found && prior != member.NodeID {
				return errInvalidRF3Manifest
			}
			nodes[member.NodeID], addresses[member.PeerAddress] = member.PeerAddress, member.NodeID
		}
	}
	return nil
}

// The running transport has a fixed queue and dial destination for each
// startup peer. New groups may combine that roster differently, but a reload
// cannot enroll a new physical peer merely by publishing its group files.
func validateRF3ReloadTransportRoster(current, next rf3Manifest) error {
	known := make(map[rafttransport.NodeID]string)
	for _, bundle := range current.groupBundles() {
		for _, member := range bundle.Members {
			known[member.NodeID] = member.PeerAddress
		}
		if target := bundle.EnrolledTarget; target != nil {
			known[target.NodeID] = target.PeerAddress
		}
	}
	for _, bundle := range next.groupBundles() {
		for _, member := range bundle.Members {
			if address, present := known[member.NodeID]; !present || address != member.PeerAddress {
				return fmt.Errorf("%w: reload requires a peer outside the running transport roster", errInvalidRF3Manifest)
			}
		}
	}
	return nil
}

func reloadPreparedRF3Groups(ctx context.Context, current *rf3Manifest, profile *rafttransport.PeerTLS,
	peer *raftservice.AuthenticatedExecutionPeerRuntime, inventory *rf3AdoptedGroupInventory, schemas *rf3SchemaActivator, nodeOwners ...*rf3NodeOwner,
) error {
	if current == nil || current.reloadPath == "" || inventory == nil || schemas == nil {
		return errInvalidRF3Manifest
	}
	var nodeOwner *rf3NodeOwner
	if len(nodeOwners) > 1 {
		return errInvalidRF3Manifest
	}
	if len(nodeOwners) == 1 {
		nodeOwner = nodeOwners[0]
	}
	if (current.NodeLog != nil) != (nodeOwner != nil) {
		return errInvalidRF3Manifest
	}
	next, err := loadRF3Manifest(current.reloadPath)
	if err != nil {
		return err
	}
	if err := validateRF3GroupTransition(*current, next); err != nil {
		return err
	}
	if err := validateRF3ReloadTransportRoster(*current, next); err != nil {
		return err
	}
	// The configuration is the recovery inventory for these groups. Fence it
	// before publishing any Runtime, including the parent-directory rename.
	for _, path := range []string{current.reloadPath, filepath.Dir(current.reloadPath)} {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		if err := errors.Join(f.Sync(), f.Close()); err != nil {
			return err
		}
	}
	oldBundles, nextBundles := current.groupBundles(), next.groupBundles()
	nextGroups := make(map[raftmember.GroupKey]struct{}, len(nextBundles))
	for _, bundle := range nextBundles {
		nextGroups[bundle.Route.Group] = struct{}{}
	}
	if len(nextBundles) < len(oldBundles) {
		inventory.mu.Lock()
		defer inventory.mu.Unlock()
		if inventory.liveCount() != 0 || inventory.failed || inventory.root == nil {
			return errInvalidRF3Manifest
		}
		for _, bundle := range oldBundles {
			if _, retained := nextGroups[bundle.Route.Group]; retained {
				continue
			}
			schemas.mu.RLock()
			generation := schemas.groups[bundle.Route.Group]
			schemas.mu.RUnlock()
			if generation == nil || generation.identity.Group != bundle.Route.Group {
				return errInvalidRF3Manifest
			}
			if err := peer.UnregisterExecutionGroup(generation.identity); err != nil {
				return err
			}
			schemas.mu.Lock()
			delete(schemas.groups, bundle.Route.Group)
			schemas.mu.Unlock()
			currentChildren := inventory.nativeChildren.Load()
			if currentChildren != nil {
				replacement := make(rf3NativeChildren, len(*currentChildren))
				for group, identity := range *currentChildren {
					if group != bundle.Route.Group {
						replacement[group] = identity
					}
				}
				inventory.nativeChildren.Store(&replacement)
			}
			delete(inventory.runtimes, bundle.Route.Group)
			fmt.Fprintf(os.Stderr, "RF3 retired group table=%q group=%x member=%d\n",
				generation.base.UserTable, generation.identity.Group.GroupID, generation.identity.MemberID)
		}
		inventory.manifest = next
		if err := inventory.save(inventory.entries); err != nil {
			return err
		}
		next.reloadPath, next.reloadSignals = current.reloadPath, current.reloadSignals
		*current = next
		return nil
	}
	for _, bundle := range nextBundles[len(oldBundles):] {
		if err := ctx.Err(); err != nil {
			return err
		}
		set, err := prepareRF3GroupSetOnNode(next.withGroup(bundle), profile, sqldriver.ReplicatedOpenOptions{
			WriterLockContext: ctx, WriterLockDeadline: time.Now().Add(rf3StartupWriterLockWait),
		}, nodeOwner)
		if err != nil {
			return err
		}
		item := &set.groups[0]
		if item.restoreOperation != ([32]byte{}) {
			return item.close(errInvalidRF3Manifest)
		}
		runtime, err := item.adoptRuntime()
		if err != nil {
			if runtime != nil {
				return errors.Join(err, runtime.Close())
			}
			return item.close(err)
		}
		identity := runtime.Identity()
		command := commandFenceFromPublication(item.base.Binding.Authority, identity, item.publication.ReplicaSetVersion)
		inventory.mu.Lock()
		if !inventory.nativeChildCapacity(identity) {
			inventory.mu.Unlock()
			return errors.Join(errRF3SplitChildRegistryBound, runtime.Close())
		}
		err = peer.RegisterExecutionGroup(set.members, raftservice.ExecutionGroup{
			Runtime: runtime, Identity: identity, Command: command, Read: item.apply, Recovery: item.apply,
		})
		if err != nil {
			inventory.mu.Unlock()
			return errors.Join(err, runtime.Close())
		}
		// Recovery comes from the fsynced manifest, not a fabricated split
		// receipt. Reuse the existing dynamic native-authority snapshot.
		inventory.publishNativeChild(identity)
		inventory.mu.Unlock()
		schemas.mu.Lock()
		schemas.groups[identity.Group] = &rf3SchemaGeneration{identity: identity, path: item.manifest.SQL.Path,
			wal: item.recoveryLog(), base: item.base.Clone(), applyID: item.applyIdentity, apply: item.apply}
		schemas.mu.Unlock()
		current.Groups = append(current.groupBundles(), bundle)
		fmt.Fprintf(os.Stderr, "RF3 prepared group loaded table=%q group=%x member=%d\n", item.base.UserTable, identity.Group.GroupID, identity.MemberID)
	}
	return nil
}
