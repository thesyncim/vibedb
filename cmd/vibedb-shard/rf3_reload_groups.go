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
	if len(all) < len(old) || len(all) > maxRF3ManifestGroups || current.DevelopmentOnly || next.DevelopmentOnly {
		return errInvalidRF3Manifest
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
			group.EnrolledTarget != nil || group.Members != all[0].Members || group.MemberCount != all[0].MemberCount {
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
			bundle.EnrolledTarget != nil || bundle.Members != all[0].Members || bundle.MemberCount != all[0].MemberCount {
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
	return nil
}

func reloadPreparedRF3Groups(ctx context.Context, current *rf3Manifest, profile *rafttransport.PeerTLS,
	peer *raftservice.AuthenticatedExecutionPeerRuntime, inventory *rf3AdoptedGroupInventory, schemas *rf3SchemaActivator,
) error {
	if current == nil || current.reloadPath == "" || inventory == nil || schemas == nil {
		return errInvalidRF3Manifest
	}
	next, err := loadRF3Manifest(current.reloadPath)
	if err != nil {
		return err
	}
	if err := validateRF3GroupTransition(*current, next); err != nil {
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
		set, err := prepareRF3GroupSet(next.withGroup(bundle), profile, sqldriver.ReplicatedOpenOptions{
			WriterLockContext: ctx, WriterLockDeadline: time.Now().Add(rf3StartupWriterLockWait),
		})
		if err != nil {
			return err
		}
		item := &set.groups[0]
		if item.restoreOperation != ([32]byte{}) {
			return item.close(errInvalidRF3Manifest)
		}
		runtime, err := raftmember.AdoptPipelinedRuntime(item.wal, item.database, item.apply)
		if err != nil {
			return err
		}
		if err := runtime.ConfigureWALGeneration(raftmember.WALGenerationDriverOptions{
			IntervalTicks: rf3WALGenerationIntervalTicks, Key: item.key,
		}); err != nil {
			return errors.Join(err, runtime.Close())
		}
		clear(item.key.Material[:])
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
			wal: item.wal, base: item.base.Clone(), applyID: item.applyIdentity, apply: item.apply}
		schemas.mu.Unlock()
		current.Groups = append(current.groupBundles(), bundle)
		fmt.Fprintf(os.Stderr, "RF3 prepared group loaded table=%q group=%x member=%d\n", item.base.UserTable, identity.Group.GroupID, identity.MemberID)
	}
	return nil
}
