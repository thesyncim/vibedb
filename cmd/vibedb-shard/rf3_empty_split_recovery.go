package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"google.golang.org/protobuf/proto"
)

// The prepared set has already excluded exact durable source retirements.
// Adopted children retain their separate certified WALs while ordinary moved
// groups are recovered afterwards from the physical node log.
func recoverRF3EmptySplitChildren(ctx context.Context, runtime *rf3EmptyNodeRuntime,
	prepared *preparedRF3Set, inventory *rf3AdoptedGroupInventory, profile *rafttransport.PeerTLS,
) error {
	if ctx == nil || runtime == nil || prepared == nil || inventory == nil || profile == nil ||
		runtime.registry == nil || runtime.peer == nil || runtime.grants == nil || runtime.schemas == nil || runtime.donors == nil {
		return errRF3Serving
	}
	for index := range prepared.groups {
		item := &prepared.groups[index]
		if !item.adoptedChild || item.nodeOwner != nil || item.nodeLog != nil || item.wal == nil {
			return errRF3Serving
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		entry, resources, err := inventory.recoveredChildResources(item)
		if err != nil {
			return err
		}
		if err = enrollRF3RetainedChildPeers(ctx, runtime, resources, entry.receipt, profile, item.manifest.NodeIncarnation); err != nil {
			return fmt.Errorf("restore split child peers: %w", err)
		}
		// Keep non-owning metadata for schema registration, and immediately
		// detach transferred handles from the startup cleanup list.
		metadata := *item
		clear(metadata.key.Material[:])
		adopted, err := item.adoptRuntime()
		if adopted != nil {
			item.database, item.apply, item.wal = nil, nil, nil
			clear(item.key.Material[:])
		}
		if err != nil {
			if adopted != nil {
				err = errors.Join(err, adopted.Close())
			}
			return err
		}
		publication, err := adopted.Publication()
		if err != nil || publication.ReplicaSetVersion != metadata.publication.ReplicaSetVersion ||
			!proto.Equal(publication.ConfState, metadata.publication.ConfState) {
			return errors.Join(errRF3Serving, err, adopted.Close())
		}
		identity := adopted.Identity()
		var roster []rafttransport.Member
		for _, member := range prepared.members {
			if member.Group == identity.Group {
				roster = append(roster, member)
			}
		}
		grant, _, err := runtime.grants.Register(identity.Group, metadata.manifest.Route.MembershipGrantPath)
		if err != nil {
			return errors.Join(err, adopted.Close())
		}
		rollback, err := registerRF3SplitChildServices(runtime.schemas, runtime.donors, metadata, identity)
		if err != nil {
			return errors.Join(err, adopted.Close())
		}
		command := commandFenceFromPublication(metadata.base.Binding.Authority, identity, publication.ReplicaSetVersion)
		if err = runtime.RegisterExecutionGroupWithGrant(roster, raftservice.ExecutionGroup{
			Runtime: adopted, Identity: identity, Command: command, Read: metadata.apply, Recovery: metadata.apply,
		}, grant); err != nil {
			return errors.Join(err, rollback(), adopted.Close())
		}
		inventory.mu.Lock()
		err = inventory.recordNativeChild(entry, rf3AdoptedRuntime{identity: identity, apply: metadata.apply})
		inventory.mu.Unlock()
		if err != nil {
			// The peer owns the runtime now; its shutdown retires it. Never
			// close a handle concurrently with its execution lane.
			return err
		}
	}
	return nil
}

func (inventory *rf3AdoptedGroupInventory) recoveredChildResources(item *preparedRF3Group) (rf3AdoptedGroupEntry, rf3SplitChildResources, error) {
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	if inventory.root == nil || inventory.failed || item == nil {
		return rf3AdoptedGroupEntry{}, rf3SplitChildResources{}, errRF3Serving
	}
	for _, entry := range inventory.entries {
		if entry.operation == ([32]byte{}) || entry.certificate != item.splitRuntimeDigest {
			continue
		}
		resources, err := inventory.entryResources(entry)
		if err != nil {
			return entry, resources, err
		}
		paths, err := resources.Registry.childPaths(entry.operation, uint8(entry.child))
		if err != nil || paths.Database != item.manifest.SQL.Path || paths.WAL != item.manifest.WAL.Path || paths.Root != item.manifest.Route.MemberRoot {
			return entry, resources, errors.Join(errRF3Serving, err)
		}
		return entry, resources, nil
	}
	return rf3AdoptedGroupEntry{}, rf3SplitChildResources{}, errRF3Serving
}

func enrollRF3RetainedChildPeers(ctx context.Context, runtime *rf3EmptyNodeRuntime,
	resources rf3SplitChildResources, receipt [32]byte, profile *rafttransport.PeerTLS, incarnation uint64,
) error {
	if len(resources.Peers) == 0 || receipt == ([32]byte{}) {
		return errRF3Serving
	}
	registry := runtime.registry
	for _, peer := range resources.Peers {
		intent := rafttransport.EnrollmentIntent{
			Domain: profile.LocalIdentity().TrustDomain, Peer: peer,
			Digest:            rf3EmptyNodePeerEnrollmentDigest(replication.Digest(receipt), peer.NodeID),
			DirectoryRevision: registry.PeerDirectoryRevision(),
		}
		verifier := rafttransport.EnrollmentVerifierFunc(func(candidate rafttransport.EnrollmentIntent) error {
			// The immutable template and child receipt were authenticated by
			// inventory recovery. A different enrollment receipt may already
			// own this same physical record, but none of its pins may change.
			actual := candidate.Peer
			actual.EnrollmentDigest = peer.EnrollmentDigest
			if actual != peer || candidate.Domain != profile.LocalIdentity().TrustDomain || candidate.Group != (raftmember.GroupKey{}) {
				return rafttransport.ErrPeerUnauthorized
			}
			return nil
		})
		if peer.NodeID == profile.LocalIdentity().Node {
			if err := registry.BindLocalPeerContext(ctx, intent, profile, incarnation, verifier); err != nil {
				return err
			}
		} else if err := rf3EnrollPhysicalPeer(ctx, runtime.peer.Transport(), registry, intent, verifier); err != nil {
			return err
		}
	}
	return nil
}

// Registration borrows the exact already-open handles. The caller retains
// runtime ownership until execution publication succeeds.
func registerRF3SplitChildServices(schemas *rf3SchemaActivator, donors *rf3DynamicDonorServices,
	item preparedRF3Group, identity raftmember.RuntimeIdentity,
) (func() error, error) {
	if schemas == nil {
		return nil, errRF3Serving
	}
	single, err := newRF3SchemaActivator(schemas.owners, []preparedRF3Group{item}, []raftmember.RuntimeIdentity{identity})
	if err != nil {
		return nil, err
	}
	state := single.groups[identity.Group]
	schemas.mu.Lock()
	if schemas.groups[identity.Group] != nil || len(schemas.groups) >= maxRF3ManifestGroups {
		schemas.mu.Unlock()
		return nil, errRF3Serving
	}
	schemas.groups[identity.Group] = state
	schemas.mu.Unlock()
	rollback := func() error {
		var err error
		if donors != nil {
			err = donors.Unregister(identity)
		}
		return errors.Join(err, schemas.UnregisterDynamic(identity))
	}
	if donors != nil {
		if err := donors.Register(identity.Group); err != nil {
			return nil, errors.Join(err, rollback())
		}
	}
	return rollback, nil
}
