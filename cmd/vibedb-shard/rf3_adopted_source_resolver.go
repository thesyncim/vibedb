package main

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Source promotion is performed only when an authenticated later catalog
// names a live adopted allocation and the serialized owner confirms that its
// runtime is actually installed. A failed host registration therefore cannot
// expose a closed apply handle. Restart bypasses this cache: inventory groups
// enter the ordinary startup path before any listener serves traffic.
type rf3AdoptedSourceResolver struct {
	mu          sync.Mutex
	registries  *splitcontroller.LocalPlanAdmissionRegistries
	inventory   *rf3AdoptedGroupInventory
	hosted      rf3HostedSplitSources
	observation *splitcontroller.LocalPlanObservationProvider
	owners      splitcontroller.LocalObservationOwner
	factory     *splitcontroller.LocalAdmittedGrantFactory
	makeSource  func(raftmember.RuntimeIdentity, raftservice.CommandFence, *sqldriver.ReplicatedApply, *splitcontroller.RuntimeStoreRegistry) (splitcontroller.AdmittedSourceRuntime, error)
	live        map[raftmember.GroupKey]rf3RetainedSource
}

type rf3RetainedSource struct {
	runtime  rf3AdoptedRuntime
	registry *splitcontroller.RuntimeStoreRegistry
	origin   [32]byte
}

func (resolver *rf3AdoptedSourceResolver) isRetained(group raftmember.GroupKey) bool {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	_, found := resolver.live[group]
	return found
}

func (resolver *rf3AdoptedSourceResolver) ResolveLocalPlanAdmissionStores(ctx context.Context, plan *splitcontroller.Plan) ([]*splitcontroller.RuntimeStoreRegistry, error) {
	return resolver.registries.ResolveLocalPlanAdmissionStores(ctx, plan)
}

func (resolver *rf3AdoptedSourceResolver) ResolveCatalogPlanAdmissionStores(ctx context.Context, catalog *gateway.Snapshot, plan *splitcontroller.Plan) ([]*splitcontroller.RuntimeStoreRegistry, error) {
	if resolver == nil || ctx == nil || catalog == nil || plan == nil {
		return nil, splitcontroller.ErrPlanAdmission
	}
	if resolver.inventory != nil || resolver.hosted != nil {
		if err := resolver.ensureSource(ctx, catalog, plan); err != nil {
			return nil, err
		}
	}
	return resolver.registries.ResolveLocalPlanAdmissionStores(ctx, plan)
}

func (resolver *rf3AdoptedSourceResolver) ensureSource(ctx context.Context, catalog *gateway.Snapshot, plan *splitcontroller.Plan) error {
	distribution, shard, allocation := plan.SourceAllocation()
	var descriptor gateway.ReplicatedShardDescriptor
	for _, candidate := range catalog.ReplicatedShardDescriptors() {
		if candidate.Distribution == distribution && candidate.Shard == shard && candidate.AllocationGeneration == allocation {
			descriptor = candidate
			break
		}
	}
	return resolver.ensureDescriptor(ctx, descriptor, plan)
}

func (resolver *rf3AdoptedSourceResolver) ensureDescriptor(ctx context.Context, descriptor gateway.ReplicatedShardDescriptor, plans ...*splitcontroller.Plan) error {
	distribution, shard, allocation := descriptor.Distribution, descriptor.Shard, descriptor.AllocationGeneration
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	live, retained := resolver.live[descriptor.Group]
	var paths rf3SplitChildPaths
	var entry rf3AdoptedGroupEntry
	var hosted rf3HostedSplitSource
	var hostedFound bool
	if resolver.hosted != nil {
		var err error
		hosted, hostedFound, err = resolver.hosted.lookupHostedSplitSource(descriptor.Group)
		if err != nil {
			return err
		}
	}
	if hostedFound {
		if retained && (live.origin != hosted.origin || !sameRF3DonorIdentity(live.runtime.identity, hosted.runtime.identity)) {
			return errRF3Serving
		}
		found := false
		for _, replica := range descriptor.Replicas {
			identity := hosted.runtime.identity
			if replica.Member == identity.MemberID && replica.Node == hosted.node &&
				replica.StoreID == identity.StoreID && replica.NodeIncarnation == identity.NodeIncarnation {
				found = true
				break
			}
		}
		if !found {
			return errRF3Serving
		}
		live.runtime, live.origin = hosted.runtime, hosted.origin
	} else if !retained {
		if descriptor.SplitOrigin == nil {
			return nil
		}
		inventory := resolver.inventory
		if inventory == nil {
			return nil
		}
		inventory.mu.Lock()
		if inventory.root == nil || inventory.failed {
			inventory.mu.Unlock()
			return errRF3Serving
		}
		prepared, found := inventory.runtimes[descriptor.Group]
		if !found {
			inventory.mu.Unlock()
			return nil
		} // Startup groups already use the normal retained path.
		for _, candidate := range inventory.entries {
			if candidate.operation == descriptor.SplitOrigin.Operation && candidate.child == uint64(descriptor.SplitOrigin.Child) {
				entry = candidate
				break
			}
		}
		if entry.operation == ([32]byte{}) || entry.plan != descriptor.SplitOrigin.PlanDigest || entry.cutover != descriptor.SplitOrigin.CutoverDigest {
			inventory.mu.Unlock()
			return errRF3Serving
		}
		resources, resourceErr := inventory.entryResources(entry)
		inventory.mu.Unlock()
		if resourceErr != nil {
			return resourceErr
		}
		parent := descriptor.SplitOrigin.RootGroup
		if entry.group == rf3DynamicTemplateSlot {
			parent = descriptor.SplitOrigin.ParentGroup
		}
		if resources.SourceGroup != parent {
			return errRF3Serving
		}
		root := resources.Registry
		var err error
		paths, err = root.childPaths(entry.operation, uint8(entry.child))
		if err != nil {
			return err
		}
		live.runtime = prepared
	}
	if !hostedFound && resolver.hosted != nil {
		current, found, err := resolver.hosted.lookupRetainedSplitRuntime(descriptor.Group)
		if err != nil {
			return err
		}
		if found {
			if !sameRF3DonorIdentity(live.runtime.identity, current.identity) {
				return errRF3Serving
			}
			live.runtime = current
		}
	}
	identity := live.runtime.identity
	observed, err := resolver.owners.ObserveReplica(ctx, identity.Group, identity.MemberID)
	if err != nil || observed.Identity != identity || observed.Status.MemberID != identity.MemberID || identity.RelationManifestDigest != descriptor.Command.RelationManifestDigest {
		return errors.Join(errRF3Serving, err)
	}
	binding, command := observed.State.Binding, descriptor.Command
	ownershipMatches := binding.OwnershipEpoch == command.OwnershipEpoch && binding.RoutingVersion == command.RoutingVersion && binding.RouteGeneration == command.RouteGeneration
	if !ownershipMatches && len(plans) == 1 {
		ownershipMatches = plans[0].SourceAdmissionIsSealed(observed.State)
	}
	if binding.Distribution != string(distribution) || binding.Shard != string(shard) || binding.AllocationGeneration != uint64(allocation) ||
		binding.ActivePolicyGeneration != command.ActivePolicyGeneration || binding.ProtectionEpoch != command.ProtectionEpoch ||
		!ownershipMatches || binding.SchemaGeneration != command.SchemaGeneration ||
		observed.Publication.ReplicaSetVersion != command.ReplicaSetVersion {
		return errRF3Serving
	}
	if !retained {
		root, digest := paths.Root, entry.certificate
		if hostedFound {
			root, digest = hosted.root, hosted.origin
			if err := prepareRF3HostedSplitRoot(hosted); err != nil {
				return err
			}
		}
		live.registry, err = resolver.registries.OpenPreparedSource(root, digest)
		if err != nil {
			return err
		}
	}
	registry := live.registry
	source, err := resolver.makeSource(identity, descriptor.Command, live.runtime.apply, registry)
	if err != nil {
		return err
	}
	if retained {
		err = resolver.factory.RefreshSource(source)
	} else {
		err = resolver.factory.RegisterSource(source)
	}
	if err != nil {
		return err
	}
	if err = resolver.registries.RegisterRetained(splitcontroller.RetainedPlanRuntimeRegistry{
		Distribution: distribution, Shard: shard, Allocation: allocation, Registry: registry,
	}); err != nil {
		return err
	}
	group := splitcontroller.LocalObservationGroup{Identity: identity, Command: descriptor.Command, Registry: registry, Capture: live.runtime.apply}
	if err = resolver.observation.RegisterGroups([]splitcontroller.LocalObservationGroup{group}); err != nil {
		if err = resolver.observation.RefreshRetainedGroup(group); err != nil {
			return err
		}
	}
	resolver.live[descriptor.Group] = live
	return nil
}
