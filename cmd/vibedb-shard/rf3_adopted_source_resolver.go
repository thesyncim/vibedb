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
	observation *splitcontroller.LocalPlanObservationProvider
	owners      splitcontroller.LocalObservationOwner
	factory     *splitcontroller.LocalAdmittedGrantFactory
	makeSource  func(raftmember.RuntimeIdentity, raftservice.CommandFence, *sqldriver.ReplicatedApply, *splitcontroller.RuntimeStoreRegistry) (splitcontroller.AdmittedSourceRuntime, error)
	live        map[raftmember.GroupKey]rf3RetainedSource
}

type rf3RetainedSource struct {
	runtime  rf3AdoptedRuntime
	registry *splitcontroller.RuntimeStoreRegistry
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
	if resolver.inventory != nil {
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
	if !retained {
		if descriptor.SplitOrigin == nil {
			return nil
		}
		inventory := resolver.inventory
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
		if entry.operation == ([32]byte{}) || entry.plan != descriptor.SplitOrigin.PlanDigest || entry.cutover != descriptor.SplitOrigin.CutoverDigest ||
			inventory.manifest.groupBundles()[entry.group].Route.Group != descriptor.SplitOrigin.RootGroup {
			inventory.mu.Unlock()
			return errRF3Serving
		}
		root := inventory.manifest.groupBundles()[entry.group].ChildRegistry
		inventory.mu.Unlock()
		var err error
		paths, err = root.childPaths(entry.operation, uint8(entry.child))
		if err != nil {
			return err
		}
		live.runtime = prepared
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
		live.registry, err = resolver.registries.OpenPreparedSource(paths.Root, entry.certificate)
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
	group := splitcontroller.LocalObservationGroup{Identity: identity, Command: descriptor.Command, Registry: registry}
	if err = resolver.observation.RegisterGroups([]splitcontroller.LocalObservationGroup{group}); err != nil {
		if err = resolver.observation.RefreshRetainedGroup(group); err != nil {
			return err
		}
	}
	resolver.live[descriptor.Group] = live
	return nil
}
