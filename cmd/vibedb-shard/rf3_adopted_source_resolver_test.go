package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type rf3AdoptedTestOwner struct {
	observation raftservice.ReplicaObservation
}

func (owner *rf3AdoptedTestOwner) ObserveReplica(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error) {
	return owner.observation, nil
}

type rf3AdoptedTestExecutor struct{}

func (rf3AdoptedTestExecutor) ExecuteSplitAction(context.Context, *splitcontroller.Plan, splitcontroller.Observation, splitcontroller.Action) error {
	return nil
}

func testRF3AdoptedSource(t *testing.T) (*rf3AdoptedSourceResolver, gateway.ReplicatedShardDescriptor, *rf3AdoptedTestOwner, splitcontroller.LocalObservationGroup) {
	t.Helper()
	inventory := testRF3AdoptedInventory(t)
	rootGroup := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}
	group := rootGroup
	group.GroupID[0] = 5
	group.ShardIncarnation[0] = 6
	inventory.manifest.Groups[1].Route.Group = rootGroup
	inventory.manifest.Groups[1].ChildRegistry.Root = t.TempDir()
	entry := testRF3AdoptedEntry(1)
	if err := inventory.record(entry); err != nil {
		t.Fatal(err)
	}
	paths, err := inventory.manifest.Groups[1].ChildRegistry.childPaths(entry.operation, uint8(entry.child))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.Root, 0700); err != nil {
		t.Fatal(err)
	}
	identity := raftmember.RuntimeIdentity{Group: group, Distribution: "d", Shard: "child", AllocationGeneration: 2, MemberID: 1, StoreID: [16]byte{7}, NodeIncarnation: 4, RelationManifestDigest: [32]byte{8}}
	command := raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 2, SchemaGeneration: 1, RoutingVersion: 2, RouteGeneration: 2, RelationManifestDigest: identity.RelationManifestDigest}
	descriptor := gateway.ReplicatedShardDescriptor{Distribution: "d", Shard: "child", AllocationGeneration: 2, Group: group, Command: command,
		SplitOrigin: &gateway.ReplicatedSplitOrigin{RootGroup: rootGroup, Operation: entry.operation, PlanDigest: entry.plan, CutoverDigest: entry.cutover, Child: uint8(entry.child)}}
	owner := &rf3AdoptedTestOwner{observation: raftservice.ReplicaObservation{Identity: identity, Status: raftmember.RuntimeStatus{MemberID: 1}, State: replicatedstate.State{Binding: replicatedstate.Binding{Distribution: "d", Shard: "child", AllocationGeneration: 2, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 2, SchemaGeneration: 1, RoutingVersion: 2, RouteGeneration: 2}}}}
	owner.observation.Publication.ReplicaSetVersion = 1
	inventory.runtimes = map[raftmember.GroupKey]rf3AdoptedRuntime{group: {identity: identity}}
	rootRegistry, err := splitcontroller.OpenRuntimeStoreRegistry(t.TempDir(), [32]byte{9}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rootRegistry.Close() })
	registries, err := splitcontroller.NewLocalPlanAdmissionRegistries(rafttransport.NodeID{1}, []splitcontroller.RetainedPlanRuntimeRegistry{{Distribution: "d", Shard: "root", Allocation: 1, Registry: rootRegistry}}, 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registries.Close() })
	initialIdentity := identity
	initialIdentity.Group = rootGroup
	initialIdentity.Shard = "root"
	initialIdentity.AllocationGeneration = 1
	initial := splitcontroller.LocalObservationGroup{Identity: initialIdentity, Command: command, Registry: rootRegistry}
	provider, err := splitcontroller.NewLocalPlanObservationProvider(owner, []splitcontroller.LocalObservationGroup{initial})
	if err != nil {
		t.Fatal(err)
	}
	makeSource := func(identity raftmember.RuntimeIdentity, command raftservice.CommandFence, _ *sqldriver.ReplicatedApply, registry *splitcontroller.RuntimeStoreRegistry) (splitcontroller.AdmittedSourceRuntime, error) {
		target, err := splitcontroller.ShardActionTargetForServing(identity, command)
		return splitcontroller.AdmittedSourceRuntime{Distribution: distribution.DistributionName(identity.Distribution), Shard: distribution.ShardID(identity.Shard), Allocation: distribution.ShardAllocationGeneration(identity.AllocationGeneration), Registry: registry, Target: target,
			NewExecutor: func(context.Context, *gateway.Snapshot, *splitcontroller.Plan, splitcontroller.PlanAdmission, *splitcontroller.RuntimeStoreLease) (splitcontroller.ShardActionExecutor, error) {
				return rf3AdoptedTestExecutor{}, nil
			}}, err
	}
	initialSource, err := makeSource(initialIdentity, command, nil, rootRegistry)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := splitcontroller.NewLocalAdmittedGrantFactory(splitcontroller.LocalAdmittedGrantFactoryOptions{Node: rafttransport.NodeID{1}, Sources: []splitcontroller.AdmittedSourceRuntime{initialSource}})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &rf3AdoptedSourceResolver{inventory: inventory, registries: registries, owners: owner, observation: provider, factory: factory, makeSource: makeSource, live: make(map[raftmember.GroupKey]rf3RetainedSource)}
	return resolver, descriptor, owner, initial
}

func TestRF3AdoptedSourceRejectsStaleIncarnationAndFences(t *testing.T) {
	for _, kind := range []string{"incarnation", "store", "member", "schema", "ownership", "routing"} {
		t.Run(kind, func(t *testing.T) {
			resolver, descriptor, owner, _ := testRF3AdoptedSource(t)
			switch kind {
			case "incarnation":
				owner.observation.Identity.NodeIncarnation++
			case "store":
				owner.observation.Identity.StoreID[0]++
			case "member":
				owner.observation.Identity.MemberID++
			case "schema":
				descriptor.Command.RelationManifestDigest[0]++
			case "ownership":
				owner.observation.State.Binding.OwnershipEpoch++
			case "routing":
				owner.observation.State.Binding.RoutingVersion++
			}
			if err := resolver.ensureDescriptor(t.Context(), descriptor); err == nil {
				t.Fatal("substituted owner authority promoted source")
			}
			if resolver.isRetained(descriptor.Group) {
				t.Fatal("failed source became retained")
			}
		})
	}
}

func TestRF3AdoptedSourceRetriesPartialPromotionAndRefreshesExactOwner(t *testing.T) {
	resolver, descriptor, owner, initial := testRF3AdoptedSource(t)
	wrong := initial
	wrong.Identity = owner.observation.Identity
	if err := resolver.observation.RegisterGroups([]splitcontroller.LocalObservationGroup{wrong}); err != nil {
		t.Fatal(err)
	}
	if err := resolver.ensureDescriptor(t.Context(), descriptor); err == nil {
		t.Fatal("foreign observation registry accepted")
	}
	if resolver.isRetained(descriptor.Group) {
		t.Fatal("partial promotion marked complete")
	}
	if err := resolver.observation.UnregisterGroups([]splitcontroller.LocalObservationGroup{wrong}); err != nil {
		t.Fatal(err)
	}
	if err := resolver.ensureDescriptor(t.Context(), descriptor); err != nil {
		t.Fatal("exact retry after partial promotion", err)
	}
	if !resolver.isRetained(descriptor.Group) {
		t.Fatal("successful promotion missing")
	}
	descriptor.Command.OwnershipEpoch++
	descriptor.Command.RoutingVersion++
	descriptor.Command.RouteGeneration++
	owner.observation.State.Binding.OwnershipEpoch++
	owner.observation.State.Binding.RoutingVersion++
	owner.observation.State.Binding.RouteGeneration++
	if err := resolver.ensureDescriptor(t.Context(), descriptor); err != nil {
		t.Fatal("next split could not refresh exact owner", err)
	}
	descriptor.Command.OwnershipEpoch--
	owner.observation.State.Binding.OwnershipEpoch--
	if err := resolver.ensureDescriptor(t.Context(), descriptor); err == nil {
		t.Fatal("rolled command fence backwards")
	}
}

func TestRF3AdoptedReceiptRejectsReferencedSymlinks(t *testing.T) {
	for _, kind := range []string{"operation", "child", "receipt", "wal", "database"} {
		t.Run(kind, func(t *testing.T) {
			template := rf3ManifestSplitChildRegistry{Root: t.TempDir()}
			operation := [32]byte{1}
			paths, err := template.childPaths(operation, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(paths.Root, 0700); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{filepath.Join(paths.Root, rf3ChildPrepareReceiptName), paths.WAL, paths.Database} {
				if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := readRF3AdoptedReceipt(template, operation, 1); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(paths.Root, rf3ChildPrepareReceiptName)
			switch kind {
			case "operation":
				path = filepath.Dir(paths.Root)
			case "child":
				path = paths.Root
			case "wal":
				path = paths.WAL
			case "database":
				path = paths.Database
			}
			moved := filepath.Join(t.TempDir(), "replacement")
			if err := os.Rename(path, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(moved, path); err != nil {
				t.Fatal(err)
			}
			if _, err := readRF3AdoptedReceipt(template, operation, 1); err == nil {
				t.Fatal("referenced symlink accepted")
			}
		})
	}
}
