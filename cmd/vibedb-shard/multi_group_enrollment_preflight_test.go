//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

// Exercise the process fixture's two identities through the shipped manifest
// parser and retained-registry admission without Linux durable allocation.
func TestServeRF3EnrolledGroupRegistryPreflight(t *testing.T) {
	first, second := rf3EnrolledGroupIdentities()
	identities := []raftstore.Identity{first, second}
	root := t.TempDir()
	nodes := rf3CommandNodes()
	peers := [3]string{"127.0.0.1:31001", "127.0.0.1:31002", "127.0.0.1:31003"}
	profile := rf3testfixture.DurableGatewayMemberProfiles()[rf3testfixture.DurableGatewayCatalogGroup]
	documents := make([][]byte, len(identities))
	for i, identity := range identities {
		cold := rf3ColdTargetIdentity(t, rf3ColdTargetOptions{
			Group: raftmember.GroupKey{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
				TopologyRecoveryEpoch: rf3CommandGroup().TopologyRecoveryEpoch, ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID},
			Distribution: identity.Distribution, Shard: identity.Shard, AllocationGeneration: identity.AllocationGeneration,
			Target: rf3ManifestEnrolledTarget{MemberID: 4, StoreID: [16]byte{byte(200 + i)}},
		})
		if cold.Distribution != identity.Distribution || cold.Shard != identity.Shard || cold.AllocationGeneration != identity.AllocationGeneration ||
			cold.GroupID != identity.GroupID || cold.ShardIncarnation != identity.ShardIncarnation || cold.MemberID != 4 || cold.StoreID == identity.StoreID {
			t.Fatal("cold learner lost exact source group or logical placement")
		}
		documents[i] = rf3testfixture.ProcessMemberManifest(rf3testfixture.ProcessMemberOptions{
			Root: filepath.Join(root, fmt.Sprintf("group-%d", i)), ControlRoot: filepath.Join(root, "control"),
			Table: profile.Table, CreateTable: profile.CreateTable, Authority: rf3CommandAuthority(), Apply: profile.Apply,
			Identity: identity, Key: raftstore.Key{ID: "preflight-key"}, WAL: rf3testfixture.DurableGatewayWALOptions(),
			Bootstrap:  rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
			Listeners:  rf3testfixture.ProcessListeners{Peer: peers[0], Native: "127.0.0.1:32001", Snapshot: "127.0.0.1:33001", Control: "127.0.0.1:34001"},
			Credential: rf3testfixture.Credential{Certificate: filepath.Join(root, "node.crt"), Key: filepath.Join(root, "node.key")},
			Roots:      filepath.Join(root, "roots.crt"), AuthorizationPolicy: filepath.Join(root, "policy.vibejson"),
			Nodes: nodes, PeerAddresses: peers,
		})
	}
	raw, err := rf3testfixture.CombineProcessManifests(documents...)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := parseRF3Manifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	retained := make([]splitcontroller.RetainedPlanRuntimeRegistry, len(identities))
	for i, bundle := range manifest.groupBundles() {
		route := bundle.Route
		if route.Distribution != identities[i].Distribution || route.Shard != identities[i].Shard ||
			route.AllocationGeneration != identities[i].AllocationGeneration || route.Group.GroupID != identities[i].GroupID {
			t.Fatal("manifest altered the exact logical or Raft group identity")
		}
		registry, openErr := splitcontroller.OpenRuntimeStoreRegistry(t.TempDir(), [32]byte{byte(i + 1)}, 2, nil)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { _ = registry.Close() })
		retained[i] = splitcontroller.RetainedPlanRuntimeRegistry{
			Distribution: distribution.DistributionName(route.Distribution), Shard: distribution.ShardID(route.Shard),
			Allocation: distribution.ShardAllocationGeneration(route.AllocationGeneration), Registry: registry,
		}
	}
	set, err := splitcontroller.NewLocalPlanAdmissionRegistries(nodes[0], retained, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	// Distinct Raft IDs cannot authorize two stores for one logical allocation.
	retained[1].Distribution, retained[1].Shard, retained[1].Allocation = retained[0].Distribution, retained[0].Shard, retained[0].Allocation
	if _, err := splitcontroller.NewLocalPlanAdmissionRegistries(nodes[0], retained, 2, nil); !errors.Is(err, splitcontroller.ErrPlanAdmission) {
		t.Fatalf("duplicate logical allocation accepted: %v", err)
	}
}
