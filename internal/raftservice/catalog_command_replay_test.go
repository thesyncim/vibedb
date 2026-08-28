package raftservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func TestCatalogCommandReplayRequiresExactNonPlacementAuthority(t *testing.T) {
	group := peerServerTestGroup()
	command := replication.CommandView{AuthorityClass: replication.CommandAuthorityTopology,
		Distribution: []byte("catalog"), Shard: []byte("controlplane"),
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
		AllocationGeneration: 1, ReplicaSetVersion: 1, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
		OwnershipEpoch: 4, SchemaGeneration: 5, RoutingVersion: 6, RouteGeneration: 7}
	fence := ServingFence{Group: group, AllocationGeneration: 1, Command: CommandFence{
		ReplicaSetVersion: 2, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
		OwnershipEpoch: 5, SchemaGeneration: 5, RoutingVersion: 7, RouteGeneration: 8}}
	if !CatalogCommandReplayMatchesFence(command, fence) || commandMatchesFence(command, fence) {
		t.Fatal("old catalog command has no exact replay-only path")
	}
	for name, mutate := range map[string]func(*replication.CommandView){
		"data":              func(c *replication.CommandView) { c.AuthorityClass = replication.CommandAuthorityData },
		"distribution":      func(c *replication.CommandView) { c.Distribution = []byte("data") },
		"shard":             func(c *replication.CommandView) { c.Shard = []byte("other") },
		"group":             func(c *replication.CommandView) { c.GroupID[0]++ },
		"allocation":        func(c *replication.CommandView) { c.AllocationGeneration++ },
		"schema":            func(c *replication.CommandView) { c.SchemaGeneration++ },
		"policy":            func(c *replication.CommandView) { c.ActivePolicyGeneration++ },
		"protection":        func(c *replication.CommandView) { c.ProtectionEpoch++ },
		"future-membership": func(c *replication.CommandView) { c.ReplicaSetVersion = 3 },
		"future-ownership":  func(c *replication.CommandView) { c.OwnershipEpoch = 6 },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := command
			mutate(&wrong)
			if CatalogCommandReplayMatchesFence(wrong, fence) {
				t.Fatal("foreign/future command admitted to catalog replay")
			}
		})
	}
}
