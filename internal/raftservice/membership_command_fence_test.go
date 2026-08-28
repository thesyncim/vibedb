package raftservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func TestMembershipStableCommandRequiresExactLogicalFence(t *testing.T) {
	group := peerServerTestGroup()
	command := replication.CommandView{AuthorityClass: replication.CommandAuthorityMembershipStableData,
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
		AllocationGeneration: 1, ReplicaSetVersion: 1, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
		OwnershipEpoch: 4, SchemaGeneration: 5, RoutingVersion: 6, RouteGeneration: 7}
	fence := ServingFence{Group: group, AllocationGeneration: 1, Command: CommandFence{
		ReplicaSetVersion: 2, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
		OwnershipEpoch: 4, SchemaGeneration: 5, RoutingVersion: 6, RouteGeneration: 7}}
	if !commandMatchesFence(command, fence) {
		t.Fatal("stable command rejected membership-only advancement")
	}
	for name, change := range map[string]func(*replication.CommandView){
		"legacy-data":         func(c *replication.CommandView) { c.AuthorityClass = replication.CommandAuthorityData },
		"legacy-route":        func(c *replication.CommandView) { c.AuthorityClass = replication.CommandAuthorityRouteSession },
		"cluster":             func(c *replication.CommandView) { c.ClusterID[0]++ },
		"cluster-incarnation": func(c *replication.CommandView) { c.ClusterIncarnation[0]++ },
		"recovery":            func(c *replication.CommandView) { c.TopologyRecoveryEpoch++ },
		"group":               func(c *replication.CommandView) { c.GroupID[0]++ },
		"shard-incarnation":   func(c *replication.CommandView) { c.ShardIncarnation[0]++ },
		"allocation":          func(c *replication.CommandView) { c.AllocationGeneration++ },
		"future-membership":   func(c *replication.CommandView) { c.ReplicaSetVersion = 3 },
		"schema":              func(c *replication.CommandView) { c.SchemaGeneration++ },
		"policy":              func(c *replication.CommandView) { c.ActivePolicyGeneration++ },
		"protection":          func(c *replication.CommandView) { c.ProtectionEpoch++ },
		"ownership":           func(c *replication.CommandView) { c.OwnershipEpoch++ },
		"routing":             func(c *replication.CommandView) { c.RoutingVersion++ },
		"route-generation":    func(c *replication.CommandView) { c.RouteGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := command
			change(&wrong)
			if commandMatchesFence(wrong, fence) {
				t.Fatal("accepted mismatched logical authority")
			}
		})
	}
}
