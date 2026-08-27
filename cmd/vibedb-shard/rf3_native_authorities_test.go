package main

import (
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func nativeAuthorityFixture(t *testing.T, count int) (*rafttransport.StaticRegistry, *serviceauthz.Gate, []preparedRF3Group, []raftservice.ServingState) {
	t.Helper()
	manifest := serveRF3TestManifest()
	var members []rafttransport.Member
	prepared := make([]preparedRF3Group, count)
	states := make([]raftservice.ServingState, count)
	for index := range prepared {
		group := serveRF3TestGroup()
		group.GroupID[1] = byte(index + 1)
		group.ShardIncarnation[1] = byte(index + 1)
		binding := sqldriver.ReplicatedShardStoreBinding{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, ShardIncarnation: group.ShardIncarnation,
			GroupID: group.GroupID, MemberID: 1, StoreID: [16]byte{byte(index + 1)},
			Distribution: "data", Shard: "shard", AllocationGeneration: uint64(index + 1), Authority: rf3CommandAuthority(),
		}
		prepared[index] = preparedRF3Group{manifest: manifest, base: sqldriver.ReplicatedShardStoreIdentity{Binding: binding}}
		identity := raftmember.RuntimeIdentity{Group: group, MemberID: binding.MemberID, StoreID: binding.StoreID,
			Distribution: binding.Distribution, Shard: binding.Shard, AllocationGeneration: binding.AllocationGeneration,
			NodeIncarnation: 1, RelationManifestDigest: [32]byte{byte(index + 1)}}
		states[index] = raftservice.ServingState{Identity: identity, Command: commandFenceFromPublication(binding.Authority, identity, 9)}
		for _, member := range manifest.Members {
			members = append(members, rafttransport.Member{Group: group, MemberID: member.MemberID,
				Node: member.NodeID, Role: rafttransport.MemberVoter, ReplicaSetVersion: 9})
		}
	}
	registry, err := rafttransport.NewStaticRegistry(manifest.Members[0].NodeID, members,
		rafttransport.Limits{MaxGroups: count, MaxMembers: len(members)})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: [16]byte{10}, Capabilities: serviceauthz.CapabilityTopology | serviceauthz.CapabilityRestoreActivate}})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	return registry, gate, prepared, states
}

func TestRF3NativeAuthoritiesDispatchEveryPreparedGroup(t *testing.T) {
	registry, gate, prepared, states := nativeAuthorityFixture(t, 3)
	authority, err := newRF3NativeAuthorities(registry, gate, prepared, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for index, state := range states {
		if !authority.serving(state) {
			t.Fatalf("prepared group %d refused native traffic", index)
		}
		if index > 0 && authority.groups[states[0].Identity.Group].baseServing(state) {
			t.Fatal("fixture does not distinguish the primary-only regression")
		}
		if allocs := testing.AllocsPerRun(100, func() { _ = authority.serving(state) }); allocs != 0 {
			t.Fatalf("serving allocations: %g", allocs)
		}
		for name, mutate := range map[string]func(*raftservice.ServingState){
			"group":           func(s *raftservice.ServingState) { s.Identity.Group.GroupID[0]++ },
			"member":          func(s *raftservice.ServingState) { s.Identity.MemberID++ },
			"store":           func(s *raftservice.ServingState) { s.Identity.StoreID[0]++ },
			"version":         func(s *raftservice.ServingState) { s.Command.ReplicaSetVersion++ },
			"invalid command": func(s *raftservice.ServingState) { s.Command.SchemaGeneration = 0 },
		} {
			t.Run(name, func(t *testing.T) {
				changed := state
				mutate(&changed)
				if authority.serving(changed) {
					t.Fatal("foreign authority admitted")
				}
			})
		}
	}
	if _, err = newRF3NativeAuthorities(registry, gate, append(prepared, prepared[0]), nil, nil); err == nil {
		t.Fatal("duplicate group accepted")
	}
	prepared[1].base.Binding.MemberID = 2
	if _, err = newRF3NativeAuthorities(registry, gate, prepared, nil, nil); err == nil {
		t.Fatal("nonlocal identity accepted")
	}
}

func TestRF3NativeAuthoritiesKeepRestoreExceptionGroupScoped(t *testing.T) {
	registry, gate, prepared, states := nativeAuthorityFixture(t, 2)
	// A restored catalog need not be the primary group of the process.
	prepared[1].base.Binding.Distribution = string(gateway.ReplicatedCatalogDistribution)
	prepared[1].base.Binding.Shard = string(gateway.ReplicatedCatalogShard)
	prepared[1].base.UserTable = gateway.ReplicatedCatalogTable
	group := states[1].Identity.Group
	operation := [32]byte{4}
	restore, err := shardservice.NewRestoreServingGate(states[1].Identity, rafttransport.NodeID{1}, operation)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := newRF3NativeAuthorities(registry, gate, prepared,
		map[raftmember.GroupKey]*shardservice.RestoreServingGate{group: restore}, map[raftmember.GroupKey][32]byte{group: operation})
	if err != nil {
		t.Fatal(err)
	}
	if !authority.serving(states[0]) || authority.serving(states[1]) {
		t.Fatal("restore gate leaked between groups")
	}
	request := shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe, Capability: serviceauthz.CapabilityTopology,
		Authority: serviceauthz.Authority{Node: [16]byte{10}, Generation: 1}, Fence: shardservice.ReplicatedFence{Group: group}}
	if !authority.transitional(states[1], &request) {
		t.Fatal("secondary restored catalog bootstrap refused")
	}
	if authority.transitional(states[0], &request) {
		t.Fatal("restore exception leaked into other group")
	}
	request.Capability = serviceauthz.CapabilityDataRead
	if authority.transitional(states[1], &request) {
		t.Fatal("restore bypass admitted data")
	}
	if authority.transitional(states[1], nil) {
		t.Fatal("nil request admitted")
	}
}
