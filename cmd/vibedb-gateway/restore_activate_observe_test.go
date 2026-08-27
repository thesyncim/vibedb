package main

import (
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestRestoreCatalogObservationAdvancesOnlyExactRuntimeIncarnation(t *testing.T) {
	route := gateway.ReplicatedRoute{Group: raftmember.GroupKey{ClusterID: [16]byte{1},
		ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 3,
		ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}, AllocationGeneration: 6}
	endpoint := gateway.ReplicatedEndpoint{Member: 1, StoreID: [16]byte{7}, Node: [16]byte{8}, NodeIncarnation: 2}
	state := shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{
		Group: route.Group, AllocationGeneration: route.AllocationGeneration,
		MemberID: endpoint.Member, StoreID: endpoint.StoreID, NodeIncarnation: 3, Term: 1, Command: route.Command}}
	got, err := bindGatewayRestoreCatalogObservation(route, endpoint, state)
	if err != nil || got.NodeIncarnation != 3 {
		t.Fatalf("observed restart endpoint=%+v err=%v", got, err)
	}
	got.NodeIncarnation = endpoint.NodeIncarnation
	if got != endpoint {
		t.Fatal("observation altered stable target identity")
	}
	for name, change := range map[string]func(*shardservice.ReplicatedMemberState){
		"incarnation-regression":    func(s *shardservice.ReplicatedMemberState) { s.Fence.NodeIncarnation = 1 },
		"foreign-store":             func(s *shardservice.ReplicatedMemberState) { s.Fence.StoreID[0]++ },
		"foreign-member":            func(s *shardservice.ReplicatedMemberState) { s.Fence.MemberID++ },
		"foreign-group":             func(s *shardservice.ReplicatedMemberState) { s.Fence.Group.GroupID[0]++ },
		"changed-command-authority": func(s *shardservice.ReplicatedMemberState) { s.Fence.Command.SchemaGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := state
			change(&candidate)
			if _, err := bindGatewayRestoreCatalogObservation(route, endpoint, candidate); err == nil {
				t.Fatal("divergent observed target accepted")
			}
		})
	}
}
