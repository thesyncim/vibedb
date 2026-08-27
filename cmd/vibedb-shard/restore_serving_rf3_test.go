package main

import (
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestRestoreCatalogPreparingAuthorityIsNarrowAndRestoreOnly(t *testing.T) {
	group := rf3CommandGroup()
	operator := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: operator.Node,
		Capabilities: serviceauthz.CapabilityTopology | serviceauthz.CapabilityRestoreActivate}})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	base := sqldriver.ReplicatedShardStoreIdentity{UserTable: gateway.ReplicatedCatalogTable,
		Binding: sqldriver.ReplicatedShardStoreBinding{Distribution: string(gateway.ReplicatedCatalogDistribution),
			Shard: string(gateway.ReplicatedCatalogShard)}}
	state := raftservice.ServingState{Identity: raftmember.RuntimeIdentity{Group: group}}
	allows := rf3RestoreCatalogPreparingAuthority(gate, [32]byte{2}, group, base,
		func(raftservice.ServingState) bool { return true })
	request := shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe,
		Authority: operator, Capability: serviceauthz.CapabilityTopology,
		Fence: shardservice.ReplicatedFence{Group: group}}
	if !allows(state, &request) {
		t.Fatal("authenticated restore bootstrap probe rejected")
	}
	request.Operation, request.Relation, request.MaxValueBytes = shardservice.ReplicatedReadLeader, 1, 1024
	request.Key, _ = orderedkey.AppendString(nil, []byte("restore/activation"), orderedkey.Ascending)
	if !allows(state, &request) {
		t.Fatal("exact activation row read rejected")
	}
	request.Key, _ = orderedkey.AppendString(nil, []byte("catalog/head"), orderedkey.Ascending)
	if allows(state, &request) {
		t.Fatal("unrelated catalog row exposed before activation")
	}
	request.Operation = shardservice.ReplicatedPropose
	request.Command = []byte("arbitrary topology command")
	if allows(state, &request) {
		t.Fatal("arbitrary topology proposal admitted")
	}
	request.Operation, request.Capability = shardservice.ReplicatedProbe, serviceauthz.CapabilityDataRead
	if allows(state, &request) {
		t.Fatal("data principal admitted")
	}
	policy, err = serviceauthz.NewPolicy(2, []serviceauthz.Entry{{Node: operator.Node,
		Capabilities: serviceauthz.CapabilityTopology}})
	if err != nil {
		t.Fatal(err)
	}
	if err = gate.Rotate(policy); err != nil {
		t.Fatal(err)
	}
	request.Capability, request.Authority.Generation = serviceauthz.CapabilityTopology, 2
	if allows(state, &request) {
		t.Fatal("topology-only principal admitted")
	}
}
