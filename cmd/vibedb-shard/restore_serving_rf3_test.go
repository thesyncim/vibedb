package main

import (
	"testing"

	"github.com/thesyncim/vibedb/distribution"
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
		Binding: sqldriver.ReplicatedShardStoreBinding{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, GroupID: group.GroupID, ShardIncarnation: group.ShardIncarnation,
			Distribution: string(gateway.ReplicatedCatalogDistribution), Shard: string(gateway.ReplicatedCatalogShard),
			AllocationGeneration: 1, MemberID: 1, StoreID: [16]byte{9},
			Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 1, ProtectionEpoch: 1,
				OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1}}}
	// Resolve the actual engine schema defaults instead of inventing a small
	// fixture relation that conceals the pre-lookup response admission contract.
	_, base.UserLimits, err = sqldriver.InitialReplicatedRelationManifest(base.Binding,
		sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}},
		sqldriver.InitialReplicatedRelationSchema{Table: gateway.ReplicatedCatalogTable, PrimaryKey: "/id"})
	if err != nil || base.UserLimits.MaxDocumentBytes <= 1024 || base.UserLimits.MaxDocumentBytes > gateway.RestoreCatalogReadAdmissionBytes {
		t.Fatalf("actual catalog schema limits=%+v err=%v", base.UserLimits, err)
	}
	state := raftservice.ServingState{Identity: raftmember.RuntimeIdentity{Group: group}}
	allows := rf3RestoreCatalogPreparingAuthority(gate, [32]byte{2}, group, base,
		func(raftservice.ServingState) bool { return true })
	request := shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe,
		Authority: operator, Capability: serviceauthz.CapabilityTopology,
		Fence: shardservice.ReplicatedFence{Group: group}}
	if !allows(state, &request) {
		t.Fatal("authenticated restore bootstrap probe rejected")
	}
	request.Operation, request.Relation, request.MaxValueBytes = shardservice.ReplicatedReadLeader, 1, gateway.RestoreCatalogReadAdmissionBytes
	request.Key, _ = orderedkey.AppendString(nil, []byte("restore/activation"), orderedkey.Ascending)
	if !allows(state, &request) {
		t.Fatal("exact activation row read rejected")
	}
	for _, maximum := range []uint32{0, 1024, gateway.RestoreCatalogReadAdmissionBytes - 1, gateway.RestoreCatalogReadAdmissionBytes + 1} {
		candidate := request
		candidate.MaxValueBytes = maximum
		if allows(state, &candidate) {
			t.Fatalf("noncanonical response ceiling %d accepted", maximum)
		}
	}
	for _, limits := range []int{0, gateway.RestoreCatalogReadAdmissionBytes + 1} {
		candidate := base
		candidate.UserLimits.MaxDocumentBytes = limits
		invalid := rf3RestoreCatalogPreparingAuthority(gate, [32]byte{2}, group, candidate,
			func(raftservice.ServingState) bool { return true })
		if invalid(state, &request) {
			t.Fatal("missing or unadmittable catalog schema accepted")
		}
	}
	for _, change := range []func(*shardservice.ReplicatedRequest){
		func(r *shardservice.ReplicatedRequest) { r.Relation = 2 },
		func(r *shardservice.ReplicatedRequest) { r.Operation = shardservice.ReplicatedReadFollower },
	} {
		candidate := request
		change(&candidate)
		if allows(state, &candidate) {
			t.Fatal("noncanonical activation read accepted")
		}
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
