package main

import (
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Internal catalog and ledger groups move through the same durable ownership
// protocol as application groups. Their private table profiles must still
// supply the canonical logical schema digest used by that protocol.
func TestDevCatalogAllGroupsRetainOwnedMoveAcrossEnrollmentHeads(t *testing.T) {
	snapshot, dataRoute, _ := testDevCatalogSnapshot(t)
	var config distribution.ClusterConfig
	for index := 0; index < snapshot.DistributionCount(); index++ {
		spec, _ := snapshot.DistributionAt(index)
		config.Distributions = append(config.Distributions, spec)
	}
	for index := 0; index < snapshot.PlacementCount(); index++ {
		placement, _ := snapshot.PlacementAt(index)
		config.Placements = append(config.Placements, placement)
	}
	for index := 0; index < snapshot.ManifestCount(); index++ {
		manifest, _ := snapshot.ManifestAt(index)
		config.Manifests = append(config.Manifests, manifest)
	}
	wantLogical := map[distribution.DistributionName]replication.Digest{
		gateway.ReplicatedCatalogDistribution: {0x71}, devLedgerDistribution: {0x72}, devDataDistribution: {0x73},
	}
	for index, original := range snapshot.ReplicatedShardDescriptors() {
		t.Run(string(original.Distribution), func(t *testing.T) {
			if original.LogicalSchemaDigest != wantLogical[original.Distribution] {
				t.Errorf("logical schema digest = %x, want canonical prepared schema %x", original.LogicalSchemaDigest, wantLogical[original.Distribution])
			}
			descriptors := snapshot.ReplicatedShardDescriptors()
			target := gateway.ReplicatedReplicaDescriptor{Member: 4, Node: [16]byte{4}, StoreID: [16]byte{90}, NodeIncarnation: 1,
				Endpoint: "enrolled", NativeEndpoint: "enrolled-native", ControlEndpoint: "enrolled-control"}
			descriptors[index].EnrolledTarget = &target
			endpoints := make(map[distribution.EndpointID]string)
			for _, descriptor := range descriptors {
				for _, replica := range descriptor.Replicas {
					for _, endpoint := range []distribution.EndpointID{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint} {
						endpoints[endpoint], _ = snapshot.Address(endpoint)
					}
				}
			}
			endpoints[target.Endpoint] = "127.0.0.1:31000"
			endpoints[target.NativeEndpoint] = "127.0.0.1:31001"
			endpoints[target.ControlEndpoint] = "127.0.0.1:31002"
			cut := func(generation uint64) *gateway.Snapshot {
				result, err := gateway.NewSnapshotWithReplicatedTableMetadata(config, endpoints, generation, nil, nil, descriptors, []gateway.ReplicatedTableProfile{dataRoute.table})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			source := cut(4)
			retiring := original.Replicas[0]
			plan, err := rebalance.PlanReplicaMove(source, raftmodel.Publication{Applied: 10, ReplicaSetVersion: 1, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}, rebalance.MoveRequest{
				Distribution: original.Distribution, Shard: original.Shard, Group: original.Group,
				RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4, Source: retiring.Endpoint, Target: target.Endpoint,
				RetiringReplica: rebalance.ReplicaIdentity{Member: retiring.Member, Node: retiring.Node, StoreID: retiring.StoreID, NodeIncarnation: retiring.NodeIncarnation, ControlEndpoint: retiring.ControlEndpoint},
			})
			if err != nil {
				t.Fatal(err)
			}
			if transition, ok := plan.TransitionIntent(); !ok || transition.SourceDescriptor.LogicalSchemaDigest != original.LogicalSchemaDigest {
				t.Error("enrolled group lost its durable ownership transition")
			}
			intent, err := rebalance.AppendReplicaMoveIntent(nil, source, plan)
			if err != nil {
				t.Fatal(err)
			}
			group := original.Group
			certificate := &replicatedstate.SnapshotBaseCertificate{Digest: [32]byte{0x91}, Manifest: replicatedstate.SnapshotArtifactManifest{State: replicatedstate.State{
				Binding: replicatedstate.Binding{
					ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation, TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
					ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID, Distribution: string(original.Distribution), Shard: string(original.Shard),
					AllocationGeneration: 1, OwnershipEpoch: 1, RoutingVersion: 1, RouteGeneration: 1,
				},
				Applied: 12, ReplicaSetVersion: 11, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}},
			}}}
			// A different enrollment publishes head five while the snapshot's own
			// group command remains at route generation one.
			recovered, err := rebalance.OpenReplicaMoveIntent(intent, cut(5), raftmodel.Publication{Applied: 15, ReplicaSetVersion: 13, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}}, certificate)
			if err != nil {
				t.Fatalf("catalog head five with group route generation one: %v", err)
			}
			if recovered.OperationID() != plan.OperationID() || recovered.CatalogGeneration() != 4 {
				t.Fatal("recovery changed the original move provenance")
			}
		})
	}
}
