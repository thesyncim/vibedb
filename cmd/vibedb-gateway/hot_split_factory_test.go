package main

import (
	"encoding/hex"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
)

func TestGatewayHotSplitFactoryFreezesPortableAndReplicaLocalIdentity(t *testing.T) {
	catalog, source, profile, work := gatewayHotSplitFactoryFixture(t)
	manifest := gatewayReplicaControlManifest{
		Shards: []gateway.ReplicatedEndpoint{
			{Node: source.Replicas[0].Node},
			{Node: source.Replicas[1].Node},
			{Node: source.Replicas[2].Node},
		},
		SplitChildRoots: []string{
			filepath.Join(t.TempDir(), "one"),
			filepath.Join(t.TempDir(), "two"),
			filepath.Join(t.TempDir(), "three"),
		},
		SplitSnapshots: []string{"127.0.0.1:9301", "127.0.0.1:9302", "127.0.0.1:9303"},
		SplitTemplate:  gatewaySplitTemplateFixture(),
	}
	factory := &gatewayHotSplitFactory{manifest: manifest}
	admission := [32]byte{0xa7, 0x42}
	split, err := factory.allocateSplit(catalog, admission, work, source)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := split.Child(1)
	if !ok || descriptor.Retained {
		t.Fatalf("child=%+v ok=%t", descriptor, ok)
	}
	target, err := factory.buildChildTarget(catalog, admission, 1, descriptor, source, profile)
	if err != nil {
		t.Fatal(err)
	}
	if len(target.Replicas) != gateway.ServingReplicaCount ||
		target.RelationManifestDigest != source.Command.RelationManifestDigest {
		t.Fatalf("target=%+v", target)
	}
	operationDirectory := hex.EncodeToString(admission[:])
	localDigests := make(map[[32]byte]struct{}, gateway.ServingReplicaCount)
	for index, replica := range target.Replicas {
		if replica.Node != source.Replicas[index].Node ||
			replica.Member != source.Replicas[index].Member ||
			replica.SQL.RelationManifestDigest == ([32]byte{}) ||
			replica.SQLPath != filepath.Join(manifest.SplitChildRoots[index], operationDirectory, "child-1", "stage.vdb") ||
			replica.SnapshotAddress != manifest.SplitSnapshots[index] {
			t.Fatalf("replica[%d]=%+v", index, replica)
		}
		localDigests[replica.SQL.RelationManifestDigest] = struct{}{}
		preparation, prepareErr := splitcontroller.NewChildPreparation(
			splitcontroller.OperationID(admission),
			gatewayHotSplitDigest("allocation", admission, target.Child, 0),
			descriptor, profile.Table, target, uint8(index),
		)
		if prepareErr != nil || !reflect.DeepEqual(preparation.ReplicaTarget().SQL, replica.SQL) {
			t.Fatalf("replica[%d] preparation=%+v err=%v", index, preparation, prepareErr)
		}
	}
	if len(localDigests) != gateway.ServingReplicaCount {
		t.Fatalf("replica-local relation identities collapsed: %x", localDigests)
	}
}

func gatewayHotSplitFactoryFixture(
	t testing.TB,
) (*gateway.Snapshot, gateway.ReplicatedShardDescriptor, gateway.ReplicatedTableProfile, hotshard.SplitWork) {
	t.Helper()
	full := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	manifest, err := distribution.NewManifest("data", 7, []distribution.Shard{{
		ID: "all", AllocationGeneration: 11, Range: full,
		Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Epoch: 13,
	}})
	if err != nil {
		t.Fatal(err)
	}
	group := raftmember.GroupKey{TopologyRecoveryEpoch: 3}
	group.ClusterID[0], group.ClusterIncarnation[0], group.ShardIncarnation[0], group.GroupID[0] = 1, 2, 3, 4
	replicas := []gateway.ReplicatedReplicaDescriptor{
		{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{11}, NodeIncarnation: 21, Endpoint: "peer-a", NativeEndpoint: "native-a", ControlEndpoint: "control-a"},
		{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{12}, NodeIncarnation: 22, Endpoint: "peer-b", NativeEndpoint: "native-b", ControlEndpoint: "control-b"},
		{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{13}, NodeIncarnation: 23, Endpoint: "peer-c", NativeEndpoint: "native-c", ControlEndpoint: "control-c"},
	}
	source := gateway.ReplicatedShardDescriptor{
		Distribution: "data", Shard: "all", Group: group, AllocationGeneration: 11,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 7, OwnershipEpoch: 13, RoutingVersion: 7, RouteGeneration: 9,
			ActivePolicyGeneration: 1, ProtectionEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{0x51},
		},
		RangeIdentity: replication.Digest{0x61}, LineageDigest: replication.Digest{0x62},
		ForwardingRuleDigest: replication.Digest{0x63}, Replicas: replicas,
	}
	profile := gateway.ReplicatedTableProfile{
		Table: "messages", Relation: 1, PrimaryKey: "/id", SchemaGeneration: 1,
		RelationManifestDigest: replication.Digest{0x51}, MaxKeyBytes: 256,
		MaxDocumentBytes: 4 << 20,
	}
	endpoints := map[distribution.EndpointID]string{
		"peer-a": "127.0.0.1:1", "peer-b": "127.0.0.1:2", "peer-c": "127.0.0.1:3",
		"native-a": "127.0.0.1:11", "native-b": "127.0.0.1:12", "native-c": "127.0.0.1:13",
		"control-a": "127.0.0.1:21", "control-b": "127.0.0.1:22", "control-c": "127.0.0.1:23",
	}
	catalog, err := gateway.NewSnapshotWithReplicatedTableMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Placements:    []distribution.TablePlacement{{Table: "messages", Distribution: "data", Columns: []string{"/id"}}},
		Manifests:     []*distribution.Manifest{manifest},
	}, endpoints, 9, nil, nil, []gateway.ReplicatedShardDescriptor{source}, []gateway.ReplicatedTableProfile{profile})
	if err != nil {
		t.Fatal(err)
	}
	boundary := distribution.KeyspacePoint{0x80}
	work := hotshard.SplitWork{Group: group, Candidate: topologyscheduler.SplitCandidate{
		CatalogGeneration: catalog.Generation(), Recommendation: autosplit.Recommendation{
			Source: autosplit.SourceIdentity{Distribution: "data", Shard: "all", AllocationGeneration: 11,
				Range: full, BucketBits: distribution.DefaultVirtualBucketBits, RoutingVersion: 7, OwnershipEpoch: 13},
			WindowSequence: 1, Kind: autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{boundary}, BoundaryCount: 1, CandidateBin: 32,
			CurrentPressurePPM: 1_100_000, BenefitPPM: 200_000,
		},
	}}
	return catalog, source, profile, work
}
