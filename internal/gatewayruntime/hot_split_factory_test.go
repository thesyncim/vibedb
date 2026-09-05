package gatewayruntime

import (
	"encoding/hex"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestGatewayHotSplitFactoryFreezesPortableAndReplicaLocalIdentity(t *testing.T) {
	catalog, source, profile, work := gatewayHotSplitFactoryFixture(t)
	manifest := gatewayReplicaControlManifest{
		Shards: []gateway.ReplicatedEndpoint{
			{Node: source.Replicas[0].Node},
			{Node: source.Replicas[1].Node},
			{Node: source.Replicas[2].Node},
		},
		SplitSnapshots: []string{"127.0.0.1:9301", "127.0.0.1:9302", "127.0.0.1:9303"},
		SplitSources:   []gatewaySplitSource{gatewaySplitSourceFixture(t, source, profile)},
	}
	sources, err := gatewayHotSplitSources(manifest, catalog)
	if err != nil {
		t.Fatal(err)
	}
	factory := &gatewayHotSplitFactory{sources: sources}
	admission := [32]byte{0xa7, 0x42}
	split, err := factory.allocateSplit(catalog, admission, work, source)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := split.Child(1)
	if !ok || descriptor.Retained {
		t.Fatalf("child=%+v ok=%t", descriptor, ok)
	}
	partitioner, err := rangesplit.NewPartitioner(split, profile.Table, []string{profile.PrimaryKey}, distribution.DefaultVirtualBucketBits)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := splitcontroller.OperationIDForSplit(catalog.Generation(), split, partitioner)
	if err != nil {
		t.Fatal(err)
	}
	target, err := factory.buildChildTarget(catalog, [32]byte(operation), 1, descriptor, source, profile)
	if err != nil {
		t.Fatal(err)
	}
	if len(target.Replicas) != gateway.ServingReplicaCount ||
		target.RelationManifestDigest == source.Command.RelationManifestDigest {
		t.Fatalf("target=%+v", target)
	}
	if source.Command.ReplicaSetVersion == 1 || target.ReplicaSetVersion != 1 {
		t.Fatalf("child inherited parent membership version: parent=%d child=%d", source.Command.ReplicaSetVersion, target.ReplicaSetVersion)
	}
	configuration := sources[source.Group]
	plan, err := splitcontroller.NewPlan(catalog, split, partitioner, []splitcontroller.ChildTarget{target}, splitcontroller.PlanSourceSchema{
		SQL: configuration.SQL, Placement: configuration.Placement, LocalIndexes: configuration.LocalIndexes,
	})
	if err != nil {
		t.Fatalf("factory target rejected by production plan: %v", err)
	}
	if plan.OperationID() != operation {
		t.Fatal("planned operation differs from prepared allocation")
	}
	raw, err := splitcontroller.AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := splitcontroller.OpenPlanIntent(raw, catalog)
	if err != nil || reopened.OperationID() != operation {
		t.Fatalf("reopen exact planned intent: %v", err)
	}
	operationDirectory := hex.EncodeToString(operation[:])
	localDigests := make(map[[32]byte]struct{}, gateway.ServingReplicaCount)
	for index, replica := range target.Replicas {
		if replica.NodeIncarnation != 1 {
			t.Fatalf("fresh child incarnation = %d", replica.NodeIncarnation)
		}
		peer, peerErr := catalog.Address(replica.Endpoint)
		native, nativeErr := catalog.Address(replica.NativeEndpoint)
		control, controlErr := catalog.Address(replica.ControlEndpoint)
		if peerErr != nil || nativeErr != nil || controlErr != nil || replica.PeerAddress != peer ||
			replica.NativeAddress != native || replica.ControlAddress != control {
			t.Fatalf("replica[%d] lost exact catalog transport destinations", index)
		}
		logical, logicalErr := sqldriver.ReplicatedRelationManifestDigest(replica.SQL)
		if logicalErr != nil || replication.Digest(logical) != profile.LogicalSchemaDigest || logical == target.RelationManifestDigest {
			t.Fatalf("child logical schema differs from table or collapsed into machine digest: %v", logicalErr)
		}
		if replica.Node != source.Replicas[index].Node ||
			replica.Member != source.Replicas[index].Member ||
			replica.SQL.RelationManifestDigest == ([32]byte{}) ||
			replica.SQLPath != filepath.Join(manifest.SplitSources[0].Replicas[index].Root, operationDirectory, "child-1", "stage.vdb") ||
			replica.SnapshotAddress != manifest.SplitSnapshots[index] {
			t.Fatalf("replica[%d]=%+v", index, replica)
		}
		localDigests[replica.SQL.RelationManifestDigest] = struct{}{}
		preparation, prepareErr := splitcontroller.NewChildPreparation(
			operation,
			gatewayHotSplitDigest("allocation", [32]byte(operation), target.Child, 0),
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

func gatewaySplitSourceFixture(t testing.TB, source gateway.ReplicatedShardDescriptor, profile gateway.ReplicatedTableProfile) gatewaySplitSource {
	t.Helper()
	entry := gatewaySplitSource{Group: source.Group, SchemaGeneration: profile.SchemaGeneration,
		RelationManifestDigest: source.Command.RelationManifestDigest, Table: profile.Table, Template: gatewaySplitTemplateFixture()}
	entry.SQL = gatewaySplitSourceSQLFixture(t, source, profile)
	entry.Placement = sqldriver.ReplicatedPlacementProfile{Format: entry.Template.Format, ShardKey: entry.Template.ShardKey,
		TupleVersion: distribution.TupleVersion(entry.Template.TupleVersion), MapperVersion: distribution.MapperVersion(entry.Template.MapperVersion),
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	logical, err := sqldriver.ReplicatedRelationManifestDigest(entry.SQL)
	if err != nil {
		t.Fatal(err)
	}
	entry.LogicalSchemaDigest = replication.Digest(logical)
	for i, replica := range source.Replicas {
		entry.Replicas[i] = gatewaySplitReplica{Node: replica.Node, Root: t.TempDir()}
	}
	return entry
}

func gatewaySplitSourceSQLFixture(t testing.TB, source gateway.ReplicatedShardDescriptor, profile gateway.ReplicatedTableProfile) sqldriver.ReplicatedShardStoreIdentity {
	t.Helper()
	binding := sqldriver.ReplicatedShardStoreBinding{ClusterID: source.Group.ClusterID, ClusterIncarnation: source.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: source.Group.TopologyRecoveryEpoch, Distribution: string(source.Distribution), Shard: string(source.Shard),
		AllocationGeneration: uint64(source.AllocationGeneration), ShardIncarnation: source.Group.ShardIncarnation, GroupID: source.Group.GroupID,
		MemberID: source.Replicas[0].Member, StoreID: source.Replicas[0].StoreID,
		Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: source.Command.ActivePolicyGeneration,
			ProtectionEpoch: source.Command.ProtectionEpoch, OwnershipEpoch: source.Command.OwnershipEpoch, SchemaGeneration: source.Command.SchemaGeneration,
			RoutingVersion: source.Command.RoutingVersion, RouteGeneration: source.Command.RouteGeneration}}
	template := gatewaySplitTemplateFixture()
	base, err := sqldriver.NewReplicatedChildShardStoreIdentity(sqldriver.ShardStoreIdentity{Distribution: source.Distribution, Shard: source.Shard,
		AllocationGeneration: source.AllocationGeneration, LogID: [16]byte{0x81}}, binding, profile.Table, strings.Repeat("91", 32), profile.PrimaryKey,
		sqldriver.ReplicatedShardStoreLimits{MaxKeyBytes: int(profile.MaxKeyBytes), MaxDocumentBytes: int(profile.MaxDocumentBytes),
			MaxBatchDocuments: template.MaxBatchDocuments, MaxBatchBytes: template.MaxBatchBytes})
	if err != nil {
		t.Fatal(err)
	}
	return base
}

func gatewaySplitSourceDigestFixture(t testing.TB, source gateway.ReplicatedShardDescriptor, profile gateway.ReplicatedTableProfile) [32]byte {
	t.Helper()
	template := gatewaySplitTemplateFixture()
	digest, err := sqldriver.ReplicatedSchemaManifest(gatewaySplitSourceSQLFixture(t, source, profile),
		sqldriver.ReplicatedPlacementProfile{Format: template.Format, ShardKey: template.ShardKey,
			TupleVersion: distribution.TupleVersion(template.TupleVersion), MapperVersion: distribution.MapperVersion(template.MapperVersion),
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return digest
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
		LogicalSchemaDigest: replication.Digest{0x51}, MaxKeyBytes: 256,
		MaxDocumentBytes: 4 << 20,
	}
	source.Command.RelationManifestDigest = gatewaySplitSourceDigestFixture(t, source, profile)
	logical, err := sqldriver.ReplicatedRelationManifestDigest(gatewaySplitSourceSQLFixture(t, source, profile))
	if err != nil {
		t.Fatal(err)
	}
	source.LogicalSchemaDigest, profile.LogicalSchemaDigest = replication.Digest(logical), replication.Digest(logical)
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
