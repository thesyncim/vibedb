package splitcontroller

import (
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestCertifiedSplitProjectsRF3ChildAndPreservesPeerCoordinates(t *testing.T) {
	plan, current, source := testReplicatedProjectionPlan(t)
	if _, err := plan.buildCertifiedReplicatedCatalogTransition(current, rangesplit.CutoverCertificate{}); err == nil {
		t.Fatal("uncertified publication accepted")
	}
	if _, err := plan.projectReplicatedSplitDescriptors(current, [32]byte{}); err == nil {
		t.Fatal("empty cutover identity accepted")
	}
	// Exercise only the deterministic metadata projection below. The public
	// publication wrapper above refuses this absent cryptographic cutover.
	descriptors, err := plan.projectReplicatedSplitDescriptors(current, [32]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 3 {
		t.Fatalf("descriptors=%d", len(descriptors))
	}
	retained, child := descriptors[0], descriptors[2]
	if retained.Group != source.Group || !reflect.DeepEqual(retained.Replicas, source.Replicas) ||
		retained.RangeIdentity != source.RangeIdentity || retained.Command.OwnershipEpoch != source.Command.OwnershipEpoch+1 ||
		retained.Command.RoutingVersion != 12 || retained.Command.RouteGeneration != 20 {
		t.Fatalf("retained=%+v", retained)
	}
	target := plan.targets[1]
	if child.Group.GroupID != target.WAL.GroupID || child.Command.RelationManifestDigest != target.RelationManifestDigest ||
		child.RangeIdentity == source.RangeIdentity || child.SplitOrigin == nil || child.SplitOrigin.RootGroup != source.Group ||
		child.SplitOrigin.ParentGroup != source.Group || child.SplitOrigin.Operation != [32]byte(plan.operation) || child.SplitOrigin.Child != 1 {
		t.Fatalf("child=%+v", child)
	}
	next, err := gateway.BuildManifestTransitionsWithReplicatedMetadata(current, []*distribution.Manifest{plan.targetManifest}, plan.next, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := next.ResolveReplicatedRoute(child.Distribution, child.Shard, workspace[:0])
	if !ok || len(route.Replicas) != 3 {
		t.Fatal("new child cannot route")
	}
	for i, replica := range route.Replicas {
		if replica.Endpoint != string(target.Replicas[i].Endpoint) || replica.NativeEndpoint != string(target.Replicas[i].NativeEndpoint) || replica.Endpoint == replica.NativeEndpoint {
			t.Fatal("native leader mistaken for Raft endpoint")
		}
	}
	path := filepath.Join(t.TempDir(), "catalog.vibejson")
	if err = gateway.SaveSnapshot(path, next); err != nil {
		t.Fatal(err)
	}
	reopened, err := gateway.LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(next.ReplicatedShardDescriptors(), reopened.ReplicatedShardDescriptors()) {
		t.Fatal("reopen lost child serving coordinates/provenance")
	}
	// Exported descriptors are detached; a caller cannot mutate live authority.
	exported := reopened.ReplicatedShardDescriptors()
	for i := range exported {
		if exported[i].SplitOrigin != nil {
			exported[i].SplitOrigin.Operation[0]++
		}
	}
	if !reflect.DeepEqual(next.ReplicatedShardDescriptors(), reopened.ReplicatedShardDescriptors()) {
		t.Fatal("caller mutated checkpoint")
	}
}

func testReplicatedProjectionPlan(t testing.TB) (*Plan, *gateway.Snapshot, gateway.ReplicatedShardDescriptor) {
	t.Helper()
	basePlan, _, target, _ := testPlanWithChildLeaders(t, []distribution.EndpointID{"node-b", "node-c", "node-d"})
	leaders := []distribution.EndpointID{"source-a", "source-b", "source-c"}
	manifest, err := distribution.NewManifest("orders", 11, []distribution.Shard{{ID: "source", AllocationGeneration: 7,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: leaders, Epoch: 5}})
	if err != nil {
		t.Fatal(err)
	}
	source := gateway.ReplicatedShardDescriptor{Distribution: "orders", Shard: "source", AllocationGeneration: 7,
		Group: raftmember.GroupKey{ClusterID: testID(1), ClusterIncarnation: testID(2), TopologyRecoveryEpoch: 1, ShardIncarnation: testID(7), GroupID: testID(8)},
		Command: raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 5,
			SchemaGeneration: 1, RelationManifestDigest: target.RelationManifestDigest, RoutingVersion: 11, RouteGeneration: 19},
		RangeIdentity: replication.Digest{1}, LineageDigest: replication.Digest{2}, ForwardingRuleDigest: replication.Digest{3}}
	endpoints := make(map[distribution.EndpointID]string)
	for i, leader := range leaders {
		replica := gateway.ReplicatedReplicaDescriptor{Member: uint64(i + 1), Node: testID(byte(80 + i)), StoreID: testID(byte(90 + i)), NodeIncarnation: 1,
			Endpoint: leader, NativeEndpoint: leader + "-native", ControlEndpoint: leader + "-control"}
		source.Replicas = append(source.Replicas, replica)
		for j, id := range []distribution.EndpointID{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint} {
			endpoints[id] = "127.0.0.1:" + strconv.Itoa(1000+i*3+j)
		}
	}
	for i, replica := range target.Replicas {
		for j, id := range []distribution.EndpointID{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint} {
			endpoints[id] = "127.0.0.1:" + strconv.Itoa(2000+i*3+j)
		}
	}
	ledger := source
	ledger.Distribution, ledger.Shard = "request-ledger", "ledger"
	ledger.Group.GroupID[0]++
	ledger.Group.ShardIncarnation[0]++
	ledger.RequestLedgerRanges = []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{9}}}
	ledgerManifest, err := distribution.NewManifest(ledger.Distribution, 11, []distribution.Shard{{ID: ledger.Shard,
		AllocationGeneration: ledger.AllocationGeneration, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: leaders, Epoch: 5}})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := gateway.NewSnapshotWithReplicatedMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "orders", Arity: 1, MapperVersion: distribution.NativeMapperVersion}, {Name: ledger.Distribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Placements:    []distribution.TablePlacement{{Table: "docs", Distribution: "orders", Columns: []string{"/tenant"}}}, Manifests: []*distribution.Manifest{manifest, ledgerManifest}},
		endpoints, 19, nil, nil, []gateway.ReplicatedShardDescriptor{source, ledger})
	if err != nil {
		t.Fatal(err)
	}
	split, err := autosplit.PlanSplit(manifest, autosplit.SplitRequest{Recommendation: autosplit.Recommendation{
		Source: basePlan.source, Kind: autosplit.RecommendationBinarySplit, Boundaries: [2]distribution.KeyspacePoint{{0x80}}, BoundaryCount: 1, CandidateBin: 32, BenefitPPM: 1},
		RetainChild: 0, NextRoutingVersion: 12, AllocationHighWater: 7, Destinations: []autosplit.Destination{{Shard: "right", AllocationGeneration: 8,
			Leaders: []distribution.EndpointID{"node-b", "node-c", "node-d"}, OwnershipEpoch: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := rangesplit.NewPartitioner(split, "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewPlan(catalog, split, partitioner, []ChildTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	return plan, catalog, source
}
