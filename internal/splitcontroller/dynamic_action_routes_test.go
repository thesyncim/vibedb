package splitcontroller

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardcontrol"
)

type outcomeUnknownAdmissionNodeClient struct {
	mu       sync.Mutex
	attempts map[rafttransport.NodeID]int
}

func (client *outcomeUnknownAdmissionNodeClient) Install(
	_ context.Context, node rafttransport.NodeID, _ *gateway.Snapshot, _ PlanAdmission,
) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.attempts[node]++
	if client.attempts[node] == 1 {
		return ErrRuntimeStoreOutcomeUnknown
	}
	return nil
}

func TestRF3AdmissionSettlesEveryMemberBeforePublishingExactRoutes(t *testing.T) {
	plan, catalog := testRF3AdmissionPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := NewDynamicShardActionRoutes(1)
	if err != nil {
		t.Fatal(err)
	}
	client := &outcomeUnknownAdmissionNodeClient{attempts: make(map[rafttransport.NodeID]int)}
	coordinator, err := NewRF3PlanAdmissionCoordinator(RF3PlanAdmissionCoordinatorOptions{
		Client: client, Routes: routes, MaxConcurrent: 3, MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = coordinator.AdmitPlan(t.Context(), catalog, plan, admission); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	if len(client.attempts) != gateway.ServingReplicaCount {
		t.Fatalf("admitted nodes=%d", len(client.attempts))
	}
	for node, attempts := range client.attempts {
		if attempts != 2 {
			t.Fatalf("node=%x attempts=%d", node, attempts)
		}
	}
	client.mu.Unlock()

	childTarget, err := remoteActionTarget(plan, Observation{}, Action{Kind: ActionStageChild, Child: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := shardcontrol.Request{Operation: [32]byte(plan.OperationID()), PlanDigest: admission.PlanDigest}
	childRoute, err := routes.ResolveShardControl(t.Context(), childTarget, Action{Kind: ActionStageChild, Child: 1}, request)
	if err != nil || childRoute.Group != childTarget.Group || len(childRoute.Replicas) != gateway.ServingReplicaCount {
		t.Fatalf("child route=%+v err=%v", childRoute, err)
	}
	for _, replica := range childRoute.Replicas {
		if replica.ControlAddress == "" || replica.Address == "" || replica.DataAddress == "" {
			t.Fatalf("unresolved child replica=%+v", replica)
		}
	}
	wrong := request
	wrong.PlanDigest[0]++
	if _, err = routes.ResolveShardControl(t.Context(), childTarget, Action{}, wrong); !errors.Is(err, ErrShardControlRoute) {
		t.Fatalf("wrong plan digest err=%v", err)
	}
}

func testRF3AdmissionPlan(t testing.TB) (*Plan, *gateway.Snapshot) {
	t.Helper()
	sourceLeaders := []distribution.EndpointID{"source-a", "source-b", "source-c"}
	childLeaders := []distribution.EndpointID{"right-a", "right-b", "right-c"}
	manifest, err := distribution.NewManifest("orders", 11, []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: sourceLeaders, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "orders", Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Placements:    []distribution.TablePlacement{{Table: "docs", Distribution: "orders", Columns: []string{"/tenant"}}},
		Manifests:     []*distribution.Manifest{manifest},
	}
	endpoints := make(map[distribution.EndpointID]string, 18)
	for index, endpoint := range append(append([]distribution.EndpointID(nil), sourceLeaders...), childLeaders...) {
		base := 1000 + index*3
		endpoints[endpoint] = "127.0.0.1:" + decimalPort(base)
		endpoints[endpoint+"-peer"] = "127.0.0.1:" + decimalPort(base+1)
		endpoints[endpoint+"-control"] = "127.0.0.1:" + decimalPort(base+2)
	}
	group := raftmember.GroupKey{
		ClusterID: testID(1), ClusterIncarnation: testID(2), TopologyRecoveryEpoch: 1,
		ShardIncarnation: testID(3), GroupID: testID(4),
	}
	descriptor := gateway.ReplicatedShardDescriptor{
		Distribution: "orders", Shard: "source", Group: group, AllocationGeneration: 7,
		RangeIdentity: replication.Digest{1}, LineageDigest: replication.Digest{2},
		ForwardingRuleDigest: replication.Digest{3},
		RequestLedgerRanges:  []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{4}}},
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 5, SchemaGeneration: 1, RelationManifestDigest: [32]byte{9},
			RoutingVersion: 11, RouteGeneration: 20,
		},
		Replicas: make([]gateway.ReplicatedReplicaDescriptor, gateway.ServingReplicaCount),
	}
	for index := range descriptor.Replicas {
		descriptor.Replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: uint64(index + 1), Node: testID(byte(20 + index)),
			StoreID: testID(byte(30 + index)), NodeIncarnation: uint64(index + 1),
			Endpoint: sourceLeaders[index], NativeEndpoint: sourceLeaders[index] + "-peer",
			ControlEndpoint: sourceLeaders[index] + "-control",
		}
	}
	catalog, err := gateway.NewSnapshotWithReplicatedMetadata(
		config, endpoints, 19, nil, nil, []gateway.ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	var boundary distribution.KeyspacePoint
	boundary[0] = 0x80
	split, err := autosplit.PlanSplit(manifest, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: autosplit.SourceIdentity{
				Distribution: "orders", Shard: "source", AllocationGeneration: 7,
				Range:          distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
				BucketBits:     distribution.DefaultVirtualBucketBits,
				RoutingVersion: 11, OwnershipEpoch: 5,
			}, Kind: autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{boundary}, BoundaryCount: 1,
			CandidateBin: 32, BenefitPPM: 1,
		}, RetainChild: 0, NextRoutingVersion: 12, AllocationHighWater: 7,
		Destinations: []autosplit.Destination{{
			Shard: "right", AllocationGeneration: 8, Leaders: childLeaders, OwnershipEpoch: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := rangesplit.NewPartitioner(
		split, "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	target := testChildTarget(t, split, partitioner)
	for index := range target.Replicas {
		target.Replicas[index].Node = descriptor.Replicas[index].Node
	}
	plan, err := NewPlan(catalog, split, partitioner, []ChildTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	return plan, catalog
}

func decimalPort(value int) string {
	const digits = "0123456789"
	var raw [10]byte
	position := len(raw)
	for value > 0 {
		position--
		raw[position] = digits[value%10]
		value /= 10
	}
	return string(raw[position:])
}
