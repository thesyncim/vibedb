package splitcontroller

import (
	"context"
	"errors"
	"reflect"
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

func TestAdmittedSourceSealRoutesAndGrantsStayExactBeforeCatalogPublication(t *testing.T) {
	plan, catalog := testRF3AdmissionPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := NewDynamicShardActionRoutes(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := routes.InstallPlanRoutes(catalog, plan, admission); err != nil {
		t.Fatal(err)
	}
	state := testSourceState(plan)
	observed := Observation{Catalog: catalog, SourceState: state, SourceStatus: testLeaderStatus(state)}
	parent, err := remoteActionTarget(plan, observed, Action{Kind: ActionStartCapture})
	if err != nil {
		t.Fatal(err)
	}
	sealed := parent
	sealed.Authority.OwnershipEpoch = uint64(plan.children[plan.retained].OwnershipEpoch)
	sealed.Authority.RoutingVersion = uint64(plan.targetManifest.Version())
	sealed.Authority.RouteGeneration = plan.next
	request := shardcontrol.Request{Operation: [32]byte(plan.OperationID()), PlanDigest: admission.PlanDigest}
	action := Action{Kind: ActionCatchUpTail}
	route, err := routes.ResolveShardControl(t.Context(), sealed, action, request)
	if err != nil || !targetMatchesRoute(sealed, route) || catalog.Generation() != plan.current {
		t.Fatalf("post-seal control route unavailable before catalog CAS: %v", err)
	}
	if _, err := routes.ResolveShardControl(t.Context(), sealed, Action{Kind: ActionStartCapture}, request); err == nil {
		t.Fatal("sealed control route widened pre-seal actions")
	}
	grants, err := NewDynamicShardActionGrants(1)
	if err != nil {
		t.Fatal(err)
	}
	executor := &recordingShardActionExecutor{}
	grant := ShardActionGrant{Operation: plan.OperationID(), PlanDigest: admission.PlanDigest, Target: parent,
		Plan: plan, Observer: &testPlanObserver{operation: plan.OperationID(), observed: observed},
		Executor: executor, Actions: sourceSplitActionMask()}
	if err := grants.Install([]ShardActionGrant{grant}); err != nil {
		t.Fatal(err)
	}
	resolved, found := grants.resolve(plan.OperationID(), admission.PlanDigest, sealed)
	if !found || resolved.Executor != executor || resolved.Actions != sourceSealedActionMask() || len(grants.grants) != 1 {
		t.Fatal("sealed source did not retain exactly one lifecycle owner with restricted actions")
	}
	for _, mutate := range []func(*ShardActionTarget){
		func(v *ShardActionTarget) { v.Authority.OwnershipEpoch++ },
		func(v *ShardActionTarget) { v.Authority.SchemaGeneration++ },
		func(v *ShardActionTarget) { v.Authority.RouteGeneration++ },
		func(v *ShardActionTarget) { v.Group.GroupID[0]++ },
		func(v *ShardActionTarget) { v.RelationManifestDigest[0]++ },
	} {
		forged := sealed
		mutate(&forged)
		if _, err := routes.ResolveShardControl(t.Context(), forged, action, request); err == nil {
			t.Fatal("substituted sealed route accepted")
		}
		if _, found := grants.resolve(plan.OperationID(), admission.PlanDigest, forged); found {
			t.Fatal("substituted sealed action grant accepted")
		}
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
		ShardIncarnation: testID(7), GroupID: testID(8),
	}
	descriptor := gateway.ReplicatedShardDescriptor{
		Distribution: "orders", Shard: "source", Group: group, AllocationGeneration: 7,
		RangeIdentity: replication.Digest{1}, LineageDigest: replication.Digest{2},
		ForwardingRuleDigest: replication.Digest{3},
		RequestLedgerRanges:  []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{4}}},
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 5, SchemaGeneration: 1, RelationManifestDigest: [32]byte{9},
			RoutingVersion: 11, RouteGeneration: 19,
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
		replica := &target.Replicas[index]
		replica.Node = descriptor.Replicas[index].Node
		replica.PeerAddress = endpoints[replica.Endpoint]
		replica.NativeAddress = endpoints[replica.NativeEndpoint]
		replica.ControlAddress = endpoints[replica.ControlEndpoint]
	}
	schema := bindProjectionSourceAndChildSchemas(t, &descriptor, &target)
	catalog, err = gateway.NewSnapshotWithReplicatedMetadata(config, endpoints, 19, nil, nil, []gateway.ReplicatedShardDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewPlan(catalog, split, partitioner, []ChildTarget{target}, schema)
	if err != nil {
		t.Fatal(err)
	}
	return plan, catalog
}

func TestPreparedChildRouteBindsLogicalEndpointsToExactTransport(t *testing.T) {
	plan, catalog := testRF3AdmissionPlan(t)
	raw, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPlanIntent(raw, catalog)
	if err != nil {
		t.Fatal(err)
	}
	target, _ := plan.Target(1)
	recovered, _ := reopened.Target(1)
	if !reflect.DeepEqual(target, recovered) {
		t.Fatal("plan intent changed prepared transport destinations")
	}
	if _, err := exactPreparedChildRoute(catalog, plan, 1, target); err != nil {
		t.Fatalf("exact target rejected: %v", err)
	}
	for _, mutate := range []func(*ChildReplicaTarget){
		func(replica *ChildReplicaTarget) { replica.PeerAddress = "127.0.0.1:8888" },
		func(replica *ChildReplicaTarget) { replica.NativeAddress = "127.0.0.1:8888" },
		func(replica *ChildReplicaTarget) { replica.ControlAddress = "127.0.0.1:8888" },
		func(replica *ChildReplicaTarget) { replica.ControlAddress = "" },
	} {
		candidate := cloneChildTarget(target)
		mutate(&candidate.Replicas[0])
		if _, err := exactPreparedChildRoute(catalog, plan, 1, candidate); !errors.Is(err, ErrShardControlRoute) {
			t.Fatalf("catalog/intent transport mismatch accepted: %v", err)
		}
	}
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
