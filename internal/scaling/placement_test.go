package scaling

import (
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

const placementGeneration = 7

type placementFixture struct {
	snapshot    *gateway.Snapshot
	nodes       []gateway.NodeRecord
	descriptors []gateway.ReplicatedShardDescriptor
	demands     []ReplicaDemand
}

func newPlacementFixture(t *testing.T, shardCount int) placementFixture {
	t.Helper()
	if shardCount < 1 {
		t.Fatal("placement fixture requires a shard")
	}
	endpoints := make(map[distribution.EndpointID]string, shardCount*9)
	shards := make([]distribution.Shard, shardCount)
	descriptors := make([]gateway.ReplicatedShardDescriptor, shardCount)
	for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
		shardID := distribution.ShardID("s" + string(rune('0'+shardIndex)))
		allocation := distribution.ShardAllocationGeneration(shardIndex + 1)
		start := distribution.KeyspacePoint{}
		if shardIndex != 0 {
			start[0] = byte(shardIndex)
		}
		end := distribution.KeyspaceEnd{Max: shardIndex == shardCount-1}
		if !end.Max {
			end.Point[0] = byte(shardIndex + 1)
		}
		leaderEndpoints := make([]distribution.EndpointID, gateway.ServingReplicaCount)
		replicas := make([]gateway.ReplicatedReplicaDescriptor, gateway.ServingReplicaCount)
		for ordinal := 0; ordinal < gateway.ServingReplicaCount; ordinal++ {
			nodeNumber := ordinal + 1
			data := distribution.EndpointID("data-" + string(rune('0'+nodeNumber)))
			native := distribution.EndpointID("native-" + string(rune('0'+nodeNumber)))
			control := distribution.EndpointID("control-" + string(rune('0'+nodeNumber)))
			leaderEndpoints[ordinal] = data
			endpoints[data] = "127.0.0.1:" + string(rune('1'+nodeNumber)) + "00"
			endpoints[native] = "127.0.0.1:" + string(rune('2'+nodeNumber)) + "00"
			endpoints[control] = "127.0.0.1:" + string(rune('3'+nodeNumber)) + "00"
			replicas[ordinal] = gateway.ReplicatedReplicaDescriptor{
				Member:          uint64(nodeNumber),
				Node:            rafttransport.NodeID{byte(nodeNumber)},
				StoreID:         [16]byte{byte(0x20 + shardIndex*gateway.ServingReplicaCount + ordinal)},
				NodeIncarnation: 1,
				Endpoint:        data,
				NativeEndpoint:  native,
				ControlEndpoint: control,
			}
		}
		shards[shardIndex] = distribution.Shard{
			ID:                   shardID,
			AllocationGeneration: allocation,
			Range:                distribution.KeyRange{Start: start, End: end},
			Leaders:              leaderEndpoints,
			Epoch:                1,
		}
		descriptors[shardIndex] = gateway.ReplicatedShardDescriptor{
			Distribution:         "data",
			Shard:                shardID,
			Group:                placementGroup(byte(shardIndex + 1)),
			AllocationGeneration: allocation,
			Command:              placementCommand(),
			RangeIdentity:        replication.Digest{byte(0x40 + shardIndex)},
			LineageDigest:        replication.Digest{byte(0x50 + shardIndex)},
			ForwardingRuleDigest: replication.Digest{byte(0x60 + shardIndex)},
			Replicas:             replicas,
		}
	}
	manifest, err := distribution.NewManifest("data", 1, shards)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(
		distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
			Manifests:     []*distribution.Manifest{manifest},
		}, endpoints, placementGeneration, nil, nil, descriptors,
	)
	if err != nil {
		t.Fatal(err)
	}
	nodes := make([]gateway.NodeRecord, 5)
	for index := range nodes {
		nodes[index] = placementNode(index+1, gateway.NodeActive)
		if index < gateway.ServingReplicaCount {
			nodes[index].Used[autosplit.ResourceLiveBytes] = 100
		}
	}
	demands := make([]ReplicaDemand, 0, shardCount*gateway.ServingReplicaCount)
	for _, descriptor := range descriptors {
		for ordinal := uint8(0); ordinal < gateway.ServingReplicaCount; ordinal++ {
			demands = append(demands, ReplicaDemand{
				CatalogGeneration: placementGeneration,
				Group:             descriptor.Group,
				ReplicaOrdinal:    ordinal,
				Demand:            placementDemand(100),
				MigrationBytes:    100,
			})
		}
	}
	return placementFixture{snapshot: snapshot, nodes: nodes, descriptors: descriptors, demands: demands}
}

func placementNode(number int, lifecycle gateway.NodeLifecycle) gateway.NodeRecord {
	capacity := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 1_000
	}
	return gateway.NodeRecord{
		NodeID:            rafttransport.NodeID{byte(number)},
		Incarnation:       1,
		DataEndpoint:      distribution.EndpointID("node-data-" + string(rune('0'+number))),
		NativeEndpoint:    distribution.EndpointID("node-native-" + string(rune('0'+number))),
		ControlEndpoint:   distribution.EndpointID("node-control-" + string(rune('0'+number))),
		DataAddress:       "10.0.0." + string(rune('0'+number)) + ":7100",
		NativeAddress:     "10.0.0." + string(rune('0'+number)) + ":7200",
		ControlAddress:    "10.0.0." + string(rune('0'+number)) + ":7300",
		FailureDomain:     "zone-" + string(rune('0'+number)),
		Roles:             gateway.NodeRoleStorage,
		Capacity:          capacity,
		MigrationCapacity: 1_000_000,
		MaxReceives:       8,
		Lifecycle:         lifecycle,
		Revision:          1,
		CatalogGeneration: placementGeneration,
	}
}

func placementDemand(liveBytes uint64) autosplit.CapacityVector {
	demand := autosplit.CapacityVector{}
	demand[autosplit.ResourceLiveBytes] = liveBytes
	return demand
}

func placementGroup(seed byte) raftmember.GroupKey {
	return raftmember.GroupKey{
		ClusterID:             [16]byte{1},
		ClusterIncarnation:    [16]byte{2},
		TopologyRecoveryEpoch: 3,
		ShardIncarnation:      [16]byte{seed},
		GroupID:               [16]byte{seed + 0x10},
	}
}

func placementCommand() raftservice.CommandFence {
	return raftservice.CommandFence{
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: 1,
		ProtectionEpoch:        1,
		OwnershipEpoch:         1,
		SchemaGeneration:       1,
		RelationManifestDigest: [32]byte{9},
		RoutingVersion:         1,
		RouteGeneration:        1,
	}
}

func placementIdentity(replica gateway.ReplicatedReplicaDescriptor) gateway.ReplicaIdentity {
	return gateway.ReplicaIdentity{
		Member:          replica.Member,
		Node:            replica.Node,
		NodeIncarnation: replica.NodeIncarnation,
		StoreID:         replica.StoreID,
		Endpoint:        replica.Endpoint,
		NativeEndpoint:  replica.NativeEndpoint,
		ControlEndpoint: replica.ControlEndpoint,
	}
}

func placementInFlightIntent(descriptor gateway.ReplicatedShardDescriptor, target gateway.NodeRecord) gateway.GroupEnrollmentIntent {
	source := placementIdentity(descriptor.Replicas[0])
	targetIdentity := gateway.ReplicaIdentity{
		Member:          4,
		Node:            target.NodeID,
		NodeIncarnation: target.Incarnation,
		StoreID:         [16]byte{0xf4},
		Endpoint:        distribution.EndpointID("target-data"),
		NativeEndpoint:  distribution.EndpointID("target-native"),
		ControlEndpoint: distribution.EndpointID("target-control"),
	}
	return gateway.GroupEnrollmentIntent{
		IntentID:                 [32]byte{0xa1},
		Group:                    descriptor.Group,
		Distribution:             descriptor.Distribution,
		Shard:                    descriptor.Shard,
		AllocationGeneration:     descriptor.AllocationGeneration,
		CatalogGeneration:        placementGeneration,
		ReplicaOrdinal:           0,
		Source:                   source,
		SnapshotSourceMember:     source.Member,
		Target:                   targetIdentity,
		ExpectedRosterDigest:     replication.Digest{0x71},
		ExpectedDescriptorDigest: replication.Digest{0x72},
		ExpectedManifestDigest:   replication.Digest{0x73},
		ExpectedCommand:          descriptor.Command,
		TargetNodeRevision:       target.Revision,
		State:                    gateway.EnrollmentReserved,
		Revision:                 1,
	}
}

func placementRequest(kind gateway.ScalingKind, target int) gateway.ScalingIntentRequest {
	request := gateway.ScalingIntentRequest{
		Kind:      kind,
		RequestID: [32]byte{byte(0x90 + target)},
		MaxMoves:  1,
	}
	if target != 0 {
		request.Targets = []gateway.NodeReference{{NodeID: rafttransport.NodeID{byte(target)}, Incarnation: 1}}
	}
	return request
}

func placementBlocker(plan PlacementPlan, code string) bool {
	for _, blocker := range plan.Blockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func TestPlanScaleInMovesFollowerAndAllowsOverloadedSource(t *testing.T) {
	fixture := newPlacementFixture(t, 1)
	fixture.nodes[1].Lifecycle = gateway.NodeDraining
	fixture.nodes[1].Capacity[autosplit.ResourceLiveBytes] = 50
	fixture.nodes[1].Used[autosplit.ResourceLiveBytes] = 200
	request := placementRequest(gateway.ScalingScaleIn, 4)
	request.Drain = gateway.NodeReference{NodeID: fixture.nodes[1].NodeID, Incarnation: 1}
	plan, err := Plan(PlacementInput{
		Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementMoves || len(plan.Moves) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	move := plan.Moves[0]
	if move.ReplicaOrdinal != 1 || move.Source.Member != 2 || move.SourceNode.NodeID != fixture.nodes[1].NodeID ||
		move.TargetNode.NodeID != fixture.nodes[3].NodeID || move.ExpectedCatalogGeneration != placementGeneration ||
		move.Source.NativeEndpoint == "" || move.Source.ControlEndpoint == "" {
		t.Fatalf("follower move = %+v", move)
	}
}

func TestPlanScaleOutUsesActiveEmptyTargetAndMeasuredColdDemand(t *testing.T) {
	fixture := newPlacementFixture(t, 1)
	request := placementRequest(gateway.ScalingScaleOut, 4)
	plan, err := Plan(PlacementInput{
		Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementMoves || len(plan.Moves) != 1 || plan.Moves[0].TargetNode.NodeID != fixture.nodes[3].NodeID {
		t.Fatalf("scale-out plan = %+v", plan)
	}
	if plan.Moves[0].Demand[autosplit.ResourceLiveBytes] != 100 || plan.Moves[0].MigrationBytes != 100 {
		t.Fatalf("cold replica demand was not retained: %+v", plan.Moves[0])
	}

	joining := fixture.nodes[3]
	joining.Lifecycle = gateway.NodeJoining
	fixture.nodes[3] = joining
	blocked, err := Plan(PlacementInput{
		Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands,
	})
	if err != nil {
		t.Fatal(err)
	}
	if blocked.State != PlacementBlocked || !placementBlocker(blocked, BlockerInvalidLifecycle) {
		t.Fatalf("joining target was admitted: %+v", blocked)
	}
}

func TestPlanRequiresGenerationFencedStorageEvidence(t *testing.T) {
	fixture := newPlacementFixture(t, 1)
	request := placementRequest(gateway.ScalingScaleOut, 4)
	plan, err := Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementBlocked || len(plan.Moves) != 0 || !placementBlocker(plan, BlockerCapacityEvidence) {
		t.Fatalf("missing evidence was not blocked: %+v", plan)
	}

	bad := fixture.demands[0]
	bad.Demand = autosplit.CapacityVector{}
	bad.MigrationBytes = 0
	if _, err := Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: []ReplicaDemand{bad}}); err == nil {
		t.Fatal("zero non-empty demand was accepted")
	}
}

func TestPlanStaleIdentityFailureDomainAndMigrationBlockers(t *testing.T) {
	fixture := newPlacementFixture(t, 1)
	request := placementRequest(gateway.ScalingScaleOut, 4)
	request.Targets[0].Incarnation = 2
	plan, err := Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementBlocked || !placementBlocker(plan, BlockerStaleIncarnation) {
		t.Fatalf("stale target was not blocked: %+v", plan)
	}

	fixture = newPlacementFixture(t, 1)
	fixture.nodes[3].MigrationCapacity = 50
	request = placementRequest(gateway.ScalingScaleOut, 4)
	plan, err = Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementBlocked || !placementBlocker(plan, BlockerMigrationCapacity) {
		t.Fatalf("migration cap was not enforced: %+v", plan)
	}

	fixture = newPlacementFixture(t, 1)
	fixture.nodes[3].FailureDomain = fixture.nodes[2].FailureDomain
	fixture.nodes[1].Lifecycle = gateway.NodeDraining
	request = placementRequest(gateway.ScalingScaleIn, 4)
	request.Drain = gateway.NodeReference{NodeID: fixture.nodes[1].NodeID, Incarnation: 1}
	plan, err = Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementBlocked || !placementBlocker(plan, BlockerFailureDomain) {
		t.Fatalf("failure-domain collision was not enforced: %+v", plan)
	}
}

func TestPlanRebalanceHysteresisIsValidNoWork(t *testing.T) {
	fixture := newPlacementFixture(t, 1)
	fixture.nodes[0].Used[autosplit.ResourceLiveBytes] = 500
	fixture.nodes[3].Used[autosplit.ResourceLiveBytes] = 450
	fixture.nodes[4].Used[autosplit.ResourceLiveBytes] = 450
	request := placementRequest(gateway.ScalingRebalance, 0)
	plan, err := Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementNoWork || len(plan.Moves) != 0 || !placementBlocker(plan, BlockerNoImprovement) {
		t.Fatalf("balanced rebalance was not no-work: %+v", plan)
	}
}

func TestPlanBatchContinuationIsDeterministic(t *testing.T) {
	fixture := newPlacementFixture(t, 2)
	request := placementRequest(gateway.ScalingScaleOut, 4)
	first, err := Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands})
	if err != nil {
		t.Fatal(err)
	}
	if first.State != PlacementMoves || len(first.Moves) != 1 || first.RemainingReplicas == 0 {
		t.Fatalf("batch did not expose continuation: %+v", first)
	}

	reversedNodes := append([]gateway.NodeRecord(nil), fixture.nodes...)
	sort.Slice(reversedNodes, func(i, j int) bool { return reversedNodes[i].NodeID[0] > reversedNodes[j].NodeID[0] })
	reversedDemands := append([]ReplicaDemand(nil), fixture.demands...)
	sort.Slice(reversedDemands, func(i, j int) bool {
		if reversedDemands[i].Group != reversedDemands[j].Group {
			return compareGroups(reversedDemands[i].Group, reversedDemands[j].Group) > 0
		}
		return reversedDemands[i].ReplicaOrdinal > reversedDemands[j].ReplicaOrdinal
	})
	second, err := Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: reversedNodes, Request: request, Demands: reversedDemands})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Moves, second.Moves) || first.State != second.State || first.RemainingReplicas != second.RemainingReplicas {
		t.Fatalf("non-deterministic plans:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func TestPlanOverflowAndKnownEmptyEvidence(t *testing.T) {
	fixture := newPlacementFixture(t, 1)
	fixture.nodes[3].Capacity[autosplit.ResourceLiveBytes] = math.MaxUint64
	fixture.nodes[3].Used[autosplit.ResourceLiveBytes] = math.MaxUint64
	request := placementRequest(gateway.ScalingScaleOut, 4)
	plan, err := Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementBlocked || !placementBlocker(plan, BlockerOverflow) {
		t.Fatalf("target overflow was not blocked: %+v", plan)
	}

	fixture = newPlacementFixture(t, 1)
	fixture.nodes[1].Lifecycle = gateway.NodeDraining
	fixture.nodes[1].Used = autosplit.CapacityVector{}
	for index := range fixture.demands {
		if fixture.demands[index].ReplicaOrdinal == 1 {
			fixture.demands[index].Demand = autosplit.CapacityVector{}
			fixture.demands[index].MigrationBytes = 0
			fixture.demands[index].KnownEmpty = true
		}
	}
	request = placementRequest(gateway.ScalingScaleIn, 4)
	request.Drain = gateway.NodeReference{NodeID: fixture.nodes[1].NodeID, Incarnation: 1}
	plan, err = Plan(PlacementInput{Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request, Demands: fixture.demands})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementMoves || len(plan.Moves) != 1 || !zeroCapacityVector(plan.Moves[0].Demand) {
		t.Fatalf("known-empty replica was not admitted: %+v", plan)
	}
}

func TestPlanReservesMeasuredInFlightTargetBeforeOtherGroups(t *testing.T) {
	fixture := newPlacementFixture(t, 2)
	fixture.nodes[3].Capacity[autosplit.ResourceLiveBytes] = 150
	for index := range fixture.demands {
		if fixture.demands[index].Group == fixture.descriptors[0].Group && fixture.demands[index].ReplicaOrdinal == 0 {
			fixture.demands[index].Demand[autosplit.ResourceLiveBytes] = 80
			fixture.demands[index].MigrationBytes = 80
		}
	}
	request := placementRequest(gateway.ScalingScaleOut, 4)
	intent := placementInFlightIntent(fixture.descriptors[0], fixture.nodes[3])
	plan, err := Plan(PlacementInput{
		Snapshot: fixture.snapshot, Nodes: fixture.nodes, Request: request,
		Demands: fixture.demands, InFlight: []gateway.GroupEnrollmentIntent{intent},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.State != PlacementBlocked || len(plan.Moves) != 0 || !placementBlocker(plan, BlockerTargetCapacity) {
		t.Fatalf("in-flight target reservation was ignored: %+v", plan)
	}
}
