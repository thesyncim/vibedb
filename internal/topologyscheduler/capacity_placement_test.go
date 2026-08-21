package topologyscheduler

import (
	"errors"
	"slices"
	"strconv"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

func TestPlaceSplitDestinationsUsesPressureAndFailureDomains(t *testing.T) {
	catalog, sources := capacityPlacementCatalog(t)
	candidates := []SplitCandidate{admissionCandidate(10, sources[0], 300_000, 100)}
	decision := admissionDecision(t, catalog, candidates)
	reservations := capacityReservations(1)
	nodes := []NodeCapacity{
		capacityNode("node-a", 1, 0),
		capacityNode("node-b", 3, 800),
		capacityNode("node-c", 2, 100),
		capacityNode("node-d", 2, 150),
		capacityNode("node-e", 4, 300),
	}
	policy := DefaultCapacityPlacementPolicy()
	policy.ReplicaCount = 2
	policy.MaxPhysicalMigrationBytes = 1_000
	var workspace CapacityPlacementWorkspace
	cut, err := PlaceSplitDestinations(
		catalog, candidates, decision, reservations, nodes, policy, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if cut.Count() != 1 || cut.PhysicalMigrationBytes() != 200 {
		t.Fatalf("cut count/migration = %d/%d", cut.Count(), cut.PhysicalMigrationBytes())
	}
	if got := placedEndpoints(t, cut, nodes, 0); !slices.Equal(
		got, []distribution.EndpointID{"node-c", "node-e"},
	) {
		t.Fatalf("placed endpoints = %v", got)
	}

	reversed := slices.Clone(nodes)
	slices.Reverse(reversed)
	cut, err = PlaceSplitDestinations(
		catalog, candidates, decision, reservations, reversed, policy, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := placedEndpoints(t, cut, reversed, 0); !slices.Equal(
		got, []distribution.EndpointID{"node-c", "node-e"},
	) {
		t.Fatalf("reversed-input endpoints = %v", got)
	}
}

func TestPlaceSplitDestinationsSpreadsSiblingPrimaries(t *testing.T) {
	catalog, sources := capacityPlacementCatalog(t)
	candidates := []SplitCandidate{
		admissionCandidate(10, sources[0], 300_000, 100),
		admissionCandidate(10, sources[1], 290_000, 100),
	}
	decision := admissionDecision(t, catalog, candidates)
	reservations := capacityReservations(2)
	nodes := []NodeCapacity{
		capacityNode("node-c", 2, 100), capacityNode("node-d", 3, 100),
	}
	policy := DefaultCapacityPlacementPolicy()
	policy.MaxPhysicalMigrationBytes = 1_000
	var workspace CapacityPlacementWorkspace
	cut, err := PlaceSplitDestinations(
		catalog, candidates, decision, reservations, nodes, policy, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first, second := placedEndpoints(t, cut, nodes, 0), placedEndpoints(t, cut, nodes, 1); !slices.Equal(first, []distribution.EndpointID{"node-c"}) ||
		!slices.Equal(second, []distribution.EndpointID{"node-d"}) {
		t.Fatalf("sibling endpoints = %v / %v", first, second)
	}
}

func TestCapacityPlacementBuildsFencedSplitPlans(t *testing.T) {
	catalog, sources := capacityPlacementCatalog(t)
	candidates := []SplitCandidate{
		admissionCandidate(10, sources[0], 300_000, 100),
		admissionCandidate(10, sources[1], 290_000, 100),
	}
	decision := admissionDecision(t, catalog, candidates)
	reservations := capacityReservations(2)
	nodes := []NodeCapacity{
		capacityNode("node-c", 2, 100), capacityNode("node-d", 3, 100),
	}
	policy := DefaultCapacityPlacementPolicy()
	policy.MaxPhysicalMigrationBytes = 1_000
	var workspace CapacityPlacementWorkspace
	cut, err := PlaceSplitDestinations(
		catalog, candidates, decision, reservations, nodes, policy, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildCapacityPlacedSplitPlanBatch(
		catalog, candidates, decision, reservations, nodes, policy, cut,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Count() != 2 {
		t.Fatalf("batch count = %d", batch.Count())
	}
	for index, endpoint := range []distribution.EndpointID{"node-c", "node-d"} {
		_, plan, _ := batch.PlanAt(index)
		child, _ := plan.Child(1)
		if len(child.Leaders) != 1 || child.Leaders[0] != endpoint {
			t.Fatalf("plan %d leaders = %v", index, child.Leaders)
		}
	}

	nodes[cut.destinations[0].nodes[0]].CatalogGeneration--
	if _, err := BuildCapacityPlacedSplitPlanBatch(
		catalog, candidates, decision, reservations, nodes, policy, cut,
	); !errors.Is(err, ErrInvalidCapacityPlacement) {
		t.Fatalf("mutated node error = %v", err)
	}
	nodes = []NodeCapacity{
		capacityNode("node-c", 2, 100), capacityNode("node-d", 3, 100),
	}
	nodes[cut.destinations[0].nodes[0]].Endpoint = "node-e"
	if _, err := BuildCapacityPlacedSplitPlanBatch(
		catalog, candidates, decision, reservations, nodes, policy, cut,
	); !errors.Is(err, ErrInvalidCapacityPlacement) {
		t.Fatalf("mutated endpoint error = %v", err)
	}
	nodes = []NodeCapacity{
		capacityNode("node-c", 2, 100), capacityNode("node-d", 3, 100),
	}
	policy.MaxProjectedPressurePPM++
	if _, err := BuildCapacityPlacedSplitPlanBatch(
		catalog, candidates, decision, reservations, nodes, policy, cut,
	); !errors.Is(err, ErrInvalidCapacityPlacement) {
		t.Fatalf("mutated policy error = %v", err)
	}
}

func TestPlaceSplitDestinationsFailsClosedOnCapacityAndEvidence(t *testing.T) {
	catalog, sources := capacityPlacementCatalog(t)
	candidates := []SplitCandidate{admissionCandidate(10, sources[0], 300_000, 100)}
	decision := admissionDecision(t, catalog, candidates)
	baseReservations := capacityReservations(1)
	baseNodes := []NodeCapacity{
		capacityNode("node-c", 2, 100), capacityNode("node-d", 3, 100),
	}
	basePolicy := DefaultCapacityPlacementPolicy()
	basePolicy.MaxPhysicalMigrationBytes = 1_000
	tests := []struct {
		name   string
		mutate func(*[]SplitReservation, *[]NodeCapacity, *CapacityPlacementPolicy)
	}{
		{"stale node", func(_ *[]SplitReservation, nodes *[]NodeCapacity, _ *CapacityPlacementPolicy) {
			(*nodes)[0].CatalogGeneration--
		}},
		{"duplicate endpoint", func(_ *[]SplitReservation, nodes *[]NodeCapacity, _ *CapacityPlacementPolicy) {
			(*nodes)[1].Endpoint = (*nodes)[0].Endpoint
		}},
		{"unknown endpoint", func(_ *[]SplitReservation, nodes *[]NodeCapacity, _ *CapacityPlacementPolicy) {
			(*nodes)[0].Endpoint = "missing"
		}},
		{"migration mismatch", func(reservations *[]SplitReservation, _ *[]NodeCapacity, _ *CapacityPlacementPolicy) {
			(*reservations)[0].Destinations[0].MigrationBytes++
		}},
		{"physical migration cap", func(_ *[]SplitReservation, _ *[]NodeCapacity, policy *CapacityPlacementPolicy) {
			policy.ReplicaCount = 2
			policy.MaxPhysicalMigrationBytes = 150
		}},
		{"no distinct domains", func(_ *[]SplitReservation, nodes *[]NodeCapacity, policy *CapacityPlacementPolicy) {
			policy.ReplicaCount = 2
			(*nodes)[1].FailureDomain = (*nodes)[0].FailureDomain
		}},
		{"pressure cap", func(_ *[]SplitReservation, nodes *[]NodeCapacity, _ *CapacityPlacementPolicy) {
			(*nodes)[0].Used[autosplit.ResourceLiveBytes] = 800
			(*nodes)[1].Used[autosplit.ResourceLiveBytes] = 800
		}},
		{"resource overflow", func(_ *[]SplitReservation, nodes *[]NodeCapacity, _ *CapacityPlacementPolicy) {
			(*nodes)[0].Used[autosplit.ResourceLiveBytes] = ^uint64(0)
			(*nodes)[1].Used[autosplit.ResourceLiveBytes] = ^uint64(0)
		}},
		{"receive saturation", func(_ *[]SplitReservation, nodes *[]NodeCapacity, _ *CapacityPlacementPolicy) {
			(*nodes)[0].ActiveReceives = (*nodes)[0].MaxReceives
			(*nodes)[1].ActiveReceives = (*nodes)[1].MaxReceives
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reservations := baseReservations
			nodes := slices.Clone(baseNodes)
			policy := basePolicy
			test.mutate(&reservations, &nodes, &policy)
			var workspace CapacityPlacementWorkspace
			if _, err := PlaceSplitDestinations(
				catalog, candidates, decision, reservations, nodes, policy, &workspace,
			); !errors.Is(err, ErrInvalidCapacityPlacement) {
				t.Fatalf("PlaceSplitDestinations error = %v", err)
			}
		})
	}
}

func TestPlaceSplitDestinationsIsFixedSpaceAndWarmAllocationFree(t *testing.T) {
	if size := unsafe.Sizeof(CapacityPlacementWorkspace{}); size > 320<<10 {
		t.Fatalf("CapacityPlacementWorkspace size = %d, want <= 320 KiB", size)
	}
	if size := unsafe.Sizeof(CapacityPlacementCut{}); size > 2<<10 {
		t.Fatalf("CapacityPlacementCut size = %d, want <= 2 KiB", size)
	}
	catalog, sources := capacityPlacementCatalog(t)
	candidates := []SplitCandidate{admissionCandidate(10, sources[0], 300_000, 100)}
	decision := admissionDecision(t, catalog, candidates)
	reservations := capacityReservations(1)
	nodes := []NodeCapacity{
		capacityNode("node-c", 2, 100), capacityNode("node-d", 3, 100),
	}
	policy := DefaultCapacityPlacementPolicy()
	policy.MaxPhysicalMigrationBytes = 1_000
	var workspace CapacityPlacementWorkspace
	if _, err := PlaceSplitDestinations(
		catalog, candidates, decision, reservations, nodes, policy, &workspace,
	); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1_000, func() {
		cut, err := PlaceSplitDestinations(
			catalog, candidates, decision, reservations, nodes, policy, &workspace,
		)
		if err != nil || cut.Count() != 1 {
			panic("unexpected capacity placement")
		}
	}); allocations != 0 {
		t.Fatalf("capacity placement allocations = %v, want 0", allocations)
	}
}

func BenchmarkPlaceSplitDestinations(b *testing.B) {
	endpoints := map[distribution.EndpointID]string{
		"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2",
	}
	nodes := make([]NodeCapacity, 256)
	for index := range nodes {
		endpoint := distribution.EndpointID("node-" + strconv.Itoa(index))
		endpoints[endpoint] = "127.0.0.1:" + strconv.Itoa(index+1000)
		nodes[index] = capacityNode(endpoint, uint32(index+1), uint64(index%500))
	}
	catalog, sources := capacityPlacementCatalogWithEndpoints(b, endpoints)
	candidates := []SplitCandidate{admissionCandidate(10, sources[0], 300_000, 100)}
	decision := admissionDecision(b, catalog, candidates)
	reservations := capacityReservations(1)
	policy := DefaultCapacityPlacementPolicy()
	policy.ReplicaCount = 3
	policy.MaxPhysicalMigrationBytes = 1_000
	var workspace CapacityPlacementWorkspace
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := PlaceSplitDestinations(
			catalog, candidates, decision, reservations, nodes, policy, &workspace,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func capacityPlacementCatalog(
	t testing.TB,
) (*gateway.Snapshot, [2]autosplit.SourceIdentity) {
	t.Helper()
	return capacityPlacementCatalogWithEndpoints(t, map[distribution.EndpointID]string{
		"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2",
		"node-c": "127.0.0.1:3", "node-d": "127.0.0.1:4", "node-e": "127.0.0.1:5",
	})
}

func capacityPlacementCatalogWithEndpoints(
	t testing.TB,
	endpoints map[distribution.EndpointID]string,
) (*gateway.Snapshot, [2]autosplit.SourceIdentity) {
	t.Helper()
	middle := distribution.KeyspacePoint{0x80}
	manifest, err := distribution.NewManifest("primary", 7, []distribution.Shard{
		{ID: "left", AllocationGeneration: 1,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
			Leaders: []distribution.EndpointID{"node-a"}, Epoch: 3},
		{ID: "right", AllocationGeneration: 2,
			Range:   distribution.KeyRange{Start: middle, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"node-b"}, Epoch: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: "primary", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: "primary", Columns: []string{"/tenant"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}, endpoints, 10)
	if err != nil {
		t.Fatal(err)
	}
	return catalog, [2]autosplit.SourceIdentity{
		admissionSource(manifest, 0), admissionSource(manifest, 1),
	}
}

func capacityNode(
	endpoint distribution.EndpointID,
	domain uint32,
	used uint64,
) NodeCapacity {
	return NodeCapacity{
		CatalogGeneration: 10, Endpoint: endpoint, FailureDomain: domain,
		Flags:             NodePlacementReady,
		Capacity:          autosplit.CapacityVector{autosplit.ResourceLiveBytes: 1_000},
		Used:              autosplit.CapacityVector{autosplit.ResourceLiveBytes: used},
		MigrationCapacity: 1_000, MaxReceives: 16,
	}
}

func capacityReservations(count int) []SplitReservation {
	reservations := make([]SplitReservation, count)
	for index := range reservations {
		reservations[index] = SplitReservation{
			RetainChild: 0, DestinationCount: 1,
			Destinations: [autosplit.MaxSplitChildren - 1]DestinationReservation{{
				Shard:                distribution.ShardID("new-" + string(rune('a'+index))),
				AllocationGeneration: distribution.ShardAllocationGeneration(index + 3),
				OwnershipEpoch:       1,
				Demand:               autosplit.CapacityVector{autosplit.ResourceLiveBytes: 100},
				MigrationBytes:       100,
			}},
		}
	}
	return reservations
}

func placedEndpoints(
	t testing.TB,
	cut CapacityPlacementCut,
	nodes []NodeCapacity,
	destination int,
) []distribution.EndpointID {
	t.Helper()
	endpoints := make([]distribution.EndpointID, 0, cut.replicaCount)
	for replica := 0; replica < int(cut.replicaCount); replica++ {
		ordinal, ok := cut.EndpointOrdinal(destination, replica)
		if !ok || int(ordinal) >= len(nodes) {
			t.Fatalf("endpoint ordinal %d/%d missing", destination, replica)
		}
		endpoints = append(endpoints, nodes[ordinal].Endpoint)
	}
	return endpoints
}
