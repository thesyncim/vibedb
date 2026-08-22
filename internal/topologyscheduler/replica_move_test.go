package topologyscheduler

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"slices"
	"strconv"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

func TestSelectReplicaMovesRelievesDominantPressureAndPreservesDomains(t *testing.T) {
	endpoints := replicaMoveEndpoints("source", "follower", "node-c", "node-d", "node-e")
	catalog, sources := replicaMoveCatalog(t, 1, []distribution.EndpointID{"source", "follower"}, endpoints)
	candidates := []ReplicaMoveCandidate{replicaMoveCandidate(sources[0], 300, 100)}
	nodes := []NodeCapacity{
		replicaMoveNode("source", 1, 900),
		replicaMoveNode("follower", 2, 400),
		replicaMoveNode("node-c", 3, 100),
		replicaMoveNode("node-d", 2, 0),
		replicaMoveNode("node-e", 4, 200),
	}
	policy := DefaultReplicaMovePolicy()
	var workspace ReplicaMoveWorkspace
	cut, err := SelectReplicaMoves(catalog, candidates, nodes, policy, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := ResolveReplicaMove(catalog, candidates, nodes, policy, cut, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cut.Count() != 1 || cut.PhysicalMigrationBytes() != 100 ||
		selection.SourceEndpoint != "source" || selection.TargetEndpoint != "node-c" ||
		selection.Source != sources[0] || selection.Demand != candidates[0].Demand {
		t.Fatalf("cut/selection = %+v / %+v", cut, selection)
	}

	reversed := slices.Clone(nodes)
	slices.Reverse(reversed)
	cut, err = SelectReplicaMoves(catalog, candidates, reversed, policy, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	selection, err = ResolveReplicaMove(catalog, candidates, reversed, policy, cut, 0)
	if err != nil || selection.TargetEndpoint != "node-c" {
		t.Fatalf("reversed selection = %+v, %v", selection, err)
	}
}

func TestSelectReplicaMovesAccountsForSiblingReservations(t *testing.T) {
	endpoints := replicaMoveEndpoints("source", "follower", "node-c", "node-d")
	catalog, sources := replicaMoveCatalog(t, 2, []distribution.EndpointID{"source", "follower"}, endpoints)
	candidates := []ReplicaMoveCandidate{
		replicaMoveCandidate(sources[0], 200, 100),
		replicaMoveCandidate(sources[1], 200, 100),
	}
	nodes := []NodeCapacity{
		replicaMoveNode("source", 1, 900),
		replicaMoveNode("follower", 2, 300),
		replicaMoveNode("node-c", 3, 100),
		replicaMoveNode("node-d", 4, 100),
	}
	policy := DefaultReplicaMovePolicy()
	policy.MaxMoves = 2
	policy.MaxMovesPerTargetNode = 1
	var workspace ReplicaMoveWorkspace
	cut, err := SelectReplicaMoves(catalog, candidates, nodes, policy, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	if cut.Count() != 2 || cut.PhysicalMigrationBytes() != 200 {
		t.Fatalf("cut = %+v", cut)
	}
	first, err := ResolveReplicaMove(catalog, candidates, nodes, policy, cut, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ResolveReplicaMove(catalog, candidates, nodes, policy, cut, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.TargetEndpoint != "node-c" || second.TargetEndpoint != "node-d" {
		t.Fatalf("reserved targets = %q / %q", first.TargetEndpoint, second.TargetEndpoint)
	}

	slices.Reverse(candidates)
	cut, err = SelectReplicaMoves(catalog, candidates, nodes, policy, &workspace)
	if err != nil || cut.Count() != 2 {
		t.Fatalf("reversed cut/error = %+v / %v", cut, err)
	}
	first, _ = ResolveReplicaMove(catalog, candidates, nodes, policy, cut, 0)
	second, _ = ResolveReplicaMove(catalog, candidates, nodes, policy, cut, 1)
	if first.Source.Shard != "shard-0" || first.TargetEndpoint != "node-c" ||
		second.Source.Shard != "shard-1" || second.TargetEndpoint != "node-d" {
		t.Fatalf("reversed reservations = %+v / %+v", first, second)
	}
}

func TestSelectReplicaMovesRejectsRemainingFailureDomainCollision(t *testing.T) {
	endpoints := replicaMoveEndpoints("source", "follower-a", "follower-b", "node-c")
	catalog, sources := replicaMoveCatalog(
		t, 1, []distribution.EndpointID{"source", "follower-a", "follower-b"}, endpoints,
	)
	candidates := []ReplicaMoveCandidate{replicaMoveCandidate(sources[0], 300, 100)}
	nodes := []NodeCapacity{
		replicaMoveNode("source", 1, 900),
		replicaMoveNode("follower-a", 2, 300),
		replicaMoveNode("follower-b", 2, 300),
		replicaMoveNode("node-c", 3, 100),
	}
	policy := DefaultReplicaMovePolicy()
	var workspace ReplicaMoveWorkspace
	cut, err := SelectReplicaMoves(catalog, candidates, nodes, policy, &workspace)
	if err != nil || cut.Count() != 0 || cut.Invalid != 1 {
		t.Fatalf("domain-collision cut/error = %+v / %v", cut, err)
	}
	policy.DistinctFailureDomains = false
	cut, err = SelectReplicaMoves(catalog, candidates, nodes, policy, &workspace)
	if err != nil || cut.Count() != 1 {
		t.Fatalf("domain-disabled cut/error = %+v / %v", cut, err)
	}
}

func TestSelectReplicaMovesFailsClosedOnCapacityEvidence(t *testing.T) {
	endpoints := replicaMoveEndpoints("source", "follower", "node-c", "node-d")
	catalog, sources := replicaMoveCatalog(t, 1, []distribution.EndpointID{"source", "follower"}, endpoints)
	baseCandidate := replicaMoveCandidate(sources[0], 300, 100)
	baseNodes := []NodeCapacity{
		replicaMoveNode("source", 1, 900),
		replicaMoveNode("follower", 2, 300),
		replicaMoveNode("node-c", 3, 100),
		replicaMoveNode("node-d", 4, 200),
	}
	tests := []struct {
		name          string
		mutate        func(*ReplicaMoveCandidate, *[]NodeCapacity, *ReplicaMovePolicy)
		wantError     bool
		wantStale     uint16
		wantInvalid   uint16
		wantBudget    uint16
		wantSaturated uint16
	}{
		{name: "stale candidate", mutate: func(candidate *ReplicaMoveCandidate, _ *[]NodeCapacity, _ *ReplicaMovePolicy) {
			candidate.CatalogGeneration--
		}, wantStale: 1},
		{name: "zero demand", mutate: func(candidate *ReplicaMoveCandidate, _ *[]NodeCapacity, _ *ReplicaMovePolicy) {
			candidate.Demand = autosplit.CapacityVector{}
		}, wantInvalid: 1},
		{name: "demand exceeds source", mutate: func(candidate *ReplicaMoveCandidate, _ *[]NodeCapacity, _ *ReplicaMovePolicy) {
			candidate.Demand[autosplit.ResourceLiveBytes] = 901
		}, wantInvalid: 1},
		{name: "migration budget", mutate: func(_ *ReplicaMoveCandidate, _ *[]NodeCapacity, policy *ReplicaMovePolicy) {
			policy.MaxPhysicalMigrationBytes = 99
		}, wantBudget: 1},
		{name: "receive saturation", mutate: func(_ *ReplicaMoveCandidate, nodes *[]NodeCapacity, _ *ReplicaMovePolicy) {
			for index := 2; index < len(*nodes); index++ {
				(*nodes)[index].ActiveReceives = (*nodes)[index].MaxReceives
			}
		}, wantSaturated: 1},
		{name: "migration saturation", mutate: func(_ *ReplicaMoveCandidate, nodes *[]NodeCapacity, _ *ReplicaMovePolicy) {
			for index := 2; index < len(*nodes); index++ {
				(*nodes)[index].MigrationUsed = (*nodes)[index].MigrationCapacity
			}
		}, wantSaturated: 1},
		{name: "duplicate node endpoint", mutate: func(_ *ReplicaMoveCandidate, nodes *[]NodeCapacity, _ *ReplicaMovePolicy) {
			(*nodes)[3].Endpoint = (*nodes)[2].Endpoint
		}, wantError: true},
		{name: "unknown node endpoint", mutate: func(_ *ReplicaMoveCandidate, nodes *[]NodeCapacity, _ *ReplicaMovePolicy) {
			(*nodes)[3].Endpoint = "missing"
		}, wantError: true},
		{name: "invalid node generation", mutate: func(_ *ReplicaMoveCandidate, nodes *[]NodeCapacity, _ *ReplicaMovePolicy) {
			(*nodes)[3].CatalogGeneration--
		}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := baseCandidate
			nodes := slices.Clone(baseNodes)
			policy := DefaultReplicaMovePolicy()
			test.mutate(&candidate, &nodes, &policy)
			var workspace ReplicaMoveWorkspace
			cut, err := SelectReplicaMoves(
				catalog, []ReplicaMoveCandidate{candidate}, nodes, policy, &workspace,
			)
			if test.wantError {
				if !errors.Is(err, ErrInvalidReplicaMove) {
					t.Fatalf("error = %v, want ErrInvalidReplicaMove", err)
				}
				return
			}
			if err != nil || cut.Count() != 0 || cut.Stale != test.wantStale ||
				cut.Invalid != test.wantInvalid || cut.Budget != test.wantBudget ||
				cut.Saturated != test.wantSaturated {
				t.Fatalf("cut/error = %+v / %v", cut, err)
			}
		})
	}
}

func TestResolveReplicaMoveRejectsMutatedHandoff(t *testing.T) {
	endpoints := replicaMoveEndpoints("source", "follower", "node-c")
	catalog, sources := replicaMoveCatalog(t, 1, []distribution.EndpointID{"source", "follower"}, endpoints)
	baseCandidates := []ReplicaMoveCandidate{replicaMoveCandidate(sources[0], 300, 100)}
	baseNodes := []NodeCapacity{
		replicaMoveNode("source", 1, 900),
		replicaMoveNode("follower", 2, 300),
		replicaMoveNode("node-c", 3, 100),
	}
	basePolicy := DefaultReplicaMovePolicy()
	var workspace ReplicaMoveWorkspace
	cut, err := SelectReplicaMoves(catalog, baseCandidates, baseNodes, basePolicy, &workspace)
	if err != nil || cut.Count() != 1 {
		t.Fatalf("cut/error = %+v / %v", cut, err)
	}
	tests := []struct {
		name   string
		mutate func(*[]ReplicaMoveCandidate, *[]NodeCapacity, *ReplicaMovePolicy, *ReplicaMoveCut)
	}{
		{"candidate", func(candidates *[]ReplicaMoveCandidate, _ *[]NodeCapacity, _ *ReplicaMovePolicy, _ *ReplicaMoveCut) {
			(*candidates)[0].MigrationBytes++
		}},
		{"node", func(_ *[]ReplicaMoveCandidate, nodes *[]NodeCapacity, _ *ReplicaMovePolicy, _ *ReplicaMoveCut) {
			(*nodes)[2].Used[autosplit.ResourceLiveBytes]++
		}},
		{"policy", func(_ *[]ReplicaMoveCandidate, _ *[]NodeCapacity, policy *ReplicaMovePolicy, _ *ReplicaMoveCut) {
			policy.MinProjectedReliefPPM++
		}},
		{"cut", func(_ *[]ReplicaMoveCandidate, _ *[]NodeCapacity, _ *ReplicaMovePolicy, cut *ReplicaMoveCut) {
			cut.moves[0].targetNode = cut.moves[0].sourceNode
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidates := slices.Clone(baseCandidates)
			nodes := slices.Clone(baseNodes)
			policy := basePolicy
			mutatedCut := cut
			test.mutate(&candidates, &nodes, &policy, &mutatedCut)
			if _, err := ResolveReplicaMove(
				catalog, candidates, nodes, policy, mutatedCut, 0,
			); !errors.Is(err, ErrInvalidReplicaMove) {
				t.Fatalf("error = %v, want ErrInvalidReplicaMove", err)
			}
		})
	}
}

func TestSelectReplicaMovesIsFixedSpaceAndWarmAllocationFree(t *testing.T) {
	if size := unsafe.Sizeof(ReplicaMoveWorkspace{}); size > 384<<10 {
		t.Fatalf("ReplicaMoveWorkspace size = %d, want <= 384 KiB", size)
	}
	if size := unsafe.Sizeof(ReplicaMoveCut{}); size > 1<<10 {
		t.Fatalf("ReplicaMoveCut size = %d, want <= 1 KiB", size)
	}
	endpoints := replicaMoveEndpoints("source", "follower", "node-c")
	catalog, sources := replicaMoveCatalog(t, 1, []distribution.EndpointID{"source", "follower"}, endpoints)
	candidates := []ReplicaMoveCandidate{replicaMoveCandidate(sources[0], 300, 100)}
	nodes := []NodeCapacity{
		replicaMoveNode("source", 1, 900),
		replicaMoveNode("follower", 2, 300),
		replicaMoveNode("node-c", 3, 100),
	}
	policy := DefaultReplicaMovePolicy()
	var workspace ReplicaMoveWorkspace
	if _, err := SelectReplicaMoves(catalog, candidates, nodes, policy, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1_000, func() {
		cut, err := SelectReplicaMoves(catalog, candidates, nodes, policy, &workspace)
		if err != nil || cut.Count() != 1 {
			panic("unexpected replica move cut")
		}
	}); allocations != 0 {
		t.Fatalf("replica move allocations = %v, want 0", allocations)
	}
}

func BenchmarkSelectReplicaMoves(b *testing.B) {
	const (
		shardCount = 64
		nodeCount  = 256
	)
	endpoints := make(map[distribution.EndpointID]string, nodeCount)
	nodes := make([]NodeCapacity, nodeCount)
	for index := range nodes {
		endpoint := distribution.EndpointID("node-" + strconv.Itoa(index))
		endpoints[endpoint] = "127.0.0.1:" + strconv.Itoa(index+1000)
		nodes[index] = NodeCapacity{
			CatalogGeneration: 10, Endpoint: endpoint, FailureDomain: uint32(index + 1),
			Flags:             NodePlacementReady,
			Capacity:          autosplit.CapacityVector{autosplit.ResourceLiveBytes: 100_000},
			Used:              autosplit.CapacityVector{autosplit.ResourceLiveBytes: uint64(10_000 + index)},
			MigrationCapacity: 1 << 30, MaxReceives: 64,
		}
	}
	nodes[0].Used[autosplit.ResourceLiveBytes] = 90_000
	catalog, sources := replicaMoveCatalog(
		b, shardCount, []distribution.EndpointID{"node-0", "node-1"}, endpoints,
	)
	candidates := make([]ReplicaMoveCandidate, shardCount)
	for index := range candidates {
		candidates[index] = replicaMoveCandidate(sources[index], 500, 1_000)
	}
	policy := DefaultReplicaMovePolicy()
	policy.MaxMoves = 16
	policy.MaxMovesPerSourceNode = 16
	policy.MinProjectedReliefPPM = 1_000
	var workspace ReplicaMoveWorkspace
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		cut, err := SelectReplicaMoves(catalog, candidates, nodes, policy, &workspace)
		if err != nil || cut.Count() != 16 {
			b.Fatalf("cut/error = %+v / %v", cut, err)
		}
	}
}

func replicaMoveCatalog(
	t testing.TB,
	shardCount int,
	leaders []distribution.EndpointID,
	endpoints map[distribution.EndpointID]string,
) (*gateway.Snapshot, []autosplit.SourceIdentity) {
	t.Helper()
	shift := uint(64 - bits.TrailingZeros64(uint64(shardCount)))
	shards := make([]distribution.Shard, shardCount)
	for index := range shards {
		binary.BigEndian.PutUint64(shards[index].Range.Start[:], uint64(index)<<shift)
		if index == shardCount-1 {
			shards[index].Range.End.Max = true
		} else {
			binary.BigEndian.PutUint64(shards[index].Range.End.Point[:], uint64(index+1)<<shift)
		}
		shards[index].ID = distribution.ShardID("shard-" + strconv.Itoa(index))
		shards[index].AllocationGeneration = distribution.ShardAllocationGeneration(index + 1)
		shards[index].Leaders = slices.Clone(leaders)
		shards[index].Epoch = distribution.OwnershipEpoch(index + 3)
	}
	manifest, err := distribution.NewManifest("primary", 7, shards)
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
	sources := make([]autosplit.SourceIdentity, shardCount)
	for index := range sources {
		metadata, _ := manifest.ShardMetadataAt(index)
		sources[index] = autosplit.SourceIdentity{
			Distribution: manifest.Distribution(), Shard: metadata.ID,
			AllocationGeneration: metadata.AllocationGeneration, Range: metadata.Range,
			BucketBits:     distribution.DefaultVirtualBucketBits,
			RoutingVersion: manifest.Version(), OwnershipEpoch: metadata.Epoch,
		}
	}
	return catalog, sources
}

func replicaMoveEndpoints(endpoints ...distribution.EndpointID) map[distribution.EndpointID]string {
	result := make(map[distribution.EndpointID]string, len(endpoints))
	for index, endpoint := range endpoints {
		result[endpoint] = "127.0.0.1:" + strconv.Itoa(index+1000)
	}
	return result
}

func replicaMoveNode(endpoint distribution.EndpointID, domain uint32, used uint64) NodeCapacity {
	return NodeCapacity{
		CatalogGeneration: 10, Endpoint: endpoint, FailureDomain: domain,
		Flags:             NodePlacementReady,
		Capacity:          autosplit.CapacityVector{autosplit.ResourceLiveBytes: 1_000},
		Used:              autosplit.CapacityVector{autosplit.ResourceLiveBytes: used},
		MigrationCapacity: 1_000, MaxReceives: 16,
	}
}

func replicaMoveCandidate(
	source autosplit.SourceIdentity,
	demand uint64,
	migration uint64,
) ReplicaMoveCandidate {
	return ReplicaMoveCandidate{
		CatalogGeneration: 10, Source: source,
		Demand:         autosplit.CapacityVector{autosplit.ResourceLiveBytes: demand},
		MigrationBytes: migration,
	}
}
