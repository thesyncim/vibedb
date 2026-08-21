package topologyscheduler

import (
	"slices"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

func TestSelectSplitsBoundsHotRangesAcrossDistributions(t *testing.T) {
	catalog, sources := admissionCatalog(t)
	candidates := []SplitCandidate{
		admissionCandidate(9, sources[2], 400_000, 1),
		admissionCandidate(10, changedEpoch(sources[2]), 390_000, 1),
		admissionCandidate(10, sources[0], 300_000, 60),
		admissionCandidate(10, sources[1], 290_000, 50),
		admissionCandidate(10, sources[2], 280_000, 40),
		admissionCandidate(10, sources[0], 250_000, 1),
	}
	policy := Policy{
		MaxBatch: 2, MaxPerDistribution: 1,
		MinBenefitPPM: 100_000, MigrationBudget: 100,
	}
	var workspace Workspace
	decision, err := SelectSplits(catalog, candidates, policy, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	if decision.CatalogGeneration != 10 || decision.Count != 2 ||
		decision.MigrationBytes != 100 || decision.Stale != 1 ||
		decision.Invalid != 1 || decision.Distribution != 1 {
		t.Fatalf("decision = %+v", decision)
	}
	first := candidates[decision.Ordinals[0]].Recommendation.Source
	second := candidates[decision.Ordinals[1]].Recommendation.Source
	if first != sources[0] || second != sources[2] {
		t.Fatalf("selected sources = %+v, %+v", first, second)
	}
}

func TestSelectSplitsRejectsDuplicatesAndMigrationOverflow(t *testing.T) {
	catalog, sources := admissionCatalog(t)
	candidates := []SplitCandidate{
		admissionCandidate(10, sources[0], 300_000, 60),
		admissionCandidate(10, sources[0], 250_000, 1),
		admissionCandidate(10, sources[2], 200_000, 41),
	}
	var workspace Workspace
	decision, err := SelectSplits(catalog, candidates, Policy{
		MaxBatch: 3, MaxPerDistribution: 2,
		MinBenefitPPM: 100_000, MigrationBudget: 100,
	}, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Count != 1 || decision.Duplicate != 1 || decision.Budget != 1 ||
		decision.Ordinals[0] != 0 {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestSelectSplitsDeterministicallyOrdersEqualPriorityAlternatives(t *testing.T) {
	catalog, sources := admissionCatalog(t)
	preferred := admissionCandidate(10, sources[0], 300_000, 20)
	alternative := preferred
	alternative.Recommendation.WindowSequence++
	alternative.Recommendation.FanoutTaxPPM++

	policy := Policy{
		MaxBatch: 1, MaxPerDistribution: 1,
		MinBenefitPPM: 100_000, MigrationBudget: 100,
	}
	var workspace Workspace
	for _, candidates := range [][]SplitCandidate{
		{alternative, preferred},
		{preferred, alternative},
	} {
		decision, err := SelectSplits(catalog, candidates, policy, &workspace)
		if err != nil {
			t.Fatal(err)
		}
		selected := candidates[decision.Ordinals[0]]
		if selected.Recommendation.WindowSequence != preferred.Recommendation.WindowSequence ||
			selected.Recommendation.FanoutTaxPPM != preferred.Recommendation.FanoutTaxPPM {
			t.Fatalf("selected non-canonical equal-priority alternative: %+v", selected)
		}
	}
}

func TestSelectSplitsIsFixedSpaceAndWarmAllocationFree(t *testing.T) {
	if size := unsafe.Sizeof(Workspace{}); size != MaxCandidates*2 {
		t.Fatalf("Workspace size = %d, want %d", size, MaxCandidates*2)
	}
	if size := unsafe.Sizeof(Decision{}); size > 160 {
		t.Fatalf("Decision size = %d, want <= 160", size)
	}
	catalog, sources := admissionCatalog(t)
	candidates := []SplitCandidate{
		admissionCandidate(10, sources[2], 200_000, 20),
		admissionCandidate(10, sources[0], 300_000, 30),
	}
	var workspace Workspace
	policy := DefaultPolicy()
	if _, err := SelectSplits(catalog, candidates, policy, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1_000, func() {
		decision, err := SelectSplits(catalog, candidates, policy, &workspace)
		if err != nil || decision.Count != 2 {
			panic("unexpected admission result")
		}
	}); allocations != 0 {
		t.Fatalf("warm split admission allocations = %v, want 0", allocations)
	}
	if _, err := SelectSplits(catalog, candidates, Policy{}, &workspace); err != ErrInvalidAdmission {
		t.Fatalf("zero policy error = %v", err)
	}
}

func BenchmarkSelectSplits(b *testing.B) {
	catalog, sources := admissionCatalog(b)
	candidates := make([]SplitCandidate, 256)
	for index := range candidates {
		source := sources[index%len(sources)]
		candidates[index] = admissionCandidate(
			10, source, 200_000+uint64(index), uint64(index+1),
		)
	}
	slices.Reverse(candidates)
	var workspace Workspace
	policy := DefaultPolicy()
	if _, err := SelectSplits(catalog, candidates, policy, &workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := SelectSplits(catalog, candidates, policy, &workspace); err != nil {
			b.Fatal(err)
		}
	}
}

func admissionCatalog(
	t testing.TB,
) (*gateway.Snapshot, [3]autosplit.SourceIdentity) {
	t.Helper()
	middle := distribution.KeyspacePoint{0x80}
	d1, err := distribution.NewManifest("primary", 7, []distribution.Shard{
		{
			ID: "p-left", AllocationGeneration: 1,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
			Leaders: []distribution.EndpointID{"node-a"}, Epoch: 3,
		},
		{
			ID: "p-right", AllocationGeneration: 2,
			Range:   distribution.KeyRange{Start: middle, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"node-b"}, Epoch: 5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	d2, err := distribution.NewManifest("secondary", 9, []distribution.Shard{{
		ID: "s-all", AllocationGeneration: 1,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"node-c"}, Epoch: 7,
	}})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := gateway.NewSnapshot(
		distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{
				{Name: "primary", Arity: 1, MapperVersion: distribution.NativeMapperVersion},
				{Name: "secondary", Arity: 1, MapperVersion: distribution.NativeMapperVersion},
			},
			Placements: []distribution.TablePlacement{
				{Table: "docs", Distribution: "primary", Columns: []string{"/tenant"}},
				{Table: "events", Distribution: "secondary", Columns: []string{"/tenant"}},
			},
			Manifests: []*distribution.Manifest{d1, d2},
		},
		map[distribution.EndpointID]string{
			"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2", "node-c": "127.0.0.1:3",
		},
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	return catalog, [3]autosplit.SourceIdentity{
		admissionSource(d1, 0), admissionSource(d1, 1), admissionSource(d2, 0),
	}
}

func admissionSource(
	manifest *distribution.Manifest,
	ordinal int,
) autosplit.SourceIdentity {
	shard, _ := manifest.ShardMetadataAt(ordinal)
	return autosplit.SourceIdentity{
		Distribution: manifest.Distribution(), Shard: shard.ID,
		AllocationGeneration: shard.AllocationGeneration, Range: shard.Range,
		BucketBits:     distribution.DefaultVirtualBucketBits,
		RoutingVersion: manifest.Version(), OwnershipEpoch: shard.Epoch,
	}
}

func admissionCandidate(
	catalogGeneration uint64,
	source autosplit.SourceIdentity,
	benefit uint64,
	migration uint64,
) SplitCandidate {
	boundary := distribution.KeyspacePoint{0x80}
	switch source.Range.Start[0] {
	case 0x00:
		if !source.Range.End.Max {
			boundary[0] = 0x40
		}
	case 0x80:
		boundary[0] = 0xc0
	}
	return SplitCandidate{
		CatalogGeneration: catalogGeneration, MigrationBytes: migration,
		Recommendation: autosplit.Recommendation{
			Source: source, WindowSequence: 1, Kind: autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{boundary}, BoundaryCount: 1,
			CandidateBin: 32, CurrentPressurePPM: 950_000,
			PredictedPressurePPM: 950_000 - benefit, BenefitPPM: benefit,
		},
	}
}

func changedEpoch(source autosplit.SourceIdentity) autosplit.SourceIdentity {
	source.OwnershipEpoch++
	return source
}
