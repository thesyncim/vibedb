package topologyscheduler

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

func TestBuildSplitPlanBatchBindsSameDistributionPlacements(t *testing.T) {
	catalog, sources := admissionCatalog(t)
	candidates := []SplitCandidate{
		admissionCandidate(10, sources[0], 300_000, 60),
		admissionCandidate(10, sources[1], 290_000, 50),
	}
	decision := admissionDecision(t, catalog, candidates)
	placements := splitPlacements()

	batch, err := BuildSplitPlanBatch(catalog, candidates, decision, placements)
	if err != nil {
		t.Fatal(err)
	}
	if batch.CatalogGeneration() != 10 || batch.Count() != 2 {
		t.Fatalf("batch generation/count = %d/%d", batch.CatalogGeneration(), batch.Count())
	}
	for index, wantOrdinal := range []uint16{0, 1} {
		ordinal, plan, ok := batch.PlanAt(index)
		if !ok || ordinal != wantOrdinal || plan == nil ||
			plan.Source != candidates[wantOrdinal].Recommendation.Source ||
			plan.Manifest().Version() != 8 {
			t.Fatalf("plan %d = ordinal %d, %+v, %v", index, ordinal, plan, ok)
		}
	}
	if _, _, ok := batch.PlanAt(2); ok {
		t.Fatal("out-of-range plan reported present")
	}

	placements[0].Destinations[0].Leaders[0] = "node-a"
	_, first, _ := batch.PlanAt(0)
	child, _ := first.Child(1)
	if child.Leaders[0] != "node-c" {
		t.Fatalf("plan retained caller placement backing: %+v", child.Leaders)
	}
}

func TestBuildSplitPlanBatchRejectsUnfencedOrCollidingCuts(t *testing.T) {
	catalog, sources := admissionCatalog(t)
	baseCandidates := []SplitCandidate{
		admissionCandidate(10, sources[0], 300_000, 60),
		admissionCandidate(10, sources[1], 290_000, 50),
	}
	baseDecision := admissionDecision(t, catalog, baseCandidates)

	tests := []struct {
		name   string
		mutate func(*[]SplitCandidate, *Decision, *[]SplitPlacement)
	}{
		{
			name: "stale decision generation",
			mutate: func(_ *[]SplitCandidate, decision *Decision, _ *[]SplitPlacement) {
				decision.CatalogGeneration--
			},
		},
		{
			name: "changed source after admission",
			mutate: func(candidates *[]SplitCandidate, _ *Decision, _ *[]SplitPlacement) {
				(*candidates)[0].Recommendation.Source.OwnershipEpoch++
			},
		},
		{
			name: "forged migration total",
			mutate: func(_ *[]SplitCandidate, decision *Decision, _ *[]SplitPlacement) {
				decision.MigrationBytes++
			},
		},
		{
			name: "duplicate admitted ordinal",
			mutate: func(_ *[]SplitCandidate, decision *Decision, _ *[]SplitPlacement) {
				decision.Ordinals[1] = decision.Ordinals[0]
				decision.MigrationBytes = 120
			},
		},
		{
			name: "missing destination",
			mutate: func(_ *[]SplitCandidate, _ *Decision, placements *[]SplitPlacement) {
				(*placements)[0].DestinationCount = 0
			},
		},
		{
			name: "retired allocation reuse",
			mutate: func(_ *[]SplitCandidate, _ *Decision, placements *[]SplitPlacement) {
				(*placements)[0].Destinations[0].AllocationGeneration = 2
			},
		},
		{
			name: "unknown endpoint",
			mutate: func(_ *[]SplitCandidate, _ *Decision, placements *[]SplitPlacement) {
				(*placements)[0].Destinations[0].Leaders = []distribution.EndpointID{"node-missing"}
			},
		},
		{
			name: "cross-plan allocation collision",
			mutate: func(_ *[]SplitCandidate, _ *Decision, placements *[]SplitPlacement) {
				(*placements)[1].Destinations[0].AllocationGeneration =
					(*placements)[0].Destinations[0].AllocationGeneration
			},
		},
		{
			name: "cross-plan shard collision",
			mutate: func(_ *[]SplitCandidate, _ *Decision, placements *[]SplitPlacement) {
				(*placements)[1].Destinations[0].Shard =
					(*placements)[0].Destinations[0].Shard
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidates := append([]SplitCandidate(nil), baseCandidates...)
			decision := baseDecision
			placements := splitPlacements()
			test.mutate(&candidates, &decision, &placements)
			if _, err := BuildSplitPlanBatch(catalog, candidates, decision, placements); !errors.Is(err, ErrInvalidPlacement) {
				t.Fatalf("BuildSplitPlanBatch error = %v, want ErrInvalidPlacement", err)
			}
		})
	}
}

func TestBuildSplitPlanBatchRejectsActiveShardIdentity(t *testing.T) {
	catalog, sources := admissionCatalog(t)
	candidates := []SplitCandidate{admissionCandidate(10, sources[0], 300_000, 60)}
	decision := admissionDecision(t, catalog, candidates)
	placements := splitPlacements()[:1]
	placements[0].Destinations[0].Shard = sources[1].Shard
	if _, err := BuildSplitPlanBatch(catalog, candidates, decision, placements); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("BuildSplitPlanBatch error = %v", err)
	}
}

func TestBuildSplitPlanBatchKeepsAllocationNamespacesPerDistribution(t *testing.T) {
	catalog, sources := admissionCatalog(t)
	candidates := []SplitCandidate{
		admissionCandidate(10, sources[0], 300_000, 60),
		admissionCandidate(10, sources[2], 290_000, 50),
	}
	decision := admissionDecision(t, catalog, candidates)
	placements := splitPlacements()
	placements[0].Destinations[0].Shard = "new"
	placements[1].Destinations[0] = autosplit.Destination{
		Shard: "new", AllocationGeneration: 3,
		Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1,
	}
	batch, err := BuildSplitPlanBatch(catalog, candidates, decision, placements)
	if err != nil {
		t.Fatal(err)
	}
	_, primary, _ := batch.PlanAt(0)
	_, secondary, _ := batch.PlanAt(1)
	if primary.Manifest().Distribution() != "primary" || primary.Manifest().Version() != 8 ||
		secondary.Manifest().Distribution() != "secondary" || secondary.Manifest().Version() != 10 {
		t.Fatalf("target manifests = %s/%d and %s/%d",
			primary.Manifest().Distribution(), primary.Manifest().Version(),
			secondary.Manifest().Distribution(), secondary.Manifest().Version())
	}
}

func admissionDecision(
	t testing.TB,
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
) Decision {
	t.Helper()
	var workspace Workspace
	decision, err := SelectSplits(catalog, candidates, Policy{
		MaxBatch: 2, MaxPerDistribution: 2,
		MinBenefitPPM: 100_000, MigrationBudget: 1_000,
	}, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func splitPlacements() []SplitPlacement {
	return []SplitPlacement{
		{
			RetainChild: 0, DestinationCount: 1,
			Destinations: [autosplit.MaxSplitChildren - 1]autosplit.Destination{{
				Shard: "p-left-new", AllocationGeneration: 3,
				Leaders: []distribution.EndpointID{"node-c"}, OwnershipEpoch: 1,
			}},
		},
		{
			RetainChild: 0, DestinationCount: 1,
			Destinations: [autosplit.MaxSplitChildren - 1]autosplit.Destination{{
				Shard: "p-right-new", AllocationGeneration: 4,
				Leaders: []distribution.EndpointID{"node-c"}, OwnershipEpoch: 1,
			}},
		},
	}
}
