package topologyscheduler

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
)

func TestFeedbackDefersInflightAndAppliesWindowBackoff(t *testing.T) {
	catalog, sources := admissionCatalog(t)
	candidate := admissionCandidate(10, sources[0], 300_000, 60)
	candidates := []SplitCandidate{candidate}
	policy := DefaultPolicy()
	feedbackPolicy := FeedbackPolicy{BaseRetryWindows: 2, MaxRetryWindows: 4}
	var workspace Workspace
	var feedback FeedbackTable

	decision, err := SelectSplitsWithFeedback(
		catalog, candidates, policy, &workspace, &feedback,
	)
	if err != nil || decision.Count != 1 {
		t.Fatalf("initial decision = %+v, %v", decision, err)
	}
	if err := feedback.Start(candidates, decision); err != nil {
		t.Fatal(err)
	}
	if stats := feedback.Stats(); stats.Entries != 1 || stats.InFlight != 1 {
		t.Fatalf("in-flight stats = %+v", stats)
	}
	decision, err = SelectSplitsWithFeedback(
		catalog, candidates, policy, &workspace, &feedback,
	)
	if err != nil || decision.Count != 0 || decision.Deferred != 1 {
		t.Fatalf("in-flight decision = %+v, %v", decision, err)
	}
	if err := feedback.Finish(candidate, FeedbackRetryable, feedbackPolicy); err != nil {
		t.Fatal(err)
	}

	candidates[0].Recommendation.WindowSequence = 2
	decision, _ = SelectSplitsWithFeedback(catalog, candidates, policy, &workspace, &feedback)
	if decision.Count != 0 || decision.Deferred != 1 {
		t.Fatalf("window 2 decision = %+v", decision)
	}
	candidates[0].Recommendation.WindowSequence = 3
	decision, _ = SelectSplitsWithFeedback(catalog, candidates, policy, &workspace, &feedback)
	if decision.Count != 1 {
		t.Fatalf("window 3 decision = %+v", decision)
	}
	if err := feedback.Start(candidates, decision); err != nil {
		t.Fatal(err)
	}
	if err := feedback.Finish(candidates[0], FeedbackRetryable, feedbackPolicy); err != nil {
		t.Fatal(err)
	}

	candidates[0].Recommendation.WindowSequence = 6
	decision, _ = SelectSplitsWithFeedback(catalog, candidates, policy, &workspace, &feedback)
	if decision.Count != 0 || decision.Deferred != 1 {
		t.Fatalf("window 6 decision = %+v", decision)
	}
	candidates[0].Recommendation.WindowSequence = 7
	decision, _ = SelectSplitsWithFeedback(catalog, candidates, policy, &workspace, &feedback)
	if decision.Count != 1 {
		t.Fatalf("window 7 decision = %+v", decision)
	}
	if err := feedback.Start(candidates, decision); err != nil {
		t.Fatal(err)
	}
	if err := feedback.Finish(candidates[0], FeedbackSucceeded, feedbackPolicy); err != nil {
		t.Fatal(err)
	}
	if stats := feedback.Stats(); stats != (FeedbackStats{}) {
		t.Fatalf("completed stats = %+v", stats)
	}
}

func TestFeedbackStartIsAtomicForDuplicateOrInvalidCuts(t *testing.T) {
	_, sources := admissionCatalog(t)
	candidates := []SplitCandidate{
		admissionCandidate(10, sources[0], 300_000, 60),
		admissionCandidate(10, sources[0], 290_000, 50),
	}
	decision := Decision{Count: 2, Ordinals: [MaxBatch]uint16{0, 1}}
	var feedback FeedbackTable
	if err := feedback.Start(candidates, decision); !errors.Is(err, ErrInvalidFeedback) {
		t.Fatalf("duplicate Start error = %v", err)
	}
	if stats := feedback.Stats(); stats != (FeedbackStats{}) {
		t.Fatalf("duplicate Start mutated table: %+v", stats)
	}
	decision.Ordinals[1] = 2
	if err := feedback.Start(candidates, decision); !errors.Is(err, ErrInvalidFeedback) {
		t.Fatalf("invalid ordinal Start error = %v", err)
	}
	if stats := feedback.Stats(); stats != (FeedbackStats{}) {
		t.Fatalf("invalid Start mutated table: %+v", stats)
	}
}

func TestFeedbackCapacityFailsClosedUntilInflightEntryFinishes(t *testing.T) {
	_, sources := admissionCatalog(t)
	candidates := make([]SplitCandidate, MaxBatch)
	decision := Decision{Count: MaxBatch}
	for index := range candidates {
		decision.Ordinals[index] = uint16(index)
	}
	var feedback FeedbackTable
	allocation := uint64(100)
	for batch := 0; batch < MaxFeedbackEntries/MaxBatch; batch++ {
		for index := range candidates {
			candidates[index] = admissionCandidate(10, sources[0], 300_000, 1)
			candidates[index].Recommendation.Source.AllocationGeneration =
				distribution.ShardAllocationGeneration(allocation)
			allocation++
		}
		if err := feedback.Start(candidates, decision); err != nil {
			t.Fatalf("Start batch %d: %v", batch, err)
		}
	}
	if stats := feedback.Stats(); stats.Entries != MaxFeedbackEntries ||
		stats.InFlight != MaxFeedbackEntries {
		t.Fatalf("full stats = %+v", stats)
	}
	extra := []SplitCandidate{admissionCandidate(10, sources[0], 300_000, 1)}
	extra[0].Recommendation.Source.AllocationGeneration =
		distribution.ShardAllocationGeneration(allocation)
	extraDecision := Decision{Count: 1}
	if err := feedback.Start(extra, extraDecision); !errors.Is(err, ErrInvalidFeedback) {
		t.Fatalf("full Start error = %v", err)
	}
	if err := feedback.Finish(candidates[0], FeedbackCancelled, DefaultFeedbackPolicy()); err != nil {
		t.Fatal(err)
	}
	if err := feedback.Start(extra, extraDecision); err != nil {
		t.Fatalf("Start after release: %v", err)
	}
}

func TestFeedbackFullCoolingTableEvictsOldestAdvisoryEntry(t *testing.T) {
	_, sources := admissionCatalog(t)
	all := make([]SplitCandidate, MaxFeedbackEntries)
	decision := Decision{Count: MaxBatch}
	for index := 0; index < MaxBatch; index++ {
		decision.Ordinals[index] = uint16(index)
	}
	for index := range all {
		all[index] = admissionCandidate(10, sources[0], 300_000, 1)
		all[index].Recommendation.Source.AllocationGeneration =
			distribution.ShardAllocationGeneration(index + 100)
	}
	var feedback FeedbackTable
	for start := 0; start < len(all); start += MaxBatch {
		if err := feedback.Start(all[start:start+MaxBatch], decision); err != nil {
			t.Fatalf("Start at %d: %v", start, err)
		}
	}
	for index := range all {
		if err := feedback.Finish(
			all[index], FeedbackRetryable,
			FeedbackPolicy{BaseRetryWindows: 100, MaxRetryWindows: 100},
		); err != nil {
			t.Fatalf("Finish %d: %v", index, err)
		}
	}
	extra := []SplitCandidate{admissionCandidate(10, sources[0], 300_000, 1)}
	extra[0].Recommendation.Source.AllocationGeneration =
		distribution.ShardAllocationGeneration(MaxFeedbackEntries + 100)
	if err := feedback.Start(extra, Decision{Count: 1}); err != nil {
		t.Fatal(err)
	}
	if _, ok := feedback.find(sourceKeyFor(all[0].Recommendation.Source)); ok {
		t.Fatal("oldest cooling entry was not evicted")
	}
	if _, ok := feedback.find(sourceKeyFor(extra[0].Recommendation.Source)); !ok {
		t.Fatal("new in-flight entry missing after eviction")
	}
	if stats := feedback.Stats(); stats.Entries != MaxFeedbackEntries ||
		stats.InFlight != 1 || stats.Cooling != MaxFeedbackEntries-1 {
		t.Fatalf("post-eviction stats = %+v", stats)
	}
}

func TestFeedbackIsFixedSpaceAndWarmAllocationFree(t *testing.T) {
	if size := unsafe.Sizeof(FeedbackTable{}); size > 56<<10 {
		t.Fatalf("FeedbackTable size = %d, want <= 56 KiB", size)
	}
	catalog, sources := admissionCatalog(t)
	candidates := []SplitCandidate{admissionCandidate(10, sources[0], 300_000, 60)}
	policy := DefaultPolicy()
	var workspace Workspace
	var feedback FeedbackTable
	if allocations := testing.AllocsPerRun(1_000, func() {
		decision, err := SelectSplitsWithFeedback(
			catalog, candidates, policy, &workspace, &feedback,
		)
		if err != nil || decision.Count != 1 {
			panic("unexpected feedback admission")
		}
	}); allocations != 0 {
		t.Fatalf("feedback admission allocations = %v, want 0", allocations)
	}
	decision := Decision{Count: 1}
	if allocations := testing.AllocsPerRun(1_000, func() {
		feedback = FeedbackTable{}
		if feedback.Start(candidates, decision) != nil ||
			feedback.Finish(candidates[0], FeedbackCancelled, DefaultFeedbackPolicy()) != nil {
			panic("unexpected feedback transition")
		}
	}); allocations != 0 {
		t.Fatalf("feedback transition allocations = %v, want 0", allocations)
	}
}

func BenchmarkSelectSplitsWithFeedback(b *testing.B) {
	catalog, sources := admissionCatalog(b)
	candidates := make([]SplitCandidate, 256)
	for index := range candidates {
		candidates[index] = admissionCandidate(
			10, sources[index%len(sources)], 200_000+uint64(index), uint64(index+1),
		)
	}
	var workspace Workspace
	var feedback FeedbackTable
	policy := DefaultPolicy()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := SelectSplitsWithFeedback(
			catalog, candidates, policy, &workspace, &feedback,
		); err != nil {
			b.Fatal(err)
		}
	}
}
