package autosplit

import (
	"math"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
)

func TestSketchFixedSpaceAccountingAndDeterminism(t *testing.T) {
	range_ := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	source := testSource(range_)
	a, err := NewSketch(source)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewSketch(source)
	load := LoadVector{ResourceWriteCPU: 11, ResourceRequests: 1, ResourceLiveBytes: 7}
	for i := 0; i < 200; i++ {
		point := testPoint(uint64(i%17) << 58)
		if !a.ObservePoint(point, load, uint32(i%5+1)) ||
			!b.ObservePoint(point, load, uint32(i%5+1)) {
			t.Fatal("in-range point was rejected")
		}
	}
	if *a != *b {
		t.Fatal("identical observation streams produced different sketches")
	}
	if a.Samples() != 200 || a.total[ResourceWriteCPU] != 2200 ||
		a.total[ResourceLiveBytes] != 1400 {
		t.Fatalf("accounting = samples %d total %+v", a.Samples(), a.total)
	}
	pulse := a.Pulse(17)
	if pulse.Source != source || pulse.Sequence != 17 || pulse.Samples != a.Samples() ||
		pulse.Total != a.total || pulse.Bounded != a.bounded {
		t.Fatalf("pulse = %+v", pulse)
	}
	if got := unsafe.Sizeof(Pulse{}); got > 192 {
		t.Fatalf("Pulse size = %d B, want <= 192 B", got)
	} else {
		t.Logf("Pulse size = %d B", got)
	}
	if a.ObservePoint(testPoint(math.MaxUint64), load, 1) != true {
		t.Fatal("maximum point must belong to a Max-ended range")
	}
	outsideRange := distribution.KeyRange{
		Start: testPoint(10), End: distribution.KeyspaceEnd{Point: testPoint(20)},
	}
	outside, _ := NewSketch(testSource(outsideRange))
	if outside.ObservePoint(testPoint(9), load, 1) {
		t.Fatal("out-of-range point was accepted")
	}
	if got := unsafe.Sizeof(Sketch{}); got > 4096 {
		t.Fatalf("Sketch size = %d B, want <= 4096 B", got)
	} else {
		t.Logf("Sketch size = %d B", got)
	}
	if got := unsafe.Sizeof(Tracker{}); got > 128 {
		t.Fatalf("Tracker size = %d B, want <= 128 B", got)
	} else {
		t.Logf("Tracker size = %d B", got)
	}
}

func TestRangeArithmeticAcrossNarrowAndMaxEndedRanges(t *testing.T) {
	seed := uint64(0x9e3779b97f4a7c15)
	for i := 0; i < 10_000; i++ {
		seed = seed*6364136223846793005 + 1442695040888963407
		start := seed
		seed = seed*6364136223846793005 + 1442695040888963407
		width := seed%1_000_000 + 1
		endMax := width > math.MaxUint64-start
		var end distribution.KeyspaceEnd
		if endMax {
			end.Max = true
		} else {
			end.Point = testPoint(start + width)
		}
		range_ := distribution.KeyRange{Start: testPoint(start), End: end}
		sketch, err := NewSketch(testSource(range_))
		if err != nil {
			t.Fatalf("range %d: %v", i, err)
		}
		available := width
		if endMax {
			available = -start
		}
		point := testPoint(start + seed%available)
		bin := sketch.binFor(point)
		if bin < 0 || bin >= BinCount {
			t.Fatalf("range %d point %x bin = %d", i, point, bin)
		}
		var previous distribution.KeyspacePoint
		for k := 1; k < BinCount; k++ {
			boundary := sketch.boundary(k)
			if distribution.ComparePoints(boundary, range_.Start) < 0 ||
				(!range_.End.Max && distribution.ComparePoints(boundary, range_.End.Point) >= 0) {
				t.Fatalf("range %d boundary %d = %x outside %+v", i, k, boundary, range_)
			}
			if k > 1 && distribution.ComparePoints(boundary, previous) < 0 {
				t.Fatalf("range %d boundaries decreased at %d", i, k)
			}
			previous = boundary
		}
	}
}

func TestObserveSpanUsesHalfOpenBoundarySemantics(t *testing.T) {
	sketch, _ := NewSketch(testSource(distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}))
	start := testPoint(0)
	end := distribution.KeyspaceEnd{Point: testPoint(uint64(32) << 58)}
	if !sketch.ObserveSpan(start, end, 3) {
		t.Fatal("valid span rejected")
	}
	cross := int64(0)
	for k := 1; k < BinCount; k++ {
		cross += sketch.cross[k]
		want := int64(0)
		if k < 32 {
			want = 3
		}
		if cross != want {
			t.Fatalf("boundary %d crossing = %d, want %d", k, cross, want)
		}
	}
	if sketch.ObserveSpan(testPoint(9), distribution.KeyspaceEnd{Point: testPoint(9)}, 1) {
		t.Fatal("empty span accepted")
	}
}

func TestRecommendBalancedBinarySplit(t *testing.T) {
	sketch, source := balancedSketch(t)
	policy := Policy{TriggerPressurePPM: 1_100_000, MinBenefitPPM: 500_000}
	rec := Recommend(sketch, balancedCapacities(), policy)
	if rec.Kind != RecommendationBinarySplit || rec.Reason != ReasonNone {
		t.Fatalf("recommendation = %+v", rec)
	}
	if rec.BoundaryCount != 1 || rec.Boundaries[0] != testPoint(uint64(1)<<63) || rec.CandidateBin != 32 {
		t.Fatalf("split = count %d boundary %x bin %d", rec.BoundaryCount, rec.Boundaries[0], rec.CandidateBin)
	}
	if rec.CurrentPressurePPM != 2_000_000 || rec.PredictedPressurePPM != 1_000_000 || rec.BenefitPPM != 1_000_000 {
		t.Fatalf("pressures = current %d predicted %d benefit %d", rec.CurrentPressurePPM, rec.PredictedPressurePPM, rec.BenefitPPM)
	}
	assertChildrenCover(t, source.Range, rec)
}

func TestRecommendFanoutTaxPreventsHarmfulSplit(t *testing.T) {
	sketch, _ := balancedSketch(t)
	sketch.ObserveUnbounded(1_000)
	policy := Policy{
		TriggerPressurePPM: 1_100_000, MinBenefitPPM: 100_000,
		FanoutWeightPPM: 1_000_000,
	}
	rec := Recommend(sketch, balancedCapacities(), policy)
	if rec.Kind != RecommendationNone || rec.Reason != ReasonNoBenefit {
		t.Fatalf("fanout-heavy recommendation = %+v", rec)
	}
	if rec.FanoutTaxPPM < 900_000 {
		t.Fatalf("fanout tax = %d, want material penalty", rec.FanoutTaxPPM)
	}
}

func TestRecommendChargesBoundedSpanFanout(t *testing.T) {
	sketch, _ := balancedSketch(t)
	if !sketch.ObserveSpan(
		sketch.source.Range.Start,
		sketch.source.Range.End,
		1_000,
	) {
		t.Fatal("full bounded span rejected")
	}
	policy := Policy{
		TriggerPressurePPM: 1_100_000, MinBenefitPPM: 100_000,
		FanoutWeightPPM: 1_000_000,
	}
	rec := Recommend(sketch, balancedCapacities(), policy)
	if rec.Kind != RecommendationNone || rec.Reason != ReasonNoBenefit {
		t.Fatalf("bounded-span recommendation = %+v", rec)
	}
	if rec.FanoutTaxPPM < 900_000 {
		t.Fatalf("bounded-span fanout tax = %d, want material penalty", rec.FanoutTaxPPM)
	}
}

func TestRecommendCelebrityIsolationAndUnsplittable(t *testing.T) {
	range_ := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	source := SourceIdentity{Distribution: "d", Shard: "s", Range: range_, RoutingVersion: 7, OwnershipEpoch: 9}
	sketch, _ := NewSketch(source)
	hot := testPoint(12345)
	neighbor := testPoint(12445)
	for range 70 {
		sketch.ObservePoint(hot, LoadVector{ResourceWriteCPU: 10, ResourceRequests: 1, ResourceLiveBytes: 1}, 10)
	}
	for range 10 {
		sketch.ObservePoint(neighbor, LoadVector{ResourceWriteCPU: 10, ResourceRequests: 1, ResourceLiveBytes: 1}, 1)
	}
	if !sketch.ObserveSpan(
		hot,
		distribution.KeyspaceEnd{Point: testPoint(pointUint64(hot) + 2)},
		10,
	) {
		t.Fatal("celebrity-crossing span rejected")
	}
	policy := Policy{
		TriggerPressurePPM: 1_000_000, MaxChildPressurePPM: 750_000,
		CelebritySharePPM: 500_000,
	}
	capacities := CapacitySet{
		Current: capFor(500, 40, 40), Left: capFor(1_000, 100, 100), Right: capFor(1_000, 100, 100),
		Isolated: capFor(1_200, 120, 120),
	}
	rec := Recommend(sketch, capacities, policy)
	if rec.Kind != RecommendationIsolatePoint || rec.HotPoint != hot || rec.BoundaryCount != 2 {
		t.Fatalf("isolation recommendation = %+v", rec)
	}
	if rec.Boundaries[0] != hot || pointUint64(rec.Boundaries[1]) != pointUint64(hot)+1 {
		t.Fatalf("isolation boundaries = %x, %x", rec.Boundaries[0], rec.Boundaries[1])
	}
	assertChildrenCover(t, source.Range, rec)

	defaultCapacities := CapacitySet{
		Current: capFor(500, 40, 40), Left: capFor(900, 100, 100), Right: capFor(900, 100, 100),
		Isolated: capFor(1_200, 120, 120),
	}
	defaultRec := Recommend(sketch, defaultCapacities, DefaultPolicy())
	if defaultRec.Kind != RecommendationIsolatePoint || defaultRec.BenefitPPM < DefaultPolicy().MinBenefitPPM {
		t.Fatalf("default-policy isolation recommendation = %+v", defaultRec)
	}
	if defaultRec.FanoutTaxPPM == 0 {
		t.Fatal("default-policy isolation omitted conservative boundary fanout tax")
	}

	capacities.Isolated = capFor(600, 100, 100)
	rec = Recommend(sketch, capacities, policy)
	if rec.Kind != RecommendationUnsplittableHotKey || rec.HotPoint != hot || rec.BoundaryCount != 0 {
		t.Fatalf("unsplittable recommendation = %+v", rec)
	}
}

func TestRecommendDoesNotCallCrowdedBinHotKeyUnsplittable(t *testing.T) {
	range_ := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	sketch, _ := NewSketch(testSource(range_))
	hot := testPoint(12345)
	neighbor := testPoint(12445)
	for range 70 {
		sketch.ObservePoint(hot, LoadVector{ResourceWriteCPU: 10, ResourceRequests: 1}, 10)
	}
	for range 10 {
		sketch.ObservePoint(neighbor, LoadVector{ResourceWriteCPU: 100, ResourceRequests: 1}, 1)
	}
	policy := Policy{
		TriggerPressurePPM: 1_000_000, MaxChildPressurePPM: 750_000,
		CelebritySharePPM: 500_000,
	}
	capacities := CapacitySet{
		Current: capFor(500, 100, 1), Left: capFor(2_000, 100, 1),
		Right: capFor(2_000, 100, 1), Isolated: capFor(1_000, 100, 1),
	}
	rec := Recommend(sketch, capacities, policy)
	if rec.Kind == RecommendationUnsplittableHotKey {
		t.Fatalf("crowded-bin recommendation falsely proved hot key unsplittable: %+v", rec)
	}
}

func TestRecommendRejectsChildrenThatRemainOverloaded(t *testing.T) {
	sketch, _ := balancedSketch(t)
	capacities := balancedCapacities()
	policy := Policy{
		TriggerPressurePPM: 1_100_000, MinBenefitPPM: 1,
		MaxChildPressurePPM: 700_000,
	}
	rec := Recommend(sketch, capacities, policy)
	if rec.Kind != RecommendationNone || rec.Reason != ReasonNoValidBoundary {
		t.Fatalf("overloaded-child recommendation = %+v", rec)
	}
}

func TestRecommendRefusesOverflowAndRetainsSourceFence(t *testing.T) {
	sketch, source := balancedSketch(t)
	sketch.overflow = true
	rec := Recommend(sketch, balancedCapacities(), Policy{})
	if rec.Reason != ReasonEvidenceOverflow {
		t.Fatalf("overflow reason = %v", rec.Reason)
	}
	sketch.overflow = false
	rec = Recommend(sketch, balancedCapacities(), Policy{})
	if rec.Source != source {
		t.Fatalf("recommendation source = %+v, want fenced %+v", rec.Source, source)
	}
}

func TestRecommendNoEvidenceRetainsSourceFence(t *testing.T) {
	source := testSource(balancedRange())
	sketch, err := NewSketch(source)
	if err != nil {
		t.Fatal(err)
	}
	rec := Recommend(sketch, balancedCapacities(), DefaultPolicy())
	if rec.Reason != ReasonNoEvidence || rec.Source != source {
		t.Fatalf("empty recommendation = %+v, want source %+v", rec, source)
	}
}

func TestTrackerRequiresSustainedStableEvidenceAndAppliesCooldown(t *testing.T) {
	policy := DefaultTrackerPolicy()
	hot := Recommendation{
		Source: testSource(balancedRange()),
		Kind:   RecommendationBinarySplit, CandidateBin: 32,
		CurrentPressurePPM: 1_100_000, BenefitPPM: 200_000,
	}
	cold := Recommendation{Source: hot.Source, Kind: RecommendationNone, CurrentPressurePPM: 1_100_000}
	var burst Tracker
	if burst.Observe(hot, policy) {
		t.Fatal("one-window burst qualified")
	}
	for range 7 {
		if burst.Observe(cold, policy) {
			t.Fatal("isolated burst qualified later")
		}
	}

	var sustained Tracker
	for i := 0; i < 7; i++ {
		if sustained.Observe(hot, policy) {
			t.Fatalf("qualified early at window %d", i+1)
		}
	}
	if !sustained.Observe(hot, policy) {
		t.Fatal("eight stable hot windows did not qualify")
	}
	for i := 0; i < int(policy.CooldownWindows); i++ {
		if sustained.Observe(hot, policy) {
			t.Fatalf("qualified during cooldown at window %d", i+1)
		}
	}
}

func TestTrackerBoundaryDriftResetsEvidence(t *testing.T) {
	policy := DefaultTrackerPolicy()
	var tracker Tracker
	rec := Recommendation{
		Source: testSource(balancedRange()),
		Kind:   RecommendationBinarySplit, CandidateBin: 10,
		CurrentPressurePPM: 1_100_000, BenefitPPM: 200_000,
	}
	for range 5 {
		tracker.Observe(rec, policy)
	}
	rec.CandidateBin = 30
	if tracker.Observe(rec, policy) {
		t.Fatal("drifting candidate retained old evidence")
	}
	for i := 0; i < 6; i++ {
		if tracker.Observe(rec, policy) {
			t.Fatalf("qualified before a complete post-drift window at %d", i)
		}
	}
}

func TestTrackerResetsAcrossSourceFence(t *testing.T) {
	policy := DefaultTrackerPolicy()
	rec := Recommendation{
		Source: testSource(balancedRange()), Kind: RecommendationBinarySplit,
		CandidateBin: 32, CurrentPressurePPM: 1_100_000, BenefitPPM: 200_000,
	}
	var tracker Tracker
	for range 7 {
		tracker.Observe(rec, policy)
	}
	rec.Source.OwnershipEpoch++
	if tracker.Observe(rec, policy) {
		t.Fatal("new ownership epoch inherited old evidence")
	}
	for i := 0; i < 6; i++ {
		if tracker.Observe(rec, policy) {
			t.Fatalf("new ownership epoch qualified early at %d", i)
		}
	}
}

func TestHotPathsAllocateZero(t *testing.T) {
	sketch, _ := balancedSketch(t)
	load := LoadVector{ResourceWriteCPU: 1, ResourceRequests: 1}
	point := testPoint(42)
	if got := testing.AllocsPerRun(1_000, func() {
		sketch.ObservePoint(point, load, 1)
	}); got != 0 {
		t.Fatalf("ObservePoint allocs = %v", got)
	}
	capacities := balancedCapacities()
	policy := Policy{TriggerPressurePPM: 1, MinBenefitPPM: 1}
	if got := testing.AllocsPerRun(1_000, func() {
		_ = Recommend(sketch, capacities, policy)
	}); got != 0 {
		t.Fatalf("Recommend allocs = %v", got)
	}
}

func balancedSketch(t testing.TB) (*Sketch, SourceIdentity) {
	t.Helper()
	range_ := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	source := testSource(range_)
	sketch, err := NewSketch(source)
	if err != nil {
		t.Fatal(err)
	}
	load := LoadVector{ResourceWriteCPU: 10, ResourceRequests: 1, ResourceLiveBytes: 10}
	for i := range BinCount {
		point := testPoint(uint64(i)<<58 | uint64(1)<<57)
		sketch.ObservePoint(point, load, 0)
	}
	return sketch, source
}

func balancedCapacities() CapacitySet {
	cap := capFor(320, 32, 320)
	return CapacitySet{Current: cap, Left: cap, Right: cap, Isolated: cap}
}

func capFor(write, requests, live uint64) CapacityVector {
	return CapacityVector{ResourceWriteCPU: write, ResourceRequests: requests, ResourceLiveBytes: live}
}

func testPoint(value uint64) distribution.KeyspacePoint { return uint64Point(value) }

func testSource(range_ distribution.KeyRange) SourceIdentity {
	return SourceIdentity{
		Distribution: "d", Shard: "s", Range: range_,
		RoutingVersion: 1, OwnershipEpoch: 2,
	}
}

func assertChildrenCover(t testing.TB, source distribution.KeyRange, rec Recommendation) {
	t.Helper()
	start := source.Start
	for i := 0; i < int(rec.BoundaryCount); i++ {
		boundary := rec.Boundaries[i]
		r := distribution.KeyRange{Start: start, End: distribution.KeyspaceEnd{Point: boundary}}
		if !r.Valid() {
			t.Fatalf("invalid child %d: %+v", i, r)
		}
		start = boundary
	}
	last := distribution.KeyRange{Start: start, End: source.End}
	if !last.Valid() {
		t.Fatalf("invalid final child: %+v", last)
	}
	if start == source.Start && rec.BoundaryCount != 0 {
		t.Fatal("boundaries made no progress")
	}
}
