package autosplit

import (
	"errors"
	"math"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
)

func TestSketchFixedSpaceAccountingAndDeterminism(t *testing.T) {
	range_ := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	source := testSource(range_)
	a, err := NewSketch(source, 17)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewSketch(source, 17)
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
	pulse := a.Pulse()
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
	outside, _ := NewSketch(testSource(outsideRange), 1)
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
	if got := unsafe.Sizeof(CapacitySet{}); got > 320 {
		t.Fatalf("CapacitySet size = %d B, want <= 320 B", got)
	} else {
		t.Logf("CapacitySet size = %d B", got)
	}
}

func TestSketchRejectsIncompleteWindowFence(t *testing.T) {
	source := testSource(balancedRange())
	if _, err := NewSketch(source, 0); !errors.Is(err, ErrInvalidSequence) {
		t.Fatalf("zero sequence err = %v, want ErrInvalidSequence", err)
	}
	source.AllocationGeneration = 0
	if _, err := NewSketch(source, 1); !errors.Is(err, ErrInvalidSource) {
		t.Fatalf("zero allocation err = %v, want ErrInvalidSource", err)
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
		sketch, err := NewSketch(testSource(range_), 1)
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
	sketch, _ := NewSketch(testSource(distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}), 1)
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

func TestRecommendProjectsUnknownLocalityWithoutFirstBinBias(t *testing.T) {
	source := testSource(balancedRange())
	sketch, _ := NewSketch(source, 1)
	sketch.observeUniform(LoadVector{
		ResourceWriteCPU: 64, ResourceRequests: 64, ResourceLiveBytes: 64,
	})
	cap := capFor(32, 32, 32)
	recommendation := Recommend(sketch, CapacitySet{
		Source: source, WindowSequence: 1,
		Current: cap, Left: cap, Right: cap, Isolated: cap,
	}, Policy{TriggerPressurePPM: 1_100_000, MinBenefitPPM: 500_000})
	if recommendation.Kind != RecommendationBinarySplit ||
		recommendation.CandidateBin != BinCount/2 ||
		recommendation.Boundaries[0] != testPoint(uint64(1)<<63) {
		t.Fatalf("uniform recommendation = %+v", recommendation)
	}

	tiny, _ := NewSketch(source, 2)
	tiny.observeUniform(LoadVector{ResourceRequests: 1})
	tinyRecommendation := Recommend(tiny, CapacitySet{
		Source: source, WindowSequence: 2,
		Current:  CapacityVector{ResourceRequests: 1},
		Left:     CapacityVector{ResourceRequests: 1},
		Right:    CapacityVector{ResourceRequests: 1},
		Isolated: CapacityVector{ResourceRequests: 1},
	}, Policy{TriggerPressurePPM: 1, MinBenefitPPM: 1})
	if tinyRecommendation.Kind != RecommendationNone {
		t.Fatalf("one unknown request fabricated a split: %+v", tinyRecommendation)
	}
}

func TestRecommendCelebrityIsolationAndUnsplittable(t *testing.T) {
	range_ := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	source := SourceIdentity{
		Distribution: "d", Shard: "s", AllocationGeneration: 3,
		Range: range_, BucketBits: distribution.DefaultVirtualBucketBits,
		RoutingVersion: 7, OwnershipEpoch: 9,
	}
	sketch, _ := NewSketch(source, 1)
	hot := testPoint(uint64(12345)<<(64-source.BucketBits) | 17)
	hotBucket := testPoint(uint64(12345) << (64 - source.BucketBits))
	neighbor := testPoint(uint64(12346)<<(64-source.BucketBits) | 17)
	for range 70 {
		sketch.ObservePoint(hot, LoadVector{ResourceWriteCPU: 10, ResourceRequests: 1, ResourceLiveBytes: 1}, 10)
	}
	for range 10 {
		sketch.ObservePoint(neighbor, LoadVector{ResourceWriteCPU: 10, ResourceRequests: 1, ResourceLiveBytes: 1}, 1)
	}
	if !sketch.ObserveSpan(
		hot,
		distribution.KeyspaceEnd{Point: testPoint(uint64(12347) << (64 - source.BucketBits))},
		10,
	) {
		t.Fatal("celebrity-crossing span rejected")
	}
	policy := Policy{
		TriggerPressurePPM: 1_000_000, MaxChildPressurePPM: 750_000,
		CelebritySharePPM: 500_000,
	}
	capacities := CapacitySet{
		Source: source, WindowSequence: 1,
		Current: capFor(500, 40, 40), Left: capFor(1_000, 100, 100), Right: capFor(1_000, 100, 100),
		Isolated: capFor(1_200, 120, 120),
	}
	rec := Recommend(sketch, capacities, policy)
	if rec.Kind != RecommendationIsolateBucket || rec.HotBucketStart != hotBucket || rec.BoundaryCount != 2 {
		t.Fatalf("isolation recommendation = %+v", rec)
	}
	if rec.Boundaries[0] != hotBucket ||
		pointUint64(rec.Boundaries[1]) != pointUint64(hotBucket)+(uint64(1)<<(64-source.BucketBits)) {
		t.Fatalf("isolation boundaries = %x, %x", rec.Boundaries[0], rec.Boundaries[1])
	}
	assertChildrenCover(t, source.Range, rec)

	defaultCapacities := CapacitySet{
		Source: source, WindowSequence: 1,
		Current: capFor(500, 40, 40), Left: capFor(900, 100, 100), Right: capFor(900, 100, 100),
		Isolated: capFor(1_200, 120, 120),
	}
	defaultRec := Recommend(sketch, defaultCapacities, DefaultPolicy())
	if defaultRec.Kind != RecommendationIsolateBucket || defaultRec.BenefitPPM < DefaultPolicy().MinBenefitPPM {
		t.Fatalf("default-policy isolation recommendation = %+v", defaultRec)
	}
	if defaultRec.FanoutTaxPPM == 0 {
		t.Fatal("default-policy isolation omitted conservative boundary fanout tax")
	}

	capacities.Isolated = capFor(600, 100, 100)
	rec = Recommend(sketch, capacities, policy)
	if rec.Kind != RecommendationUnsplittableBucket || rec.HotBucketStart != hotBucket || rec.BoundaryCount != 0 {
		t.Fatalf("unsplittable recommendation = %+v", rec)
	}
}

func TestRecommendDoesNotCallCrowdedBinHotKeyUnsplittable(t *testing.T) {
	range_ := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	source := testSource(range_)
	sketch, _ := NewSketch(source, 1)
	hot := testPoint(uint64(12345)<<(64-source.BucketBits) | 17)
	neighbor := testPoint(uint64(12346)<<(64-source.BucketBits) | 17)
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
		Source: source, WindowSequence: 1,
		Current: capFor(500, 100, 1), Left: capFor(2_000, 100, 1),
		Right: capFor(2_000, 100, 1), Isolated: capFor(1_000, 100, 1),
	}
	rec := Recommend(sketch, capacities, policy)
	if rec.Kind == RecommendationUnsplittableBucket {
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
	sketch, err := NewSketch(source, 1)
	if err != nil {
		t.Fatal(err)
	}
	rec := Recommend(sketch, balancedCapacities(), DefaultPolicy())
	if rec.Reason != ReasonNoEvidence || rec.Source != source || rec.WindowSequence != 1 {
		t.Fatalf("empty recommendation = %+v, want source %+v", rec, source)
	}
}

func TestRecommendRejectsCapacityFenceMismatchBeforePlanning(t *testing.T) {
	sketch, source := balancedSketch(t)
	matched := balancedCapacities()

	mutations := []struct {
		name   string
		mutate func(*CapacitySet)
	}{
		{name: "zero", mutate: func(capacities *CapacitySet) { *capacities = CapacitySet{} }},
		{name: "distribution", mutate: func(capacities *CapacitySet) { capacities.Source.Distribution = "other" }},
		{name: "shard", mutate: func(capacities *CapacitySet) { capacities.Source.Shard = "other" }},
		{name: "allocation", mutate: func(capacities *CapacitySet) { capacities.Source.AllocationGeneration++ }},
		{name: "range", mutate: func(capacities *CapacitySet) { capacities.Source.Range.Start = testPoint(1) }},
		{name: "routing", mutate: func(capacities *CapacitySet) { capacities.Source.RoutingVersion++ }},
		{name: "ownership", mutate: func(capacities *CapacitySet) { capacities.Source.OwnershipEpoch++ }},
		{name: "sequence", mutate: func(capacities *CapacitySet) { capacities.WindowSequence++ }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			capacities := matched
			test.mutate(&capacities)
			rec := Recommend(sketch, capacities, Policy{
				TriggerPressurePPM: 1, MinBenefitPPM: 1,
			})
			if rec.Source != source || rec.WindowSequence != 1 ||
				rec.Kind != RecommendationNone || rec.Reason != ReasonSourceMismatch {
				t.Fatalf("mismatch recommendation = %+v", rec)
			}
			if rec.BoundaryCount != 0 || rec.Boundaries != [2]distribution.KeyspacePoint{} ||
				rec.HotBucketStart != (distribution.KeyspacePoint{}) || rec.CandidateBin != 0 ||
				rec.CurrentPressurePPM != 0 || rec.PredictedPressurePPM != 0 ||
				rec.BenefitPPM != 0 || rec.FanoutTaxPPM != 0 || rec.MigrationTaxPPM != 0 {
				t.Fatalf("mismatch leaked decision state: %+v", rec)
			}
		})
	}
}

func TestRecommendCapacityMismatchPrecedesEvidenceState(t *testing.T) {
	source := testSource(balancedRange())
	sketch, err := NewSketch(source, 9)
	if err != nil {
		t.Fatal(err)
	}
	sketch.overflow = true
	capacities := balancedCapacities()
	if rec := Recommend(sketch, capacities, DefaultPolicy()); rec.Reason != ReasonSourceMismatch || rec.Source != source || rec.WindowSequence != 9 {
		t.Fatalf("mismatch precedence = %+v", rec)
	}
	if rec := Recommend(nil, capacities, DefaultPolicy()); rec.Reason != ReasonNoEvidence {
		t.Fatalf("nil sketch reason = %v, want ReasonNoEvidence", rec.Reason)
	}
}

func TestCapacityMismatchCannotSatisfyTrackerEvidence(t *testing.T) {
	sketch, source := balancedSketch(t)
	second := *sketch
	second.sequence = 2
	capacities := balancedCapacities()
	mismatch := Recommend(&second, capacities, DefaultPolicy())
	policy := TrackerPolicy{
		WindowCount: 3, RequiredWindows: 3, TriggerPressurePPM: 1,
	}
	var tracker Tracker
	if tracker.Observe(testBinaryRecommendation(source, 1, 32), policy) ||
		tracker.Observe(mismatch, policy) ||
		tracker.Observe(testBinaryRecommendation(source, 3, 32), policy) ||
		tracker.Observe(testBinaryRecommendation(source, 4, 32), policy) {
		t.Fatal("capacity mismatch advanced actionable tracker evidence")
	}
	if !tracker.Observe(testBinaryRecommendation(source, 5, 32), policy) {
		t.Fatal("three subsequent valid windows did not qualify")
	}
}

func TestTrackerRequiresSustainedStableEvidenceAndAppliesCooldown(t *testing.T) {
	policy := DefaultTrackerPolicy()
	hot := testBinaryRecommendation(testSource(balancedRange()), 1, 32)
	var burst Tracker
	if burst.Observe(hot, policy) {
		t.Fatal("one-window burst qualified")
	}
	for sequence := uint64(2); sequence <= 8; sequence++ {
		cold := Recommendation{
			Source: hot.Source, WindowSequence: sequence,
			Kind: RecommendationNone, CurrentPressurePPM: 1_100_000,
		}
		if burst.Observe(cold, policy) {
			t.Fatal("isolated burst qualified later")
		}
	}

	var sustained Tracker
	for i := 0; i < 7; i++ {
		hot.WindowSequence = uint64(i + 1)
		if sustained.Observe(hot, policy) {
			t.Fatalf("qualified early at window %d", i+1)
		}
	}
	hot.WindowSequence = 8
	if !sustained.Observe(hot, policy) {
		t.Fatal("eight stable hot windows did not qualify")
	}
	for i := 0; i < int(policy.CooldownWindows); i++ {
		hot.WindowSequence++
		if sustained.Observe(hot, policy) {
			t.Fatalf("qualified during cooldown at window %d", i+1)
		}
	}
}

func TestTrackerBoundaryDriftResetsEvidence(t *testing.T) {
	policy := DefaultTrackerPolicy()
	var tracker Tracker
	rec := testBinaryRecommendation(testSource(balancedRange()), 1, 10)
	for sequence := uint64(1); sequence <= 5; sequence++ {
		rec.WindowSequence = sequence
		tracker.Observe(rec, policy)
	}
	rec.CandidateBin = 30
	rec.Boundaries[0] = boundaryForRange(rec.Source.Range, 30)
	rec.WindowSequence = 6
	if tracker.Observe(rec, policy) {
		t.Fatal("drifting candidate retained old evidence")
	}
	for i := 0; i < 6; i++ {
		rec.WindowSequence++
		if tracker.Observe(rec, policy) {
			t.Fatalf("qualified before a complete post-drift window at %d", i)
		}
	}
}

func TestTrackerResetsAcrossSourceFence(t *testing.T) {
	policy := DefaultTrackerPolicy()
	rec := testBinaryRecommendation(testSource(balancedRange()), 1, 32)
	var tracker Tracker
	for sequence := uint64(1); sequence <= 7; sequence++ {
		rec.WindowSequence = sequence
		tracker.Observe(rec, policy)
	}
	rec.Source.OwnershipEpoch++
	rec.WindowSequence = 1
	if tracker.Observe(rec, policy) {
		t.Fatal("new ownership epoch inherited old evidence")
	}
	for i := 0; i < 6; i++ {
		rec.WindowSequence++
		if tracker.Observe(rec, policy) {
			t.Fatalf("new ownership epoch qualified early at %d", i)
		}
	}
}

func TestTrackerIgnoresReplayRegressionAndInvalidWindows(t *testing.T) {
	policy := DefaultTrackerPolicy()
	source := testSource(balancedRange())
	rec := testBinaryRecommendation(source, 1, 32)
	var tracker Tracker
	if tracker.Observe(rec, policy) {
		t.Fatal("first window qualified")
	}
	rec.WindowSequence = 2
	if tracker.Observe(rec, policy) {
		t.Fatal("second window qualified")
	}

	before := tracker
	for _, invalid := range []Recommendation{
		rec,
		testBinaryRecommendation(source, 1, 32),
		testBinaryRecommendation(source, 0, 32),
	} {
		if tracker.Observe(invalid, policy) {
			t.Fatalf("ignored window qualified: %+v", invalid)
		}
		if tracker != before {
			t.Fatalf("ignored window mutated tracker\n got: %+v\nwant: %+v", tracker, before)
		}
	}
	invalidSource := testBinaryRecommendation(source, 3, 32)
	invalidSource.Source.AllocationGeneration = 0
	if tracker.Observe(invalidSource, policy) || tracker != before {
		t.Fatalf("invalid source mutated tracker: %+v", tracker)
	}
}

func TestTrackerSequenceGapStartsFreshEvidence(t *testing.T) {
	policy := DefaultTrackerPolicy()
	rec := testBinaryRecommendation(testSource(balancedRange()), 1, 32)
	var tracker Tracker
	for sequence := uint64(1); sequence <= 7; sequence++ {
		rec.WindowSequence = sequence
		if tracker.Observe(rec, policy) {
			t.Fatalf("qualified before gap at %d", sequence)
		}
	}
	rec.WindowSequence = 9
	rec.CurrentPressurePPM = 1_200_000
	if tracker.Observe(rec, policy) {
		t.Fatal("gapped window inherited prior evidence")
	}
	if tracker.seen != 1 || tracker.lastSequence != 9 ||
		tracker.fast != 1_200_000 || tracker.slow != 1_200_000 {
		t.Fatalf("post-gap tracker = %+v, want one fresh window at sequence 9", tracker)
	}
	for sequence := uint64(10); sequence <= 15; sequence++ {
		rec.WindowSequence = sequence
		if tracker.Observe(rec, policy) {
			t.Fatalf("qualified early after gap at %d", sequence)
		}
	}
	rec.WindowSequence = 16
	if !tracker.Observe(rec, policy) {
		t.Fatal("eight contiguous post-gap windows did not qualify")
	}
}

func TestTrackerRejectsMalformedActionableWindows(t *testing.T) {
	policy := DefaultTrackerPolicy()
	source := testSource(balancedRange())
	var tracker Tracker
	for sequence := uint64(1); sequence <= 16; sequence++ {
		rec := testBinaryRecommendation(source, sequence, 32)
		rec.Boundaries[0] = testPoint(7)
		if tracker.Observe(rec, policy) {
			t.Fatalf("malformed actionable window qualified at %d", sequence)
		}
	}
	if tracker.history != 0 || tracker.stable {
		t.Fatalf("malformed windows entered actionable history: %+v", tracker)
	}
}

func TestTrackerEverySourceCoordinateStartsFresh(t *testing.T) {
	base := testSource(balancedRange())
	changes := []struct {
		name   string
		change func(*SourceIdentity)
	}{
		{"allocation", func(s *SourceIdentity) { s.AllocationGeneration++ }},
		{"routing", func(s *SourceIdentity) { s.RoutingVersion++ }},
		{"ownership", func(s *SourceIdentity) { s.OwnershipEpoch++ }},
		{"range", func(s *SourceIdentity) {
			s.Range.Start = testPoint(uint64(1) << (64 - s.BucketBits))
		}},
	}
	for _, test := range changes {
		t.Run(test.name, func(t *testing.T) {
			policy := DefaultTrackerPolicy()
			var tracker Tracker
			for sequence := uint64(1); sequence <= 7; sequence++ {
				tracker.Observe(testBinaryRecommendation(base, sequence, 32), policy)
			}
			next := base
			test.change(&next)
			if tracker.Observe(testBinaryRecommendation(next, 1, 32), policy) {
				t.Fatal("new source coordinate inherited old evidence")
			}
			if tracker.source != next || tracker.lastSequence != 1 || tracker.seen != 1 {
				t.Fatalf("fresh tracker = %+v", tracker)
			}
		})
	}
}

func TestTrackerSlowBoundaryWalkCannotQualify(t *testing.T) {
	policy := DefaultTrackerPolicy()
	source := testSource(balancedRange())
	var tracker Tracker
	for i := 0; i < int(policy.WindowCount); i++ {
		rec := testBinaryRecommendation(source, uint64(i+1), uint8(10+i))
		if tracker.Observe(rec, policy) {
			t.Fatalf("one-bin boundary walk qualified at window %d", i+1)
		}
	}
}

func TestTrackerIsolationRequiresExactHotBucket(t *testing.T) {
	policy := DefaultTrackerPolicy()
	source := testSource(balancedRange())
	hotA := testPoint(uint64(100)<<(64-source.BucketBits) | 1)
	hotB := testPoint(uint64(101)<<(64-source.BucketBits) | 1)
	if binForRange(source.Range, hotA) != binForRange(source.Range, hotB) {
		t.Fatal("test hot points must occupy the same compact bin")
	}
	var tracker Tracker
	for sequence := uint64(1); sequence <= 5; sequence++ {
		if tracker.Observe(testIsolateRecommendation(source, sequence, hotA), policy) {
			t.Fatalf("first hot bucket qualified early at %d", sequence)
		}
	}
	if tracker.Observe(testIsolateRecommendation(source, 6, hotB), policy) {
		t.Fatal("different hot bucket inherited same-bin evidence")
	}
	for sequence := uint64(7); sequence <= 12; sequence++ {
		if tracker.Observe(testIsolateRecommendation(source, sequence, hotB), policy) {
			t.Fatalf("second hot bucket qualified early at %d", sequence)
		}
	}
	if !tracker.Observe(testIsolateRecommendation(source, 13, hotB), policy) {
		t.Fatal("eight exact hot-bucket windows did not qualify")
	}
}

func TestTrackerSourceAllocationAndSequenceExhaustionFence(t *testing.T) {
	policy := DefaultTrackerPolicy()
	source := testSource(balancedRange())
	var tracker Tracker
	maxRec := testBinaryRecommendation(source, math.MaxUint64, 32)
	if tracker.Observe(maxRec, policy) {
		t.Fatal("first terminal-sequence window qualified")
	}
	before := tracker
	if tracker.Observe(maxRec, policy) || tracker != before {
		t.Fatal("terminal sequence replay mutated tracker")
	}

	source.AllocationGeneration++
	rec := testBinaryRecommendation(source, 1, 32)
	if tracker.Observe(rec, policy) {
		t.Fatal("new allocation inherited terminal sequence evidence")
	}
	if tracker.source != source || tracker.lastSequence != 1 || tracker.seen != 1 {
		t.Fatalf("new allocation tracker = %+v", tracker)
	}
}

func TestTrackerGapPreservesCooldownAndReplayDoesNotAgeIt(t *testing.T) {
	policy := DefaultTrackerPolicy()
	rec := testBinaryRecommendation(testSource(balancedRange()), 1, 32)
	var tracker Tracker
	for sequence := uint64(1); sequence <= 8; sequence++ {
		rec.WindowSequence = sequence
		tracker.Observe(rec, policy)
	}
	if tracker.cooldown != policy.CooldownWindows {
		t.Fatalf("cooldown = %d, want %d", tracker.cooldown, policy.CooldownWindows)
	}
	rec.WindowSequence = 10
	if tracker.Observe(rec, policy) {
		t.Fatal("gapped cooldown window qualified")
	}
	if tracker.cooldown != policy.CooldownWindows-1 {
		t.Fatalf("gap cooldown = %d, want %d", tracker.cooldown, policy.CooldownWindows-1)
	}
	before := tracker
	if tracker.Observe(rec, policy) || tracker != before {
		t.Fatal("replayed cooldown window aged or mutated tracker")
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
	capacities.WindowSequence++
	if got := testing.AllocsPerRun(1_000, func() {
		_ = Recommend(sketch, capacities, policy)
	}); got != 0 {
		t.Fatalf("mismatched Recommend allocs = %v", got)
	}
	trackerPolicy := DefaultTrackerPolicy()
	rec := testBinaryRecommendation(sketch.source, 1, 32)
	var tracker Tracker
	if got := testing.AllocsPerRun(1_000, func() {
		rec.WindowSequence++
		_ = tracker.Observe(rec, trackerPolicy)
	}); got != 0 {
		t.Fatalf("Tracker.Observe allocs = %v", got)
	}
}

func balancedSketch(t testing.TB) (*Sketch, SourceIdentity) {
	t.Helper()
	range_ := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	source := testSource(range_)
	sketch, err := NewSketch(source, 1)
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
	return CapacitySet{
		Source: testSource(balancedRange()), WindowSequence: 1,
		Current: cap, Left: cap, Right: cap, Isolated: cap,
	}
}

func capFor(write, requests, live uint64) CapacityVector {
	return CapacityVector{ResourceWriteCPU: write, ResourceRequests: requests, ResourceLiveBytes: live}
}

func testPoint(value uint64) distribution.KeyspacePoint { return uint64Point(value) }

func testSource(range_ distribution.KeyRange) SourceIdentity {
	return SourceIdentity{
		Distribution: "d", Shard: "s", AllocationGeneration: 1, Range: range_,
		BucketBits:     distribution.DefaultVirtualBucketBits,
		RoutingVersion: 1, OwnershipEpoch: 2,
	}
}

func testBinaryRecommendation(source SourceIdentity, sequence uint64, bin uint8) Recommendation {
	return Recommendation{
		Source: source, WindowSequence: sequence,
		Kind: RecommendationBinarySplit, Reason: ReasonNone,
		Boundaries:    [2]distribution.KeyspacePoint{boundaryForRange(source.Range, int(bin))},
		BoundaryCount: 1, CandidateBin: bin,
		CurrentPressurePPM: 1_100_000, BenefitPPM: 200_000,
	}
}

func testIsolateRecommendation(
	source SourceIdentity,
	sequence uint64,
	hot distribution.KeyspacePoint,
) Recommendation {
	bucket, _ := distribution.VirtualBucketForPoint(hot, source.BucketBits)
	bucketRange, _ := distribution.VirtualBucketRange(bucket, source.BucketBits)
	hot = bucketRange.Start
	var boundaries [2]distribution.KeyspacePoint
	count := isolationBoundaries(source.Range, source.BucketBits, hot, &boundaries)
	return Recommendation{
		Source: source, WindowSequence: sequence,
		Kind: RecommendationIsolateBucket, Reason: ReasonNone,
		Boundaries: boundaries, BoundaryCount: count,
		CandidateBin: uint8(binForRange(source.Range, hot)), HotBucketStart: hot,
		CurrentPressurePPM: 1_100_000, BenefitPPM: 200_000,
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
