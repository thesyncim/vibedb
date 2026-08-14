package autosplit

import (
	"math"
	"math/bits"

	"github.com/thesyncim/vibedb/distribution"
)

// PPM is the fixed-point scale used for utilization, penalties, and policy.
const PPM uint64 = 1_000_000

// SourceIdentity fences a shadow recommendation to the exact serving range it
// observed. A future workflow must compare every field before acting on it.
type SourceIdentity struct {
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	Range                distribution.KeyRange
	RoutingVersion       distribution.RoutingVersion
	OwnershipEpoch       distribution.OwnershipEpoch
}

func (s SourceIdentity) valid() bool {
	return s.Distribution != "" && s.Shard != "" &&
		s.AllocationGeneration != 0 && s.RoutingVersion != 0 &&
		s.OwnershipEpoch != 0 && s.Range.Valid()
}

// CapacitySet describes current, binary-child, and optional isolated-point
// placements. Isolated is required before an IsolatePoint recommendation can
// be actionable; a zero vector makes an observed hot point unsplittable.
type CapacitySet struct {
	Current  CapacityVector
	Left     CapacityVector
	Right    CapacityVector
	Isolated CapacityVector
}

// Policy contains deterministic shadow-decision bounds. Use DefaultPolicy as
// a conservative starting point; zero values deliberately disable a bound or
// penalty so simulations can isolate each term.
type Policy struct {
	TriggerPressurePPM  uint64
	MinBenefitPPM       uint64
	MaxChildPressurePPM uint64
	CelebritySharePPM   uint64
	FanoutWeightPPM     uint64
	MigrationWeightPPM  uint64
	MigrationBudget     uint64
	UncertaintyPPM      uint64
	MinChildLiveBytes   uint64
}

// DefaultPolicy returns conservative fixed-point shadow defaults.
func DefaultPolicy() Policy {
	return Policy{
		TriggerPressurePPM:  900_000,
		MinBenefitPPM:       100_000,
		MaxChildPressurePPM: 850_000,
		CelebritySharePPM:   300_000,
		FanoutWeightPPM:     250_000,
		MigrationWeightPPM:  100_000,
		MigrationBudget:     1 << 30,
		UncertaintyPPM:      10_000,
	}
}

// RecommendationKind is a shadow-mode outcome. No kind mutates topology.
type RecommendationKind uint8

const (
	RecommendationNone RecommendationKind = iota
	RecommendationBinarySplit
	RecommendationIsolatePoint
	RecommendationUnsplittableHotKey
)

// Reason explains a RecommendationNone result.
type Reason uint8

const (
	ReasonNone Reason = iota
	ReasonNoEvidence
	ReasonEvidenceOverflow
	ReasonSourceMismatch
	ReasonBelowTrigger
	ReasonNoValidBoundary
	ReasonNoBenefit
)

// Recommendation is a pure, generation-fenced shadow decision. IsolatePoint
// may carry one or two boundaries, producing two or three non-overlapping
// children. UnsplittableHotKey carries HotPoint and no actionable boundary.
type Recommendation struct {
	Source         SourceIdentity
	WindowSequence uint64
	Kind           RecommendationKind
	Reason         Reason

	Boundaries    [2]distribution.KeyspacePoint
	BoundaryCount uint8
	CandidateBin  uint8
	HotPoint      distribution.KeyspacePoint

	CurrentPressurePPM   uint64
	PredictedPressurePPM uint64
	BenefitPPM           uint64
	FanoutTaxPPM         uint64
	MigrationTaxPPM      uint64
}

// Recommend evaluates every compact boundary with a dominant-resource
// counterfactual. The source identity comes only from the fenced sketch, so a
// caller cannot relabel stale evidence as a newer ownership incarnation. It is
// side-effect free and performs no allocation.
func Recommend(sketch *Sketch, capacities CapacitySet, policy Policy) Recommendation {
	out := Recommendation{Kind: RecommendationNone}
	if sketch == nil {
		out.Reason = ReasonNoEvidence
		return out
	}
	source := sketch.source
	out.Source, out.WindowSequence = source, sketch.sequence
	if sketch.samples == 0 {
		out.Reason = ReasonNoEvidence
		return out
	}
	if sketch.overflow {
		out.Reason = ReasonEvidenceOverflow
		return out
	}
	current := dominantUtil(sketch.total, capacities.Current)
	out.CurrentPressurePPM = current
	if current < policy.TriggerPressurePPM {
		out.Reason = ReasonBelowTrigger
		return out
	}

	migrationTax := uint64(0)
	if policy.MigrationWeightPPM != 0 && policy.MigrationBudget != 0 {
		migrationTax = mulPPM(ratioPPM(sketch.total[ResourceLiveBytes], policy.MigrationBudget), policy.MigrationWeightPPM)
	}

	var left [ResourceCount]uint64
	bestObjective := uint64(math.MaxUint64)
	bestLoad := uint64(math.MaxUint64)
	bestFanout := uint64(0)
	bestBin := -1
	crossing := int64(0)
	spanQueries := saturatingPlainAdd(sketch.bounded, sketch.unbounded)
	queries := max(sketch.total[ResourceRequests], spanQueries)
	lastBoundary := distribution.KeyspacePoint{}
	for k := 1; k < BinCount; k++ {
		for resource := range ResourceCount {
			left[resource] = saturatingPlainAdd(left[resource], uint64(sketch.bins[k-1][resource]))
		}
		var crossingOK bool
		crossing, crossingOK = addSigned(crossing, sketch.cross[k])
		if !crossingOK {
			out.Reason = ReasonEvidenceOverflow
			return out
		}
		boundary := sketch.boundary(k)
		if boundary == source.Range.Start || boundary == lastBoundary ||
			(!source.Range.End.Max && boundary == source.Range.End.Point) {
			lastBoundary = boundary
			continue
		}
		lastBoundary = boundary

		var right [ResourceCount]uint64
		for resource := range ResourceCount {
			right[resource] = sketch.total[resource] - left[resource]
		}
		if left[ResourceLiveBytes] < policy.MinChildLiveBytes || right[ResourceLiveBytes] < policy.MinChildLiveBytes {
			continue
		}
		loadCost := max(dominantUtil(left, capacities.Left), dominantUtil(right, capacities.Right))
		if policy.MaxChildPressurePPM != 0 && loadCost > policy.MaxChildPressurePPM {
			continue
		}
		fanoutNumerator := saturatingPlainAdd(uint64(max(crossing, 0)), sketch.unbounded)
		fanoutTax := uint64(0)
		if queries != 0 && policy.FanoutWeightPPM != 0 {
			fanoutTax = mulPPM(ratioPPM(fanoutNumerator, queries), policy.FanoutWeightPPM)
		}
		objective := saturatingPlainAdd(loadCost, policy.UncertaintyPPM)
		objective = saturatingPlainAdd(objective, fanoutTax)
		objective = saturatingPlainAdd(objective, migrationTax)
		if objective < bestObjective || (objective == bestObjective && k < bestBin) {
			bestObjective, bestLoad, bestFanout, bestBin = objective, loadCost, fanoutTax, k
		}
	}

	celebrity, hasCelebrity := strongestCelebrity(sketch, policy.CelebritySharePPM)
	if hasCelebrity && policy.MaxChildPressurePPM != 0 &&
		(bestBin < 0 || bestLoad > policy.MaxChildPressurePPM) {
		// SpaceSaving proves a lower bound for identity/frequency. Cost the entire
		// compact bin against isolated capacity (an upper bound for the hot point),
		// then subtract only the point load observed while its slot was resident
		// from the bin. The remaining bin load is an upper bound for neighbours;
		// charge it to both residual sides because the compact bin does not retain
		// their exact position around the point.
		var hotLoad, residualLeft, residualRight [ResourceCount]uint64
		hotBin := sketch.binFor(celebrity.point)
		for resource := range ResourceCount {
			hotLoad[resource] = uint64(sketch.bins[hotBin][resource])
			for bin := 0; bin < hotBin; bin++ {
				residualLeft[resource] += uint64(sketch.bins[bin][resource])
			}
			for bin := hotBin + 1; bin < BinCount; bin++ {
				residualRight[resource] += uint64(sketch.bins[bin][resource])
			}
			knownHot := min(celebrity.load[resource], hotLoad[resource])
			binResidual := hotLoad[resource] - knownHot
			residualLeft[resource] = saturatingPlainAdd(residualLeft[resource], binResidual)
			residualRight[resource] = saturatingPlainAdd(residualRight[resource], binResidual)
		}
		hotUtil := dominantUtil(hotLoad, capacities.Isolated)
		provenHotUtil := dominantUtil(celebrity.load, capacities.Isolated)
		out.HotPoint = celebrity.point
		out.CandidateBin = uint8(hotBin)
		out.BoundaryCount = isolationBoundaries(source.Range, celebrity.point, &out.Boundaries)
		if out.BoundaryCount == 0 {
			out.Kind = RecommendationUnsplittableHotKey
			return out
		}
		hasLeft := distribution.ComparePoints(celebrity.point, source.Range.Start) > 0
		hasRight := pointUint64(celebrity.point) != math.MaxUint64 &&
			(source.Range.End.Max || distribution.ComparePoints(
				uint64Point(pointUint64(celebrity.point)+1), source.Range.End.Point) < 0)
		leftUtil, rightUtil := uint64(0), uint64(0)
		if hasLeft {
			leftUtil = dominantUtil(residualLeft, capacities.Left)
		}
		if hasRight {
			rightUtil = dominantUtil(residualRight, capacities.Right)
		}
		isolationLoad := max(hotUtil, max(leftUtil, rightUtil))
		if isolationLoad > policy.MaxChildPressurePPM {
			if provenHotUtil > policy.MaxChildPressurePPM {
				out.PredictedPressurePPM = provenHotUtil
				out.Kind = RecommendationUnsplittableHotKey
				out.BoundaryCount = 0
				return out
			}
			out.Kind = RecommendationNone
			out.Reason = ReasonNoValidBoundary
			out.BoundaryCount = 0
			return out
		}
		// Exact point boundaries are finer than the 64-bin crossing sketch. Charge
		// every bounded and unbounded request to every new boundary: conservative
		// for admission and never optimistic about scatter amplification.
		fanoutNumerator := saturatingPlainMul(spanQueries, uint64(out.BoundaryCount))
		fanoutTax := uint64(0)
		if queries != 0 && policy.FanoutWeightPPM != 0 {
			fanoutTax = mulPPM(ratioPPM(fanoutNumerator, queries), policy.FanoutWeightPPM)
		}
		out.FanoutTaxPPM = fanoutTax
		out.MigrationTaxPPM = migrationTax
		out.PredictedPressurePPM = isolationLoad
		out.PredictedPressurePPM = saturatingPlainAdd(out.PredictedPressurePPM, policy.UncertaintyPPM)
		out.PredictedPressurePPM = saturatingPlainAdd(out.PredictedPressurePPM, fanoutTax)
		out.PredictedPressurePPM = saturatingPlainAdd(out.PredictedPressurePPM, migrationTax)
		out.Kind = RecommendationIsolatePoint
		if current > out.PredictedPressurePPM {
			out.BenefitPPM = current - out.PredictedPressurePPM
		}
		if out.BenefitPPM < policy.MinBenefitPPM {
			out.Kind = RecommendationNone
			out.Reason = ReasonNoBenefit
			out.BoundaryCount = 0
		}
		return out
	}

	if bestBin < 0 {
		out.Reason = ReasonNoValidBoundary
		return out
	}
	out.PredictedPressurePPM = bestObjective
	out.FanoutTaxPPM = bestFanout
	out.MigrationTaxPPM = migrationTax
	if current > bestObjective {
		out.BenefitPPM = current - bestObjective
	}
	if out.BenefitPPM < policy.MinBenefitPPM {
		out.Reason = ReasonNoBenefit
		return out
	}
	out.Kind = RecommendationBinarySplit
	out.Boundaries[0] = sketch.boundary(bestBin)
	out.BoundaryCount = 1
	out.CandidateBin = uint8(bestBin)
	return out
}

func strongestCelebrity(sketch *Sketch, sharePPM uint64) (heavyHitter, bool) {
	if sharePPM == 0 || sketch.hotTotal == 0 {
		return heavyHitter{}, false
	}
	best := -1
	bestLower := uint64(0)
	for i := range sketch.heavy {
		h := &sketch.heavy[i]
		if !h.used || h.estimate < h.error {
			continue
		}
		lower := h.estimate - h.error
		if ratioPPM(lower, sketch.hotTotal) < sharePPM {
			continue
		}
		if best < 0 || lower > bestLower ||
			(lower == bestLower && distribution.ComparePoints(h.point, sketch.heavy[best].point) < 0) {
			best, bestLower = i, lower
		}
	}
	if best < 0 {
		return heavyHitter{}, false
	}
	return sketch.heavy[best], true
}

func isolationBoundaries(r distribution.KeyRange, point distribution.KeyspacePoint, out *[2]distribution.KeyspacePoint) uint8 {
	n := uint8(0)
	if distribution.ComparePoints(point, r.Start) > 0 {
		out[n] = point
		n++
	}
	value := pointUint64(point)
	if value != math.MaxUint64 {
		next := uint64Point(value + 1)
		if r.End.Max || distribution.ComparePoints(next, r.End.Point) < 0 {
			out[n] = next
			n++
		}
	}
	return n
}

func dominantUtil(load [ResourceCount]uint64, capacity CapacityVector) uint64 {
	var pressure uint64
	for resource := range ResourceCount {
		if load[resource] == 0 {
			continue
		}
		if capacity[resource] == 0 {
			return math.MaxUint64
		}
		pressure = max(pressure, ratioPPM(load[resource], capacity[resource]))
	}
	return pressure
}

func ratioPPM(value, capacity uint64) uint64 {
	if value == 0 {
		return 0
	}
	if capacity == 0 {
		return math.MaxUint64
	}
	hi, lo := bits.Mul64(value, PPM)
	if hi >= capacity {
		return math.MaxUint64
	}
	q, _ := bits.Div64(hi, lo, capacity)
	return q
}

func mulPPM(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	if hi >= PPM {
		return math.MaxUint64
	}
	q, _ := bits.Div64(hi, lo, PPM)
	return q
}

func saturatingPlainAdd(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func saturatingPlainMul(a, b uint64) uint64 {
	if b != 0 && a > math.MaxUint64/b {
		return math.MaxUint64
	}
	return a * b
}

func addSigned(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64, false
	}
	if b < 0 && a < math.MinInt64-b {
		return math.MinInt64, false
	}
	return a + b, true
}
