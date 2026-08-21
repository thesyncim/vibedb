// Package topologyscheduler contains bounded, side-effect-free admission for
// topology work. Data-plane evidence stays local to shard allocations; this
// package never makes a tenant a placement or scheduling unit.
package topologyscheduler

import (
	"cmp"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

const (
	MaxCandidates = 4096
	MaxBatch      = 64
)

var ErrInvalidAdmission = errors.New("topologyscheduler: invalid split admission cut")

// SplitCandidate carries one sustained hot-range recommendation plus a cold
// estimate of bytes that must move. CatalogGeneration fences the evidence to
// the immutable catalog observed by the telemetry controller.
type SplitCandidate struct {
	CatalogGeneration uint64
	Recommendation    autosplit.Recommendation
	MigrationBytes    uint64
}

// Policy bounds one successor publication. MaxPerDistribution prevents one
// logical keyspace from monopolizing a batch while still allowing parallel
// splits within that keyspace. MigrationBudget is an admission bound, not a
// memory reservation.
type Policy struct {
	MaxBatch           uint8
	MaxPerDistribution uint8
	MinBenefitPPM      uint64
	MigrationBudget    uint64
}

// DefaultPolicy returns conservative bounded admission for one catalog CAS.
func DefaultPolicy() Policy {
	return Policy{
		MaxBatch: 16, MaxPerDistribution: 4,
		MinBenefitPPM: 100_000, MigrationBudget: 1 << 30,
	}
}

// Decision is a fixed-size selection into the caller-owned candidates slice.
// Ordinals are priority ordered. Diagnostic counters make skipped work visible
// without allocating reason records per candidate.
type Decision struct {
	CatalogGeneration uint64
	MigrationBytes    uint64
	Ordinals          [MaxBatch]uint16
	Count             uint8

	Stale        uint16
	Invalid      uint16
	Duplicate    uint16
	Distribution uint16
	Budget       uint16
	Deferred     uint16
}

// Workspace is caller-owned fixed scratch. Reusing it keeps warm admission
// allocation-free regardless of candidate count.
type Workspace struct {
	order [MaxCandidates]uint16
}

// SelectSplits returns the highest-benefit exact allocation cuts admitted by
// policy. It is deterministic for the same catalog and candidate set,
// independent of input order except between byte-identical duplicates.
func SelectSplits(
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
	policy Policy,
	workspace *Workspace,
) (Decision, error) {
	return selectSplits(catalog, candidates, policy, workspace, nil)
}

// SelectSplitsWithFeedback applies the same deterministic admission cut while
// deferring exact source incarnations that are already in flight or inside a
// retry window. Feedback is a scheduling optimization, never topology proof.
func SelectSplitsWithFeedback(
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
	policy Policy,
	workspace *Workspace,
	feedback *FeedbackTable,
) (Decision, error) {
	if feedback == nil {
		return Decision{}, ErrInvalidAdmission
	}
	return selectSplits(catalog, candidates, policy, workspace, feedback)
}

func selectSplits(
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
	policy Policy,
	workspace *Workspace,
	feedback *FeedbackTable,
) (Decision, error) {
	if catalog == nil || workspace == nil || len(candidates) > MaxCandidates ||
		policy.MaxBatch == 0 || policy.MaxBatch > MaxBatch ||
		policy.MaxPerDistribution == 0 ||
		policy.MaxPerDistribution > policy.MaxBatch {
		return Decision{}, ErrInvalidAdmission
	}
	decision := Decision{CatalogGeneration: catalog.Generation()}
	for index := range candidates {
		workspace.order[index] = uint16(index)
	}
	slices.SortFunc(workspace.order[:len(candidates)], func(left, right uint16) int {
		return compareCandidates(candidates[left], candidates[right])
	})

	for _, ordinal := range workspace.order[:len(candidates)] {
		candidate := &candidates[ordinal]
		recommendation := candidate.Recommendation
		switch {
		case candidate.CatalogGeneration != catalog.Generation():
			decision.Stale++
			continue
		case !recommendation.Actionable() ||
			recommendation.BenefitPPM < policy.MinBenefitPPM ||
			!exactSource(catalog, recommendation.Source):
			decision.Invalid++
			continue
		case feedback != nil && !feedback.eligible(candidate):
			decision.Deferred++
			continue
		case selectedSource(&decision, candidates, recommendation.Source):
			decision.Duplicate++
			continue
		case selectedDistributionCount(
			&decision, candidates, recommendation.Source.Distribution,
		) >= int(policy.MaxPerDistribution):
			decision.Distribution++
			continue
		case candidate.MigrationBytes > policy.MigrationBudget-decision.MigrationBytes:
			decision.Budget++
			continue
		}
		decision.Ordinals[decision.Count] = ordinal
		decision.Count++
		decision.MigrationBytes += candidate.MigrationBytes
		if decision.Count == policy.MaxBatch {
			break
		}
	}
	return decision, nil
}

func compareCandidates(left, right SplitCandidate) int {
	a, b := left.Recommendation, right.Recommendation
	if order := cmp.Compare(b.BenefitPPM, a.BenefitPPM); order != 0 {
		return order
	}
	if order := cmp.Compare(b.CurrentPressurePPM, a.CurrentPressurePPM); order != 0 {
		return order
	}
	if order := cmp.Compare(left.MigrationBytes, right.MigrationBytes); order != 0 {
		return order
	}
	if order := cmp.Compare(left.CatalogGeneration, right.CatalogGeneration); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Source.Distribution, b.Source.Distribution); order != 0 {
		return order
	}
	if order := distribution.ComparePoints(a.Source.Range.Start, b.Source.Range.Start); order != 0 {
		return order
	}
	if order := compareRangeEnds(a.Source.Range.End, b.Source.Range.End); order != 0 {
		return order
	}
	if order := cmp.Compare(
		a.Source.AllocationGeneration, b.Source.AllocationGeneration,
	); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Source.Shard, b.Source.Shard); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Source.OwnershipEpoch, b.Source.OwnershipEpoch); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Source.RoutingVersion, b.Source.RoutingVersion); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Source.BucketBits, b.Source.BucketBits); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Kind, b.Kind); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Reason, b.Reason); order != 0 {
		return order
	}
	if order := cmp.Compare(a.BoundaryCount, b.BoundaryCount); order != 0 {
		return order
	}
	for index := range a.Boundaries {
		if order := distribution.ComparePoints(a.Boundaries[index], b.Boundaries[index]); order != 0 {
			return order
		}
	}
	if order := cmp.Compare(a.CandidateBin, b.CandidateBin); order != 0 {
		return order
	}
	if order := distribution.ComparePoints(a.HotBucketStart, b.HotBucketStart); order != 0 {
		return order
	}
	if order := cmp.Compare(a.WindowSequence, b.WindowSequence); order != 0 {
		return order
	}
	if order := cmp.Compare(a.PredictedPressurePPM, b.PredictedPressurePPM); order != 0 {
		return order
	}
	if order := cmp.Compare(a.FanoutTaxPPM, b.FanoutTaxPPM); order != 0 {
		return order
	}
	return cmp.Compare(a.MigrationTaxPPM, b.MigrationTaxPPM)
}

func compareRangeEnds(left, right distribution.KeyspaceEnd) int {
	switch {
	case left.Max && right.Max:
		return 0
	case left.Max:
		return 1
	case right.Max:
		return -1
	default:
		return distribution.ComparePoints(left.Point, right.Point)
	}
}

func exactSource(catalog *gateway.Snapshot, source autosplit.SourceIdentity) bool {
	manifest, ok := catalog.Manifest(source.Distribution)
	if !ok || manifest.Version() != source.RoutingVersion {
		return false
	}
	shard, ok := manifest.ShardMetadataForRange(source.Range)
	return ok && shard.ID == source.Shard &&
		shard.AllocationGeneration == source.AllocationGeneration &&
		shard.Epoch == source.OwnershipEpoch
}

func selectedSource(
	decision *Decision,
	candidates []SplitCandidate,
	source autosplit.SourceIdentity,
) bool {
	for index := 0; index < int(decision.Count); index++ {
		selected := candidates[decision.Ordinals[index]].Recommendation.Source
		if selected == source {
			return true
		}
	}
	return false
}

func selectedDistributionCount(
	decision *Decision,
	candidates []SplitCandidate,
	distributionName distribution.DistributionName,
) int {
	count := 0
	for index := 0; index < int(decision.Count); index++ {
		selected := candidates[decision.Ordinals[index]].Recommendation.Source
		if selected.Distribution == distributionName {
			count++
		}
	}
	return count
}
