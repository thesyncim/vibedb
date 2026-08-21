package topologyscheduler

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

var ErrInvalidPlacement = errors.New("topologyscheduler: invalid split placement cut")

// SplitPlacement supplies topology-prepared identities for one selected
// recommendation. Destinations are in child key-range order with the retained
// child omitted. Their endpoints must already exist in the pinned catalog.
type SplitPlacement struct {
	RetainChild      uint8
	DestinationCount uint8
	Destinations     [autosplit.MaxSplitChildren - 1]autosplit.Destination
}

// SplitPlanBatch is an immutable, bounded handoff to split preparation. It
// grants no data-plane or catalog authority.
type SplitPlanBatch struct {
	catalogGeneration uint64
	count             uint8
	ordinals          [MaxBatch]uint16
	plans             [MaxBatch]*autosplit.SplitPlan
}

// CatalogGeneration reports the immutable catalog cut used by every plan.
func (b *SplitPlanBatch) CatalogGeneration() uint64 {
	if b == nil {
		return 0
	}
	return b.catalogGeneration
}

// Count reports the number of immutable plans in the batch.
func (b *SplitPlanBatch) Count() int {
	if b == nil {
		return 0
	}
	return int(b.count)
}

// PlanAt returns one candidate ordinal and immutable split plan in admission
// priority order.
func (b *SplitPlanBatch) PlanAt(index int) (uint16, *autosplit.SplitPlan, bool) {
	if b == nil || index < 0 || index >= int(b.count) {
		return 0, nil, false
	}
	return b.ordinals[index], b.plans[index], true
}

// BuildSplitPlanBatch binds one admission decision to independently prepared
// destination identities. It rechecks every generation and source fence,
// enforces the catalog's durable allocation high-water, rejects cross-plan
// destination collisions, and only then constructs immutable split plans.
func BuildSplitPlanBatch(
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
	decision Decision,
	placements []SplitPlacement,
) (*SplitPlanBatch, error) {
	if catalog == nil || decision.CatalogGeneration != catalog.Generation() ||
		decision.Count == 0 || decision.Count > MaxBatch ||
		len(candidates) > MaxCandidates || len(placements) != int(decision.Count) {
		return nil, ErrInvalidPlacement
	}

	var migrationBytes uint64
	for index := 0; index < int(decision.Count); index++ {
		ordinal := int(decision.Ordinals[index])
		if ordinal >= len(candidates) {
			return nil, ErrInvalidPlacement
		}
		candidate := &candidates[ordinal]
		recommendation := candidate.Recommendation
		if candidate.CatalogGeneration != catalog.Generation() ||
			!recommendation.Actionable() || !exactSource(catalog, recommendation.Source) ||
			selectedOrdinalBefore(decision, index, uint16(ordinal)) ||
			selectedSourceBefore(candidates, decision, index, recommendation.Source) ||
			candidate.MigrationBytes > ^uint64(0)-migrationBytes {
			return nil, ErrInvalidPlacement
		}
		migrationBytes += candidate.MigrationBytes

		placement := &placements[index]
		if placement.DestinationCount != recommendation.BoundaryCount ||
			placement.DestinationCount == 0 ||
			placement.DestinationCount > autosplit.MaxSplitChildren-1 ||
			placement.RetainChild > placement.DestinationCount ||
			placementResourcesConflict(candidates, decision, placements, index) {
			return nil, ErrInvalidPlacement
		}
		manifest, _ := catalog.Manifest(recommendation.Source.Distribution)
		nextAllocation, ok := catalog.NextShardAllocationGeneration(
			recommendation.Source.Distribution,
		)
		if !ok {
			return nil, ErrInvalidPlacement
		}
		if !validPlacement(catalog, manifest, placement, nextAllocation-1) {
			return nil, ErrInvalidPlacement
		}
	}
	if migrationBytes != decision.MigrationBytes {
		return nil, ErrInvalidPlacement
	}

	batch := &SplitPlanBatch{
		catalogGeneration: catalog.Generation(), count: decision.Count,
	}
	for index := 0; index < int(decision.Count); index++ {
		ordinal := decision.Ordinals[index]
		candidate := &candidates[ordinal]
		manifest, _ := catalog.Manifest(candidate.Recommendation.Source.Distribution)
		nextAllocation, _ := catalog.NextShardAllocationGeneration(
			candidate.Recommendation.Source.Distribution,
		)
		placement := &placements[index]
		plan, err := autosplit.PlanSplit(manifest, autosplit.SplitRequest{
			Recommendation:      candidate.Recommendation,
			RetainChild:         placement.RetainChild,
			NextRoutingVersion:  manifest.Version() + 1,
			AllocationHighWater: nextAllocation - 1,
			Destinations:        placement.Destinations[:placement.DestinationCount],
		})
		if err != nil {
			return nil, fmt.Errorf(
				"%w: candidate %d: %w", ErrInvalidPlacement, ordinal, err,
			)
		}
		batch.ordinals[index], batch.plans[index] = ordinal, plan
	}
	return batch, nil
}

func validPlacement(
	catalog *gateway.Snapshot,
	manifest *distribution.Manifest,
	placement *SplitPlacement,
	highWater distribution.ShardAllocationGeneration,
) bool {
	lastAllocation := highWater
	for index := 0; index < int(placement.DestinationCount); index++ {
		destination := &placement.Destinations[index]
		if destination.Shard == "" || destination.OwnershipEpoch == 0 ||
			len(destination.Leaders) == 0 ||
			destination.AllocationGeneration <= lastAllocation ||
			activeShardID(manifest, destination.Shard) {
			return false
		}
		lastAllocation = destination.AllocationGeneration
		for prior := 0; prior < index; prior++ {
			if destination.Shard == placement.Destinations[prior].Shard ||
				destination.AllocationGeneration ==
					placement.Destinations[prior].AllocationGeneration {
				return false
			}
		}
		for _, endpoint := range destination.Leaders {
			if _, err := catalog.Address(endpoint); err != nil {
				return false
			}
		}
	}
	return true
}

func activeShardID(manifest *distribution.Manifest, shardID distribution.ShardID) bool {
	for index := 0; index < manifest.ShardCount(); index++ {
		metadata, _ := manifest.ShardMetadataAt(index)
		if metadata.ID == shardID {
			return true
		}
	}
	return false
}

func selectedOrdinalBefore(decision Decision, limit int, ordinal uint16) bool {
	for index := 0; index < limit; index++ {
		if decision.Ordinals[index] == ordinal {
			return true
		}
	}
	return false
}

func selectedSourceBefore(
	candidates []SplitCandidate,
	decision Decision,
	limit int,
	source autosplit.SourceIdentity,
) bool {
	for index := 0; index < limit; index++ {
		if candidates[decision.Ordinals[index]].Recommendation.Source == source {
			return true
		}
	}
	return false
}

func placementResourcesConflict(
	candidates []SplitCandidate,
	decision Decision,
	placements []SplitPlacement,
	index int,
) bool {
	distributionName := candidates[decision.Ordinals[index]].Recommendation.Source.Distribution
	placement := &placements[index]
	for prior := 0; prior < index; prior++ {
		if candidates[decision.Ordinals[prior]].Recommendation.Source.Distribution != distributionName {
			continue
		}
		priorPlacement := &placements[prior]
		for destinationIndex := 0; destinationIndex < int(placement.DestinationCount); destinationIndex++ {
			destination := &placement.Destinations[destinationIndex]
			for priorIndex := 0; priorIndex < int(priorPlacement.DestinationCount); priorIndex++ {
				priorDestination := &priorPlacement.Destinations[priorIndex]
				if destination.Shard == priorDestination.Shard ||
					destination.AllocationGeneration == priorDestination.AllocationGeneration {
					return true
				}
			}
		}
	}
	return false
}
