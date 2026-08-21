package autosplit

import (
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
)

const MaxSplitChildren = 3

var ErrInvalidSplit = errors.New("autosplit: invalid generation-fenced split")

// Destination is one topology-prepared, non-serving child allocation. Its
// allocation generation must be above the distribution's durable lifetime
// high-water; a retired identity is never reused.
type Destination struct {
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	Leaders              []distribution.EndpointID
	OwnershipEpoch       distribution.OwnershipEpoch
}

// SplitRequest turns one sustained recommendation into exact desired routing
// geometry. RetainChild stays on the source allocation; every other child is
// assigned a Destination in keyspace order. The data plane must populate and
// catch up those destinations before publishing the returned manifest.
type SplitRequest struct {
	Recommendation      Recommendation
	RetainChild         uint8
	NextRoutingVersion  distribution.RoutingVersion
	AllocationHighWater distribution.ShardAllocationGeneration
	Destinations        []Destination
}

// SplitChild is one immutable child description. Retained marks the child that
// continues on the source allocation at a higher ownership epoch.
type SplitChild struct {
	Range                distribution.KeyRange
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	Leaders              []distribution.EndpointID
	OwnershipEpoch       distribution.OwnershipEpoch
	Retained             bool
}

// SplitChildIdentity is the allocation-free scalar view used by control loops.
// Ordered leaders remain in the immutable plan and are accessed separately.
type SplitChildIdentity struct {
	Range                distribution.KeyRange
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	OwnershipEpoch       distribution.OwnershipEpoch
	Retained             bool
}

// SplitPlan is a bounded desired-state artifact. It does not publish topology
// or move data. Manifest becomes publishable only after every non-retained
// child has installed a certified snapshot, caught up, and passed cutover
// validation against this exact Source incarnation.
type SplitPlan struct {
	Source        SourceIdentity
	ChildCount    uint8
	RetainedChild uint8
	children      [MaxSplitChildren]SplitChild
	manifest      *distribution.Manifest
}

// Child returns a detached child record.
func (p *SplitPlan) Child(index int) (SplitChild, bool) {
	if p == nil || index < 0 || index >= int(p.ChildCount) {
		return SplitChild{}, false
	}
	child := p.children[index]
	child.Leaders = slices.Clone(child.Leaders)
	return child, true
}

// ChildIdentity returns one allocation-free scalar child view.
func (p *SplitPlan) ChildIdentity(index int) (SplitChildIdentity, bool) {
	if p == nil || index < 0 || index >= int(p.ChildCount) {
		return SplitChildIdentity{}, false
	}
	child := p.children[index]
	return SplitChildIdentity{
		Range: child.Range, Shard: child.Shard,
		AllocationGeneration: child.AllocationGeneration,
		OwnershipEpoch:       child.OwnershipEpoch, Retained: child.Retained,
	}, true
}

// ChildLeader returns one borrowed immutable endpoint identity without cloning
// the complete leader list.
func (p *SplitPlan) ChildLeader(child, leader int) (distribution.EndpointID, bool) {
	if p == nil || child < 0 || child >= int(p.ChildCount) || leader < 0 ||
		leader >= len(p.children[child].Leaders) {
		return "", false
	}
	return p.children[child].Leaders[leader], true
}

// Manifest returns the immutable desired routing manifest.
func (p *SplitPlan) Manifest() *distribution.Manifest {
	if p == nil {
		return nil
	}
	return p.manifest
}

// PlanSplit validates stale-evidence, bucket geometry, allocation lineage, and
// ownership fencing before constructing a desired manifest. It is deliberately
// separate from cutover: a recommendation never gains publication authority.
func PlanSplit(current *distribution.Manifest, request SplitRequest) (*SplitPlan, error) {
	rec := request.Recommendation
	if current == nil || rec.Source.Distribution != current.Distribution() ||
		rec.Source.RoutingVersion != current.Version() ||
		current.Version() == ^distribution.RoutingVersion(0) ||
		request.NextRoutingVersion != current.Version()+1 || !actionableRecommendation(rec) {
		return nil, ErrInvalidSplit
	}
	sourceOrdinal, source, ok := exactSourceShard(current, rec.Source)
	if !ok || rec.BoundaryCount == 0 || rec.BoundaryCount >= MaxSplitChildren {
		return nil, ErrInvalidSplit
	}
	if !distribution.VirtualBucketBoundary(source.Range.Start, rec.Source.BucketBits) ||
		(!source.Range.End.Max &&
			!distribution.VirtualBucketBoundary(source.Range.End.Point, rec.Source.BucketBits)) {
		return nil, ErrInvalidSplit
	}
	childCount := int(rec.BoundaryCount) + 1
	if int(request.RetainChild) >= childCount || len(request.Destinations) != childCount-1 ||
		source.Epoch == ^distribution.OwnershipEpoch(0) {
		return nil, ErrInvalidSplit
	}

	var ranges [MaxSplitChildren]distribution.KeyRange
	start := source.Range.Start
	for i := 0; i < int(rec.BoundaryCount); i++ {
		boundary := rec.Boundaries[i]
		if !distribution.VirtualBucketBoundary(boundary, rec.Source.BucketBits) ||
			distribution.ComparePoints(start, boundary) >= 0 ||
			(!source.Range.End.Max && distribution.ComparePoints(boundary, source.Range.End.Point) >= 0) {
			return nil, ErrInvalidSplit
		}
		ranges[i] = distribution.KeyRange{
			Start: start, End: distribution.KeyspaceEnd{Point: boundary},
		}
		start = boundary
	}
	ranges[childCount-1] = distribution.KeyRange{Start: start, End: source.Range.End}
	for i := 0; i < childCount; i++ {
		if !ranges[i].Valid() {
			return nil, ErrInvalidSplit
		}
	}

	plan := &SplitPlan{
		Source: rec.Source, ChildCount: uint8(childCount), RetainedChild: request.RetainChild,
	}
	destinationOrdinal := 0
	lastAllocation := request.AllocationHighWater
	for i := 0; i < childCount; i++ {
		child := SplitChild{Range: ranges[i]}
		if i == int(request.RetainChild) {
			child.Shard = source.ID
			child.AllocationGeneration = source.AllocationGeneration
			child.Leaders = source.Leaders
			child.OwnershipEpoch = source.Epoch + 1
			child.Retained = true
		} else {
			destination := request.Destinations[destinationOrdinal]
			destinationOrdinal++
			if destination.Shard == "" || len(destination.Leaders) == 0 ||
				destination.OwnershipEpoch == 0 ||
				destination.AllocationGeneration <= lastAllocation {
				return nil, ErrInvalidSplit
			}
			if destinationConflicts(
				current, request.Destinations[:destinationOrdinal-1], destination,
			) {
				return nil, ErrInvalidSplit
			}
			for _, leader := range destination.Leaders {
				if leader == "" {
					return nil, ErrInvalidSplit
				}
			}
			lastAllocation = destination.AllocationGeneration
			child.Shard = destination.Shard
			child.AllocationGeneration = destination.AllocationGeneration
			child.Leaders = slices.Clone(destination.Leaders)
			child.OwnershipEpoch = destination.OwnershipEpoch
		}
		plan.children[i] = child
	}

	var replacements [MaxSplitChildren]distribution.Shard
	for child := 0; child < childCount; child++ {
		descriptor := &plan.children[child]
		replacements[child] = distribution.Shard{
			ID: descriptor.Shard, AllocationGeneration: descriptor.AllocationGeneration,
			Range: descriptor.Range, Leaders: descriptor.Leaders, Epoch: descriptor.OwnershipEpoch,
		}
	}
	manifest, err := current.ReplaceShard(
		sourceOrdinal, request.NextRoutingVersion, replacements[:childCount],
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSplit, err)
	}
	plan.manifest = manifest
	return plan, nil
}

func destinationConflicts(
	current *distribution.Manifest,
	prior []Destination,
	destination Destination,
) bool {
	for index := 0; index < current.ShardCount(); index++ {
		metadata, _ := current.ShardMetadataAt(index)
		if destination.Shard == metadata.ID ||
			destination.AllocationGeneration == metadata.AllocationGeneration {
			return true
		}
	}
	for index := range prior {
		if destination.Shard == prior[index].Shard ||
			destination.AllocationGeneration == prior[index].AllocationGeneration {
			return true
		}
	}
	return false
}

func exactSourceShard(
	manifest *distribution.Manifest,
	source SourceIdentity,
) (int, distribution.Shard, bool) {
	ordinal, ok := manifest.ShardOrdinalForRange(source.Range)
	if !ok {
		return 0, distribution.Shard{}, false
	}
	shard, ok := manifest.ShardInfo(ordinal)
	return ordinal, shard, ok && shard.ID == source.Shard &&
		shard.AllocationGeneration == source.AllocationGeneration &&
		shard.Epoch == source.OwnershipEpoch
}
