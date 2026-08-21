package rangesplit

import (
	"cmp"
	"errors"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
)

var ErrManifestTransition = errors.New("rangesplit: manifest does not exactly apply split plan")

// MaxComposedManifestSplits bounds one catalog publication batch. Data-plane
// preparation remains independent; composition is a cold control-plane step.
const MaxComposedManifestSplits = 64

// ManifestTransition is one individually validated split result prepared from
// the same source manifest. Target changes exactly Partitioner.source.
type ManifestTransition struct {
	Partitioner *Partitioner
	Target      *distribution.Manifest
}

// ComposeManifestTransitions combines disjoint split results prepared from one
// source manifest into one successor routing generation. It validates every
// individual one-source transition before composition and rejects two plans
// for the same source allocation. No proof is weakened: publication must still
// validate every split certificate and retained-prune proof separately.
func ComposeManifestTransitions(
	current *distribution.Manifest,
	transitions []ManifestTransition,
) (*distribution.Manifest, error) {
	if current == nil || current.Version() == ^distribution.RoutingVersion(0) ||
		len(transitions) == 0 || len(transitions) > MaxComposedManifestSplits {
		return nil, ErrManifestTransition
	}
	var order [MaxComposedManifestSplits]uint8
	var sources [MaxComposedManifestSplits]int
	targetCount := current.ShardCount()
	for index := range transitions {
		transition := transitions[index]
		if transition.Partitioner == nil || transition.Target == nil ||
			transition.Partitioner.ValidateManifestTransition(current, transition.Target) != nil {
			return nil, ErrManifestTransition
		}
		source, ok := transition.Partitioner.sourceOrdinal(current)
		if !ok {
			return nil, ErrManifestTransition
		}
		order[index] = uint8(index)
		sources[index] = source
		extra := int(transition.Partitioner.childCount) - 1
		if targetCount > math.MaxInt-extra {
			return nil, ErrManifestTransition
		}
		targetCount += extra
	}
	slices.SortFunc(order[:len(transitions)], func(left, right uint8) int {
		return cmp.Compare(sources[left], sources[right])
	})
	for index := 1; index < len(transitions); index++ {
		if sources[order[index-1]] == sources[order[index]] {
			return nil, ErrManifestTransition
		}
	}

	shards := make([]distribution.Shard, 0, targetCount)
	replacement := 0
	for currentOrdinal := 0; currentOrdinal < current.ShardCount(); currentOrdinal++ {
		if replacement < len(transitions) &&
			sources[order[replacement]] == currentOrdinal {
			partitioner := transitions[order[replacement]].Partitioner
			for child := 0; child < int(partitioner.childCount); child++ {
				descriptor := partitioner.children[child]
				shards = append(shards, distribution.Shard{
					ID: descriptor.Shard, AllocationGeneration: descriptor.AllocationGeneration,
					Range: descriptor.Range, Leaders: descriptor.Leaders,
					Epoch: descriptor.OwnershipEpoch,
				})
			}
			replacement++
			continue
		}
		shard, ok := current.ShardInfo(currentOrdinal)
		if !ok {
			return nil, ErrManifestTransition
		}
		shards = append(shards, shard)
	}
	if replacement != len(transitions) {
		return nil, ErrManifestTransition
	}
	manifest, err := distribution.NewManifest(
		current.Distribution(), current.Version()+1, shards,
	)
	if err != nil {
		return nil, errors.Join(ErrManifestTransition, err)
	}
	return manifest, nil
}

// ValidatePublicationTransition binds every data-plane proof to the exact
// control-plane replacement it authorizes. It grants no authority itself; the
// catalog must still compare-and-swap its durable and in-memory generations.
func (p *Partitioner) ValidatePublicationTransition(
	current *distribution.Manifest,
	next *distribution.Manifest,
	currentGeneration uint64,
	nextGeneration uint64,
	certificate CutoverCertificate,
	prune RetainedPruneCursor,
) error {
	if p == nil || p.VerifyCutoverCertificate(certificate) != nil {
		return ErrCutoverCertificate
	}
	if p.VerifyRetainedPruneCompletion(certificate, prune) != nil {
		return ErrRetainedPrune
	}
	if currentGeneration == math.MaxUint64 || nextGeneration != currentGeneration+1 ||
		certificate.SourceCoordinates().RouteGeneration != nextGeneration {
		return ErrManifestTransition
	}
	return p.ValidateManifestTransition(current, next)
}

// ValidateManifestTransition proves that next changes exactly one source shard
// in current into this partitioner's ordered children. Every unrelated shard,
// leader list, allocation generation, range, and ownership epoch must remain
// byte-for-byte identical.
func (p *Partitioner) ValidateManifestTransition(
	current *distribution.Manifest,
	next *distribution.Manifest,
) error {
	if p == nil || current == nil || next == nil ||
		current.Distribution() != p.source.Distribution ||
		next.Distribution() != p.source.Distribution ||
		current.Version() != p.source.RoutingVersion || next.Version() != p.target ||
		next.ShardCount() != current.ShardCount()+int(p.childCount)-1 {
		return ErrManifestTransition
	}
	currentOrdinal, nextOrdinal := 0, 0
	sourceSeen := false
	for currentOrdinal < current.ShardCount() {
		before, ok := current.ShardMetadataAt(currentOrdinal)
		if !ok {
			return ErrManifestTransition
		}
		if sourceShardMatches(
			current, currentOrdinal, before, p.source, p.children[p.retained].Leaders,
		) {
			if sourceSeen {
				return ErrManifestTransition
			}
			sourceSeen = true
			for child := 0; child < int(p.childCount); child++ {
				after, ok := next.ShardMetadataAt(nextOrdinal)
				if !ok || !splitChildMatches(
					next, nextOrdinal, after, p.children[child],
				) {
					return ErrManifestTransition
				}
				nextOrdinal++
			}
		} else {
			after, ok := next.ShardMetadataAt(nextOrdinal)
			if !ok || !equalManifestShard(before, after) ||
				!current.SameShardLeaders(currentOrdinal, next, nextOrdinal) {
				return ErrManifestTransition
			}
			nextOrdinal++
		}
		currentOrdinal++
	}
	if !sourceSeen || nextOrdinal != next.ShardCount() {
		return ErrManifestTransition
	}
	return nil
}

// ValidatePublishedManifestTransition recognizes this exact child replacement
// inside an already-authoritative successor that may also contain other
// disjoint splits. It is for restart/completion recognition only; it does not
// authorize publication and therefore intentionally does not validate changes
// outside this source range.
func (p *Partitioner) ValidatePublishedManifestTransition(
	current *distribution.Manifest,
	published *distribution.Manifest,
) error {
	if p == nil || current == nil || published == nil ||
		current.Distribution() != p.source.Distribution ||
		published.Distribution() != p.source.Distribution ||
		current.Version() != p.source.RoutingVersion || published.Version() != p.target {
		return ErrManifestTransition
	}
	if _, ok := p.sourceOrdinal(current); !ok {
		return ErrManifestTransition
	}
	for ordinal := 0; ordinal < published.ShardCount(); ordinal++ {
		first, ok := published.ShardMetadataAt(ordinal)
		if !ok || first.Range.Start != p.source.Range.Start {
			continue
		}
		if ordinal+int(p.childCount) > published.ShardCount() {
			return ErrManifestTransition
		}
		for child := 0; child < int(p.childCount); child++ {
			shard, ok := published.ShardMetadataAt(ordinal + child)
			if !ok || !splitChildMatches(
				published, ordinal+child, shard, p.children[child],
			) {
				return ErrManifestTransition
			}
		}
		return nil
	}
	return ErrManifestTransition
}

func (p *Partitioner) sourceOrdinal(manifest *distribution.Manifest) (int, bool) {
	if p == nil || manifest == nil {
		return 0, false
	}
	for ordinal := 0; ordinal < manifest.ShardCount(); ordinal++ {
		shard, ok := manifest.ShardMetadataAt(ordinal)
		if ok && sourceShardMatches(
			manifest, ordinal, shard, p.source, p.children[p.retained].Leaders,
		) {
			return ordinal, true
		}
	}
	return 0, false
}

func sourceShardMatches(
	manifest *distribution.Manifest,
	ordinal int,
	shard distribution.ShardMetadata,
	source autosplit.SourceIdentity,
	leaders []distribution.EndpointID,
) bool {
	return shard.ID == source.Shard &&
		shard.AllocationGeneration == source.AllocationGeneration &&
		shard.Range == source.Range && shard.Epoch == source.OwnershipEpoch &&
		manifest.ShardLeadersEqual(ordinal, leaders)
}

func splitChildMatches(
	manifest *distribution.Manifest,
	ordinal int,
	shard distribution.ShardMetadata,
	child autosplit.SplitChild,
) bool {
	return shard.ID == child.Shard &&
		shard.AllocationGeneration == child.AllocationGeneration &&
		shard.Range == child.Range && shard.Epoch == child.OwnershipEpoch &&
		manifest.ShardLeadersEqual(ordinal, child.Leaders)
}

func equalManifestShard(left, right distribution.ShardMetadata) bool {
	return left.ID == right.ID &&
		left.AllocationGeneration == right.AllocationGeneration &&
		left.Range == right.Range && left.Epoch == right.Epoch
}
