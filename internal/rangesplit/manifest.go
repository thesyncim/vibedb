package rangesplit

import (
	"errors"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
)

var ErrManifestTransition = errors.New("rangesplit: manifest does not exactly apply split plan")

// ValidatePublicationTransition binds every data-plane proof to the exact
// control-plane replacement it authorizes. It grants no authority itself; the
// catalog must still compare-and-swap its durable and in-memory generations.
func (p *Partitioner) ValidatePublicationTransition(
	current *distribution.Manifest,
	next *distribution.Manifest,
	certificate CutoverCertificate,
	prune RetainedPruneCursor,
) error {
	if p == nil || p.VerifyCutoverCertificate(certificate) != nil {
		return ErrCutoverCertificate
	}
	if p.VerifyRetainedPruneCompletion(certificate, prune) != nil {
		return ErrRetainedPrune
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
