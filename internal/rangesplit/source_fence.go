package rangesplit

import (
	"crypto/sha256"
	"encoding/binary"
	"math"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// GeometryDigest is the preallocation operation namespace. The serving recipe
// Digest additionally binds an exact applied source fence when the catalog
// generation is ahead of that shard's last topology transition.
func (p *Partitioner) GeometryDigest() [32]byte {
	if p == nil {
		return [32]byte{}
	}
	if p.geometryDigest != ([32]byte{}) {
		return p.geometryDigest
	}
	return p.digest
}

// BindSourceFence returns an immutable recipe with an exact source publication
// and target catalog generation. It never infers target authority from a
// merely increasing number. The controller validates the catalog CAS first;
// the resulting portable spec is then committed as capture activation.
func (p *Partitioner) BindSourceFence(before TailSourceCoordinates, targetGeneration uint64) (*Partitioner, error) {
	if p == nil || before.OwnershipEpoch != uint64(p.source.OwnershipEpoch) ||
		before.RoutingVersion == 0 || before.RoutingVersion > uint64(p.source.RoutingVersion) ||
		before.RouteGeneration == 0 || targetGeneration <= before.RouteGeneration ||
		before.OwnershipEpoch == math.MaxUint64 || uint64(p.target) <= before.RoutingVersion {
		return nil, ErrSourceFence
	}
	if p.sourceCoordinates != (TailSourceCoordinates{}) {
		if p.sourceCoordinates != before || p.targetGeneration != targetGeneration {
			return nil, ErrSourceFence
		}
		return p, nil
	}
	bound := *p
	bound.geometryDigest = p.GeometryDigest()
	bound.sourceCoordinates, bound.targetGeneration = before, targetGeneration
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/range-split/applied-source-fence\x00"))
	_, _ = h.Write(bound.geometryDigest[:])
	var fixed [32]byte
	binary.LittleEndian.PutUint64(fixed[0:8], before.OwnershipEpoch)
	binary.LittleEndian.PutUint64(fixed[8:16], before.RoutingVersion)
	binary.LittleEndian.PutUint64(fixed[16:24], before.RouteGeneration)
	binary.LittleEndian.PutUint64(fixed[24:32], targetGeneration)
	_, _ = h.Write(fixed[:])
	_ = h.Sum(bound.digest[:0])
	bound.bindRelationDigest()
	return &bound, nil
}

func (p *Partitioner) initialCoordinates(routeGeneration uint64) TailSourceCoordinates {
	if p.sourceCoordinates != (TailSourceCoordinates{}) {
		return p.sourceCoordinates
	}
	return TailSourceCoordinates{OwnershipEpoch: uint64(p.source.OwnershipEpoch), RoutingVersion: uint64(p.source.RoutingVersion), RouteGeneration: routeGeneration}
}

func (p *Partitioner) sealCoordinates(before TailSourceCoordinates) TailSourceCoordinates {
	if before != p.initialCoordinates(before.RouteGeneration) || before.OwnershipEpoch == math.MaxUint64 || before.RouteGeneration == math.MaxUint64 {
		return TailSourceCoordinates{}
	}
	generation := p.targetGeneration
	if generation == 0 {
		generation = before.RouteGeneration + 1
	}
	return TailSourceCoordinates{OwnershipEpoch: uint64(p.children[p.retained].OwnershipEpoch), RoutingVersion: uint64(p.target), RouteGeneration: generation}
}

// AuthorizesOwnershipTransition is consumed only with the matching durable
// split-capture activation witness. Bare process-local captures cannot grant
// a jump in the replicated ownership state machine.
func (c *SourceCapture) AuthorizesOwnershipTransition(v replicatedstate.OwnershipTransitionView) bool {
	if c == nil || c.partitioner == nil || c.partitioner.sourceCoordinates == (TailSourceCoordinates{}) {
		return false
	}
	p := c.partitioner
	before := TailSourceCoordinates{OwnershipEpoch: v.OwnershipEpoch, RoutingVersion: v.RoutingVersion, RouteGeneration: v.RouteGeneration}
	after := TailSourceCoordinates{OwnershipEpoch: v.ToOwnershipEpoch, RoutingVersion: v.ToRoutingVersion, RouteGeneration: v.ToRouteGeneration}
	return before == p.sourceCoordinates && after == p.sealCoordinates(before) &&
		v.FromOwnedRange == p.source.Range && v.ToOwnedRange == p.children[p.retained].Range
}
