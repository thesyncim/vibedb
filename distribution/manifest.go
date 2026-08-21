package distribution

import (
	"slices"
	"strings"
)

// Shard is one physical shard: a unique id, the half-open keyspace range it
// owns, at least one leader endpoint, and the ownership epoch that fences its
// writers. It is the input record to NewManifest; a validated Manifest holds
// defensive copies. Epoch is optional metadata carried onto routed targets; it
// stays zero until static ownership is configured, and never participates in
// keyspace validation.
type Shard struct {
	ID                   ShardID
	AllocationGeneration ShardAllocationGeneration
	Range                KeyRange
	Leaders              []EndpointID
	Epoch                OwnershipEpoch
}

// Manifest is an immutable, validated shard layout for one routing generation
// of a distribution. It has no exported mutators; every field is copied at
// construction, so a published Manifest is safe to share across goroutines.
type Manifest struct {
	distribution DistributionName
	version      RoutingVersion
	shards       []Shard
	starts       []KeyspacePoint // shards[i].Range.Start, retained for binary search
}

// ShardMetadata is the immutable scalar identity of one manifest shard. It
// intentionally excludes the leaders slice so callers can inspect transition
// metadata without receiving mutable backing storage or allocating a copy.
type ShardMetadata struct {
	ID                   ShardID
	AllocationGeneration ShardAllocationGeneration
	Range                KeyRange
	Epoch                OwnershipEpoch
	LeaderCount          int
}

// NewManifest validates shards and returns an immutable Manifest, or a
// *ManifestError (matching ErrInvalidManifest) on any violation. Input slices
// are defensively copied. Validation requires: at least one shard; unique,
// non-empty shard ids; at least one non-empty leader per shard; every range
// valid (Start < End); ranges sorted by start with no gaps or overlaps; the
// first range starting at zero; only the final range ending at Max and the
// final range ending exactly at Max — together, complete coverage of the
// 8-byte keyspace; and a unique, nonzero topology allocation generation for
// every physical shard.
func NewManifest(distribution DistributionName, version RoutingVersion, shards []Shard) (*Manifest, error) {
	if len(shards) == 0 {
		return nil, &ManifestError{Reason: "manifest defines no shards"}
	}

	seen := make(map[ShardID]struct{}, len(shards))
	for i := range shards {
		s := shards[i]
		if s.ID == "" {
			return nil, &ManifestError{Reason: "empty shard id"}
		}
		if _, dup := seen[s.ID]; dup {
			return nil, &ManifestError{Reason: "duplicate shard id " + string(s.ID)}
		}
		seen[s.ID] = struct{}{}
		if len(s.Leaders) == 0 {
			return nil, &ManifestError{Reason: "shard " + string(s.ID) + " has no leader endpoint"}
		}
		for _, ep := range s.Leaders {
			if ep == "" {
				return nil, &ManifestError{Reason: "shard " + string(s.ID) + " has an empty leader endpoint"}
			}
		}
		if !s.Range.Valid() {
			return nil, &ManifestError{Reason: "shard " + string(s.ID) + " range start is not below its end"}
		}
	}

	if shards[0].Range.Start != (KeyspacePoint{}) {
		return nil, &ManifestError{Reason: "first range does not start at zero"}
	}
	last := len(shards) - 1
	if !shards[last].Range.End.Max {
		return nil, &ManifestError{Reason: "final range does not end at Max"}
	}
	for i := 0; i < last; i++ {
		if shards[i].Range.End.Max {
			return nil, &ManifestError{Reason: "non-final range ends at Max"}
		}
		if ComparePoints(shards[i].Range.Start, shards[i+1].Range.Start) >= 0 {
			return nil, &ManifestError{Reason: "ranges are not sorted by start"}
		}
		switch ComparePoints(shards[i].Range.End.Point, shards[i+1].Range.Start) {
		case -1:
			return nil, &ManifestError{Reason: "gap between shard ranges"}
		case 1:
			return nil, &ManifestError{Reason: "overlapping shard ranges"}
		}
	}
	generations := make(map[ShardAllocationGeneration]struct{}, len(shards))
	for i := range shards {
		generation := shards[i].AllocationGeneration
		if generation == 0 {
			return nil, &ManifestError{Reason: "shard " + string(shards[i].ID) + " has a zero allocation generation"}
		}
		if _, duplicate := generations[generation]; duplicate {
			return nil, &ManifestError{Reason: "duplicate shard allocation generation"}
		}
		generations[generation] = struct{}{}
	}

	cp := make([]Shard, len(shards))
	starts := make([]KeyspacePoint, len(shards))
	for i := range shards {
		cp[i] = shards[i]
		cp[i].ID = ShardID(strings.Clone(string(shards[i].ID)))
		cp[i].Leaders = make([]EndpointID, len(shards[i].Leaders))
		for j := range shards[i].Leaders {
			cp[i].Leaders[j] = EndpointID(strings.Clone(string(shards[i].Leaders[j])))
		}
		starts[i] = shards[i].Range.Start
	}
	return &Manifest{
		distribution: DistributionName(strings.Clone(string(distribution))),
		version:      version, shards: cp, starts: starts,
	}, nil
}

// Distribution reports the distribution this manifest routes.
func (m *Manifest) Distribution() DistributionName { return m.distribution }

// Version reports the routing generation this manifest represents.
func (m *Manifest) Version() RoutingVersion { return m.version }

// ShardCount reports the number of shards.
func (m *Manifest) ShardCount() int { return len(m.shards) }

// ShardInfo returns a defensive copy of the shard at index i and reports
// whether i is in range. The returned Shard's Leaders slice is independent of
// the manifest, preserving immutability.
func (m *Manifest) ShardInfo(i int) (Shard, bool) {
	if i < 0 || i >= len(m.shards) {
		return Shard{}, false
	}
	s := m.shards[i]
	s.Leaders = slices.Clone(m.shards[i].Leaders)
	return s, true
}

// ShardMetadataAt returns allocation-free scalar metadata for shard i.
func (m *Manifest) ShardMetadataAt(i int) (ShardMetadata, bool) {
	if i < 0 || i >= len(m.shards) {
		return ShardMetadata{}, false
	}
	shard := &m.shards[i]
	return ShardMetadata{
		ID: shard.ID, AllocationGeneration: shard.AllocationGeneration,
		Range: shard.Range, Epoch: shard.Epoch,
		LeaderCount: len(shard.Leaders),
	}, true
}

// ShardMetadataForRange returns allocation-free scalar metadata when r is the
// exact range of one active shard. The lookup is O(log shard_count) over the
// manifest's immutable range-start index; overlapping or stale range geometry
// does not match.
func (m *Manifest) ShardMetadataForRange(r KeyRange) (ShardMetadata, bool) {
	if m == nil || !r.Valid() {
		return ShardMetadata{}, false
	}
	index := m.searchStart(r.Start)
	if index < 0 || m.shards[index].Range != r {
		return ShardMetadata{}, false
	}
	return m.ShardMetadataAt(index)
}

// SameShardLeaders reports whether shard i and other's shard j have the exact
// same ordered leader identity without cloning either immutable slice.
func (m *Manifest) SameShardLeaders(i int, other *Manifest, j int) bool {
	if m == nil || other == nil || i < 0 || i >= len(m.shards) ||
		j < 0 || j >= len(other.shards) {
		return false
	}
	return slices.Equal(m.shards[i].Leaders, other.shards[j].Leaders)
}

// ShardLeadersEqual compares one immutable manifest leader set with leaders
// without cloning either slice.
func (m *Manifest) ShardLeadersEqual(i int, leaders []EndpointID) bool {
	return m != nil && i >= 0 && i < len(m.shards) &&
		slices.Equal(m.shards[i].Leaders, leaders)
}

// Equal reports exact semantic manifest equality without allocating defensive
// shard copies. Distribution, routing version, range geometry, shard ids,
// ordered leaders, and ownership epochs all participate.
func (m *Manifest) Equal(other *Manifest) bool {
	if m == nil || other == nil || m.distribution != other.distribution ||
		m.version != other.version || len(m.shards) != len(other.shards) {
		return false
	}
	for i := range m.shards {
		left, right := &m.shards[i], &other.shards[i]
		if left.ID != right.ID || left.AllocationGeneration != right.AllocationGeneration ||
			left.Range != right.Range ||
			left.Epoch != right.Epoch || !slices.Equal(left.Leaders, right.Leaders) {
			return false
		}
	}
	return true
}

// searchStart returns the highest index whose range start is <= p, or -1 when
// p precedes every start. It is a closure-free binary search, O(log n) and
// allocation-free.
func (m *Manifest) searchStart(p KeyspacePoint) int {
	lo, hi := 0, len(m.starts)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if ComparePoints(m.starts[mid], p) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

// ResolvePoint returns the id of the shard owning p, in O(log shard_count) via
// binary search over sorted range starts. ok is false only for a degenerate
// manifest that does not cover p, which a validated manifest never produces.
func (m *Manifest) ResolvePoint(p KeyspacePoint) (ShardID, bool) {
	i := m.searchStart(p)
	if i < 0 || !m.shards[i].Range.Contains(p) {
		return "", false
	}
	return m.shards[i].ID, true
}

// ResolvePointTarget resolves p to the owning shard's fenced leader target
// without cloning the manifest's leader slice. It is the zero-allocation point
// routing primitive for callers that need the complete dispatch identity rather
// than only the shard id.
func (m *Manifest) ResolvePointTarget(p KeyspacePoint) (Target, bool) {
	i := m.searchStart(p)
	if i < 0 || !m.shards[i].Range.Contains(p) {
		return Target{}, false
	}
	shard := &m.shards[i]
	return Target{
		Shard:                shard.ID,
		AllocationGeneration: shard.AllocationGeneration,
		Endpoint:             shard.Leaders[0],
		OwnershipEpoch:       shard.Epoch,
		Role:                 RoleLeader,
	}, true
}

// ResolveRange returns the ids of every shard overlapping r, in keyspace
// order, in O(log shard_count + overlapping_shards): one binary search to the
// first candidate followed by a forward walk that stops at the first shard
// starting at or after r.End. ok is false when r is malformed.
func (m *Manifest) ResolveRange(r KeyRange) ([]ShardID, bool) {
	if !r.Valid() {
		return nil, false
	}
	i := m.searchStart(r.Start)
	if i < 0 {
		i = 0
	}
	var out []ShardID
	for ; i < len(m.shards); i++ {
		s := m.shards[i]
		if !pointBelowEnd(s.Range.Start, r.End) {
			break // shard starts at or after r.End: no further overlap
		}
		if pointBelowEnd(r.Start, s.Range.End) {
			out = append(out, s.ID)
		}
	}
	return out, true
}

// ResolveDestinationSet unions point and range resolution into a deduplicated
// set of shard ids ordered by keyspace position. It reports a
// *DestinationError (matching ErrInvalidDestination) for a malformed range.
// Work is bounded by the destinations and the shards they touch, never a scan
// of every shard.
func (m *Manifest) ResolveDestinationSet(d DestinationSet) ([]ShardID, error) {
	var idx []int
	for _, p := range d.Points {
		i := m.searchStart(p)
		if i < 0 || !m.shards[i].Range.Contains(p) {
			continue
		}
		idx = append(idx, i)
	}
	for _, r := range d.Ranges {
		if !r.Valid() {
			return nil, &DestinationError{Reason: "range start is not below its end"}
		}
		i := m.searchStart(r.Start)
		if i < 0 {
			i = 0
		}
		for ; i < len(m.shards); i++ {
			if !pointBelowEnd(m.shards[i].Range.Start, r.End) {
				break
			}
			if pointBelowEnd(r.Start, m.shards[i].Range.End) {
				idx = append(idx, i)
			}
		}
	}
	if len(idx) == 0 {
		return nil, nil
	}
	slices.Sort(idx)
	out := make([]ShardID, 0, len(idx))
	prev := -1
	for _, i := range idx {
		if i == prev {
			continue
		}
		out = append(out, m.shards[i].ID)
		prev = i
	}
	return out, nil
}
