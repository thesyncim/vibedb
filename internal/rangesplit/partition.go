// Package rangesplit provides non-serving, topology-fenced physical split
// primitives. It deliberately separates row partitioning from publication:
// producing child rows never grants serving authority.
package rangesplit

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

var (
	ErrInvalidPartition = errors.New("rangesplit: invalid partition plan")
	ErrSourceFence      = errors.New("rangesplit: source state differs from split plan")
	ErrRowOutsideSource = errors.New("rangesplit: row maps outside source range")
	ErrPartitionBound   = errors.New("rangesplit: partition counter overflow")
)

var splitPlanDigestDomain = []byte("vibedb/range-split/plan\x00")

// RowSink durably or transiently accepts one borrowed source row. key and
// value remain valid only until the callback returns. A nil sink is permitted
// only for the retained child, whose rows already exist on the source.
type RowSink func(key, value []byte) error

// PartitionStats is fixed-size evidence from one complete source scan.
type PartitionStats struct {
	PlanDigest      [sha256.Size]byte
	SourceDigest    [sha256.Size]byte
	SourceBase      [sha256.Size]byte
	SourceEntry     [sha256.Size]byte
	SourceApplied   uint64
	SourceTerm      uint64
	RouteGeneration uint64
	Rows            [autosplit.MaxSplitChildren]uint64
	Bytes           [autosplit.MaxSplitChildren]uint64
}

// PartitionWorkspace owns all mutable scan state, including the reusable
// vibejson structural index and one pre-bound visitor. Reuse it serially and
// do not copy it after first use.
type PartitionWorkspace struct {
	document distribution.DocumentPointWorkspace
	scan     partitionScan
	visit    func(key, value []byte) error
	bound    *partitionScan
}

type partitionScan struct {
	partitioner *Partitioner
	sinks       []RowSink
	document    *distribution.DocumentPointWorkspace
	stats       PartitionStats
}

// Partitioner is an immutable child geometry plus one compiled vibejson
// placement program. It is safe for concurrent use with distinct workspaces.
type Partitioner struct {
	source             autosplit.SourceIdentity
	ranges             [autosplit.MaxSplitChildren]distribution.KeyRange
	children           [autosplit.MaxSplitChildren]autosplit.SplitChild
	childCount         uint8
	retained           uint8
	collection         string
	columns            []string
	program            *distribution.DocumentPointProgram
	target             distribution.RoutingVersion
	targetDistribution distribution.DistributionName
	manifest           []distribution.Shard
	digest             [sha256.Size]byte
	geometryDigest     [sha256.Size]byte
	sourceCoordinates  TailSourceCoordinates
	targetGeneration   uint64
}

// NewPartitioner binds one desired split to the collection's compiled shard
// key. It copies the bounded child ranges and retains no mutable plan state.
func NewPartitioner(
	plan *autosplit.SplitPlan,
	collection string,
	columns []string,
	bucketBits uint8,
) (*Partitioner, error) {
	if plan == nil || plan.Manifest() == nil || plan.ChildCount < 2 ||
		plan.ChildCount > autosplit.MaxSplitChildren ||
		plan.RetainedChild >= plan.ChildCount || collection == "" ||
		!utf8.ValidString(collection) || plan.Source.BucketBits != bucketBits {
		return nil, ErrInvalidPartition
	}
	program, err := distribution.CompileDocumentPointProgram(columns, bucketBits)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPartition, err)
	}
	p := &Partitioner{
		source: plan.Source, childCount: plan.ChildCount,
		retained: plan.RetainedChild, collection: strings.Clone(collection), columns: slices.Clone(columns), program: program,
		target:             plan.Manifest().Version(),
		targetDistribution: plan.Manifest().Distribution(),
	}
	for ordinal := 0; ordinal < plan.Manifest().ShardCount(); ordinal++ {
		shard, _ := plan.Manifest().ShardInfo(ordinal)
		p.manifest = append(p.manifest, shard)
	}
	p.source.Distribution = distribution.DistributionName(strings.Clone(string(p.source.Distribution)))
	p.source.Shard = distribution.ShardID(strings.Clone(string(p.source.Shard)))
	for child := 0; child < int(plan.ChildCount); child++ {
		descriptor, ok := plan.Child(child)
		if !ok {
			return nil, ErrInvalidPartition
		}
		descriptor.Shard = distribution.ShardID(strings.Clone(string(descriptor.Shard)))
		descriptor.Leaders = slices.Clone(descriptor.Leaders)
		for ordinal := range descriptor.Leaders {
			descriptor.Leaders[ordinal] = distribution.EndpointID(
				strings.Clone(string(descriptor.Leaders[ordinal])),
			)
		}
		p.children[child] = descriptor
		p.ranges[child] = descriptor.Range
	}
	p.digest, err = SplitPlanDigest(plan)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Digest returns the exact immutable split-plan identity.
func (p *Partitioner) Digest() [sha256.Size]byte {
	if p == nil {
		return [sha256.Size]byte{}
	}
	return p.digest
}

// CollectionName returns the immutable user collection bound into every
// artifact and tail proof produced by this partitioner.
func (p *Partitioner) CollectionName() string {
	if p == nil {
		return ""
	}
	return p.collection
}

// SourceDistribution returns the immutable distribution whose source range is
// being split.
func (p *Partitioner) SourceDistribution() distribution.DistributionName {
	if p == nil {
		return ""
	}
	return p.source.Distribution
}

// PartitionSnapshot scans the source user collection exactly once. Every row
// is parsed once and dispatched to exactly one child. It copies no row bytes.
func (p *Partitioner) PartitionSnapshot(
	snapshot *replicatedstate.ReadSnapshot,
	sinks []RowSink,
	workspace *PartitionWorkspace,
) (PartitionStats, error) {
	if p == nil || snapshot == nil {
		return PartitionStats{}, ErrInvalidPartition
	}
	user, ok := snapshot.Collection(p.collection)
	if !ok || user == nil {
		return PartitionStats{}, ErrInvalidPartition
	}
	return p.partitionRows(snapshot.State(), user.RangeRaw, sinks, workspace)
}

func (p *Partitioner) partitionRows(
	state replicatedstate.State,
	rangeRows func(func(key, value []byte) error) error,
	sinks []RowSink,
	workspace *PartitionWorkspace,
) (PartitionStats, error) {
	if p == nil || rangeRows == nil || workspace == nil ||
		len(sinks) != int(p.childCount) || !p.matchesSource(state) {
		if p != nil && rangeRows != nil && workspace != nil &&
			len(sinks) == int(p.childCount) {
			return PartitionStats{}, ErrSourceFence
		}
		return PartitionStats{}, ErrInvalidPartition
	}
	for child, sink := range sinks {
		if sink == nil && child != int(p.retained) {
			return PartitionStats{}, ErrInvalidPartition
		}
	}
	workspace.scan.partitioner = p
	workspace.scan.sinks = sinks
	workspace.scan.document = &workspace.document
	workspace.scan.stats = PartitionStats{
		PlanDigest: p.digest, SourceDigest: state.DataChainDigest,
		SourceBase: state.SnapshotBaseDigest, SourceEntry: state.LastEntryDigest,
		SourceApplied: state.Applied,
		SourceTerm:    state.LastTerm, RouteGeneration: state.Binding.RouteGeneration,
	}
	if workspace.visit == nil || workspace.bound != &workspace.scan {
		workspace.visit = workspace.scan.visitRow
		workspace.bound = &workspace.scan
	}
	err := rangeRows(workspace.visit)
	stats := workspace.scan.stats
	workspace.scan.partitioner = nil
	workspace.scan.sinks = nil
	workspace.scan.document = nil
	if err != nil {
		return PartitionStats{}, err
	}
	return stats, nil
}

func (s *partitionScan) visitRow(key, value []byte) error {
	point, err := s.partitioner.program.Point(value, s.document)
	if err != nil {
		return err
	}
	if !s.partitioner.source.Range.Contains(point) {
		return ErrRowOutsideSource
	}
	child := s.partitioner.childFor(point)
	if child < 0 {
		return ErrRowOutsideSource
	}
	rowBytes := uint64(len(key)) + uint64(len(value))
	if s.stats.Rows[child] == math.MaxUint64 ||
		s.stats.Bytes[child] > math.MaxUint64-rowBytes {
		return ErrPartitionBound
	}
	if sink := s.sinks[child]; sink != nil {
		if err := sink(key, value); err != nil {
			return err
		}
	}
	s.stats.Rows[child]++
	s.stats.Bytes[child] += rowBytes
	return nil
}

func (p *Partitioner) matchesSource(state replicatedstate.State) bool {
	binding := state.Binding
	return state.Applied != 0 && state.LastTerm != 0 &&
		state.DataChainDigest != ([sha256.Size]byte{}) &&
		state.LastEntryDigest != ([sha256.Size]byte{}) &&
		state.SnapshotBaseDigest != ([sha256.Size]byte{}) &&
		binding.RouteGeneration != 0 &&
		binding.Distribution == string(p.source.Distribution) &&
		binding.Shard == string(p.source.Shard) &&
		binding.AllocationGeneration == uint64(p.source.AllocationGeneration) &&
		binding.OwnershipEpoch == uint64(p.source.OwnershipEpoch) &&
		binding.RoutingVersion == p.initialCoordinates(binding.RouteGeneration).RoutingVersion &&
		(p.sourceCoordinates == (TailSourceCoordinates{}) || binding.RouteGeneration == p.sourceCoordinates.RouteGeneration) &&
		binding.OwnedRange == p.source.Range
}

func (p *Partitioner) childFor(point distribution.KeyspacePoint) int {
	for child := 0; child < int(p.childCount); child++ {
		if p.ranges[child].Contains(point) {
			return child
		}
	}
	return -1
}

// SplitPlanDigest returns a deterministic identity over the complete source,
// child geometry, allocation lineage, ownership fences, and endpoint set.
func SplitPlanDigest(plan *autosplit.SplitPlan) ([sha256.Size]byte, error) {
	if plan == nil || plan.Manifest() == nil || plan.ChildCount < 2 ||
		plan.ChildCount > autosplit.MaxSplitChildren ||
		plan.RetainedChild >= plan.ChildCount {
		return [sha256.Size]byte{}, ErrInvalidPartition
	}
	h := sha256.New()
	_, _ = h.Write(splitPlanDigestDomain)
	writeSourceIdentity(h, plan.Source)
	var header [10]byte
	header[0], header[1] = plan.ChildCount, plan.RetainedChild
	binary.LittleEndian.PutUint64(header[2:10], uint64(plan.Manifest().Version()))
	_, _ = h.Write(header[:])
	for child := 0; child < int(plan.ChildCount); child++ {
		descriptor, ok := plan.Child(child)
		if !ok {
			return [sha256.Size]byte{}, ErrInvalidPartition
		}
		writeChild(h, descriptor)
	}
	manifest := plan.Manifest()
	writeBytes(h, []byte(manifest.Distribution()))
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(manifest.ShardCount()))
	_, _ = h.Write(count[:])
	for ordinal := 0; ordinal < manifest.ShardCount(); ordinal++ {
		shard, ok := manifest.ShardInfo(ordinal)
		if !ok {
			return [sha256.Size]byte{}, ErrInvalidPartition
		}
		writeManifestShard(h, shard)
	}
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	if digest == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, ErrInvalidPartition
	}
	return digest, nil
}

func writeManifestShard(h hash.Hash, shard distribution.Shard) {
	writeBytes(h, []byte(shard.ID))
	var fixed [33]byte
	copy(fixed[0:8], shard.Range.Start[:])
	copy(fixed[8:16], shard.Range.End.Point[:])
	if shard.Range.End.Max {
		fixed[16] = 1
	}
	binary.LittleEndian.PutUint64(fixed[17:25], uint64(shard.AllocationGeneration))
	binary.LittleEndian.PutUint64(fixed[25:33], uint64(shard.Epoch))
	_, _ = h.Write(fixed[:])
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(shard.Leaders)))
	_, _ = h.Write(count[:])
	for _, leader := range shard.Leaders {
		writeBytes(h, []byte(leader))
	}
}

func writeSourceIdentity(h hash.Hash, source autosplit.SourceIdentity) {
	writeBytes(h, []byte(source.Distribution))
	writeBytes(h, []byte(source.Shard))
	var fixed [42]byte
	binary.LittleEndian.PutUint64(fixed[0:8], uint64(source.AllocationGeneration))
	copy(fixed[8:16], source.Range.Start[:])
	copy(fixed[16:24], source.Range.End.Point[:])
	if source.Range.End.Max {
		fixed[24] = 1
	}
	fixed[25] = source.BucketBits
	binary.LittleEndian.PutUint64(fixed[26:34], uint64(source.RoutingVersion))
	binary.LittleEndian.PutUint64(fixed[34:42], uint64(source.OwnershipEpoch))
	_, _ = h.Write(fixed[:])
}

func writeChild(h hash.Hash, child autosplit.SplitChild) {
	writeBytes(h, []byte(child.Shard))
	var fixed [34]byte
	copy(fixed[0:8], child.Range.Start[:])
	copy(fixed[8:16], child.Range.End.Point[:])
	if child.Range.End.Max {
		fixed[16] = 1
	}
	if child.Retained {
		fixed[17] = 1
	}
	binary.LittleEndian.PutUint64(fixed[18:26], uint64(child.AllocationGeneration))
	binary.LittleEndian.PutUint64(fixed[26:34], uint64(child.OwnershipEpoch))
	_, _ = h.Write(fixed[:])
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(child.Leaders)))
	_, _ = h.Write(count[:])
	for _, leader := range child.Leaders {
		writeBytes(h, []byte(leader))
	}
}

func writeBytes(h hash.Hash, value []byte) {
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(value)
}
