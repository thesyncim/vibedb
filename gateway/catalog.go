package gateway

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/storeio"
	queryplanner "github.com/thesyncim/vibedb/planner"
	vibejson "github.com/thesyncim/vibejson"
)

// The minimal authoritative catalog: one immutable snapshot generation of the
// routing configuration plus endpoint membership, a lock-free holder mirroring
// distribution.ManifestHolder, and durable JSON persistence using the same
// temp-file/sync/rename/directory-fsync recipe the SQL catalog uses.

// catalogVersion is the on-disk format version. A decoder rejects an unknown
// version rather than guessing an older or newer layout.
const catalogVersion = 1

// maxCatalogBytes bounds both an untrusted catalog read and a prospective
// rewrite, so a corrupt or hostile file cannot force an unbounded allocation.
const maxCatalogBytes = 16 << 20

// ErrCatalogTooLarge reports a catalog file or encoding beyond the size bound.
var ErrCatalogTooLarge = errors.New("gateway: catalog snapshot exceeds the maximum size")

// ErrInvalidCatalog is the sentinel every catalog snapshot malformation matches
// under errors.Is.
var ErrInvalidCatalog = errors.New("gateway: invalid catalog snapshot")

// ErrCatalogWriterLocked reports that another process or goroutine is
// publishing the same durable catalog. Callers may retry; the active publisher
// retains the only authority to compare and replace the on-disk generation.
var ErrCatalogWriterLocked = errors.New("gateway: catalog snapshot has an active writer")

// ErrCatalogGenerationNotNewer reports an attempted durable publication whose
// generation is not strictly newer than the snapshot already on disk.
var ErrCatalogGenerationNotNewer = errors.New("gateway: catalog generation is not newer than the durable generation")

// ErrCatalogGenerationMismatch reports an exact-generation publication whose
// observed authority differs from the generation the controller planned from.
// It is the topology compare-and-swap fence: callers must reload and replan
// rather than applying a valid transition over unrelated concurrent changes.
var ErrCatalogGenerationMismatch = errors.New("gateway: catalog generation differs from the expected generation")

// ErrCatalogPublishOutcomeUnknown reports that the catalog rename completed
// but a post-publication close, unlock, or pinned parent-directory durability
// fence failed. The generation may be visible, but retry safety and crash
// survival cannot be asserted from the returned error alone.
var ErrCatalogPublishOutcomeUnknown = errors.New("gateway: catalog publication outcome is unknown")

// ErrCatalogDurabilityUnsupported reports a platform where the runtime cannot
// prove an atomic same-directory replacement plus directory durability. Loading
// remains supported; authoritative publication fails closed.
var ErrCatalogDurabilityUnsupported = errors.New("gateway: durable catalog publication is unsupported on this platform")

// CatalogError reports why a catalog snapshot was rejected. It wraps
// ErrInvalidCatalog.
type CatalogError struct {
	Reason string
}

func (e *CatalogError) Error() string { return "gateway: invalid catalog snapshot: " + e.Reason }

func (e *CatalogError) Unwrap() error { return ErrInvalidCatalog }

// Snapshot is one immutable, atomically published generation of authoritative
// cluster metadata. It embeds the routing configuration — distributions, table
// placements, and the manifests whose shards carry the per-shard ownership
// epochs — alongside the endpoint membership that resolves each opaque endpoint
// to a network address, and a monotonic publication generation.
//
// It has no exported mutable storage, and NewSnapshot defensively copies its
// inputs, so a published snapshot is safe to share across goroutines. Scalar
// catalog records are exposed by value; placement columns are copied; manifests
// are themselves immutable.
type Snapshot struct {
	config                         distribution.ClusterConfig
	endpoints                      map[distribution.EndpointID]string
	generation                     uint64
	planner                        []plannerTable
	plannerIndexes                 []plannerIndex
	plannerIndexPaths              []plannerStringRef
	plannerIndexRelations          []uint32
	plannerGlobalIndexPrograms     []plannerGlobalIndexProgram
	plannerGlobalIndexPointers     []vibejson.CompiledPointer
	plannerGlobalPointerExtraBytes uint64
	plannerIndexSpans              []plannerIndexSpan
	plannerIndexStrings            string
	statistics                     *queryplanner.StatisticsCatalog
	replicatedShards               []replicatedCatalogShard
	replicatedReplicas             []ReplicatedEndpoint
	replicatedTables               []replicatedCatalogTable
	durableRequestLedgerTopology   *DurableRequestLedgerTopology
	indexLineage                   []plannerIndexLineageRef
	shardLineage                   []plannerShardLineageRef
	indexIDHighWater               uint64
	// shardGenerationHighWaters is aligned with Distributions. One scalar per
	// logical keyspace fences every removed shard identity without retaining a
	// tombstone per split, merge, or move.
	shardGenerationHighWaters []distribution.ShardAllocationGeneration
	catalogLineagePresent     bool
	planSeed                  maphash.Seed
	planCache                 atomic.Pointer[preparedPlanCache]
}

// plannerTable is the compact, cache-friendly table directory used only by the
// distributed planner. Strings and catalog records stay in ClusterConfig; this
// entry stores only 32-bit indices, avoiding both a retained hash map and a
// duplicate string header per table on the route hot path.
type plannerTable struct {
	placement uint32
	spec      uint32
	manifest  uint32
}

// NewSnapshot validates config and endpoints and returns an immutable snapshot
// pinned to generation. Validation reuses ClusterConfig.Validate and additionally
// requires every shard leader endpoint to resolve to an address, so a routed
// target always has a transport destination in the generation that routed it.
// Inputs are defensively copied.
func NewSnapshot(config distribution.ClusterConfig, endpoints map[distribution.EndpointID]string, generation uint64) (*Snapshot, error) {
	return NewSnapshotWithPlannerMetadata(config, endpoints, generation, nil, nil)
}

// NewSnapshotWithIndexes validates and defensively compacts routing, endpoint,
// and distributed index metadata into one immutable catalog generation.
func NewSnapshotWithIndexes(config distribution.ClusterConfig, endpoints map[distribution.EndpointID]string, generation uint64, indexes []IndexDescriptor) (*Snapshot, error) {
	return NewSnapshotWithPlannerMetadata(config, endpoints, generation, indexes, nil)
}

// NewSnapshotWithPlannerMetadata validates and defensively compacts routing,
// index, and statistics metadata into one immutable generation. Statistics are
// advisory for cost only: every entry must name a placed table, but missing
// statistics remain legal and can never weaken routing or semantic checks.
func NewSnapshotWithPlannerMetadata(
	config distribution.ClusterConfig,
	endpoints map[distribution.EndpointID]string,
	generation uint64,
	indexes []IndexDescriptor,
	statistics []queryplanner.TableStatistics,
) (*Snapshot, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := validateCompactPlannerDimensions(config); err != nil {
		return nil, err
	}
	for _, spec := range config.Distributions {
		if spec.MapperVersion != distribution.NativeMapperVersion {
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"distribution %q mapper version %d is unsupported",
				spec.Name, spec.MapperVersion)}
		}
	}
	for _, m := range config.Manifests {
		for i := 0; i < m.ShardCount(); i++ {
			shard, _ := m.ShardInfo(i)
			for _, ep := range shard.Leaders {
				if _, ok := endpoints[ep]; !ok {
					return nil, &CatalogError{Reason: fmt.Sprintf(
						"distribution %q shard %q leader endpoint %q resolves to no address",
						m.Distribution(), shard.ID, ep)}
				}
			}
		}
	}
	cloned := cloneConfig(config)
	planner := buildPlannerTables(cloned)
	var activePartitions map[distribution.DistributionName]map[string]struct{}
	if len(statistics) != 0 {
		activePartitions = make(map[distribution.DistributionName]map[string]struct{}, len(cloned.Manifests))
		for _, manifest := range cloned.Manifests {
			partitions := make(map[string]struct{}, manifest.ShardCount())
			for shardIndex := 0; shardIndex < manifest.ShardCount(); shardIndex++ {
				metadata, _ := manifest.ShardMetadataAt(shardIndex)
				partitions[string(metadata.ID)] = struct{}{}
			}
			activePartitions[manifest.Distribution()] = partitions
		}
	}
	for i := range statistics {
		placement, ok := cloned.Placement(statistics[i].Table)
		if !ok {
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"statistics table %q has no placement", statistics[i].Table)}
		}
		partitions, ok := activePartitions[placement.Distribution]
		if !ok {
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"statistics table %q has no routing manifest", statistics[i].Table)}
		}
		for _, partition := range statistics[i].Partitions {
			if _, found := partitions[partition.Partition]; !found {
				return nil, &CatalogError{Reason: fmt.Sprintf(
					"statistics table %q partition %q is not active in distribution %q",
					statistics[i].Table, partition.Partition, placement.Distribution)}
			}
		}
	}
	statisticsCatalog, err := queryplanner.NewStatisticsCatalog(generation, statistics)
	if err != nil {
		return nil, &CatalogError{Reason: err.Error()}
	}
	indexBuild, err := buildPlannerIndexes(cloned, planner, indexes)
	if err != nil {
		return nil, err
	}
	indexLineage := buildPlannerIndexLineage(indexBuild.indexes)
	shardLineage, err := buildPlannerShardLineage(cloned)
	if err != nil {
		return nil, err
	}
	snapshot := &Snapshot{
		config:                         cloned,
		endpoints:                      cloneEndpoints(endpoints),
		generation:                     generation,
		planner:                        planner,
		plannerIndexes:                 indexBuild.indexes,
		plannerIndexPaths:              indexBuild.paths,
		plannerIndexRelations:          indexBuild.relations,
		plannerGlobalIndexPrograms:     indexBuild.globalPrograms,
		plannerGlobalIndexPointers:     indexBuild.globalPointers,
		plannerGlobalPointerExtraBytes: indexBuild.globalPointerExtraBytes,
		plannerIndexSpans:              indexBuild.spans,
		plannerIndexStrings:            indexBuild.arena,
		statistics:                     statisticsCatalog,
		indexLineage:                   indexLineage,
		shardLineage:                   shardLineage,
		planSeed:                       maphash.MakeSeed(),
	}
	return snapshot, nil
}

// Generation reports this snapshot's monotonic publication generation.
func (s *Snapshot) Generation() uint64 { return s.generation }

// DistributionCount reports the number of logical keyspaces in the snapshot.
func (s *Snapshot) DistributionCount() int {
	if s == nil {
		return 0
	}
	return len(s.config.Distributions)
}

// DistributionAt returns distribution i by value.
func (s *Snapshot) DistributionAt(i int) (distribution.DistributionSpec, bool) {
	if s == nil || i < 0 || i >= len(s.config.Distributions) {
		return distribution.DistributionSpec{}, false
	}
	return s.config.Distributions[i], true
}

// PlacementCount reports the number of placed logical tables.
func (s *Snapshot) PlacementCount() int {
	if s == nil {
		return 0
	}
	return len(s.config.Placements)
}

// PlacementAt returns placement i with an independently owned Columns slice.
func (s *Snapshot) PlacementAt(i int) (distribution.TablePlacement, bool) {
	if s == nil || i < 0 || i >= len(s.config.Placements) {
		return distribution.TablePlacement{}, false
	}
	placement := s.config.Placements[i]
	placement.Columns = slices.Clone(placement.Columns)
	return placement, true
}

// ManifestCount reports the number of immutable routing manifests.
func (s *Snapshot) ManifestCount() int {
	if s == nil {
		return 0
	}
	return len(s.config.Manifests)
}

// ManifestAt returns immutable manifest i.
func (s *Snapshot) ManifestAt(i int) (*distribution.Manifest, bool) {
	if s == nil || i < 0 || i >= len(s.config.Manifests) {
		return nil, false
	}
	return s.config.Manifests[i], true
}

// Spec resolves a distribution by name.
func (s *Snapshot) Spec(name distribution.DistributionName) (distribution.DistributionSpec, bool) {
	if s == nil {
		return distribution.DistributionSpec{}, false
	}
	return s.config.Spec(name)
}

// Placement resolves a table and returns an independently owned Columns slice.
func (s *Snapshot) Placement(table string) (distribution.TablePlacement, bool) {
	if s == nil {
		return distribution.TablePlacement{}, false
	}
	placement, ok := s.config.Placement(table)
	if !ok {
		return distribution.TablePlacement{}, false
	}
	placement.Columns = slices.Clone(placement.Columns)
	return placement, true
}

// Manifest resolves an immutable routing manifest by distribution name.
func (s *Snapshot) Manifest(name distribution.DistributionName) (*distribution.Manifest, bool) {
	if s == nil {
		return nil, false
	}
	return s.config.Manifest(name)
}

// Validate rechecks the snapshot's private routing configuration.
func (s *Snapshot) Validate() error {
	if s == nil {
		return &CatalogError{Reason: "snapshot is nil"}
	}
	if err := s.config.Validate(); err != nil {
		return err
	}
	return validateDurableRequestLedgerCatalogPresence(s)
}

// PlannerMetadataBytes reports the retained bytes in the compact table
// directory itself. Table, column, distribution, and manifest strings are
// shared with ClusterConfig and therefore are not counted twice.
func (s *Snapshot) PlannerMetadataBytes() uint64 {
	return uint64(cap(s.planner)) * uint64(unsafe.Sizeof(plannerTable{}))
}

// CatalogTransitionMetadataBytes reports the compact active-record directories
// and lifetime high-waters retained solely to validate a later catalog
// generation. It excludes the routing/index records the planner already owns.
func (s *Snapshot) CatalogTransitionMetadataBytes() uint64 {
	if s == nil {
		return 0
	}
	return uint64(cap(s.indexLineage))*uint64(unsafe.Sizeof(plannerIndexLineageRef(0))) +
		uint64(cap(s.shardLineage))*uint64(unsafe.Sizeof(plannerShardLineageRef{})) +
		uint64(cap(s.shardGenerationHighWaters))*uint64(unsafe.Sizeof(distribution.ShardAllocationGeneration(0))) +
		uint64(unsafe.Sizeof(s.indexIDHighWater))
}

func retainedReplicatedMetadataBytes(
	shards []replicatedCatalogShard,
	replicas []ReplicatedEndpoint,
	tables []replicatedCatalogTable,
) uint64 {
	retained := uint64(cap(shards))*uint64(unsafe.Sizeof(replicatedCatalogShard{})) +
		uint64(cap(replicas))*uint64(unsafe.Sizeof(ReplicatedEndpoint{}))
	retained += replicatedTableMetadataBytes(tables)
	return retained
}

// plannerTableFor resolves a logical table to its placement, distribution spec,
// and manifest with one binary search and no allocation.
func (s *Snapshot) plannerTableFor(table string) (
	distribution.TablePlacement,
	distribution.DistributionSpec,
	*distribution.Manifest,
	bool,
) {
	ordinal, ok := s.plannerTableOrdinal(table)
	if !ok {
		return distribution.TablePlacement{}, distribution.DistributionSpec{}, nil, false
	}
	entry := s.planner[ordinal]
	return s.config.Placements[entry.placement],
		s.config.Distributions[entry.spec],
		s.config.Manifests[entry.manifest],
		true
}

func buildPlannerTables(config distribution.ClusterConfig) []plannerTable {
	if len(config.Placements) == 0 {
		return nil
	}
	specs := make(map[distribution.DistributionName]uint32, len(config.Distributions))
	for i := range config.Distributions {
		specs[config.Distributions[i].Name] = uint32(i)
	}
	manifests := make(map[distribution.DistributionName]uint32, len(config.Manifests))
	for i := range config.Manifests {
		manifests[config.Manifests[i].Distribution()] = uint32(i)
	}
	entries := make([]plannerTable, len(config.Placements))
	for i := range config.Placements {
		placement := &config.Placements[i]
		entries[i] = plannerTable{
			placement: uint32(i),
			spec:      specs[placement.Distribution],
			manifest:  manifests[placement.Distribution],
		}
	}
	slices.SortFunc(entries, func(a, b plannerTable) int {
		at := config.Placements[a.placement].Table
		bt := config.Placements[b.placement].Table
		switch {
		case at < bt:
			return -1
		case at > bt:
			return 1
		default:
			return 0
		}
	})
	return entries
}

// OwnershipEpoch returns the fencing epoch the named shard is served under in
// this generation, read from the distribution's manifest — the single source of
// truth for per-shard ownership. ok is false when the distribution or shard is
// absent.
func (s *Snapshot) OwnershipEpoch(dist distribution.DistributionName, shard distribution.ShardID) (distribution.OwnershipEpoch, bool) {
	m, ok := s.Manifest(dist)
	if !ok {
		return 0, false
	}
	for i := 0; i < m.ShardCount(); i++ {
		info, _ := m.ShardMetadataAt(i)
		if info.ID == shard {
			return info.Epoch, true
		}
	}
	return 0, false
}

// ShardAllocationGeneration returns the topology allocation identity of the
// named shard. Unlike OwnershipEpoch, it never changes for ordinary elections
// or membership updates and is comparable through the distribution's
// allocation high-water.
func (s *Snapshot) ShardAllocationGeneration(
	dist distribution.DistributionName,
	shard distribution.ShardID,
) (distribution.ShardAllocationGeneration, bool) {
	m, ok := s.Manifest(dist)
	if !ok {
		return 0, false
	}
	for i := 0; i < m.ShardCount(); i++ {
		info, _ := m.ShardMetadataAt(i)
		if info.ID == shard {
			return info.AllocationGeneration, true
		}
	}
	return 0, false
}

// NextShardAllocationGeneration returns the next topology allocation identity
// available in dist's lifetime namespace. The catalog publisher still provides
// the serialization point: concurrent authorities may propose the same value,
// but only one can publish against the pinned generation and the loser must
// reload before retrying. ok is false for an unknown distribution or when the
// uint64 namespace is exhausted; allocation never falls back to wall time.
func (s *Snapshot) NextShardAllocationGeneration(
	dist distribution.DistributionName,
) (next distribution.ShardAllocationGeneration, ok bool) {
	if s == nil {
		return 0, false
	}
	ordinal := -1
	for i := range s.config.Distributions {
		if s.config.Distributions[i].Name == dist {
			ordinal = i
			break
		}
	}
	if ordinal < 0 {
		return 0, false
	}

	var highWater distribution.ShardAllocationGeneration
	if s.catalogLineagePresent {
		if len(s.shardGenerationHighWaters) != len(s.config.Distributions) {
			return 0, false
		}
		highWater = s.shardGenerationHighWaters[ordinal]
	} else if manifest, exists := s.Manifest(dist); exists {
		for i := 0; i < manifest.ShardCount(); i++ {
			metadata, _ := manifest.ShardMetadataAt(i)
			highWater = max(highWater, metadata.AllocationGeneration)
		}
	}
	if highWater == ^distribution.ShardAllocationGeneration(0) {
		return 0, false
	}
	return highWater + 1, true
}

// NextIndexID returns the next cluster-lifetime index identity. Like shard
// allocation, the returned proposal becomes authoritative only through a
// successful monotonic catalog publication. ok is false after uint64 exhaustion.
func (s *Snapshot) NextIndexID() (next uint64, ok bool) {
	if s == nil {
		return 0, false
	}
	highWater := s.indexIDHighWater
	if !s.catalogLineagePresent {
		for i := range s.plannerIndexes {
			highWater = max(highWater, s.plannerIndexes[i].indexID)
		}
	}
	if highWater == ^uint64(0) {
		return 0, false
	}
	return highWater + 1, true
}

// cloneConfig returns a defensive copy of c. The manifests are already
// immutable, so only the pointer slice and the mutable placement columns are
// cloned.
func cloneConfig(c distribution.ClusterConfig) distribution.ClusterConfig {
	// Placement columns are numerous and tiny. Store every placement's slice in
	// one flat backing array and intern repeated distribution/column spellings,
	// replacing O(tables) small allocations with two compact arrays.
	columnCount := 0
	for i := range c.Placements {
		columnCount += len(c.Placements[i].Columns)
	}
	columnArena := make([]string, columnCount)
	interned := make(map[string]string, len(c.Distributions)+len(c.Placements)+columnCount)
	intern := func(value string) string {
		if canonical, ok := interned[value]; ok {
			return canonical
		}
		canonical := strings.Clone(value)
		interned[canonical] = canonical
		return canonical
	}
	out := distribution.ClusterConfig{
		Distributions: slices.Clone(c.Distributions),
		Manifests:     slices.Clone(c.Manifests),
	}
	for i := range out.Distributions {
		out.Distributions[i].Name = distribution.DistributionName(
			intern(string(out.Distributions[i].Name)),
		)
	}
	if c.Placements != nil {
		out.Placements = make([]distribution.TablePlacement, len(c.Placements))
		columnOffset := 0
		for i, p := range c.Placements {
			p.Table = intern(p.Table)
			p.Distribution = distribution.DistributionName(intern(string(p.Distribution)))
			if p.TenantPath != "" {
				p.TenantPath = intern(p.TenantPath)
			}
			if p.AffinityGroup != "" {
				p.AffinityGroup = intern(p.AffinityGroup)
			}
			start := columnOffset
			for _, column := range p.Columns {
				columnArena[columnOffset] = intern(column)
				columnOffset++
			}
			p.Columns = columnArena[start:columnOffset:columnOffset]
			out.Placements[i] = p
		}
	}
	return out
}

func cloneEndpoints(endpoints map[distribution.EndpointID]string) map[distribution.EndpointID]string {
	if endpoints == nil {
		return nil
	}
	out := make(map[distribution.EndpointID]string, len(endpoints))
	for id, address := range endpoints {
		ownedID := distribution.EndpointID(strings.Clone(string(id)))
		out[ownedID] = strings.Clone(address)
	}
	return out
}

// CatalogHolder publishes immutable Snapshot generations for lock-free
// metadata reads and tracks the generations pinned by executor operations. A
// reader always observes one whole generation — the old or the new — never a
// mixed or partially published state. Publication and operational pinning share
// one short lock so, after a generation is published, no new operation can pin
// an older write plan. Per-row planning and execution remain lock-free.
type CatalogHolder struct {
	ptr atomic.Pointer[Snapshot]

	leaseMu      sync.Mutex
	activeLeases map[uint64]uint64
	leaseChanged chan struct{}
	leaseWaiters uint64
}

// NewCatalogHolder returns a holder seeded with initial, which may be nil.
func NewCatalogHolder(initial *Snapshot) *CatalogHolder {
	h := &CatalogHolder{
		activeLeases: make(map[uint64]uint64),
		leaseChanged: make(chan struct{}),
	}
	if initial != nil {
		snapshot, err := initialCatalogState(initial)
		if err == nil {
			h.ptr.Store(snapshot)
		}
	}
	return h
}

// Publish atomically installs s only when its generation is strictly newer
// than the current generation, and reports whether it did. Catalog authority
// never moves backward; stale or equal publishers are refused.
func (h *CatalogHolder) Publish(s *Snapshot) bool { return h.PublishNewer(s) }

// PublishNewer installs s only if it is a strictly newer generation than the
// current one (or none is published), and reports whether it did. It is the
// strongly ordered publication primitive: concurrent publishers converge on the
// highest generation and a stale republish is refused, while a reader still
// observes one whole generation.
func (h *CatalogHolder) PublishNewer(s *Snapshot) bool {
	return h.publishNewerChecked(s) == nil
}

// PublishAfter atomically installs s only when the currently published
// generation exactly equals expectedGeneration. A nil holder state is
// generation zero. This is the in-memory half of a topology cutover CAS; it
// prevents a controller from publishing a transition planned from stale
// metadata even when its proposed generation is numerically newer.
func (h *CatalogHolder) PublishAfter(expectedGeneration uint64, s *Snapshot) error {
	if s == nil {
		return &CatalogError{Reason: "next catalog snapshot is nil"}
	}
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	h.initLeaseTrackerLocked()
	cur := h.ptr.Load()
	currentGeneration := uint64(0)
	if cur != nil {
		currentGeneration = cur.generation
	}
	if currentGeneration != expectedGeneration {
		return fmt.Errorf(
			"%w: expected=%d current=%d",
			ErrCatalogGenerationMismatch, expectedGeneration, currentGeneration,
		)
	}
	if s.generation <= currentGeneration {
		return fmt.Errorf(
			"%w: proposed=%d current=%d",
			ErrCatalogGenerationNotNewer, s.generation, currentGeneration,
		)
	}
	next, err := advanceCatalogState(cur, s)
	if err != nil {
		return err
	}
	h.ptr.Store(next)
	h.signalLeaseChangeLocked()
	return nil
}

func (h *CatalogHolder) publishReplicaReplacementAfter(
	expectedGeneration uint64,
	s *Snapshot,
	grant membershipgrant.Grant,
) error {
	if s == nil || !grant.Valid() {
		return &CatalogError{Reason: "invalid certified replica replacement"}
	}
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	h.initLeaseTrackerLocked()
	current := h.ptr.Load()
	currentGeneration := uint64(0)
	if current != nil {
		currentGeneration = current.generation
	}
	if currentGeneration != expectedGeneration {
		return fmt.Errorf(
			"%w: expected=%d current=%d",
			ErrCatalogGenerationMismatch, expectedGeneration, currentGeneration,
		)
	}
	next, err := advanceCatalogStateReplicaReplacement(current, s, grant)
	if err != nil {
		return err
	}
	h.ptr.Store(next)
	h.signalLeaseChangeLocked()
	return nil
}

func (h *CatalogHolder) publishReplicaReplacementPostRemoveAfter(
	expectedGeneration uint64,
	s *Snapshot,
	grant membershipgrant.Grant,
	observedReplicaSetVersion uint64,
) error {
	if s == nil || !grant.Valid() || observedReplicaSetVersion == 0 {
		return &CatalogError{Reason: "invalid certified post-remove fence"}
	}
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	h.initLeaseTrackerLocked()
	current := h.ptr.Load()
	currentGeneration := uint64(0)
	if current != nil {
		currentGeneration = current.generation
	}
	if currentGeneration != expectedGeneration {
		return fmt.Errorf(
			"%w: expected=%d current=%d",
			ErrCatalogGenerationMismatch, expectedGeneration, currentGeneration,
		)
	}
	if err := validateReplicaReplacementPostRemoveTransition(
		current, s, grant, observedReplicaSetVersion,
	); err != nil {
		return err
	}
	h.ptr.Store(s)
	h.signalLeaseChangeLocked()
	return nil
}

// publishNewerChecked is the diagnostic publication path used by catalog
// refresh. The bool API remains convenient for optimistic callers, while a
// topology loader must distinguish an ordinary stale generation from a newer
// standalone snapshot whose cross-generation lineage is invalid.
func (h *CatalogHolder) publishNewerChecked(s *Snapshot) error {
	if s == nil {
		return &CatalogError{Reason: "next catalog snapshot is nil"}
	}
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	h.initLeaseTrackerLocked()
	cur := h.ptr.Load()
	if cur != nil && s.generation <= cur.generation {
		return fmt.Errorf(
			"%w: proposed=%d current=%d",
			ErrCatalogGenerationNotNewer, s.generation, cur.generation,
		)
	}
	next, err := advanceCatalogState(cur, s)
	if err != nil {
		return err
	}
	h.ptr.Store(next)
	h.signalLeaseChangeLocked()
	return nil
}

// Current returns the most recently published Snapshot, or nil if none has been
// published. It is a lock-free metadata-inspection API. Code that dispatches
// shard work must acquire an operational lease through Executor so schema
// generation drains can observe it.
func (h *CatalogHolder) Current() *Snapshot {
	return h.ptr.Load()
}

// catalogLease pins one immutable generation for an executor operation. A
// stale-generation retry may hold more than one lease until the operation
// returns; this deliberately makes a schema barrier wait for the whole old
// operation, not merely its last network attempt.
type catalogLease struct {
	holder     *CatalogHolder
	snapshot   *Snapshot
	generation uint64
	released   bool
}

// catalogLeaseSet keeps the ordinary initial attempt plus the default retry
// budget inline. Only an explicitly oversized retry policy allocates overflow
// storage. Query and Exec retain prior-attempt leases until return so a schema
// drain fences the complete logical operation without allocating per attempt.
type catalogLeaseSet struct {
	inline   [4]catalogLease
	overflow []catalogLease
	count    int
}

func (s *catalogLeaseSet) add(lease catalogLease) {
	if lease.holder == nil {
		return
	}
	if s.count < len(s.inline) {
		s.inline[s.count] = lease
	} else {
		s.overflow = append(s.overflow, lease)
	}
	s.count++
}

func (s *catalogLeaseSet) release() {
	if s == nil {
		return
	}
	for i := range s.overflow {
		s.overflow[len(s.overflow)-1-i].release()
	}
	inlineCount := min(s.count, len(s.inline))
	for i := 0; i < inlineCount; i++ {
		s.inline[inlineCount-1-i].release()
	}
}

// pinCurrent atomically observes and leases the current generation with
// respect to publication. Once generation G is published, no later call can
// acquire a lease below G.
func (h *CatalogHolder) pinCurrent() catalogLease {
	if h == nil {
		return catalogLease{}
	}
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	h.initLeaseTrackerLocked()
	snapshot := h.ptr.Load()
	if snapshot == nil {
		return catalogLease{}
	}
	generation := snapshot.Generation()
	h.activeLeases[generation]++
	return catalogLease{holder: h, snapshot: snapshot, generation: generation}
}

func (l *catalogLease) release() {
	if l == nil || l.holder == nil || l.released {
		return
	}
	l.released = true
	h := l.holder
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	active := h.activeLeases[l.generation]
	if active <= 1 {
		delete(h.activeLeases, l.generation)
	} else {
		h.activeLeases[l.generation] = active - 1
	}
	h.signalLeaseChangeLocked()
}

func (h *CatalogHolder) initLeaseTrackerLocked() {
	if h.activeLeases == nil {
		h.activeLeases = make(map[uint64]uint64)
	}
	if h.leaseChanged == nil {
		h.leaseChanged = make(chan struct{})
	}
}

func (h *CatalogHolder) signalLeaseChangeLocked() {
	h.initLeaseTrackerLocked()
	if h.leaseWaiters == 0 {
		return
	}
	close(h.leaseChanged)
	h.leaseChanged = make(chan struct{})
}

// CatalogDrainStatus is the local serving process's catalog-generation fence
// state. A cluster schema controller gathers one drained acknowledgement from
// every serving gateway before starting a historical index scan.
type CatalogDrainStatus struct {
	CurrentGeneration      uint64
	OldestActiveGeneration uint64
	ActiveOlderOperations  uint64
}

// DurableCatalogPublication is an opaque receipt returned only after the
// exact-generation catalog CAS has reached durable storage.
type DurableCatalogPublication struct {
	snapshot   *Snapshot
	generation uint64
}

// SaveSnapshotAfterWithReceipt durably publishes s and returns the receipt
// required to authorize destructive post-publication cleanup.
func SaveSnapshotAfterWithReceipt(path string, expectedGeneration uint64, s *Snapshot) (DurableCatalogPublication, error) {
	if err := SaveSnapshotAfter(path, expectedGeneration, s); err != nil {
		return DurableCatalogPublication{}, err
	}
	return DurableCatalogPublication{snapshot: s, generation: s.Generation()}, nil
}

// AuthorizeRetainedPrune succeeds only while the exact durably published
// snapshot is current and every older local serving lease is drained.
// RetainedPruneAuthority is the sealed capability returned only after durable
// catalog publication and old-generation lease drain. Its private marker makes
// it impossible for another package to implement or fabricate.
type RetainedPruneAuthority interface {
	Manifest() *distribution.Manifest
	Generation() uint64
	Operation() [sha256.Size]byte
	Certificate() [sha256.Size]byte
	retainedPruneAuthoritySeal()
}

type retainedPruneAuthority struct {
	manifest    *distribution.Manifest
	generation  uint64
	operation   [sha256.Size]byte
	certificate [sha256.Size]byte
}

func (a retainedPruneAuthority) Manifest() *distribution.Manifest { return a.manifest }
func (a retainedPruneAuthority) Generation() uint64               { return a.generation }
func (a retainedPruneAuthority) Operation() [sha256.Size]byte     { return a.operation }
func (a retainedPruneAuthority) Certificate() [sha256.Size]byte   { return a.certificate }
func (retainedPruneAuthority) retainedPruneAuthoritySeal()        {}

func (h *CatalogHolder) AuthorizeRetainedPrune(
	receipt DurableCatalogPublication,
	distributionName distribution.DistributionName,
	operation [sha256.Size]byte,
	certificate [sha256.Size]byte,
) (RetainedPruneAuthority, error) {
	if h == nil || receipt.snapshot == nil || operation == ([sha256.Size]byte{}) ||
		certificate == ([sha256.Size]byte{}) {
		return nil, ErrStaleGeneration
	}
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	h.initLeaseTrackerLocked()
	current := h.ptr.Load()
	status := h.drainStatusLocked(receipt.generation)
	if current == nil || current.Generation() != receipt.generation ||
		status.CurrentGeneration != receipt.generation || status.ActiveOlderOperations != 0 {
		return nil, ErrStaleGeneration
	}
	manifest, ok := current.Manifest(distributionName)
	durableManifest, durableOK := receipt.snapshot.Manifest(distributionName)
	if !ok || !durableOK || !manifest.Equal(durableManifest) {
		return nil, ErrStaleGeneration
	}
	return retainedPruneAuthority{
		manifest: manifest, generation: receipt.generation,
		operation: operation, certificate: certificate,
	}, nil
}

// DrainStatus reports active operations older than generation without waiting.
// OldestActiveGeneration is zero when there is no such operation.
func (h *CatalogHolder) DrainStatus(generation uint64) CatalogDrainStatus {
	if h == nil {
		return CatalogDrainStatus{}
	}
	h.leaseMu.Lock()
	defer h.leaseMu.Unlock()
	return h.drainStatusLocked(generation)
}

func (h *CatalogHolder) drainStatusLocked(generation uint64) CatalogDrainStatus {
	status := CatalogDrainStatus{}
	if current := h.ptr.Load(); current != nil {
		status.CurrentGeneration = current.Generation()
	}
	for activeGeneration, count := range h.activeLeases {
		if activeGeneration >= generation || count == 0 {
			continue
		}
		status.ActiveOlderOperations += count
		if status.OldestActiveGeneration == 0 || activeGeneration < status.OldestActiveGeneration {
			status.OldestActiveGeneration = activeGeneration
		}
	}
	return status
}

// WaitOlderDrained waits until this serving process has published at least
// generation and every operation pinned below it has returned. Publication and
// pin acquisition use the same lock, so a successful return is a stable local
// fence: a late operation cannot subsequently acquire an older plan.
func (h *CatalogHolder) WaitOlderDrained(ctx context.Context, generation uint64) (CatalogDrainStatus, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if h == nil || generation == 0 {
		return CatalogDrainStatus{}, ErrStaleGeneration
	}
	for {
		h.leaseMu.Lock()
		h.initLeaseTrackerLocked()
		status := h.drainStatusLocked(generation)
		if status.CurrentGeneration >= generation && status.ActiveOlderOperations == 0 {
			h.leaseMu.Unlock()
			return status, nil
		}
		changed := h.leaseChanged
		h.leaseWaiters++
		h.leaseMu.Unlock()
		var err error
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-changed:
		}
		h.leaseMu.Lock()
		h.leaseWaiters--
		h.leaseMu.Unlock()
		if err != nil {
			return status, err
		}
	}
}

// The on-disk catalog format. It is a versioned JSON document; keyspace points
// are big-endian hex so the file is inspectable and platform-independent, and
// the ordering of every collection is deterministic so equal snapshots persist
// to identical bytes.

type persistedCatalog struct {
	Version          int                                    `json:"version"`
	Generation       uint64                                 `json:"generation"`
	Distributions    []persistedDistribution                `json:"distributions"`
	Placements       []persistedPlacement                   `json:"placements,omitempty"`
	Indexes          []persistedIndex                       `json:"indexes,omitempty"`
	Statistics       []queryplanner.TableStatistics         `json:"statistics,omitempty"`
	Manifests        []persistedManifest                    `json:"manifests"`
	Endpoints        []persistedEndpoint                    `json:"endpoints"`
	ReplicatedShards []persistedReplicatedShard             `json:"replicated_shards,omitempty"`
	ReplicatedTables []persistedReplicatedTable             `json:"replicated_tables,omitempty"`
	RequestLedger    *persistedDurableRequestLedgerTopology `json:"request_ledger,omitempty"`
	Lineage          *persistedCatalogLineage               `json:"lineage,omitempty"`
}

type persistedDistribution struct {
	Name          string `json:"name"`
	Arity         int    `json:"arity"`
	MapperVersion uint32 `json:"mapper_version"`
	BucketBits    uint8  `json:"bucket_bits,omitempty"`
}

type persistedPlacement struct {
	Table         string   `json:"table"`
	Distribution  string   `json:"distribution"`
	Columns       []string `json:"columns"`
	TenantPath    string   `json:"tenant_path,omitempty"`
	AffinityGroup string   `json:"affinity_group,omitempty"`
}

type persistedIndex struct {
	IndexID      uint64         `json:"index_id"`
	Incarnation  uint64         `json:"incarnation"`
	Table        string         `json:"table"`
	Name         string         `json:"name"`
	Relation     string         `json:"relation,omitempty"`
	Paths        []string       `json:"paths"`
	LocatorPaths []string       `json:"locator_paths,omitempty"`
	PrimaryPath  string         `json:"primary_path,omitempty"`
	Flags        IndexFlags     `json:"flags"`
	Lifecycle    IndexLifecycle `json:"lifecycle"`
}

type persistedManifest struct {
	Distribution string           `json:"distribution"`
	Version      uint64           `json:"version"`
	Shards       []persistedShard `json:"shards"`
}

type persistedShard struct {
	ID         string   `json:"id"`
	Generation uint64   `json:"generation"`
	Start      string   `json:"start"`
	End        string   `json:"end,omitempty"`
	EndMax     bool     `json:"end_max,omitempty"`
	Leaders    []string `json:"leaders"`
	Epoch      uint64   `json:"epoch,omitempty"`
}

type persistedEndpoint struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// persistedCatalogLineage stays compact under unbounded index churn and shard
// split/merge cycles. ShardGenerationHighWaters is aligned with Distributions,
// avoiding repeated logical names in both memory and JSON.
type persistedCatalogLineage struct {
	IndexIDHighWater          uint64   `json:"index_id_high_water"`
	ShardGenerationHighWaters []uint64 `json:"shard_generation_high_waters"`
}

// toPersisted renders s in the on-disk form with every collection in a
// deterministic order.
func toPersisted(s *Snapshot) persistedCatalog {
	pc := persistedCatalog{
		Version: catalogVersion, Generation: s.generation,
		Lineage: &persistedCatalogLineage{IndexIDHighWater: s.indexIDHighWater},
	}
	pc.Lineage.ShardGenerationHighWaters = make([]uint64, len(s.shardGenerationHighWaters))
	for i := range s.shardGenerationHighWaters {
		pc.Lineage.ShardGenerationHighWaters[i] = uint64(s.shardGenerationHighWaters[i])
	}
	for _, d := range s.config.Distributions {
		pc.Distributions = append(pc.Distributions, persistedDistribution{
			Name: string(d.Name), Arity: d.Arity, MapperVersion: uint32(d.MapperVersion),
			BucketBits: d.BucketBits,
		})
	}
	for _, p := range s.config.Placements {
		pc.Placements = append(pc.Placements, persistedPlacement{
			Table: p.Table, Distribution: string(p.Distribution), Columns: p.Columns,
			TenantPath: p.TenantPath, AffinityGroup: p.AffinityGroup,
		})
	}
	for tableOrdinal := range s.plannerIndexSpans {
		table := s.config.Placements[s.planner[tableOrdinal].placement].Table
		span := s.plannerIndexSpans[tableOrdinal]
		for i := uint32(0); i < span.count; i++ {
			ordinal := span.first + i
			metadata := s.indexMetadata(table, ordinal)
			paths := make([]string, metadata.PathCount)
			copy(paths, metadata.Paths[:metadata.PathCount])
			locators := make([]string, metadata.LocatorCount)
			copy(locators, metadata.LocatorPaths[:metadata.LocatorCount])
			relation := ""
			primaryPath := ""
			if metadata.Global() {
				relation = metadata.Relation
				program, _ := s.globalIndexPointerProgram(ordinal)
				primaryPath = metadata.LocatorPaths[program.primary]
			}
			pc.Indexes = append(pc.Indexes, persistedIndex{
				IndexID: metadata.IndexID, Incarnation: metadata.Incarnation,
				Table: metadata.Table, Name: metadata.Name, Relation: relation,
				Paths: paths, LocatorPaths: locators, PrimaryPath: primaryPath,
				Flags: metadata.Flags, Lifecycle: metadata.Lifecycle,
			})
		}
	}
	pc.Statistics = s.statistics.Descriptors()
	pc.ReplicatedShards = persistedReplicatedDescriptors(s.replicatedDescriptors())
	pc.ReplicatedTables = persistedReplicatedTableProfiles(s.replicatedTableProfiles())
	pc.RequestLedger = persistedDurableRequestLedgerTopologyFromSnapshot(s)
	for _, m := range s.config.Manifests {
		pm := persistedManifest{Distribution: string(m.Distribution()), Version: uint64(m.Version())}
		for i := 0; i < m.ShardCount(); i++ {
			shard, _ := m.ShardInfo(i)
			ps := persistedShard{
				ID:         string(shard.ID),
				Generation: uint64(shard.AllocationGeneration),
				Start:      hex.EncodeToString(shard.Range.Start[:]),
				EndMax:     shard.Range.End.Max,
				Epoch:      uint64(shard.Epoch),
			}
			if !shard.Range.End.Max {
				ps.End = hex.EncodeToString(shard.Range.End.Point[:])
			}
			for _, ep := range shard.Leaders {
				ps.Leaders = append(ps.Leaders, string(ep))
			}
			pm.Shards = append(pm.Shards, ps)
		}
		pc.Manifests = append(pc.Manifests, pm)
	}
	ids := make([]string, 0, len(s.endpoints))
	for id := range s.endpoints {
		ids = append(ids, string(id))
	}
	slices.Sort(ids)
	for _, id := range ids {
		pc.Endpoints = append(pc.Endpoints, persistedEndpoint{ID: id, Address: s.endpoints[distribution.EndpointID(id)]})
	}
	return pc
}

// SaveSnapshot durably publishes s to path with a crash-safe, monotonic recipe:
// encode and bound the size, acquire the catalog's cross-process writer lease,
// validate that the durable generation and index incarnations advance, write a
// sibling temporary file, fsync it, rename it over the destination, then fsync
// the directory. A failure before the rename leaves the previous catalog intact
// and removes the temporary file. The lease makes the compare-and-rename one
// indivisible publication, so concurrent successful writers cannot roll the
// durable authority backward.
// Platforms without a proven atomic replace plus directory durability return
// ErrCatalogDurabilityUnsupported before creating a lease or temporary file.
// The serialization proof covers cooperating publishers using this API; an
// administrator must not unlink or replace the persistent .lock entry while
// catalog publication is live.
func SaveSnapshot(path string, s *Snapshot) error {
	return saveSnapshot(path, s, nil)
}

// SaveSnapshotAfter durably publishes s only when the catalog currently on
// disk has exactly expectedGeneration. The comparison executes while holding
// the same cross-process writer lease as the atomic replacement, so a
// successful return proves the transition was applied to the topology it was
// planned from. An absent durable catalog is generation zero.
func SaveSnapshotAfter(path string, expectedGeneration uint64, s *Snapshot) error {
	return saveSnapshot(path, s, &expectedGeneration)
}

func saveSnapshot(path string, s *Snapshot, expectedGeneration *uint64) (err error) {
	if err := validateDurableRequestLedgerCatalogPresence(s); err != nil {
		return err
	}
	if s == nil {
		return errors.New("gateway: SaveSnapshot requires a non-nil snapshot")
	}
	if !catalogDurabilitySupported() {
		return ErrCatalogDurabilityUnsupported
	}
	root, base, err := openCatalogRoot(path)
	if err != nil {
		return err
	}
	publishErr := saveSnapshotAtRootAfter(root, base, s, expectedGeneration)
	closeErr := root.Close()
	if closeErr == nil {
		return publishErr
	}
	if publishErr == nil || errors.Is(publishErr, ErrCatalogPublishOutcomeUnknown) {
		return errors.Join(publishErr, ErrCatalogPublishOutcomeUnknown, closeErr)
	}
	return errors.Join(publishErr, closeErr)
}

func saveSnapshotAtRoot(root *os.Root, base string, s *Snapshot) (err error) {
	return saveSnapshotAtRootAfter(root, base, s, nil)
}

func saveSnapshotAtRootAfter(
	root *os.Root,
	base string,
	s *Snapshot,
	expectedGeneration *uint64,
) (err error) {
	if root == nil || base == "" || s == nil {
		return errors.New("gateway: invalid pinned catalog publication")
	}
	ok := false
	lockFile, err := openCatalogRootFile(root, base+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	lockInfo, err := lockFile.Stat()
	if err != nil {
		_ = lockFile.Close()
		return err
	}
	if err := storeio.LockWriter(lockFile); err != nil {
		_ = lockFile.Close()
		if errors.Is(err, storeio.ErrWriterLocked) {
			return fmt.Errorf("%w: %v", ErrCatalogWriterLocked, err)
		}
		return err
	}
	defer func() {
		cleanupErr := errors.Join(storeio.UnlockWriter(lockFile), lockFile.Close())
		if cleanupErr == nil {
			return
		}
		if ok {
			err = errors.Join(err, ErrCatalogPublishOutcomeUnknown, cleanupErr)
		} else {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if err := verifyCatalogEntryUnchanged(root, base+".lock", lockInfo); err != nil {
		return err
	}

	current, currentEntry, currentFile, loadErr := loadSnapshotAt(root, base)
	defer func() {
		if currentFile != nil {
			err = errors.Join(err, currentFile.Close())
		}
	}()
	var nextState *Snapshot
	switch {
	case loadErr == nil:
		if expectedGeneration != nil && current.generation != *expectedGeneration {
			return fmt.Errorf(
				"%w: expected=%d durable=%d",
				ErrCatalogGenerationMismatch, *expectedGeneration, current.generation,
			)
		}
		if s.generation <= current.generation {
			return fmt.Errorf(
				"%w: proposed=%d durable=%d",
				ErrCatalogGenerationNotNewer, s.generation, current.generation,
			)
		}
		currentState, stateErr := initialCatalogState(current)
		if stateErr != nil {
			return stateErr
		}
		nextState, err = advanceCatalogState(currentState, s)
		if err != nil {
			return err
		}
	case !errors.Is(loadErr, os.ErrNotExist):
		return loadErr
	default:
		if expectedGeneration != nil && *expectedGeneration != 0 {
			return fmt.Errorf(
				"%w: expected=%d durable=0",
				ErrCatalogGenerationMismatch, *expectedGeneration,
			)
		}
		nextState, err = initialCatalogState(s)
		if err != nil {
			return err
		}
	}

	persisted := toPersisted(nextState)
	compact, err := vibejson.Marshal(&persisted)
	if err != nil {
		return err
	}
	raw, err := vibejson.AppendIndent(nil, compact, "", "  ")
	if err != nil {
		return err
	}
	if len(raw) > maxCatalogBytes {
		return fmt.Errorf("%w: encoded catalog is %d bytes, maximum is %d",
			ErrCatalogTooLarge, len(raw), maxCatalogBytes)
	}
	tmp, tmpName, err := createCatalogTemp(root, base)
	if err != nil {
		return err
	}
	tmpOpen := true
	defer func() {
		var cleanupErr error
		if tmpOpen {
			cleanupErr = tmp.Close()
		}
		if !ok {
			if removeErr := root.Remove(tmpName); removeErr != nil &&
				!errors.Is(removeErr, os.ErrNotExist) {
				cleanupErr = errors.Join(cleanupErr, removeErr)
			}
		}
		if cleanupErr != nil {
			if ok {
				err = errors.Join(err, ErrCatalogPublishOutcomeUnknown, cleanupErr)
			} else {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	tmpInfo, err := tmp.Stat()
	if err != nil {
		return err
	}
	if err := verifyCatalogEntryUnchanged(root, base+".lock", lockInfo); err != nil {
		return err
	}
	if err := verifyCatalogEntryUnchanged(root, base, currentEntry); err != nil {
		return err
	}
	if err := verifyCatalogEntryUnchanged(root, tmpName, tmpInfo); err != nil {
		return err
	}
	if err := replaceCatalogEntry(root, tmpName, base); err != nil {
		return err
	}
	ok = true
	tmpCloseErr := tmp.Close()
	tmpOpen = false
	var currentCloseErr error
	if currentFile != nil {
		currentCloseErr = currentFile.Close()
		currentFile = nil
	}
	syncErr := fsyncCatalogRoot(root)
	postPublishErr := errors.Join(tmpCloseErr, currentCloseErr, syncErr)
	if postPublishErr != nil {
		return errors.Join(ErrCatalogPublishOutcomeUnknown, postPublishErr)
	}
	return nil
}

// LoadSnapshot reads and validates the catalog persisted at path, returning the
// same immutable snapshot generation SaveSnapshot wrote. The read is size-bounded
// and every manifest is re-validated through NewManifest, so a corrupt or
// inconsistent file fails closed rather than routing.
func LoadSnapshot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return loadSnapshotFile(file, path)
}

// AppendSnapshotDocument appends the canonical compact vibejson image used by
// the replicated catalog relation. It carries the same validated fields and
// lineage as the durable file format without whitespace or a second schema.
func AppendSnapshotDocument(dst []byte, snapshot *Snapshot) ([]byte, error) {
	return appendSnapshotDocumentBounded(dst, snapshot, maxCatalogBytes)
}

// appendSnapshotDocumentBounded applies a stricter caller-owned value budget
// before growing dst. The replicated catalog uses this to account for its
// outer /id envelope within the smaller mutation-value ceiling, while the file
// catalog retains maxCatalogBytes.
func appendSnapshotDocumentBounded(
	dst []byte,
	snapshot *Snapshot,
	maximum int,
) ([]byte, error) {
	if snapshot == nil {
		return dst, ErrInvalidCatalog
	}
	if err := validateDurableRequestLedgerCatalogPresence(snapshot); err != nil {
		return dst, err
	}
	if maximum <= 0 || maximum > maxCatalogBytes {
		return dst, ErrCatalogTooLarge
	}
	state, err := initialCatalogState(snapshot)
	if err != nil {
		return dst, err
	}
	persisted := toPersisted(state)
	compact, err := vibejson.Marshal(&persisted)
	if err != nil {
		return dst, err
	}
	if len(compact) > maximum {
		return dst, ErrCatalogTooLarge
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, compact)
	if err != nil || len(dst)-start > maximum {
		return dst[:start], errors.Join(err, ErrCatalogTooLarge)
	}
	return dst, nil
}

// OpenSnapshotDocument validates one bounded replicated catalog image and
// reconstructs the same immutable Snapshot accepted by LoadSnapshot.
func OpenSnapshotDocument(raw []byte) (*Snapshot, error) {
	if len(raw) == 0 || len(raw) > maxCatalogBytes {
		return nil, ErrCatalogTooLarge
	}
	snapshot, err := decodeSnapshotBytes(raw)
	if err != nil {
		return nil, err
	}
	canonical, err := AppendSnapshotDocument(nil, snapshot)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.Join(err, ErrInvalidCatalog)
	}
	return snapshot, nil
}

// loadSnapshotFile consumes file on every return path.
func loadSnapshotFile(file *os.File, label string) (*Snapshot, error) {
	snapshot, err := decodeSnapshotFile(file, label)
	return snapshot, errors.Join(err, file.Close())
}

func decodeSnapshotFile(file *os.File, label string) (*Snapshot, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > maxCatalogBytes {
		return nil, fmt.Errorf("%w: %s is %d bytes, maximum is %d",
			ErrCatalogTooLarge, label, info.Size(), maxCatalogBytes)
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxCatalogBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxCatalogBytes {
		return nil, fmt.Errorf("%w: %s grew beyond %d bytes while it was read",
			ErrCatalogTooLarge, label, maxCatalogBytes)
	}
	return decodeSnapshotBytes(raw)
}

func decodeSnapshotBytes(raw []byte) (*Snapshot, error) {
	var pc persistedCatalog
	if err := vibejson.Unmarshal(raw, &pc); err != nil {
		return nil, err
	}
	if pc.Version != catalogVersion {
		return nil, &CatalogError{Reason: fmt.Sprintf("unsupported catalog version %d", pc.Version)}
	}
	config, endpoints, indexes, statistics, err := pc.toConfig()
	if err != nil {
		return nil, err
	}
	replicated, err := pc.replicatedDescriptors()
	if err != nil {
		return nil, err
	}
	replicatedTables, err := pc.replicatedTableProfiles()
	if err != nil {
		return nil, err
	}
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, pc.Generation, indexes, statistics, replicated, replicatedTables,
	)
	if err != nil {
		return nil, err
	}
	if err := snapshot.attachPersistedDurableRequestLedgerTopology(pc.RequestLedger); err != nil {
		return nil, err
	}
	if err := validateDurableRequestLedgerCatalogPresence(snapshot); err != nil {
		return nil, err
	}
	if pc.Lineage == nil {
		return nil, &CatalogError{Reason: "catalog lineage fence is missing"}
	}
	snapshot.catalogLineagePresent = true
	snapshot.indexIDHighWater = pc.Lineage.IndexIDHighWater
	snapshot.shardGenerationHighWaters = make(
		[]distribution.ShardAllocationGeneration, len(pc.Lineage.ShardGenerationHighWaters),
	)
	for i := range pc.Lineage.ShardGenerationHighWaters {
		snapshot.shardGenerationHighWaters[i] = distribution.ShardAllocationGeneration(
			pc.Lineage.ShardGenerationHighWaters[i],
		)
	}
	state, err := initialCatalogState(snapshot)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func openCatalogRoot(path string) (*os.Root, string, error) {
	if path == "" {
		return nil, "", errors.New("gateway: catalog path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, "", err
	}
	base := filepath.Base(absolute)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return nil, "", errors.New("gateway: catalog path has no file name")
	}
	dir, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return nil, "", err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, "", err
	}
	return root, base, nil
}

// openCatalogRootFile opens one regular non-symlink root entry and proves that
// the descriptor is still the entry observed after open. For O_CREATE callers,
// a pre-existing entry is also compared against the descriptor.
func openCatalogRootFile(root *os.Root, name string, flag int, perm os.FileMode) (*os.File, error) {
	attempts := 1
	if flag&os.O_CREATE != 0 {
		// os.Root deliberately fails closed when an absent final component
		// appears during its open walk. Concurrent first publishers can cause
		// that benign race on the shared lease entry, so repeat the complete
		// before/descriptor/after proof rather than weakening it.
		attempts = 100
	}
	for attempt := 0; attempt < attempts; attempt++ {
		before, beforeErr := root.Lstat(name)
		if beforeErr != nil && !errors.Is(beforeErr, os.ErrNotExist) {
			return nil, beforeErr
		}
		if beforeErr == nil && !before.Mode().IsRegular() {
			return nil, &CatalogError{Reason: fmt.Sprintf("catalog entry %q is not a regular non-symlink file", name)}
		}
		file, err := root.OpenFile(name, flag, perm)
		if err != nil {
			if flag&os.O_CREATE != 0 && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		fileInfo, fileErr := file.Stat()
		after, afterErr := root.Lstat(name)
		stable := fileErr == nil && afterErr == nil && fileInfo.Mode().IsRegular() &&
			after.Mode().IsRegular() && os.SameFile(fileInfo, after)
		if beforeErr == nil {
			stable = stable && os.SameFile(before, after)
		}
		if stable {
			return file, nil
		}
		_ = file.Close()
		if fileErr != nil {
			return nil, fileErr
		}
		if flag&os.O_CREATE != 0 && errors.Is(afterErr, os.ErrNotExist) {
			continue
		}
		if afterErr != nil {
			return nil, afterErr
		}
		return nil, &CatalogError{Reason: fmt.Sprintf("catalog entry %q changed while opening", name)}
	}
	return nil, &CatalogError{Reason: fmt.Sprintf("catalog entry %q did not stabilize while opening", name)}
}

func loadSnapshotAt(root *os.Root, name string) (*Snapshot, os.FileInfo, *os.File, error) {
	before, err := root.Lstat(name)
	if err != nil {
		return nil, nil, nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, nil, nil, &CatalogError{Reason: "catalog is not a regular non-symlink file"}
	}
	file, err := openCatalogRootFile(root, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, nil, err
	}
	if !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, nil, nil, &CatalogError{Reason: "catalog changed while opening"}
	}
	snapshot, err := decodeSnapshotFile(file, name)
	if err != nil {
		return nil, nil, nil, errors.Join(err, file.Close())
	}
	return snapshot, before, file, nil
}

func createCatalogTemp(root *os.Root, base string) (*os.File, string, error) {
	var nonce [16]byte
	for attempts := 0; attempts < 100; attempts++ {
		if _, err := cryptorand.Read(nonce[:]); err != nil {
			return nil, "", err
		}
		name := "." + base + ".tmp-" + hex.EncodeToString(nonce[:])
		file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", errors.New("gateway: could not allocate a unique catalog temporary file")
}

func verifyCatalogEntryUnchanged(root *os.Root, name string, expected os.FileInfo) error {
	current, err := root.Lstat(name)
	if expected == nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		return &CatalogError{Reason: "catalog appeared outside the active writer lease"}
	}
	if err != nil {
		return err
	}
	if !current.Mode().IsRegular() || !os.SameFile(expected, current) {
		return &CatalogError{Reason: "catalog changed outside the active writer lease"}
	}
	return nil
}

// toConfig reconstructs the routing configuration and endpoint membership from
// the on-disk form, validating every manifest through NewManifest.
func (pc persistedCatalog) toConfig() (distribution.ClusterConfig, map[distribution.EndpointID]string, []IndexDescriptor, []queryplanner.TableStatistics, error) {
	var config distribution.ClusterConfig
	for _, d := range pc.Distributions {
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name:          distribution.DistributionName(d.Name),
			Arity:         d.Arity,
			MapperVersion: distribution.MapperVersion(d.MapperVersion),
			BucketBits:    d.BucketBits,
		})
	}
	for _, p := range pc.Placements {
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table:         p.Table,
			Distribution:  distribution.DistributionName(p.Distribution),
			Columns:       slices.Clone(p.Columns),
			TenantPath:    p.TenantPath,
			AffinityGroup: p.AffinityGroup,
		})
	}
	for _, pm := range pc.Manifests {
		shards := make([]distribution.Shard, len(pm.Shards))
		for i, ps := range pm.Shards {
			shard, err := ps.toShard()
			if err != nil {
				return config, nil, nil, nil, err
			}
			shards[i] = shard
		}
		m, err := distribution.NewManifest(
			distribution.DistributionName(pm.Distribution),
			distribution.RoutingVersion(pm.Version),
			shards,
		)
		if err != nil {
			return config, nil, nil, nil, err
		}
		config.Manifests = append(config.Manifests, m)
	}
	endpoints := make(map[distribution.EndpointID]string, len(pc.Endpoints))
	for _, e := range pc.Endpoints {
		if e.ID == "" {
			return config, nil, nil, nil, &CatalogError{Reason: "endpoint has an empty id"}
		}
		if _, dup := endpoints[distribution.EndpointID(e.ID)]; dup {
			return config, nil, nil, nil, &CatalogError{Reason: "duplicate endpoint id " + e.ID}
		}
		endpoints[distribution.EndpointID(e.ID)] = e.Address
	}
	indexes := make([]IndexDescriptor, len(pc.Indexes))
	for i := range pc.Indexes {
		index := &pc.Indexes[i]
		indexes[i] = IndexDescriptor{
			IndexID: index.IndexID, Incarnation: index.Incarnation,
			Table: index.Table, Name: index.Name, Relation: index.Relation,
			Paths: slices.Clone(index.Paths), LocatorPaths: slices.Clone(index.LocatorPaths),
			PrimaryPath: index.PrimaryPath,
			Flags:       index.Flags, Lifecycle: index.Lifecycle,
		}
	}
	return config, endpoints, indexes, slices.Clone(pc.Statistics), nil
}

// toShard reconstructs one manifest shard, decoding its big-endian hex keyspace
// bounds.
func (ps persistedShard) toShard() (distribution.Shard, error) {
	start, err := decodePoint(ps.Start)
	if err != nil {
		return distribution.Shard{}, &CatalogError{Reason: "shard " + ps.ID + " start: " + err.Error()}
	}
	end := distribution.KeyspaceEnd{Max: ps.EndMax}
	if !ps.EndMax {
		point, err := decodePoint(ps.End)
		if err != nil {
			return distribution.Shard{}, &CatalogError{Reason: "shard " + ps.ID + " end: " + err.Error()}
		}
		end.Point = point
	}
	leaders := make([]distribution.EndpointID, len(ps.Leaders))
	for i, ep := range ps.Leaders {
		leaders[i] = distribution.EndpointID(ep)
	}
	return distribution.Shard{
		ID:                   distribution.ShardID(ps.ID),
		AllocationGeneration: distribution.ShardAllocationGeneration(ps.Generation),
		Range:                distribution.KeyRange{Start: start, End: end},
		Leaders:              leaders,
		Epoch:                distribution.OwnershipEpoch(ps.Epoch),
	}, nil
}

// decodePoint parses a big-endian hex keyspace point of exactly KeyspaceWidth
// bytes.
func decodePoint(s string) (distribution.KeyspacePoint, error) {
	var p distribution.KeyspacePoint
	b, err := hex.DecodeString(s)
	if err != nil {
		return p, err
	}
	if len(b) != distribution.KeyspaceWidth {
		return p, fmt.Errorf("keyspace point is %d bytes, want %d", len(b), distribution.KeyspaceWidth)
	}
	copy(p[:], b)
	return p, nil
}
