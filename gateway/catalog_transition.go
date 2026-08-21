package gateway

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

// CertifiedRangeSplit is one independently prepared split result for a shared
// catalog successor. Target changes exactly the partitioner's source shard.
type CertifiedRangeSplit struct {
	Target      *distribution.Manifest
	Partitioner *rangesplit.Partitioner
	Certificate rangesplit.CutoverCertificate
	Prune       rangesplit.RetainedPruneCursor
}

// BuildCertifiedRangeSplitBatch validates and composes disjoint range splits
// prepared from one catalog generation. Splits may share a distribution when
// their source allocations differ. The result remains unpublished; one later
// durable and in-memory catalog CAS grants authority to the whole batch.
func BuildCertifiedRangeSplitBatch(
	current *Snapshot,
	nextGeneration uint64,
	splits []CertifiedRangeSplit,
) (*Snapshot, error) {
	if current == nil || current.Generation() == ^uint64(0) ||
		nextGeneration != current.Generation()+1 || len(splits) == 0 ||
		len(splits) > rangesplit.MaxComposedManifestSplits {
		return nil, rangesplit.ErrManifestTransition
	}
	var order [rangesplit.MaxComposedManifestSplits]uint8
	for index := range splits {
		split := &splits[index]
		if split.Target == nil || split.Partitioner == nil {
			return nil, rangesplit.ErrManifestTransition
		}
		source, ok := current.Manifest(split.Target.Distribution())
		if !ok {
			return nil, rangesplit.ErrManifestTransition
		}
		if err := split.Partitioner.ValidatePublicationTransition(
			source, split.Target, current.Generation(), nextGeneration,
			split.Certificate, split.Prune,
		); err != nil {
			return nil, err
		}
		order[index] = uint8(index)
	}
	slices.SortFunc(order[:len(splits)], func(left, right uint8) int {
		return cmp.Compare(
			splits[left].Target.Distribution(), splits[right].Target.Distribution(),
		)
	})

	var manifests [rangesplit.MaxComposedManifestSplits]*distribution.Manifest
	manifestCount := 0
	for first := 0; first < len(splits); {
		distributionName := splits[order[first]].Target.Distribution()
		last := first + 1
		for last < len(splits) &&
			splits[order[last]].Target.Distribution() == distributionName {
			last++
		}
		currentManifest, _ := current.Manifest(distributionName)
		var transitions [rangesplit.MaxComposedManifestSplits]rangesplit.ManifestTransition
		for index := first; index < last; index++ {
			split := &splits[order[index]]
			transitions[index-first] = rangesplit.ManifestTransition{
				Partitioner: split.Partitioner, Target: split.Target,
			}
		}
		combined, err := rangesplit.ComposeManifestTransitions(
			currentManifest, transitions[:last-first],
		)
		if err != nil {
			return nil, err
		}
		manifests[manifestCount] = combined
		manifestCount++
		first = last
	}
	return BuildManifestTransitions(current, manifests[:manifestCount], nextGeneration)
}

// BuildCertifiedRangeSplitTransition constructs an unpublished catalog
// generation only after cutover, retained-source pruning, and the exact
// one-source manifest replacement all match the same split plan. Durable and
// in-memory authority still move through SaveSnapshotAfter and
// CatalogHolder.PublishAfter.
func BuildCertifiedRangeSplitTransition(
	current *Snapshot,
	nextManifest *distribution.Manifest,
	nextGeneration uint64,
	partitioner *rangesplit.Partitioner,
	certificate rangesplit.CutoverCertificate,
	prune rangesplit.RetainedPruneCursor,
) (*Snapshot, error) {
	if current == nil || nextManifest == nil || partitioner == nil {
		return nil, rangesplit.ErrManifestTransition
	}
	currentManifest, ok := current.Manifest(nextManifest.Distribution())
	if !ok {
		return nil, rangesplit.ErrManifestTransition
	}
	if err := partitioner.ValidatePublicationTransition(
		currentManifest, nextManifest, current.Generation(), nextGeneration,
		certificate, prune,
	); err != nil {
		return nil, err
	}
	return BuildManifestTransition(current, nextManifest, nextGeneration)
}

// BuildManifestTransition constructs an unpublished catalog generation by
// replacing exactly one distribution manifest while preserving the current
// placements, endpoint directory, indexes, and statistics. Publication still
// requires PublishAfter or SaveSnapshotAfter: constructing this value grants
// no topology authority.
//
// This is deliberately a cold control-plane operation. The returned snapshot
// is independently owned and fully revalidated; routed requests continue to
// use the compact immutable metadata already published in current.
func BuildManifestTransition(
	current *Snapshot,
	nextManifest *distribution.Manifest,
	nextGeneration uint64,
) (*Snapshot, error) {
	if nextManifest == nil {
		return nil, &CatalogError{Reason: "manifest transition requires current and next snapshots"}
	}
	return BuildManifestTransitions(
		current, []*distribution.Manifest{nextManifest}, nextGeneration,
	)
}

// BuildManifestTransitions constructs one unpublished generation by replacing
// multiple distinct distribution manifests in one catalog clone. Publication
// still requires PublishAfter or SaveSnapshotAfter.
func BuildManifestTransitions(
	current *Snapshot,
	nextManifests []*distribution.Manifest,
	nextGeneration uint64,
) (*Snapshot, error) {
	if current == nil || len(nextManifests) == 0 ||
		len(nextManifests) > len(current.config.Manifests) {
		return nil, &CatalogError{Reason: "manifest transition requires current and next snapshots"}
	}
	if nextGeneration <= current.generation {
		return nil, fmt.Errorf(
			"%w: proposed=%d current=%d",
			ErrCatalogGenerationNotNewer, nextGeneration, current.generation,
		)
	}
	config := cloneConfig(current.config)
	for transition := range nextManifests {
		nextManifest := nextManifests[transition]
		if nextManifest == nil {
			return nil, &CatalogError{Reason: "manifest transition requires current and next snapshots"}
		}
		for prior := 0; prior < transition; prior++ {
			if nextManifests[prior].Distribution() == nextManifest.Distribution() {
				return nil, &CatalogError{Reason: fmt.Sprintf(
					"manifest transition repeats distribution %q",
					nextManifest.Distribution(),
				)}
			}
		}
		found := false
		for i := range config.Manifests {
			if config.Manifests[i].Distribution() != nextManifest.Distribution() {
				continue
			}
			config.Manifests[i] = nextManifest
			found = true
			break
		}
		if !found {
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"manifest transition references unknown distribution %q",
				nextManifest.Distribution(),
			)}
		}
	}
	indexes := current.indexDescriptors()
	statistics := current.statistics.Descriptors()
	next, err := NewSnapshotWithPlannerMetadata(
		config, current.endpoints, nextGeneration, indexes, statistics,
	)
	if err != nil {
		return nil, err
	}
	currentState, err := initialCatalogState(current)
	if err != nil {
		return nil, err
	}
	if _, err := advanceCatalogState(currentState, next); err != nil {
		return nil, err
	}
	return next, nil
}

func (s *Snapshot) indexDescriptors() []IndexDescriptor {
	if s == nil || len(s.plannerIndexes) == 0 {
		return nil
	}
	descriptors := make([]IndexDescriptor, 0, len(s.plannerIndexes))
	for tableOrdinal := range s.plannerIndexSpans {
		table := s.config.Placements[s.planner[tableOrdinal].placement].Table
		span := s.plannerIndexSpans[tableOrdinal]
		for i := uint32(0); i < span.count; i++ {
			ordinal := span.first + i
			metadata := s.indexMetadata(table, ordinal)
			descriptor := IndexDescriptor{
				IndexID: metadata.IndexID, Incarnation: metadata.Incarnation,
				Table: metadata.Table, Name: metadata.Name, Relation: metadata.Relation,
				Paths:        make([]string, metadata.PathCount),
				LocatorPaths: make([]string, metadata.LocatorCount),
				Flags:        metadata.Flags, Lifecycle: metadata.Lifecycle,
			}
			copy(descriptor.Paths, metadata.Paths[:metadata.PathCount])
			copy(descriptor.LocatorPaths, metadata.LocatorPaths[:metadata.LocatorCount])
			if metadata.Global() {
				program, _ := s.globalIndexPointerProgram(ordinal)
				descriptor.PrimaryPath = metadata.LocatorPaths[program.primary]
			}
			descriptors = append(descriptors, descriptor)
		}
	}
	return descriptors
}

// These compact active-record directories turn cross-generation validation
// into merge walks. They are planner metadata, not lifetime tombstones: the
// high-waters remain constant-size under churn. Each index reference is one
// four-byte ordinal; plannerIndex already carries its owning placement ordinal.
// Each shard reference is eight bytes and points into its immutable manifest.
type plannerIndexLineageRef uint32

type plannerShardLineageRef struct {
	manifest uint32
	shard    uint32
}

func buildPlannerIndexLineage(
	indexes []plannerIndex,
) []plannerIndexLineageRef {
	if len(indexes) == 0 {
		return nil
	}
	refs := make([]plannerIndexLineageRef, len(indexes))
	for i := range indexes {
		refs[i] = plannerIndexLineageRef(i)
	}
	slices.SortFunc(refs, func(a, b plannerIndexLineageRef) int {
		return cmpUint64(indexes[a].indexID, indexes[b].indexID)
	})
	return refs
}

func buildPlannerShardLineage(config distribution.ClusterConfig) ([]plannerShardLineageRef, error) {
	limit := uint64(^uint32(0))
	if platformLimit := uint64(^uint(0) >> 1); platformLimit < limit {
		limit = platformLimit
	}
	total := uint64(0)
	for _, manifest := range config.Manifests {
		count := uint64(manifest.ShardCount())
		if count > limit || total > limit-count {
			return nil, &CatalogError{Reason: "shard count exceeds compact catalog capacity"}
		}
		total += count
	}
	if total == 0 {
		return nil, nil
	}
	refs := make([]plannerShardLineageRef, 0, int(total))
	for manifestOrdinal, manifest := range config.Manifests {
		for shardOrdinal := 0; shardOrdinal < manifest.ShardCount(); shardOrdinal++ {
			refs = append(refs, plannerShardLineageRef{
				manifest: uint32(manifestOrdinal), shard: uint32(shardOrdinal),
			})
		}
	}
	slices.SortFunc(refs, func(a, b plannerShardLineageRef) int {
		return compareShardLineageRefsInConfig(config, a, config, b)
	})
	return refs, nil
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func (s *Snapshot) indexMetadataForLineage(ref plannerIndexLineageRef) IndexMetadata {
	return s.indexMetadataForOrdinal(uint32(ref))
}

func (s *Snapshot) indexMetadataForOrdinal(ordinal uint32) IndexMetadata {
	entry := s.plannerIndexes[ordinal]
	table := s.config.Placements[entry.placement].Table
	return s.indexMetadata(table, ordinal)
}

const plannerIndexDefinitionMask = plannerIndexFlagsMask | plannerIndexPathCountMask | plannerIndexLocatorCountMask

func samePlannerIndexLogicalIdentity(
	aSnapshot *Snapshot,
	a plannerIndex,
	bSnapshot *Snapshot,
	b plannerIndex,
) bool {
	return aSnapshot.config.Placements[a.placement].Table ==
		bSnapshot.config.Placements[b.placement].Table &&
		aSnapshot.indexName(a.name) == bSnapshot.indexName(b.name)
}

func samePlannerIndexDefinition(
	aSnapshot *Snapshot, aOrdinal uint32,
	a plannerIndex,
	bSnapshot *Snapshot, bOrdinal uint32,
	b plannerIndex,
) bool {
	if a.properties()&plannerIndexDefinitionMask !=
		b.properties()&plannerIndexDefinitionMask ||
		!samePlannerIndexLogicalIdentity(aSnapshot, a, bSnapshot, b) ||
		aSnapshot.indexRelation(aSnapshot.config.Placements[a.placement].Table, aOrdinal, a) !=
			bSnapshot.indexRelation(bSnapshot.config.Placements[b.placement].Table, bOrdinal, b) {
		return false
	}
	pathCount := a.pathCount() + a.locatorCount()
	for i := uint8(0); i < pathCount; i++ {
		if aSnapshot.indexString(aSnapshot.plannerIndexPaths[a.pathBase+uint32(i)]) !=
			bSnapshot.indexString(bSnapshot.plannerIndexPaths[b.pathBase+uint32(i)]) {
			return false
		}
	}
	if a.flags()&IndexGlobal != 0 {
		aProgram, aOK := aSnapshot.globalIndexPointerProgram(aOrdinal)
		bProgram, bOK := bSnapshot.globalIndexPointerProgram(bOrdinal)
		if !aOK || !bOK || aProgram.primary != bProgram.primary {
			return false
		}
	}
	return true
}

func compareShardLineageRefsInConfig(
	aConfig distribution.ClusterConfig,
	a plannerShardLineageRef,
	bConfig distribution.ClusterConfig,
	b plannerShardLineageRef,
) int {
	aManifest := aConfig.Manifests[a.manifest]
	bManifest := bConfig.Manifests[b.manifest]
	if byDistribution := strings.Compare(
		string(aManifest.Distribution()), string(bManifest.Distribution()),
	); byDistribution != 0 {
		return byDistribution
	}
	aMetadata, _ := aManifest.ShardMetadataAt(int(a.shard))
	bMetadata, _ := bManifest.ShardMetadataAt(int(b.shard))
	return strings.Compare(string(aMetadata.ID), string(bMetadata.ID))
}

// Catalog lineage is deliberately constant-size with respect to topology and
// index churn. Index IDs come from one strictly increasing, never-reused
// cluster namespace. Shard IDs are fenced by a per-distribution allocation
// generation high-water: an identity absent from the immediately preceding
// generation is new (or reintroduced) only when its topology allocation is
// above that distribution's lifetime maximum. Raft-derived ownership epochs
// remain comparable only inside one exact shard allocation.

// initialCatalogState validates or synthesizes the lineage carried by one
// independently loaded snapshot. New in-memory snapshots have no lineage yet;
// their active records establish the initial high-waters. Persisted snapshots
// carry explicit lineage and must not contradict their active records.
func initialCatalogState(snapshot *Snapshot) (*Snapshot, error) {
	if snapshot == nil {
		return nil, nil
	}
	indexHighWater, shardHighWaters, err := normalizeCatalogLineage(snapshot)
	if err != nil {
		return nil, err
	}
	return snapshotWithCatalogLineage(snapshot, indexHighWater, shardHighWaters), nil
}

// advanceCatalogState validates every cross-generation routing, index, and
// ownership fence and returns the immutable snapshot to publish. Lineage is
// inherited even when the caller skipped intermediate catalog generations.
func advanceCatalogState(current, next *Snapshot) (*Snapshot, error) {
	if next == nil {
		return nil, &CatalogError{Reason: "next catalog snapshot is nil"}
	}
	if current == nil {
		return initialCatalogState(next)
	}
	if err := validateRoutingTransition(current, next); err != nil {
		return nil, err
	}
	indexHighWater, err := advanceIndexIDHighWater(current, next)
	if err != nil {
		return nil, err
	}
	shardHighWaters, err := advanceShardGenerationHighWaters(current, next)
	if err != nil {
		return nil, err
	}
	return snapshotWithCatalogLineage(next, indexHighWater, shardHighWaters), nil
}

// snapshotWithCatalogLineage copies immutable headers without copying the
// atomic plan-cache field. Active routing/index arrays remain shared and
// immutable. The only lineage allocation is one uint64-equivalent epoch per
// distribution, independent of index/shard churn.
func snapshotWithCatalogLineage(
	snapshot *Snapshot,
	indexHighWater uint64,
	shardHighWaters []distribution.ShardAllocationGeneration,
) *Snapshot {
	out := &Snapshot{
		config:                         snapshot.config,
		endpoints:                      snapshot.endpoints,
		generation:                     snapshot.generation,
		planner:                        snapshot.planner,
		plannerIndexes:                 snapshot.plannerIndexes,
		plannerIndexPaths:              snapshot.plannerIndexPaths,
		plannerIndexRelations:          snapshot.plannerIndexRelations,
		plannerGlobalIndexPrograms:     snapshot.plannerGlobalIndexPrograms,
		plannerGlobalIndexPointers:     snapshot.plannerGlobalIndexPointers,
		plannerGlobalPointerExtraBytes: snapshot.plannerGlobalPointerExtraBytes,
		plannerIndexSpans:              snapshot.plannerIndexSpans,
		plannerIndexStrings:            snapshot.plannerIndexStrings,
		statistics:                     snapshot.statistics,
		indexLineage:                   snapshot.indexLineage,
		shardLineage:                   snapshot.shardLineage,
		indexIDHighWater:               indexHighWater,
		shardGenerationHighWaters:      slices.Clone(shardHighWaters),
		catalogLineagePresent:          true,
		planSeed:                       snapshot.planSeed,
	}
	out.planCache.Store(snapshot.planCache.Load())
	return out
}

func normalizeCatalogLineage(
	snapshot *Snapshot,
) (uint64, []distribution.ShardAllocationGeneration, error) {
	activeIndexHighWater := uint64(0)
	for i := range snapshot.plannerIndexes {
		activeIndexHighWater = max(activeIndexHighWater, snapshot.plannerIndexes[i].indexID)
	}

	distributionOrdinals := make(map[distribution.DistributionName]int, len(snapshot.config.Distributions))
	for i := range snapshot.config.Distributions {
		distributionOrdinals[snapshot.config.Distributions[i].Name] = i
	}
	activeShardHighWaters := make([]distribution.ShardAllocationGeneration, len(snapshot.config.Distributions))
	for _, manifest := range snapshot.config.Manifests {
		ordinal, ok := distributionOrdinals[manifest.Distribution()]
		if !ok {
			return 0, nil, &CatalogError{Reason: fmt.Sprintf(
				"manifest references unknown distribution %q", manifest.Distribution())}
		}
		for i := 0; i < manifest.ShardCount(); i++ {
			metadata, _ := manifest.ShardMetadataAt(i)
			activeShardHighWaters[ordinal] = max(
				activeShardHighWaters[ordinal], metadata.AllocationGeneration,
			)
		}
	}

	if !snapshot.catalogLineagePresent {
		return activeIndexHighWater, activeShardHighWaters, nil
	}
	if snapshot.indexIDHighWater < activeIndexHighWater {
		return 0, nil, &CatalogError{Reason: "index id high-water is below an active index id"}
	}
	if len(snapshot.shardGenerationHighWaters) != len(snapshot.config.Distributions) {
		return 0, nil, &CatalogError{Reason: "shard allocation high-water directory has the wrong length"}
	}
	for i := range activeShardHighWaters {
		if snapshot.shardGenerationHighWaters[i] < activeShardHighWaters[i] {
			return 0, nil, &CatalogError{Reason: fmt.Sprintf(
				"distribution %q shard allocation high-water is below an active generation",
				snapshot.config.Distributions[i].Name)}
		}
	}
	return snapshot.indexIDHighWater, slices.Clone(snapshot.shardGenerationHighWaters), nil
}

func advanceIndexIDHighWater(current, next *Snapshot) (uint64, error) {
	if next.catalogLineagePresent && next.indexIDHighWater < current.indexIDHighWater {
		return 0, &CatalogError{Reason: "index id high-water regressed"}
	}
	highWater := current.indexIDHighWater
	if next.catalogLineagePresent {
		highWater = max(highWater, next.indexIDHighWater)
	}

	currentOrdinal := 0
	for _, nextRef := range next.indexLineage {
		nextEntry := next.plannerIndexes[nextRef]
		for currentOrdinal < len(current.indexLineage) &&
			current.plannerIndexes[current.indexLineage[currentOrdinal]].indexID < nextEntry.indexID {
			currentOrdinal++
		}
		active := currentOrdinal < len(current.indexLineage) &&
			current.plannerIndexes[current.indexLineage[currentOrdinal]].indexID == nextEntry.indexID
		var oldEntry plannerIndex
		if active {
			oldEntry = current.plannerIndexes[current.indexLineage[currentOrdinal]]
		}
		switch {
		case active && nextEntry.incarnation == oldEntry.incarnation:
			if !samePlannerIndexDefinition(
				current, uint32(current.indexLineage[currentOrdinal]), oldEntry,
				next, uint32(nextRef), nextEntry,
			) ||
				nextEntry.lifecycle() < oldEntry.lifecycle() {
				return 0, &CatalogError{Reason: fmt.Sprintf(
					"index %d changed definition or regressed lifecycle within one incarnation",
					nextEntry.indexID)}
			}
		case active && nextEntry.incarnation > oldEntry.incarnation:
			if !samePlannerIndexLogicalIdentity(current, oldEntry, next, nextEntry) {
				return 0, &CatalogError{Reason: fmt.Sprintf(
					"index %d replacement changed logical identity", nextEntry.indexID)}
			}
		case active:
			return 0, &CatalogError{Reason: fmt.Sprintf(
				"index %d incarnation regressed", nextEntry.indexID)}
		case nextEntry.indexID <= current.indexIDHighWater:
			return 0, &CatalogError{Reason: fmt.Sprintf(
				"index id %d was reused below the lifetime high-water %d",
				nextEntry.indexID, current.indexIDHighWater)}
		}
		highWater = max(highWater, nextEntry.indexID)
	}
	return highWater, nil
}

func advanceShardGenerationHighWaters(
	current, next *Snapshot,
) ([]distribution.ShardAllocationGeneration, error) {
	if next.catalogLineagePresent && len(next.shardGenerationHighWaters) != len(next.config.Distributions) {
		return nil, &CatalogError{Reason: "shard allocation high-water directory has the wrong length"}
	}

	currentDistributionOrdinals := make(map[distribution.DistributionName]int, len(current.config.Distributions))
	for i := range current.config.Distributions {
		currentDistributionOrdinals[current.config.Distributions[i].Name] = i
	}
	nextDistributionOrdinals := make(map[distribution.DistributionName]int, len(next.config.Distributions))
	for i := range next.config.Distributions {
		nextDistributionOrdinals[next.config.Distributions[i].Name] = i
	}

	highWaters := make([]distribution.ShardAllocationGeneration, len(next.config.Distributions))
	for i := range next.config.Distributions {
		name := next.config.Distributions[i].Name
		if currentOrdinal, exists := currentDistributionOrdinals[name]; exists {
			highWaters[i] = current.shardGenerationHighWaters[currentOrdinal]
			if next.catalogLineagePresent && next.shardGenerationHighWaters[i] < highWaters[i] {
				return nil, &CatalogError{Reason: fmt.Sprintf(
					"distribution %q shard allocation high-water regressed", name)}
			}
		}
		if next.catalogLineagePresent {
			highWaters[i] = max(highWaters[i], next.shardGenerationHighWaters[i])
		}
	}

	currentShardOrdinal := 0
	for _, nextRef := range next.shardLineage {
		manifest := next.config.Manifests[nextRef.manifest]
		distributionName := manifest.Distribution()
		nextDistributionOrdinal, ok := nextDistributionOrdinals[distributionName]
		if !ok {
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"manifest references unknown distribution %q", distributionName)}
		}
		currentDistributionOrdinal, distributionExisted := currentDistributionOrdinals[distributionName]
		var priorHighWater distribution.ShardAllocationGeneration
		if distributionExisted {
			priorHighWater = current.shardGenerationHighWaters[currentDistributionOrdinal]
		}
		metadata, _ := manifest.ShardMetadataAt(int(nextRef.shard))
		for currentShardOrdinal < len(current.shardLineage) &&
			compareShardLineageRefsInConfig(
				current.config, current.shardLineage[currentShardOrdinal], next.config, nextRef,
			) < 0 {
			currentShardOrdinal++
		}
		active := currentShardOrdinal < len(current.shardLineage) &&
			compareShardLineageRefsInConfig(
				current.config, current.shardLineage[currentShardOrdinal], next.config, nextRef,
			) == 0
		var oldManifest *distribution.Manifest
		var oldMetadata distribution.ShardMetadata
		var oldRef plannerShardLineageRef
		if active {
			oldRef = current.shardLineage[currentShardOrdinal]
			oldManifest = current.config.Manifests[oldRef.manifest]
			oldMetadata, _ = oldManifest.ShardMetadataAt(int(oldRef.shard))
		}
		switch {
		case active && metadata.AllocationGeneration != oldMetadata.AllocationGeneration:
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"shard %q/%q changed allocation generation without a lifecycle transition",
				distributionName, metadata.ID)}
		case active && metadata.Epoch < oldMetadata.Epoch:
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"shard %q/%q ownership epoch regressed", distributionName, metadata.ID)}
		case active && (metadata.Range != oldMetadata.Range ||
			!oldManifest.SameShardLeaders(int(oldRef.shard), manifest, int(nextRef.shard))) &&
			metadata.Epoch <= oldMetadata.Epoch:
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"shard %q/%q changed ownership without a higher epoch",
				distributionName, metadata.ID)}
		case !active && distributionExisted && metadata.AllocationGeneration <= priorHighWater:
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"new or reintroduced shard %q/%q allocation generation %d is not above lifetime high-water %d",
				distributionName, metadata.ID, metadata.AllocationGeneration, priorHighWater)}
		}
		if next.catalogLineagePresent &&
			next.shardGenerationHighWaters[nextDistributionOrdinal] < metadata.AllocationGeneration {
			return nil, &CatalogError{Reason: fmt.Sprintf(
				"distribution %q shard allocation high-water is below an active generation",
				distributionName)}
		}
		highWaters[nextDistributionOrdinal] = max(
			highWaters[nextDistributionOrdinal], metadata.AllocationGeneration,
		)
	}
	return highWaters, nil
}

// validateRoutingTransition is O(distributions + placements + manifests).
// Planner tables are already sorted by table, so surviving placements are
// compared with a merge walk rather than repeated linear catalog lookups.
func validateRoutingTransition(current, next *Snapshot) error {
	nextSpecs := make(map[distribution.DistributionName]distribution.DistributionSpec, len(next.config.Distributions))
	for i := range next.config.Distributions {
		nextSpecs[next.config.Distributions[i].Name] = next.config.Distributions[i]
	}
	for i := range current.config.Distributions {
		old := current.config.Distributions[i]
		candidate, exists := nextSpecs[old.Name]
		if !exists {
			return &CatalogError{Reason: fmt.Sprintf(
				"distribution %q was removed without an incarnation protocol", old.Name)}
		}
		if candidate.Name != old.Name || candidate.Arity != old.Arity ||
			candidate.MapperVersion != old.MapperVersion ||
			candidate.EffectiveBucketBits() != old.EffectiveBucketBits() {
			return &CatalogError{Reason: fmt.Sprintf(
				"distribution %q changed immutable mapper identity", old.Name)}
		}
	}

	nextOrdinal := 0
	for _, oldEntry := range current.planner {
		old := current.config.Placements[oldEntry.placement]
		for nextOrdinal < len(next.planner) {
			candidate := next.config.Placements[next.planner[nextOrdinal].placement]
			if candidate.Table >= old.Table {
				break
			}
			nextOrdinal++
		}
		if nextOrdinal == len(next.planner) {
			return &CatalogError{Reason: fmt.Sprintf(
				"placement for table %q was removed without an incarnation protocol", old.Table)}
		}
		candidate := next.config.Placements[next.planner[nextOrdinal].placement]
		if candidate.Table != old.Table {
			return &CatalogError{Reason: fmt.Sprintf(
				"placement for table %q was removed without an incarnation protocol", old.Table)}
		}
		if candidate.Distribution != old.Distribution ||
			candidate.TenantPath != old.TenantPath ||
			candidate.AffinityGroup != old.AffinityGroup ||
			!slices.Equal(candidate.Columns, old.Columns) {
			return &CatalogError{Reason: fmt.Sprintf(
				"placement for table %q changed immutable routing identity", old.Table)}
		}
		nextOrdinal++
	}

	nextManifests := make(map[distribution.DistributionName]*distribution.Manifest, len(next.config.Manifests))
	for _, manifest := range next.config.Manifests {
		nextManifests[manifest.Distribution()] = manifest
	}
	for _, old := range current.config.Manifests {
		candidate, exists := nextManifests[old.Distribution()]
		if !exists {
			return &CatalogError{Reason: fmt.Sprintf(
				"manifest for distribution %q was removed", old.Distribution())}
		}
		if candidate.Version() < old.Version() {
			return &CatalogError{Reason: fmt.Sprintf(
				"manifest for distribution %q regressed routing version", old.Distribution())}
		}
		if candidate.Version() == old.Version() && !old.Equal(candidate) {
			return &CatalogError{Reason: fmt.Sprintf(
				"manifest for distribution %q changed within one routing version", old.Distribution())}
		}
	}
	return nil
}
