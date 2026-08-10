package gateway

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	vibejson "github.com/thesyncim/vibejson"
)

// IndexFlags describe the physical and access-path properties certified by an
// authoritative catalog generation. IndexCovering only certifies that the
// ordered key paths can serve index-only reads when they cover a projection;
// this first descriptor does not model or certify a separate INCLUDE payload.
type IndexFlags uint8

const (
	IndexLocal IndexFlags = 1 << iota
	IndexUnique
	IndexCovering
	IndexOrdered
)

const allIndexFlags = IndexLocal | IndexUnique | IndexCovering | IndexOrdered

// IndexLifecycle is the catalog-controlled publication state of an index
// incarnation. Only Ready is eligible for ordinary planning; the other states
// let build and retirement protocols publish their progress without making a
// partially built or draining structure look usable.
type IndexLifecycle uint8

const (
	IndexBuilding IndexLifecycle = iota + 1
	IndexCatchingUp
	IndexReady
	IndexDraining
)

func (l IndexLifecycle) valid() bool {
	return l >= IndexBuilding && l <= IndexDraining
}

// IndexDescriptor is the cold-path catalog input for one distributed index.
// IndexID is globally unique for the cluster lifetime and is never reused,
// including after the descriptor disappears. It is stable across lifecycle
// changes. Incarnation strictly increases whenever the physical index is
// replaced, fencing plans prepared against retired storage. Paths are ordered
// RFC 6901 JSON pointers and contain between one and four entries.
type IndexDescriptor struct {
	IndexID     uint64
	Incarnation uint64
	Table       string
	Name        string
	Paths       []string
	Flags       IndexFlags
	Lifecycle   IndexLifecycle
}

// IndexMetadata is the allocation-free public view of one compact descriptor.
// Only the first PathCount entries of Paths are populated. The strings are
// immutable views into the snapshot's single index string arena.
type IndexMetadata struct {
	IndexID     uint64
	Incarnation uint64
	Table       string
	Name        string
	Paths       [4]string
	PathCount   uint8
	Flags       IndexFlags
	Lifecycle   IndexLifecycle
}

// Ready reports whether this exact incarnation is eligible for planning.
func (m IndexMetadata) Ready() bool { return m.Lifecycle == IndexReady }

type plannerStringRef struct {
	offset uint32
	length uint32
}

const plannerStringRefBytes = unsafe.Sizeof(plannerStringRef{})

var (
	_ [8 - plannerStringRefBytes]byte
	_ [plannerStringRefBytes - 8]byte
)

// plannerIndex is deliberately 32 bytes. The owning placement ordinal is
// retained in the record so the ID-sorted transition directory needs only one
// uint32 ordinal per active index. Name length and the remaining small
// properties are packed beside its arena offset without narrowing their
// validated domains.
type plannerIndexNameRef struct {
	offset           uint32
	lengthProperties uint32
}

type plannerIndex struct {
	indexID     uint64
	incarnation uint64
	name        plannerIndexNameRef
	pathBase    uint32
	placement   uint32
}

const (
	plannerIndexFlagsMask      = uint16((1 << 4) - 1)
	plannerIndexLifecycleShift = 4
	plannerIndexLifecycleMask  = uint16((1<<3)-1) << plannerIndexLifecycleShift
	plannerIndexPathCountShift = 7
	plannerIndexPathCountMask  = uint16((1<<3)-1) << plannerIndexPathCountShift
)

func newPlannerIndex(
	indexID, incarnation uint64,
	name plannerStringRef,
	pathBase, placement uint32,
	flags IndexFlags,
	lifecycle IndexLifecycle,
	pathCount uint8,
) plannerIndex {
	properties := uint16(flags) |
		uint16(lifecycle)<<plannerIndexLifecycleShift |
		uint16(pathCount)<<plannerIndexPathCountShift
	return plannerIndex{
		indexID: indexID, incarnation: incarnation,
		name: plannerIndexNameRef{
			offset: name.offset,
			lengthProperties: name.length |
				uint32(properties)<<16,
		},
		pathBase: pathBase, placement: placement,
	}
}

func (p plannerIndex) properties() uint16 {
	return uint16(p.name.lengthProperties >> 16)
}

func (p plannerIndex) flags() IndexFlags {
	return IndexFlags(p.properties() & plannerIndexFlagsMask)
}

func (p plannerIndex) lifecycle() IndexLifecycle {
	return IndexLifecycle((p.properties() & plannerIndexLifecycleMask) >> plannerIndexLifecycleShift)
}

func (p plannerIndex) pathCount() uint8 {
	return uint8((p.properties() & plannerIndexPathCountMask) >> plannerIndexPathCountShift)
}

const plannerIndexBytes = unsafe.Sizeof(plannerIndex{})

var (
	_ [32 - plannerIndexBytes]byte
	_ [plannerIndexBytes - 32]byte
)

type plannerIndexSpan struct {
	first uint32
	count uint32
}

// IndexSet is an immutable, allocation-free view of one table's index run.
// It is valid for as long as its Snapshot is reachable.
type IndexSet struct {
	snapshot *Snapshot
	span     plannerIndexSpan
	table    string
}

// Len returns the number of indexes in the table's name-sorted run.
func (s IndexSet) Len() int { return int(s.span.count) }

// At returns the index at ordinal in name order.
func (s IndexSet) At(ordinal int) (IndexMetadata, bool) {
	if s.snapshot == nil || ordinal < 0 || ordinal >= int(s.span.count) {
		return IndexMetadata{}, false
	}
	return s.snapshot.indexMetadata(s.table, s.span.first+uint32(ordinal)), true
}

// Lookup finds name in this table run with a binary search.
func (s IndexSet) Lookup(name string) (IndexMetadata, bool) {
	if s.snapshot == nil {
		return IndexMetadata{}, false
	}
	lo, hi := uint32(0), s.span.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		candidate := s.snapshot.indexName(s.snapshot.plannerIndexes[s.span.first+mid].name)
		if candidate < name {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == s.span.count {
		return IndexMetadata{}, false
	}
	entry := s.snapshot.plannerIndexes[s.span.first+lo]
	if s.snapshot.indexName(entry.name) != name {
		return IndexMetadata{}, false
	}
	return s.snapshot.indexMetadata(s.table, s.span.first+lo), true
}

// LookupIncarnation is a storage-identity fence: it returns metadata only when
// the logical name still denotes the exact ID and incarnation the caller
// pinned. A dropped/recreated index therefore cannot satisfy a stale plan.
func (s IndexSet) LookupIncarnation(name string, indexID, incarnation uint64) (IndexMetadata, bool) {
	metadata, ok := s.Lookup(name)
	if !ok || metadata.IndexID != indexID || metadata.Incarnation != incarnation {
		return IndexMetadata{}, false
	}
	return metadata, true
}

// Indexes returns the name-sorted immutable index run for table. Missing and
// index-free tables return an empty set. The operation allocates nothing.
func (s *Snapshot) Indexes(table string) IndexSet {
	if s == nil || len(s.plannerIndexSpans) == 0 {
		return IndexSet{}
	}
	ordinal, ok := s.plannerTableOrdinal(table)
	if !ok {
		return IndexSet{}
	}
	canonical := s.config.Placements[s.planner[ordinal].placement].Table
	return IndexSet{snapshot: s, span: s.plannerIndexSpans[ordinal], table: canonical}
}

// Index resolves one index by table and name without allocation.
func (s *Snapshot) Index(table, name string) (IndexMetadata, bool) {
	return s.Indexes(table).Lookup(name)
}

// IndexIncarnation resolves an index only if its complete physical identity
// matches, fencing stale plans across drop/recreate and rebuild publication.
func (s *Snapshot) IndexIncarnation(table, name string, indexID, incarnation uint64) (IndexMetadata, bool) {
	return s.Indexes(table).LookupIncarnation(name, indexID, incarnation)
}

// PlannerIndexMetadataBytes reports every retained byte owned exclusively by
// the compact planner index directory: 32 bytes per index, 8 bytes per path
// reference, 8 bytes per aligned table span, and the exact interned string-arena
// length. The separate cold catalog-transition directory is reported by
// CatalogTransitionMetadataBytes.
func (s *Snapshot) PlannerIndexMetadataBytes() uint64 {
	if s == nil {
		return 0
	}
	return uint64(cap(s.plannerIndexes))*uint64(unsafe.Sizeof(plannerIndex{})) +
		uint64(cap(s.plannerIndexPaths))*uint64(unsafe.Sizeof(plannerStringRef{})) +
		uint64(cap(s.plannerIndexSpans))*uint64(unsafe.Sizeof(plannerIndexSpan{})) +
		uint64(len(s.plannerIndexStrings))
}

func (s *Snapshot) plannerTableOrdinal(table string) (uint32, bool) {
	lo, hi := 0, len(s.planner)
	for lo < hi {
		mid := lo + (hi-lo)/2
		candidate := s.config.Placements[s.planner[mid].placement].Table
		if candidate < table {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(s.planner) || s.config.Placements[s.planner[lo].placement].Table != table {
		return 0, false
	}
	return uint32(lo), true
}

func (s *Snapshot) indexString(ref plannerStringRef) string {
	return s.plannerIndexStrings[ref.offset : ref.offset+ref.length]
}

func (s *Snapshot) indexName(ref plannerIndexNameRef) string {
	length := ref.lengthProperties & uint32(math.MaxUint16)
	return s.plannerIndexStrings[ref.offset : ref.offset+length]
}

func (s *Snapshot) indexMetadata(table string, ordinal uint32) IndexMetadata {
	entry := s.plannerIndexes[ordinal]
	return s.indexMetadataFromEntry(table, entry)
}

func (s *Snapshot) indexMetadataFromEntry(table string, entry plannerIndex) IndexMetadata {
	pathCount := entry.pathCount()
	metadata := IndexMetadata{
		IndexID: entry.indexID, Incarnation: entry.incarnation,
		Table: table, Name: s.indexName(entry.name), PathCount: pathCount,
		Flags: entry.flags(), Lifecycle: entry.lifecycle(),
	}
	for i := uint8(0); i < pathCount; i++ {
		metadata.Paths[i] = s.indexString(s.plannerIndexPaths[entry.pathBase+uint32(i)])
	}
	return metadata
}

type plannerIndexBuild struct {
	indexes []plannerIndex
	paths   []plannerStringRef
	spans   []plannerIndexSpan
	arena   string
}

func validateCompactPlannerDimensions(config distribution.ClusterConfig) error {
	for kind, count := range map[string]int{
		"distribution": len(config.Distributions),
		"placement":    len(config.Placements),
		"manifest":     len(config.Manifests),
	} {
		if err := validateCompactPlannerCount(kind, uint64(count)); err != nil {
			return err
		}
	}
	return nil
}

func validateCompactPlannerCount(kind string, count uint64) error {
	if count > uint64(math.MaxUint32) {
		return &CatalogError{Reason: kind + " count exceeds compact planner capacity"}
	}
	return nil
}

func (s *Snapshot) forEachIndex(visit func(IndexMetadata)) {
	if s == nil || visit == nil {
		return
	}
	for tableOrdinal, span := range s.plannerIndexSpans {
		table := s.config.Placements[s.planner[tableOrdinal].placement].Table
		for i := uint32(0); i < span.count; i++ {
			visit(s.indexMetadata(table, span.first+i))
		}
	}
}

func buildPlannerIndexes(config distribution.ClusterConfig, planner []plannerTable, descriptors []IndexDescriptor) (plannerIndexBuild, error) {
	if len(descriptors) == 0 {
		return plannerIndexBuild{}, nil
	}
	if uint64(len(descriptors)) > uint64(math.MaxUint32) {
		return plannerIndexBuild{}, &CatalogError{Reason: "index count exceeds compact catalog capacity"}
	}

	placements := make(map[string]distribution.TablePlacement, len(config.Placements))
	for i := range config.Placements {
		placements[config.Placements[i].Table] = config.Placements[i]
	}
	ids := make(map[uint64]string, len(descriptors))
	names := make(map[string]struct{}, len(descriptors))
	pathCount := uint64(0)
	for i := range descriptors {
		d := &descriptors[i]
		if err := validateIndexCatalogName("table", d.Table); err != nil {
			return plannerIndexBuild{}, indexCatalogError(d, err.Error())
		}
		placement, ok := placements[d.Table]
		if !ok {
			return plannerIndexBuild{}, indexCatalogError(d, "table has no placement")
		}
		if d.IndexID == 0 {
			return plannerIndexBuild{}, indexCatalogError(d, "index id is zero")
		}
		if previous, duplicate := ids[d.IndexID]; duplicate {
			return plannerIndexBuild{}, indexCatalogError(d, fmt.Sprintf("index id %d is also used by %s", d.IndexID, previous))
		}
		ids[d.IndexID] = d.Table + "." + d.Name
		if d.Incarnation == 0 {
			return plannerIndexBuild{}, indexCatalogError(d, "incarnation is zero")
		}
		if err := validateIndexCatalogName("name", d.Name); err != nil {
			return plannerIndexBuild{}, indexCatalogError(d, err.Error())
		}
		nameKey := d.Table + "\x00" + d.Name
		if _, duplicate := names[nameKey]; duplicate {
			return plannerIndexBuild{}, indexCatalogError(d, "duplicate index name on table")
		}
		names[nameKey] = struct{}{}
		if d.Flags&^allIndexFlags != 0 {
			return plannerIndexBuild{}, indexCatalogError(d, fmt.Sprintf("unknown flags %#x", d.Flags&^allIndexFlags))
		}
		if d.Flags&IndexUnique != 0 && d.Flags&IndexLocal == 0 {
			return plannerIndexBuild{}, indexCatalogError(d,
				"global uniqueness is not certified; a unique index must be local")
		}
		if d.Flags&IndexLocal == 0 {
			return plannerIndexBuild{}, indexCatalogError(d,
				"only shard-local indexes are supported")
		}
		if !d.Lifecycle.valid() {
			return plannerIndexBuild{}, indexCatalogError(d, fmt.Sprintf("invalid lifecycle %d", d.Lifecycle))
		}
		if len(d.Paths) < 1 || len(d.Paths) > 4 {
			return plannerIndexBuild{}, indexCatalogError(d, "path count must be in [1,4]")
		}
		pathCount += uint64(len(d.Paths))
		if pathCount > uint64(math.MaxUint32) {
			return plannerIndexBuild{}, indexCatalogError(d, "path count exceeds compact catalog capacity")
		}
		for pathIndex, path := range d.Paths {
			if !utf8.ValidString(path) {
				return plannerIndexBuild{}, indexCatalogError(d, fmt.Sprintf("path %d is not valid UTF-8", pathIndex))
			}
			if _, err := vibejson.CompilePointer(path); err != nil {
				return plannerIndexBuild{}, indexCatalogError(d, fmt.Sprintf("path %d %q is invalid: %v", pathIndex, path, err))
			}
			for prior := range pathIndex {
				if d.Paths[prior] == path {
					return plannerIndexBuild{}, indexCatalogError(d, fmt.Sprintf("path %q is repeated", path))
				}
			}
		}
		if d.Flags&(IndexLocal|IndexUnique) == IndexLocal|IndexUnique {
			for _, shardPath := range placement.Columns {
				if !slices.Contains(d.Paths, shardPath) {
					return plannerIndexBuild{}, indexCatalogError(d,
						fmt.Sprintf("local unique index does not contain shard-key path %q", shardPath))
				}
			}
		}
	}
	maxInt := int(^uint(0) >> 1)
	if pathCount > uint64(maxInt) || int(pathCount) > maxInt-len(descriptors) {
		return plannerIndexBuild{}, &CatalogError{Reason: "index metadata exceeds platform allocation capacity"}
	}
	pathCountInt := int(pathCount)
	arenaLimit := uint64(math.MaxUint32)
	if uint64(maxInt) < arenaLimit {
		arenaLimit = uint64(maxInt)
	}

	ordered := slices.Clone(descriptors)
	slices.SortFunc(ordered, func(a, b IndexDescriptor) int {
		if byTable := strings.Compare(a.Table, b.Table); byTable != 0 {
			return byTable
		}
		return strings.Compare(a.Name, b.Name)
	})

	// Interning exists only while the snapshot is built. The retained form is
	// one exact-size string plus fixed-width references, never one map per table
	// or index.
	refs := make(map[string]plannerStringRef, len(descriptors)+pathCountInt)
	arenaBytes := make([]byte, 0)
	intern := func(value string) (plannerStringRef, error) {
		if ref, ok := refs[value]; ok {
			return ref, nil
		}
		if uint64(len(arenaBytes))+uint64(len(value)) > arenaLimit {
			return plannerStringRef{}, &CatalogError{Reason: "index string arena exceeds compact catalog capacity"}
		}
		ref := plannerStringRef{offset: uint32(len(arenaBytes)), length: uint32(len(value))}
		arenaBytes = append(arenaBytes, value...)
		refs[value] = ref
		return ref, nil
	}

	build := plannerIndexBuild{
		indexes: make([]plannerIndex, len(ordered)),
		paths:   make([]plannerStringRef, pathCountInt),
		spans:   make([]plannerIndexSpan, len(planner)),
	}
	tableOrdinals := make(map[string]uint32, len(planner))
	for i := range planner {
		tableOrdinals[config.Placements[planner[i].placement].Table] = uint32(i)
	}
	pathBase := uint32(0)
	for i := range ordered {
		d := &ordered[i]
		name, err := intern(d.Name)
		if err != nil {
			return plannerIndexBuild{}, err
		}
		tableOrdinal := tableOrdinals[d.Table]
		entry := newPlannerIndex(
			d.IndexID, d.Incarnation, name, pathBase,
			planner[tableOrdinal].placement,
			d.Flags, d.Lifecycle, uint8(len(d.Paths)),
		)
		for _, path := range d.Paths {
			ref, err := intern(path)
			if err != nil {
				return plannerIndexBuild{}, err
			}
			build.paths[pathBase] = ref
			pathBase++
		}
		build.indexes[i] = entry
		span := &build.spans[tableOrdinal]
		if span.count == 0 {
			span.first = uint32(i)
		}
		span.count++
	}
	build.arena = string(arenaBytes)
	return build, nil
}

func validateIndexCatalogName(kind, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("index %s is empty", kind)
	case !utf8.ValidString(name):
		return fmt.Errorf("index %s is not valid UTF-8", kind)
	case len(name) > math.MaxUint16:
		return fmt.Errorf("index %s is %d bytes, maximum is %d", kind, len(name), math.MaxUint16)
	case strings.IndexByte(name, 0) >= 0:
		return fmt.Errorf("index %s contains NUL", kind)
	default:
		return nil
	}
}

func indexCatalogError(d *IndexDescriptor, reason string) error {
	identity := d.Table + "." + d.Name
	if d.Table == "" && d.Name == "" {
		identity = "descriptor"
	}
	return &CatalogError{Reason: "index " + identity + ": " + reason}
}
