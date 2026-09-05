package gateway

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// ReplicatedTableProfile is the portable base-relation cut needed to turn a
// catalog placement into native RF3 reads and writes. It deliberately carries
// no executor tuning: these fields are durable schema identity, while batching
// and command capacities remain admission policy.
// LogicalSchemaDigest is shared across shard allocations of one schema;
// the per-shard machine validation digest stays in the route's Command.
type ReplicatedTableProfile struct {
	Table               string
	Relation            replication.RelationID
	PrimaryKey          string
	SchemaGeneration    uint64
	LogicalSchemaDigest replication.Digest
	MaxKeyBytes         uint16
	MaxDocumentBytes    uint32
}

// ResolvedReplicatedTableKey is one exact, fenced base-table destination.
// Profile strings refer to immutable catalog storage and remain safe after the
// call; Route.Replicas occupies the caller-provided scratch.
type ResolvedReplicatedTableKey struct {
	Profile ReplicatedTableProfile
	Route   ReplicatedRoute
	RouteID replication.Digest
	Point   distribution.KeyspacePoint
}

// replicatedCatalogTable is the compact hot directory. The table and primary
// strings already live in ClusterConfig; retaining one sorted planner ordinal
// avoids a map and duplicate string headers per replicated table.
type replicatedCatalogTable struct {
	digest      replication.Digest
	mapper      *distribution.NativeMapper
	schema      uint64
	planner     uint32
	maxDocument uint32
	maxKey      uint16
	relation    replication.RelationID
}

type unresolvedReplicatedCatalogTable struct {
	profile ReplicatedTableProfile
	planner uint32
}

type replicatedRelationSlot struct {
	distribution distribution.DistributionName
	relation     replication.RelationID
}

func (snapshot *Snapshot) attachReplicatedTableProfiles(
	profiles []ReplicatedTableProfile,
) error {
	if snapshot == nil || len(profiles) > len(snapshot.planner) ||
		uint64(len(profiles)) > uint64(^uint32(0)) {
		return &CatalogError{Reason: "replicated table directory exceeds its bound"}
	}
	if len(profiles) == 0 {
		return nil
	}
	unresolved := make([]unresolvedReplicatedCatalogTable, len(profiles))
	relationSlots := make(map[replicatedRelationSlot]struct{}, len(profiles))
	for ordinal := range profiles {
		profile := profiles[ordinal]
		plannerOrdinal, found := snapshot.plannerTableOrdinal(profile.Table)
		if !found || plannerOrdinal < 0 || uint64(plannerOrdinal) > uint64(^uint32(0)) {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated table %q has no placement", profile.Table,
			)}
		}
		entry := snapshot.planner[plannerOrdinal]
		placement := snapshot.config.Placements[entry.placement]
		primary, err := vibejson.CompilePointer(profile.PrimaryKey)
		if !collectionname.Valid(profile.Table) || strings.IndexByte(profile.Table, 0) >= 0 ||
			profile.Relation == 0 || profile.Relation > replication.MaxRelationID ||
			len(profile.PrimaryKey) == 0 || len(profile.PrimaryKey) > replication.MaxIdentityBytes ||
			err != nil || len(primary.Tokens) == 0 || primary.String() != profile.PrimaryKey ||
			len(placement.Columns) != 1 || placement.Columns[0] != profile.PrimaryKey ||
			profile.SchemaGeneration == 0 ||
			profile.LogicalSchemaDigest == (replication.Digest{}) ||
			profile.MaxKeyBytes == 0 || int(profile.MaxKeyBytes) > replication.MaxMutationKeyBytes ||
			profile.MaxDocumentBytes == 0 ||
			uint64(profile.MaxDocumentBytes) > uint64(replication.MaxMutationValueBytes) {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated table %q does not satisfy the current base-relation contract",
				profile.Table,
			)}
		}
		slot := replicatedRelationSlot{
			distribution: placement.Distribution,
			relation:     profile.Relation,
		}
		if _, duplicate := relationSlots[slot]; duplicate {
			return &CatalogError{Reason: fmt.Sprintf(
				"distribution %q assigns relation %d to more than one table",
				placement.Distribution, profile.Relation,
			)}
		}
		relationSlots[slot] = struct{}{}
		manifest := snapshot.config.Manifests[entry.manifest]
		for shardOrdinal := 0; shardOrdinal < manifest.ShardCount(); shardOrdinal++ {
			metadata, _ := manifest.ShardMetadataAt(shardOrdinal)
			route, ok := snapshot.replicatedShardAt(placement.Distribution, metadata.ID)
			if !ok || route.command.SchemaGeneration != profile.SchemaGeneration ||
				route.logicalSchema != profile.LogicalSchemaDigest {
				return &CatalogError{Reason: fmt.Sprintf(
					"replicated table %q does not match RF3 shard %q/%q",
					profile.Table, placement.Distribution, metadata.ID,
				)}
			}
		}
		unresolved[ordinal] = unresolvedReplicatedCatalogTable{
			profile: profile, planner: uint32(plannerOrdinal),
		}
	}
	slices.SortFunc(unresolved, func(left, right unresolvedReplicatedCatalogTable) int {
		leftTable := snapshot.config.Placements[snapshot.planner[left.planner].placement].Table
		rightTable := snapshot.config.Placements[snapshot.planner[right.planner].placement].Table
		switch {
		case leftTable < rightTable:
			return -1
		case leftTable > rightTable:
			return 1
		default:
			return 0
		}
	})
	tables := make([]replicatedCatalogTable, len(unresolved))
	for ordinal := range unresolved {
		if ordinal != 0 && unresolved[ordinal-1].planner == unresolved[ordinal].planner {
			return &CatalogError{Reason: "duplicate replicated table profile"}
		}
		profile := unresolved[ordinal].profile
		planner := snapshot.planner[unresolved[ordinal].planner]
		spec := snapshot.config.Distributions[planner.spec]
		tables[ordinal] = replicatedCatalogTable{
			digest: profile.LogicalSchemaDigest, schema: profile.SchemaGeneration,
			planner: unresolved[ordinal].planner, maxDocument: profile.MaxDocumentBytes,
			maxKey: profile.MaxKeyBytes, relation: profile.Relation,
			mapper: distribution.NewNativeMapperWithBucketBits(1, spec.EffectiveBucketBits()),
		}
	}
	snapshot.replicatedTables = tables
	return nil
}

func (snapshot *Snapshot) replicatedTableProfileAt(
	entry replicatedCatalogTable,
) (ReplicatedTableProfile, bool) {
	if snapshot == nil || int(entry.planner) >= len(snapshot.planner) {
		return ReplicatedTableProfile{}, false
	}
	planner := snapshot.planner[entry.planner]
	if int(planner.placement) >= len(snapshot.config.Placements) {
		return ReplicatedTableProfile{}, false
	}
	placement := snapshot.config.Placements[planner.placement]
	if len(placement.Columns) != 1 {
		return ReplicatedTableProfile{}, false
	}
	return ReplicatedTableProfile{
		Table: placement.Table, Relation: entry.relation, PrimaryKey: placement.Columns[0],
		SchemaGeneration: entry.schema, LogicalSchemaDigest: entry.digest,
		MaxKeyBytes: entry.maxKey, MaxDocumentBytes: entry.maxDocument,
	}, true
}

func compareBytesString(left []byte, right string) int {
	limit := min(len(left), len(right))
	for index := 0; index < limit; index++ {
		rightByte := right[index]
		if left[index] < rightByte {
			return -1
		}
		if left[index] > rightByte {
			return 1
		}
	}
	switch {
	case len(left) < len(right):
		return -1
	case len(left) > len(right):
		return 1
	default:
		return 0
	}
}

func (snapshot *Snapshot) replicatedTableAtBytes(
	table []byte,
) (replicatedCatalogTable, bool) {
	if snapshot == nil || len(table) == 0 {
		return replicatedCatalogTable{}, false
	}
	low, high := 0, len(snapshot.replicatedTables)
	for low < high {
		middle := int(uint(low+high) >> 1)
		entry := snapshot.replicatedTables[middle]
		profile, ok := snapshot.replicatedTableProfileAt(entry)
		if !ok {
			return replicatedCatalogTable{}, false
		}
		if compareBytesString(table, profile.Table) > 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == len(snapshot.replicatedTables) {
		return replicatedCatalogTable{}, false
	}
	entry := snapshot.replicatedTables[low]
	profile, ok := snapshot.replicatedTableProfileAt(entry)
	return entry, ok && vibejson.BytesEqualString(table, profile.Table)
}

// ResolveReplicatedTableKey maps one already-canonical ordered primary key to
// its exact RF3 allocation. The method performs no document parse and no map
// lookup. Supplying len(orderedKey)+16 bytes of scalarScratch and capacity for
// ServingReplicaCount in replicaScratch keeps successful resolution
// allocation-free.
func (snapshot *Snapshot) ResolveReplicatedTableKey(
	table []byte,
	orderedKey []byte,
	scalarScratch []byte,
	replicaScratch []ReplicatedEndpoint,
) (ResolvedReplicatedTableKey, bool) {
	return snapshot.resolveReplicatedTableKey(table, orderedKey, scalarScratch, replicaScratch, true)
}

// resolveReplicatedTableKeyWithoutRouteID skips the SHA256 route digest.
// Batch readers use it for non-leading points whose route authority is
// compared directly against the leading point's authority; the digest is a
// pure function of that authority, so equality without hashing rejects
// exactly the same mismatches (and one strict superset: a hash collision).
func (snapshot *Snapshot) resolveReplicatedTableKeyWithoutRouteID(
	table []byte,
	orderedKey []byte,
	scalarScratch []byte,
	replicaScratch []ReplicatedEndpoint,
) (ResolvedReplicatedTableKey, bool) {
	return snapshot.resolveReplicatedTableKey(table, orderedKey, scalarScratch, replicaScratch, false)
}

func (snapshot *Snapshot) resolveReplicatedTableKey(
	table []byte,
	orderedKey []byte,
	scalarScratch []byte,
	replicaScratch []ReplicatedEndpoint,
	wantRouteID bool,
) (ResolvedReplicatedTableKey, bool) {
	prep, ok := snapshot.replicatedTableResolvePrepFor(table)
	if !ok {
		return ResolvedReplicatedTableKey{}, false
	}
	return snapshot.resolveReplicatedTableKeyPrepared(
		&prep, orderedKey, scalarScratch, replicaScratch,
		wantRouteID, ReplicatedRoute{}, false,
	)
}

// replicatedTableResolvePrep is the per-table invariant prefix of key
// resolution: catalog entry, profile, placement, and mapper checks. The
// snapshot is pinned, so a prep stays valid for the whole batch. The table
// slice aliases the caller's request and must not outlive the call.
type replicatedTableResolvePrep struct {
	table    []byte
	profile  ReplicatedTableProfile
	specName distribution.DistributionName
	manifest *distribution.Manifest
	mapper   *distribution.NativeMapper
}

func (snapshot *Snapshot) replicatedTableResolvePrepFor(
	table []byte,
) (replicatedTableResolvePrep, bool) {
	entry, ok := snapshot.replicatedTableAtBytes(table)
	if !ok {
		return replicatedTableResolvePrep{}, false
	}
	profile, ok := snapshot.replicatedTableProfileAt(entry)
	if !ok {
		return replicatedTableResolvePrep{}, false
	}
	planner := snapshot.planner[entry.planner]
	spec := snapshot.config.Distributions[planner.spec]
	manifest := snapshot.config.Manifests[planner.manifest]
	if entry.mapper == nil || entry.mapper.Arity() != 1 ||
		entry.mapper.Version() != spec.MapperVersion ||
		entry.mapper.VirtualBucketBits() != spec.EffectiveBucketBits() {
		return replicatedTableResolvePrep{}, false
	}
	return replicatedTableResolvePrep{
		table: table, profile: profile, specName: spec.Name,
		manifest: manifest, mapper: entry.mapper,
	}, true
}

// resolveReplicatedTableKeyPrepared binds one key against a cached prep.
// reuseRoute skips the shard lookup when it already carries this prep's
// route for the key's shard (same prep and equal target shard); the route
// is a pure function of the pinned snapshot plus spec and shard, so the
// skipped lookup would rebuild an identical value. Only value fields of a
// reused route are read: callers must nil its Replicas before caching, as
// they may alias recycled scratch.
func (snapshot *Snapshot) resolveReplicatedTableKeyPrepared(
	prep *replicatedTableResolvePrep,
	orderedKey []byte,
	scalarScratch []byte,
	replicaScratch []ReplicatedEndpoint,
	wantRouteID bool,
	reuseRoute ReplicatedRoute,
	reuseOK bool,
) (ResolvedReplicatedTableKey, bool) {
	profile := prep.profile
	if len(orderedKey) == 0 || len(orderedKey) > int(profile.MaxKeyBytes) {
		return ResolvedReplicatedTableKey{}, false
	}
	// DecodeComponent may expand a wide exact-number key by a few spelling
	// bytes. Requiring caller workspace keeps that storage explicit and prevents
	// an unsafe transient string view from forcing a hidden heap allocation.
	if cap(scalarScratch) < len(orderedKey)+16 {
		return ResolvedReplicatedTableKey{}, false
	}
	component, decoded, next, err := orderedkey.DecodeComponent(
		scalarScratch[:0], orderedKey, 0,
	)
	if err != nil || component.Descending || next != len(orderedKey) {
		return ResolvedReplicatedTableKey{}, false
	}
	payload := decoded[component.PayloadStart:component.PayloadEnd]
	var scalar distribution.Scalar
	var canonicalStorage [replication.MaxMutationKeyBytes]byte
	canonical := canonicalStorage[:0]
	ok := false
	switch component.Kind {
	case orderedkey.KindString:
		scalar = distribution.NewString(byteview.String(payload))
		canonical, ok = orderedkey.AppendString(canonical, payload, orderedkey.Ascending)
	case orderedkey.KindNumber:
		scalar, err = distribution.NewNumber(byteview.String(payload))
		if err == nil {
			canonical, ok = orderedkey.AppendNumber(canonical, payload, orderedkey.Ascending)
		}
	default:
		return ResolvedReplicatedTableKey{}, false
	}
	if err != nil || !ok || !bytes.Equal(canonical, orderedKey) {
		return ResolvedReplicatedTableKey{}, false
	}
	var values [1]distribution.Scalar
	values[0] = scalar
	point, err := prep.mapper.PointFor(values[:])
	if err != nil {
		return ResolvedReplicatedTableKey{}, false
	}
	target, ok := prep.manifest.ResolvePointTarget(point)
	if !ok {
		return ResolvedReplicatedTableKey{}, false
	}
	route := reuseRoute
	if !reuseOK || target.Shard != route.Shard {
		route, ok = snapshot.ResolveReplicatedRoute(
			prep.specName, target.Shard, replicaScratch,
		)
		if !ok {
			return ResolvedReplicatedTableKey{}, false
		}
	}
	if route.AllocationGeneration != uint64(target.AllocationGeneration) ||
		route.Command.OwnershipEpoch != uint64(target.OwnershipEpoch) ||
		route.Command.RoutingVersion > uint64(prep.manifest.Version()) ||
		route.Command.SchemaGeneration != profile.SchemaGeneration ||
		route.LogicalSchemaDigest != profile.LogicalSchemaDigest {
		return ResolvedReplicatedTableKey{}, false
	}
	resolved := ResolvedReplicatedTableKey{
		Profile: profile, Route: route, Point: point,
	}
	if wantRouteID {
		resolved.RouteID = replicatedRouteID(route)
	}
	return resolved, true
}

func replicatedRouteID(route ReplicatedRoute) replication.Digest {
	return replicatedRouteAuthorityDigest(route)
}

// ReplicatedTableMetadataBytes reports only the compact table directory. Table
// and primary-key bytes are shared with the placement catalog.
func (snapshot *Snapshot) ReplicatedTableMetadataBytes() uint64 {
	if snapshot == nil {
		return 0
	}
	return replicatedTableMetadataBytes(snapshot.replicatedTables)
}

func replicatedTableMetadataBytes(tables []replicatedCatalogTable) uint64 {
	return uint64(cap(tables))*uint64(unsafe.Sizeof(replicatedCatalogTable{})) +
		uint64(len(tables))*uint64(unsafe.Sizeof(distribution.NativeMapper{}))
}

func (snapshot *Snapshot) replicatedTableProfiles() []ReplicatedTableProfile {
	if snapshot == nil || len(snapshot.replicatedTables) == 0 {
		return nil
	}
	profiles := make([]ReplicatedTableProfile, len(snapshot.replicatedTables))
	for ordinal, entry := range snapshot.replicatedTables {
		profile, ok := snapshot.replicatedTableProfileAt(entry)
		if !ok {
			return nil
		}
		profiles[ordinal] = profile
	}
	return profiles
}

// ReplicatedTableProfiles returns the detached portable table/schema directory
// used by cold topology allocation. Replica-local SQL storage names and LogIDs
// are intentionally absent.
func (snapshot *Snapshot) ReplicatedTableProfiles() []ReplicatedTableProfile {
	return snapshot.replicatedTableProfiles()
}

func validateReplicatedTableTransition(current, next *Snapshot) error {
	if current == nil || len(current.replicatedTables) == 0 {
		return nil
	}
	nextOrdinal := 0
	for _, oldEntry := range current.replicatedTables {
		old, ok := current.replicatedTableProfileAt(oldEntry)
		if !ok {
			return &CatalogError{Reason: "current replicated table directory is corrupt"}
		}
		for nextOrdinal < len(next.replicatedTables) {
			candidate, candidateOK := next.replicatedTableProfileAt(next.replicatedTables[nextOrdinal])
			if !candidateOK {
				return &CatalogError{Reason: "next replicated table directory is corrupt"}
			}
			if candidate.Table >= old.Table {
				break
			}
			nextOrdinal++
		}
		if nextOrdinal == len(next.replicatedTables) {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated table %q lost its base-relation profile", old.Table,
			)}
		}
		candidate, _ := next.replicatedTableProfileAt(next.replicatedTables[nextOrdinal])
		if candidate.SchemaGeneration == old.SchemaGeneration {
			var oldDDL, newDDL string
			for _, d := range current.replicatedTableDeclarations {
				if d.declaration.Table == old.Table {
					oldDDL = d.declaration.CreateTable
				}
			}
			for _, d := range next.replicatedTableDeclarations {
				if d.declaration.Table == old.Table {
					newDDL = d.declaration.CreateTable
				}
			}
			if oldDDL != newDDL {
				return &CatalogError{Reason: "replicated table declaration changed within one schema generation"}
			}
		}
		sameGenerationChanged := candidate.SchemaGeneration == old.SchemaGeneration &&
			(candidate.Relation != old.Relation || candidate.PrimaryKey != old.PrimaryKey ||
				candidate.MaxKeyBytes != old.MaxKeyBytes ||
				candidate.MaxDocumentBytes != old.MaxDocumentBytes ||
				candidate.LogicalSchemaDigest != old.LogicalSchemaDigest)
		if candidate.Table != old.Table || candidate.SchemaGeneration < old.SchemaGeneration ||
			sameGenerationChanged ||
			(candidate.SchemaGeneration != old.SchemaGeneration &&
				candidate.LogicalSchemaDigest == old.LogicalSchemaDigest) {
			return &CatalogError{Reason: fmt.Sprintf(
				"replicated table %q changed or regressed its base-relation profile", old.Table,
			)}
		}
	}
	return nil
}

type persistedReplicatedTable struct {
	CreateTable         string `json:"create_table,omitempty"`
	Table               string `json:"table"`
	Relation            uint16 `json:"relation"`
	PrimaryKey          string `json:"primary_key"`
	SchemaGeneration    uint64 `json:"schema_generation"`
	LogicalSchemaDigest string `json:"logical_schema_digest"`
	MaxKeyBytes         uint16 `json:"max_key_bytes"`
	MaxDocumentBytes    uint32 `json:"max_document_bytes"`
}

func (catalog persistedCatalog) replicatedTableDeclarations() []ReplicatedTableDeclaration {
	var declarations []ReplicatedTableDeclaration
	for _, table := range catalog.ReplicatedTables {
		if table.CreateTable != "" {
			declarations = append(declarations, ReplicatedTableDeclaration{Table: table.Table, CreateTable: table.CreateTable})
		}
	}
	return declarations
}

func persistedReplicatedTableProfiles(
	profiles []ReplicatedTableProfile,
) []persistedReplicatedTable {
	if len(profiles) == 0 {
		return nil
	}
	persisted := make([]persistedReplicatedTable, len(profiles))
	for ordinal, profile := range profiles {
		persisted[ordinal] = persistedReplicatedTable{
			Table: profile.Table, Relation: uint16(profile.Relation),
			PrimaryKey: profile.PrimaryKey, SchemaGeneration: profile.SchemaGeneration,
			LogicalSchemaDigest: hex.EncodeToString(profile.LogicalSchemaDigest[:]),
			MaxKeyBytes:         profile.MaxKeyBytes, MaxDocumentBytes: profile.MaxDocumentBytes,
		}
	}
	return persisted
}

func (catalog persistedCatalog) replicatedTableProfiles() ([]ReplicatedTableProfile, error) {
	if len(catalog.ReplicatedTables) == 0 {
		return nil, nil
	}
	profiles := make([]ReplicatedTableProfile, len(catalog.ReplicatedTables))
	for ordinal, persisted := range catalog.ReplicatedTables {
		var digest replication.Digest
		if err := decodeFixed32Hex(persisted.LogicalSchemaDigest, (*[32]byte)(&digest)); err != nil {
			return nil, &CatalogError{Reason: "replicated table relation manifest digest: " + err.Error()}
		}
		profiles[ordinal] = ReplicatedTableProfile{
			Table: persisted.Table, Relation: replication.RelationID(persisted.Relation),
			PrimaryKey: persisted.PrimaryKey, SchemaGeneration: persisted.SchemaGeneration,
			LogicalSchemaDigest: digest, MaxKeyBytes: persisted.MaxKeyBytes,
			MaxDocumentBytes: persisted.MaxDocumentBytes,
		}
	}
	return profiles, nil
}
