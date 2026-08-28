package gateway

import (
	"crypto/sha256"
	"errors"
	"math"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// SchemaDDLReplicaBuild binds an authenticated build response to its replica.
// The coordinator retains all requests and receipts before installation. A
// plan constructed here is not permission to publish or release the route gate.
type SchemaDDLReplicaBuild struct {
	Node    rafttransport.NodeID
	Member  uint64
	Request schemainstall.BuildRequest
	Target  sqldriver.ReplicatedSchemaDDLTarget
}

// BuildReplicatedSchemaDDLPlan derives one complete distribution schema cut
// from all three receipts for every affected shard. It updates portable table
// schema, shard-specific command manifests, and exact index metadata together,
// preserving table placement, replica rosters, durable range/request identities,
// declarations and lifetime index-ID high-waters. No catalog is published here.
func BuildReplicatedSchemaDDLPlan(current *Snapshot, operation [32]byte, table, sql string, builds []SchemaDDLReplicaBuild) (*Snapshot, []SchemaRolloutReplicaPlan, error) {
	if current == nil || current.Generation() == math.MaxUint64 || operation == ([32]byte{}) || len(sql) == 0 || len(sql) > sqldriver.ReplicatedChildSchemaMaxBytes {
		return nil, nil, ErrSchemaRollout
	}
	state, err := initialCatalogState(current)
	if err != nil {
		return nil, nil, err
	}
	placement, found := state.Placement(table)
	if !found {
		return nil, nil, sqldriver.ErrTableNotFound
	}
	indexes, noOp, err := schemaDDLPlanIndexes(state, table, sql)
	if err != nil {
		return nil, nil, err
	}
	descriptors, profiles := state.replicatedDescriptors(), state.replicatedTableProfiles()
	sourceDescriptors := state.replicatedDescriptors()
	var sourceProfile ReplicatedTableProfile
	for _, profile := range profiles {
		if profile.Table == table {
			sourceProfile = profile
			break
		}
	}
	if sourceProfile.Table == "" {
		return nil, nil, ErrSchemaRollout
	}
	type replicaKey struct {
		group  raftmember.GroupKey
		node   rafttransport.NodeID
		member uint64
	}
	want := make(map[replicaKey]int)
	for i, d := range descriptors {
		if d.Distribution == placement.Distribution {
			for _, replica := range d.Replicas {
				want[replicaKey{d.Group, replica.Node, replica.Member}] = i
			}
		}
	}
	if len(want) == 0 || len(builds) != len(want) {
		return nil, nil, ErrSchemaRollout
	}
	seen := make(map[replicaKey]bool, len(builds))
	groupCuts := make(map[raftmember.GroupKey]sqldriver.ReplicatedSchemaTargetProof)
	var logical replication.Digest
	sqlDigest := sha256.Sum256([]byte(sql))
	for _, build := range builds {
		r := build.Request
		key := replicaKey{r.Group, build.Node, build.Member}
		i, found := want[key]
		if !found || seen[key] {
			return nil, nil, ErrSchemaRollout
		}
		seen[key] = true
		before := sourceDescriptors[i]
		if _, err := schemainstall.BuildRequestDigest(r); err != nil || r.Operation != operation || r.SQLBytes != uint64(len(sql)) || r.SQLDigest != sqlDigest ||
			r.AllocationGeneration != before.AllocationGeneration || r.FromSchemaGeneration != before.Command.SchemaGeneration || r.FromRelationManifestDigest != replication.Digest(before.Command.RelationManifestDigest) ||
			build.Target.NoOp != noOp {
			return nil, nil, errors.Join(err, ErrSchemaRollout)
		}
		if err := sqldriver.ValidateReplicatedSchemaDDLTarget(build.Target, r.SourceApplied, r.FromSchemaGeneration); err != nil {
			return nil, nil, err
		}
		if noOp {
			continue
		}
		proof := build.Target.Proof
		if prior, exists := groupCuts[r.Group]; exists && (prior.SourceApplied != proof.SourceApplied || prior.ApplyContract != proof.ApplyContract ||
			prior.Catalog.RelationManifestDigest != proof.Catalog.RelationManifestDigest || prior.Relations.TotalRows != proof.Relations.TotalRows) {
			return nil, nil, ErrSchemaRollout
		}
		groupCuts[r.Group] = proof
		description, err := sqldriver.DescribeReplicatedSchemaCatalogImage(build.Target.Catalog)
		if err != nil {
			return nil, nil, err
		}
		if logical != (replication.Digest{}) && logical != replication.Digest(description.LogicalSchemaDigest) {
			return nil, nil, ErrSchemaRollout
		}
		logical = replication.Digest(description.LogicalSchemaDigest)
		if err := validateSchemaDDLDescription(state, before, sourceProfile, build.Member, description, indexes); err != nil {
			return nil, nil, err
		}
		descriptors[i].Command.SchemaGeneration = proof.Catalog.SchemaGeneration
		descriptors[i].Command.RelationManifestDigest = proof.Catalog.RelationManifestDigest
		descriptors[i].LogicalSchemaDigest = logical
	}
	if noOp {
		return current, nil, nil
	}
	for i := range profiles {
		p, found := state.Placement(profiles[i].Table)
		if !found {
			return nil, nil, ErrSchemaRollout
		}
		if p.Distribution == placement.Distribution {
			profiles[i].SchemaGeneration++
			profiles[i].LogicalSchemaDigest = logical
		}
	}
	target, err := NewSnapshotWithReplicatedTableMetadata(state.config, state.endpoints, state.Generation()+1, indexes,
		state.statistics.Descriptors(), descriptors, profiles, state.ReplicatedTableDeclarations())
	if err != nil {
		return nil, nil, err
	}
	target, err = advanceCatalogState(state, target)
	if err != nil {
		return nil, nil, err
	}
	if _, err := schemaRolloutChanges(state, target); err != nil {
		return nil, nil, err
	}
	plans := make([]SchemaRolloutReplicaPlan, 0, len(builds))
	for _, build := range builds {
		// Derive against the specific group. A node/member pair can occur in
		// several groups on one multigroup node and is not globally unique.
		before := sourceDescriptors[want[replicaKey{build.Request.Group, build.Node, build.Member}]]
		proof := build.Target.Proof
		plans = append(plans, SchemaRolloutReplicaPlan{Node: build.Node, Member: build.Member, Bundle: slices.Clone(build.Target.Catalog),
			Request: schemainstall.Request{Operation: operation, Group: before.Group, AllocationGeneration: before.AllocationGeneration,
				FromSchemaGeneration: before.Command.SchemaGeneration, FromRelationManifestDigest: replication.Digest(before.Command.RelationManifestDigest),
				ToSchemaGeneration: proof.Catalog.SchemaGeneration, ToRelationManifestDigest: proof.Catalog.RelationManifestDigest,
				ApplyContractDigest: proof.ApplyContract, BundleDigest: proof.Catalog.Digest, BundleBytes: proof.Catalog.Bytes}})
	}
	changes, _ := schemaRolloutChanges(state, target)
	if err := validateSchemaRolloutReplicaPlans(operation, target, changes, plans); err != nil {
		return nil, nil, err
	}
	slices.SortFunc(plans, func(a, b SchemaRolloutReplicaPlan) int {
		if c := compareMembershipGrantGroup(a.Request.Group, b.Request.Group); c != 0 {
			return c
		}
		if a.Member < b.Member {
			return -1
		}
		if a.Member > b.Member {
			return 1
		}
		return 0
	})
	return target, plans, nil
}

func validateSchemaDDLDescription(current *Snapshot, descriptor ReplicatedShardDescriptor, profile ReplicatedTableProfile, member uint64, d sqldriver.ReplicatedSchemaCatalogDescription, indexes []IndexDescriptor) error {
	b := d.Store.Binding
	want := descriptor.Command
	if b.ClusterID != descriptor.Group.ClusterID || b.ClusterIncarnation != descriptor.Group.ClusterIncarnation ||
		b.TopologyRecoveryEpoch != descriptor.Group.TopologyRecoveryEpoch || b.ShardIncarnation != descriptor.Group.ShardIncarnation || b.GroupID != descriptor.Group.GroupID ||
		b.Distribution != string(descriptor.Distribution) || b.Shard != string(descriptor.Shard) || b.MemberID != member || b.AllocationGeneration != uint64(descriptor.AllocationGeneration) ||
		b.Authority.SchemaGeneration != want.SchemaGeneration+1 || b.Authority.ActivePolicyGeneration != want.ActivePolicyGeneration ||
		b.Authority.ProtectionEpoch != want.ProtectionEpoch || b.Authority.OwnershipEpoch != want.OwnershipEpoch || b.Authority.RoutingVersion != want.RoutingVersion || b.Authority.RouteGeneration != want.RouteGeneration ||
		d.Store.UserTable != profile.Table || d.Store.UserPrimaryKey != profile.PrimaryKey || d.Store.UserLimits.MaxKeyBytes != int(profile.MaxKeyBytes) || d.Store.UserLimits.MaxDocumentBytes != int(profile.MaxDocumentBytes) {
		return ErrSchemaRollout
	}
	for _, replica := range descriptor.Replicas {
		if replica.Member == member && replica.StoreID != b.StoreID {
			return ErrSchemaRollout
		}
	}
	manifest, found := current.Manifest(descriptor.Distribution)
	if !found {
		return ErrSchemaRollout
	}
	_, shard := manifestShardOrdinal(manifest, descriptor.Shard)
	if d.Placement.Range != shard.Range || d.Placement.ShardKey != profile.PrimaryKey {
		return ErrSchemaRollout
	}
	declared, _ := current.declaredTableInfo(profile.Table)
	slices.SortFunc(declared.Columns, func(a, b sqldriver.ColumnInfo) int { return strings.Compare(a.Path, b.Path) })
	if !slices.Equal(declared.Columns, d.Table.Columns) {
		return ErrSchemaRollout
	}
	count := 0
	for _, index := range indexes {
		if index.Table != profile.Table || index.Flags&IndexLocal == 0 {
			continue
		}
		count++
		found := false
		for _, actual := range d.Table.Indexes {
			if actual.Name == index.Name && slices.Equal(actual.Paths, index.Paths) {
				found = true
				break
			}
		}
		if !found {
			return ErrSchemaRollout
		}
	}
	if count != len(d.Table.Indexes) {
		return ErrSchemaRollout
	}
	return nil
}

func schemaDDLPlanIndexes(current *Snapshot, table, sql string) ([]IndexDescriptor, bool, error) {
	statement, err := query.PrepareDML(sql)
	if err != nil {
		return nil, false, err
	}
	defer statement.Release()
	indexes := current.indexDescriptors()
	tree := statement.Tree()
	find := func(name string) int {
		for i, index := range indexes {
			if index.Table == table && index.Name == name {
				return i
			}
		}
		return -1
	}
	switch tree.Kind {
	case sqlast.KindCreateIndex:
		index, err := statement.LowerIndex()
		if err != nil {
			return nil, false, err
		}
		if index.Table != table {
			return nil, false, ErrSchemaRollout
		}
		if find(index.Definition.Name) >= 0 {
			if index.IfNotExists {
				return indexes, true, nil
			}
			return nil, false, sqldriver.ErrIndexExists
		}
		id, ok := current.NextIndexID()
		if !ok {
			return nil, false, ErrSchemaRollout
		}
		indexes = append(indexes, IndexDescriptor{IndexID: id, Incarnation: 1, Table: table, Name: index.Definition.Name,
			Paths: slices.Clone(index.Definition.Paths), Flags: IndexLocal, Lifecycle: IndexReady})
	case sqlast.KindDropIndex:
		if tree.DropIndex.HasTable && tree.DropIndex.Table != table {
			return nil, false, ErrSchemaRollout
		}
		i := find(tree.DropIndex.Name)
		if i < 0 {
			if tree.DropIndex.IfExists {
				return indexes, true, nil
			}
			return nil, false, sqldriver.ErrIndexNotFound
		}
		if indexes[i].Flags&IndexLocal == 0 {
			return nil, false, ErrSchemaRollout
		}
		indexes = slices.Delete(indexes, i, i+1)
	case sqlast.KindTruncate:
		if tree.Truncate.Table != table {
			return nil, false, ErrSchemaRollout
		}
	default:
		return nil, false, ErrSchemaRollout
	}
	old := current.indexDescriptors()
	for i := range indexes {
		if indexes[i].Table != table || indexes[i].Flags&IndexLocal == 0 {
			continue
		}
		for _, before := range old {
			if before.IndexID != indexes[i].IndexID {
				continue
			}
			if before.Incarnation == math.MaxUint64 || before.Lifecycle != IndexReady {
				return nil, false, ErrSchemaRollout
			}
			indexes[i].Incarnation++
		}
	}
	return indexes, false, nil
}
