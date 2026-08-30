package gateway

import (
	"cmp"
	"crypto/sha256"
	"errors"
	"fmt"
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
	indexes, indexNoOp, err := schemaDDLPlanIndexes(state, table, sql)
	if err != nil {
		return nil, nil, err
	}
	declarations, declarationNoOp, err := schemaDDLPlanDeclarations(state, table, sql)
	if err != nil {
		return nil, nil, err
	}
	noOp := indexNoOp || declarationNoOp
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
		expected, ok := declaredTableInfoFromDeclarations(declarations, table, sourceProfile.PrimaryKey)
		if !ok {
			return nil, nil, ErrSchemaRollout
		}
		if err := validateSchemaDDLDescription(state, before, sourceProfile, build.Member, description, indexes, expected); err != nil {
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
		state.statistics.Descriptors(), descriptors, profiles, declarations)
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

// ReconcileAppliedReplicatedSchemaDDLCatalog repairs the narrow publication
// cut where every shard descriptor already names the retained target, while
// the portable index/declaration metadata is still the source view. It returns
// matched=false for an ordinary source-generation rollout. The returned plans
// reconstruct the exact already-applied replica cuts for terminal drain; they
// never authorize another activation.
func ReconcileAppliedReplicatedSchemaDDLCatalog(current *Snapshot, operation [32]byte,
	table, sql string, builds []SchemaDDLReplicaBuild,
) (*Snapshot, []SchemaRolloutReplicaPlan, bool, error) {
	if current == nil || current.Generation() == math.MaxUint64 || operation == ([32]byte{}) ||
		len(sql) == 0 || len(sql) > sqldriver.ReplicatedChildSchemaMaxBytes {
		return nil, nil, false, ErrSchemaRollout
	}
	state, err := initialCatalogState(current)
	if err != nil {
		return nil, nil, false, err
	}
	placement, found := state.Placement(table)
	if !found {
		return nil, nil, false, sqldriver.ErrTableNotFound
	}
	statement, err := sqlast.ParseStatement(sql)
	if err != nil {
		return nil, nil, false, err
	}
	descriptors, profiles := state.replicatedDescriptors(), state.replicatedTableProfiles()
	type replicaKey struct {
		group  raftmember.GroupKey
		node   rafttransport.NodeID
		member uint64
	}
	want := make(map[replicaKey]int)
	for i, descriptor := range descriptors {
		if descriptor.Distribution == placement.Distribution {
			for _, replica := range descriptor.Replicas {
				want[replicaKey{descriptor.Group, replica.Node, replica.Member}] = i
			}
		}
	}
	if len(want) == 0 || len(builds) != len(want) {
		return nil, nil, false, ErrSchemaRollout
	}
	// A single ordinary source descriptor means this is not the repair cut.
	for _, build := range builds {
		i, ok := want[replicaKey{build.Request.Group, build.Node, build.Member}]
		if !ok {
			return nil, nil, false, ErrSchemaRollout
		}
		if build.Request.FromSchemaGeneration == descriptors[i].Command.SchemaGeneration {
			return nil, nil, false, nil
		}
	}
	indexes, indexNoOp, err := schemaDDLPlanIndexes(state, table, sql)
	alreadyCataloged := statement.Kind == sqlast.KindCreateIndex && errors.Is(err, sqldriver.ErrIndexExists) ||
		statement.Kind == sqlast.KindDropIndex && errors.Is(err, sqldriver.ErrIndexNotFound)
	if alreadyCataloged {
		indexes = state.indexDescriptors()
		err = nil
	}
	if err != nil {
		return nil, nil, true, err
	}
	declarations, declarationNoOp, err := schemaDDLPlanDeclarations(state, table, sql)
	alreadyCataloged = alreadyCataloged || statement.Kind == sqlast.KindAlterTable && declarationNoOp ||
		statement.Kind == sqlast.KindTruncate
	if err != nil || indexNoOp || declarationNoOp && !alreadyCataloged {
		return nil, nil, true, errors.Join(err, ErrSchemaRolloutConflict)
	}
	var sourceProfile ReplicatedTableProfile
	for _, profile := range profiles {
		if profile.Table == table {
			sourceProfile = profile
			break
		}
	}
	if sourceProfile.Table == "" {
		return nil, nil, true, ErrSchemaRollout
	}
	seen := make(map[replicaKey]bool, len(builds))
	plans := make([]SchemaRolloutReplicaPlan, 0, len(builds))
	sqlDigest := sha256.Sum256([]byte(sql))
	var logical replication.Digest
	for _, build := range builds {
		r := build.Request
		key := replicaKey{r.Group, build.Node, build.Member}
		i, ok := want[key]
		if !ok || seen[key] {
			return nil, nil, true, ErrSchemaRollout
		}
		seen[key] = true
		currentDescriptor := descriptors[i]
		proof := build.Target.Proof
		if _, err := schemainstall.BuildRequestDigest(r); err != nil || r.Operation != operation ||
			r.SQLBytes != uint64(len(sql)) || r.SQLDigest != sqlDigest ||
			r.AllocationGeneration != currentDescriptor.AllocationGeneration ||
			r.FromSchemaGeneration+1 != currentDescriptor.Command.SchemaGeneration ||
			proof.Catalog.SchemaGeneration != currentDescriptor.Command.SchemaGeneration ||
			proof.Catalog.RelationManifestDigest != currentDescriptor.Command.RelationManifestDigest ||
			build.Target.NoOp {
			return nil, nil, true, errors.Join(err, ErrSchemaRolloutConflict)
		}
		if err := sqldriver.ValidateReplicatedSchemaDDLTarget(build.Target, r.SourceApplied, r.FromSchemaGeneration); err != nil {
			return nil, nil, true, err
		}
		description, err := sqldriver.DescribeReplicatedSchemaCatalogImage(build.Target.Catalog)
		if err != nil {
			return nil, nil, true, err
		}
		if logical != (replication.Digest{}) && logical != replication.Digest(description.LogicalSchemaDigest) {
			return nil, nil, true, ErrSchemaRolloutConflict
		}
		logical = replication.Digest(description.LogicalSchemaDigest)
		expected, ok := declaredTableInfoFromDeclarations(declarations, table, sourceProfile.PrimaryKey)
		if !ok {
			return nil, nil, true, ErrSchemaRollout
		}
		sourceDescriptor := currentDescriptor
		sourceDescriptor.Command.SchemaGeneration = r.FromSchemaGeneration
		sourceDescriptor.Command.RelationManifestDigest = r.FromRelationManifestDigest
		if err := validateSchemaDDLDescription(state, sourceDescriptor, sourceProfile, build.Member, description, indexes, expected); err != nil {
			return nil, nil, true, err
		}
		plans = append(plans, SchemaRolloutReplicaPlan{Node: build.Node, Member: build.Member,
			Bundle: slices.Clone(build.Target.Catalog), Request: schemainstall.Request{
				Operation: operation, Group: r.Group, AllocationGeneration: r.AllocationGeneration,
				FromSchemaGeneration: r.FromSchemaGeneration, FromRelationManifestDigest: r.FromRelationManifestDigest,
				ToSchemaGeneration:       build.Target.Proof.Catalog.SchemaGeneration,
				ToRelationManifestDigest: build.Target.Proof.Catalog.RelationManifestDigest,
				ApplyContractDigest:      build.Target.Proof.ApplyContract,
				BundleDigest:             build.Target.Proof.Catalog.Digest, BundleBytes: build.Target.Proof.Catalog.Bytes}})
	}
	slices.SortFunc(plans, func(a, b SchemaRolloutReplicaPlan) int {
		if c := compareMembershipGrantGroup(a.Request.Group, b.Request.Group); c != 0 {
			return c
		}
		return cmp.Compare(a.Member, b.Member)
	})
	for _, profile := range profiles {
		p, ok := state.Placement(profile.Table)
		if !ok {
			return nil, nil, true, ErrSchemaRollout
		}
		if p.Distribution == placement.Distribution &&
			(profile.SchemaGeneration == 0 || profile.LogicalSchemaDigest != logical) {
			return nil, nil, true, ErrSchemaRolloutConflict
		}
	}
	if alreadyCataloged {
		return current, plans, true, nil
	}
	target, err := NewSnapshotWithReplicatedTableMetadata(state.config, state.endpoints,
		state.Generation()+1, indexes, state.statistics.Descriptors(), descriptors, profiles, declarations)
	if err != nil {
		return nil, nil, true, err
	}
	target, err = advanceCatalogState(state, target)
	return target, plans, true, err
}

// ResolveReplicatedSchemaDDLTable returns the one base table affected by the
// supported online DDL grammar. DROP INDEX follows PostgreSQL's table-free
// spelling by resolving the retained catalog index identity.
func ResolveReplicatedSchemaDDLTable(current *Snapshot, sql string) (string, error) {
	if current == nil {
		return "", ErrSchemaRollout
	}
	statement, err := query.PrepareDML(sql)
	if err != nil {
		return "", err
	}
	defer statement.Release()
	tree := statement.Tree()
	switch tree.Kind {
	case sqlast.KindAlterTable:
		return tree.AlterTable.Table, nil
	case sqlast.KindCreateIndex:
		return tree.CreateIndex.Table, nil
	case sqlast.KindTruncate:
		return tree.Truncate.Table, nil
	case sqlast.KindDropIndex:
		if tree.DropIndex.HasTable {
			return tree.DropIndex.Table, nil
		}
		result := ""
		for _, index := range current.indexDescriptors() {
			if index.Name != tree.DropIndex.Name {
				continue
			}
			if result != "" && result != index.Table {
				return "", ErrSchemaRollout
			}
			result = index.Table
		}
		if result == "" {
			if tree.DropIndex.IfExists {
				return "", nil
			}
			return "", sqldriver.ErrIndexNotFound
		}
		return result, nil
	default:
		return "", sqlast.NewFeatureNotSupportedError(sql, 0,
			"distributed PostgreSQL DDL supports ALTER TABLE ADD COLUMN, CREATE INDEX, DROP INDEX, and TRUNCATE")
	}
}

func validateSchemaDDLDescription(current *Snapshot, descriptor ReplicatedShardDescriptor, profile ReplicatedTableProfile, member uint64, d sqldriver.ReplicatedSchemaCatalogDescription, indexes []IndexDescriptor, declared sqldriver.TableInfo) error {
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
	slices.SortFunc(declared.Columns, func(a, b sqldriver.ColumnInfo) int { return strings.Compare(a.Path, b.Path) })
	slices.SortFunc(d.Table.Columns, func(a, b sqldriver.ColumnInfo) int { return strings.Compare(a.Path, b.Path) })
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

func declaredTableInfoFromDeclarations(declarations []ReplicatedTableDeclaration, table, primaryKey string) (sqldriver.TableInfo, bool) {
	for _, declaration := range declarations {
		if declaration.Table != table {
			continue
		}
		tree, err := sqlast.ParseStatement(declaration.CreateTable)
		if err != nil || tree.CreateTable == nil {
			return sqldriver.TableInfo{}, false
		}
		info := sqldriver.TableInfo{Name: table, PrimaryKey: primaryKey}
		for _, column := range tree.CreateTable.Columns {
			info.Columns = append(info.Columns, sqldriver.ColumnInfo{
				Path: string(column.Path.AppendPointer(nil)), Types: column.Type, Required: column.Required,
			})
		}
		return info, true
	}
	return sqldriver.TableInfo{}, false
}

func schemaDDLPlanDeclarations(current *Snapshot, table, text string) ([]ReplicatedTableDeclaration, bool, error) {
	declarations := current.ReplicatedTableDeclarations()
	statement, err := sqlast.ParseStatement(text)
	if err != nil {
		return nil, false, err
	}
	if statement.Kind != sqlast.KindAlterTable {
		return declarations, false, nil
	}
	for i := range declarations {
		if declarations[i].Table != table {
			continue
		}
		created, err := sqlast.ParseStatement(declarations[i].CreateTable)
		if err != nil || created.CreateTable == nil {
			return nil, false, errors.Join(err, ErrSchemaRollout)
		}
		for _, column := range created.CreateTable.Columns {
			if string(column.Path.AppendPointer(nil)) == string(statement.AlterTable.Column.Path.AppendPointer(nil)) {
				if statement.AlterTable.IfNotExists {
					return declarations, true, nil
				}
				return nil, false, ErrSchemaRollout
			}
		}
		created.CreateTable.Columns = append(created.CreateTable.Columns, statement.AlterTable.Column)
		declarations[i].CreateTable = renderReplicatedCreateTable(created.CreateTable)
		return declarations, false, nil
	}
	return nil, false, ErrSchemaRollout
}

func renderReplicatedCreateTable(table *sqlast.CreateTableStmt) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	appendDDLIdentifier(&b, table.Table)
	b.WriteString(" (")
	for i, column := range table.Columns {
		if i != 0 {
			b.WriteString(", ")
		}
		appendDDLPath(&b, column.Path)
		b.WriteByte(' ')
		if column.Type == sqlast.TypeAny {
			b.WriteString("ANY")
		} else {
			b.WriteString((column.Type &^ sqlast.TypeNull).String())
		}
		if column.Required {
			b.WriteString(" NOT NULL")
		}
	}
	if len(table.PrimaryKey) != 0 {
		if len(table.Columns) != 0 {
			b.WriteString(", ")
		}
		b.WriteString("PRIMARY KEY (")
		for i, path := range table.PrimaryKey {
			if i != 0 {
				b.WriteString(", ")
			}
			appendDDLPath(&b, path)
		}
		b.WriteByte(')')
	}
	b.WriteByte(')')
	return b.String()
}

func appendDDLIdentifier(b *strings.Builder, identifier string) {
	b.WriteByte('"')
	b.WriteString(strings.ReplaceAll(identifier, "\"", "\"\""))
	b.WriteByte('"')
}

func appendDDLPath(b *strings.Builder, path *sqlast.PathExpr) {
	for i, segment := range path.Segments {
		if segment.IsIndex {
			fmt.Fprintf(b, "[%d]", segment.Index)
			continue
		}
		if i != 0 {
			b.WriteByte('.')
		}
		appendDDLIdentifier(b, segment.Key)
	}
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
	case sqlast.KindAlterTable:
		if tree.AlterTable.Table != table {
			return nil, false, ErrSchemaRollout
		}
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
