package driver

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	vibesql "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

// ReplicatedChildSchemaMaxBytes bounds the complete cold provisioning schema,
// before parsing or allocating statement metadata.
const ReplicatedChildSchemaMaxBytes = 64 << 10

// ValidateReplicatedChildSchema authenticates explicit provisioning DDL against
// the retained relation manifest. Only exact table definitions and named
// exact indexes are accepted: provisioning cannot infer names or run data DML.
func ValidateReplicatedChildSchema(expected ReplicatedShardStoreIdentity, createTable string,
	statements []string, globals []ReplicatedGlobalIndexRelation,
) error {
	if validateReplicatedShardStoreIdentity(expected) != nil || !replicatedBundleProvisioningMatches(expected, globals) {
		return ErrReplicatedShardStoreProfile
	}
	return validateReplicatedChildSchemaDefinition(expected.UserTable, expected.UserPrimaryKey,
		createTable, statements, globals, &expected.Relations[0])
}

// ValidateReplicatedChildSchemaDefinition checks an initial schema before any
// filesystem effects. It grants no retained identity or topology authority;
// child preparation must additionally call ValidateReplicatedChildSchema.
func ValidateReplicatedChildSchemaDefinition(table, primaryKey, createTable string,
	statements []string, globals []ReplicatedGlobalIndexRelation,
) error {
	return validateReplicatedChildSchemaDefinition(table, primaryKey, createTable, statements, globals, nil)
}

func validateReplicatedChildSchemaDefinition(table, primaryKey, createTable string,
	statements []string, globals []ReplicatedGlobalIndexRelation, expected *ReplicatedShardRelationIdentity,
) error {
	fail := func() error {
		return fmt.Errorf("%w: child schema differs from retained relation manifest", ErrReplicatedShardStoreProfile)
	}
	if len(createTable) == 0 || len(createTable) > ReplicatedChildSchemaMaxBytes ||
		len(statements) > 4096+replication.MaxRelationsPerBundle-1 ||
		len(globals) >= replication.MaxRelationsPerBundle {
		return fail()
	}
	total := len(createTable)
	for _, statement := range statements {
		if len(statement) == 0 || len(statement) > ReplicatedChildSchemaMaxBytes-total {
			return fail()
		}
		total += len(statement)
	}
	if validateReplicatedBundleRelationName(table) != nil {
		return fail()
	}
	for i, global := range globals {
		if global.Relation != uint16(i+2) || global.Table == table || validateReplicatedBundleRelationName(global.Table) != nil ||
			global.IndexID == 0 || global.Incarnation == 0 || global.LocatorCount == 0 || global.LocatorCount > 8 ||
			!validReplicatedGlobalIndexPlacement(global.KeyEncoding, global.KeyArity, global.TupleVersion, global.MapperVersion, global.BucketBits) {
			return fail()
		}
		for j := 0; j < i; j++ {
			if globals[j].Table == global.Table {
				return fail()
			}
		}
	}
	base, err := vibesql.ParseStatement(createTable)
	if err != nil || !replicatedChildTableMatches(base, table, primaryKey) {
		return fail()
	}
	prepared, err := query.PrepareParsedDML(createTable, base)
	if err != nil {
		return fail()
	}
	definition, err := prepared.LowerTable()
	prepared.Release()
	if err != nil || validatePrimarySchema(primaryKey, definition.Schema) != nil {
		return fail()
	}
	if expected != nil && replicatedSchemaDigest(schemaMetaFrom(definition.Schema)) != expected.SchemaDigest {
		return fail()
	}
	created := make([]bool, len(globals))
	indexes := make([]indexMeta, 0, len(statements))
	names := make(map[string]struct{}, len(statements))
	for _, source := range statements {
		statement, err := vibesql.ParseStatement(source)
		if err != nil {
			return fail()
		}
		switch {
		case statement.CreateTable != nil:
			found := false
			for i, global := range globals {
				if statement.CreateTable.Table != global.Table {
					continue
				}
				if created[i] || len(statement.CreateTable.Columns) != 0 || !replicatedChildTableMatches(statement, global.Table, "/key") {
					return fail()
				}
				created[i], found = true, true
				break
			}
			if !found {
				return fail()
			}
		case statement.CreateIndex != nil:
			index := statement.CreateIndex
			if !index.HasName || index.IfNotExists || index.Table != table || len(indexes) == 4096 {
				return fail()
			}
			if _, duplicate := names[index.Name]; duplicate {
				return fail()
			}
			paths := make([]string, len(index.Paths))
			for i, path := range index.Paths {
				paths[i] = string(path.AppendPointer(nil))
			}
			compiled, err := store.CompileExactIndex(store.IndexDefinition{Name: index.Name, Paths: paths})
			if err != nil {
				return fail()
			}
			names[index.Name] = struct{}{}
			indexes = append(indexes, indexMeta{Name: index.Name, Paths: compiled.Specs[:compiled.N]})
		default:
			return fail()
		}
	}
	for _, present := range created {
		if !present {
			return fail()
		}
	}
	if expected != nil && replicatedLocalIndexDigest(indexes) != expected.LocalIndexDigest {
		return fail()
	}
	return nil
}

func replicatedChildTableMatches(statement *vibesql.Statement, table, primaryKey string) bool {
	if statement == nil || statement.CreateTable == nil {
		return false
	}
	definition := statement.CreateTable
	return definition.Table == table && !definition.IfNotExists &&
		len(definition.PrimaryKey) == 1 && string(definition.PrimaryKey[0].AppendPointer(nil)) == primaryKey
}
