package main

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

func rf3QuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func rf3IndexPath(pointer string) (string, error) {
	if len(pointer) < 2 || pointer[0] != '/' {
		return "", errRF3Serving
	}
	segments := strings.Split(pointer[1:], "/")
	for i := range segments {
		segments[i] = strings.ReplaceAll(strings.ReplaceAll(segments[i], "~1", "/"), "~0", "~")
		segments[i] = rf3QuoteIdentifier(segments[i])
	}
	return strings.Join(segments, "."), nil
}

func rf3RenderRetainedCreateTable(table sqldriver.TableInfo) (string, error) {
	if table.Name == "" || table.PrimaryKey == "" || len(table.Columns) == 0 {
		return "", errRF3Serving
	}
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	b.WriteString(rf3QuoteIdentifier(table.Name))
	b.WriteString(" (")
	for i, column := range table.Columns {
		path, err := rf3IndexPath(column.Path)
		if err != nil {
			return "", err
		}
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString(path)
		b.WriteByte(' ')
		types := column.Types &^ sqlast.TypeNull
		if types == sqlast.TypeAny {
			b.WriteString("ANY")
		} else {
			b.WriteString(types.String())
		}
		if column.Required {
			b.WriteString(" NOT NULL")
		}
	}
	primary, err := rf3IndexPath(table.PrimaryKey)
	if err != nil {
		return "", err
	}
	b.WriteString(", PRIMARY KEY (")
	b.WriteString(primary)
	b.WriteString("))")
	return b.String(), nil
}

func rf3RetainedCreateTableMatches(text string, table sqldriver.TableInfo) bool {
	statement, err := sqlast.ParseStatement(text)
	if err != nil || statement.CreateTable == nil || statement.CreateTable.Table != table.Name {
		return false
	}
	primary := ""
	if len(statement.CreateTable.PrimaryKey) == 1 {
		primary = string(statement.CreateTable.PrimaryKey[0].AppendPointer(nil))
	}
	columns := make([]sqldriver.ColumnInfo, 0, len(statement.CreateTable.Columns))
	for _, column := range statement.CreateTable.Columns {
		columns = append(columns, sqldriver.ColumnInfo{Path: string(column.Path.AppendPointer(nil)),
			Types: column.Type, Required: column.Required})
	}
	want := slices.Clone(table.Columns)
	slices.SortFunc(columns, func(a, b sqldriver.ColumnInfo) int { return strings.Compare(a.Path, b.Path) })
	slices.SortFunc(want, func(a, b sqldriver.ColumnInfo) int { return strings.Compare(a.Path, b.Path) })
	return primary == table.PrimaryKey && slices.Equal(columns, want)
}

// The persisted startup manifest is the generation-zero provisioning input.
// Online DDL advances the authenticated SQL catalog without rewriting that
// operator file. Reconstruct only the local-index portion of the cold split
// template from the retained catalog; unrelated global-index declarations keep
// their original explicit statements and identities.
func refreshRF3SplitChildSchema(registry rf3ManifestSplitChildRegistry,
	description sqldriver.ReplicatedSchemaCatalogDescription,
) (rf3ManifestSplitChildRegistry, error) {
	if description.Store.UserTable != registry.Table || description.Table.Name != registry.Table {
		return registry, errRF3Serving
	}
	if !rf3RetainedCreateTableMatches(registry.CreateTable, description.Table) {
		create, err := rf3RenderRetainedCreateTable(description.Table)
		if err != nil {
			return registry, err
		}
		registry.CreateTable = create
	}
	statements := make([]string, 0, len(registry.SchemaStatements)+len(description.Table.Indexes))
	for _, source := range registry.SchemaStatements {
		statement, err := query.PrepareDML(source)
		if err != nil {
			return registry, err
		}
		tree := statement.Tree()
		// Only local indexes on the user table are reconstructed below.
		// Preserve every other retained statement, including CREATE TABLE for
		// colocated global-index relations in a multi-table bundle.
		if tree.CreateIndex == nil || tree.CreateIndex.Table != registry.Table {
			statements = append(statements, source)
		}
		statement.Release()
	}
	for _, index := range description.Table.Indexes {
		paths := make([]string, len(index.Paths))
		for i, pointer := range index.Paths {
			path, err := rf3IndexPath(pointer)
			if err != nil {
				return registry, err
			}
			paths[i] = path
		}
		statements = append(statements, fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
			rf3QuoteIdentifier(index.Name), rf3QuoteIdentifier(registry.Table), strings.Join(paths, ", ")))
	}
	registry.SchemaStatements = statements
	if !rf3SplitChildSchemaMatchesRetained(registry, description.Store) {
		return registry, errRF3Serving
	}
	return registry, nil
}

func parseRF3ChildSchemaStatements(node vibejson.Node, used int) ([]string, error) {
	count, ok := node.ArrayLen()
	if !ok || count < 1 || count > 4096+replication.MaxRelationsPerBundle-1 {
		return nil, errInvalidRF3Manifest
	}
	statements := make([]string, 0, count)
	iter, _ := node.ArrayIter()
	for range count {
		value, present := iter.Next()
		if !present || used >= sqldriver.ReplicatedChildSchemaMaxBytes {
			return nil, errInvalidRF3Manifest
		}
		statement, err := rf3ManifestSQLText(value, sqldriver.ReplicatedChildSchemaMaxBytes-used)
		if err != nil {
			return nil, err
		}
		used += len(statement)
		statements = append(statements, statement)
	}
	return statements, nil
}

// SQL declarations may contain quoted identifiers and line breaks. Identity
// strings retain their stricter unescaped grammar; only bounded DDL decodes
// JSON escapes, off the serving path.
func rf3ManifestSQLText(node vibejson.Node, maximum int) (string, error) {
	if value, ok := node.StringBytes(); ok {
		if len(value) == 0 || len(value) > maximum || bytes.IndexByte(value, 0) >= 0 {
			return "", errInvalidRF3Manifest
		}
		return string(value), nil
	}
	if len(node.Raw().Bytes()) > maximum*6+2 {
		return "", errInvalidRF3Manifest
	}
	value, ok := node.AppendText(nil)
	if !ok || len(value) == 0 || len(value) > maximum || bytes.IndexByte(value, 0) >= 0 {
		return "", errInvalidRF3Manifest
	}
	return string(value), nil
}

func parseRF3ChildGlobalIndexes(node vibejson.Node) ([]sqldriver.ReplicatedGlobalIndexRelation, error) {
	count, ok := node.ArrayLen()
	if !ok || count < 1 || count >= replication.MaxRelationsPerBundle {
		return nil, errInvalidRF3Manifest
	}
	result := make([]sqldriver.ReplicatedGlobalIndexRelation, count)
	iter, _ := node.ArrayIter()
	for i := range result {
		value, present := iter.Next()
		if !present {
			return nil, errInvalidRF3Manifest
		}
		fields, ok := value.ObjectIter()
		if !ok {
			return nil, errInvalidRF3Manifest
		}
		value, err := nextRF3Field(&fields, `"relation"`)
		relation, valid := value.Uint64()
		if err != nil || !valid || relation != uint64(i+2) {
			return nil, errInvalidRF3Manifest
		}
		result[i].Relation = uint16(relation)
		value, err = nextRF3Field(&fields, `"table"`)
		if err != nil {
			return nil, err
		}
		if result[i].Table, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return nil, err
		}
		for _, field := range []struct {
			name   string
			target *uint64
		}{
			{`"index_id"`, &result[i].IndexID}, {`"incarnation"`, &result[i].Incarnation},
		} {
			value, err = nextRF3Field(&fields, field.name)
			if err != nil {
				return nil, err
			}
			if *field.target, err = rf3ManifestPositiveUint64(value); err != nil {
				return nil, err
			}
		}
		value, err = nextRF3Field(&fields, `"locator_count"`)
		locator, valid := value.Uint64()
		if err != nil || !valid || locator < 1 || locator > 8 {
			return nil, errInvalidRF3Manifest
		}
		result[i].LocatorCount = uint8(locator)
		value, err = nextRF3Field(&fields, `"unique"`)
		if result[i].Unique, ok = value.Bool(); err != nil || !ok {
			return nil, errInvalidRF3Manifest
		}
		var numbers [5]uint8
		for j, key := range []string{`"key_encoding"`, `"key_arity"`, `"tuple_version"`, `"mapper_version"`, `"bucket_bits"`} {
			value, err = nextRF3Field(&fields, key)
			number, valid := value.Uint64()
			if err != nil || !valid || number < 1 || number > 255 {
				return nil, errInvalidRF3Manifest
			}
			numbers[j] = uint8(number)
		}
		result[i].KeyEncoding, result[i].KeyArity = sqldriver.ReplicatedRelationKeyEncoding(numbers[0]), numbers[1]
		result[i].TupleVersion, result[i].MapperVersion = distribution.TupleVersion(numbers[2]), distribution.MapperVersion(numbers[3])
		result[i].BucketBits = numbers[4]
		if _, _, extra := fields.Next(); extra {
			return nil, errInvalidRF3Manifest
		}
	}
	return result, nil
}

func rf3SplitChildSchemaMatchesRetained(registry rf3ManifestSplitChildRegistry, base sqldriver.ReplicatedShardStoreIdentity) bool {
	return registry.Table == base.UserTable && sqldriver.ValidateReplicatedChildSchema(base,
		registry.CreateTable, registry.SchemaStatements, registry.GlobalIndexes) == nil
}
