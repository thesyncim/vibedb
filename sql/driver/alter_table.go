package driver

import (
	"context"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

const primaryKeySchemaTypes = store.SchemaBool | store.SchemaNumber | store.SchemaString

// compileAddedColumnSchema builds the complete immutable target schema for one
// additive ALTER. A schemaless SQL table still has a driver-enforced scalar
// primary key, so its first declared column also makes that invariant explicit.
func compileAddedColumnSchema(
	meta *tableMeta,
	field store.SchemaField,
	ifNotExists bool,
) (*store.Schema, bool, error) {
	if meta == nil {
		return nil, false, fmt.Errorf("vibedb: ALTER TABLE has no table metadata")
	}
	definition := store.SchemaDefinition{Root: store.SchemaObject}
	if meta.Schema == nil {
		definition.Fields = append(definition.Fields, store.SchemaField{
			Path: meta.PrimaryKey, Types: primaryKeySchemaTypes, Required: true,
		})
	} else {
		definition.Root = store.SchemaType(meta.Schema.Root)
		definition.Fields = make([]store.SchemaField, len(meta.Schema.Fields))
		for i := range meta.Schema.Fields {
			definition.Fields[i] = store.SchemaField{
				Path:     meta.Schema.Fields[i].Path,
				Types:    store.SchemaType(meta.Schema.Fields[i].Types),
				Required: meta.Schema.Fields[i].Required,
			}
		}
	}
	for i := range definition.Fields {
		if definition.Fields[i].Path != field.Path {
			continue
		}
		if ifNotExists {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("%w: %q", ErrColumnExists, field.Path)
	}
	definition.Fields = append(definition.Fields, field)
	schema, err := store.CompileSchema(definition)
	if err != nil {
		return nil, false, fmt.Errorf("vibedb: ALTER TABLE ADD COLUMN: %w", err)
	}
	if err := validatePrimarySchema(meta.PrimaryKey, schema); err != nil {
		return nil, false, fmt.Errorf("vibedb: ALTER TABLE primary key: %w", err)
	}
	return schema, false, nil
}

func (d *database) alterTableAddColumnStorageLockedContext(
	ctx context.Context,
	statement *query.DMLStatement,
) error {
	definition, err := statement.LowerAlterTable()
	if err != nil {
		return err
	}
	t, ok := d.tables[definition.Table]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTableNotFound, definition.Table)
	}
	schema, noOp, err := compileAddedColumnSchema(
		t.meta, definition.Field, definition.IfNotExists,
	)
	if err != nil || noOp {
		return err
	}
	return d.replaceTableStorageLockedContext(
		ctx, definition.Table, t.meta.Indexes, schema, true,
	)
}
