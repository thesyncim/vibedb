package driver

import (
	"context"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
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
		if definition.IfExists {
			return nil
		}
		return fmt.Errorf("%w: %q", ErrTableNotFound, definition.Table)
	}
	switch definition.Action {
	case sqlast.AlterAddColumn:
		schema, noOp, err := compileAddedColumnSchema(
			t.meta, definition.Field, definition.IfNotExists,
		)
		if err != nil || noOp {
			return err
		}
		return d.replaceTableStorageLockedContext(
			ctx, definition.Table, t.meta.Indexes, schema, true,
		)
	case sqlast.AlterDropColumn:
		if definition.Field.Path == t.meta.PrimaryKey {
			return fmt.Errorf("vibedb: ALTER TABLE cannot drop primary key column %q", definition.Field.Path)
		}
		schema, noOp, err := compileMutatedColumnSchema(t.meta, definition.Field.Path, definition.IfExists, func(fields []store.SchemaField, idx int) ([]store.SchemaField, error) {
			return append(fields[:idx], fields[idx+1:]...), nil
		})
		if err != nil || noOp {
			return err
		}
		return d.replaceTableStorageLockedContext(ctx, definition.Table, t.meta.Indexes, schema, true)
	case sqlast.AlterSetNotNull:
		schema, noOp, err := compileMutatedColumnSchema(t.meta, definition.Field.Path, false, func(fields []store.SchemaField, idx int) ([]store.SchemaField, error) {
			fields[idx].Required = true
			fields[idx].Types &^= store.SchemaNull
			return fields, nil
		})
		if err != nil || noOp {
			return err
		}
		return d.replaceTableStorageLockedContext(ctx, definition.Table, t.meta.Indexes, schema, true)
	case sqlast.AlterDropNotNull:
		if definition.Field.Path == t.meta.PrimaryKey {
			return fmt.Errorf("vibedb: ALTER TABLE cannot drop NOT NULL on primary key column %q", definition.Field.Path)
		}
		schema, noOp, err := compileMutatedColumnSchema(t.meta, definition.Field.Path, false, func(fields []store.SchemaField, idx int) ([]store.SchemaField, error) {
			fields[idx].Required = false
			fields[idx].Types |= store.SchemaNull
			return fields, nil
		})
		if err != nil || noOp {
			return err
		}
		return d.replaceTableStorageLockedContext(ctx, definition.Table, t.meta.Indexes, schema, true)
	case sqlast.AlterSetType:
		schema, noOp, err := compileMutatedColumnSchema(t.meta, definition.Field.Path, false, func(fields []store.SchemaField, idx int) ([]store.SchemaField, error) {
			newTypes := definition.Field.Types
			if !fields[idx].Required {
				newTypes |= store.SchemaNull
			}
			fields[idx].Types = newTypes
			return fields, nil
		})
		if err != nil || noOp {
			return err
		}
		return d.replaceTableStorageLockedContext(ctx, definition.Table, t.meta.Indexes, schema, true)
	case sqlast.AlterRenameColumn:
		if definition.Field.Path == t.meta.PrimaryKey {
			return fmt.Errorf("vibedb: ALTER TABLE cannot rename primary key column %q", definition.Field.Path)
		}
		newPtr := "/" + definition.NewName
		schema, noOp, err := compileMutatedColumnSchema(t.meta, definition.Field.Path, false, func(fields []store.SchemaField, idx int) ([]store.SchemaField, error) {
			for i := range fields {
				if fields[i].Path == newPtr {
					return nil, fmt.Errorf("%w: %q", ErrColumnExists, newPtr)
				}
			}
			fields[idx].Path = newPtr
			return fields, nil
		})
		if err != nil || noOp {
			return err
		}
		return d.replaceTableStorageLockedContext(ctx, definition.Table, t.meta.Indexes, schema, true)
	case sqlast.AlterSetDefault:
		if alt := statement.Tree().AlterTable; alt != nil && t.meta != nil && t.meta.Schema != nil && alt.Column.Path != nil {
			ptr := string(alt.Column.Path.AppendPointer(nil))
			for i := range t.meta.Schema.Fields {
				if t.meta.Schema.Fields[i].Path == ptr {
					t.meta.Schema.Fields[i].DefaultSQL = alt.Column.DefaultSQL
					if _, err := d.persistCatalogLocked(); err != nil {
						return err
					}
					d.advanceLayoutEpochLocked()
					return nil
				}
			}
		}
		return nil
	case sqlast.AlterDropDefault:
		if alt := statement.Tree().AlterTable; alt != nil && t.meta != nil && t.meta.Schema != nil && alt.Column.Path != nil {
			ptr := string(alt.Column.Path.AppendPointer(nil))
			for i := range t.meta.Schema.Fields {
				if t.meta.Schema.Fields[i].Path == ptr {
					t.meta.Schema.Fields[i].DefaultSQL = ""
					if _, err := d.persistCatalogLocked(); err != nil {
						return err
					}
					d.advanceLayoutEpochLocked()
					return nil
				}
			}
		}
		return nil
	case sqlast.AlterAddConstraint:
		if alt := statement.Tree().AlterTable; alt != nil && t.meta != nil && alt.Column.CheckSQL != "" {
			t.meta.Checks = append(t.meta.Checks, alt.Column.CheckSQL)
			if _, err := d.persistCatalogLocked(); err != nil {
				return err
			}
			d.advanceLayoutEpochLocked()
		}
		return nil
	case sqlast.AlterDropConstraint:
		return nil
	case sqlast.AlterRenameTable:
		if _, exists := d.tables[definition.NewName]; exists {
			return fmt.Errorf("%w: %q", ErrTableExists, definition.NewName)
		}
		d.tables[definition.NewName] = t
		delete(d.tables, definition.Table)
		if d.catalog.Tables != nil {
			d.catalog.Tables[definition.NewName] = d.catalog.Tables[definition.Table]
			delete(d.catalog.Tables, definition.Table)
		}
		_, err := d.persistCatalogLocked()
		return err
	default:
		return fmt.Errorf("vibedb: unsupported ALTER TABLE action %d", definition.Action)
	}
}

func compileMutatedColumnSchema(
	meta *tableMeta,
	path string,
	ifExists bool,
	mutate func(fields []store.SchemaField, idx int) ([]store.SchemaField, error),
) (*store.Schema, bool, error) {
	if meta == nil || meta.Schema == nil {
		if ifExists {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("%w: %q", query.ErrUndefinedColumn, path)
	}
	fields := make([]store.SchemaField, len(meta.Schema.Fields))
	idx := -1
	for i := range meta.Schema.Fields {
		fields[i] = store.SchemaField{
			Path:     meta.Schema.Fields[i].Path,
			Types:    store.SchemaType(meta.Schema.Fields[i].Types),
			Required: meta.Schema.Fields[i].Required,
		}
		if fields[i].Path == path {
			idx = i
		}
	}
	if idx < 0 {
		if ifExists {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("%w: %q", query.ErrUndefinedColumn, path)
	}
	nextFields, err := mutate(fields, idx)
	if err != nil {
		return nil, false, err
	}
	schema, err := store.CompileSchema(store.SchemaDefinition{
		Root:   store.SchemaType(meta.Schema.Root),
		Fields: nextFields,
	})
	if err != nil {
		return nil, false, fmt.Errorf("vibedb: ALTER TABLE: %w", err)
	}
	if err := validatePrimarySchema(meta.PrimaryKey, schema); err != nil {
		return nil, false, fmt.Errorf("vibedb: ALTER TABLE primary key: %w", err)
	}
	return schema, false, nil
}
