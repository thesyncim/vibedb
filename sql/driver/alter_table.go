package driver

import (
	"context"
	"fmt"
	"strings"

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
		var prevFields []schemaFieldMeta
		if t.meta != nil && t.meta.Schema != nil {
			prevFields = append(prevFields, t.meta.Schema.Fields...)
		}
		if err := d.replaceTableStorageLockedContext(
			ctx, definition.Table, t.meta.Indexes, schema, true,
		); err != nil {
			return err
		}
		return d.attachAddedColumnMetadataLocked(definition.Table, statement.Tree().AlterTable, prevFields)
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
		if err := d.replaceTableStorageLockedContext(ctx, definition.Table, t.meta.Indexes, schema, true); err != nil {
			return err
		}
		return d.dropOwnedColumnSequencesLocked(definition.Table, strings.TrimPrefix(definition.Field.Path, "/"))
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
		var renamedMeta schemaFieldMeta
		if t.meta != nil && t.meta.Schema != nil {
			for _, f := range t.meta.Schema.Fields {
				if f.Path == definition.Field.Path {
					renamedMeta = f
					break
				}
			}
		}
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
		if err := d.replaceTableStorageLockedContext(ctx, definition.Table, t.meta.Indexes, schema, true); err != nil {
			return err
		}
		return d.updateRenamedColumnMetadataLocked(
			definition.Table,
			strings.TrimPrefix(definition.Field.Path, "/"),
			definition.NewName,
			newPtr,
			renamedMeta,
		)
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

func (d *database) attachAddedColumnMetadataLocked(
	tableName string,
	alt *sqlast.AlterTableStmt,
	prevFields []schemaFieldMeta,
) error {
	t := d.tables[tableName]
	if t == nil || t.meta == nil || t.meta.Schema == nil {
		return nil
	}
	changed := false
	// Restore prior fields' default/identity metadata preserved across storage rebuild.
	if len(prevFields) > 0 {
		prevByPath := make(map[string]schemaFieldMeta, len(prevFields))
		for _, pf := range prevFields {
			if pf.DefaultSQL != "" || pf.Identity != "" || pf.IdentitySequence != "" {
				prevByPath[pf.Path] = pf
			}
		}
		for i := range t.meta.Schema.Fields {
			if pf, ok := prevByPath[t.meta.Schema.Fields[i].Path]; ok {
				t.meta.Schema.Fields[i].DefaultSQL = pf.DefaultSQL
				t.meta.Schema.Fields[i].Identity = pf.Identity
				t.meta.Schema.Fields[i].IdentitySequence = pf.IdentitySequence
				changed = true
			}
		}
	}
	var newSeqName string
	var newSeqMeta *sequenceMeta
	if alt != nil && alt.Column.Path != nil {
		col := alt.Column
		colPath := string(col.Path.AppendPointer(nil))
		colName := strings.TrimPrefix(colPath, "/")
		for i := range t.meta.Schema.Fields {
			if t.meta.Schema.Fields[i].Path != colPath {
				continue
			}
			if col.HasDefault && col.DefaultSQL != "" {
				t.meta.Schema.Fields[i].DefaultSQL = col.DefaultSQL
				changed = true
			}
			if col.Serial != sqlast.SerialNone || col.Identity != sqlast.IdentityNone {
				if err := checkCatalogSequenceCount(len(d.catalog.Sequences) + 1); err != nil {
					return err
				}
				seqName := d.allocateOwnedSequenceNameLocked(tableName, colName, nil)
				d.seqGen++
				var seqOpts sqlast.SequenceOptions
				defaultDT := "BIGINT"
				if col.Serial != sqlast.SerialNone {
					defaultDT = serialDefaultDataType(col.Serial)
				} else if col.IdentityOptions != nil {
					seqOpts = *col.IdentityOptions
				}
				seqMeta, seqErr := buildSequenceMetaFromOptions(
					seqName, seqOpts, defaultDT, tableName, colName, d.seqGen,
				)
				if seqErr != nil {
					return seqErr
				}
				newSeqName = seqName
				newSeqMeta = seqMeta
				t.meta.Schema.Fields[i].IdentitySequence = seqName
				if col.Serial != sqlast.SerialNone {
					t.meta.Schema.Fields[i].DefaultSQL = "nextval('" + escapeSQLSingleQuote(seqName) + "')"
				} else if col.Identity == sqlast.IdentityAlways {
					t.meta.Schema.Fields[i].Identity = "always"
				} else if col.Identity == sqlast.IdentityByDefault {
					t.meta.Schema.Fields[i].Identity = "by_default"
				}
				changed = true
			}
			break
		}
	}
	if !changed && newSeqMeta == nil {
		return nil
	}
	if newSeqMeta != nil {
		d.catalog.Sequences[newSeqName] = newSeqMeta
	}
	if published, err := d.persistCatalogLocked(); err != nil {
		if !published && newSeqMeta != nil {
			delete(d.catalog.Sequences, newSeqName)
		}
		return err
	}
	if newSeqMeta != nil {
		d.seqMu.Lock()
		if d.sequences == nil {
			d.sequences = make(map[string]*runtimeSequence)
		}
		d.sequences[newSeqName] = &runtimeSequence{
			name:         newSeqName,
			meta:         *newSeqMeta,
			curValue:     newSeqMeta.Start,
			curCalled:    false,
			logRemaining: 0,
		}
		d.seqMu.Unlock()
		_ = d.persistSequenceEntry(newSeqName, newSeqMeta.Start, false, newSeqMeta.Generation)
	}
	d.advanceLayoutEpochLocked()
	return nil
}

func preserveSchemaFieldMetadata(dst, src *schemaMeta) {
	if dst == nil || src == nil || len(dst.Fields) == 0 || len(src.Fields) == 0 {
		return
	}
	byPath := make(map[string]schemaFieldMeta, len(src.Fields))
	for _, f := range src.Fields {
		if f.DefaultSQL != "" || f.Identity != "" || f.IdentitySequence != "" {
			byPath[f.Path] = f
		}
	}
	if len(byPath) == 0 {
		return
	}
	for i := range dst.Fields {
		if prev, ok := byPath[dst.Fields[i].Path]; ok {
			dst.Fields[i].DefaultSQL = prev.DefaultSQL
			dst.Fields[i].Identity = prev.Identity
			dst.Fields[i].IdentitySequence = prev.IdentitySequence
		}
	}
}

func (d *database) dropOwnedColumnSequencesLocked(tableName, colName string) error {
	var dropped []string
	var saved map[string]*sequenceMeta
	for name, meta := range d.catalog.Sequences {
		if meta != nil && meta.OwnerTable == tableName && meta.OwnerColumn == colName {
			if saved == nil {
				saved = make(map[string]*sequenceMeta)
			}
			saved[name] = meta
			dropped = append(dropped, name)
		}
	}
	if len(dropped) == 0 {
		return nil
	}
	for _, name := range dropped {
		delete(d.catalog.Sequences, name)
	}
	if published, err := d.persistCatalogLocked(); err != nil {
		if !published {
			for name, meta := range saved {
				d.catalog.Sequences[name] = meta
			}
		}
		return err
	}
	d.seqMu.Lock()
	for _, name := range dropped {
		if rt, ok := d.sequences[name]; ok {
			rt.mu.Lock()
			rt.dropped = true
			rt.mu.Unlock()
			delete(d.sequences, name)
		}
	}
	d.seqMu.Unlock()
	_ = d.deletePersistedSequenceEntries(dropped)
	return nil
}

func (d *database) updateRenamedColumnMetadataLocked(
	tableName, oldColName, newColName, newPtr string,
	oldField schemaFieldMeta,
) error {
	t := d.tables[tableName]
	if t == nil || t.meta == nil || t.meta.Schema == nil {
		return nil
	}
	changed := false
	if oldField.DefaultSQL != "" || oldField.Identity != "" || oldField.IdentitySequence != "" {
		for i := range t.meta.Schema.Fields {
			if t.meta.Schema.Fields[i].Path == newPtr {
				t.meta.Schema.Fields[i].DefaultSQL = oldField.DefaultSQL
				t.meta.Schema.Fields[i].Identity = oldField.Identity
				t.meta.Schema.Fields[i].IdentitySequence = oldField.IdentitySequence
				changed = true
				break
			}
		}
	}
	var updatedSeqs []string
	for name, meta := range d.catalog.Sequences {
		if meta != nil && meta.OwnerTable == tableName && meta.OwnerColumn == oldColName {
			meta.OwnerColumn = newColName
			updatedSeqs = append(updatedSeqs, name)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if _, err := d.persistCatalogLocked(); err != nil {
		return err
	}
	if len(updatedSeqs) > 0 {
		d.seqMu.RLock()
		for _, name := range updatedSeqs {
			if rt, ok := d.sequences[name]; ok {
				rt.mu.Lock()
				rt.meta.OwnerColumn = newColName
				rt.mu.Unlock()
			}
		}
		d.seqMu.RUnlock()
	}
	d.advanceLayoutEpochLocked()
	return nil
}
