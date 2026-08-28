package driver

import (
	"context"
	"slices"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

// The catalog-read door.
//
// Protocol adapters need to answer client-side introspection — a psql
// backslash command, a driver's table browser — from the catalog this package
// already owns, without executing catalog SQL the bounded dialect refuses.
// Everything below is a read-only copy of what CREATE TABLE and CREATE INDEX
// persisted; nothing here can observe uncommitted state, because DDL is not
// transactional in this runtime and publishes atomically under the catalog
// lock this read takes.

// A TableInfo describes one cataloged table: its name, its primary-key path,
// its declared columns in the catalog's canonical path-sorted order (the
// order the compiled schema persists), and its exact indexes in creation
// order.
type TableInfo struct {
	Name string
	// PrimaryKey is the RFC 6901 pointer the storage key is derived from,
	// exactly as the catalog persists it (for example "/id").
	PrimaryKey string
	Columns    []ColumnInfo
	Indexes    []IndexInfo
}

// A ColumnInfo describes one declared column constraint.
type ColumnInfo struct {
	// Path is the column's RFC 6901 pointer, exactly as persisted.
	Path string
	// Types is the set of JSON types the column accepts, in the SQL dialect's
	// own vocabulary. A nullable column carries sqlast.TypeNull in the set.
	Types sqlast.JSONType
	// Required reports whether the path must be present in every document.
	// NOT NULL in this dialect is Required together with an absent TypeNull.
	Required bool
}

// An IndexInfo describes one declared exact index.
type IndexInfo struct {
	Name string
	// Paths are the indexed RFC 6901 pointers, in declaration order.
	Paths []string
}

// Tables returns a copy of the catalog: every table, sorted by name.
//
// The result is a snapshot taken under the catalog read lock and shares no
// storage with the catalog, so a caller may hold it across later DDL without
// observing mutation. This is a cold-path read for introspection; it is not
// allocation-free and is not meant to sit on a query path.
func (s *Session) Tables(ctx context.Context) ([]TableInfo, error) {
	if err := s.live(); err != nil {
		return nil, err
	}
	d := s.conn.db
	if err := rlockContext(ctx, &d.mu); err != nil {
		return nil, err
	}
	defer d.mu.RUnlock()
	if d.closed {
		return nil, ErrDatabaseClosed
	}
	out := make([]TableInfo, 0, len(d.catalog.Tables))
	for name, meta := range d.catalog.Tables {
		out = append(out, tableInfoFromMeta(name, meta))
	}
	slices.SortFunc(out, func(a, b TableInfo) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out, nil
}

func tableInfoFromMeta(name string, meta *tableMeta) TableInfo {
	info := TableInfo{Name: name, PrimaryKey: meta.PrimaryKey}
	if meta.Schema != nil {
		info.Columns = make([]ColumnInfo, 0, len(meta.Schema.Fields))
		for _, field := range meta.Schema.Fields {
			info.Columns = append(info.Columns, ColumnInfo{Path: field.Path, Types: jsonTypeFromStore(store.SchemaType(field.Types)), Required: field.Required})
		}
	}
	for _, index := range meta.Indexes {
		info.Indexes = append(info.Indexes, IndexInfo{Name: index.Name, Paths: slices.Clone(index.Paths)})
	}
	return info
}

// jsonTypeFromStore maps the persisted store type set back onto the SQL
// dialect's vocabulary one bit at a time. The two enumerations are declared in
// the same order today, but sqlast documents that a consumer must not depend
// on that numerically; mapping each bit explicitly turns a renumbering on
// either side into a visible bug here rather than a silently permuted schema.
func jsonTypeFromStore(t store.SchemaType) sqlast.JSONType {
	var out sqlast.JSONType
	if t&store.SchemaNull != 0 {
		out |= sqlast.TypeNull
	}
	if t&store.SchemaBool != 0 {
		out |= sqlast.TypeBool
	}
	if t&store.SchemaNumber != 0 {
		out |= sqlast.TypeNumber
	}
	if t&store.SchemaInteger != 0 {
		out |= sqlast.TypeInteger
	}
	if t&store.SchemaString != 0 {
		out |= sqlast.TypeString
	}
	if t&store.SchemaArray != 0 {
		out |= sqlast.TypeArray
	}
	if t&store.SchemaObject != 0 {
		out |= sqlast.TypeObject
	}
	return out
}
