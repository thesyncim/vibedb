package driver

import (
	sqldriver "database/sql/driver"
	"io"
	"reflect"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

type rows struct {
	conn     *conn
	stmt     *stmt
	cursor   query.Cursor
	snapshot *durable.Snapshot
	scratch  []byte
	schema   []query.OutputColumn
	schemaOK bool
	closed   bool
}

var (
	_ sqldriver.Rows                           = (*rows)(nil)
	_ sqldriver.RowsColumnTypeScanType         = (*rows)(nil)
	_ sqldriver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ sqldriver.RowsColumnTypeNullable         = (*rows)(nil)
)

func (r *rows) Columns() []string {
	if r.stmt != nil && r.stmt.explain {
		return []string{"QUERY PLAN"}
	}
	if r.stmt == nil || r.stmt.query == nil {
		return nil
	}
	return r.stmt.query.Columns()
}

func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var err error
	if r.snapshot != nil {
		err = r.snapshot.Close()
		r.snapshot = nil
	}
	if r.conn != nil {
		r.conn.open = false
	}
	// The connection owns and reuses this rows value. A temporary database/sql
	// statement is otherwise reachable from the idle connection forever,
	// including its parsed SQL arena and compiler high-water storage. The
	// cursor also points at that statement's result. Break both ownership
	// edges at the lifecycle boundary; resetRows installs fresh ones.
	r.cursor = query.Cursor{}
	r.conn = nil
	r.stmt = nil
	// Output headers borrow the compiled statement's immutable plan storage.
	// ColumnTypes may have populated this connection-owned cache, so merely
	// dropping stmt would still retain the plan through these strings. Clear
	// the elements before keeping the capacity for the next query.
	clear(r.schema)
	r.schema = r.schema[:0]
	r.schemaOK = false
	return err
}

func (r *rows) Next(dest []sqldriver.Value) error {
	if r.closed || !r.cursor.Next() {
		return io.EOF
	}
	r.scratch = r.scratch[:0]
	for i := range dest {
		dest[i] = r.value(r.cursor.Cell(i))
	}
	return nil
}

func (r *rows) value(cell query.Cell) sqldriver.Value {
	switch cell.Kind() {
	case query.TypeNull:
		return nil
	case query.TypeBool:
		value, _ := cell.Bool()
		return value
	case query.TypeNumber:
		if value, ok := cell.Int64(); ok {
			return value
		}
		if raw := cell.Payload(); len(raw) != 0 {
			return raw
		}
		start := len(r.scratch)
		r.scratch = cell.AppendJSON(r.scratch)
		return r.scratch[start:]
	case query.TypeString:
		value, _ := cell.TextBytes()
		if len(value) == 0 {
			return []byte{}
		}
		return value
	default:
		if raw := cell.Payload(); len(raw) != 0 {
			return raw
		}
		return []byte{}
	}
}

func (r *rows) columnSchema() []query.OutputColumn {
	if r.stmt != nil && r.stmt.explain {
		if !r.schemaOK {
			r.schema = append(r.schema[:0], query.OutputColumn{
				Header: "QUERY PLAN", Type: query.TypeString,
			})
			r.schemaOK = true
		}
		return r.schema
	}
	if r.stmt == nil || r.stmt.query == nil {
		return nil
	}
	if !r.schemaOK {
		r.schema = r.stmt.query.AppendSchema(r.schema[:0])
		r.schemaOK = true
	}
	return r.schema
}

func (c *conn) resetRows(
	statement *stmt,
	cursor query.Cursor,
	snapshot *durable.Snapshot,
) *rows {
	r := &c.rowset
	r.conn = c
	r.stmt = statement
	r.cursor = cursor
	r.snapshot = snapshot
	r.scratch = r.scratch[:0]
	r.schema = r.schema[:0]
	r.schemaOK = false
	r.closed = false
	return r
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if r.stmt != nil && r.stmt.explain {
		return reflect.TypeFor[string]()
	}
	schema := r.columnSchema()
	if index >= 0 && index < len(schema) {
		switch {
		case schema[index].Reduction == query.ReductionCount ||
			schema[index].Reduction == query.ReductionWindowInteger:
			return reflect.TypeFor[int64]()
		case schema[index].Type == query.TypeString:
			return reflect.TypeFor[[]byte]()
		case schema[index].Type == query.TypeBool:
			return reflect.TypeFor[bool]()
		}
	}
	return reflect.TypeFor[any]()
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if r.stmt != nil && r.stmt.explain {
		return "TEXT"
	}
	schema := r.columnSchema()
	if index < 0 || index >= len(schema) {
		return "JSON"
	}
	switch schema[index].Type {
	case query.TypeString:
		return "TEXT"
	case query.TypeBool:
		return "BOOLEAN"
	case query.TypeNumber:
		return "NUMERIC"
	}
	if schema[index].Reduction == query.ReductionNone {
		return "JSON"
	}
	if schema[index].Reduction == query.ReductionCount ||
		schema[index].Reduction == query.ReductionWindowInteger {
		return "BIGINT"
	}
	return "NUMERIC"
}

func (r *rows) ColumnTypeNullable(index int) (bool, bool) {
	if r.stmt != nil && r.stmt.explain {
		return false, true
	}
	schema := r.columnSchema()
	if index < 0 || index >= len(schema) {
		return true, true
	}
	reduction := schema[index].Reduction
	return reduction != query.ReductionCount &&
		reduction != query.ReductionWindowInteger, true
}
