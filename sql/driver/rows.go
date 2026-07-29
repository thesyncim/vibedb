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
	case query.KindNull:
		return nil
	case query.KindBool:
		value, _ := cell.Bool()
		return value
	case query.KindNumber:
		if value, ok := cell.Int64(); ok {
			return value
		}
		if raw := cell.Payload(); len(raw) != 0 {
			return raw
		}
		start := len(r.scratch)
		r.scratch = cell.AppendJSON(r.scratch)
		return r.scratch[start:]
	case query.KindString:
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
	schema := r.columnSchema()
	if index >= 0 && index < len(schema) && schema[index].Reduction == query.ReductionCount {
		return reflect.TypeFor[int64]()
	}
	return reflect.TypeFor[any]()
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	schema := r.columnSchema()
	if index < 0 || index >= len(schema) || schema[index].Reduction == query.ReductionNone {
		return "JSON"
	}
	if schema[index].Reduction == query.ReductionCount {
		return "BIGINT"
	}
	return "NUMERIC"
}

func (r *rows) ColumnTypeNullable(index int) (bool, bool) {
	schema := r.columnSchema()
	return index < 0 || index >= len(schema) || schema[index].Reduction != query.ReductionCount, true
}
