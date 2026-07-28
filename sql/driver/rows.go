package driver

import (
	sqldriver "database/sql/driver"
	"io"
	"reflect"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

type rows struct {
	stmt     *stmt
	cursor   query.Cursor
	snapshot *durable.Snapshot
	scratch  []byte
	schema   []query.OutputColumn
	closed   bool
}

var (
	_ sqldriver.Rows                           = (*rows)(nil)
	_ sqldriver.RowsColumnTypeScanType         = (*rows)(nil)
	_ sqldriver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ sqldriver.RowsColumnTypeNullable         = (*rows)(nil)
)

func (r *rows) Columns() []string { return r.stmt.query.Columns() }

func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var err error
	if r.snapshot != nil {
		err = r.snapshot.Close()
	}
	r.stmt.conn.open = false
	if r.stmt.adhoc {
		err = errorsJoin(err, r.stmt.Close())
	}
	return err
}

func errorsJoin(a, b error) error {
	if a != nil {
		return a
	}
	return b
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
	if r.schema == nil {
		r.schema = r.stmt.query.AppendSchema(nil)
	}
	return r.schema
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
