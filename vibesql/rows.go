package vibesql

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/thesyncim/vibedb/query"
)

// Rows, and the JSON-to-driver.Value mapping.
//
// # How a JSON value becomes a driver.Value
//
// driver.Value is a closed set — nil, bool, int64, float64, []byte, string,
// time.Time — and JSON is not. The mapping below is chosen so that no value is
// silently changed on the way out, which rules out the obvious answer for
// numbers.
//
// A number is int64 when it is an integer int64 can hold, and its exact decimal
// digits as []byte otherwise. It is emphatically not float64. This engine
// compares numbers by exact decimal value on purpose: 9007199254740992 and
// 9007199254740993 are two distinct values here, and both are one float64.
// Routing every number through float64 would make a driver that returns the
// wrong integer for a perfectly ordinary 19-digit identifier, and would do it
// silently. Digits cost a []byte and scan into a float64, a string, a
// json.Number, or a big.Int as the caller chooses; a collapsed float cannot be
// uncollapsed. Non-integer numbers take the same path for the same reason —
// 0.1 is not representable in binary floating point, and the document said 0.1.
//
// A string is []byte rather than string, and the reason is lifetime, not taste.
// A projected string cell borrows either the collection's document bytes or the
// execution workspace's decoded-text buffer, both of which are reused by the
// next execution. Handing that out as a Go string would be handing out a string
// whose bytes change later, which is a contract violation no amount of
// documentation repairs. database/sql copies a []byte on Scan into every
// destination but sql.RawBytes, so []byte is both correct and free: the copy
// happens once, at the boundary where the caller decides to keep the value.
//
// An object or an array has no driver.Value analogue at all, so it is its exact
// JSON bytes. That is lossless, it is what the document held, and it scans
// straight into json.Unmarshal or a []byte. Inventing a wrapper type would make
// it unscannable by anything that did not import this package.
//
// Null is nil, and covers the absent path too, because this engine defines an
// absent path and an explicit null as one value. See the package documentation.

// A rows is one open result set. It reads its connection's Exec in place: the
// cells it hands out borrow the collection and the workspace, and database/sql
// copies them at Scan.
type rows struct {
	stmt   *stmt
	cursor query.Cursor
	handle handle
	// scratch formats the computed aggregates that carry no source bytes. It
	// is reset per row, which is exactly the lifetime driver.Rows.Next
	// promises for a []byte it writes into dest.
	scratch []byte
	// schema is the output column metadata, filled lazily because most callers
	// never ask for it.
	schema []query.OutputColumn
	closed bool
}

var (
	_ driver.Rows                           = (*rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*rows)(nil)
)

// Columns returns the output column names, which are the SELECT list's aliases
// where it gave them and the engine's path and aggregate spellings otherwise.
func (r *rows) Columns() []string { return r.stmt.statement.Columns() }

// Close releases the snapshot the execution leased and hands the connection
// back. It is idempotent, because database/sql closes rows on several paths.
func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.handle.close()
	r.stmt.conn.open = false
	if r.stmt.adhoc {
		// Nothing else holds an ad-hoc statement, so its rows own it.
		return r.stmt.Close()
	}
	return nil
}

// Next fills dest with the next row, reporting io.EOF at the end.
//
// The values written into dest borrow storage that stays valid until the next
// call, which is precisely the contract driver.Rows states and database/sql
// honours by copying every []byte at Scan.
func (r *rows) Next(dest []driver.Value) error {
	if r.closed || !r.cursor.Next() {
		return io.EOF
	}
	r.scratch = r.scratch[:0]
	for i := range dest {
		dest[i] = r.value(r.cursor.Cell(i))
	}
	return nil
}

// emptyBytes stands in for a zero-length value whose backing pointer is nil.
//
// The property it defends is that an empty value stays distinguishable from a
// missing one. database/sql clones a []byte into a *[]byte destination and its
// clone of a nil slice is nil, which is also exactly what it writes for NULL —
// so a caller testing the destination for nil, an ordinary thing to write,
// would read an empty string as an absent field.
//
// It is a guard rather than a fix: the engine's string and payload views are
// built with byteview, whose zero-length views keep a real backing pointer and
// so are already non-nil. TestDriverEmptyStringIsNotNull pins the observable
// contract rather than either layer's implementation of it, which is what makes
// this safe to keep without claiming it is load-bearing today.
var emptyBytes = []byte{}

// value maps one cell onto the driver's value domain. See the file comment for
// why each case chooses what it does.
func (r *rows) value(cell query.Cell) driver.Value {
	switch cell.Kind() {
	case query.KindNull:
		return nil
	case query.KindBool:
		b, _ := cell.Bool()
		return b
	case query.KindNumber:
		if i, ok := cell.Int64(); ok {
			return i
		}
		if raw := cell.Payload(); len(raw) != 0 {
			// A projected number keeps the document's exact spelling, which is
			// the most faithful thing there is to hand back.
			return raw
		}
		// A computed aggregate has no source bytes. Formatting it into the
		// row's own buffer keeps the value exact and the row allocation-free
		// once the buffer has grown.
		start := len(r.scratch)
		r.scratch = cell.AppendJSON(r.scratch)
		return r.scratch[start:len(r.scratch):len(r.scratch)]
	case query.KindString:
		text, _ := cell.TextBytes()
		if len(text) == 0 {
			return emptyBytes
		}
		return text
	default:
		if raw := cell.Payload(); len(raw) != 0 {
			return raw
		}
		return emptyBytes
	}
}

// columnSchema returns the output schema, compiling it on first use.
func (r *rows) columnSchema() []query.OutputColumn {
	if r.schema == nil {
		r.schema = r.stmt.statement.AppendSchema(nil)
	}
	return r.schema
}

// ColumnTypeScanType reports the Go type a column scans into.
//
// It is honest rather than flattering, and the two answers differ. A projection
// of a schemaless path has no single type: one row's value may be a number, the
// next a string, the next an object, and the next absent. The only truthful
// answer is any, and claiming []byte or string would be claiming a schema this
// store does not have. COUNT is the one column with a real type — a row tally
// is always an int64 — so it says so.
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	schema := r.columnSchema()
	if index < 0 || index >= len(schema) {
		return anyType
	}
	if schema[index].Reduction == query.ReductionCount {
		return int64Type
	}
	return anyType
}

var (
	anyType   = reflect.TypeOf((*any)(nil)).Elem()
	int64Type = reflect.TypeOf(int64(0))
)

// ColumnTypeDatabaseTypeName names the column's type in this store's own terms.
// A projection is JSON because that is literally what it is; an aggregate is
// the numeric type its reduction produces.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	schema := r.columnSchema()
	if index < 0 || index >= len(schema) {
		return "JSON"
	}
	switch schema[index].Reduction {
	case query.ReductionNone:
		return "JSON"
	case query.ReductionCount:
		return "BIGINT"
	default:
		return "NUMERIC"
	}
}

// ColumnTypeNullable reports whether a column can be null.
//
// Everything a schemaless store projects can be: a path may be absent, and this
// engine makes absent and null one value. The reductions can be too — SUM, AVG,
// MIN, and MAX over no contributing row are null. COUNT alone never is, because
// counting nothing is zero.
func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	schema := r.columnSchema()
	if index < 0 || index >= len(schema) {
		return true, true
	}
	return schema[index].Reduction != query.ReductionCount, true
}
