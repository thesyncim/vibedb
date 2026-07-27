package pgwire

import (
	"strconv"

	"github.com/thesyncim/vibedb/query"
)

// RowDescription, DataRow, and the type mapping — the one genuinely new
// decision in this package.
//
// # Why every projected column is json (OID 114)
//
// RowDescription must declare one type OID per column before a single row has
// been read, and this store is schemaless: a path holds a number in one
// document and a string in the next, a projection has no declared type, and
// there is no moment at which the server could learn one that is not after the
// point where it has already had to answer. PostgreSQL has no "any" type in a
// result set, so a type must be picked that every value can honestly be.
//
// Four candidates were considered, and the constraint that decides between them
// is not expressiveness — it is which of them can be encoded in *both* wire
// formats without a chance of silent corruption.
//
// text (OID 25) is the obvious answer and is the wrong one. It is a lossy cast
// performed by the server: the string "123" and the number 123 both arrive as
// the three bytes "123", and a client has no way left to tell them apart. In a
// store whose whole point is that a path's type is per-document, erasing type
// at the boundary erases the only information the client needed.
//
// numeric (OID 1700) is right for an aggregate and impossible for a projection,
// which may not be a number at all. It also has a nontrivial binary encoding —
// a base-10000 digit vector with a weight, a sign, and a display scale — and
// this package has no PostgreSQL to validate an implementation of it against.
// Shipping an unvalidated numeric_send is precisely the failure the format
// question is dangerous for: a subtly wrong digit grouping produces a number
// the client accepts and misreads. It is not used here.
//
// jsonb (OID 3802) describes a normalized document: PostgreSQL's jsonb sorts
// object keys, drops duplicate keys, and does not preserve number spelling.
// This engine preserves all three — key order, duplicates, and the document's
// exact decimal digits — so declaring jsonb would be declaring a normalization
// that did not happen.
//
// json (OID 114) is what these values actually are. It is the only candidate
// whose domain contains every kind a cell can hold: null, boolean, number,
// string, array, and object all have a JSON spelling, and the spelling is the
// document's own bytes rather than a re-rendering. Exact decimal survives
// because a projected number's text is the digits the document was written
// with; nothing passes through float64 on this path. Containers, which have no
// scalar OID at all, need no special case. And — the property that settles it —
// json's *binary* format is byte-for-byte its text format. PostgreSQL's
// json_send emits the value's text with no header, no length, and no
// transformation, so a client asking for binary and a client asking for text
// receive the same bytes, and there is no encoder here that could get binary
// subtly wrong. Binary is therefore supported for every column this server can
// produce rather than refused, which removes the worst failure mode available
// instead of documenting it.
//
// The exception is COUNT, which is declared int8 (OID 20). A row tally is
// genuinely an int64 — it is the one column in this dialect with a real static
// type — and clients want to scan it into an integer rather than through a JSON
// decoder. int8's binary format is eight big-endian bytes, which is the other
// encoding this package can be certain of.
//
// # What the mapping costs
//
// A string arrives JSON-quoted. `SELECT name` yields `"alice"`, seven bytes,
// not five. Clients that understand OID 114 — psycopg, node-postgres, pgx —
// parse it back into a native string automatically; a client that treats every
// column as raw text sees the quotes. That is the price of not erasing the
// difference between the string "123" and the number 123, and it is paid in a
// place the client can undo.
//
// A client's JSON decoder may narrow a number that this server did not. The
// bytes on the wire are exact — 9007199254740993 is sent as those seventeen
// digits — but a JavaScript client parsing them with JSON.parse gets a float64
// and loses the last digit. That loss is the client's decoder, not this
// encoding, and it is avoidable on every client by reading the column as raw
// text or as a lazy JSON value. Declaring numeric would have moved the loss
// rather than removed it, because a schemaless column cannot be declared
// numeric in the first place.
//
// SUM, AVG, MIN, and MAX are json rather than numeric, so a client that wants
// a decimal type gets a JSON number. There is a second, larger caveat that
// belongs to the engine and not to this package: a computed aggregate that is
// not an integer is held as a float64 inside [query.Cell] and formatted from
// there, so AVG's exactness is bounded by float64 before this code ever sees
// it. Projected numbers, which are the ones the exactness guarantee is about,
// keep their document bytes and are unaffected.
//
// # Absent, null, and NULL
//
// This engine defines an absent path and an explicit JSON null as one value:
// both produce a null cell and both satisfy IS NULL. The protocol has exactly
// one NULL, encoded as a length of -1, and that is what both become. The
// distinction is not destroyed, it is simply not carried by the value: a client
// that needs it asks for it in the statement, with this dialect's IS MISSING,
// which is true only for a path that resolves to nothing.
//
// One consequence is worth stating because it surprises people: a NULL from
// this server never means "the JSON literal null was stored here" and never
// means "the field was absent". It means one of the two and does not say which.

// A columnType is how one result column is declared on the wire.
type columnType struct {
	oid int32
	// size is the type's fixed byte width, or -1 for a variable-width type. It
	// is advisory — the DataRow carries its own length — but clients do read it
	// and a wrong value there is a wrong value.
	size int16
}

var (
	typeJSON = columnType{oid: oidJSON, size: -1}
	typeInt8 = columnType{oid: oidInt8, size: 8}
	typeText = columnType{oid: oidText, size: -1}
)

// A column is one entry in a RowDescription.
type column struct {
	name string
	typ  columnType
}

// columnsFor maps a statement's output schema onto wire columns.
func columnsFor(dst []column, names []string, schema []query.OutputColumn) []column {
	dst = dst[:0]
	for i, name := range names {
		typ := typeJSON
		if i < len(schema) && schema[i].Reduction == query.ReductionCount {
			typ = typeInt8
		}
		dst = append(dst, column{name: name, typ: typ})
	}
	return dst
}

// rowDescription writes the result schema.
//
// The table OID and column attribute number are zero because these columns do
// not come from a table in the sense the fields mean: a projection is a path
// into a document, and claiming an attnum would be claiming a catalog entry
// that does not exist. The type modifier is -1, which is "no modifier" for
// every type here.
//
// The format code in each field must be the format the DataRow will actually
// use. Sending a description that says text and rows that are binary is the
// single most damaging inconsistency available in this protocol, because the
// client decodes without complaint and gets nonsense, so the formats are passed
// in from the same slice the row encoder reads.
func (w *writer) rowDescription(cols []column, formats []int16) {
	w.begin(msgRowDescription)
	w.int16(len(cols))
	for i, c := range cols {
		w.str(c.name)
		w.int32(0) // table OID: not from a table
		w.int16(0) // column attribute number: not from a table
		w.int32(c.typ.oid)
		w.int16(int(c.typ.size))
		w.int32(-1) // type modifier
		w.int16(int(formatFor(formats, i)))
	}
	w.end()
}

// appendCell appends one cell's wire encoding for the given declared type and
// format, or reports that it cannot.
//
// It never guesses. A cell whose kind does not match the type the column was
// declared as is an internal inconsistency between columnsFor and the executor,
// and the only safe response is an error the client can see, because the
// alternative is a value the client decodes successfully and reads wrongly.
func appendCell(dst []byte, cell query.Cell, typ columnType, format int16) ([]byte, error) {
	switch typ.oid {
	case oidInt8:
		v, ok := cell.Int64()
		if !ok {
			return dst, newError(sqlstateInternalError,
				"a column declared int8 produced a value that is not an integer")
		}
		if format == formatBinary {
			return appendUint64(dst, uint64(v)), nil
		}
		return strconv.AppendInt(dst, v, 10), nil

	case oidJSON:
		// Text and binary are the same bytes for json; see the file comment.
		// The length check catches the one way this could go wrong quietly —
		// a cell that renders to nothing would be sent as a zero-length value,
		// which is not valid JSON and which a client would fail to parse far
		// from here.
		start := len(dst)
		dst = cell.AppendJSON(dst)
		if len(dst) == start {
			return dst[:start], newError(sqlstateInternalError,
				"a result cell produced no JSON encoding")
		}
		return dst, nil

	default:
		return dst, newError(sqlstateInternalError, "unhandled result column type")
	}
}

// appendUint64 appends v in big-endian order. It is written out rather than
// taken from encoding/binary so this file has no import that could make a
// reader wonder whether a different endianness is in play; the protocol is
// big-endian everywhere and nowhere else.
func appendUint64(dst []byte, v uint64) []byte {
	return append(dst,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// A rowEncoder holds the per-row storage a session reuses across every row of
// every result set it serves.
//
// Both fields exist because a DataRow needs each value's length before the
// value, and every encoder here appends: the values are laid down end to end in
// values, spans records where each one stopped, and the message is written from
// the two. Retaining them across rows is what makes an encoded row free of
// allocation once a session has served one result set, which is the same
// steady-state argument the executor and the database/sql driver make.
type rowEncoder struct {
	values []byte
	spans  []int32
}

// nullSpan marks a column that is SQL NULL, which the protocol encodes as a
// length of -1 and no bytes. It is a sentinel in the span list rather than a
// parallel bool slice because -1 is already the wire's own spelling for it.
const nullSpan = int32(-1)

// row writes one DataRow.
func (e *rowEncoder) row(w *writer, cells []query.Cell, cols []column, formats []int16) error {
	e.values = e.values[:0]
	e.spans = e.spans[:0]
	for i := range cells {
		if cells[i].IsNull() {
			e.spans = append(e.spans, nullSpan)
			continue
		}
		var err error
		e.values, err = appendCell(e.values, cells[i], cols[i].typ, formatFor(formats, i))
		if err != nil {
			return err
		}
		e.spans = append(e.spans, int32(len(e.values)))
	}
	w.begin(msgDataRow)
	w.int16(len(cells))
	prev := int32(0)
	for _, end := range e.spans {
		if end == nullSpan {
			w.int32(-1)
			continue
		}
		w.int32(end - prev)
		w.raw(e.values[prev:end])
		prev = end
	}
	w.end()
	return nil
}

// fixedRow writes one DataRow of values already held as text, a nil entry
// meaning NULL. It serves the fixed result sets in command.go — SHOW,
// version(), a literal SELECT — which have no cells and no schemaless types.
//
// A text column needs no conversion in either format, because text's binary
// encoding is its bytes unchanged. An int8 column asked for in binary is
// converted back from its digits, which is exact for every value that reached
// this path (they are all produced here from an int64 in the first place) and
// is the only place in this package where a value is re-parsed rather than
// carried.
func (w *writer) fixedRow(cols []column, values []*string, formats []int16) {
	w.begin(msgDataRow)
	w.int16(len(values))
	for i, v := range values {
		if v == nil {
			w.int32(-1)
			continue
		}
		if cols[i].typ.oid == oidInt8 && formatFor(formats, i) == formatBinary {
			n, err := strconv.ParseInt(*v, 10, 64)
			if err == nil {
				w.int32(8)
				w.buf = appendUint64(w.buf, uint64(n))
				continue
			}
		}
		w.int32(int32(len(*v)))
		w.rawString(*v)
	}
	w.end()
}
