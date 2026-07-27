package query

import (
	"fmt"
	"math"
	"strconv"

	"github.com/thesyncim/vibejson/x/byteview"
)

// A Result is the column-oriented output of a query: one ResultColumn per
// selected column, in Select order, each holding one Cell per result row.
// RowCount is the number of rows (the length every column shares). A pure
// projection yields one row per surviving document; a query with aggregates
// and no GROUP BY yields exactly one row; a GROUP BY yields one row per group.
//
// Cells that project a document value borrow the Segment's storage, exactly
// like the RawValue they came from. RunInto may additionally place decoded
// escaped text in its [Exec]'s Workspace. Computed aggregates remain typed and
// borrow no formatted-number arena. Copy projected Cell.JSON and, when needed,
// Cell.TextBytes before the Segment or the Exec is reused if a cell must
// outlive that borrowing boundary. A [FromFile] execution instead copies
// selected values into Result-owned backing, so its cells survive snapshot
// close and page eviction.
type Result struct {
	Columns  []ResultColumn
	RowCount int
	fileData []byte
}

// Release drops all storage retained by r. Reusing r through [Query.RunInto]
// normally gives better throughput; Release is useful after an unusually large
// result should not pin its high-water capacity. [Exec.Release] does this and
// the rest of an execution's retained storage in one call.
func (r *Result) Release() {
	if r == nil {
		return
	}
	for i := range r.Columns {
		clear(r.Columns[i].Cells)
		r.Columns[i] = ResultColumn{}
	}
	r.Columns = nil
	r.RowCount = 0
	r.fileData = nil
}

// A ResultColumn is one output column: its Header (the projection path or the
// aggregate spelling, e.g. "sum(price)") and its Cells, one per row. Header is
// display/compatibility metadata; its stable execution and transport ID is the
// column's ordinal, available before execution through [Query.AppendSchema].
type ResultColumn struct {
	Header string
	Cells  []Cell
}

// Column returns the result column with the given header, and whether one
// exists. Headers are unique within a Result.
func (r Result) Column(header string) (ResultColumn, bool) {
	for _, c := range r.Columns {
		if c.Header == header {
			return c, true
		}
	}
	return ResultColumn{}, false
}

// A Cell is one value in a Result: a projected document value or a computed
// aggregate. Its typed accessors report false for the wrong kind, matching the
// core RawValue/Node accessors. JSON returns the exact bytes for a projected
// value and a formatted encoding for a computed one. The representation uses
// one tagged value word rather than parallel integer, float, and bool fields;
// it occupies 56 bytes on 64-bit targets.
type Cell struct {
	// raw and text are the only dual representation a cell can need: raw keeps
	// exact JSON while text keeps a decoded JSON string. Numeric and boolean
	// values share one tagged word instead of retaining parallel Go values.
	raw  []byte
	text string
	word uint64
	kind CellKind
	flag cellFlag
	// version is zero for core JSON types and participates in the identity of
	// an extension type. It occupies existing alignment padding, so making the
	// output model extensible does not enlarge Cell on 64-bit targets.
	version uint16
}

type cellFlag uint8

const (
	cellInteger cellFlag = 1 << iota
	cellTrue
)

var (
	nullBytes  = []byte("null")
	trueBytes  = []byte("true")
	falseBytes = []byte("false")
)

// cellFromScalar builds a projection cell from a classified document value,
// preserving the value's exact source bytes.
func cellFromScalar(s scalar) Cell {
	switch s.kind {
	case kindNull:
		return Cell{kind: KindNull, raw: nullBytes}
	case kindBool:
		raw := falseBytes
		flag := cellFlag(0)
		if s.bval {
			raw = trueBytes
			flag = cellTrue
		}
		return Cell{kind: KindBool, flag: flag, raw: raw}
	case kindNumber:
		if s.isInt {
			return Cell{kind: KindNumber, flag: cellInteger, word: uint64(s.ival), raw: s.num}
		}
		f, _ := s.float64OfNumber()
		return Cell{kind: KindNumber, word: math.Float64bits(f), raw: s.num}
	case kindString:
		return Cell{kind: KindString, text: s.sval, raw: s.raw}
	default:
		return Cell{kind: KindJSON, raw: s.raw}
	}
}

// ownFileCell moves the variable-width parts of cell into the Result's
// reusable packed arena. It is the durable.Snapshot ownership boundary: worker,
// page-cache, and execution-workspace storage may be reused immediately after
// materialization without leaving a borrowed result.
func (r *Result) ownFileCell(cell Cell) Cell {
	if len(cell.raw) != 0 {
		start := len(r.fileData)
		r.fileData = append(r.fileData, cell.raw...)
		cell.raw = r.fileData[start:len(r.fileData):len(r.fileData)]
	}
	if len(cell.text) != 0 {
		start := len(r.fileData)
		r.fileData = append(r.fileData, cell.text...)
		cell.text = byteview.String(
			r.fileData[start:len(r.fileData):len(r.fileData)],
		)
	}
	return cell
}

// nullCell builds a null result, the value of an aggregate over no rows and of
// an absent projection.
func nullCell() Cell {
	return Cell{kind: KindNull, raw: nullBytes}
}

// Kind returns the cell's JSON kind.
func (c Cell) Kind() CellKind { return c.kind }

// Type returns the versioned value type. It is the transport-oriented spelling
// of [Cell.Kind]; Kind remains convenient for JSON-only callers.
func (c Cell) Type() ValueType { return c.kind }

// TypeVersion returns zero for built-in JSON values and the negotiated
// semantic version for an extension value.
func (c Cell) TypeVersion() uint16 { return c.version }

// IsExtension reports whether the cell carries a negotiated opaque type.
func (c Cell) IsExtension() bool { return c.kind.IsExtension() }

// ExtensionCell creates a borrowed opaque extension value. The payload is not
// copied and must remain immutable for the cell's lifetime. Length-delimited
// result framing lets readers skip an optional unknown extension; required
// unknown types must be rejected during schema negotiation.
func ExtensionCell(valueType ValueType, version uint16, payload []byte) (Cell, error) {
	if valueType < ValueTypeExtensionStart {
		return Cell{}, fmt.Errorf("query: extension value type %#x is reserved", valueType)
	}
	return Cell{kind: valueType, version: version, raw: payload}, nil
}

// Payload returns the borrowed representation bytes. For a projected core
// value these are exact JSON bytes; for an extension they are the opaque
// length-delimited payload negotiated for Type and TypeVersion.
func (c Cell) Payload() []byte { return c.raw }

// IsNull reports whether the cell is null or absent.
func (c Cell) IsNull() bool { return c.kind == KindNull }

// Bool returns the cell's boolean value, and false for a non-boolean cell.
func (c Cell) Bool() (bool, bool) {
	if c.kind != KindBool {
		return false, false
	}
	return c.flag&cellTrue != 0, true
}

// Float64 returns the cell's numeric value as a float64, and false for a
// non-numeric cell.
func (c Cell) Float64() (float64, bool) {
	if c.kind != KindNumber {
		return 0, false
	}
	if c.flag&cellInteger != 0 {
		return float64(int64(c.word)), true
	}
	return math.Float64frombits(c.word), true
}

// Int64 returns the cell's numeric value as an int64 when it is an integer
// within range, and false otherwise.
func (c Cell) Int64() (int64, bool) {
	if c.kind == KindNumber && c.flag&cellInteger != 0 {
		return int64(c.word), true
	}
	return 0, false
}

// Text returns the cell's decoded string content, and false for a non-string
// cell.
func (c Cell) Text() (string, bool) {
	if c.kind != KindString {
		return "", false
	}
	return c.text, true
}

// TextBytes returns decoded string content without allocating. The slice is a
// read-only borrowed view with the same lifetime as the Cell and must not be
// modified. For a non-string it returns nil and false.
func (c Cell) TextBytes() ([]byte, bool) {
	if c.kind != KindString {
		return nil, false
	}
	return byteview.Bytes(c.text), true
}

// JSON returns the cell as JSON bytes: the exact source bytes for a projected
// value, or a newly formatted encoding for a computed numeric aggregate. The
// projected slice must not be modified and borrows the Segment. Call
// [Cell.AppendJSON] with retained storage when computed values must not
// allocate.
func (c Cell) JSON() []byte {
	if c.IsExtension() {
		return nil
	}
	if c.raw != nil {
		return c.raw
	}
	return c.AppendJSON(nil)
}

// AppendJSON appends the cell's compact JSON representation to dst. It is the
// caller-buffered transport form of [Cell.JSON] and allocates only if dst does
// not have enough capacity.
func (c Cell) AppendJSON(dst []byte) []byte {
	if c.IsExtension() {
		return dst
	}
	if c.raw != nil {
		return append(dst, c.raw...)
	}
	if c.kind != KindNumber {
		return dst
	}
	if c.flag&cellInteger != 0 {
		return strconv.AppendInt(dst, int64(c.word), 10)
	}
	return strconv.AppendFloat(dst, math.Float64frombits(c.word), 'g', -1, 64)
}

// String returns compact JSON for a core value and a diagnostic descriptor for
// an opaque extension. It is a debugging API, not a transport path.
func (c Cell) String() string {
	if c.IsExtension() {
		return fmt.Sprintf("<value-type %#x v%d: %d bytes>", c.kind, c.version, len(c.raw))
	}
	return string(c.AppendJSON(nil))
}
