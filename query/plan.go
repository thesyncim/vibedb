package query

import (
	"fmt"
)

// Prepare compiles q now instead of at its first execution, so a malformed
// query — an unparsable path, a projection absent from GROUP BY, a mixed
// projection and aggregate — is reported where it was written rather than from
// inside a hot loop. It is otherwise optional: execution compiles once through
// the same [sync.Once] and returns the identical error. Repeated calls are
// idempotent and return the cached result, so Prepare is also the cheap way to
// force compilation before timing anything.
func (q *Query) Prepare() error {
	if q == nil {
		return fmt.Errorf("query: cannot prepare a nil Query")
	}
	_, err := q.compiled()
	return err
}

// A Reduction identifies the typed reduction performed by an output column.
// ReductionNone denotes a projected JSON value.
type Reduction uint8

const (
	ReductionNone Reduction = iota
	ReductionCount
	ReductionSum
	ReductionAvg
	ReductionMin
	ReductionMax
	// ReductionWindowInteger identifies analytic outputs whose SQL type is an
	// exact signed 64-bit integer: ranking functions, NTILE, and window COUNT.
	ReductionWindowInteger
	// ReductionWindowNumber identifies analytic outputs that are statically
	// numeric but are not restricted to int64, including distribution and
	// window aggregate functions.
	ReductionWindowNumber
)

// OutputRepresentation identifies the caller-visible SQL representation of a
// statically typed expression. Its zero value deliberately preserves the
// engine's established JSON boundary: knowing that a JSON cell currently
// contains a string or number is not enough to reinterpret that value as SQL
// text or numeric. Only lowering that actually computes a SQL scalar opts in.
type OutputRepresentation uint8

const (
	OutputJSON OutputRepresentation = iota
	OutputSQLNumber
	OutputSQLText
)

// OutputColumn is cold result-schema metadata. Ordinal is the stable column ID
// used by the typed result batch; Header is its display spelling.
type OutputColumn struct {
	Header    string
	Ordinal   uint32
	Reduction Reduction
	// Type is TypeAny for a schemaless projection and TypeNumber for the
	// current aggregate family. Future aggregate or schema-aware output types
	// extend ValueType without changing column ordinals or instruction opcodes.
	Type ValueType
	// Representation is independent of Type. VALUES, ordinary projections,
	// set operands, and EXPLAIN may have a statically known cell kind while
	// still retaining exact JSON encoding. Computed arithmetic and
	// concatenation explicitly select their SQL boundary representation.
	Representation OutputRepresentation
}

// AppendSchema appends q's output schema to dst, compiling q if execution has
// not already done so, and allocating nothing when dst has enough capacity.
// Headers borrow immutable compiled-plan storage. A query that does not
// compile has no schema and leaves dst untouched; the error is reported by
// [Query.Prepare] or by execution, so a transport encoder negotiating a schema
// never has to distinguish "no columns" from "not a query".
func (q *Query) AppendSchema(dst []OutputColumn) []OutputColumn {
	if q == nil {
		return dst
	}
	p, err := q.compiled()
	if err != nil {
		return dst
	}
	for i, col := range p.columns {
		valueType := TypeAny
		if col.agg != aggNone {
			valueType = TypeNumber
		}
		dst = append(dst, OutputColumn{
			Header:    p.headers[i],
			Ordinal:   uint32(i),
			Reduction: Reduction(col.agg),
			Type:      valueType,
		})
	}
	return dst
}
