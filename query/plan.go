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
