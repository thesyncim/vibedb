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

// PrepareSQL parses one statement of the SQL dialect and returns the same
// prepared Query the equivalent programmatic builder produces. SQL is
// therefore an optional compile-time adapter, not the executor's
// representation: the returned Query has already discarded the source text and
// holds only the typed plan.
//
// It is the plain form, for a statement the plan can express by itself. A
// statement with a placeholder, a HAVING clause, or an OFFSET needs a binding
// step or a post-execution filter that a bare Query has nowhere to put, so
// those are refused here and answered by [PrepareStatement], which returns a
// [Statement] carrying both.
func PrepareSQL(src string) (*Query, error) {
	s, err := PrepareStatement(src)
	if err != nil {
		return nil, err
	}
	if s.params != 0 {
		return nil, fmt.Errorf(
			"query: this statement has %d placeholder(s), which a bare Query cannot bind; "+
				"use PrepareStatement", s.params)
	}
	if s.tree.Having != nil {
		return nil, fmt.Errorf(
			"query: HAVING filters after the reduction, which the plan has no node for; " +
				"use PrepareStatement, whose cursor applies it")
	}
	if s.tree.Offset != nil {
		return nil, fmt.Errorf(
			"query: OFFSET skips rows the plan cannot skip; use PrepareStatement, whose " +
				"cursor applies it")
	}
	// The Query borrows the Statement's compiler, and the Statement is
	// unreachable from here on, so nothing can ever re-lower it and invalidate
	// the plan. The plan's own interior pointers keep every arena chunk it
	// reads alive, which is what makes handing back the borrowed Query safe
	// rather than merely convenient.
	return &s.q, nil
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
// used by the typed result batch; Header is a compatibility/display spelling,
// not an execution key.
type OutputColumn struct {
	Header    string
	Ordinal   uint32
	Reduction Reduction
	// Type is TypeAny for a schemaless projection and TypeNumber for the
	// current aggregate family. Future aggregate or schema-aware output types
	// extend ValueType without changing column ordinals or instruction opcodes.
	Type ValueType
	// Flags control column framing. Unknown required types fail preparation;
	// an explicitly optional length-delimited column may be skipped by a
	// negotiated older reader.
	Flags OutputFlags
}

// OutputFlags describe schema-level compatibility behavior independently from
// a value type's physical properties.
type OutputFlags uint16

const (
	// OutputOptional permits a reader that does not recognize Type to skip the
	// complete length-delimited column.
	OutputOptional OutputFlags = 1 << iota
)

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
