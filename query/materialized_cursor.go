package query

import (
	"errors"

	"github.com/thesyncim/vibejson"
)

// NullCell constructs a SQL NULL for materialized native results. Cell's zero
// value is invalid, not NULL; adapters must preserve this distinction.
func NullCell() Cell { return nullCell() }

// ParseJSONCell validates one native wire value and borrows its exact JSON.
// Escaped strings own their decoded spelling. The caller retains raw until
// every result/cursor referencing the cell has been released.
func ParseJSONCell(raw []byte) (Cell, error) {
	if !vibejson.Valid(raw) {
		return Cell{}, errors.New("query: invalid JSON cell")
	}
	var decoded []byte
	return cellFromScalar(classifyRawInto(vibejson.RawValue{Src: raw}, &decoded)), nil
}

// NewResultCursor borrows a complete materialized result. No SQL clauses are
// reapplied: distributed merge has already evaluated order, offset and limit.
func NewResultCursor(result *Result) (Cursor, error) {
	if result == nil || result.RowCount < 0 {
		return Cursor{}, errors.New("query: invalid materialized result")
	}
	for _, column := range result.Columns {
		if len(column.Cells) != result.RowCount {
			return Cursor{}, errors.New("query: materialized row arity mismatch")
		}
	}
	return (&Statement{outputs: len(result.Columns)}).cursor(result), nil
}
