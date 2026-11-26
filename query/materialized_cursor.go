package query

import (
	"errors"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// NullCell constructs a SQL NULL for materialized native results. Cell's zero
// value is invalid, not NULL; adapters must preserve this distinction.
func NullCell() Cell { return nullCell() }

// ParseJSONCell validates one native wire value and borrows its exact JSON.
// Escaped strings own their decoded spelling. The caller retains raw until
// every result/cursor referencing the cell has been released.
func ParseJSONCell(raw []byte) (Cell, error) {
	if n := len(raw); n != 0 {
		switch b0 := raw[0]; {
		case b0 == '"':
			if n >= 2 && raw[n-1] == '"' {
				inner := raw[1 : n-1]
				simple := true
				for _, b := range inner {
					if b < 0x20 || b >= 0x80 || b == '"' || b == '\\' {
						simple = false
						break
					}
				}
				if simple {
					return Cell{kind: TypeString, text: byteview.String(inner), raw: raw}, nil
				}
			}
		case b0 == '-' || (b0 >= '0' && b0 <= '9'):
			if v, ok := parseSimpleCanonicalJSONInt64(raw); ok {
				return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(v), raw: raw}, nil
			}
		case b0 == 't' && n == 4 && string(raw) == "true":
			return Cell{kind: TypeBool, flag: cellTrue, raw: trueBytes}, nil
		case b0 == 'f' && n == 5 && string(raw) == "false":
			return Cell{kind: TypeBool, raw: falseBytes}, nil
		case b0 == 'n' && n == 4 && string(raw) == "null":
			return Cell{kind: TypeNull, raw: nullBytes}, nil
		}
	}
	if !vibejson.Valid(raw) {
		return Cell{}, errors.New("query: invalid JSON cell")
	}
	var decoded []byte
	return cellFromScalar(classifyRawInto(vibejson.RawValue{Src: raw}, &decoded)), nil
}

func parseSimpleCanonicalJSONInt64(src []byte) (int64, bool) {
	if len(src) == 0 || len(src) > 20 {
		return 0, false
	}
	at := 0
	negative := src[0] == '-'
	if negative {
		at++
	}
	if at == len(src) || src[at] < '0' || src[at] > '9' {
		return 0, false
	}
	if src[at] == '0' && (at+1 != len(src) || negative) {
		return 0, false
	}
	limit := uint64(1<<63 - 1)
	if negative {
		limit++
	}
	var value uint64
	for ; at < len(src); at++ {
		c := src[at]
		if c < '0' || c > '9' {
			return 0, false
		}
		digit := uint64(c - '0')
		if value > (limit-digit)/10 {
			return 0, false
		}
		value = value*10 + digit
	}
	if negative {
		if value == uint64(1)<<63 {
			return -1 << 63, true
		}
		return -int64(value), true
	}
	return int64(value), true
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
	// Distributed merge has already applied every relational clause. A nil
	// statement marks this simpler cursor shape and avoids allocating a
	// one-field Statement for every wire response.
	return Cursor{res: result, cur: -1, left: -1}, nil
}
