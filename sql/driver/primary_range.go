package driver

import (
	"bytes"
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// primaryRangeProgram is the prepared, allocation-free description of the
// native ordered-primary bounds a top-level conjunction can supply. Terms are
// retained as parser-backed operands; execution only binds and compares their
// canonical storage bytes. The complete SQL predicate is still evaluated by
// query after the seek.
type primaryRangeProgram struct {
	path            string
	terms           []primaryRangeTerm
	coversPredicate bool
}

type primaryRangeTerm struct {
	operand   sqlast.Operand
	lower     bool
	inclusive bool
}

type primaryRangeBinding struct {
	lower          []byte
	upper          []byte
	lowerExclusive bool
}

// compilePrimaryRangeProgram recognizes range leaves only at the root or under
// top-level AND. A range below OR/NOT cannot safely prune the whole predicate.
// Multiple lower and upper terms are retained so bind can select their exact
// intersection after parameters acquire types.
func compilePrimaryRangeProgram(
	where *sqlast.Expr,
	primaryPath string,
) *primaryRangeProgram {
	if where == nil || primaryPath == "" || containsRuntimeSQLPathComparison(where) {
		return nil
	}
	program := &primaryRangeProgram{path: primaryPath}
	var collect func(*sqlast.Expr) bool
	collect = func(expr *sqlast.Expr) bool {
		if expr == nil {
			return false
		}
		if expr.Kind == sqlast.ExprAnd {
			covered := len(expr.Kids) != 0
			for i := range expr.Kids {
				covered = collect(expr.Kids[i]) && covered
			}
			return covered
		}
		if expr.Path == nil || expr.Agg != sqlast.AggNone ||
			string(expr.Path.AppendPointer(nil)) != primaryPath {
			return false
		}
		switch expr.Kind {
		case sqlast.ExprCompare:
			if expr.Subquery != nil || expr.RightPath != nil {
				return false
			}
			term := primaryRangeTerm{operand: expr.Value}
			switch expr.Op {
			case sqlast.OpGt:
				term.lower = true
			case sqlast.OpGe:
				term.lower, term.inclusive = true, true
			case sqlast.OpLt:
			case sqlast.OpLe:
				term.inclusive = true
			default:
				return false
			}
			program.terms = append(program.terms, term)
			return true
		case sqlast.ExprBetween:
			if expr.Negated || len(expr.List) != 2 {
				return false
			}
			program.terms = append(program.terms,
				primaryRangeTerm{operand: expr.List[0], lower: true, inclusive: true},
				primaryRangeTerm{operand: expr.List[1], inclusive: true},
			)
			return true
		}
		return false
	}
	program.coversPredicate = collect(where)
	if len(program.terms) == 0 {
		return nil
	}
	return program
}

// bindPrimaryRangeProgram intersects every prepared bound in native ordered-key
// space. eligible is false only when a legal operand is too large to encode
// under this collection's storage-key ceiling; that case falls back to the full
// scan because a range comparison is not necessarily empty. A NULL bound makes
// its conjunct UNKNOWN and therefore makes the WHERE result empty.
func (c *conn) bindPrimaryRangeProgram(
	program *primaryRangeProgram,
	args []any,
	maxKeyBytes int,
) (binding primaryRangeBinding, eligible, empty bool, err error) {
	if program == nil || len(program.terms) == 0 {
		return primaryRangeBinding{}, false, false, nil
	}
	c.pointKeyRaw = c.pointKeyRaw[:0]
	lowerStart, lowerEnd := -1, -1
	upperStart, upperEnd := -1, -1
	lowerExclusive, upperExclusive := false, false
	for i := range program.terms {
		term := program.terms[i]
		start := len(c.pointKeyRaw)
		var matchable bool
		var null bool
		c.pointKeyRaw, matchable, null, err = appendPrimaryRangeOperand(
			c.pointKeyRaw, term.operand, args, maxKeyBytes,
		)
		if err != nil {
			return primaryRangeBinding{}, false, false,
				fmt.Errorf("vibedb: primary key range: %w", err)
		}
		if null {
			return primaryRangeBinding{}, true, true, nil
		}
		if !matchable {
			return primaryRangeBinding{}, false, false, nil
		}
		end := len(c.pointKeyRaw)
		candidate := c.pointKeyRaw[start:end]
		if term.lower {
			cmp := 1
			if lowerStart >= 0 {
				cmp = bytes.Compare(candidate, c.pointKeyRaw[lowerStart:lowerEnd])
			}
			if lowerStart < 0 || cmp > 0 {
				lowerStart, lowerEnd = start, end
				lowerExclusive = !term.inclusive
			} else if cmp == 0 && !term.inclusive {
				lowerExclusive = true
			}
			continue
		}
		cmp := -1
		if upperStart >= 0 {
			cmp = bytes.Compare(candidate, c.pointKeyRaw[upperStart:upperEnd])
		}
		if upperStart < 0 || cmp < 0 {
			upperStart, upperEnd = start, end
			upperExclusive = !term.inclusive
		} else if cmp == 0 && !term.inclusive {
			upperExclusive = true
		}
	}
	if lowerStart >= 0 && upperStart >= 0 {
		cmp := bytes.Compare(
			c.pointKeyRaw[lowerStart:lowerEnd], c.pointKeyRaw[upperStart:upperEnd],
		)
		if cmp > 0 || cmp == 0 && (lowerExclusive || upperExclusive) {
			return primaryRangeBinding{}, true, true, nil
		}
	}
	// The durable upper endpoint is exclusive. Appending a zero byte produces
	// the immediate lexical successor of the complete encoded key, including an
	// authored <= or BETWEEN upper value without decoding or type branching.
	if upperStart >= 0 && !upperExclusive {
		start := len(c.pointKeyRaw)
		c.pointKeyRaw = append(
			c.pointKeyRaw, c.pointKeyRaw[upperStart:upperEnd]...,
		)
		c.pointKeyRaw = append(c.pointKeyRaw, 0)
		upperStart, upperEnd = start, len(c.pointKeyRaw)
	}
	if lowerStart >= 0 {
		binding.lower = c.pointKeyRaw[lowerStart:lowerEnd:lowerEnd]
	}
	if upperStart >= 0 {
		binding.upper = c.pointKeyRaw[upperStart:upperEnd:upperEnd]
	}
	binding.lowerExclusive = lowerExclusive
	return binding, true, false, nil
}

func appendPrimaryRangeOperand(
	dst []byte,
	operand sqlast.Operand,
	args []any,
	maxKeyBytes int,
) (out []byte, matchable, null bool, err error) {
	if operand.Kind == sqlast.OperandParam && operand.Ordinal < len(args) {
		if value, ok := args[operand.Ordinal].([]byte); ok {
			out, matchable, err = appendBoundedPrimaryString(dst, value, maxKeyBytes)
			return out, matchable, false, err
		}
	}
	value, err := operandValue(operand, args)
	if err != nil {
		return dst, false, false, err
	}
	if value == nil {
		return dst, false, true, nil
	}
	out, matchable, err = appendBoundedPrimaryScalarKey(dst, value, maxKeyBytes)
	return out, matchable, false, err
}
