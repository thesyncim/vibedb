package query

import "errors"

// ErrCardinalityViolation classifies a query whose result cardinality is not
// valid for the expression consuming it.
var ErrCardinalityViolation = errors.New("query: cardinality violation")

// CardinalityViolationError reports that a scalar subquery produced more than
// one row. Scalar subqueries accept zero rows as SQL NULL and one row as their
// value; a second row is therefore sufficient to establish this error.
type CardinalityViolationError struct{}

func (*CardinalityViolationError) Error() string {
	return "query: more than one row returned by a subquery used as an expression"
}

// Unwrap lets callers classify the error with errors.Is.
func (*CardinalityViolationError) Unwrap() error { return ErrCardinalityViolation }
