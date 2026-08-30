package sql

// UndefinedOperatorError is PostgreSQL's analysis-time rejection of an
// operator whose operand types have no catalog entry. PostgreSQL classifies
// missing operators with SQLSTATE 42883 (undefined_function), so this remains
// distinct from both an incompatible result type (42804) and an unsupported
// engine feature (0A000).
type UndefinedOperatorError struct {
	ParseError
	Left  string
	Right string
}

func (e *UndefinedOperatorError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newUndefinedOperatorError(
	src string, pos int, left, right string,
) *UndefinedOperatorError {
	return &UndefinedOperatorError{
		ParseError: parseErrorAt(src, pos,
			"operator does not exist: "+left+" = "+right),
		Left:  left,
		Right: right,
	}
}

// NewUndefinedOperatorError constructs a positioned undefined-operator
// diagnostic for a later analysis layer that has resolved operand domains.
// Parser-owned callers use the unexported spelling above; query lowering needs
// the exported boundary when client-declared parameter types participate in
// CASE operator resolution.
func NewUndefinedOperatorError(
	src string, pos int, left, right string,
) *UndefinedOperatorError {
	return newUndefinedOperatorError(src, pos, left, right)
}
