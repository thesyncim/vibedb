package sql

// UndefinedOperatorError is PostgreSQL's analysis-time rejection of an
// operator whose operand types have no catalog entry. PostgreSQL classifies
// missing operators with SQLSTATE 42883 (undefined_function), so this remains
// distinct from both an incompatible result type (42804) and an unsupported
// engine feature (0A000).
type UndefinedOperatorError struct {
	ParseError
	Left         string
	Operator     string
	Right        string
	Unpositioned bool
}

const undefinedOperatorHint = "No operator matches the given name and argument types. " +
	"You might need to add explicit type casts."

// SQLHint exposes PostgreSQL's canonical undefined-operator guidance to wire
// and distributed protocol adapters without coupling the parser to either.
func (e *UndefinedOperatorError) SQLHint() string {
	if e == nil {
		return ""
	}
	return undefinedOperatorHint
}

func (e *UndefinedOperatorError) Error() string {
	if e == nil {
		return ""
	}
	if e.Unpositioned {
		return e.Msg
	}
	return e.ParseError.Error()
}

func (e *UndefinedOperatorError) Unwrap() error {
	if e == nil || e.Unpositioned {
		return nil
	}
	return &e.ParseError
}

func newUndefinedOperatorError(
	src string, pos int, left, right string,
) *UndefinedOperatorError {
	return newUndefinedComparisonOperatorError(src, pos, left, "=", right)
}

func newUndefinedComparisonOperatorError(
	src string, pos int, left, operator, right string,
) *UndefinedOperatorError {
	return &UndefinedOperatorError{
		ParseError: parseErrorAt(src, pos,
			"operator does not exist: "+left+" "+operator+" "+right),
		Left:     left,
		Operator: operator,
		Right:    right,
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

// NewUndefinedComparisonOperatorError constructs a positioned
// undefined-operator diagnostic for any comparison spelling. It is used by
// runtime-typed paths, whose live domains are not known until row extraction.
func NewUndefinedComparisonOperatorError(
	src string, pos int, left, operator, right string,
) *UndefinedOperatorError {
	return newUndefinedComparisonOperatorError(src, pos, left, operator, right)
}

// NewUnpositionedUndefinedComparisonOperatorError constructs an
// undefined-operator diagnostic whose originating SQL text is not the text
// being executed. Stored view definitions are parsed independently and then
// rewritten into an outer statement, so their byte offsets must never be
// interpreted against that unrelated outer source. The concrete error still
// carries SQLSTATE classification metadata, but deliberately does not unwrap
// to ParseError and therefore cannot publish a bogus protocol position.
func NewUnpositionedUndefinedComparisonOperatorError(
	left, operator, right string,
) *UndefinedOperatorError {
	return &UndefinedOperatorError{
		ParseError: parseErrorAt("", 0,
			"operator does not exist: "+left+" "+operator+" "+right),
		Left:         left,
		Operator:     operator,
		Right:        right,
		Unpositioned: true,
	}
}
