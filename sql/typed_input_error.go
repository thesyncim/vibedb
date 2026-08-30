package sql

// InvalidTextRepresentationError is a source-positioned type-input failure.
// It unwraps to ParseError so every SQL-front-end diagnostic retains the
// established byte/line/column contract, while protocol adapters can map this
// semantic class to PostgreSQL SQLSTATE 22P02 without matching error text.
// The rejected value is deliberately omitted because SQL text can contain
// application data that must not be copied into logs or ErrorResponse fields.
type InvalidTextRepresentationError struct {
	ParseError
	Target string
}

func (e *InvalidTextRepresentationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newInvalidTextRepresentationError(
	src string, pos int, target, reason string,
) *InvalidTextRepresentationError {
	return &InvalidTextRepresentationError{
		ParseError: parseErrorAt(src, pos, reason),
		Target:     target,
	}
}
