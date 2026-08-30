package sql

// CannotCoerceError is PostgreSQL's analysis-time rejection of a cast for
// which no explicit conversion path exists. It remains a ParseError so every
// caller keeps exact source positioning, while protocol adapters can map the
// semantic class to SQLSTATE 42846 instead of the runtime 42804 class.
type CannotCoerceError struct {
	ParseError
	Source string
	Target string
}

func (e *CannotCoerceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newCannotCoerceError(
	src string, pos int, source, target string,
) *CannotCoerceError {
	return &CannotCoerceError{
		ParseError: parseErrorAt(src, pos,
			"cannot cast type "+source+" to "+target),
		Source: source,
		Target: target,
	}
}
