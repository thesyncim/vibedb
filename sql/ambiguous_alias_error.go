package sql

import "strings"

// AmbiguousAliasError reports a qualifier that names both the INSERT target
// range and ON CONFLICT's EXCLUDED pseudo-relation. It unwraps to ParseError
// for exact positioning while giving protocol adapters PostgreSQL's
// ambiguous-alias class (42P09).
type AmbiguousAliasError struct {
	ParseError
	Name string
}

func (e *AmbiguousAliasError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newAmbiguousAliasError(src string, pos int, name string) *AmbiguousAliasError {
	return &AmbiguousAliasError{
		ParseError: parseErrorAt(src, pos,
			"table reference "+quoteSQLIdentifier(name)+" is ambiguous"),
		Name: strings.Clone(name),
	}
}
