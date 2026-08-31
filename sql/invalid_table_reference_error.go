package sql

import (
	"fmt"
	"strings"
)

// InvalidTableReferenceError reports a mutation expression that qualifies a
// path with the physical target name after an explicit alias hid that name.
// It unwraps to ParseError for exact source positioning while giving protocol
// adapters PostgreSQL's undefined-table class (42P01).
type InvalidTableReferenceError struct {
	ParseError
	Table string
	Alias string
}

func (e *InvalidTableReferenceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

// SQLHint gives protocol adapters PostgreSQL-style corrective guidance.
func (e *InvalidTableReferenceError) SQLHint() string {
	if e == nil || e.Alias == "" {
		return ""
	}
	return fmt.Sprintf("Perhaps you meant to reference the table alias %s.",
		quoteSQLIdentifier(e.Alias))
}

func newInvalidTableReferenceError(
	src string, pos int, table, alias string,
) *InvalidTableReferenceError {
	return &InvalidTableReferenceError{
		ParseError: parseErrorAt(src, pos, fmt.Sprintf(
			"invalid reference to target table %q after it was aliased as %q",
			table, alias,
		)),
		Table: strings.Clone(table),
		Alias: strings.Clone(alias),
	}
}
