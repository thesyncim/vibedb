package sql

import (
	"strings"
)

// AmbiguousColumnError reports a column reference whose source cannot be
// chosen without guessing. It unwraps to ParseError for ordinary position
// handling while giving protocol adapters a stable ambiguous-column class.
//
// INSERT ... ON CONFLICT DO UPDATE has two row namespaces with the same
// declared columns: the conflicting target row and EXCLUDED. PostgreSQL
// therefore requires those right-hand-side references to be qualified.
type AmbiguousColumnError struct {
	ParseError
	Name string
}

func (e *AmbiguousColumnError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func quoteSQLIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
