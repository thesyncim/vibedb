package sql

import (
	"fmt"
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

func newAmbiguousColumnError(
	src string,
	pos int,
	name string,
	target string,
) *AmbiguousColumnError {
	message := fmt.Sprintf(
		"ON CONFLICT column reference %q is ambiguous; qualify the current value with %s. or the candidate value with EXCLUDED.",
		name, quoteSQLIdentifier(target),
	)
	if target == "excluded" {
		message = fmt.Sprintf(
			"ON CONFLICT column reference %q is ambiguous, and the target table name excluded collides with the EXCLUDED pseudo-relation; INSERT target aliases are required to name the current row but are not supported yet",
			name,
		)
	}
	return &AmbiguousColumnError{
		ParseError: parseErrorAt(src, pos, message),
		Name:       strings.Clone(name),
	}
}

func quoteSQLIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
