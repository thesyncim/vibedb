package sql

import (
	"fmt"
	"strings"
)

// QualifiedAssignmentTargetError reports a dotted SET left-hand side. SQL
// target columns are always written without a table/range qualifier, even
// when the mutation target has an alias. It unwraps to ParseError for exact
// positioning while allowing protocol adapters to use undefined_column
// (42703), PostgreSQL's class for this mistake.
type QualifiedAssignmentTargetError struct {
	ParseError
	Table     string
	Qualifier string
}

func (e *QualifiedAssignmentTargetError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func (e *QualifiedAssignmentTargetError) SQLHint() string {
	return "SET target columns cannot be qualified."
}

func newQualifiedAssignmentTargetError(
	src string, pos int, table, qualifier string,
) *QualifiedAssignmentTargetError {
	return &QualifiedAssignmentTargetError{
		ParseError: parseErrorAt(src, pos, fmt.Sprintf(
			"column %s of relation %s does not exist",
			quoteSQLIdentifier(qualifier), quoteSQLIdentifier(table),
		)),
		Table:     strings.Clone(table),
		Qualifier: strings.Clone(qualifier),
	}
}
