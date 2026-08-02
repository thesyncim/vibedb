package query

import "fmt"

// InsertSelectDocumentParameterError reports a VALUES query-source parameter
// that cannot supply one exact JSON document. Position is the zero-based byte
// offset of the authored placeholder; protocol adapters convert it to SQL's
// one-based character position.
type InsertSelectDocumentParameterError struct {
	Pos       int
	Parameter int
	Cause     error
}

func (e *InsertSelectDocumentParameterError) Error() string {
	if e == nil {
		return ErrParameterType.Error()
	}
	if e.Cause == nil {
		return fmt.Sprintf(
			"INSERT SELECT document parameter %d is invalid", e.Parameter,
		)
	}
	return fmt.Sprintf(
		"INSERT SELECT document parameter %d %v", e.Parameter, e.Cause,
	)
}

func (e *InsertSelectDocumentParameterError) Unwrap() error {
	return ErrParameterType
}

func (e *InsertSelectDocumentParameterError) Position() int {
	if e == nil {
		return 0
	}
	return e.Pos
}

// InsertDocumentParameterPosition reports whether one zero-based statement
// placeholder is on the complete-document output lineage of INSERT ... SELECT
// and, when it is, the exact zero-based byte offset of its authored token.
//
// The lookup is constant-time and allocation-free. Ordinary VALUES inserts,
// scalar SELECT parameters, predicates, set tails, and placeholders that do
// not contribute to the stored output return ok=false.
func (d *DMLStatement) InsertDocumentParameterPosition(
	ordinal int,
) (position int, ok bool) {
	if d == nil || ordinal < 0 || ordinal >= len(d.insertDocumentPositions) {
		return 0, false
	}
	position = d.insertDocumentPositions[ordinal]
	if position == 0 {
		return 0, false
	}
	return position - 1, true
}
