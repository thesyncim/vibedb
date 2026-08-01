package sql

import (
	"fmt"
	"strings"
)

// DuplicateCTEError reports two definitions with the same name in one WITH
// scope. It unwraps to ParseError for ordinary position handling while keeping
// a semantic type that protocol adapters can map to duplicate_alias (42712)
// without inspecting prose.
type DuplicateCTEError struct {
	ParseError
	Name     string
	FirstPos int
}

func (e *DuplicateCTEError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newDuplicateCTEError(
	src string, pos int, name string, firstPos int,
) *DuplicateCTEError {
	return &DuplicateCTEError{
		ParseError: parseErrorAt(src, pos, fmt.Sprintf(
			"common table expression %q is declared more than once; its first declaration is at offset %d",
			name, firstPos,
		)),
		Name: strings.Clone(name), FirstPos: firstPos,
	}
}

// CTEColumnAliasArityError reports an alias list longer than the defining
// query's output list. A shorter list is valid SQL and leaves the remaining
// output names unchanged. The concrete type gives adapters a stable semantic
// hook (42P10) and carries the counts needed for a useful diagnostic.
type CTEColumnAliasArityError struct {
	ParseError
	Name    string
	Aliases int
	Outputs int
}

func (e *CTEColumnAliasArityError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newCTEColumnAliasArityError(
	src string, pos int, name string, aliases int, outputs int,
) *CTEColumnAliasArityError {
	return &CTEColumnAliasArityError{
		ParseError: parseErrorAt(src, pos, fmt.Sprintf(
			"common table expression %q has %d column aliases but its query has %d outputs",
			name, aliases, outputs,
		)),
		Name: strings.Clone(name), Aliases: aliases, Outputs: outputs,
	}
}
