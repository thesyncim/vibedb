package sql

import (
	"fmt"
	"strings"
)

// AmbiguousOutputError reports an ORDER BY name that matches more than one
// explicit SELECT-list alias. It unwraps to ParseError for ordinary position
// handling while giving protocol adapters a stable ambiguous-column class.
type AmbiguousOutputError struct {
	ParseError
	Name string
}

func (e *AmbiguousOutputError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newAmbiguousOutputError(src string, pos int, name string) *AmbiguousOutputError {
	return &AmbiguousOutputError{
		ParseError: parseErrorAt(src, pos, fmt.Sprintf(
			"ORDER BY output name %q is ambiguous; give SELECT outputs unique aliases", name,
		)),
		Name: strings.Clone(name),
	}
}

// InvalidOrderPositionError reports an ORDER BY ordinal that is not a
// positive, existing SELECT-list position. It unwraps to ParseError for source
// positioning while giving protocol adapters a stable invalid-column-reference
// class instead of forcing them to inspect diagnostic prose.
type InvalidOrderPositionError struct {
	ParseError
	Position string
	Outputs  int
}

func (e *InvalidOrderPositionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newInvalidOrderPositionError(
	src string,
	pos int,
	position string,
	outputs int,
) *InvalidOrderPositionError {
	message := fmt.Sprintf(
		"ORDER BY position %s is not in the SELECT list, which has %d outputs",
		position, outputs,
	)
	return &InvalidOrderPositionError{
		ParseError: parseErrorAt(src, pos, message),
		Position:   strings.Clone(position),
		Outputs:    outputs,
	}
}
