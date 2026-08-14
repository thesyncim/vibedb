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
