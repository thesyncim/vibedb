//go:build 386

package query

import (
	"errors"
	"math"
	"testing"
)

func TestRecursiveCTEStatementParamBaseOverflow386(t *testing.T) {
	statement, err := PrepareStatement(`SELECT node FROM seeds WHERE node = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	_, err = PrepareRecursiveCTEStatementTerm(
		statement,
		RecursiveCTEStatementTermOptions{ParamBase: math.MaxInt},
	)
	var parameter *RecursiveCTEStatementParameterError
	if !errors.As(err, &parameter) ||
		!errors.Is(err, ErrRecursiveCTEStatement) ||
		parameter.ParamBase != math.MaxInt || parameter.Params != 1 {
		t.Fatalf("386 ParamBase overflow = %#v (%v)", parameter, err)
	}
}
