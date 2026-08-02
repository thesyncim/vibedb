//go:build 386

package query

import (
	"math"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestRecursiveSQLBridgeRejectsParamBaseOverflow386(t *testing.T) {
	var parser sqlast.Parser
	var tree sqlast.SelectStmt
	if err := parser.Parse(&tree, recursiveSQLGraphStatement); err != nil {
		t.Fatal(err)
	}
	definition := &tree.With.CTEs[0]
	definition.Query.ParamBase = math.MaxInt
	if _, err := PrepareParsedRecursiveSQLStatement(
		recursiveSQLGraphStatement, &tree, RecursiveSQLStatementOptions{},
	); err == nil {
		t.Fatal("recursive SQL ParamBase overflow prepared")
	}
}

func TestRecursiveSQLCaseRejectsScalarOrdinalOverflow386(t *testing.T) {
	var parser sqlast.Parser
	var tree sqlast.SelectStmt
	if err := parser.Parse(&tree, recursiveSQLCaseFullFrame); err != nil {
		t.Fatal(err)
	}
	term := tree.With.CTEs[1].Recursive.Term
	parameter := &term.Columns[0].Scalar.Whens[0].Predicate.ScalarRight.Left.Value
	parameter.Ordinal = math.MaxInt
	if _, err := cloneRecursiveSQLFullFrameTree(term, 1, math.MaxInt); err == nil {
		t.Fatal("recursive CASE scalar ordinal overflow cloned")
	}
	if parameter.Ordinal != math.MaxInt {
		t.Fatalf("failed recursive CASE clone mutated parser ordinal to %d", parameter.Ordinal)
	}
}
