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
