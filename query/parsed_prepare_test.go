package query

import (
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestPrepareParsedStatementRetainsOneAST(t *testing.T) {
	tree, err := sqlast.Parse(`SELECT n FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := PrepareParsedStatement("diagnostic source only", tree)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.tree != tree {
		t.Fatal("PrepareParsedStatement copied or reparsed the SELECT AST")
	}
	if statement.NumParams() != 1 ||
		len(statement.Columns()) != 1 || statement.Columns()[0] != "n" {
		t.Fatalf(
			"prepared statement = params %d columns %v",
			statement.NumParams(), statement.Columns(),
		)
	}
	if statement.SQL() != "diagnostic source only" {
		t.Fatalf("diagnostic SQL = %q", statement.SQL())
	}
}

func TestPrepareParsedDMLRetainsOneAST(t *testing.T) {
	tree, err := sqlast.ParseStatement(`DELETE FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := PrepareParsedDML("diagnostic source only", tree)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.Tree() != tree {
		t.Fatal("PrepareParsedDML copied or reparsed the DML AST")
	}
	if statement.Kind() != DMLDelete || statement.NumParams() != 1 {
		t.Fatalf(
			"prepared DML = kind %s params %d",
			statement.Kind(), statement.NumParams(),
		)
	}
}

func TestPrepareParsedRejectsInvalidInputs(t *testing.T) {
	if _, err := PrepareParsedStatement("", nil); err == nil ||
		!strings.Contains(err.Error(), "nil SELECT") {
		t.Fatalf("nil SELECT = %v", err)
	}
	if _, err := PrepareParsedDML("", nil); err == nil ||
		!strings.Contains(err.Error(), "nil statement") {
		t.Fatalf("nil DML = %v", err)
	}
	selectTree, err := sqlast.ParseStatement(`SELECT n FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareParsedDML("", selectTree); err == nil ||
		!strings.Contains(err.Error(), "given a SELECT") {
		t.Fatalf("SELECT as DML = %v", err)
	}
}
