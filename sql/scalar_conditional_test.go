package sql

import (
	"strings"
	"testing"
)

func TestScalarConditionalParseReuseAndBounds(t *testing.T) {
	source := `SELECT COALESCE(n, GREATEST(0, ?), 3), LEAST(n, 7), NULLIF(name, '') FROM docs`
	var p Parser
	var stmt SelectStmt
	for range 2 {
		if err := p.Parse(&stmt, source); err != nil {
			t.Fatal(err)
		}
		checkStatementInvariants(t, &stmt)
		if stmt.Params != 1 || stmt.Columns[0].Alias != "coalesce" || stmt.Columns[1].Alias != "least" || stmt.Columns[2].Alias != "nullif" {
			t.Fatalf("conditional AST = %s", dumpStmt(&stmt))
		}
	}
	for _, source := range []string{
		`SELECT COALESCE()`, `SELECT NULLIF(1)`, `SELECT NULLIF(1,2,3)`, `SELECT LEAST(1,)`,
		`SELECT GREATEST(` + strings.Repeat(`1,`, 1024) + `1)`,
		`SELECT ` + strings.Repeat(`COALESCE(1,`, 65) + `2` + strings.Repeat(`)`, 65),
	} {
		if err := p.Parse(&stmt, source); err == nil {
			t.Fatalf("accepted invalid conditional expression %q", source)
		}
	}
	if err := p.Parse(&stmt, `SELECT COALESCE(`+strings.Repeat(`NULL,`, 1023)+`1)`); err != nil {
		t.Fatal(err)
	}
	if err := p.Parse(&stmt, `SELECT coalesce, greatest.x, least, nullif FROM docs`); err != nil {
		t.Fatal(err)
	}
	for _, c := range stmt.Columns {
		if c.Scalar != nil || c.Path == nil {
			t.Fatal("conditional name shadowed a stored field")
		}
	}
	if err := p.Parse(&stmt, `SELECT id FROM docs WHERE coalesce IN ('x') AND greatest LIKE 'n%' AND least IS MISSING AND nullif BETWEEN 1 AND 2`); err != nil {
		t.Fatalf("conditional names shadowed path predicates: %v", err)
	}
	if err := p.Parse(&stmt, `SELECT CASE WHEN coalesce THEN 1 ELSE 0 END FROM docs WHERE id = nullif`); err != nil {
		t.Fatalf("conditional names shadowed Boolean or comparison paths: %v", err)
	}
	if stmt.Where.Kind != ExprCompare || stmt.Where.RightPath == nil {
		t.Fatal("ordinary path comparison lost its native predicate")
	}
	if err := p.Parse(&stmt, source); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := p.Parse(&stmt, source); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warmed conditional parse allocated %v times", allocs)
	}
}
