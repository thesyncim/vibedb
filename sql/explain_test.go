package sql

import "testing"

func TestExplainParsesAsRowReturningSelect(t *testing.T) {
	statement, err := ParseStatement(
		`EXPLAIN SELECT id FROM docs WHERE kind = 'active'`)
	if err != nil {
		t.Fatal(err)
	}
	if !statement.Explain || !statement.ReturnsRows() || statement.Select == nil {
		t.Fatalf("explain statement = %+v", statement)
	}
	if got := statement.Table(); got != "docs" {
		t.Fatalf("explain table = %q, want docs", got)
	}
}

func TestExplainRejectsWrites(t *testing.T) {
	if _, err := ParseStatement(`EXPLAIN INSERT INTO docs VALUES (?)`); err == nil {
		t.Fatal("EXPLAIN accepted a write")
	}
}
