package sql

import "testing"

func TestCreateIndexUsingBeforeColumns(t *testing.T) {
	var p Parser
	var stmt Statement
	if err := p.ParseStatement(&stmt, `CREATE INDEX idx_body ON docs USING tin (body)`); err != nil {
		t.Fatalf("ParseStatement failed: %v", err)
	}
	if stmt.Kind != KindCreateIndex || stmt.CreateIndex.Method != "tin" || stmt.CreateIndex.Table != "docs" {
		t.Fatalf("unexpected AST: %+v", stmt.CreateIndex)
	}
	if len(stmt.CreateIndex.Paths) != 1 || stmt.CreateIndex.Paths[0].Spec() != "body" {
		t.Fatalf("unexpected paths: %+v", stmt.CreateIndex.Paths)
	}
	if err := p.ParseStatement(&stmt, `CREATE INDEX idx_dup ON docs USING tin (body) USING tin`); err == nil {
		t.Fatal("expected error for duplicate USING clause")
	}
	if err := p.ParseStatement(&stmt, `CREATE INDEX idx_multi ON docs USING tin (a, b)`); err == nil {
		t.Fatal("expected error for compound USING tin index")
	}
}
