package sql

import "testing"

func TestPostgreSQLColumnTypesModifiersAndCasts(t *testing.T) {
	ddl := `CREATE TABLE events (
		id UUID PRIMARY KEY,
		seq BIGSERIAL NOT NULL,
		title VARCHAR(255) NOT NULL,
		amount NUMERIC(12, 2),
		ratio DOUBLE PRECISION,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL,
		day DATE,
		payload JSONB,
		tags TEXT[],
		raw BYTEA
	)`
	var p Parser
	var stmt Statement
	if err := p.ParseStatement(&stmt, ddl); err != nil {
		t.Fatalf("ParseStatement DDL: %v", err)
	}
	if stmt.Kind != KindCreateTable || len(stmt.CreateTable.Columns) != 10 {
		t.Fatalf("unexpected CreateTable: %+v", stmt.CreateTable)
	}
	if stmt.CreateTable.Columns[8].Type&TypeArray == 0 {
		t.Fatalf("tags TEXT[] type = %v, want TypeArray", stmt.CreateTable.Columns[8].Type)
	}

	q := `SELECT '42'::INTEGER, '3.14'::FLOAT8, 123::VARCHAR, '{"a":1}'::JSONB, '2026-01-01'::DATE`
	if err := p.ParseStatement(&stmt, q); err != nil {
		t.Fatalf("ParseStatement CASTs: %v", err)
	}
	if stmt.Kind != KindSelect || len(stmt.Select.Columns) != 5 {
		t.Fatalf("unexpected SELECT: %+v", stmt.Select)
	}
}
