package sql

import "testing"

func TestCreateTablePGConstraints(t *testing.T) {
	ddl := `CREATE TABLE orders (
		id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
		user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		email VARCHAR(255) UNIQUE NOT NULL,
		amount NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (amount >= 0),
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		CONSTRAINT orders_user_email_uq UNIQUE (user_id, email),
		CONSTRAINT orders_positive CHECK (amount >= 0),
		FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE NO ACTION ON DELETE RESTRICT
	)`
	var p Parser
	var stmt Statement
	if err := p.ParseStatement(&stmt, ddl); err != nil {
		t.Fatalf("ParseStatement: %v", err)
	}
	if stmt.Kind != KindCreateTable || len(stmt.CreateTable.Columns) != 5 {
		t.Fatalf("unexpected CreateTable: %+v", stmt.CreateTable)
	}
	if !stmt.CreateTable.Columns[0].HasDefault || stmt.CreateTable.Columns[1].References != "users" ||
		!stmt.CreateTable.Columns[2].Unique || stmt.CreateTable.Columns[3].CheckExpr == nil {
		t.Fatalf("missing parsed column constraints: %+v", stmt.CreateTable.Columns)
	}
	if len(stmt.CreateTable.UniqueConstraints) != 1 || len(stmt.CreateTable.CheckConstraints) != 1 {
		t.Fatalf("table constraints: unique=%d check=%d", len(stmt.CreateTable.UniqueConstraints), len(stmt.CreateTable.CheckConstraints))
	}
}
