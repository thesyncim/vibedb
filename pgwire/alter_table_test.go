package pgwire

import "testing"

func TestAlterTableAddColumnProtocol(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(
		`CREATE TABLE alter_wire_docs (id STRING PRIMARY KEY)`,
	))

	simple := c.query(
		`ALTER TABLE alter_wire_docs ADD COLUMN note STRING`,
	)
	requireWireOK(t, simple)
	if got := commandTagOf(t, simple); got != "ALTER TABLE" {
		t.Fatalf("simple ALTER TABLE tag = %q, want ALTER TABLE", got)
	}

	extended := extendedSQL(c,
		`ALTER TABLE alter_wire_docs ADD COLUMN score INTEGER`, nil,
	)
	requireWireOK(t, extended)
	if got := commandTagOf(t, extended); got != "ALTER TABLE" {
		t.Fatalf("extended ALTER TABLE tag = %q, want ALTER TABLE", got)
	}

	duplicate := c.query(
		`ALTER TABLE alter_wire_docs ADD COLUMN score INTEGER`,
	)
	expectError(t, duplicate, sqlstateDuplicateColumn)
	if has(duplicate, msgCommandComplete) {
		t.Fatalf("duplicate ALTER TABLE emitted CommandComplete: %s", tags(duplicate))
	}

	ifNotExists := c.query(
		`ALTER TABLE alter_wire_docs ADD COLUMN IF NOT EXISTS score STRING NOT NULL`,
	)
	requireWireOK(t, ifNotExists)
	if got := commandTagOf(t, ifNotExists); got != "ALTER TABLE" {
		t.Fatalf("ALTER TABLE IF NOT EXISTS tag = %q, want ALTER TABLE", got)
	}
	insertAfterNoOp := c.query(
		`INSERT INTO alter_wire_docs (id, score) VALUES ('kept', 7)`,
	)
	requireWireOK(t, insertAfterNoOp)
	if got := commandTagOf(t, insertAfterNoOp); got != "INSERT 0 1" {
		t.Fatalf("insert after ALTER TABLE no-op tag = %q, want INSERT 0 1", got)
	}

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	inTransaction := c.query(
		`ALTER TABLE alter_wire_docs ADD COLUMN tx_only STRING`,
	)
	expectError(t, inTransaction, sqlstateActiveSQLTransaction)
	if has(inTransaction, msgCommandComplete) {
		t.Fatalf("in-transaction ALTER TABLE emitted CommandComplete: %s",
			tags(inTransaction))
	}
	assertReadyStatus(t, inTransaction, statusFailedT)
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK`), statusIdle)

	// The refused transaction must not have changed the table schema.
	afterRollback := c.query(
		`ALTER TABLE alter_wire_docs ADD COLUMN tx_only STRING`,
	)
	requireWireOK(t, afterRollback)
	if got := commandTagOf(t, afterRollback); got != "ALTER TABLE" {
		t.Fatalf("post-rollback ALTER TABLE tag = %q, want ALTER TABLE", got)
	}
}
