package pgwire

import (
	"testing"
)

func TestGUCAndSetLocalTransactionSemantics(t *testing.T) {
	c := connect(t)

	// New standard PG GUCs work.
	for _, stmt := range []string{
		`SET statement_timeout = '5s'`,
		`SET lock_timeout = 100`,
		`SET idle_in_transaction_session_timeout = '30s'`,
		`SET default_transaction_isolation = 'repeatable read'`,
		`SET default_transaction_read_only = off`,
		`SET work_mem = '128MB'`,
		`SET client_min_messages = warning`,
	} {
		if got := commandTagOf(t, c.query(stmt)); got != "SET" {
			t.Fatalf("%q: tag = %q, want SET", stmt, got)
		}
	}

	// SET LOCAL inside a transaction reverts on COMMIT.
	if got := commandTagOf(t, c.query(`SET application_name = 'outer'`)); got != "SET" {
		t.Fatalf("SET outer: %q", got)
	}
	if got := commandTagOf(t, c.query(`BEGIN`)); got != "BEGIN" {
		t.Fatalf("BEGIN: %q", got)
	}
	if got := commandTagOf(t, c.query(`SET LOCAL application_name = 'inner'`)); got != "SET" {
		t.Fatalf("SET LOCAL: %q", got)
	}
	if rows := rowsOf(t, c.query(`SHOW application_name`)); len(rows) != 1 || string(rows[0][0]) != "inner" {
		t.Fatalf("SHOW inside tx = %v, want [[inner]]", rows)
	}
	if got := commandTagOf(t, c.query(`COMMIT`)); got != "COMMIT" {
		t.Fatalf("COMMIT: %q", got)
	}
	if rows := rowsOf(t, c.query(`SHOW application_name`)); len(rows) != 1 || string(rows[0][0]) != "outer" {
		t.Fatalf("SHOW after commit = %v, want [[outer]]", rows)
	}

	// Non-local SET inside a transaction reverts on ROLLBACK.
	if got := commandTagOf(t, c.query(`BEGIN`)); got != "BEGIN" {
		t.Fatalf("BEGIN 2: %q", got)
	}
	if got := commandTagOf(t, c.query(`SET application_name = 'aborted'`)); got != "SET" {
		t.Fatalf("SET in tx: %q", got)
	}
	if got := commandTagOf(t, c.query(`ROLLBACK`)); got != "ROLLBACK" {
		t.Fatalf("ROLLBACK: %q", got)
	}
	if rows := rowsOf(t, c.query(`SHOW application_name`)); len(rows) != 1 || string(rows[0][0]) != "outer" {
		t.Fatalf("SHOW after rollback = %v, want [[outer]]", rows)
	}
}
