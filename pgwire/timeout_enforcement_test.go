package pgwire

import (
	"strings"
	"testing"
	"time"
)

func TestStatementTimeoutEnforcedAndRecoverable(t *testing.T) {
	c := connect(t)

	if got := commandTagOf(t, c.query(`CREATE TABLE seed (n INTEGER PRIMARY KEY)`)); got != "CREATE TABLE" {
		t.Fatalf("CREATE TABLE seed: %q", got)
	}
	if got := commandTagOf(t, c.query(`INSERT INTO seed (n) VALUES (1)`)); got != "INSERT 0 1" {
		t.Fatalf("INSERT seed: %q", got)
	}
	if got := commandTagOf(t, c.query(`SET statement_timeout = '15ms'`)); got != "SET" {
		t.Fatalf("SET statement_timeout: %q", got)
	}

	// Recursive CTE generating 10M rows exceeds 15ms and must be canceled with
	// SQLSTATE 57014 and a statement-timeout message.
	msgs := c.query(`WITH RECURSIVE t(n) AS (SELECT n + 0 FROM seed UNION ALL SELECT n + 1 FROM t WHERE n < 10000000) SELECT count(*) FROM t`)
	expectError(t, msgs, sqlstateQueryCanceled)

	foundTimeoutMsg := false
	for _, m := range msgs {
		if m.tag == msgErrorResponse {
			if strings.Contains(string(m.body), "statement timeout") {
				foundTimeoutMsg = true
			}
		}
	}
	if !foundTimeoutMsg {
		t.Fatalf("expected 'statement timeout' in ErrorResponse, got %v", msgs)
	}

	// After resetting statement_timeout, subsequent queries on the same session
	// must succeed cleanly (no leaked cancellation state).
	if got := commandTagOf(t, c.query(`SET statement_timeout = 0`)); got != "SET" {
		t.Fatalf("RESET statement_timeout: %q", got)
	}
	if rows := rowsOf(t, c.query(`SELECT 42`)); len(rows) != 1 || string(rows[0][0]) != "42" {
		t.Fatalf("post-timeout SELECT 42 = %v, want [[42]]", rows)
	}
}

func TestIdleInTransactionSessionTimeoutEnforced(t *testing.T) {
	c := connect(t)

	if got := commandTagOf(t, c.query(`SET idle_in_transaction_session_timeout = '40ms'`)); got != "SET" {
		t.Fatalf("SET idle_in_transaction_session_timeout: %q", got)
	}
	if got := commandTagOf(t, c.query(`BEGIN`)); got != "BEGIN" {
		t.Fatalf("BEGIN: %q", got)
	}

	// Remain idle in the open transaction past the 40ms timeout.
	time.Sleep(80 * time.Millisecond)

	// Server should send a FATAL ErrorResponse with SQLSTATE 25P03 and close.
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("expected ErrorResponse after idle-in-transaction timeout, got %q", m.tag)
	}
	body := string(m.body)
	if !strings.Contains(body, sqlstateIdleInTransactionTimeout) {
		t.Fatalf("expected SQLSTATE %s in ErrorResponse, got %q", sqlstateIdleInTransactionTimeout, body)
	}
	if !strings.Contains(body, "idle-in-transaction timeout") {
		t.Fatalf("expected idle-in-transaction timeout message, got %q", body)
	}
}
