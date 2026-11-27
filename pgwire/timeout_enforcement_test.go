package pgwire

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestStatementTimeoutEnforcedAndRecoverable(t *testing.T) {
	s := newTestServer(t, Options{MaxIntermediateBytes: UnlimitedResults})
	c := dial(t, s)
	c.startup(map[string]string{"user": "alice", "database": "appdb"})

	if got := commandTagOf(t, c.query(`CREATE TABLE seed (n INTEGER PRIMARY KEY)`)); got != "CREATE TABLE" {
		t.Fatalf("CREATE TABLE seed: %q", got)
	}
	// Populate 100 rows so a 4-way cross join produces 100M combinations without
	// hitting the recursive CTE 1000-iteration cap on fast CPUs.
	for i := 1; i <= 100; i++ {
		msgs := c.query(fmt.Sprintf(`INSERT INTO seed (n) VALUES (%d)`, i))
		if got := commandTagOf(t, msgs); got != "INSERT 0 1" {
			t.Fatalf("INSERT seed %d: %q (msgs=%v)", i, got, msgs)
		}
	}
	if got := commandTagOf(t, c.query(`SET statement_timeout = '5ms'`)); got != "SET" {
		t.Fatalf("SET statement_timeout: %q", got)
	}

	// 4-way cross join (100M rows) exceeds 5ms and must be canceled with
	// SQLSTATE 57014 and a statement-timeout message.
	msgs := c.query(`SELECT count(*) FROM seed a CROSS JOIN seed b CROSS JOIN seed c CROSS JOIN seed d`)
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
