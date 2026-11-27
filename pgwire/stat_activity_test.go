package pgwire

import (
	"strings"
	"testing"
)

func TestPgStatActivityVirtualTable(t *testing.T) {
	s := newTestServer(t, Options{})

	c1 := dial(t, s)
	c1.startup(map[string]string{"user": "alice", "database": "appdb", "application_name": "worker-1"})
	if got := commandTagOf(t, c1.query(`BEGIN`)); got != "BEGIN" {
		t.Fatalf("c1 BEGIN: %q", got)
	}

	c2 := dial(t, s)
	c2.startup(map[string]string{"user": "admin", "database": "appdb", "application_name": "inspector"})

	// Query all active sessions via pg_stat_activity.
	rows := rowsOf(t, c2.query(`SELECT pid, datname, usename, application_name, state, query FROM pg_stat_activity ORDER BY application_name`))
	if len(rows) != 2 {
		t.Fatalf("pg_stat_activity row count = %d, want 2 (rows=%v)", len(rows), rows)
	}
	// inspector (c2) is actively executing the SELECT.
	if string(rows[0][3]) != "inspector" || string(rows[0][2]) != "admin" || string(rows[0][4]) != "active" {
		t.Fatalf("inspector row = %q", rows[0])
	}
	if !strings.Contains(string(rows[0][5]), "pg_stat_activity") {
		t.Fatalf("inspector query = %q, want pg_stat_activity", rows[0][5])
	}
	// worker-1 (c1) is idle in transaction.
	if string(rows[1][3]) != "worker-1" || string(rows[1][2]) != "alice" || string(rows[1][4]) != "idle in transaction" {
		t.Fatalf("worker-1 row = %q", rows[1])
	}

	// Filtering with WHERE state = 'idle in transaction' returns only worker-1.
	filtered := rowsOf(t, c2.query(`SELECT application_name FROM pg_catalog.pg_stat_activity WHERE state = 'idle in transaction'`))
	if len(filtered) != 1 || string(filtered[0][0]) != "worker-1" {
		t.Fatalf("filtered pg_stat_activity = %v, want [[worker-1]]", filtered)
	}
}
