package pgwire

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestPgCancelAndTerminateBackend(t *testing.T) {
	s := newTestServer(t, Options{})

	// 1. Unknown PID returns false ("f").
	admin := dial(t, s)
	admin.startup(map[string]string{"user": "admin", "database": "app"})
	if rows := rowsOf(t, admin.query(`SELECT pg_cancel_backend(999999)`)); len(rows) != 1 || string(rows[0][0]) != "f" {
		t.Fatalf("pg_cancel_backend(999999) = %v, want [[f]]", rows)
	}
	if rows := rowsOf(t, admin.query(`SELECT pg_terminate_backend(999999)`)); len(rows) != 1 || string(rows[0][0]) != "f" {
		t.Fatalf("pg_terminate_backend(999999) = %v, want [[f]]", rows)
	}

	// 2. Open a second session, get its PID, verify pg_cancel_backend returns true
	//    and keeps the session alive, then verify pg_terminate_backend returns true
	//    and closes the target session cleanly.
	victim := dial(t, s)
	victim.startup(map[string]string{"user": "admin", "database": "app", "application_name": "victim"})
	pidRows := rowsOf(t, victim.query(`SELECT pg_backend_pid()`))
	if len(pidRows) != 1 {
		t.Fatalf("pg_backend_pid rows = %v", pidRows)
	}
	victimPID, err := strconv.Atoi(string(pidRows[0][0]))
	if err != nil || victimPID <= 0 {
		t.Fatalf("invalid victim pid %q: %v", pidRows[0][0], err)
	}

	if rows := rowsOf(t, admin.query(fmt.Sprintf(`SELECT pg_cancel_backend(%d)`, victimPID))); len(rows) != 1 || string(rows[0][0]) != "t" {
		t.Fatalf("pg_cancel_backend(%d) = %v, want [[t]]", victimPID, rows)
	}
	// Victim is still alive after cancel.
	if rows := rowsOf(t, victim.query(`SELECT 1`)); len(rows) != 1 || string(rows[0][0]) != "1" {
		t.Fatalf("victim SELECT 1 after cancel = %v, want [[1]]", rows)
	}

	// Terminate victim via pg_terminate_backend.
	if rows := rowsOf(t, admin.query(fmt.Sprintf(`SELECT pg_terminate_backend(%d)`, victimPID))); len(rows) != 1 || string(rows[0][0]) != "t" {
		t.Fatalf("pg_terminate_backend(%d) = %v, want [[t]]", victimPID, rows)
	}

	// Wait briefly for server unregister and verify victim no longer appears in pg_stat_activity.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		act := rowsOf(t, admin.query(`SELECT application_name FROM pg_stat_activity WHERE application_name = 'victim'`))
		if len(act) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("terminated victim session still present in pg_stat_activity")
}
