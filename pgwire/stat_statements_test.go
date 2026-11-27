package pgwire

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNormalizeSQLForStats(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "literals and whitespace",
			in:   "  SELECT  id,  name  FROM  users  WHERE  id = 42  AND  name = 'alice' ; ",
			want: "SELECT id, name FROM users WHERE id = $? AND name = $?",
		},
		{
			name: "escaped single quotes and E-strings",
			in:   `SELECT * FROM t WHERE a = 'it''s' AND b = E'foo\'bar'`,
			want: `SELECT * FROM t WHERE a = $? AND b = $?`,
		},
		{
			name: "dollar-quoted strings",
			in:   `INSERT INTO docs VALUES ($$hello world$$, $tag$nested $$ ok$tag$)`,
			want: `INSERT INTO docs VALUES ($?, $?)`,
		},
		{
			name: "double-quoted identifiers and digits in identifiers",
			in:   `SELECT "col1", col2 FROM t1_v2 WHERE id = $1 AND score >= 3.14e-2`,
			want: `SELECT "col1", col2 FROM t1_v2 WHERE id = $1 AND score >= $?`,
		},
		{
			name: "line and nested block comments",
			in:   "SELECT /* outer /* inner */ */ id FROM items -- trailing\nWHERE k = 0x1A",
			want: "SELECT id FROM items WHERE k = $?",
		},
		{
			name: "comment only",
			in:   "  -- just a comment\n  /* empty */  ",
			want: "",
		},
		{
			name: "unterminated string and block comment do not panic",
			in:   "SELECT 'unterminated",
			want: "SELECT $?",
		},
		{
			name: "unterminated dollar quote",
			in:   "SELECT $$unterminated",
			want: "SELECT $?",
		},
	}

	var buf [256]byte
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, got := normalizeSQLForStats(buf[:0], tc.in)
			if string(got) != tc.want {
				t.Fatalf("normalizeSQLForStats(%q) = %q, want %q", tc.in, string(got), tc.want)
			}
			if tc.want == "" && id != 0 {
				t.Fatalf("empty normalized SQL produced non-zero id %d", id)
			}
			if tc.want != "" && id == 0 {
				t.Fatalf("non-empty normalized SQL produced zero id")
			}
		})
	}

	id1, _ := normalizeSQLForStats(buf[:0], "SELECT id FROM users WHERE id = 1")
	id2, _ := normalizeSQLForStats(buf[:0], "SELECT   id  FROM  users  WHERE  id = 999; -- same fingerprint")
	if id1 != id2 {
		t.Fatalf("equivalent queries produced different queryIDs: %d vs %d", id1, id2)
	}
}

func TestPgStatStatementsVirtualTableAndReset(t *testing.T) {
	s := newTestServer(t, Options{})

	c := dial(t, s)
	c.startup(map[string]string{"user": "alice", "database": "appdb"})

	if tag := commandTagOf(t, c.query(`CREATE TABLE items (id INTEGER PRIMARY KEY, label STRING)`)); tag != "CREATE TABLE" {
		t.Fatalf("CREATE TABLE tag = %q", tag)
	}

	// Execute 10 INSERTs with different literal values (tests numeric ORDER BY: 10 > 2).
	for i := 1; i <= 10; i++ {
		q := fmt.Sprintf(`INSERT INTO items (id, label) VALUES (%d, 'item-%d')`, i, i)
		if tag := commandTagOf(t, c.query(q)); tag != "INSERT 0 1" {
			t.Fatalf("INSERT %d tag = %q", i, tag)
		}
	}

	// Execute 2 SELECTs with different literal predicates -> should aggregate under 1 fingerprint.
	for _, id := range []int{1, 2} {
		rows := rowsOf(t, c.query(fmt.Sprintf(`SELECT id, label FROM items WHERE id = %d`, id)))
		if len(rows) != 1 {
			t.Fatalf("SELECT id=%d rows = %v", id, rows)
		}
	}

	// Execute extended-protocol parameterized SELECT twice.
	c.send(msgParse, parseMsg("sel_item", `SELECT label FROM items WHERE id = $1`))
	c.send(msgSync, nil)
	_ = c.until(msgReadyForQuery)
	for _, id := range []string{"3", "4"} {
		c.send(msgBind, bindMsg("", "sel_item", nil, [][]byte{[]byte(id)}, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
		msgs := c.until(msgReadyForQuery)
		if len(rowsOf(t, msgs)) != 1 {
			t.Fatalf("extended SELECT id=%s rows = %v", id, rowsOf(t, msgs))
		}
	}

	// Query pg_stat_statements ordered numerically by calls DESC.
	rows := rowsOf(t, c.query(`SELECT datname, usename, calls, rows, total_exec_time, min_exec_time, max_exec_time, mean_exec_time, query FROM pg_stat_statements ORDER BY calls DESC`))
	if len(rows) < 4 {
		t.Fatalf("pg_stat_statements row count = %d, want >= 4 (rows=%v)", len(rows), rows)
	}

	// Top row must be the 10-call INSERT (proving numeric sort: 10 > 2).
	top := rows[0]
	if string(top[0]) != "appdb" || string(top[1]) != "alice" {
		t.Fatalf("top row identity = (%q, %q), want (appdb, alice)", top[0], top[1])
	}
	if string(top[2]) != "10" || string(top[3]) != "10" {
		t.Fatalf("top row calls/rows = (%q, %q), want (10, 10)", top[2], top[3])
	}
	if !strings.Contains(string(top[8]), "INSERT INTO items (id, label) VALUES ($?, $?)") {
		t.Fatalf("top row query = %q", top[8])
	}
	totalMs, err := strconv.ParseFloat(string(top[4]), 64)
	if err != nil || totalMs < 0 {
		t.Fatalf("invalid total_exec_time %q: %v", top[4], err)
	}
	minMs, err := strconv.ParseFloat(string(top[5]), 64)
	if err != nil || minMs < 0 {
		t.Fatalf("invalid min_exec_time %q: %v", top[5], err)
	}
	maxMs, err := strconv.ParseFloat(string(top[6]), 64)
	if err != nil || maxMs < minMs {
		t.Fatalf("invalid max_exec_time %q (min=%v): %v", top[6], minMs, err)
	}

	// Verify schema-qualified pg_catalog.pg_stat_statements with WHERE + LIMIT.
	limited := rowsOf(t, c.query(`SELECT calls, query FROM pg_catalog.pg_stat_statements WHERE usename = 'alice' ORDER BY calls DESC LIMIT 1`))
	if len(limited) != 1 || string(limited[0][0]) != "10" {
		t.Fatalf("limited pg_stat_statements = %v", limited)
	}

	// Verify extended query fingerprint preserved $1 and recorded 2 calls, 2 rows.
	foundExt := false
	for _, r := range rows {
		if string(r[8]) == "SELECT label FROM items WHERE id = $1" {
			foundExt = true
			if string(r[2]) != "2" || string(r[3]) != "2" {
				t.Fatalf("extended stat calls/rows = (%q, %q), want (2, 2)", r[2], r[3])
			}
		}
	}
	if !foundExt {
		t.Fatalf("extended query fingerprint not found in pg_stat_statements: %v", rows)
	}

	// Reset via pg_stat_statements_reset() and confirm table is empty.
	resetRows := rowsOf(t, c.query(`SELECT pg_stat_statements_reset()`))
	if len(resetRows) != 1 {
		t.Fatalf("pg_stat_statements_reset() rows = %v, want 1 row", resetRows)
	}
	afterReset := rowsOf(t, c.query(`SELECT queryid FROM pg_stat_statements`))
	if len(afterReset) != 0 {
		t.Fatalf("pg_stat_statements after reset = %v, want empty", afterReset)
	}
}

func TestStmtStatsBoundedEvictionAndDisable(t *testing.T) {
	reg := newStmtStatsRegistry(3)
	// Two hot queries with 10 calls each.
	for i := 0; i < 10; i++ {
		reg.record("db", "u", "SELECT * FROM hot_a WHERE id = 1", time.Millisecond, 1)
		reg.record("db", "u", "SELECT * FROM hot_b WHERE id = 2", time.Millisecond, 1)
	}
	// Many one-off ad-hoc queries should evict each other, never hot_a or hot_b.
	for i := 0; i < 20; i++ {
		reg.record("db", "u", fmt.Sprintf("SELECT col_%d FROM adhoc_%d", i, i), 100*time.Microsecond, 1)
	}
	snaps := reg.snapshot()
	if len(snaps) != 3 {
		t.Fatalf("snapshot len = %d, want 3", len(snaps))
	}
	seenA, seenB := false, false
	for _, st := range snaps {
		if strings.Contains(st.query, "hot_a") && st.calls == 10 {
			seenA = true
		}
		if strings.Contains(st.query, "hot_b") && st.calls == 10 {
			seenB = true
		}
	}
	if !seenA || !seenB {
		t.Fatalf("hot queries were evicted: snaps=%+v", snaps)
	}

	// Disabled stats via DisableStatementStats.
	s := newTestServer(t, Options{MaxStatementStats: DisableStatementStats})
	c := dial(t, s)
	c.startup(map[string]string{"user": "alice", "database": "appdb"})
	_ = c.query(`CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	_ = c.query(`INSERT INTO t (id) VALUES (1)`)
	if rows := rowsOf(t, c.query(`SELECT queryid FROM pg_stat_statements`)); len(rows) != 0 {
		t.Fatalf("disabled pg_stat_statements returned %v, want empty", rows)
	}

	// Values below DisableStatementStats are rejected.
	if _, err := NewServer(s.db, Options{Auth: Trust(), MaxStatementStats: DisableStatementStats - 1}); err == nil {
		t.Fatal("expected error for MaxStatementStats < DisableStatementStats, got nil")
	}
}

func TestStmtStatsConcurrentStress(t *testing.T) {
	reg := newStmtStatsRegistry(32)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				q := fmt.Sprintf("SELECT * FROM tbl_%d WHERE id = %d", (id+i)%40, i)
				reg.record("db", "user", q, time.Duration(i+1)*time.Microsecond, int64(i%5))
				if i%100 == 0 {
					_ = reg.snapshot()
				}
				if id == 0 && i == 250 {
					reg.reset()
				}
			}
		}(g)
	}
	wg.Wait()
	if snaps := reg.snapshot(); len(snaps) > 32 {
		t.Fatalf("snapshot exceeded bound: %d > 32", len(snaps))
	}
}

func BenchmarkStmtStatsRecordHotPath(b *testing.B) {
	reg := newStmtStatsRegistry(DefaultMaxStatementStats)
	const q = "SELECT id, name FROM users WHERE id = 42 AND status = 'active'"
	reg.record("appdb", "alice", q, time.Millisecond, 1)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reg.record("appdb", "alice", q, 250*time.Microsecond, 1)
		}
	})
}
