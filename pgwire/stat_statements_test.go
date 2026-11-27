package pgwire

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
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

func BenchmarkStmtStatsRecordHotPathLongQuery(b *testing.B) {
	reg := newStmtStatsRegistry(DefaultMaxStatementStats)
	q := "SELECT " + strings.Repeat("col_long_identifier_name, ", 24) + "id FROM wide_analytics_events WHERE tenant_id = 42 AND region = 'us-east-1' AND status = 'active'"
	if len(q) <= 256 {
		b.Fatalf("expected long query > 256 bytes, got %d", len(q))
	}
	reg.record("appdb", "alice", q, time.Millisecond, 1)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reg.record("appdb", "alice", q, 250*time.Microsecond, 1)
		}
	})
}

func TestPgStatStatementsResetDeferredToExecute(t *testing.T) {
	s := newTestServer(t, Options{})
	c := dial(t, s)
	c.startup(map[string]string{"user": "alice", "database": "appdb"})

	_ = c.query(`CREATE TABLE rtest (id INTEGER PRIMARY KEY)`)
	_ = c.query(`INSERT INTO rtest (id) VALUES (1)`)

	// Malformed query containing pg_stat_statements_reset() must NOT reset stats
	// (note: a single trailing word is a valid SQL column alias, so use FROM).
	_ = c.query(`SELECT pg_stat_statements_reset() FROM nonexistent_table`)
	if rows := rowsOf(t, c.query(`SELECT queryid FROM pg_stat_statements`)); len(rows) == 0 {
		t.Fatal("malformed query with pg_stat_statements_reset() unexpectedly reset stats")
	}

	// Parse + Describe without Execute must NOT reset stats.
	c.send(msgParse, parseMsg("reset_stmt", `SELECT pg_stat_statements_reset()`))
	c.send(msgDescribe, describeMsg('S', "reset_stmt"))
	c.send(msgSync, nil)
	_ = c.until(msgReadyForQuery)
	if rows := rowsOf(t, c.query(`SELECT queryid FROM pg_stat_statements`)); len(rows) == 0 {
		t.Fatal("Parse+Describe of pg_stat_statements_reset() unexpectedly reset stats")
	}

	// First Execute of prepared reset_stmt MUST reset stats.
	c.send(msgBind, bindMsg("", "reset_stmt", nil, nil, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	_ = c.until(msgReadyForQuery)
	if rows := rowsOf(t, c.query(`SELECT queryid FROM pg_stat_statements`)); len(rows) != 0 {
		t.Fatalf("after 1st prepared reset, pg_stat_statements = %v, want empty", rows)
	}

	// Record another statement, then re-execute the SAME prepared reset_stmt.
	_ = c.query(`INSERT INTO rtest (id) VALUES (2)`)
	if rows := rowsOf(t, c.query(`SELECT queryid FROM pg_stat_statements`)); len(rows) == 0 {
		t.Fatal("expected non-empty pg_stat_statements before 2nd prepared reset")
	}
	c.send(msgBind, bindMsg("", "reset_stmt", nil, nil, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	_ = c.until(msgReadyForQuery)
	if rows := rowsOf(t, c.query(`SELECT queryid FROM pg_stat_statements`)); len(rows) != 0 {
		t.Fatalf("after 2nd prepared reset, pg_stat_statements = %v, want empty", rows)
	}
}

func TestPgStatStatementsSuspendedPortalRecordedOnce(t *testing.T) {
	s := newTestServer(t, Options{})
	c := dial(t, s)
	c.startup(map[string]string{"user": "alice", "database": "appdb"})

	_ = c.query(`CREATE TABLE ptest (id INTEGER PRIMARY KEY)`)
	_ = c.query(`INSERT INTO ptest (id) VALUES (1), (2), (3)`)
	_ = c.query(`SELECT pg_stat_statements_reset()`)

	_ = c.query(`BEGIN`)
	c.send(msgParse, parseMsg("sel_all", `SELECT id FROM ptest ORDER BY id`))
	c.send(msgBind, bindMsg("p1", "sel_all", nil, nil, nil))
	// Fetch 1 row at a time (suspending portal twice before exhaustion).
	totalRows := 0
	for i := 0; i < 4; i++ {
		c.send(msgExecute, executeMsg("p1", 1))
		c.send(msgSync, nil)
		msgs := c.until(msgReadyForQuery)
		totalRows += len(rowsOf(t, msgs))
	}
	_ = c.query(`COMMIT`)
	if totalRows != 3 {
		t.Fatalf("suspended portal fetched %d rows, want 3", totalRows)
	}

	rows := rowsOf(t, c.query(`SELECT calls, rows, query FROM pg_stat_statements`))
	if len(rows) != 1 {
		t.Fatalf("pg_stat_statements len = %d, want 1 (%v)", len(rows), rows)
	}
	if string(rows[0][0]) != "1" || string(rows[0][1]) != "3" {
		t.Fatalf("suspended portal stat calls/rows = (%q, %q), want (1, 3)", rows[0][0], rows[0][1])
	}
}

func TestVirtualCatalogOrderByStrictWeakOrderingAndInt64Precision(t *testing.T) {
	// Two 64-bit integers that collide when converted to float64 (2^53+1 vs 2^53+2).
	a := strconv.FormatInt((1<<53)+1, 10)
	b := strconv.FormatInt((1<<53)+2, 10)
	if !compareVirtualCatalogCell(&a, &b, virtualSortInt64, false) {
		t.Fatalf("int64 ASC failed to distinguish %s < %s", a, b)
	}
	if compareVirtualCatalogCell(&b, &a, virtualSortInt64, false) {
		t.Fatalf("int64 ASC asymmetry for %s vs %s", b, a)
	}
	if !compareVirtualCatalogCell(&b, &a, virtualSortInt64, true) {
		t.Fatalf("int64 DESC failed to distinguish %s > %s", b, a)
	}

	// Verify strict weak ordering (irreflexive, asymmetric, transitive) across
	// mixed numeric-looking and non-numeric strings for all sort modes.
	vals := []*string{nil, strPtr(""), strPtr("2"), strPtr("10"), strPtr("1a"), strPtr("-5"), strPtr("2.5"), strPtr("NaN")}
	for _, mode := range []virtualCatalogSortKind{virtualSortString, virtualSortInt64, virtualSortFloat64} {
		for _, desc := range []bool{false, true} {
			for i, x := range vals {
				if compareVirtualCatalogCell(x, x, mode, desc) {
					t.Fatalf("mode=%d desc=%v not irreflexive on idx=%d", mode, desc, i)
				}
				for j, y := range vals {
					xy := compareVirtualCatalogCell(x, y, mode, desc)
					yx := compareVirtualCatalogCell(y, x, mode, desc)
					if xy && yx {
						t.Fatalf("mode=%d desc=%v not asymmetric on (%d, %d)", mode, desc, i, j)
					}
					if xy {
						for k, z := range vals {
							if compareVirtualCatalogCell(y, z, mode, desc) && !compareVirtualCatalogCell(x, z, mode, desc) {
								t.Fatalf("mode=%d desc=%v not transitive on (%d, %d, %d)", mode, desc, i, j, k)
							}
						}
					}
				}
			}
		}
	}
}

func TestHashNormalizedSQLMatchesNormalizeAndZeroAllocLongQuery(t *testing.T) {
	samples := []string{
		"",
		"   -- comment only",
		"SELECT 1; ; ",
		"SELECT 1; SELECT 2;",
		`SELECT "col1;" FROM t WHERE a = 'x' AND b = 42; -- end`,
		"SELECT " + strings.Repeat("col_long_identifier_name, ", 24) + "id FROM wide_table WHERE id = 12345 AND name = 'alice'",
	}
	var buf [256]byte
	for _, q := range samples {
		wantID, norm := normalizeSQLForStats(buf[:0], q)
		gotID, ok := hashNormalizedSQL(q)
		if (len(norm) > 0) != ok || gotID != wantID {
			t.Fatalf("hashNormalizedSQL(%q) = (%d, %v), want (%d, %v)", q, gotID, ok, wantID, len(norm) > 0)
		}
	}

	// Verify 0 heap allocations on the steady-state hot path for a >600-byte SQL query.
	reg := newStmtStatsRegistry(DefaultMaxStatementStats)
	longQ := "SELECT " + strings.Repeat("col_long_identifier_name, ", 24) + "id FROM wide_table WHERE id = 12345 AND name = 'alice'"
	reg.record("appdb", "alice", longQ, time.Millisecond, 1)
	allocs := testing.AllocsPerRun(100, func() {
		reg.record("appdb", "alice", longQ, time.Millisecond, 1)
	})
	if allocs != 0 {
		t.Fatalf("steady-state hot-path allocs for %d-byte SQL = %.1f, want 0", len(longQ), allocs)
	}
}

func TestPgStatStatementsPipelinedAsyncExec(t *testing.T) {
	b := atomicTestBackend{embeddedBackend{testDatabase(t, "users", nil)}}
	s, err := NewServerWithBackend(b, Options{Auth: Trust(), Database: "pipedb"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c := dial(t, s)
	c.startup(map[string]string{"user": "alice", "database": "pipedb"})

	// Pipeline 4 autocommit extended-protocol INSERTs back-to-back (each with
	// its own Sync, sent before reading any ReadyForQuery) so finishExtendedBatch
	// defers responses and executeRuntimeExecAsync runs them concurrently on
	// borrowed direct sessions while the read loop rebinds the unnamed portal.
	const insertSQL = `INSERT INTO users ("_pgwire_key", id, name) VALUES ($1, $2, $3)`
	c.send(msgParse, parseMsg("ins_user", insertSQL))
	for i := 1; i <= 4; i++ {
		k := fmt.Sprintf("k%d", i)
		name := fmt.Sprintf("user-%d", i)
		c.send(msgBind, bindMsg("", "ins_user", nil, [][]byte{[]byte(k), []byte(k), []byte(name)}, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
	}
	for i := 1; i <= 4; i++ {
		requireWireOK(t, c.until(msgReadyForQuery))
	}

	// Verify all 4 pipelined writes committed with their exact distinct values
	// (catching any portal valueSlots/argStore aliasing corruption).
	gotUsers := rowsOf(t, c.query(`SELECT id, name FROM users ORDER BY id`))
	if len(gotUsers) != 4 {
		t.Fatalf("pipelined INSERTs committed %d rows, want 4: %v", len(gotUsers), gotUsers)
	}
	for i := 1; i <= 4; i++ {
		wantID, wantName := fmt.Sprintf(`"k%d"`, i), fmt.Sprintf(`"user-%d"`, i)
		if string(gotUsers[i-1][0]) != wantID || string(gotUsers[i-1][1]) != wantName {
			t.Fatalf("row %d = (%q, %q), want (%q, %q)", i, gotUsers[i-1][0], gotUsers[i-1][1], wantID, wantName)
		}
	}

	// Pipeline 2 DELETEs back-to-back via executeRuntimeExecAsync.
	const deleteSQL = `DELETE FROM users WHERE id = $1`
	c.send(msgParse, parseMsg("del_user", deleteSQL))
	for _, id := range []string{"k1", "k2"} {
		c.send(msgBind, bindMsg("", "del_user", nil, [][]byte{[]byte(id)}, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
	}
	for i := 0; i < 2; i++ {
		requireWireOK(t, c.until(msgReadyForQuery))
	}

	rows := rowsOf(t, c.query(`SELECT datname, usename, calls, rows, total_exec_time, query FROM pg_stat_statements ORDER BY calls DESC`))
	var foundInsert, foundDelete bool
	for _, r := range rows {
		datname, usename, calls, rowCount, totalMsStr, q := string(r[0]), string(r[1]), string(r[2]), string(r[3]), string(r[4]), string(r[5])
		if datname != "pipedb" || usename != "alice" {
			t.Fatalf("unexpected stat identity (%q, %q), want (pipedb, alice)", datname, usename)
		}
		if totalMs, err := strconv.ParseFloat(totalMsStr, 64); err != nil || totalMs < 0 {
			t.Fatalf("invalid total_exec_time %q: %v", totalMsStr, err)
		}
		switch q {
		case insertSQL:
			foundInsert = true
			if calls != "4" || rowCount != "4" {
				t.Fatalf("pipelined INSERT stat calls/rows = (%q, %q), want (4, 4)", calls, rowCount)
			}
		case deleteSQL:
			foundDelete = true
			if calls != "2" || rowCount != "2" {
				t.Fatalf("pipelined DELETE stat calls/rows = (%q, %q), want (2, 2)", calls, rowCount)
			}
		}
	}
	if !foundInsert || !foundDelete {
		t.Fatalf("missing pipelined async stats (insert=%v, delete=%v): rows=%v", foundInsert, foundDelete, rows)
	}
}

func TestPgStatStatementsPipelinedAsyncExecBinaryAndBackpressure(t *testing.T) {
	b := atomicTestBackend{embeddedBackend{testDatabase(t, "items", nil)}}
	s, err := NewServerWithBackend(b, Options{Auth: Trust(), Database: "binpipe"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c := dial(t, s)
	c.startup(map[string]string{"user": "bob", "database": "binpipe"})

	// Pipeline 20 autocommit INSERTs (> directPipelineDepth=16) with binary
	// parameters (text, int8, float8, bool, jsonb, and NULL) so both the async
	// direct-session pool and the backpressure drain+sync fallback execute.
	const n = 20
	const insertSQL = `INSERT INTO items ("_pgwire_key", id, score, active, tag, note) VALUES ($1, $2, $3, $4, $5, $6)`
	c.send(msgParse, parseMsg("ins_bin", insertSQL, oidText, oidInt8, oidFloat8, oidBool, oidJSONB, oidText))
	formats := []int16{formatBinary, formatBinary, formatBinary, formatBinary, formatBinary, formatText}
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("k%02d", i)
		var idBuf [8]byte
		binary.BigEndian.PutUint64(idBuf[:], uint64(i*100))
		var scoreBuf [8]byte
		binary.BigEndian.PutUint64(scoreBuf[:], math.Float64bits(float64(i)+0.5))
		var boolBuf [1]byte
		if i%2 == 0 {
			boolBuf[0] = 1
		}
		// Binary jsonb format: version byte 1 followed by JSON text.
		jsonbVal := append([]byte{1}, fmt.Sprintf(`"tag-%02d"`, i)...)
		var note []byte
		if i%3 != 0 {
			note = []byte(fmt.Sprintf("note-%02d", i))
		}
		c.send(msgBind, bindMsg("", "ins_bin", formats, [][]byte{
			[]byte(key), idBuf[:], scoreBuf[:], boolBuf[:], jsonbVal, note,
		}, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
	}
	for i := 0; i < n; i++ {
		requireWireOK(t, c.until(msgReadyForQuery))
	}

	got := rowsOf(t, c.query(`SELECT id, score, active, tag, note FROM items ORDER BY id`))
	if len(got) != n {
		t.Fatalf("pipelined binary INSERTs committed %d rows, want %d", len(got), n)
	}
	for i := 1; i <= n; i++ {
		r := got[i-1]
		wantID := strconv.Itoa(i * 100)
		wantScore := strconv.FormatFloat(float64(i)+0.5, 'f', -1, 64)
		wantActive := "false"
		if i%2 == 0 {
			wantActive = "true"
		}
		wantTag := fmt.Sprintf(`"tag-%02d"`, i)
		if string(r[0]) != wantID || string(r[1]) != wantScore || string(r[2]) != wantActive || string(r[3]) != wantTag {
			t.Fatalf("row %d = (%q,%q,%q,%q), want (%q,%q,%q,%q)",
				i, r[0], r[1], r[2], r[3], wantID, wantScore, wantActive, wantTag)
		}
		if i%3 == 0 {
			if r[4] != nil {
				t.Fatalf("row %d note = %q, want NULL", i, r[4])
			}
		} else {
			wantNote := fmt.Sprintf(`"note-%02d"`, i)
			if string(r[4]) != wantNote {
				t.Fatalf("row %d note = %q, want %q", i, r[4], wantNote)
			}
		}
	}

	statRows := rowsOf(t, c.query(`SELECT calls, rows, query FROM pg_stat_statements WHERE query = 'INSERT INTO items ("_pgwire_key", id, score, active, tag, note) VALUES ($1, $2, $3, $4, $5, $6)'`))
	if len(statRows) != 1 || string(statRows[0][0]) != strconv.Itoa(n) || string(statRows[0][1]) != strconv.Itoa(n) {
		t.Fatalf("pipelined binary+backpressure stats = %v, want 1 row with calls=%d rows=%d", statRows, n, n)
	}
}

func TestDetachBoundArgsIsolationAndBoundedAllocs(t *testing.T) {
	argStore := []byte("hello-world9007199254740993")
	decodeStore := []byte("decoded-json-text")
	rawBytes := []byte("raw-bytes")
	rawJSON := []byte(`{"k":1}`)
	slots := []boundValueSlot{
		{text: byteview.String(argStore[:11])},
		{number: query.Number(byteview.String(argStore[11:]))},
		{integer: 42},
		{floating: 3.5},
		{boolean: true},
		{text: byteview.String(decodeStore)},
	}
	orig := []any{
		&slots[0].text,
		&slots[1].number,
		&slots[2].integer,
		&slots[3].floating,
		&slots[4].boolean,
		&slots[5].text,
		rawBytes,
		vibejson.RawValue{Src: rawJSON},
		nil,
	}

	detached := detachBoundArgs(orig)

	// Overwrite every source backing buffer and slot to simulate a concurrent
	// pipelined Bind on the unnamed portal.
	for i := range argStore {
		argStore[i] = 'X'
	}
	for i := range decodeStore {
		decodeStore[i] = 'Y'
	}
	for i := range rawBytes {
		rawBytes[i] = 'Z'
	}
	for i := range rawJSON {
		rawJSON[i] = ' '
	}
	clear(slots)

	if got := *(detached[0].(*string)); got != "hello-world" {
		t.Fatalf("detached[0] = %q, want %q", got, "hello-world")
	}
	if got := *(detached[1].(*query.Number)); got != "9007199254740993" {
		t.Fatalf("detached[1] = %q, want %q", got, "9007199254740993")
	}
	if got := *(detached[2].(*int64)); got != 42 {
		t.Fatalf("detached[2] = %d, want 42", got)
	}
	if got := *(detached[3].(*float64)); got != 3.5 {
		t.Fatalf("detached[3] = %v, want 3.5", got)
	}
	if got := *(detached[4].(*bool)); !got {
		t.Fatalf("detached[4] = %v, want true", got)
	}
	if got := *(detached[5].(*string)); got != "decoded-json-text" {
		t.Fatalf("detached[5] = %q, want %q", got, "decoded-json-text")
	}
	if got := string(detached[6].([]byte)); got != "raw-bytes" {
		t.Fatalf("detached[6] = %q, want %q", got, "raw-bytes")
	}
	if got := string(detached[7].(vibejson.RawValue).Src); got != `{"k":1}` {
		t.Fatalf("detached[7] = %q, want %q", got, `{"k":1}`)
	}
	if detached[8] != nil {
		t.Fatalf("detached[8] = %v, want nil", detached[8])
	}

	// Verify O(1) bounded allocations (3 allocs: out slice, slots slice, byte arena)
	// for 8 non-nil parameters.
	s0, s1 := "alpha", "beta"
	num := query.Number("1234567890")
	i64, f64, bval := int64(7), 2.5, true
	benchArgs := []any{&s0, &s1, &num, &i64, &f64, &bval, &s0, &s1}
	allocs := testing.AllocsPerRun(100, func() {
		_ = detachBoundArgs(benchArgs)
	})
	if allocs > 3 {
		t.Fatalf("detachBoundArgs allocs for 8 params = %.1f, want <= 3", allocs)
	}
}
