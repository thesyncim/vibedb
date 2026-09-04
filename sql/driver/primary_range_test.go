package driver

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestPrimaryRangeBindingReusesByteArena(t *testing.T) {
	connection := directTestConn(t).(*conn)
	directExec(t, connection, `CREATE TABLE docs (id STRING PRIMARY KEY)`, nil)
	statement, err := connection.Prepare(
		`SELECT id FROM docs WHERE id > ? AND id <= ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	prepared := statement.(*stmt)
	args := []any{"d", "h"}
	bind := func() primaryRangeBinding {
		binding, eligible, empty, bindErr := connection.bindPrimaryRangeProgram(
			prepared.primaryRange, args, 4<<10,
		)
		if bindErr != nil || !eligible || empty ||
			len(binding.lower) == 0 || len(binding.upper) == 0 ||
			!binding.lowerExclusive {
			panic("primary range bind failed")
		}
		return binding
	}
	_ = bind()
	if allocs := testing.AllocsPerRun(1000, func() { _ = bind() }); allocs != 0 {
		t.Fatalf("warm primary range bind allocations = %v, want 0", allocs)
	}
}

func TestPrimaryRangePredicateCoverage(t *testing.T) {
	connection := directTestConn(t).(*conn)
	directExec(t, connection, `CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING)`, nil)
	for _, test := range []struct {
		sql  string
		want bool
	}{
		{`SELECT id FROM docs WHERE id >= ? ORDER BY id LIMIT 32`, true},
		{`SELECT id FROM docs WHERE id > ? AND id <= ? ORDER BY id LIMIT 32`, true},
		{`SELECT id FROM docs WHERE id >= ? AND kind = 'keep' ORDER BY id LIMIT 32`, false},
	} {
		statement, err := connection.Prepare(test.sql)
		if err != nil {
			t.Fatal(err)
		}
		prepared := statement.(*stmt)
		if prepared.primaryRange == nil || prepared.primaryRange.coversPredicate != test.want {
			t.Errorf("coverage for %q = %+v, want %t", test.sql, prepared.primaryRange, test.want)
		}
		if err := statement.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPrimaryRangePredicatesSeekDurableOrderedGraph(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING)`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		kind := "keep"
		if id == "e" {
			kind = "drop"
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, kind) VALUES (?, ?)`, id, kind,
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`CREATE INDEX docs_kind ON docs(kind)`); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		sql  string
		args []any
		want []string
	}{
		{
			name: "between under conjunction",
			sql:  `SELECT id FROM docs WHERE kind = 'keep' AND id BETWEEN ? AND ? ORDER BY id`,
			args: []any{"d", "f"}, want: []string{"d", "f"},
		},
		{
			name: "exclusive",
			sql:  `SELECT id FROM docs WHERE id > ? AND id < ? ORDER BY id`,
			args: []any{"d", "h"}, want: []string{"e", "f", "g"},
		},
		{
			name: "inclusive",
			sql:  `SELECT id FROM docs WHERE id >= ? AND id <= ? ORDER BY id`,
			args: []any{"d", "f"}, want: []string{"d", "e", "f"},
		},
		{
			name: "lower only",
			sql:  `SELECT id FROM docs WHERE id >= ? ORDER BY id`,
			args: []any{"h"}, want: []string{"h", "i", "j"},
		},
		{
			name: "upper only",
			sql:  `SELECT id FROM docs WHERE id < ? ORDER BY id`,
			args: []any{"d"}, want: []string{"a", "b", "c"},
		},
		{
			name: "contradiction",
			sql:  `SELECT id FROM docs WHERE id >= ? AND id < ? ORDER BY id`,
			args: []any{"h", "d"},
		},
		{
			name: "null bound",
			sql:  `SELECT id FROM docs WHERE id BETWEEN ? AND ? ORDER BY id`,
			args: []any{nil, "f"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, err := db.Query(test.sql, test.args...)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				got = append(got, id)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("ids = %v, want %v", got, test.want)
			}
		})
	}

	var plan string
	if err := db.QueryRow(`
		EXPLAIN SELECT id FROM docs
		WHERE kind = 'keep' AND id BETWEEN ? AND ?
		ORDER BY id`, "d", "f").Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, `"access_path":"primary-key-range-or-scan"`) {
		t.Fatalf("EXPLAIN did not expose primary range candidate: %s", plan)
	}
	if err := db.QueryRow(`
		EXPLAIN ANALYZE
		SELECT id FROM docs
		WHERE kind = 'keep' AND id BETWEEN ? AND ?
		ORDER BY id`, "d", "f").Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !vibejson.Valid([]byte(plan)) {
		t.Fatalf("EXPLAIN ANALYZE returned invalid JSON: %s", plan)
	}
	for _, want := range []string{
		`"actual_access_path":"primary-key-range"`,
		`"rows_total":10`,
		`"rows_scanned":2`,
		`"primary_range_bounded":true`,
		`"index_bounded":true`,
		`"index_lookups":1`,
		`"candidate_rows":2`,
	} {
		if !strings.Contains(plan, want) {
			t.Errorf("EXPLAIN ANALYZE missing %s: %s", want, plan)
		}
	}
}

func TestTransactionPrimaryRangeKeepsOverlayVisibility(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, n INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id,n) VALUES ('a',1),('c',3)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	check := func(want int) {
		t.Helper()
		var got int
		if err := tx.QueryRow(`SELECT SUM(n) FROM docs WHERE id >= 'b'`).Scan(&got); err != nil || got != want {
			t.Fatalf("sum=%d want=%d err=%v", got, want, err)
		}
	}
	check(3)
	if _, err := tx.Exec(`INSERT INTO docs (id,n) VALUES ('b',2)`); err != nil {
		t.Fatal(err)
	}
	check(5)
	if _, err := tx.Exec(`UPDATE docs SET n=30 WHERE id='c'`); err != nil {
		t.Fatal(err)
	}
	check(32)
	if _, err := tx.Exec(`DELETE FROM docs WHERE id='b'`); err != nil {
		t.Fatal(err)
	}
	check(30)
}

func TestSQLPrimaryOrderedLimit(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE pages (id INTEGER PRIMARY KEY, bucket INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for batch := range 16 {
		var insert strings.Builder
		insert.WriteString("INSERT INTO pages (id,bucket) VALUES ")
		for row := range 64 {
			if row != 0 {
				insert.WriteByte(',')
			}
			id := batch*64 + row
			fmt.Fprintf(&insert, "(%d,%d)", id, id%7)
		}
		if _, err := db.Exec(insert.String()); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		`SELECT id FROM pages WHERE id >= 0 ORDER BY id LIMIT 17`,
		`SELECT id FROM pages p WHERE p.id >= 0 AND p.bucket=5 ORDER BY p.id LIMIT 17`,
	} {
		var plan string
		if err := db.QueryRow("EXPLAIN ANALYZE " + statement).Scan(&plan); err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			Plan struct {
				Analyze struct {
					RowsScanned uint64 `json:"rows_scanned"`
				} `json:"analyze"`
			} `json:"plan"`
		}
		if err := vibejson.Unmarshal([]byte(plan), &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Plan.Analyze.RowsScanned == 0 || decoded.Plan.Analyze.RowsScanned >= 512 {
			t.Fatalf("LIMIT did not bound SQL scan: %s", plan)
		}
	}
}
