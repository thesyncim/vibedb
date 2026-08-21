package driver

import (
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

func TestPrimaryRangePredicatesSeekDurableOrderedGraph(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING)`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		if _, err := db.Exec(
			`INSERT INTO docs (id, kind) VALUES (?, ?)`, id, "keep",
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
			args: []any{"d", "f"}, want: []string{"d", "e", "f"},
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
		`"rows_scanned":3`,
		`"primary_range_bounded":true`,
		`"index_bounded":true`,
		`"index_lookups":1`,
		`"candidate_rows":3`,
	} {
		if !strings.Contains(plan, want) {
			t.Errorf("EXPLAIN ANALYZE missing %s: %s", want, plan)
		}
	}
}
