package pgwire

import (
	"io"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

// DataRow is the only backend message emitted once per result row. Parsing,
// planning, execution, and writer construction have their own connection- or
// query-lifetime costs; this test deliberately begins after those stages and
// protects only the warmed per-row encoding contract.
func TestWarmDataRowEncodingIsAllocationFree(t *testing.T) {
	src := FromDatabase(testDatabase(t, "docs", []string{
		`{"blob":"small"}`,
		`{"blob":"also small"}`,
	}))
	lease, err := src.resolve("docs", false)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()

	for _, tc := range []struct {
		name    string
		sql     string
		formats []int16
	}{
		{name: "projected JSON", sql: `SELECT blob FROM docs`},
		{name: "text count", sql: `SELECT COUNT(*) FROM docs`},
		{
			name:    "binary count",
			sql:     `SELECT COUNT(*) FROM docs`,
			formats: []int16{formatBinary},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := query.PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Release()

			var exec query.Exec
			cursor, err := stmt.RunInto(&exec, lease.src, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !cursor.Next() {
				t.Fatal("query returned no row")
			}
			cols := columnsFor(nil, stmt.Columns(), stmt.AppendSchema(nil))
			cells := make([]query.Cell, len(cols))
			for i := range cells {
				cells[i] = cursor.Cell(i)
			}

			w := newWriter(io.Discard, 1<<20)
			var encoder rowEncoder
			if err := encoder.row(w, cells, cols, tc.formats); err != nil {
				t.Fatalf("warm DataRow encoding: %v", err)
			}
			if allocs := testing.AllocsPerRun(100, func() {
				if err := encoder.row(w, cells, cols, tc.formats); err != nil {
					panic(err)
				}
			}); allocs != 0 {
				t.Fatalf("warm DataRow encoding allocated %.2f times, want zero", allocs)
			}
		})
	}
}

// Fixed compatibility SELECTs are parsed once per query, but their already
// prepared RowDescription, DataRow, and CommandComplete messages should reuse
// the session writer's scratch. This is an encoding assertion, not a claim
// that the complete compatibility query path allocates nothing.
func TestWarmFixedResultEncodingIsAllocationFree(t *testing.T) {
	result, ok, err := (shimFunctions{
		database: "app",
		user:     "tester",
		pid:      42,
	}).parseFixedSelect(
		`SELECT version(), current_user, true, pg_backend_pid()`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(result.rows) != 1 {
		t.Fatal("compatibility SELECT did not produce one fixed result row")
	}

	for _, tc := range []struct {
		name    string
		formats []int16
	}{
		{name: "text"},
		{
			name:    "binary",
			formats: []int16{formatBinary, formatBinary, formatBinary, formatBinary},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWriter(io.Discard, 1<<20)
			encode := func() {
				if err := w.rowDescription(result.cols, tc.formats); err != nil {
					panic(err)
				}
				if err := w.fixedRow(result.cols, result.rows[0], tc.formats); err != nil {
					panic(err)
				}
				w.commandComplete(result.tag)
			}
			encode()
			if allocs := testing.AllocsPerRun(100, encode); allocs != 0 {
				t.Fatalf("warm fixed-result encoding allocated %.2f times, want zero", allocs)
			}
		})
	}
}
