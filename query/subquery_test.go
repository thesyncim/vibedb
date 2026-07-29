package query

import (
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func subqueryDatabase(t testing.TB) store.DatabaseSnapshot {
	t.Helper()
	var database store.Database
	outer, err := database.CreateCollection("orders", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := database.CreateCollection("customers", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for key, doc := range map[string]string{
		"o1": `{"id":"o1","customer":"c1"}`,
		"o2": `{"id":"o2","customer":"c2"}`,
		"o3": `{"id":"o3","customer":"c3"}`,
		"o4": `{"id":"o4","customer":null}`,
	} {
		if _, err := outer.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	for key, doc := range map[string]string{
		"c1": `{"id":"c1","tier":"pro","score":7}`,
		"c2": `{"id":"c2","tier":"free","score":9}`,
		"c3": `{"id":"c3","tier":"pro","score":null}`,
	} {
		if _, err := inner.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	return database.Snapshot()
}

func BenchmarkSQLInSubquery(b *testing.B) {
	catalog := subqueryDatabase(b)
	stmt, err := PrepareStatement(
		`SELECT id FROM orders WHERE customer IN (` +
			`SELECT id FROM customers WHERE tier = ?) ORDER BY id`,
	)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	defer exec.Release()
	args := []any{"pro"}
	source := FromDatabase(catalog, "orders")
	for i := 0; i < 2; i++ {
		cursor, err := stmt.RunInto(&exec, source, args)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, err := stmt.RunInto(&exec, source, args)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
}

func TestSQLUncorrelatedSubqueries(t *testing.T) {
	catalog := subqueryDatabase(t)
	tests := []struct {
		name string
		sql  string
		args []any
		want string
	}{
		{
			name: "IN SELECT",
			sql:  `SELECT id FROM orders WHERE customer IN (SELECT id FROM customers WHERE tier = 'pro') ORDER BY id`,
			want: "64:\"o1\"|\n64:\"o3\"|",
		},
		{
			name: "NOT IN null is unknown",
			sql:  `SELECT id FROM orders WHERE customer NOT IN (SELECT score FROM customers) ORDER BY id`,
			want: "",
		},
		{
			name: "EXISTS true",
			sql:  `SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE tier = 'pro') ORDER BY id`,
			want: "64:\"o1\"|\n64:\"o2\"|\n64:\"o3\"|\n64:\"o4\"|",
		},
		{
			name: "EXISTS false",
			sql:  `SELECT id FROM orders WHERE EXISTS (SELECT id FROM customers WHERE tier = ?)`,
			args: []any{"missing"},
			want: "",
		},
		{
			name: "scalar",
			sql:  `SELECT id FROM orders WHERE customer = (SELECT id FROM customers WHERE score = 7)`,
			want: "64:\"o1\"|",
		},
		{
			name: "empty scalar is null",
			sql:  `SELECT id FROM orders WHERE customer = (SELECT id FROM customers WHERE score = 100)`,
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatalf("PrepareStatement: %v", err)
			}
			got := runStatement(t, stmt, FromDatabase(catalog, "orders"), tc.args...)
			got = strings.TrimSpace(strings.TrimPrefix(got, "|id\n"))
			if got != tc.want {
				t.Fatalf("rows = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSQLScalarSubqueryRejectsMultipleRows(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT id FROM orders WHERE customer = (SELECT id FROM customers)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	if _, err := stmt.RunInto(&exec, FromDatabase(catalog, "orders"), nil); err == nil ||
		!strings.Contains(err.Error(), "more than one row") {
		t.Fatalf("RunInto error = %v, want scalar cardinality error", err)
	}
	if got := stmt.nested.subqueries[0].exec.Result.RowCount; got != 2 {
		t.Fatalf("scalar nested rows = %d, want the two-row cardinality cap", got)
	}
}

func TestSQLExistsStopsAfterOneNestedRow(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM customers)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	if _, err := stmt.RunInto(&exec, FromDatabase(catalog, "orders"), nil); err != nil {
		t.Fatal(err)
	}
	if got := stmt.nested.subqueries[0].exec.Result.RowCount; got != 1 {
		t.Fatalf("EXISTS nested rows = %d, want one-row early stop", got)
	}
}

func TestSQLSubqueryPlaceholderOrder(t *testing.T) {
	catalog := subqueryDatabase(t)
	for _, tc := range []struct {
		name string
		sql  string
		args []any
		want string
	}{
		{
			name: "outer before nested",
			sql: `SELECT id FROM orders WHERE id = ? OR customer IN (` +
				`SELECT id FROM customers WHERE tier = ?) ORDER BY id`,
			args: []any{"o2", "pro"},
			want: "64:\"o1\"|\n64:\"o2\"|\n64:\"o3\"|",
		},
		{
			name: "outer after nested",
			sql: `SELECT id FROM orders WHERE customer IN (` +
				`SELECT id FROM customers WHERE tier = ?) AND id <> ? ORDER BY id`,
			args: []any{"pro", "o3"},
			want: "64:\"o1\"|",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			got := runStatement(t, stmt, FromDatabase(catalog, "orders"), tc.args...)
			got = strings.TrimSpace(strings.TrimPrefix(got, "|id\n"))
			if got != tc.want {
				t.Fatalf("rows = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSQLSubqueryWarmExecutionIsAllocationFree(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT id FROM orders WHERE customer IN (` +
			`SELECT id FROM customers WHERE tier = ?) ORDER BY id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	defer exec.Release()
	args := []any{"pro"}
	run := func() {
		cursor, err := stmt.RunInto(&exec, FromDatabase(catalog, "orders"), args)
		if err != nil {
			t.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(50, run); got != 0 {
		t.Fatalf("warmed subquery allocated %.1f times per run, want 0", got)
	}
}
