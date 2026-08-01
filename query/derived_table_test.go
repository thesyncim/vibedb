package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestSQLDerivedTableExecutesAsRelation(t *testing.T) {
	catalog := subqueryDatabase(t)
	tests := []struct {
		name string
		sql  string
		args []any
		want string
	}{
		{
			name: "filter and order",
			sql: `SELECT d.id FROM (` +
				`SELECT id, tier FROM customers WHERE tier = 'pro'` +
				`) AS d WHERE d.id <> 'c3' ORDER BY d.id`,
			want: "4:\"c1\"|",
		},
		{
			name: "wildcard expands ordered schema",
			sql: `SELECT d.* FROM (` +
				`SELECT id, tier FROM customers WHERE id = 'c1'` +
				`) d`,
			want: "4:\"c1\"|4:\"pro\"|",
		},
		{
			name: "group and aggregate",
			sql: `SELECT d.tier, COUNT(*) AS n FROM (` +
				`SELECT tier FROM customers` +
				`) d GROUP BY d.tier ORDER BY d.tier`,
			want: "4:\"free\"|3:1|\n4:\"pro\"|3:2|",
		},
		{
			name: "inner order offset limit finish first",
			sql: `SELECT d.id FROM (` +
				`SELECT id FROM customers ORDER BY id LIMIT 2 OFFSET 1` +
				`) d ORDER BY d.id DESC`,
			want: "4:\"c3\"|\n4:\"c2\"|",
		},
		{
			name: "placeholder scopes",
			sql: `SELECT d.id FROM (` +
				`SELECT id, tier FROM customers WHERE tier = ?` +
				`) d WHERE d.id <> ? ORDER BY d.id`,
			args: []any{"pro", "c3"},
			want: "4:\"c1\"|",
		},
		{
			name: "nested derived",
			sql: `SELECT outer_d.id FROM (` +
				`SELECT inner_d.id FROM (` +
				`SELECT id FROM customers WHERE tier = 'pro'` +
				`) inner_d WHERE inner_d.id <> 'c1'` +
				`) outer_d`,
			want: "4:\"c3\"|",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := PrepareStatement(test.sql)
			if err != nil {
				t.Fatalf("PrepareStatement: %v", err)
			}
			defer stmt.Release()
			got := runStatement(
				t, stmt, FromDatabase(catalog, stmt.Collection()), test.args...,
			)
			lines := strings.SplitN(got, "\n", 2)
			if len(lines) != 2 {
				t.Fatalf("result = %q, want header and rows", got)
			}
			if rows := strings.TrimSpace(lines[1]); rows != test.want {
				t.Fatalf("rows = %q, want %q", rows, test.want)
			}
		})
	}
}

func TestSQLDerivedTableColumnResolution(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		is   error
	}{
		{
			name: "undefined",
			sql:  `SELECT d.missing FROM (SELECT id FROM customers) d`,
			is:   ErrUndefinedColumn,
		},
		{
			name: "ambiguous duplicate output",
			sql:  `SELECT d.id FROM (SELECT id, id FROM customers) d`,
			is:   ErrAmbiguousColumn,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := PrepareStatement(test.sql)
			if !errors.Is(err, test.is) {
				t.Fatalf("PrepareStatement error = %#v, want errors.Is(%v)", err, test.is)
			}
			var columnErr *RelationColumnError
			if !errors.As(err, &columnErr) {
				t.Fatalf("PrepareStatement error = %T, want *RelationColumnError", err)
			}
		})
	}
}

func TestSQLDerivedTableDuplicateColumnsSurviveWildcard(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT d.* FROM (SELECT id, id FROM customers WHERE id = 'c1') d`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	if got := stmt.Columns(); len(got) != 2 || got[0] != "id" || got[1] != "id" {
		t.Fatalf("Columns = %v, want duplicate id ordinals", got)
	}
	var exec Exec
	cursor, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || cursor.Cell(0).String() != `"c1"` ||
		cursor.Cell(1).String() != `"c1"` || cursor.Next() {
		t.Fatalf("duplicate wildcard row was not preserved")
	}
}

func TestSQLDerivedTableSharesIntermediateBudgetAcrossDepth(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT outer_d.id FROM (` +
			`SELECT inner_d.id FROM (` +
			`SELECT id FROM customers` +
			`) inner_d` +
			`) outer_d`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	exec.Options.IntermediateBytes = 1
	_, err = stmt.RunInto(&exec, FromDatabase(catalog, stmt.Collection()), nil)
	var budgetErr *IntermediateBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("RunInto error = %#v, want intermediate budget", err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed derived execution retained %d root rows", exec.Result.RowCount)
	}
}

func TestSQLDerivedTablePreservesExactJSONCells(t *testing.T) {
	var database store.Database
	docs, err := database.CreateCollection("docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := docs.Put("a", []byte(
		`{"n":12345678901234567890.0100,"v":{"x":[1,2]},"z":null}`,
	)); err != nil {
		t.Fatal(err)
	}
	stmt, err := PrepareStatement(
		`SELECT d.n, d.v.x[1], d.z FROM (` +
			`SELECT n, v, z FROM docs` +
			`) d`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	cursor, err := stmt.RunInto(
		&exec, FromDatabase(database.Snapshot(), stmt.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("derived exact-value query returned no row")
	}
	got := []string{
		cursor.Cell(0).String(),
		cursor.Cell(1).String(),
		cursor.Cell(2).String(),
	}
	want := []string{"12345678901234567890.0100", "2", "null"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cell %d = %q, want exact %q", i, got[i], want[i])
		}
	}
}

func TestSQLDerivedTableWarmExecutionIsAllocationFree(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT d.id FROM (` +
			`SELECT id, tier FROM customers WHERE tier = ?` +
			`) d WHERE d.id <> ? ORDER BY d.id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	defer exec.Release()
	args := []any{"pro", "missing"}
	run := func() {
		cursor, err := stmt.RunInto(
			&exec, FromDatabase(catalog, stmt.Collection()), args,
		)
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
		t.Fatalf("warmed derived execution allocated %.1f times per run, want 0", got)
	}
}
