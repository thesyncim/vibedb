package query

import (
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestSQLFromlessScalarDirectPreparedAndReuse(t *testing.T) {
	statement, err := PrepareStatement(
		`SELECT 1 + ? AS n, TRUE AS ok, NULL AS absent, ` +
			`CASE WHEN ? = 'x' THEN CAST('2.50' AS NUMERIC) ELSE -3 END AS chosen`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.Collection() != "" || statement.RequiresCatalog() {
		t.Fatalf("FROM-less source classification = %q/%v",
			statement.Collection(), statement.RequiresCatalog())
	}
	if got, want := statement.Columns(), []string{"n", "ok", "absent", "chosen"}; !slices.Equal(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}

	var execution Exec
	assert := func(args []any, wantN, wantChosen string) {
		t.Helper()
		cursor, runErr := statement.RunInto(&execution, Source{}, args)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if !cursor.Next() {
			t.Fatal("missing FROM-less scalar row")
		}
		if got := cursor.Cell(0).String(); got != wantN {
			t.Fatalf("n = %s, want %s", got, wantN)
		}
		if got, ok := cursor.Cell(1).Bool(); !ok || !got {
			t.Fatalf("ok = %v/%v, want true", got, ok)
		}
		if !cursor.Cell(2).IsNull() {
			t.Fatalf("absent = %s, want NULL", cursor.Cell(2).String())
		}
		if got := cursor.Cell(3).String(); got != wantChosen {
			t.Fatalf("chosen = %s, want %s", got, wantChosen)
		}
		if cursor.Next() {
			t.Fatal("FROM-less scalar SELECT returned more than one row")
		}
	}
	assert([]any{int64(2), "x"}, "3", "2.5")
	assert([]any{int64(4), "other"}, "5", "-3")
}

func TestSQLFromlessScalarComposesWithCTEAndSet(t *testing.T) {
	for _, test := range []struct {
		source string
		want   []string
	}{
		{
			source: `WITH seed AS (SELECT 1 AS n) SELECT n FROM seed`,
			want:   []string{"1"},
		},
		{
			source: `SELECT 1 AS n UNION ALL VALUES (2)`,
			want:   []string{"1", "2"},
		},
	} {
		statement, err := PrepareStatement(test.source)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", test.source, err)
		}
		var execution Exec
		cursor, err := statement.RunInto(&execution, Source{}, nil)
		if err != nil {
			statement.Release()
			t.Fatalf("RunInto(%q): %v", test.source, err)
		}
		var got []string
		for cursor.Next() {
			got = append(got, cursor.Cell(0).String())
		}
		statement.Release()
		if !slices.Equal(got, test.want) {
			t.Fatalf("RunInto(%q) = %v, want %v", test.source, got, test.want)
		}
	}
}

func TestSQLFromlessUnusedPhysicalCTEsRemainSourceIndependent(t *testing.T) {
	statement, err := PrepareStatement(`WITH
		unused_a AS (SELECT id FROM never_created_a),
		unused_b AS (SELECT id FROM never_created_b)
		SELECT 1 AS value`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.Collection() != "" || statement.RequiresCatalog() {
		t.Fatalf(
			"unused CTE classification = %q/%v, want empty/false",
			statement.Collection(), statement.RequiresCatalog(),
		)
	}
}

func TestSQLFromlessReachablePhysicalSubqueryStillRequiresCatalog(t *testing.T) {
	statement, err := PrepareStatement(
		`SELECT 1 AS value WHERE EXISTS (SELECT id FROM docs)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.Collection() != "" || !statement.RequiresCatalog() {
		t.Fatalf(
			"predicate subquery classification = %q/%v, want empty/true",
			statement.Collection(), statement.RequiresCatalog(),
		)
	}
}

func TestSQLFromlessNestedPhysicalSourcesPreserveCatalog(t *testing.T) {
	var database store.Database
	docs, err := database.CreateCollection("docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := docs.Put("present", []byte(`{"id":"present"}`)); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name   string
		source string
		want   []string
	}{
		{
			name: "nested exists",
			source: `SELECT 1 AS value WHERE EXISTS (` +
				`SELECT 1 WHERE EXISTS (` +
				`SELECT id FROM docs WHERE id = 'present'))`,
			want: []string{"1"},
		},
		{
			name: "reachable CTE body",
			source: `WITH probe AS (` +
				`SELECT 1 AS value WHERE EXISTS (` +
				`SELECT id FROM docs WHERE id = 'present')) ` +
				`SELECT value FROM probe`,
			want: []string{"1"},
		},
		{
			name: "reachable derived body",
			source: `SELECT value FROM (` +
				`SELECT 1 AS value WHERE EXISTS (` +
				`SELECT id FROM docs WHERE id = 'present')) AS probe`,
			want: []string{"1"},
		},
		{
			name: "FROM-less set leaf",
			source: `SELECT 1 AS value WHERE EXISTS (` +
				`SELECT id FROM docs WHERE id = 'present') ` +
				`UNION ALL VALUES (2)`,
			want: []string{"1", "2"},
		},
		{
			name: "same collection through FROM-less intermediate",
			source: `SELECT id FROM docs WHERE EXISTS (` +
				`SELECT 1 WHERE EXISTS (` +
				`SELECT id FROM docs WHERE id = 'present'))`,
			want: []string{"present"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if !statement.RequiresCatalog() {
				t.Fatal("nested physical source did not require a catalog")
			}
			var execution Exec
			cursor, err := statement.RunInto(
				&execution,
				FromDatabase(database.Snapshot(), statement.Collection()),
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for cursor.Next() {
				cell := cursor.Cell(0)
				if text, ok := cell.Text(); ok {
					got = append(got, text)
				} else {
					got = append(got, cell.String())
				}
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("rows = %v, want %v", got, test.want)
			}
		})
	}
}
