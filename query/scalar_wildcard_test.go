package query

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

func TestScalarRelationWildcardOrdinalsAndSortAliases(t *testing.T) {
	db := &store.Database{}
	collection, err := db.CreateCollection("docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, document := range []string{`{"id":"a","n":3}`, `{"id":"b","n":null}`, `{"id":"c"}`} {
		if _, err := collection.Put([]string{"a", "b", "c"}[i], []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		query   string
		columns []string
		rows    []string
	}{
		{`SELECT d.*, COALESCE(d.n,0) AS score FROM (SELECT id,n FROM docs) d ORDER BY score DESC`, []string{"id", "n", "score"}, []string{`"a",3,3`, `"b",null,0`, `"c",null,0`}},
		{`SELECT COALESCE(d.n,0) AS score,d.* FROM (SELECT id,n FROM docs) d ORDER BY score DESC LIMIT 1 OFFSET 1`, []string{"score", "id", "n"}, []string{`0,"b",null`}},
		{`SELECT d.*,d.*,COALESCE(d.n,0) FROM (SELECT id,n FROM docs) d WHERE COALESCE(d.n,0)>0`, []string{"id", "n", "id", "n", "?column?"}, []string{`"a",3,"a",3,3`}},
		{`SELECT d.*,9 AS marker FROM (SELECT 1 AS x,2 AS x) d`, []string{"x", "x", "marker"}, []string{`1,2,9`}},
		{`SELECT d.*,9 AS marker FROM (SELECT 1 AS "x.y",2 AS "x/y") d`, []string{"x.y", "x/y", "marker"}, []string{`1,2,9`}},
		{`WITH c(owner,n) AS (SELECT id,n FROM docs) SELECT c.*,COALESCE(c.n,0) FROM c WHERE COALESCE(c.n,0)>0`, []string{"owner", "n", "?column?"}, []string{`"a",3,3`}},
		{`SELECT a.*,b.*,COALESCE(a.n,b.n) AS score FROM (SELECT id,n FROM docs) a JOIN (SELECT id,n FROM docs) b ON a.id=b.id WHERE a.id='a' ORDER BY score`, []string{"id", "n", "id", "n", "score"}, []string{`"a",3,"a",3,3`}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			tree, err := sqlast.Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			columns := append([]sqlast.ResultColumn(nil), tree.Columns...)
			// Preparing twice from one tree exercises immutable ordinal binding.
			for attempt := 0; attempt < 2; attempt++ {
				statement, err := PrepareParsedStatement(tc.query, tree)
				if err != nil {
					t.Fatal(err)
				}
				exec := new(Exec)
				cursor, err := statement.RunInto(exec, FromDatabase(db.Snapshot(), statement.Collection()), nil)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(statement.Columns(), tc.columns) {
					t.Fatalf("columns=%v want=%v", statement.Columns(), tc.columns)
				}
				var rows []string
				for cursor.Next() {
					var cells []string
					for column := range tc.columns {
						cells = append(cells, string(cursor.Cell(column).JSON()))
					}
					rows = append(rows, strings.Join(cells, ","))
				}
				if !reflect.DeepEqual(rows, tc.rows) {
					t.Fatalf("rows=%v want=%v", rows, tc.rows)
				}
				exec.Release()
				statement.Release()
			}
			if !reflect.DeepEqual(tree.Columns, columns) {
				t.Fatal("prepare changed the caller's SELECT list")
			}
		})
	}
}

func TestScalarRelationWildcardTypedMetadata(t *testing.T) {
	statement, err := PrepareStatement(`SELECT d.*,COALESCE(d.label,'fallback') AS chosen FROM (SELECT CAST('alpha' AS TEXT) AS label,CAST(TRUE AS BOOL) AS enabled) d`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	schema := statement.AppendSchema(nil)
	if len(schema) != 3 || schema[0].Representation != OutputSQLText || schema[1].Representation != OutputSQLBool {
		t.Fatalf("typed wildcard metadata=%+v", schema)
	}
}

func TestScalarRelationWildcardBudgetsCancellationAndWarmReuse(t *testing.T) {
	statement, err := PrepareStatement(`SELECT d.*,COALESCE(d.n,0) AS score FROM (SELECT id,n FROM docs) d WHERE COALESCE(d.n,0)>0 ORDER BY score DESC LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromSegment(mustSegment(t, `{"id":"a","n":3}`, `{"id":"b","n":7}`, `{"id":"c"}`))
	exec := new(Exec)
	defer exec.Release()
	run := func() {
		cursor, err := statement.RunInto(exec, source, nil)
		if err != nil {
			panic(err)
		}
		if !cursor.Next() {
			panic("missing wildcard result")
		}
		value, ok := cursor.Cell(2).Int64()
		if !ok || value != 7 || cursor.Next() {
			panic("wrong wildcard result")
		}
	}
	run()
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed wildcard allocations=%v", allocs)
	}
	exec.Options.IntermediateBytes = 1
	_, err = statement.RunInto(exec, source, nil)
	var budget *IntermediateBudgetError
	if !errors.As(err, &budget) || exec.Result.RowCount != 0 {
		t.Fatalf("budget error=%v rows=%d", err, exec.Result.RowCount)
	}
	exec.Options.IntermediateBytes = 0
	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	_, err = statement.RunInto(exec, source, nil)
	if !errors.Is(err, ErrCanceled) || exec.Result.RowCount != 0 {
		t.Fatalf("cancel error=%v rows=%d", err, exec.Result.RowCount)
	}
	cancel.Reset()
	run()
}
