package query

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestGroupedScalarPredicateStages(t *testing.T) {
	source := FromSegment(mustSegment(t,
		`{"id":1,"g":"a","n":2}`, `{"id":2,"g":"a","n":3}`,
		`{"id":3,"g":"b","n":7}`, `{"id":4,"g":"b","n":null}`, `{"id":5,"g":"c"}`))
	for _, tc := range []struct {
		sql     string
		args    []any
		columns []string
		rows    []string
	}{
		{`SELECT COUNT(*) FROM docs WHERE COALESCE(n,0)>0`, nil, []string{"count(*)"}, []string{"3"}},
		{`SELECT COUNT(*), SUM(n), AVG(n), MIN(n), MAX(n) FROM docs WHERE COALESCE(n,0)>2`, nil, []string{"count(*)", "sum(n)", "avg(n)", "min(n)", "max(n)"}, []string{"2|10|5|3|7"}},
		{`SELECT COUNT(*), SUM(n) FROM docs WHERE COALESCE(n,0)>100`, nil, []string{"count(*)", "sum(n)"}, []string{"0|null"}},
		{`SELECT g, COUNT(*) AS total FROM docs WHERE n IS NOT DISTINCT FROM NULL GROUP BY g ORDER BY g`, nil, []string{"g", "total"}, []string{`"b"|1`, `"c"|1`}},
		{`SELECT DISTINCT g FROM docs WHERE COALESCE(n,0)>0 ORDER BY g`, nil, []string{"g"}, []string{`"a"`, `"b"`}},
		{`SELECT g, SUM(n) AS total FROM docs WHERE COALESCE(n,0)>0 GROUP BY g HAVING SUM(n)>? ORDER BY -SUM(n) LIMIT ? OFFSET ?`, []any{int64(4), int64(1), int64(1)}, []string{"g", "total"}, []string{`"a"|5`}},
		{`SELECT ? AS marker, SUM(n)+? AS total, SUM(n) AS raw FROM docs WHERE COALESCE(n,?)>? HAVING SUM(n)>? ORDER BY total LIMIT ?`, []any{"ok", int64(1), int64(0), int64(2), int64(9), int64(1)}, []string{"marker", "total", "raw"}, []string{`"ok"|1.1e1|10`}},
		{`WITH c AS (SELECT n FROM docs WHERE id>?) SELECT SUM(n)+? FROM c WHERE COALESCE(n,?)>?`, []any{int64(1), int64(1), int64(0), int64(2)}, []string{"?column?"}, []string{"1.1e1"}},
		{`SELECT SUM(d.n) FROM (SELECT n FROM docs WHERE id>?) AS d WHERE COALESCE(d.n,?)>?`, []any{int64(1), int64(0), int64(2)}, []string{"sum(n)"}, []string{"10"}},
		{`SELECT g, COUNT(*), ROW_NUMBER() OVER (ORDER BY g) AS rn FROM docs WHERE COALESCE(n,0)>0 GROUP BY g ORDER BY g`, nil, []string{"g", "count(*)", "rn"}, []string{`"a"|2|1`, `"b"|1|2`}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			tree, err := sqlast.Parse(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(tree)
			if err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				statement, err := PrepareParsedStatement(tc.sql, tree)
				if err != nil {
					t.Fatal(err)
				}
				defer statement.Release()
				if !reflect.DeepEqual(statement.Columns(), tc.columns) {
					t.Fatalf("columns=%v want %v", statement.Columns(), tc.columns)
				}
				var exec Exec
				defer exec.Release()
				cursor, err := statement.RunInto(&exec, source, tc.args)
				if err != nil {
					t.Fatal(err)
				}
				var rows []string
				for cursor.Next() {
					var cells []string
					for col := range tc.columns {
						cells = append(cells, string(cursor.Cell(col).JSON()))
					}
					rows = append(rows, strings.Join(cells, "|"))
				}
				if !reflect.DeepEqual(rows, tc.rows) {
					t.Fatalf("rows=%v want %v", rows, tc.rows)
				}
			}
			after, err := json.Marshal(tree)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("preparing mutated caller AST")
			}
		})
	}
}

func TestGroupedScalarPredicateBudgetsAndReuse(t *testing.T) {
	statement, err := PrepareStatement(`SELECT COUNT(*) FROM docs WHERE n / ? > 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromSegment(mustSegment(t, `{"n":1}`, `{"n":2}`, `{"n":3}`))
	var exec Exec
	defer exec.Release()
	exec.Options.Workers = 1
	exec.Options.ResultRows = 1
	args := []any{int64(1)}
	run := func() {
		var scratch [32]byte
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			t.Fatal(err)
		}
		if !cursor.Next() || string(cursor.Cell(0).AppendJSON(scratch[:0])) != "3" || cursor.Next() {
			t.Fatal("unexpected count")
		}
	}
	run()
	run()
	if allocations := testing.AllocsPerRun(100, run); allocations != 0 {
		t.Fatalf("warm allocations=%g", allocations)
	}
	exec.Options.IntermediateBytes = 1
	_, err = statement.RunInto(&exec, source, args)
	if !errors.Is(err, ErrIntermediateBudget) || exec.Result.RowCount != 0 {
		t.Fatalf("budget err=%v rows=%d", err, exec.Result.RowCount)
	}
	exec.Options.IntermediateBytes = 0
	run()
	args[0] = int64(0)
	_, err = statement.RunInto(&exec, source, args)
	if !errors.Is(err, ErrScalarDivisionByZero) || exec.Result.RowCount != 0 {
		t.Fatalf("division err=%v rows=%d", err, exec.Result.RowCount)
	}
	args[0] = int64(1)
	run()
	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	_, err = statement.RunInto(&exec, source, args)
	if !errors.Is(err, ErrCanceled) || exec.Result.RowCount != 0 {
		t.Fatalf("cancel err=%v rows=%d", err, exec.Result.RowCount)
	}
	exec.Options.Cancel = nil
	run()
}

func TestGroupedScalarPredicateJoinedSources(t *testing.T) {
	db := relationJoinDatabase(t)
	for _, tc := range []struct {
		sql  string
		rows []string
	}{
		{`SELECT a.zone, SUM(b.k) FROM a JOIN b ON a.k=b.k WHERE COALESCE(b.k,0)>0 GROUP BY a.zone`, []string{`"x",4`}},
		{`SELECT COUNT(*) FROM a LEFT JOIN b ON a.k=b.k WHERE b.k IS NOT DISTINCT FROM NULL`, []string{"2"}},
		{`SELECT COUNT(*) FROM a JOIN b ON a.k=b.k JOIN c ON b.k=c.k WHERE COALESCE(c.k,0)>0`, []string{"2"}},
		{`SELECT COUNT(*) FROM a JOIN b ON a.k=b.k JOIN c ON b.k=c.k WHERE COALESCE(c.k,0)>0 AND b.zone='x' AND a.zone='x'`, []string{"1"}},
		{`SELECT b.k FROM a JOIN b ON a.k=b.k JOIN c ON b.k=c.k WHERE COALESCE(c.k,0)>0 AND b.zone='x'`, []string{"2"}},
		{`SELECT COUNT(*) FROM a WHERE COALESCE(a.k,0)>0 AND EXISTS (SELECT b.k FROM b WHERE b.k=a.k)`, []string{"1"}},
		{`SELECT k, COUNT(*) FROM a FULL JOIN b USING (k) WHERE COALESCE(k,0)>0 GROUP BY k ORDER BY k`, []string{"1,1", "2,2", "3,1", "4,1"}},
		{`SELECT a.k, SUM("b.x".k) FROM a JOIN b AS "b.x" ON a.k="b.x".k JOIN c ON "b.x".k=c.k WHERE COALESCE("b.x".k,0)>0 GROUP BY a.k`, []string{"2,4"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			statement, exec, rows := runRelationJoinSQL(t, db, tc.sql)
			defer statement.Release()
			defer exec.Release()
			if !reflect.DeepEqual(rows, tc.rows) {
				t.Fatalf("rows=%v want %v", rows, tc.rows)
			}
		})
	}
}

func TestGroupedScalarPredicateCorrelationRefused(t *testing.T) {
	_, err := PrepareStatement(`SELECT a.k, q.total FROM a CROSS JOIN LATERAL (SELECT COUNT(*) AS total FROM b WHERE b.k=a.k AND COALESCE(b.k,0)>0) AS q`)
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) || !strings.Contains(err.Error(), "staged correlation frame") {
		t.Fatalf("correlated grouped predicate error=%T %v", err, err)
	}
}
