package query

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestAggregateHavingHiddenDependencies(t *testing.T) {
	source := FromSegment(mustSegment(t,
		`{"g":"a","n":2}`, `{"g":"a","n":3}`, `{"g":"b","n":7}`,
		`{"g":"b","n":null}`, `{"g":"c"}`))
	for _, tc := range []struct {
		sql     string
		args    []any
		columns []string
		rows    []string
	}{
		{`SELECT g FROM docs GROUP BY g HAVING SUM(n)>5`, nil, []string{"g"}, []string{`"b"`}},
		{`SELECT g FROM docs GROUP BY g HAVING COUNT(*) IN (2,3) ORDER BY g`, nil, []string{"g"}, []string{`"a"`, `"b"`}},
		{`SELECT g FROM docs GROUP BY g HAVING SUM(n) IS NULL`, nil, []string{"g"}, []string{`"c"`}},
		{`SELECT COUNT(*) FROM docs HAVING SUM(n)>10`, nil, []string{"count(*)"}, []string{"5"}},
		{`SELECT COALESCE(SUM(n),0) AS total FROM docs HAVING COUNT(*)>?`, []any{int64(1)}, []string{"total"}, []string{"12"}},
		{`SELECT 1 AS present FROM docs HAVING COUNT(*)>0`, nil, []string{"present"}, []string{"1"}},
		{`SELECT 1 AS empty FROM docs WHERE n>100 HAVING COUNT(*)=0`, nil, []string{"empty"}, []string{"1"}},
		{`SELECT 1 AS empty FROM docs WHERE COALESCE(n,0)>100 HAVING COUNT(*)=0`, nil, []string{"empty"}, []string{"1"}},
		{`SELECT 1 AS present FROM docs WHERE n>100 HAVING COUNT(*)>0`, nil, []string{"present"}, nil},
		{`SELECT CASE WHEN g='b' THEN 1/0 ELSE SUM(n) END AS total FROM docs GROUP BY g HAVING g<>'b' AND COUNT(*)>1`, nil, []string{"total"}, []string{"5"}},
		{`SELECT SUM(n)+1 AS total, MAX(n) AS biggest FROM docs GROUP BY g HAVING COUNT(*)>1 ORDER BY -SUM(n) LIMIT 1 OFFSET 1`, nil, []string{"total", "biggest"}, []string{"6|3"}},
		{`SELECT g, ROW_NUMBER() OVER (ORDER BY g) AS rn FROM docs GROUP BY g HAVING COUNT(*)>1 ORDER BY g`, nil, []string{"g", "rn"}, []string{`"a"|1`, `"b"|2`}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			statement, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if !reflect.DeepEqual(statement.Columns(), tc.columns) {
				t.Fatalf("columns=%v want=%v", statement.Columns(), tc.columns)
			}
			var exec Exec
			defer exec.Release()
			for attempt := 0; attempt < 2; attempt++ {
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
					t.Fatalf("rows=%v want=%v", rows, tc.rows)
				}
			}
		})
	}
}

func TestAggregateHavingWarmBudgetsAndReuse(t *testing.T) {
	statement, err := PrepareStatement(`SELECT SUM(n)+1 AS total FROM docs GROUP BY g HAVING COUNT(*)>? LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromSegment(mustSegment(t, `{"g":"a","n":2}`, `{"g":"a","n":3}`))
	var exec Exec
	defer exec.Release()
	exec.Options.Workers = 1
	args := []any{int64(1)}
	run := func() {
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			t.Fatal(err)
		}
		var scratch [32]byte
		if !cursor.Next() || string(cursor.Cell(0).AppendJSON(scratch[:0])) != "6" || cursor.Next() {
			t.Fatal("unexpected HAVING result")
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
