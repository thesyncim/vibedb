package driver

import (
	"reflect"
	"testing"
)

func TestNullSafeComparisonTruthAndPreparedParameters(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE comparisons (id TEXT PRIMARY KEY, n INTEGER)`,
		`INSERT INTO comparisons (id,n) VALUES ('a',NULL),('b',0),('c',1),('d',9007199254740993)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		predicate string
		args      []any
		want      []string
	}{
		{`n IS DISTINCT FROM 0`, nil, []string{"a", "c", "d"}},
		{`n IS NOT DISTINCT FROM 0`, nil, []string{"b"}},
		{`n IS NOT DISTINCT FROM NULL`, nil, []string{"a"}},
		{`NULL IS DISTINCT FROM n`, nil, []string{"b", "c", "d"}},
		{`NULL IS NOT DISTINCT FROM NULL`, nil, []string{"a", "b", "c", "d"}},
		{`TRUE IS DISTINCT FROM FALSE`, nil, []string{"a", "b", "c", "d"}},
		{`FALSE IS NOT DISTINCT FROM NULL`, nil, nil},
		{`BOOL 't' IS DISTINCT FROM 'off'`, nil, []string{"a", "b", "c", "d"}},
		{`n IS NOT DISTINCT FROM ?`, []any{nil}, []string{"a"}},
		{`n IS NOT DISTINCT FROM ?`, []any{int64(9007199254740993)}, []string{"d"}},
		{`n IS DISTINCT FROM 9007199254740993.0`, nil, []string{"a", "b", "c"}},
		{`COALESCE(n,0) IS NOT DISTINCT FROM 0`, nil, []string{"a", "b"}},
		{`NOT (n IS DISTINCT FROM 0) OR n IS NOT DISTINCT FROM NULL`, nil, []string{"a", "b"}},
	} {
		t.Run(tc.predicate, func(t *testing.T) {
			statement, err := db.Prepare(`SELECT id FROM comparisons WHERE ` + tc.predicate + ` ORDER BY id`)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Close()
			rows, err := statement.Query(tc.args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var got []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				got = append(got, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
	var value string
	if err := db.QueryRow(`SELECT CASE WHEN 9007199254740993 IS NOT DISTINCT FROM 9007199254740993.0 THEN 'equal' ELSE 'different' END`).Scan(&value); err != nil || value != "equal" {
		t.Fatalf("CASE value=%s err=%v", value, err)
	}
	if _, err := db.Exec(`UPDATE comparisons SET n=CASE WHEN n IS NOT DISTINCT FROM NULL THEN 2 ELSE n END WHERE id='a'`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT n FROM comparisons WHERE id='a'`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("mutation n=%d err=%v", n, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM comparisons HAVING COUNT(*) IS DISTINCT FROM 0`).Scan(&n); err != nil || n != 4 {
		t.Fatalf("HAVING count=%d err=%v", n, err)
	}
	for _, statement := range []string{
		`SELECT id FROM comparisons WHERE n IS DISTINCT 0`,
		`SELECT id FROM comparisons WHERE n IS NOT DISTINCT FROM`,
		`SELECT id FROM comparisons WHERE n IS DISTINCT FROM '{}'::JSON`,
		`SELECT id FROM comparisons WHERE id IS NOT DISTINCT FROM n LIMIT 0`,
	} {
		rows, err := db.Query(statement)
		if rows != nil {
			rows.Close()
		}
		if err == nil {
			t.Fatalf("invalid comparison accepted: %s", statement)
		}
	}
}
