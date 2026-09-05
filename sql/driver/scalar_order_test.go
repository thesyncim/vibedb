package driver

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestScalarOrderEvaluatesHiddenKeysBeforeOffsetAndLimit(t *testing.T) {
	db := openTestDB(t)
	for _, sql := range []string{
		`CREATE TABLE ordered_values (id TEXT PRIMARY KEY, n NUMBER)`,
		`INSERT INTO ordered_values VALUES ('{"id":"a","n":null}'), ('{"id":"b","n":-2}'), ('{"id":"c","n":3}'), ('{"id":"d","n":1}'), ('{"id":"e"}')`,
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		order string
		args  []any
		want  []string
	}{
		{`COALESCE(n,9), id`, nil, []string{"b", "d", "c", "a", "e"}},
		{`-n NULLS LAST, id`, nil, []string{"c", "d", "b", "a", "e"}},
		{`GREATEST(n,0) DESC, id`, nil, []string{"c", "d", "a", "b", "e"}},
		{`CASE WHEN n<0 THEN 0 ELSE n END NULLS FIRST, id`, nil, []string{"a", "e", "b", "d", "c"}},
		{`COALESCE(n,?), id LIMIT ? OFFSET ?`, []any{9, 2, 1}, []string{"d", "c"}},
	} {
		t.Run(tc.order, func(t *testing.T) {
			rows, err := db.Query(`SELECT id FROM ordered_values ORDER BY `+tc.order, tc.args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			columns, err := rows.Columns()
			if err != nil || !reflect.DeepEqual(columns, []string{"id"}) {
				t.Fatalf("hidden sort columns leaked: %v %v", columns, err)
			}
			var ids []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("got=%v want=%v", ids, tc.want)
			}
		})
	}
	if _, err := db.Exec(`INSERT INTO ordered_values (id,n) VALUES ('large-a',9007199254740992),('large-b',9007199254740993)`); err != nil {
		t.Fatal(err)
	}
	var first string
	if err := db.QueryRow(`SELECT id FROM ordered_values ORDER BY COALESCE(n,0) DESC LIMIT 1`).Scan(&first); err != nil || first != "large-b" {
		t.Fatalf("exact numeric order: %s %v", first, err)
	}
}

func TestScalarOrderAcrossGroupingRelationsAndWindows(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE ordered_groups (id TEXT PRIMARY KEY, category TEXT, n NUMBER)`,
		`INSERT INTO ordered_groups (id,category,n) VALUES ('a','x',1),('b','x',2),('c','y',4)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		sql  string
		want []string
	}{
		{`SELECT category FROM ordered_groups GROUP BY category ORDER BY SUM(n) DESC`, []string{"y", "x"}},
		{`SELECT category, COUNT(*) FROM ordered_groups GROUP BY category ORDER BY COALESCE(SUM(n),0) DESC`, []string{"y|1", "x|2"}},
		{`SELECT category FROM ordered_groups GROUP BY category ORDER BY category || '!' DESC`, []string{"y", "x"}},
		{`SELECT COUNT(*) FROM ordered_groups ORDER BY COALESCE(SUM(n),0)`, []string{"3"}},
		{`SELECT 'one' FROM ordered_groups ORDER BY SUM(n)`, []string{"one"}},
		{`SELECT a.id FROM ordered_groups a JOIN ordered_groups b ON a.id=b.id ORDER BY -b.n`, []string{"c", "b", "a"}},
		{`SELECT a.id FROM ordered_groups a LEFT JOIN ordered_groups b ON a.id=b.id ORDER BY COALESCE(b.n,0) DESC`, []string{"c", "b", "a"}},
		{`SELECT d.id FROM (SELECT id,n FROM ordered_groups) d ORDER BY -d.n`, []string{"c", "b", "a"}},
		{`SELECT id, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM ordered_groups ORDER BY -n LIMIT 2`, []string{"c|3", "b|2"}},
		{`SELECT n+1, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM ordered_groups ORDER BY -n LIMIT 2`, []string{"5|3", "3|2"}},
		{`SELECT category, ROW_NUMBER() OVER (ORDER BY category) AS rn FROM ordered_groups GROUP BY category ORDER BY SUM(n) DESC`, []string{"y|2", "x|1"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for rows.Next() {
				values := make([]any, len(columns))
				pointers := make([]any, len(values))
				for i := range values {
					pointers[i] = &values[i]
				}
				if err := rows.Scan(pointers...); err != nil {
					t.Fatal(err)
				}
				parts := make([]string, len(values))
				for i, value := range values {
					if b, ok := value.([]byte); ok {
						parts[i] = string(b)
					} else {
						parts[i] = fmt.Sprint(value)
					}
				}
				got = append(got, strings.Join(parts, "|"))
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
	for _, statement := range []string{
		`SELECT id FROM ordered_groups ORDER BY SUM(n)`,
		`SELECT category FROM ordered_groups GROUP BY category ORDER BY n+1`,
		`SELECT COUNT(*) FROM ordered_groups ORDER BY n+1`,
		`SELECT d.id FROM (SELECT id,n FROM ordered_groups) d ORDER BY COALESCE(d.missing,0)`,
	} {
		rows, err := db.Query(statement)
		if err == nil {
			rows.Close()
			t.Errorf("accepted invalid query: %s", statement)
		}
	}
}
