package driver

import (
	"reflect"
	"testing"
)

func TestScalarRelationWildcardPreparedResults(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE samples (id TEXT PRIMARY KEY, n INT)`,
		`INSERT INTO samples (id,n) VALUES ('a',2),('b',3),('c',NULL)`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	statement, err := db.Prepare(`SELECT d.*,COALESCE(d.n,0) AS score FROM (SELECT id,n FROM samples) d WHERE COALESCE(d.n,0)>? ORDER BY score DESC LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	for _, threshold := range []int{0, 2, 3} {
		rows, err := statement.Query(threshold)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil || !reflect.DeepEqual(columns, []string{"id", "n", "score"}) {
			t.Fatalf("columns=%v err=%v", columns, err)
		}
		count := 0
		for rows.Next() {
			var id string
			var n, score int
			if err := rows.Scan(&id, &n, &score); err != nil || id != "b" || n != 3 || score != 3 {
				t.Fatalf("row=%s,%d,%d err=%v", id, n, score, err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		want := 1
		if threshold == 3 {
			want = 0
		}
		if count != want {
			t.Fatalf("rows=%d want=%d", count, want)
		}
	}
}
