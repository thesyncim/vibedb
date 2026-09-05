package driver

import (
	stdsql "database/sql"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGroupedScalarPredicateDurablePrepared(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, source := range []string{
		`CREATE TABLE measures (id TEXT PRIMARY KEY, category TEXT, n INT)`,
		`INSERT INTO measures (id,category,n) VALUES ('a','first',2),('b','first',3),('c','second',7),('d','second',NULL)`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		statement, err := db.Prepare(`WITH selected AS (SELECT category,n FROM measures WHERE id<>?) SELECT category, SUM(n) AS total FROM selected WHERE COALESCE(n,?)>? GROUP BY category HAVING SUM(n)>? ORDER BY -SUM(n) LIMIT ?`)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			threshold int
			want      []string
		}{{0, []string{"second", "first"}}, {5, []string{"second"}}, {9, nil}} {
			rows, err := statement.Query("unused", 0, tc.threshold, 0, 2)
			if err != nil {
				t.Fatal(err)
			}
			columns, err := rows.Columns()
			if err != nil || !reflect.DeepEqual(columns, []string{"category", "total"}) {
				t.Fatalf("columns=%v err=%v", columns, err)
			}
			var got []string
			for rows.Next() {
				var category string
				var total int
				if err := rows.Scan(&category, &total); err != nil {
					t.Fatal(err)
				}
				want := 5
				if category == "second" {
					want = 7
				}
				if total != want {
					t.Fatalf("category=%s total=%d want=%d", category, total, want)
				}
				got = append(got, category)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("categories=%v want=%v", got, tc.want)
			}
		}
		statement.Close()
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM measures WHERE n IS NOT DISTINCT FROM ?`, nil).Scan(&count); err != nil || count != 1 {
			t.Fatalf("null count=%d err=%v", count, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = stdsql.Open("vibedb", path)
		if err != nil {
			t.Fatal(err)
		}
	}
}
