package driver

import (
	"reflect"
	"testing"
)

func TestAggregateHavingPreparedMetadata(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE samples (id TEXT PRIMARY KEY, category TEXT, n INT)`,
		`INSERT INTO samples (id,category,n) VALUES ('a','first',2),('b','first',3),('c','second',7)`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	statement, err := db.Prepare(`SELECT category FROM samples GROUP BY category HAVING COUNT(*)>? ORDER BY category`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	for _, tc := range []struct {
		minimum    int
		categories []string
	}{
		{0, []string{"first", "second"}}, {1, []string{"first"}}, {2, nil},
	} {
		rows, err := statement.Query(tc.minimum)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil || !reflect.DeepEqual(columns, []string{"category"}) {
			t.Fatalf("columns=%v err=%v", columns, err)
		}
		var got []string
		for rows.Next() {
			var category string
			if err := rows.Scan(&category); err != nil {
				t.Fatal(err)
			}
			got = append(got, category)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if !reflect.DeepEqual(got, tc.categories) {
			t.Fatalf("categories=%v want=%v", got, tc.categories)
		}
	}
	var total int
	if err := db.QueryRow(`SELECT COALESCE(SUM(n),0) FROM samples HAVING COUNT(*)>1`).Scan(&total); err != nil || total != 12 {
		t.Fatalf("total=%d err=%v", total, err)
	}
	if err := db.QueryRow(`SELECT 1 FROM samples WHERE COALESCE(n,0)>100 HAVING COUNT(*)=0`).Scan(&total); err != nil || total != 1 {
		t.Fatalf("empty aggregate=%d err=%v", total, err)
	}
}
