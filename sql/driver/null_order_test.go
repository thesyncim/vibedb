package driver

import (
	"strings"
	"testing"
)

func TestNullOrderingPathsScalarWindowsSetsAndMutation(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE null_order (id TEXT PRIMARY KEY, n INT)`,
		`INSERT INTO null_order (id, n) VALUES ('a', NULL), ('b', 2), ('c', 1), ('d', NULL)`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ source, want string }{
		{`SELECT id FROM null_order ORDER BY n ASC NULLS LAST, id`, "cbad"},
		{`SELECT id FROM null_order ORDER BY n DESC NULLS FIRST, id`, "adbc"},
		{`SELECT id FROM null_order ORDER BY n ASC NULLS FIRST, id`, "adcb"},
		{`SELECT id FROM null_order ORDER BY n DESC NULLS LAST, id`, "bcad"},
		{`SELECT id, NULLIF(n, 2) AS k FROM null_order ORDER BY k NULLS LAST, id LIMIT 2 OFFSET 1`, "ab"},
		{`SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM null_order ORDER BY n NULLS LAST, id`, "cbad"},
		{`SELECT id, n FROM null_order WHERE id <= 'b' UNION ALL SELECT id, n FROM null_order WHERE id > 'b' ORDER BY 2 NULLS LAST, 1`, "cbad"},
		{`SELECT id FROM null_order GROUP BY id, n ORDER BY n DESC NULLS FIRST, id`, "adbc"},
	} {
		rows, err := db.Query(tc.source)
		if err != nil {
			t.Fatalf("%s: %v", tc.source, err)
		}
		columns, _ := rows.Columns()
		var ids strings.Builder
		for rows.Next() {
			var id string
			var other any
			args := []any{&id}
			if len(columns) == 2 {
				args = append(args, &other)
			}
			if err := rows.Scan(args...); err != nil {
				t.Fatal(err)
			}
			ids.WriteString(id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if ids.String() != tc.want {
			t.Fatalf("%s = %q, want %q", tc.source, ids.String(), tc.want)
		}
	}
	var id string
	if err := db.QueryRow(`DELETE FROM null_order ORDER BY id DESC NULLS FIRST LIMIT 1 RETURNING id`).Scan(&id); err != nil || id != "d" {
		t.Fatalf("ordered delete = %q: %v", id, err)
	}
}
