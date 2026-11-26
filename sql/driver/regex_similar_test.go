package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestRegexOperatorsAndSimilarTo(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE words (id INT PRIMARY KEY, w TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO words (id, w) VALUES (1, 'Alpha')`,
		`INSERT INTO words (id, w) VALUES (2, 'alpha123')`,
		`INSERT INTO words (id, w) VALUES (3, 'Beta')`,
		`INSERT INTO words (id, w) VALUES (4, 'gamma_9')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	cases := []struct {
		name string
		sql  string
		want []int
	}{
		{"~ case-sensitive", `SELECT id FROM words WHERE w ~ '^alpha' ORDER BY id`, []int{2}},
		{"~* case-insensitive", `SELECT id FROM words WHERE w ~* '^alpha' ORDER BY id`, []int{1, 2}},
		{"!~ negated", `SELECT id FROM words WHERE w !~ '[0-9]' ORDER BY id`, []int{1, 3}},
		{"!~* negated ci", `SELECT id FROM words WHERE w !~* 'a' ORDER BY id`, nil},
		{"scalar LHS ~", `SELECT id FROM words WHERE LOWER(w) ~ '^beta$' ORDER BY id`, []int{3}},
		{"SIMILAR TO", `SELECT id FROM words WHERE w SIMILAR TO '(Alpha|Beta)%' ORDER BY id`, []int{1, 3}},
		{"NOT SIMILAR TO", `SELECT id FROM words WHERE w NOT SIMILAR TO '%[0-9]+' ORDER BY id`, []int{1, 3}},
		{"SIMILAR TO ESCAPE", `SELECT id FROM words WHERE w SIMILAR TO 'gamma!_%' ESCAPE '!' ORDER BY id`, []int{4}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.sql)
			if err != nil {
				t.Fatalf("query %s: %v", tc.sql, err)
			}
			defer rows.Close()
			var got []int
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					t.Fatalf("scan: %v", err)
				}
				got = append(got, id)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}
