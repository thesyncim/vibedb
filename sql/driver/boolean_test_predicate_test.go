package driver

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestBooleanTestsVisibilityTruthTable(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE memberships (id TEXT PRIMARY KEY)`,
		`INSERT INTO memberships VALUES ('{"id":"a","hidden":true}'), ('{"id":"b","hidden":false}'), ('{"id":"c","hidden":null}'), ('{"id":"d"}')`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ predicate, ids string }{
		{"hidden", "a"}, {"NOT hidden", "b"}, {"COALESCE(hidden,FALSE)", "a"},
		{"hidden IS NOT TRUE OR id='a'", "abcd"},
		{"NOT (hidden IS TRUE OR id='b')", "cd"},
		{"hidden IS TRUE OR id IS NULL", "a"},
		{"hidden IS TRUE", "a"}, {"hidden IS FALSE", "b"},
		{"hidden IS NOT TRUE", "bcd"}, {"hidden IS NOT FALSE", "acd"},
		{"NOT (hidden IS TRUE)", "bcd"}, {"NOT (hidden IS NOT FALSE)", "b"},
		{"hidden IS TRUE OR hidden IS FALSE", "ab"},
		{"COALESCE(hidden, FALSE) IS FALSE", "bcd"},
		{"CASE WHEN hidden IS NOT TRUE THEN TRUE ELSE FALSE END IS TRUE", "bcd"},
	} {
		rows, err := db.Query("SELECT id FROM memberships WHERE " + tc.predicate + " ORDER BY id")
		if err != nil {
			t.Fatalf("%s: %v", tc.predicate, err)
		}
		var ids strings.Builder
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids.WriteString(id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if ids.String() != tc.ids {
			t.Fatalf("%s = %q, want %q", tc.predicate, ids.String(), tc.ids)
		}
	}
	if _, err := db.Exec(`INSERT INTO memberships VALUES ('{"id":"e","hidden":"false"}')`); err != nil {
		t.Fatal(err)
	}
	for _, predicate := range []string{"hidden", "NOT hidden", "hidden IS NOT TRUE", "hidden IS NOT TRUE OR id='z'", `"$doc"->>'hidden'`} {
		rows, err := db.Query(`SELECT id FROM memberships WHERE ` + predicate)
		if rows != nil {
			rows.Close()
		}
		if !errors.Is(err, query.ErrScalarType) {
			t.Fatalf("nonboolean truth test %s = %v", predicate, err)
		}
	}
}
