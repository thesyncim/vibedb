package driver

import (
	"strings"
	"testing"
)

func TestSQLLikeAndILikeEndToEnd(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, name ANY)`); err != nil {
		t.Fatal(err)
	}
	for _, doc := range []string{
		`{"id":"1","name":"alpha"}`,
		`{"id":"2","name":"ALPHA"}`,
		`{"id":"3","name":"beta"}`,
		`{"id":"4","name":7}`,
	} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, doc); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := db.Query(`SELECT id FROM docs WHERE name LIKE ? ORDER BY id`, `a%`)
	if err != nil {
		t.Fatal(err)
	}
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
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if want := "1"; strings.Join(got, ",") != want {
		t.Fatalf("LIKE rows = %q, want %q", strings.Join(got, ","), want)
	}

	rows, err = db.Query(`SELECT id FROM docs WHERE name ILIKE ? ORDER BY id`, `a%`)
	if err != nil {
		t.Fatal(err)
	}
	got = got[:0]
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
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if want := "1,2"; strings.Join(got, ",") != want {
		t.Fatalf("ILIKE rows = %q, want %q", strings.Join(got, ","), want)
	}
}
