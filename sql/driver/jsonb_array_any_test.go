package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestJSONBKeyExistsSetAndArrayAnyAll(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE docs (id TEXT PRIMARY KEY, custom JSONB, tags JSONB)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO docs (id, custom, tags) VALUES ('a', '{"name":"alice","role":"admin"}', '["x","y"]')`,
		`INSERT INTO docs (id, custom, tags) VALUES ('b', '{"name":"bob"}', '["y","z"]')`,
		`INSERT INTO docs (id, custom, tags) VALUES ('c', '{"other":1}', '["z"]')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	assertIDs := func(name, q string, want []string) {
		t.Helper()
		rows, err := db.Query(q)
		if err != nil {
			t.Fatalf("%s query: %v", name, err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("%s scan: %v", name, err)
			}
			got = append(got, id)
		}
		if len(got) != len(want) {
			t.Fatalf("%s got %v, want %v", name, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s got %v, want %v", name, got, want)
			}
		}
	}

	// JSONB ?, ?|, ?&
	assertIDs("jsonb ?", `SELECT id FROM docs WHERE custom ? 'name' ORDER BY id`, []string{"a", "b"})
	assertIDs("jsonb ?|", `SELECT id FROM docs WHERE custom ?| ARRAY['role', 'other'] ORDER BY id`, []string{"a", "c"})
	assertIDs("jsonb ?&", `SELECT id FROM docs WHERE custom ?& ARRAY['name', 'role'] ORDER BY id`, []string{"a"})

	// 'x' = ANY(tags), 'z' <> ALL(tags), id = ANY(ARRAY[...])
	assertIDs("lit = ANY(path)", `SELECT id FROM docs WHERE 'x' = ANY(tags) ORDER BY id`, []string{"a"})
	assertIDs("lit = ANY(path) y", `SELECT id FROM docs WHERE 'y' = ANY(tags) ORDER BY id`, []string{"a", "b"})
	assertIDs("lit <> ALL(path)", `SELECT id FROM docs WHERE 'z' <> ALL(tags) ORDER BY id`, []string{"a"})
	assertIDs("path = ANY(ARRAY[...])", `SELECT id FROM docs WHERE id = ANY(ARRAY['a', 'c']) ORDER BY id`, []string{"a", "c"})
	assertIDs("path <> ALL(ARRAY[...])", `SELECT id FROM docs WHERE id <> ALL(ARRAY['a', 'c']) ORDER BY id`, []string{"b"})

	// ARRAY[...] scalar constructor
	var arrOut string
	if err := db.QueryRow(`SELECT ARRAY['x', 'y']`).Scan(&arrOut); err != nil {
		t.Fatalf("SELECT ARRAY: %v", err)
	}
	if arrOut != `["x","y"]` {
		t.Fatalf("ARRAY['x','y'] got %q, want [\"x\",\"y\"]", arrOut)
	}

	// UPDATE ... SET custom = jsonb_set(...)
	if _, err := db.Exec(`UPDATE docs SET custom = jsonb_set(custom, '{name}', '"updated"') WHERE id = 'a'`); err != nil {
		t.Fatalf("UPDATE jsonb_set: %v", err)
	}
	var updatedName string
	if err := db.QueryRow(`SELECT custom->>'name' FROM docs WHERE id = 'a'`).Scan(&updatedName); err != nil {
		t.Fatalf("select after jsonb_set: %v", err)
	}
	if updatedName != "updated" {
		t.Fatalf("after jsonb_set got %q, want updated", updatedName)
	}
}
