package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestUpdateFromAndDeleteUsing(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TABLE docs (id TEXT PRIMARY KEY, n INT, tag TEXT)`,
		`CREATE TABLE changes (id TEXT PRIMARY KEY, n INT, del BOOL)`,
		`INSERT INTO docs (id, n, tag) VALUES ('a', 1, 'keep'), ('b', 2, 'keep'), ('c', 3, 'keep')`,
		`INSERT INTO changes (id, n, del) VALUES ('a', 100, false), ('c', 300, true)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}

	// 1. UPDATE ... FROM with alias and RETURNING
	rows, err := db.Query(`UPDATE docs SET n = ch.n FROM changes AS ch WHERE docs.id = ch.id RETURNING id, n`)
	if err != nil {
		t.Fatalf("UPDATE FROM: %v", err)
	}
	updated := make(map[string]int)
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatalf("scan update: %v", err)
		}
		updated[id] = n
	}
	rows.Close()
	if len(updated) != 2 || updated["a"] != 100 || updated["c"] != 300 {
		t.Fatalf("UPDATE FROM returned %v, want {a:100 c:300}", updated)
	}

	// 2. DELETE ... USING with RETURNING
	rows, err = db.Query(`DELETE FROM docs USING changes WHERE docs.id = changes.id AND changes.del = true RETURNING id`)
	if err != nil {
		t.Fatalf("DELETE USING: %v", err)
	}
	var deleted []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan delete: %v", err)
		}
		deleted = append(deleted, id)
	}
	rows.Close()
	if len(deleted) != 1 || deleted[0] != "c" {
		t.Fatalf("DELETE USING returned %v, want [c]", deleted)
	}

	// Verify final table state
	rows, err = db.Query(`SELECT id, n FROM docs ORDER BY id`)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	defer rows.Close()
	var ids []string
	var ns []int
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
		ns = append(ns, n)
	}
	if len(ids) != 2 || ids[0] != "a" || ns[0] != 100 || ids[1] != "b" || ns[1] != 2 {
		t.Fatalf("final state (%v, %v), want ([a b], [100 2])", ids, ns)
	}
}
