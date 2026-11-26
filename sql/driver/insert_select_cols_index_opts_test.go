package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestInsertSelectColumnsAndCreateIndexOptions(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TABLE src (id TEXT PRIMARY KEY, n INT, hidden TEXT)`,
		`CREATE TABLE dst (id TEXT PRIMARY KEY, n INT, hidden TEXT)`,
		`INSERT INTO src (id, n, hidden) VALUES ('a', 10, 'h1'), ('b', 20, 'h2')`,
		`INSERT INTO dst (id, n) SELECT id, n FROM src`,
		`CREATE INDEX idx_partial ON dst(n) WHERE n > 0`,
		`CREATE INDEX idx_expr ON dst(GREATEST(n, 0))`,
		`CREATE INDEX idx_cover ON dst(n) INCLUDE (hidden)`,
		`CREATE INDEX idx_desc ON dst(n DESC NULLS LAST)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	rows, err := db.Query(`SELECT id, n FROM dst ORDER BY id`)
	if err != nil {
		t.Fatalf("query dst: %v", err)
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
	if len(ids) != 2 || ids[0] != "a" || ns[0] != 10 || ids[1] != "b" || ns[1] != 20 {
		t.Fatalf("dst got (%v, %v), want ([a b], [10 20])", ids, ns)
	}
}
