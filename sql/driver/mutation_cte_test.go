package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestDataModifyingCTEs(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TABLE docs (id TEXT PRIMARY KEY, n INT)`,
		`INSERT INTO docs (id, n) VALUES ('a', 10), ('b', 20), ('c', 30)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	// 1. WITH removed AS (DELETE ... RETURNING n) SELECT n FROM removed
	var deletedN int64
	if err := db.QueryRow(`WITH removed AS (DELETE FROM docs WHERE id = 'a' RETURNING n) SELECT n FROM removed`).Scan(&deletedN); err != nil {
		t.Fatalf("DELETE CTE: %v", err)
	}
	if deletedN != 10 {
		t.Fatalf("DELETE CTE got %d, want 10", deletedN)
	}

	// 2. WITH bumped AS (UPDATE ... RETURNING id, n) SELECT id, n FROM bumped
	var updID string
	var updN int64
	if err := db.QueryRow(`WITH bumped AS (UPDATE docs SET n = n + 5 WHERE id = 'b' RETURNING id, n) SELECT id, n FROM bumped`).Scan(&updID, &updN); err != nil {
		t.Fatalf("UPDATE CTE: %v", err)
	}
	if updID != "b" || updN != 25 {
		t.Fatalf("UPDATE CTE got (%q, %d), want (b, 25)", updID, updN)
	}

	// 3. WITH added AS (INSERT ... RETURNING id, n) SELECT id, n FROM added
	var insID string
	var insN int64
	if err := db.QueryRow(`WITH added AS (INSERT INTO docs (id, n) VALUES ('d', 40) RETURNING id, n) SELECT id, n FROM added`).Scan(&insID, &insN); err != nil {
		t.Fatalf("INSERT CTE: %v", err)
	}
	if insID != "d" || insN != 40 {
		t.Fatalf("INSERT CTE got (%q, %d), want (d, 40)", insID, insN)
	}
}
