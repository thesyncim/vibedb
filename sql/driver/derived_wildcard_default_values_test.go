package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestDerivedWildcardScalarProjection(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TABLE docs (id TEXT PRIMARY KEY, n INT)`,
		`INSERT INTO docs (id, n) VALUES ('a', 5), ('b', NULL)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	rows, err := db.Query(`SELECT d.*, COALESCE(d.n, 0) FROM (SELECT * FROM docs) AS d`)
	if err != nil {
		t.Fatalf("derived wildcard + scalar: %v", err)
	}
	defer rows.Close()
	var count int
	var sum int64
	for rows.Next() {
		var doc string
		var coalesced int64
		if err := rows.Scan(&doc, &coalesced); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
		sum += coalesced
	}
	if count != 2 || sum != 5 {
		t.Fatalf("got count=%d sum=%d, want count=2 sum=5", count, sum)
	}
}
