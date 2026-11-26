package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestWindowIgnoreNullsFromLastAndDistinct(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO items (id, val) VALUES (1, NULL)`,
		`INSERT INTO items (id, val) VALUES (2, 10)`,
		`INSERT INTO items (id, val) VALUES (3, NULL)`,
		`INSERT INTO items (id, val) VALUES (4, 10)`,
		`INSERT INTO items (id, val) VALUES (5, 20)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("insert %q: %v", stmt, err)
		}
	}

	rows, err := db.Query(`
		SELECT id,
		       FIRST_VALUE(val) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fv,
		       LAST_VALUE(val) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv,
		       NTH_VALUE(val, 1) FROM LAST IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nv,
		       COUNT(DISTINCT val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS cd
		FROM items
		ORDER BY id
		LIMIT 1`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected 1 row")
	}
	var id, fv, lv, nv, cd int64
	if err := rows.Scan(&id, &fv, &lv, &nv, &cd); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if id != 1 || fv != 10 || lv != 20 || nv != 20 || cd != 2 {
		t.Fatalf("got (%d, %d, %d, %d, %d), want (1, 10, 20, 20, 2)", id, fv, lv, nv, cd)
	}
}
