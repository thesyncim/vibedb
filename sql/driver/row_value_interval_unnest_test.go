package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestRowValueComparisonIntervalArithAndUnnest(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE items (id TEXT PRIMARY KEY, n INT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO items (id, n) VALUES ('a', 1)`,
		`INSERT INTO items (id, n) VALUES ('b', 1)`,
		`INSERT INTO items (id, n) VALUES ('a2', 2)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// 1. Row-value comparison (keyset pagination)
	rows, err := db.Query(`SELECT id FROM items WHERE (n, id) > (1, 'a') ORDER BY n, id`)
	if err != nil {
		t.Fatalf("row value >: %v", err)
	}
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, id)
	}
	rows.Close()
	if len(got) != 2 || got[0] != "b" || got[1] != "a2" {
		t.Fatalf("row value > got %v, want [b a2]", got)
	}

	rows, err = db.Query(`SELECT id FROM items WHERE (n, id) = (1, 'b')`)
	if err != nil {
		t.Fatalf("row value =: %v", err)
	}
	got = got[:0]
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, id)
	}
	rows.Close()
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("row value = got %v, want [b]", got)
	}

	// 2. Timestamp + INTERVAL arithmetic
	var tsOut string
	if err := db.QueryRow(`SELECT TIMESTAMP '2026-01-01T00:00:00Z' + INTERVAL '1 hour 30 minutes'`).Scan(&tsOut); err != nil {
		t.Fatalf("timestamp + interval: %v", err)
	}
	if tsOut != "2026-01-01T01:30:00Z" {
		t.Fatalf("timestamp + interval got %q, want 2026-01-01T01:30:00Z", tsOut)
	}
	var nowPlus string
	if err := db.QueryRow(`SELECT NOW() + INTERVAL '1 second'`).Scan(&nowPlus); err != nil {
		t.Fatalf("NOW() + INTERVAL: %v", err)
	}
	if _, err := time.Parse(time.RFC3339Nano, nowPlus); err != nil {
		t.Fatalf("NOW() + INTERVAL returned non-RFC3339 %q: %v", nowPlus, err)
	}

	// 3. UNNEST(ARRAY[...]) AS value
	rows, err = db.Query(`SELECT value FROM unnest(ARRAY['x', 'y', 'z']) AS value`)
	if err != nil {
		t.Fatalf("unnest: %v", err)
	}
	var vals []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan unnest: %v", err)
		}
		vals = append(vals, v)
	}
	rows.Close()
	if len(vals) != 3 || vals[0] != "x" || vals[1] != "y" || vals[2] != "z" {
		t.Fatalf("unnest got %v, want [x y z]", vals)
	}
}
