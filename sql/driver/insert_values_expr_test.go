package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestInsertValuesScalarExpressions(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (id TEXT PRIMARY KEY, name TEXT, score INT, ts TEXT, tags JSONB)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO events (id, name, score, ts, tags) VALUES (GEN_RANDOM_UUID(), LOWER('HELLO'), 10 + 5 * 2, NOW(), ARRAY['a', 'b'])`); err != nil {
		t.Fatalf("INSERT VALUES exprs: %v", err)
	}

	var id, name, ts, tags string
	var score int
	if err := db.QueryRow(`SELECT id, name, score, ts, tags FROM events`).Scan(&id, &name, &score, &ts, &tags); err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(id) != 36 {
		t.Fatalf("GEN_RANDOM_UUID() id = %q, want 36-char UUID", id)
	}
	if name != "hello" {
		t.Fatalf("LOWER('HELLO') = %q, want hello", name)
	}
	if score != 20 {
		t.Fatalf("10 + 5 * 2 = %d, want 20", score)
	}
	if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
		t.Fatalf("NOW() = %q, want RFC3339: %v", ts, err)
	}
	if tags != `["a","b"]` {
		t.Fatalf("ARRAY['a','b'] = %q, want [\"a\",\"b\"]", tags)
	}
}
