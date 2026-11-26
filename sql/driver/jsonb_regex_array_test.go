package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestJSONBArrayIndexRegexAndArrayFunctions(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, tags TEXT[], meta JSONB)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO items VALUES ('{"id":1,"tags":["alpha","beta","gamma"],"meta":{"nested":{"k":"v"},"drop":null}}')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var firstTag, joined, extracted, replaced string
	var card, rcount int64
	var rlike bool
	err = db.QueryRow(`
		SELECT tags ->> 0,
		       ARRAY_TO_STRING(tags, ','),
		       JSONB_EXTRACT_PATH_TEXT(meta, 'nested', 'k'),
		       REGEXP_REPLACE('foo-123-bar-456', '[0-9]+', 'X', 'g'),
		       CARDINALITY(tags),
		       REGEXP_COUNT('a1b2c3', '[0-9]'),
		       REGEXP_LIKE('Hello World', 'world', 'i')
		FROM items WHERE id = 1`).Scan(&firstTag, &joined, &extracted, &replaced, &card, &rcount, &rlike)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if firstTag != "alpha" || joined != "alpha,beta,gamma" || extracted != "v" ||
		replaced != "foo-X-bar-X" || card != 3 || rcount != 3 || !rlike {
		t.Fatalf("got (%q, %q, %q, %q, %d, %d, %t)", firstTag, joined, extracted, replaced, card, rcount, rlike)
	}
}
