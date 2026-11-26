package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestDollarQuotedEStringsAndDeleteAlias(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO notes (id, body) VALUES (1, $$hello 'world'$$), (2, $tag$line1$tag$), (3, E'a\nb\tc')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var b1, b2, b3 string
	if err := db.QueryRow(`SELECT body FROM notes WHERE id = 1`).Scan(&b1); err != nil || b1 != "hello 'world'" {
		t.Fatalf("b1 = %q, %v", b1, err)
	}
	if err := db.QueryRow(`SELECT body FROM notes WHERE id = 2`).Scan(&b2); err != nil || b2 != "line1" {
		t.Fatalf("b2 = %q, %v", b2, err)
	}
	if err := db.QueryRow(`SELECT body FROM notes WHERE id = 3`).Scan(&b3); err != nil || b3 != "a\nb\tc" {
		t.Fatalf("b3 = %q, %v", b3, err)
	}

	var deletedID int64
	if err := db.QueryRow(`DELETE FROM notes AS n WHERE n.id = 2 RETURNING n.id`).Scan(&deletedID); err != nil || deletedID != 2 {
		t.Fatalf("DELETE AS alias = %d, %v", deletedID, err)
	}
}
