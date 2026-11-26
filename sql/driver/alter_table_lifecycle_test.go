package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestAlterTableFullLifecycle(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE accounts (id INTEGER PRIMARY KEY, name STRING NOT NULL, note STRING NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO accounts (id, name, note) VALUES (1, 'Ada', 'hello')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE accounts ALTER COLUMN note SET NOT NULL`); err != nil {
		t.Fatalf("SET NOT NULL: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE accounts ALTER COLUMN note DROP NOT NULL`); err != nil {
		t.Fatalf("DROP NOT NULL: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE accounts ALTER COLUMN note SET DATA TYPE TEXT`); err != nil {
		t.Fatalf("SET DATA TYPE: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE accounts ALTER COLUMN note SET DEFAULT 'none'`); err != nil {
		t.Fatalf("SET DEFAULT: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE accounts ALTER COLUMN note DROP DEFAULT`); err != nil {
		t.Fatalf("DROP DEFAULT: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE accounts RENAME COLUMN note TO memo`); err != nil {
		t.Fatalf("RENAME COLUMN: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE accounts DROP COLUMN IF EXISTS memo`); err != nil {
		t.Fatalf("DROP COLUMN: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE accounts RENAME TO members`); err != nil {
		t.Fatalf("RENAME TABLE: %v", err)
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM members WHERE id = 1`).Scan(&name); err != nil || name != "Ada" {
		t.Fatalf("SELECT after rename = %q, %v; want Ada", name, err)
	}
}
