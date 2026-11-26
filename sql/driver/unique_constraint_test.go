package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestCreateTableUniqueConstraintEnforcement(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(255) UNIQUE NOT NULL, tenant TEXT NOT NULL, handle TEXT NOT NULL, UNIQUE (tenant, handle))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO users (id, email, tenant, handle) VALUES (1, 'ada@example.com', 't1', 'ada')`); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO users (id, email, tenant, handle) VALUES (2, 'ada@example.com', 't1', 'other')`); err == nil {
		t.Fatal("expected unique violation for duplicate email")
	}
	if _, err := db.Exec(`INSERT INTO users (id, email, tenant, handle) VALUES (3, 'grace@example.com', 't1', 'ada')`); err == nil {
		t.Fatal("expected unique violation for duplicate (tenant, handle)")
	}
	if _, err := db.Exec(`INSERT INTO users (id, email, tenant, handle) VALUES (4, 'grace@example.com', 't2', 'ada')`); err != nil {
		t.Fatalf("insert 4 (different tenant): %v", err)
	}
}
