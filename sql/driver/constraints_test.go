package driver_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestDefaultAndCheckConstraintEnforcement(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "constraints.vdb")
	db, err := sql.Open("vibedb", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE accounts (
		id TEXT PRIMARY KEY,
		status TEXT DEFAULT 'active',
		balance INTEGER DEFAULT 100 CHECK (balance >= 0),
		credit_limit INTEGER DEFAULT 500,
		CHECK (balance <= credit_limit)
	)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// 1. Omitted columns receive their DEFAULT values.
	if _, err := db.Exec(`INSERT INTO accounts (id) VALUES ('a1')`); err != nil {
		t.Fatalf("INSERT with defaults: %v", err)
	}
	var status string
	var balance, limit int
	if err := db.QueryRow(`SELECT status, balance, credit_limit FROM accounts WHERE id = 'a1'`).Scan(&status, &balance, &limit); err != nil {
		t.Fatal(err)
	}
	if status != "active" || balance != 100 || limit != 500 {
		t.Fatalf("defaults = (%q, %d, %d), want (active, 100, 500)", status, balance, limit)
	}

	// 2. Column-level CHECK violation rejected on INSERT.
	if _, err := db.Exec(`INSERT INTO accounts (id, balance) VALUES ('a2', -10)`); !errors.Is(err, sqldriver.ErrCheckViolation) {
		t.Fatalf("negative balance INSERT err = %v, want ErrCheckViolation", err)
	}

	// 3. Table-level CHECK violation rejected on INSERT.
	if _, err := db.Exec(`INSERT INTO accounts (id, balance, credit_limit) VALUES ('a3', 600, 500)`); !errors.Is(err, sqldriver.ErrCheckViolation) {
		t.Fatalf("balance > credit_limit INSERT err = %v, want ErrCheckViolation", err)
	}

	// 4. CHECK violation rejected on UPDATE.
	if _, err := db.Exec(`UPDATE accounts SET balance = -5 WHERE id = 'a1'`); !errors.Is(err, sqldriver.ErrCheckViolation) {
		t.Fatalf("negative balance UPDATE err = %v, want ErrCheckViolation", err)
	}
	if _, err := db.Exec(`UPDATE accounts SET balance = 999 WHERE id = 'a1'`); !errors.Is(err, sqldriver.ErrCheckViolation) {
		t.Fatalf("balance > credit_limit UPDATE err = %v, want ErrCheckViolation", err)
	}

	// 5. Valid UPDATE succeeds.
	if _, err := db.Exec(`UPDATE accounts SET balance = 250 WHERE id = 'a1'`); err != nil {
		t.Fatalf("valid UPDATE: %v", err)
	}

	// 6. Reopen persists DEFAULT and CHECK metadata across restarts.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := sql.Open("vibedb", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if _, err := db2.Exec(`INSERT INTO accounts (id) VALUES ('a4')`); err != nil {
		t.Fatalf("INSERT after reopen: %v", err)
	}
	if err := db2.QueryRow(`SELECT status, balance, credit_limit FROM accounts WHERE id = 'a4'`).Scan(&status, &balance, &limit); err != nil {
		t.Fatal(err)
	}
	if status != "active" || balance != 100 || limit != 500 {
		t.Fatalf("defaults after reopen = (%q, %d, %d), want (active, 100, 500)", status, balance, limit)
	}
	if _, err := db2.Exec(`INSERT INTO accounts (id, balance) VALUES ('a5', -1)`); !errors.Is(err, sqldriver.ErrCheckViolation) {
		t.Fatalf("CHECK after reopen err = %v, want ErrCheckViolation", err)
	}

	// 7. ALTER TABLE SET DEFAULT, DROP DEFAULT, and ADD CONSTRAINT CHECK.
	if _, err := db2.Exec(`ALTER TABLE accounts ALTER COLUMN status SET DEFAULT 'pending'`); err != nil {
		t.Fatalf("ALTER SET DEFAULT: %v", err)
	}
	if _, err := db2.Exec(`INSERT INTO accounts (id) VALUES ('a6')`); err != nil {
		t.Fatalf("INSERT after ALTER SET DEFAULT: %v", err)
	}
	if err := db2.QueryRow(`SELECT status FROM accounts WHERE id = 'a6'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("status after ALTER SET DEFAULT = %q, want pending", status)
	}
	if _, err := db2.Exec(`ALTER TABLE accounts ADD CONSTRAINT chk_min_limit CHECK (credit_limit >= 100)`); err != nil {
		t.Fatalf("ALTER ADD CONSTRAINT CHECK: %v", err)
	}
	if _, err := db2.Exec(`INSERT INTO accounts (id, balance, credit_limit) VALUES ('a7', 50, 50)`); !errors.Is(err, sqldriver.ErrCheckViolation) {
		t.Fatalf("INSERT violating ALTER CHECK err = %v, want ErrCheckViolation", err)
	}
	if _, err := db2.Exec(`ALTER TABLE accounts ALTER COLUMN status DROP DEFAULT`); err != nil {
		t.Fatalf("ALTER DROP DEFAULT: %v", err)
	}
	if _, err := db2.Exec(`INSERT INTO accounts (id) VALUES ('a8')`); err != nil {
		t.Fatalf("INSERT after ALTER DROP DEFAULT: %v", err)
	}
	var nullStatus sql.NullString
	if err := db2.QueryRow(`SELECT status FROM accounts WHERE id = 'a8'`).Scan(&nullStatus); err != nil {
		t.Fatal(err)
	}
	if nullStatus.Valid {
		t.Fatalf("expected NULL status after DROP DEFAULT, got %q", nullStatus.String)
	}
}
