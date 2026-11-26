package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestCompositePrimaryKeyAndConflictTarget(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	db, err := sql.Open("vibedb", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE members (app_pk INT, channel_cid TEXT, user_id TEXT, role TEXT, PRIMARY KEY (app_pk, channel_cid, user_id))`); err != nil {
		t.Fatalf("CREATE TABLE composite PK: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO members (app_pk, channel_cid, user_id, role) VALUES (1, 'ch1', 'u1', 'member')`,
		`INSERT INTO members (app_pk, channel_cid, user_id, role) VALUES (1, 'ch1', 'u2', 'member')`,
		`INSERT INTO members (app_pk, channel_cid, user_id, role) VALUES (1, 'ch2', 'u1', 'admin')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("insert %s: %v", stmt, err)
		}
	}

	// Duplicate composite PK must fail.
	if _, err := db.Exec(`INSERT INTO members (app_pk, channel_cid, user_id, role) VALUES (1, 'ch1', 'u1', 'dup')`); err == nil {
		t.Fatal("expected duplicate composite PK error, got nil")
	}

	// ON CONFLICT (app_pk, channel_cid, user_id) DO NOTHING must succeed and skip.
	if _, err := db.Exec(`INSERT INTO members (app_pk, channel_cid, user_id, role) VALUES (1, 'ch1', 'u1', 'ignored') ON CONFLICT (app_pk, channel_cid, user_id) DO NOTHING`); err != nil {
		t.Fatalf("ON CONFLICT composite DO NOTHING: %v", err)
	}

	// ON CONFLICT (app_pk, channel_cid, user_id) DO UPDATE must update the matching row.
	if _, err := db.Exec(`INSERT INTO members (app_pk, channel_cid, user_id, role) VALUES (1, 'ch1', 'u1', 'owner') ON CONFLICT (app_pk, channel_cid, user_id) DO UPDATE SET role = EXCLUDED.role`); err != nil {
		t.Fatalf("ON CONFLICT composite DO UPDATE: %v", err)
	}

	var role string
	if err := db.QueryRow(`SELECT role FROM members WHERE app_pk = 1 AND channel_cid = 'ch1' AND user_id = 'u1'`).Scan(&role); err != nil {
		t.Fatalf("select after upsert: %v", err)
	}
	if role != "owner" {
		t.Fatalf("role = %q, want owner", role)
	}

	// Reopen database and verify composite PK catalog persistence + enforcement.
	db.Close()
	db2, err := sql.Open("vibedb", dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	var count int
	if err := db2.QueryRow(`SELECT COUNT(*) FROM members`).Scan(&count); err != nil {
		t.Fatalf("count after reopen: %v", err)
	}
	if count != 3 {
		t.Fatalf("count after reopen = %d, want 3", count)
	}
	if _, err := db2.Exec(`INSERT INTO members (app_pk, channel_cid, user_id, role) VALUES (1, 'ch1', 'u1', 'dup2')`); err == nil {
		t.Fatal("expected duplicate composite PK error after reopen, got nil")
	}
}
