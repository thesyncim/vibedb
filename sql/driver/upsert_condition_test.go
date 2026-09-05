package driver

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestUpsertConditionRowsReturningTransactionAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := sql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err = db.Exec(`CREATE TABLE counters (id TEXT PRIMARY KEY, n INTEGER NOT NULL, enabled BOOLEAN)`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO counters (id,n,enabled) VALUES ('a',12,true),('b',18,true),('c',24,true)`); err != nil {
		t.Fatal(err)
	}
	const text = `INSERT INTO counters (id,n,enabled) VALUES (?,?,?) ON CONFLICT DO UPDATE SET n=counters.n/EXCLUDED.n WHERE EXCLUDED.enabled AND counters.n>?`
	prepared, err := db.Prepare(text)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id          string
		n           int
		enabled     any
		floor, rows int64
	}{
		{"a", 0, false, 0, 0}, {"a", 0, nil, 0, 0}, {"a", 0, true, 12, 0},
		{"a", 3, true, 0, 1}, {"a", 0, false, 0, 0}, {"new", 0, false, 0, 1},
	} {
		result, err := prepared.Exec(tc.id, tc.n, tc.enabled, tc.floor)
		if err != nil {
			t.Fatal(err)
		}
		if rows, err := result.RowsAffected(); err != nil || rows != tc.rows {
			t.Fatalf("rows=%d err=%v want=%d", rows, err, tc.rows)
		}
	}
	_ = prepared.Close()
	var id string
	if err := db.QueryRow(text+` RETURNING id`, "a", 0, false, 0).Scan(&id); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("skipped RETURNING: %v", err)
	}
	// A later failing row rolls back the earlier evaluated postimage.
	if _, err := db.Exec(`INSERT INTO counters (id,n,enabled) VALUES ('a',2,true),('b',0,true) ON CONFLICT DO UPDATE SET n=counters.n/EXCLUDED.n WHERE EXCLUDED.enabled`); err == nil {
		t.Fatal("division by zero accepted")
	}
	// The transaction sees its own staged version in the next condition.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SAVEPOINT before_condition`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(text, "a", 2, true, 0); err != nil {
		t.Fatal(err)
	}
	if result, err := tx.Exec(text, "a", 0, true, 2); err != nil {
		t.Fatal(err)
	} else if rows, _ := result.RowsAffected(); rows != 0 {
		t.Fatalf("staged condition rows=%d", rows)
	}
	if _, err := tx.Exec(`ROLLBACK TO SAVEPOINT before_condition`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO counters (id,n,enabled) VALUES ('b',30,true) ON CONFLICT DO UPDATE SET "$doc"=EXCLUDED."$doc" WHERE counters.n<EXCLUDED.n`); err != nil {
		t.Fatal(err)
	}
	if result, err := tx.Exec(`INSERT INTO counters (id,n,enabled) VALUES ('c',99,false) ON CONFLICT DO UPDATE SET n=EXCLUDED.n WHERE EXCLUDED.enabled`); err != nil {
		t.Fatal(err)
	} else if rows, _ := result.RowsAffected(); rows != 0 {
		t.Fatalf("direct condition rows=%d", rows)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id string
		n  int64
	}{{"a", 4}, {"b", 30}, {"c", 24}, {"new", 0}} {
		var n int64
		if err := db.QueryRow(`SELECT n FROM counters WHERE id=?`, tc.id).Scan(&n); err != nil || n != tc.n {
			t.Fatalf("reopen %s n=%d err=%v want=%d", tc.id, n, err, tc.n)
		}
	}
}

func TestUpsertConditionValidatesNamesAndCandidateBeforeBranch(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE counters (id TEXT PRIMARY KEY,n INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{`n=1 WHERE counters.missing=1`, `"$doc"=EXCLUDED."$doc" WHERE EXCLUDED.missing=1`, `n=1 WHERE n=1`} {
		if _, err := db.Prepare(`INSERT INTO counters (id,n) VALUES ('a',1) ON CONFLICT DO UPDATE SET ` + suffix); err == nil {
			t.Fatalf("invalid namespace accepted: %s", suffix)
		}
	}
	if _, err := db.Exec(`INSERT INTO counters (id,n) VALUES ('a',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO counters (id,n) VALUES ('a',NULL) ON CONFLICT DO UPDATE SET n=1 WHERE false`); err == nil {
		t.Fatal("false condition hid invalid candidate")
	}
}
