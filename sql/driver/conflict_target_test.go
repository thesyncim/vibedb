package driver

import (
	"errors"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestConflictTargetValidatesPrimaryAndRemainsAtomic(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE target_docs (id TEXT PRIMARY KEY, other TEXT, n INT)`,
		`CREATE UNIQUE INDEX target_other ON target_docs(other)`,
		`INSERT INTO target_docs (id, other, n) VALUES ('a', 'first', 1)`,
		`INSERT INTO target_docs (id, other, n) VALUES ('a', 'second', 2) ON CONFLICT (id) DO NOTHING`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	var n int64
	if err := db.QueryRow(`INSERT INTO target_docs (id, other, n) VALUES ('a', 'first', 4)
		ON CONFLICT (id) DO UPDATE SET n = GREATEST(target_docs.n, EXCLUDED.n) RETURNING n`).Scan(&n); err != nil || n != 4 {
		t.Fatalf("target upsert = %d: %v", n, err)
	}
	for _, target := range []string{"other", "missing"} {
		_, err := db.Exec(`INSERT INTO target_docs (id, other, n) VALUES ('b', 'second', 2) ON CONFLICT (` + target + `) DO NOTHING`)
		var unsupported *sqlast.FeatureNotSupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("invalid target %s = %v", target, err)
		}
	}
	// The explicit primary target must not turn an unrelated unique violation
	// into a skipped row, and the earlier row in the batch must roll back.
	if _, err := db.Exec(`INSERT INTO target_docs (id, other, n) VALUES ('b', 'second', 2), ('c', 'first', 3) ON CONFLICT (id) DO NOTHING`); err == nil {
		t.Fatal("secondary unique conflict was ignored")
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM target_docs`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("failed batch left %d rows: %v", n, err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO target_docs (id, other, n) VALUES ('a', 'first', 9) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT n FROM target_docs WHERE id = 'a'`).Scan(&n); err != nil || n != 9 {
		t.Fatalf("transaction upsert = %d: %v", n, err)
	}
}
