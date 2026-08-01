package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestTruncateDatabaseSQLReopenAndIndexReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX by_kind ON docs (kind)`); err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{
		`{"id":"a","kind":"x"}`,
		`{"id":"b","kind":"y"}`,
	} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, document); err != nil {
			t.Fatal(err)
		}
	}
	result, err := db.Exec(`TRUNCATE TABLE docs`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 0 {
		t.Fatalf("TRUNCATE rows affected = %d, err %v, want 0", affected, err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("TRUNCATE left %d rows", count)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"c","kind":"z"}`,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("reopened TRUNCATE table has %d rows, want 1", count)
	}
	if _, err := db.Exec(`DROP INDEX by_kind ON docs`); err != nil {
		t.Fatalf("DROP preserved index after reopen: %v", err)
	}
	if _, err := db.Exec(`CREATE INDEX by_kind ON docs (kind)`); err != nil {
		t.Fatalf("recreate dropped index: %v", err)
	}
}

func TestDropIndexIfExistsQualificationAndAmbiguity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"alpha", "beta"} {
		if _, err := db.Exec(`CREATE TABLE ` + table + ` (id STRING PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO `+table+` VALUES (?)`, `{"id":"one","kind":"x"}`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE INDEX by_kind ON ` + table + ` (kind)`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`DROP INDEX by_kind`); !errors.Is(err, ErrIndexAmbiguous) {
		t.Fatalf("unqualified duplicate DROP INDEX = %v, want ambiguity", err)
	}
	if _, err := db.Exec(`DROP INDEX IF EXISTS by_kind`); !errors.Is(err, ErrIndexAmbiguous) {
		t.Fatalf("ambiguous DROP INDEX IF EXISTS = %v, want ambiguity", err)
	}
	if _, err := db.Exec(`DROP INDEX by_kind ON alpha`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP INDEX by_kind ON alpha`); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("second qualified DROP INDEX = %v, want not found", err)
	}
	for _, statement := range []string{
		`DROP INDEX IF EXISTS by_kind ON alpha`,
		`DROP INDEX IF EXISTS absent`,
		`DROP INDEX IF EXISTS by_kind ON missing_table`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if _, err := db.Exec(`DROP INDEX by_kind ON missing_table`); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("qualified missing-table DROP INDEX = %v, want table not found", err)
	}
	// The failed ambiguous operations left both definitions intact. After the
	// qualified alpha removal, beta is the unique unqualified target.
	if _, err := db.Exec(`DROP INDEX by_kind`); err != nil {
		t.Fatalf("unique unqualified DROP INDEX: %v", err)
	}
	if _, err := db.Exec(`DROP INDEX IF EXISTS by_kind`); err != nil {
		t.Fatalf("unqualified no-op DROP INDEX IF EXISTS: %v", err)
	}
}

func TestStorageReplacementDDLRejectedInTransactions(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX by_kind ON docs (kind)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"a","kind":"x"}`,
	); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`TRUNCATE docs`,
		`DROP INDEX by_kind ON docs`,
	} {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(statement); !errors.Is(err, ErrDDLInTransaction) {
			_ = tx.Rollback()
			t.Fatalf("transactional %s = %v, want ErrDDLInTransaction", statement, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rejected transactional DDL changed row count to %d", count)
	}
	if _, err := db.Exec(`DROP INDEX by_kind ON docs`); err != nil {
		t.Fatalf("rejected transactional DROP changed index: %v", err)
	}
}

func TestTypedRuntimeStorageDDLFailedTransactionState(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(ctx)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	defer session.Close()
	defer database.Close()
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY)`,
		`CREATE INDEX by_kind ON docs (kind)`,
		`INSERT INTO docs VALUES ('{"id":"a","kind":"x"}')`,
	} {
		prepared := runtimePrepare(t, session, statement)
		if _, err := prepared.Exec(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	truncate := runtimePrepare(t, session, `TRUNCATE docs`)
	drop := runtimePrepare(t, session, `DROP INDEX by_kind ON docs`)
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := truncate.Exec(ctx, nil); !errors.Is(err, ErrDDLInTransaction) {
		t.Fatalf("typed transactional TRUNCATE = %v", err)
	}
	if _, err := drop.Exec(ctx, nil); !errors.Is(err, ErrTransactionFailed) {
		t.Fatalf("typed failed-state DROP INDEX = %v", err)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := truncate.Exec(ctx, nil); err != nil {
		t.Fatalf("typed autocommit TRUNCATE after rollback: %v", err)
	}
}
