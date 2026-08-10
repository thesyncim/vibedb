package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"testing"
)

func isolationCount(t testing.TB, tx *stdsql.Tx) int64 {
	t.Helper()
	var count int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestDatabaseSQLReadCommittedRefreshAndRepeatableReadCut(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, value) VALUES ('a', 'before')`); err != nil {
		t.Fatal(err)
	}

	rc, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, rc); got != 1 {
		t.Fatalf("READ COMMITTED first count = %d, want 1", got)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, value) VALUES ('b', 'outside')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE docs SET "$doc" = '{"id":"a","value":"after"}' WHERE id = 'a'`); err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, rc); got != 2 {
		t.Fatalf("READ COMMITTED refreshed count = %d, want 2", got)
	}
	var value string
	if err := rc.QueryRow(`SELECT value FROM docs WHERE id = 'a'`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "after" {
		t.Fatalf("READ COMMITTED refreshed value = %q, want after", value)
	}
	if err := rc.Rollback(); err != nil {
		t.Fatal(err)
	}

	rr, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, rr); got != 2 {
		t.Fatalf("REPEATABLE READ first count = %d, want 2", got)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, value) VALUES ('c', 'later')`); err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, rr); got != 2 {
		t.Fatalf("REPEATABLE READ second count = %d, want fixed 2", got)
	}
	if err := rr.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestReadCommittedPreservesOverlayAndSavepointsAcrossRefresh(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO docs (id) VALUES ('kept')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SAVEPOINT stable`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs (id) VALUES ('rolled')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id) VALUES ('outside')`); err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, tx); got != 3 {
		t.Fatalf("refreshed overlay count = %d, want 3", got)
	}
	if _, err := tx.Exec(`ROLLBACK TO stable`); err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, tx); got != 2 {
		t.Fatalf("rollback-to after refresh count = %d, want 2", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestReadCommittedDirtyKeysKeepTheirStatementObservation(t *testing.T) {
	t.Run("freshly observed key", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO docs (id, value) VALUES ('key', 'initial')`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelReadCommitted,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`UPDATE docs SET "$doc" = '{"id":"key","value":"outside"}' WHERE id = 'key'`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(
			`UPDATE docs SET "$doc" = '{"id":"key","value":"transaction"}' WHERE id = 'key'`,
		); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("fresh-cut writer conflicted with an already observed write: %v", err)
		}
	})

	t.Run("earlier pending key", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('early', 'initial'), ('late', 'initial')`,
		); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelReadCommitted,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(
			`UPDATE docs SET "$doc" = '{"id":"early","value":"pending"}' WHERE id = 'early'`,
		); err != nil {
			t.Fatal(err)
		}
		for _, statement := range []string{
			`UPDATE docs SET "$doc" = '{"id":"early","value":"outside"}' WHERE id = 'early'`,
			`UPDATE docs SET "$doc" = '{"id":"late","value":"outside"}' WHERE id = 'late'`,
		} {
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.Exec(
			`UPDATE docs SET "$doc" = '{"id":"late","value":"pending"}' WHERE id = 'late'`,
		); err != nil {
			t.Fatal(err)
		}
		// Restaging the early overlay after another refreshed statement must not
		// replace the key's original observation with the newer revision.
		if _, err := tx.Exec(
			`UPDATE docs SET "$doc" = '{"id":"early","value":"restaged"}' WHERE id = 'early'`,
		); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("COMMIT = %v, want earlier-key ErrTransactionConflict", err)
		}
	})
}

func TestSerializableRejectsWriteSkew(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, on_call BOOL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, on_call) VALUES ('a', true), ('b', true)`); err != nil {
		t.Fatal(err)
	}
	begin := func() *stdsql.Tx {
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelSerializable,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	left, right := begin(), begin()
	for _, tx := range []*stdsql.Tx{left, right} {
		var count int64
		if err := tx.QueryRow(`SELECT COUNT(*) FROM docs WHERE on_call = true`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("on-call count = %d, want 2", count)
		}
	}
	if _, err := left.Exec(`UPDATE docs SET "$doc" = '{"id":"a","on_call":false}' WHERE id = 'a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Exec(`UPDATE docs SET "$doc" = '{"id":"b","on_call":false}' WHERE id = 'b'`); err != nil {
		t.Fatal(err)
	}
	if err := left.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := right.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("second serializable COMMIT = %v, want ErrTransactionConflict", err)
	}
}

func TestIsolationLevelRefusalsAndTypedReadCommitted(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadUncommitted,
	}); !errors.Is(err, ErrUnsupportedIsolation) {
		t.Fatalf("READ UNCOMMITTED = %v, want ErrUnsupportedIsolation", err)
	}
	if _, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelLinearizable,
	}); !errors.Is(err, ErrUnsupportedIsolation) {
		t.Fatalf("LINEARIZABLE = %v, want ErrUnsupportedIsolation", err)
	}

	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	other, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	create := runtimePrepare(t, session, `CREATE TABLE docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, other, `INSERT INTO docs VALUES (?)`)
	count := runtimePrepare(t, session, `SELECT COUNT(*) FROM docs`)
	if err := session.Begin(context.Background(), TxOptions{
		Isolation: IsolationReadCommitted,
	}); err != nil {
		t.Fatal(err)
	}
	assertTypedIsolationCount(t, count, 0)
	if _, err := insert.Exec(
		context.Background(), []any{[]byte(`{"id":"outside"}`)},
	); err != nil {
		t.Fatal(err)
	}
	assertTypedIsolationCount(t, count, 1)
	if err := session.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(context.Background(), TxOptions{
		Isolation: IsolationLevel(255),
	}); !errors.Is(err, ErrUnsupportedIsolation) {
		t.Fatalf("typed invalid isolation = %v, want ErrUnsupportedIsolation", err)
	}
}

func TestReadCommittedCatalogChangeFailsClosed(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`CREATE TABLE late (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	var count int64
	err = tx.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count)
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("statement after catalog change = %v, want ErrTransactionConflict", err)
	}
}

func TestReadCommittedPlainExplainUsesStatementCatalogFence(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var plan string
	if err := tx.QueryRow(`EXPLAIN SELECT id FROM docs`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE late (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	err = tx.QueryRow(`EXPLAIN SELECT id FROM docs`).Scan(&plan)
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("plain EXPLAIN after catalog change = %v, want ErrTransactionConflict", err)
	}
}

func TestPreparedPointPlanRevalidatesPrimaryKeyAfterRecreate(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
		t.Fatal(err)
	}
	prepared, err := db.Prepare(`SELECT value FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for _, statement := range []string{
		`DROP TABLE docs`,
		`CREATE TABLE docs (code STRING PRIMARY KEY, id STRING, value STRING)`,
		`INSERT INTO docs (code, id, value) VALUES ('storage-key', 'lookup', 'found')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	var value string
	if err := prepared.QueryRow("lookup").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "found" {
		t.Fatalf("recreated-table query = %q, want found", value)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Stmt(prepared).QueryRow("lookup").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "found" {
		t.Fatalf("recreated-table transaction query = %q, want found", value)
	}
}

func assertTypedIsolationCount(t testing.TB, prepared *Prepared, want int64) {
	t.Helper()
	cursor, err := prepared.Query(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("COUNT returned no row")
	}
	got, ok := cursor.Cell(0).Int64()
	if !ok || got != want {
		t.Fatalf("COUNT = (%d, %v), want %d", got, ok, want)
	}
}
