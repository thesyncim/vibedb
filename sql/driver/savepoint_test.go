package driver

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestSavepointPutDeletePutAcrossMarks(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"1","v":"base"}`); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`SAVEPOINT s1`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"1","v":"s1"}`, "1",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SAVEPOINT s2`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM docs WHERE id = ?`, "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"1","v":"s2"}`); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := tx.QueryRow(`SELECT v FROM docs WHERE id = ?`, "1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "s2" {
		t.Fatalf("after put-delete-put v = %s, want s2", got)
	}
	if _, err := tx.Exec(`ROLLBACK TO s1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`SELECT v FROM docs WHERE id = ?`, "1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "base" {
		t.Fatalf("after ROLLBACK TO s1 v = %s, want base", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertTableField(t, db, "docs", "1", "v", "base")
}

func TestSavepointDuplicateNameReplaceAndLIFORelease(t *testing.T) {
	ctx := context.Background()
	session := openSavepointSession(t)
	defer session.Close()
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	tx := session.conn.tx
	if err := tx.savepoint("outer"); err != nil {
		t.Fatal(err)
	}
	if err := tx.savepoint("inner"); err != nil {
		t.Fatal(err)
	}
	if err := tx.savepoint("outer"); err != nil {
		t.Fatal(err)
	}
	// Duplicate replace erased LIFO through the prior outer, so inner is gone.
	if err := tx.releaseSavepoint("inner"); !errors.Is(err, ErrSavepointNotFound) {
		t.Fatalf("release replaced inner = %v, want ErrSavepointNotFound", err)
	}
	if err := tx.releaseSavepoint("outer"); err != nil {
		t.Fatal(err)
	}
	if err := tx.releaseSavepoint("outer"); !errors.Is(err, ErrSavepointNotFound) {
		t.Fatalf("second release = %v, want ErrSavepointNotFound", err)
	}
}

func TestSavepointFailedStateRecoveryViaRollbackTo(t *testing.T) {
	ctx := context.Background()
	session := openSavepointSession(t)
	defer session.Close()
	prepared, err := session.Prepare(ctx, `CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	_ = prepared.Close()

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := session.Savepoint(ctx, "safe"); err != nil {
		t.Fatal(err)
	}
	bad, err := session.Prepare(ctx, `INSERT INTO missing VALUES (?)`)
	if err == nil {
		_ = bad.Close()
		t.Fatal("expected prepare of missing table to fail")
	}
	if session.State() != SessionFailedTransaction {
		t.Fatalf("state = %s, want failed", session.State())
	}
	if err := session.Savepoint(ctx, "x"); !errors.Is(err, ErrTransactionFailed) {
		t.Fatalf("SAVEPOINT in failed tx = %v, want ErrTransactionFailed", err)
	}
	if err := session.RollbackTo(ctx, "safe"); err != nil {
		t.Fatal(err)
	}
	if session.State() != SessionInTransaction {
		t.Fatalf("state after ROLLBACK TO = %s, want in transaction", session.State())
	}
	insert, err := session.Prepare(ctx, `INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := insert.Exec(ctx, []any{`{"id":"1"}`}); err != nil {
		t.Fatal(err)
	}
	_ = insert.Close()
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSavepointBoundRefusal(t *testing.T) {
	ctx := context.Background()
	session := openSavepointSession(t)
	defer session.Close()
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxSavepointFrames; i++ {
		name := fmt.Sprintf("sp%d", i)
		if err := session.Savepoint(ctx, name); err != nil {
			t.Fatalf("savepoint %d: %v", i, err)
		}
	}
	err := session.Savepoint(ctx, "overflow")
	if !errors.Is(err, ErrTooManySavepoints) {
		t.Fatalf("overflow = %v, want ErrTooManySavepoints", err)
	}
}

func TestSavepointMultiTableInteraction(t *testing.T) {
	db := openTestDB(t)
	for _, ddl := range []string{
		`CREATE TABLE a (PRIMARY KEY (id))`,
		`CREATE TABLE b (PRIMARY KEY (id))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO a VALUES (?)`, `{"id":"1","v":"a0"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SAVEPOINT s`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO b VALUES (?)`, `{"id":"1","v":"b1"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`UPDATE a SET "$doc" = ? WHERE id = ?`,
		`{"id":"1","v":"a1"}`, "1",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`ROLLBACK TO s`); err != nil {
		t.Fatal(err)
	}
	var bCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM b`).Scan(&bCount); err != nil {
		t.Fatal(err)
	}
	if bCount != 0 {
		t.Fatalf("b count after rollback = %d, want 0", bCount)
	}
	var got string
	if err := tx.QueryRow(`SELECT v FROM a WHERE id = ?`, "1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "a0" {
		t.Fatalf("a after rollback = %s, want a0", got)
	}
	if _, err := tx.Exec(`INSERT INTO b VALUES (?)`, `{"id":"1","v":"b2"}`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertTableField(t, db, "a", "1", "v", "a0")
	assertTableField(t, db, "b", "1", "v", "b2")
}

func openSavepointSession(t *testing.T) *Session {
	t.Helper()
	database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return session
}
