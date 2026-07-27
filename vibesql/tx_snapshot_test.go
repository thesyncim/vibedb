package vibesql

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

// A transaction's SELECT path must read the same overlay its DML planner uses.
// This covers both halves: a row absent from the begin snapshot and a
// replacement of a row present there.
func TestTransactionSelectReadsStagedInsertAndUpdate(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(2)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(
		`INSERT INTO users VALUES ('staged', {"id":201,"name":"inserted","tier":"staged"})`,
	); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	var name string
	if err := tx.QueryRow(`SELECT name FROM users WHERE id = 201`).Scan(&name); err != nil {
		t.Fatalf("SELECT staged INSERT: %v", err)
	}
	if name != "inserted" {
		t.Fatalf("staged INSERT name = %q, want inserted", name)
	}

	if _, err := tx.Exec(
		`UPDATE users SET "$doc" = ? WHERE "$key" = '1'`,
		[]byte(`{"id":1,"name":"updated","tier":"pro"}`),
	); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if err := tx.QueryRow(`SELECT name FROM users WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("SELECT staged UPDATE: %v", err)
	}
	if name != "updated" {
		t.Fatalf("staged UPDATE name = %q, want updated", name)
	}
}

// Begin, rather than the first statement, fixes the generation a transaction
// sees. A concurrent replacement must not change a repeated point read and a
// concurrent matching insert must not appear as a phantom.
func TestTransactionRepeatableReadsExcludeConcurrentPhantoms(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(4)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// Move the live collection before the transaction's first statement. The
	// transaction must still read the generation Begin retained, rather than
	// choosing a snapshot lazily here.
	if _, err := db.Exec(
		`UPDATE users SET "$doc" = ? WHERE "$key" = '1'`,
		[]byte(`{"id":1,"name":"outside-before-first-read","tier":"pro"}`),
	); err != nil {
		t.Fatalf("concurrent UPDATE before first read: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO users VALUES ('phantom-before-first-read', {"id":202,"name":"phantom","tier":"pro"})`,
	); err != nil {
		t.Fatalf("concurrent INSERT before first read: %v", err)
	}

	var beforeName string
	if err := tx.QueryRow(`SELECT name FROM users WHERE id = 1`).Scan(&beforeName); err != nil {
		t.Fatalf("first point read: %v", err)
	}
	var beforeCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM users WHERE tier = 'pro'`).Scan(&beforeCount); err != nil {
		t.Fatalf("first range read: %v", err)
	}
	if beforeName != "amy" || beforeCount != 3 {
		t.Fatalf("first read after concurrent commits = (%q,%d), want begin state (amy,3)",
			beforeName, beforeCount)
	}

	if _, err := db.Exec(
		`UPDATE users SET "$doc" = ? WHERE "$key" = '1'`,
		[]byte(`{"id":1,"name":"outside-after-first-read","tier":"pro"}`),
	); err != nil {
		t.Fatalf("concurrent UPDATE: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO users VALUES ('phantom-after-first-read', {"id":203,"name":"phantom","tier":"pro"})`,
	); err != nil {
		t.Fatalf("concurrent INSERT: %v", err)
	}

	var afterName string
	if err := tx.QueryRow(`SELECT name FROM users WHERE id = 1`).Scan(&afterName); err != nil {
		t.Fatalf("repeated point read: %v", err)
	}
	var afterCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM users WHERE tier = 'pro'`).Scan(&afterCount); err != nil {
		t.Fatalf("repeated range read: %v", err)
	}
	if afterName != beforeName {
		t.Fatalf("point read changed from %q to %q inside one transaction", beforeName, afterName)
	}
	if afterCount != beforeCount {
		t.Fatalf("range count changed from %d to %d inside one transaction", beforeCount, afterCount)
	}
}

// Rollback must discard the query-visible overlay as well as the durable write
// set; otherwise read-your-writes can accidentally turn into publication.
func TestTransactionRollbackDiscardsSelectedOverlay(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(2)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`INSERT INTO users VALUES ('rolled', {"id":301,"name":"visible-only-inside"})`,
	); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRow(`SELECT name FROM users WHERE id = 301`).Scan(&name); err != nil {
		t.Fatalf("SELECT before rollback: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := db.QueryRow(`SELECT name FROM users WHERE id = 301`).Scan(&name); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("SELECT after rollback = %v, want sql.ErrNoRows", err)
	}
}

// First-committer-wins is checked before any member of the batch is applied.
// The unrelated staged insert makes this an atomicity check as well as a
// same-key conflict check.
func TestTransactionConcurrentConflictPublishesNothing(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{MaxBatchDocuments: 64})
	db.SetMaxOpenConns(4)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`UPDATE users SET "$doc" = ? WHERE "$key" = '1'`,
		[]byte(`{"id":1,"name":"transaction"}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`INSERT INTO users VALUES ('must-not-publish', {"id":401})`,
	); err != nil {
		t.Fatal(err)
	}

	writerDone := make(chan error, 1)
	go func() {
		_, writeErr := db.Exec(
			`UPDATE users SET "$doc" = ? WHERE "$key" = '1'`,
			[]byte(`{"id":1,"name":"winner"}`),
		)
		writerDone <- writeErr
	}()
	if err := <-writerDone; err != nil {
		t.Fatalf("concurrent writer: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("Commit = %v, want ErrConflict", err)
	}

	var name string
	if err := db.QueryRow(`SELECT name FROM users WHERE id = 1`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "winner" {
		t.Fatalf("conflicting key = %q, want winner", name)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 401`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("a non-conflicting member of the rejected batch was published")
	}
}

// A one-slot lease table is an exact detector for leaked transaction
// snapshots: the next Begin fails immediately if Commit or Rollback retained
// the preceding lease.
func TestTransactionCommitAndRollbackReleaseSnapshot(t *testing.T) {
	db := writableDurable(t, "users", keyedCorpus(), durable.Options{
		MaxBatchDocuments: 64,
		MaxSnapshotLeases: 1,
	})
	db.SetMaxOpenConns(1)

	rolled, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := rolled.Rollback(); err != nil {
		t.Fatal(err)
	}

	committed, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin after Rollback: %v", err)
	}
	if _, err := committed.Exec(
		`UPDATE users SET "$doc" = ? WHERE "$key" = '1'`,
		[]byte(`{"id":1,"name":"committed"}`),
	); err != nil {
		t.Fatal(err)
	}
	if err := committed.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	after, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin after Commit: %v", err)
	}
	if err := after.Rollback(); err != nil {
		t.Fatal(err)
	}
}
