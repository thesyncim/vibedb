package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

// Ported from vibesql/tx_snapshot_test.go
// TestTransactionSelectReadsStagedInsertAndUpdate.
func TestTransactionSelectReadsStagedInsertAndUpdate(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","name":"before"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"new","name":"inserted"}`); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRow(`SELECT name FROM docs WHERE id = ?`, "new").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "inserted" {
		t.Fatalf("staged INSERT name = %q", name)
	}
	if _, err := tx.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","name":"updated"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`SELECT name FROM docs WHERE id = ?`, "base").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "updated" {
		t.Fatalf("staged UPDATE name = %q", name)
	}
}

// Ported from vibesql/tx_snapshot_test.go
// TestTransactionRepeatableReadsExcludeConcurrentPhantoms.
func TestTransactionRepeatableReadsExcludeConcurrentPhantoms(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","kind":"x","name":"before"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","kind":"x","name":"outside"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"phantom","kind":"x"}`); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRow(`SELECT name FROM docs WHERE id = ?`, "base").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "before" {
		t.Fatalf("point read = %q, want begin snapshot", name)
	}
	var count int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("transaction count = %d, want 1", count)
	}
}

// Ported from vibesql/tx_snapshot_test.go
// TestTransactionConcurrentConflictPublishesNothing.
func TestTransactionConcurrentConflictPublishesNothing(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","writer":"initial"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"tx"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"atomic"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"winner"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("Commit = %v, want ErrTransactionConflict", err)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE id = ?`, "atomic").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("non-conflicting member of rejected batch was published")
	}
}

// Ported from vibesql/write_test.go TestTransactionConflictAbortsTheCommit,
// using two transactions opened concurrently from one database/sql handle.
func TestConcurrentTransactionsOnOneHandle(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"base","writer":"initial"}`); err != nil {
		t.Fatal(err)
	}
	first, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"first"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"base","writer":"second"}`, "base"); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("second Commit = %v, want ErrTransactionConflict", err)
	}
}

// Ported from vibesql/tx_snapshot_test.go
// TestTransactionRollbackDiscardsSelectedOverlay.
func TestTransactionRollbackDiscardsSelectedOverlay(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"rolled","name":"inside"}`); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRow(`SELECT name FROM docs WHERE id = ?`, "rolled").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT name FROM docs WHERE id = ?`, "rolled").Scan(&name); !errors.Is(err, stdsql.ErrNoRows) {
		t.Fatalf("read after rollback = %v, want sql.ErrNoRows", err)
	}
}

// Ported from vibesql/tx_snapshot_test.go
// TestTransactionCommitAndRollbackReleaseSnapshot.
func TestTransactionCommitAndRollbackReleaseSnapshot(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed"}`); err != nil {
		t.Fatal(err)
	}
	rolled, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := rolled.Rollback(); err != nil {
		t.Fatal(err)
	}
	committed, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := committed.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"committed"}`); err != nil {
		t.Fatal(err)
	}
	if err := committed.Commit(); err != nil {
		t.Fatal(err)
	}
	after, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := after.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Ported from vibesql/write_test.go
// TestTransactionRefusesAnUnsupportedIsolationLevel.
func TestTransactionIsolationLevelContract(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginTx(context.Background(),
		&stdsql.TxOptions{Isolation: stdsql.LevelSerializable}); err == nil {
		t.Fatal("LevelSerializable was accepted")
	}
	tx, err := db.BeginTx(context.Background(),
		&stdsql.TxOptions{Isolation: stdsql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Ported from vibesql/write_test.go
// TestTransactionRefusesToExceedTheBatchBound.
func TestTransactionRefusesToExceedTheBatchBound(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < 64; i++ {
		if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`,
			fmt.Sprintf(`{"id":"tx-%02d"}`, i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	_, err = tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"overflow"}`)
	if !errors.Is(err, ErrTransactionTooLarge) || !errors.Is(err, durable.ErrBatchTooLarge) {
		t.Fatalf("overflow = %v, want typed driver and durable batch errors", err)
	}
}

// Ported from vibesql/driver_test.go TestDriverPreparedMatchesAdHoc, extended
// with the prepared-statement lifecycle across transactions.
func TestPreparedStatementReuseAcrossTransactions(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"a","n":1}`); err != nil {
		t.Fatal(err)
	}
	prepared, err := db.Prepare(`SELECT n FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for i := 0; i < 2; i++ {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := tx.Stmt(prepared).QueryRow("a").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("transaction %d read %d", i, n)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

// Ported from vibesql's stmt lifecycle contract in stmt.go.
func TestStatementAfterClose(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	statement, err := db.Prepare(`SELECT * FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := statement.Query(); err == nil {
		t.Fatal("Query after statement Close succeeded")
	}
}

// Ported from vibesql/driver_test.go TestDriverBigIntegerSurvivesTheRoundTrip
// and TestDriverValueMapping, adapted to the document-derived INSERT dialect.
func TestDriverValueRoundTrips(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	raw := `{"id":"values","nil":null,"bool":true,"int":9007199254740993,"float":0.1,"string":"hi","array":[1,2]}`
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, raw); err != nil {
		t.Fatal(err)
	}
	var nilValue any
	var boolean bool
	var integer int64
	var decimal, text, array []byte
	if err := db.QueryRow(
		`SELECT nil, bool, int, float, string, array FROM docs WHERE id = ?`, "values",
	).Scan(&nilValue, &boolean, &integer, &decimal, &text, &array); err != nil {
		t.Fatal(err)
	}
	if nilValue != nil || !boolean || integer != 9007199254740993 ||
		string(decimal) != "0.1" || string(text) != "hi" || string(array) != "[1,2]" {
		t.Fatalf("round trip = %#v %v %d %q %q %q",
			nilValue, boolean, integer, decimal, text, array)
	}
}

func TestTransactionIndexedTableIsTypedEngineGate(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX ON docs(kind)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed","kind":"x"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"later","kind":"x"}`)
	if !errors.Is(err, ErrTransactionIndexedTable) ||
		!errors.Is(err, durable.ErrPrimaryBatchIndexedUnsupported) {
		t.Fatalf("indexed transaction = %v, want typed SQL and engine gates", err)
	}
}
