package driver

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestDatabaseSQLScalarDirectPreparedMetadataTransactionAndRecovery(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE scalar_docs (id STRING PRIMARY KEY, n NUMBER, s STRING)`,
		`INSERT INTO scalar_docs VALUES ('{"id":"a","n":9007199254740993,"s":"x"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	rows, err := db.Query(`SELECT n + 1 AS exact, s || '!' AS label FROM scalar_docs`)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(types) != 2 || types[0].DatabaseTypeName() != "NUMERIC" ||
		types[1].DatabaseTypeName() != "TEXT" {
		t.Fatalf("scalar database/sql types = %v/%v", types[0].DatabaseTypeName(), types[1].DatabaseTypeName())
	}
	if !rows.Next() {
		t.Fatal("missing direct scalar row")
	}
	var exact, label string
	if err := rows.Scan(&exact, &label); err != nil {
		t.Fatal(err)
	}
	if exact != "9.007199254740994e15" || label != "x!" {
		t.Fatalf("direct scalar row = %q/%q", exact, label)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	prepared, err := db.Prepare(`SELECT n / ? AS quotient FROM scalar_docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err := prepared.QueryRow(int64(0), "a").Scan(&exact); !errors.Is(err, query.ErrScalarDivisionByZero) {
		t.Fatalf("prepared division error = %T %v", err, err)
	}
	if err := prepared.QueryRow(int64(1), "a").Scan(&exact); err != nil {
		t.Fatalf("prepared recovery: %v", err)
	}
	if exact != "9.007199254740993e15" {
		t.Fatalf("prepared exact quotient = %q", exact)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO scalar_docs VALUES ('{"id":"b","n":4,"s":"tx"}')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`SELECT n * 2, s || '!' FROM scalar_docs WHERE id = 'b'`).Scan(&exact, &label); err != nil {
		t.Fatalf("transaction read-your-writes: %v", err)
	}
	if exact != "8" || label != "tx!" {
		t.Fatalf("transaction scalar row = %q/%q", exact, label)
	}
}
