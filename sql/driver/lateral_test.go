package driver

import (
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const correlatedLateralDriverSQL = `SELECT a.id, d.id
FROM accounts a
CROSS JOIN LATERAL (
	SELECT i.id FROM items i WHERE i.owner = a.id
) d`

func TestDatabaseSQLCorrelatedLateralExecutesAndRemainingRefusalStaysTyped(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE accounts (id STRING PRIMARY KEY)`,
		`CREATE TABLE items (id STRING PRIMARY KEY, owner STRING)`,
		`INSERT INTO accounts VALUES ({"id":"a1"})`,
		`INSERT INTO items VALUES ({"id":"i1","owner":"a1"})`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := db.Prepare(correlatedLateralDriverSQL)
	if err != nil {
		t.Fatalf("prepare correlated LATERAL: %v", err)
	}
	defer prepared.Close()
	var account, item string
	if err := prepared.QueryRow().Scan(&account, &item); err != nil {
		t.Fatalf("execute correlated LATERAL: %v", err)
	}
	if account != "a1" || item != "i1" {
		t.Fatalf("correlated LATERAL row = %q/%q, want a1/i1", account, item)
	}

	unsupportedSQL := `SELECT a.id, d.id FROM accounts a JOIN LATERAL (` +
		`SELECT i.id, i.owner FROM items i WHERE i.owner = a.id) d USING (id)`
	unsupportedStatement, err := db.Prepare(unsupportedSQL)
	if unsupportedStatement != nil {
		unsupportedStatement.Close()
		t.Fatal("database/sql prepared unsupported JOIN LATERAL USING")
	}
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("database/sql error = %T %v, want typed feature refusal", err, err)
	}
	if want := strings.Index(unsupportedSQL, "USING"); unsupported.Pos != want {
		t.Fatalf("database/sql refusal position = %d, want %d", unsupported.Pos, want)
	}

	decorrelated, err := db.Prepare(`SELECT a.id, d.id FROM accounts a ` +
		`CROSS JOIN LATERAL (SELECT i.id FROM items i) d`)
	if err != nil {
		t.Fatalf("prepare decorrelated LATERAL: %v", err)
	}
	if err := decorrelated.Close(); err != nil {
		t.Fatal(err)
	}
}
