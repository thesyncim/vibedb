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

func TestDatabaseSQLCorrelatedLateralRefusalStaysTypedAndPositioned(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE accounts (id STRING PRIMARY KEY)`,
		`CREATE TABLE items (id STRING PRIMARY KEY, owner STRING)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := db.Prepare(correlatedLateralDriverSQL)
	if prepared != nil {
		prepared.Close()
		t.Fatal("database/sql prepared correlated LATERAL")
	}
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("database/sql error = %T %v, want typed feature refusal", err, err)
	}
	if want := strings.Index(correlatedLateralDriverSQL, "LATERAL"); unsupported.Pos != want {
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
