package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const lateralCorrelationSlotsDriverSQL = `
	SELECT a.id, q.id FROM lateral_slot_accounts a CROSS JOIN LATERAL (
		SELECT d.id AS id
		FROM lateral_slot_items i CROSS JOIN LATERAL (
			SELECT x.id FROM lateral_slot_items x
			WHERE x.owner = a.id AND x.active = i.active AND x.active = ?
		) d
		WHERE i.owner = a.id
	) q ORDER BY a.id, q.id`

func seedLateralCorrelationSlotsDriver(t testing.TB, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE lateral_slot_accounts (id STRING PRIMARY KEY)`,
		`CREATE TABLE lateral_slot_items (` +
			`id STRING PRIMARY KEY, owner STRING, active BOOL)`,
		`INSERT INTO lateral_slot_accounts VALUES ` +
			`('{"id":"a1"}'),('{"id":"a2"}')`,
		`INSERT INTO lateral_slot_items VALUES ` +
			`('{"id":"i1","owner":"a1","active":true}'),` +
			`('{"id":"i2","owner":"a1","active":false}'),` +
			`('{"id":"i3","owner":"a2","active":false}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func readLateralCorrelationSlotRows(
	t testing.TB,
	statement interface {
		Query(args ...any) (*stdsql.Rows, error)
	},
	active bool,
) []string {
	t.Helper()
	rows, err := statement.Query(active)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var account, item string
		if err := rows.Scan(&account, &item); err != nil {
			t.Fatal(err)
		}
		got = append(got, account+":"+item)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestDatabaseSQLLateralCorrelationSlotsPreparedSnapshotAndRecovery(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	seedLateralCorrelationSlotsDriver(t, db)

	prepared, err := db.Prepare(lateralCorrelationSlotsDriverSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	for _, run := range []struct {
		active bool
		want   []string
	}{
		{true, []string{"a1:i1"}},
		{false, []string{"a1:i2", "a2:i3"}},
		{true, []string{"a1:i1"}},
	} {
		if got := readLateralCorrelationSlotRows(t, prepared, run.active); !slices.Equal(got, run.want) {
			t.Fatalf("prepared active=%t rows = %v, want %v", run.active, got, run.want)
		}
	}

	unsupportedSQL := `SELECT a.id, d.id FROM lateral_slot_accounts a JOIN LATERAL (` +
		`SELECT i.id, i.owner FROM lateral_slot_items i WHERE i.owner = a.id) d USING (id)`
	unsupportedStatement, err := db.Prepare(unsupportedSQL)
	if unsupportedStatement != nil {
		_ = unsupportedStatement.Close()
		t.Fatal("unsupported JOIN LATERAL USING prepared")
	}
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("refusal = %T %v, want FeatureNotSupportedError", err, err)
	}
	if want := strings.Index(unsupportedSQL, "USING"); unsupported.Pos != want {
		t.Fatalf("refusal position = %d, want %d", unsupported.Pos, want)
	}
	if got := readLateralCorrelationSlotRows(t, prepared, false); !slices.Equal(got, []string{"a1:i2", "a2:i3"}) {
		t.Fatalf("post-refusal prepared rows = %v", got)
	}

	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`INSERT INTO lateral_slot_items VALUES (` +
		`'{"id":"i4","owner":"a2","active":true}')`); err != nil {
		t.Fatal(err)
	}
	txPrepared := tx.Stmt(prepared)
	defer txPrepared.Close()
	if got := readLateralCorrelationSlotRows(t, txPrepared, true); !slices.Equal(got, []string{"a1:i1"}) {
		t.Fatalf("BEGIN snapshot rows = %v, want [a1:i1]", got)
	}
	if _, err := tx.Exec(`INSERT INTO lateral_slot_items VALUES (` +
		`'{"id":"i5","owner":"a2","active":true}')`); err != nil {
		t.Fatal(err)
	}
	if got := readLateralCorrelationSlotRows(t, txPrepared, true); !slices.Equal(got, []string{"a1:i1", "a2:i5"}) {
		t.Fatalf("transaction read-your-writes rows = %v, want [a1:i1 a2:i5]", got)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := readLateralCorrelationSlotRows(t, prepared, true); !slices.Equal(got, []string{"a1:i1", "a2:i4"}) {
		t.Fatalf("post-rollback autocommit rows = %v, want [a1:i1 a2:i4]", got)
	}
}
