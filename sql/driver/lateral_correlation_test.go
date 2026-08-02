package driver

import (
	stdsql "database/sql"
	"slices"
	"testing"
)

const inheritedLateralDriverSQL = `
	SELECT a.id, q.id FROM lateral_frame_accounts a CROSS JOIN LATERAL (
		SELECT d.id AS id
		FROM lateral_frame_items i CROSS JOIN LATERAL (
			SELECT x.id FROM lateral_frame_items x
			WHERE x.owner = a.id AND x.active = i.active AND x.active = ?
		) d
		WHERE i.owner = a.id
	) q ORDER BY a.id`

func seedInheritedLateralDriver(t testing.TB, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE lateral_frame_accounts (id STRING PRIMARY KEY)`,
		`CREATE TABLE lateral_frame_items (` +
			`id STRING PRIMARY KEY, owner STRING, active BOOL)`,
		`INSERT INTO lateral_frame_accounts VALUES ` +
			`('{"id":"a1"}'),('{"id":"a2"}')`,
		`INSERT INTO lateral_frame_items VALUES ` +
			`('{"id":"i1","owner":"a1","active":true}'),` +
			`('{"id":"i2","owner":"a1","active":false}'),` +
			`('{"id":"i3","owner":"a2","active":false}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func TestDatabaseSQLInheritedLateralPreparedTransactionReuse(t *testing.T) {
	db := openTestDB(t)
	seedInheritedLateralDriver(t, db)
	prepared, err := db.Prepare(inheritedLateralDriverSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	read := func(q interface {
		Query(...any) (*stdsql.Rows, error)
	}, active bool) []string {
		rows, err := q.Query(active)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"id", "q.id"}; !slices.Equal(columns, want) {
			t.Fatalf("inherited LATERAL columns = %v, want %v", columns, want)
		}
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
	if got, want := read(prepared, true), []string{"a1:i1"}; !slices.Equal(got, want) {
		t.Fatalf("prepared inherited LATERAL = %v, want %v", got, want)
	}
	if got, want := read(prepared, false), []string{"a1:i2", "a2:i3"}; !slices.Equal(got, want) {
		t.Fatalf("rebound inherited LATERAL = %v, want %v", got, want)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO lateral_frame_items VALUES ` +
		`('{"id":"i4","owner":"a2","active":true}')`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if got, want := read(tx.Stmt(prepared), true), []string{"a1:i1", "a2:i4"}; !slices.Equal(got, want) {
		_ = tx.Rollback()
		t.Fatalf("transaction inherited LATERAL = %v, want %v", got, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got, want := read(prepared, true), []string{"a1:i1"}; !slices.Equal(got, want) {
		t.Fatalf("post-rollback inherited LATERAL = %v, want %v", got, want)
	}
}
