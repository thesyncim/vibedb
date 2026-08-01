package driver

import (
	stdsql "database/sql"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func seedLateralGroupDriver(t testing.TB, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE lateral_group_accounts (id STRING PRIMARY KEY, value NUMBER)`,
		`CREATE TABLE lateral_group_items (id STRING PRIMARY KEY, owner STRING)`,
		`INSERT INTO lateral_group_accounts VALUES ` +
			`('{"id":"a","value":0.1}'),('{"id":"b","value":2}')`,
		`INSERT INTO lateral_group_items VALUES ` +
			`('{"id":"i1","owner":"a"}'),('{"id":"i2","owner":"a"}'),` +
			`('{"id":"i3","owner":"b"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func TestDatabaseSQLLateralGroupedAggregatePreparedTransactionReuse(t *testing.T) {
	db := openTestDB(t)
	seedLateralGroupDriver(t, db)
	prepared, err := db.Prepare(`
		SELECT a.id, d.total FROM lateral_group_accounts a LEFT JOIN LATERAL (
			SELECT SUM(a.value) AS total FROM lateral_group_items i
			WHERE i.owner = a.id GROUP BY a.id
			HAVING SUM(a.value) >= ?
		) d ON TRUE ORDER BY a.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	read := func(q interface {
		Query(...any) (*stdsql.Rows, error)
	}) []string {
		rows, err := q.Query(query.Number("0.2"))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"id", "d.total"}; !slices.Equal(columns, want) {
			t.Fatalf("columns = %v, want %v", columns, want)
		}
		var got []string
		for rows.Next() {
			var id string
			var total []byte
			if err := rows.Scan(&id, &total); err != nil {
				t.Fatal(err)
			}
			got = append(got, id+":"+string(total))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got, want := read(prepared), []string{"a:0.2", "b:2"}; !slices.Equal(got, want) {
		t.Fatalf("prepared grouped LATERAL = %v, want %v", got, want)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO lateral_group_items VALUES ` +
		`('{"id":"pending","owner":"a"}')`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	txPrepared := tx.Stmt(prepared)
	if got, want := read(txPrepared), []string{"a:0.3", "b:2"}; !slices.Equal(got, want) {
		_ = tx.Rollback()
		t.Fatalf("transaction grouped LATERAL = %v, want %v", got, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got, want := read(prepared), []string{"a:0.2", "b:2"}; !slices.Equal(got, want) {
		t.Fatalf("post-rollback grouped LATERAL = %v, want %v", got, want)
	}
}
