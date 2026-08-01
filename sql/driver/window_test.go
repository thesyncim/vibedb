package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func seedWindowDriverTable(t testing.TB, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE events (` +
			`id STRING PRIMARY KEY, team STRING, score NUMBER, value NUMBER)`,
		`INSERT INTO events VALUES ` +
			`('{"id":"a","team":"x","score":1,"value":0.1}'),` +
			`('{"id":"b","team":"x","score":1.0,"value":0.20}'),` +
			`('{"id":"c","team":"x","score":2,"value":1}'),` +
			`('{"id":"d","team":"y","score":1,"value":7}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func TestDatabaseSQLWindowPeerDefaultsExactDecimalsAndSchema(t *testing.T) {
	db := openTestDB(t)
	seedWindowDriverTable(t, db)

	rows, err := db.Query(`
		SELECT id,
			SUM(value) OVER (PARTITION BY team ORDER BY score) AS peer_sum
		FROM events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id string
		var sum []byte
		if err := rows.Scan(&id, &sum); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		got = append(got, id+":"+string(sum))
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"a:0.3", "b:0.3", "c:1.3", "d:7"}; !slices.Equal(got, want) {
		t.Fatalf("peer/default exact sums = %v, want %v", got, want)
	}

	prepared, err := db.Prepare(`
		SELECT id,
			LAG(value, ?, NULL) OVER (PARTITION BY team ORDER BY score) AS previous,
			NTILE(?) OVER (PARTITION BY team ORDER BY score) AS tile,
			SUM(value) OVER (
				PARTITION BY team ORDER BY score
				ROWS BETWEEN ? PRECEDING AND CURRENT ROW
			) AS rolling
		FROM events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	rows, err = prepared.Query(int64(1), int64(2), int64(1))
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"id", "previous", "tile", "rolling"}; !slices.Equal(columns, want) {
		t.Fatalf("window columns = %v, want %v", columns, want)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := []string{
		types[0].DatabaseTypeName(), types[1].DatabaseTypeName(),
		types[2].DatabaseTypeName(), types[3].DatabaseTypeName(),
	}, []string{"JSON", "JSON", "BIGINT", "NUMERIC"}; !slices.Equal(got, want) {
		t.Fatalf("window database types = %v, want %v", got, want)
	}
	if types[2].ScanType() != reflect.TypeFor[int64]() {
		t.Fatalf("NTILE scan type = %v, want int64", types[2].ScanType())
	}
	if nullable, ok := types[2].Nullable(); !ok || nullable {
		t.Fatalf("NTILE nullable = %v/%v, want false/true", nullable, ok)
	}
	if nullable, ok := types[3].Nullable(); !ok || !nullable {
		t.Fatalf("rolling SUM nullable = %v/%v, want true/true", nullable, ok)
	}
	var firstID string
	var previous any
	var tile int64
	var rolling []byte
	if !rows.Next() {
		t.Fatal("prepared window returned no row")
	}
	if err := rows.Scan(&firstID, &previous, &tile, &rolling); err != nil {
		t.Fatal(err)
	}
	if firstID != "a" || previous != nil || tile != 1 || string(rolling) != "0.1" {
		t.Fatalf("first prepared row = %q/%v/%d/%q", firstID, previous, tile, rolling)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	failed, err := prepared.Query(int64(1), int64(0), int64(1))
	if failed != nil {
		_ = failed.Close()
	}
	if !errors.Is(err, query.ErrWindowArgument) {
		t.Fatalf("zero NTILE bind = %v, want ErrWindowArgument", err)
	}
	if err := prepared.QueryRow(int64(1), int64(2), int64(0)).Scan(
		&firstID, &previous, &tile, &rolling,
	); err != nil {
		t.Fatalf("prepared window did not recover after bind error: %v", err)
	}
}

func TestDatabaseSQLWindowPreparedTransactionReuse(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(2)
	seedWindowDriverTable(t, db)
	prepared, err := db.Prepare(`
		SELECT id, ROW_NUMBER() OVER (PARTITION BY team ORDER BY score) AS position
		FROM events WHERE team = ? ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO events VALUES (?)`,
		`{"id":"pending","team":"x","score":3,"value":2}`,
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	rows, err := tx.Stmt(prepared).Query("x")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if got := countWindowDriverRows(t, rows); got != 4 {
		_ = tx.Rollback()
		t.Fatalf("transaction window rows = %d, want 4", got)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	rows, err = prepared.Query("x")
	if err != nil {
		t.Fatal(err)
	}
	if got := countWindowDriverRows(t, rows); got != 3 {
		t.Fatalf("prepared window rows after rollback = %d, want 3", got)
	}
}

func TestDriverWindowBudgetCancellationNoPartialRowsAndRecovery(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE events (id STRING PRIMARY KEY, score INTEGER)`,
		`INSERT INTO events VALUES (` +
			`'{"id":"a","score":1}'), ('{"id":"b","score":2}')`,
	} {
		prepared := runtimePrepare(t, session, statement)
		if _, err := prepared.Exec(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	prepared := runtimePrepare(t, session,
		`SELECT id, ROW_NUMBER() OVER (ORDER BY score) AS position FROM events`)
	if err := session.SetIntermediateLimit(1); err != nil {
		t.Fatal(err)
	}
	cursor, err := prepared.Query(ctx, nil)
	if cursor != nil {
		_ = cursor.Close()
		t.Fatal("window budget failure published a partial cursor")
	}
	if !errors.Is(err, query.ErrIntermediateBudget) {
		t.Fatalf("window budget error = %v, want ErrIntermediateBudget", err)
	}
	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatal(err)
	}
	var cancel query.CancelFlag
	if err := session.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	cancel.Cancel()
	cursor, err = prepared.Query(ctx, nil)
	if cursor != nil {
		_ = cursor.Close()
		t.Fatal("canceled window published a partial cursor")
	}
	if !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("canceled window = %v, want ErrCanceled", err)
	}
	cancel.Reset()
	cursor, err = prepared.Query(ctx, nil)
	if err != nil {
		t.Fatalf("window after cancellation: %v", err)
	}
	if !cursor.Next() {
		t.Fatal("window after cancellation returned no row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
}

func countWindowDriverRows(t testing.TB, rows *stdsql.Rows) int {
	t.Helper()
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id string
		var position int64
		if err := rows.Scan(&id, &position); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return count
}
