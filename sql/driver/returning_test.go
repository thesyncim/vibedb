package driver

import (
	"context"
	"slices"
	"testing"
)

func TestInsertReturningDatabaseSQL(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE users (
			id STRING PRIMARY KEY,
			name STRING NOT NULL,
			score INTEGER
		)`); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		INSERT INTO users (id, name, score)
		VALUES (?, ?, ?), (?, ?, ?)
		RETURNING id, name AS display_name`,
		"a", "Ada", int64(7), "b", "Grace", int64(9),
	)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := columns, []string{"id", "display_name"}; !slices.Equal(got, want) {
		t.Fatalf("RETURNING columns = %v, want %v", got, want)
	}
	var got [][2]string
	for rows.Next() {
		var row [2]string
		if err := rows.Scan(&row[0], &row[1]); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if want := [][2]string{{"a", "Ada"}, {"b", "Grace"}}; !slices.Equal(got, want) {
		t.Fatalf("RETURNING rows = %v, want %v", got, want)
	}

	var document []byte
	if err := db.QueryRow(
		`INSERT INTO users VALUES (?) RETURNING *`,
		`{"id":"c","name":"Lin","score":11}`,
	).Scan(&document); err != nil {
		t.Fatal(err)
	}
	if string(document) != `{"id":"c","name":"Lin","score":11}` {
		t.Fatalf("RETURNING * = %s", document)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM users`, 3)
}

func TestInsertReturningMustUseQueryAndFailureIsAtomic(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?) RETURNING id`,
		`{"id":"not-written"}`,
	); err == nil {
		t.Fatal("Exec accepted INSERT RETURNING")
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)

	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"taken"}`,
	); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(
		`INSERT INTO docs VALUES (?), (?) RETURNING id`,
		`{"id":"fresh"}`, `{"id":"taken"}`,
	)
	if err == nil {
		_ = rows.Close()
		t.Fatal("duplicate INSERT RETURNING succeeded")
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 1)
}

func TestInsertOnConflictDoNothing(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"a","value":"old"}`); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		INSERT INTO docs VALUES (?), (?), (?), (?)
		ON CONFLICT DO NOTHING RETURNING id`,
		`{"id":"a","value":"ignored"}`,
		`{"id":"b","value":"new"}`,
		`{"id":"b","value":"duplicate-in-batch"}`,
		`{"id":"c","value":"new"}`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("ON CONFLICT RETURNING rows = %v, want [b c]", got)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 3)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rows, err = tx.Query(`
		INSERT INTO docs VALUES (?), (?), (?)
		ON CONFLICT DO NOTHING RETURNING id`,
		`{"id":"a","value":"ignored"}`,
		`{"id":"d","value":"new"}`,
		`{"id":"d","value":"duplicate-in-transaction"}`,
	)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	got = got[:0]
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"d"}) {
		t.Fatalf("transactional ON CONFLICT RETURNING rows = %v, want [d]", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 4)
}

func TestSelectDistinctProjection(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, team STRING)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?), (?), (?)`,
		`{"id":"1","team":"red"}`,
		`{"id":"2","team":"blue"}`,
		`{"id":"3","team":"red"}`,
		`{"id":"4","team":"blue"}`,
	); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT DISTINCT team FROM docs ORDER BY team`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var team string
		if err := rows.Scan(&team); err != nil {
			t.Fatal(err)
		}
		got = append(got, team)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"blue", "red"}) {
		t.Fatalf("SELECT DISTINCT = %v, want [blue red]", got)
	}

	var limited []string
	rows, err = db.Query(`SELECT DISTINCT team FROM docs ORDER BY team LIMIT 1 OFFSET 1`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var team string
		if err := rows.Scan(&team); err != nil {
			t.Fatal(err)
		}
		limited = append(limited, team)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(limited, []string{"red"}) {
		t.Fatalf("SELECT DISTINCT LIMIT/OFFSET = %v, want [red]", limited)
	}
}

func TestInsertReturningTypedRuntimeTransaction(t *testing.T) {
	ctx := context.Background()
	database, err := Open(t.TempDir() + "/catalog.vdb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	session, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	create := runtimePrepare(t, session, `CREATE TABLE docs (id STRING PRIMARY KEY, n INTEGER)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session,
		`INSERT INTO docs VALUES (?), (?) RETURNING id, n`)
	if !insert.ReturnsRows() || insert.Kind().String() != "INSERT" {
		t.Fatalf("RETURNING metadata = (rows=%v kind=%s)", insert.ReturnsRows(), insert.Kind())
	}
	if got, want := insert.Columns(), []string{"id", "n"}; !slices.Equal(got, want) {
		t.Fatalf("RETURNING columns = %v, want %v", got, want)
	}
	cursor, err := insert.Query(ctx, []any{
		`{"id":"x","n":1}`, `{"id":"y","n":2}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"x", "y"} {
		if !cursor.Next() {
			t.Fatalf("missing RETURNING row %d", i)
		}
		if got, ok := cursor.Cell(0).Text(); !ok || got != want {
			t.Fatalf("RETURNING id %d = (%q, %v), want %q", i, got, ok, want)
		}
	}
	if cursor.Next() {
		t.Fatal("unexpected extra RETURNING row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := insert.Exec(ctx, []any{
		`{"id":"z","n":3}`, `{"id":"w","n":4}`,
	}); err == nil {
		t.Fatal("typed Exec accepted INSERT RETURNING")
	}
	if session.State() != SessionFailedTransaction {
		t.Fatalf("Exec misuse left transaction in state %s", session.State())
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	selectCount := runtimePrepare(t, session, `SELECT COUNT(*) FROM docs`)
	count, err := selectCount.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !count.Next() {
		t.Fatal("COUNT returned no row")
	}
	if got, ok := count.Cell(0).Int64(); !ok || got != 0 {
		t.Fatalf("rolled-back count = (%d, %v), want 0", got, ok)
	}
	if err := count.Close(); err != nil {
		t.Fatal(err)
	}
}
