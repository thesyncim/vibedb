package driver

import (
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"
)

func TestJSONNumberParametersRemainExact(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id NUMBER PRIMARY KEY,
			label STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}

	const wide = "9007199254740993"
	if _, err := db.Exec(
		`INSERT INTO docs (id, label) VALUES (?, ?)`,
		json.Number(wide), "exact",
	); err != nil {
		t.Fatal(err)
	}

	var label string
	if err := db.QueryRow(
		`SELECT label FROM docs WHERE id = ? LIMIT ?`,
		json.Number(wide), json.Number("1"),
	).Scan(&label); err != nil {
		t.Fatal(err)
	}
	if label != "exact" {
		t.Fatalf("label = %q, want exact", label)
	}
}

func TestEmptyTableMutationsStillValidateEveryBinding(t *testing.T) {
	run := func(t *testing.T, materialized bool) (updateErr, deleteErr error) {
		t.Helper()
		db := openTestDB(t)
		if _, err := db.Exec(
			`CREATE TABLE docs (id STRING PRIMARY KEY)`,
		); err != nil {
			t.Fatal(err)
		}
		if materialized {
			if _, err := db.Exec(
				`INSERT INTO docs VALUES (?)`, `{"id":"once"}`,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(
				`DELETE FROM docs WHERE id = ?`, "once",
			); err != nil {
				t.Fatal(err)
			}
		}

		_, updateErr = db.Exec(
			`UPDATE docs SET "$doc" = ? WHERE id = ?`,
			int64(1), "missing",
		)
		_, deleteErr = db.Exec(
			`DELETE FROM docs WHERE id = ?`, time.Unix(0, 0),
		)
		return updateErr, deleteErr
	}

	neverUpdate, neverDelete := run(t, false)
	materializedUpdate, materializedDelete := run(t, true)
	for _, test := range []struct {
		name         string
		never        error
		materialized error
		contains     string
	}{
		{
			name: "replacement document", never: neverUpdate,
			materialized: materializedUpdate, contains: "bound to int64",
		},
		{
			name: "predicate", never: neverDelete,
			materialized: materializedDelete, contains: "time.Time",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.never == nil || test.materialized == nil {
				t.Fatalf(
					"errors before/after materialization = (%v, %v), want two binding errors",
					test.never, test.materialized,
				)
			}
			if !strings.Contains(test.never.Error(), test.contains) ||
				!strings.Contains(test.materialized.Error(), test.contains) {
				t.Fatalf(
					"errors before/after materialization = (%v, %v), want %q",
					test.never, test.materialized, test.contains,
				)
			}
		})
	}
}

func TestRawMessageDocumentParameterUsesDatabaseSQLBytes(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	document := json.RawMessage(`{"id":"raw","n":9007199254740993}`)
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, document); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := db.QueryRow(`SELECT * FROM docs WHERE id = ?`, "raw").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(document) {
		t.Fatalf("document = %s, want %s", raw, document)
	}
}

func TestNamedParametersAreRejected(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	_, err := db.Query(`SELECT * FROM docs WHERE id = ?`, stdsql.Named("id", "x"))
	if err == nil || !strings.Contains(err.Error(), "named parameters") {
		t.Fatalf("named parameter error = %v", err)
	}
}

func TestInvalidUTF8IsRejectedBeforeJSONEncoding(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, label STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	invalid := []byte{0xff}
	for _, value := range []any{string(invalid), invalid} {
		if _, err := db.Exec(
			`INSERT INTO docs (id, label) VALUES (?, ?)`,
			value, "must-not-be-written",
		); err == nil || !strings.Contains(err.Error(), "valid UTF-8") {
			t.Fatalf("flat INSERT of %T invalid UTF-8 = %v", value, err)
		}
	}
	if _, err := db.Prepare("SELECT " + string(invalid) + " FROM docs"); err == nil ||
		!strings.Contains(err.Error(), "SQL text must be valid UTF-8") {
		t.Fatalf("invalid UTF-8 SQL error = %v", err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)
}

func TestNonFiniteNumericParametersAreRejected(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs (id, n) VALUES (?, ?)`, "zero", 0,
	); err != nil {
		t.Fatal(err)
	}
	for _, value := range []float64{
		math.NaN(), math.Inf(1), math.Inf(-1),
	} {
		if _, err := db.Query(
			`SELECT * FROM docs WHERE n = ?`, value,
		); err == nil || !strings.Contains(err.Error(), "finite JSON numbers") {
			t.Errorf("predicate parameter %v = %v, want finite-number rejection", value, err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, n) VALUES (?, ?)`, "bad", value,
		); err == nil || !strings.Contains(err.Error(), "finite JSON numbers") {
			t.Errorf("INSERT parameter %v = %v, want finite-number rejection", value, err)
		}
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 1)
}

func TestAggregateArgumentPayloadIsBounded(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	wide := strings.Repeat("x", maxSQLParameterBytes)
	_, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?), (?), (?), (?)`,
		wide, wide, wide, wide, wide,
	)
	if !errors.Is(err, ErrArgumentsTooLarge) {
		t.Fatalf("20 MiB argument payload = %v, want ErrArgumentsTooLarge", err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)
}

func TestConnectionClearsBorrowedArgumentsAfterExecution(t *testing.T) {
	connection := directTestConn(t).(*conn)
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`, nil)
	insert, err := connection.Prepare(`INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Close()
	if _, err := insert.Exec([]sqldriver.Value{`{"id":"kept"}`}); err != nil {
		t.Fatal(err)
	}
	assertClearedArguments(t, connection)

	selectRow, err := connection.Prepare(
		`SELECT id FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer selectRow.Close()
	rows, err := selectRow.Query([]sqldriver.Value{"kept"})
	if err != nil {
		t.Fatal(err)
	}
	assertClearedArguments(t, connection)
	values := make([]sqldriver.Value, 1)
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	if values[0] == nil {
		t.Fatal("clearing arguments invalidated the query result")
	}
	if err := rows.Next(values); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next = %v, want EOF", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNamedArgumentOrdinalsMustBeContiguous(t *testing.T) {
	connection := &conn{}
	_, err := connection.values([]sqldriver.NamedValue{
		{Ordinal: 1, Value: "must-not-be-retained"},
		{Ordinal: 1, Value: "duplicate"},
	})
	if err == nil || !strings.Contains(err.Error(), "contiguous") {
		t.Fatalf("duplicate ordinals = %v, want contiguous-order rejection", err)
	}
	assertClearedArguments(t, connection)
}

func TestOversizedPointKeyDoesNotGrowConnectionArena(t *testing.T) {
	connection := directTestConn(t).(*conn)
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`, nil)
	directExec(t, connection, `INSERT INTO docs VALUES (?)`,
		[]sqldriver.NamedValue{{
			Ordinal: 1, Value: `{"id":"kept"}`,
		}})
	statement, err := connection.Prepare(
		`SELECT id FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()

	impossible := strings.Repeat("x", maxSQLParameterBytes)
	rows, err := statement.Query([]sqldriver.Value{impossible})
	if err != nil {
		t.Fatal(err)
	}
	values := make([]sqldriver.Value, 1)
	if err := rows.Next(values); !errors.Is(err, io.EOF) {
		t.Fatalf("oversized point probe Next = %v, want EOF", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	maxKeyBytes := connection.db.tables["docs"].collection.MaxKeyBytes()
	if cap(connection.pointKeyRaw) > maxKeyBytes {
		t.Fatalf(
			"oversized probe retained %d key bytes, table maximum is %d",
			cap(connection.pointKeyRaw), maxKeyBytes,
		)
	}
	assertClearedArguments(t, connection)
}

func assertClearedArguments(t *testing.T, connection *conn) {
	t.Helper()
	for i, value := range connection.args {
		if value != nil {
			t.Fatalf("connection argument %d retained %T after execution", i, value)
		}
	}
}
