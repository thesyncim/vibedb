package driver

import (
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestSQLDriverSharedSelectSurface(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE metrics (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO metrics VALUES (?), (?), (?), (?), (?)`,
		`{"id":"a","team":"red","active":true,"n":1,"note":null}`,
		`{"id":"b","team":"red","active":true,"n":3}`,
		`{"id":"c","team":"blue","active":false,"n":5,"note":"set"}`,
		`{"id":"d","team":"blue","active":true,"n":7,"note":null}`,
		`{"id":"e","team":"green","active":true,"n":9}`,
	); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		SELECT id
		FROM metrics
		WHERE active = TRUE AND n >= ? AND n < ?
		ORDER BY n DESC
		LIMIT ? OFFSET ?`,
		int64(1), int64(9), int64(2), int64(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	var page []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		page = append(page, id)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0] != "b" || page[1] != "a" {
		t.Fatalf("range/boolean ordered page = %v, want [b a]", page)
	}

	assertSurfaceIDs(t, db,
		`SELECT id FROM metrics WHERE note IS NULL ORDER BY id`,
		[]string{"a", "b", "d", "e"})
	assertSurfaceIDs(t, db,
		`SELECT id FROM metrics WHERE note IS MISSING ORDER BY id`,
		[]string{"b", "e"})
	assertSurfaceIDs(t, db,
		`SELECT id FROM metrics WHERE note IS NULL AND note IS NOT MISSING ORDER BY id`,
		[]string{"a", "d"})

	var (
		team                      string
		count, sum, avg, min, max int64
	)
	if err := db.QueryRow(`
		SELECT team, COUNT(*), SUM(n), AVG(n), MIN(n), MAX(n)
		FROM metrics
		GROUP BY team
		HAVING SUM(n) > ?
		ORDER BY team`,
		int64(9),
	).Scan(&team, &count, &sum, &avg, &min, &max); err != nil {
		t.Fatal(err)
	}
	if team != "blue" || count != 2 || sum != 12 || avg != 6 || min != 5 || max != 7 {
		t.Fatalf(
			"aggregate row = (%q,%d,%d,%d,%d,%d), want (blue,2,12,6,5,7)",
			team, count, sum, avg, min, max)
	}
}

func assertSurfaceIDs(t *testing.T, db *stdsql.DB, statement string, want []string) {
	t.Helper()
	rows, err := db.Query(statement)
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
	if len(got) != len(want) {
		t.Fatalf("%s: ids = %v, want %v", statement, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: ids = %v, want %v", statement, got, want)
		}
	}
}

func TestSQLDriverRejectsMultipleStatementsBeforeExecution(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}

	_, err := db.Exec(
		`INSERT INTO docs VALUES (?); INSERT INTO docs VALUES (?)`,
		`{"id":"first"}`, `{"id":"second"}`,
	)
	if err == nil || !strings.Contains(err.Error(), "only one statement") {
		t.Fatalf("multiple-statement error = %v, want one-statement rejection", err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)
}

func TestSQLDriverRejectsArgumentCountMismatchBeforeExecution(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`); err == nil {
		t.Fatal("INSERT with a missing argument succeeded")
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":"first"}`, "surplus",
	); err == nil {
		t.Fatal("INSERT with a surplus argument succeeded")
	}
	if _, err := db.Exec(
		`CREATE TABLE extra (PRIMARY KEY (id))`, "surplus",
	); err == nil {
		t.Fatal("CREATE TABLE with a surplus argument succeeded")
	}
	if _, err := db.Query(
		`SELECT id FROM docs WHERE id = ? OR id = ?`,
		"only-one",
	); err == nil {
		t.Fatal("SELECT with a missing argument succeeded")
	}
	if rows, err := db.Query(
		`SELECT id FROM docs WHERE id = ?`, "first", "surplus",
	); err == nil {
		_ = rows.Close()
		t.Fatal("SELECT with a surplus argument succeeded")
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)
}

func TestSQLDriverRejectsQueryExecAPIInversion(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`SELECT COUNT(*) FROM docs`); err == nil ||
		!strings.Contains(err.Error(), "use Query") {
		t.Fatalf("Exec(SELECT) error = %v, want use Query", err)
	}
	rows, err := db.Query(`INSERT INTO docs VALUES (?)`, `{"id":"not-written"}`)
	if err == nil {
		_ = rows.Close()
		t.Fatal("Query(INSERT) succeeded")
	}
	if !strings.Contains(err.Error(), "use Exec") {
		t.Fatalf("Query(INSERT) error = %v, want use Exec", err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)
}

func TestSQLDriverMutationResults(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}

	inserted, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a"}`, `{"id":"b"}`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := inserted.RowsAffected(); err != nil || affected != 2 {
		t.Fatalf("INSERT RowsAffected = (%d, %v), want (2, nil)", affected, err)
	}
	if id, err := inserted.LastInsertId(); !errors.Is(err, errNoLastInsertID) {
		t.Fatalf("INSERT LastInsertId = (%d, %v), want unavailable error", id, err)
	}

	deleted, err := db.Exec(`DELETE FROM docs WHERE id = ?`, "a")
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := deleted.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("DELETE RowsAffected = (%d, %v), want (1, nil)", affected, err)
	}
}

func TestSQLDriverMultiRowInsertIsAtomic(t *testing.T) {
	t.Run("first materialization", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`
			CREATE TABLE docs (
				id STRING PRIMARY KEY,
				n INTEGER NOT NULL
			)`); err != nil {
			t.Fatal(err)
		}

		_, err := db.Exec(
			`INSERT INTO docs VALUES (?), (?)`,
			`{"id":"valid","n":1}`,
			`{"id":"invalid","n":"wrong"}`,
		)
		if !errors.Is(err, store.ErrSchemaViolation) {
			t.Fatalf("invalid first INSERT = %v, want ErrSchemaViolation", err)
		}
		assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)

		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"after","n":2}`); err != nil {
			t.Fatalf("INSERT after rejected first batch: %v", err)
		}
		assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 1)
	})

	t.Run("materialized table", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`
			CREATE TABLE docs (
				id STRING PRIMARY KEY,
				n INTEGER NOT NULL
			)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed","n":0}`); err != nil {
			t.Fatal(err)
		}

		_, err := db.Exec(
			`INSERT INTO docs VALUES (?), (?)`,
			`{"id":"would-have-been-written","n":1}`,
			`{"id":"invalid","n":false}`,
		)
		if !errors.Is(err, store.ErrSchemaViolation) {
			t.Fatalf("invalid later INSERT = %v, want ErrSchemaViolation", err)
		}
		assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 1)
		assertSurfaceCount(t, db,
			`SELECT COUNT(*) FROM docs WHERE id = 'would-have-been-written'`, 0)
	})
}

func TestSQLDriverRejectsExistingKeyWithoutPartialInsert(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`,
		`{"id":"existing","value":"original"}`,
	); err != nil {
		t.Fatal(err)
	}

	_, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"new","value":"new"}`,
		`{"id":"existing","value":"replacement"}`,
	)
	if !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("duplicate INSERT = %v, want ErrDuplicatePrimaryKey", err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 1)
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs WHERE id = 'new'`, 0)

	var value string
	if err := db.QueryRow(
		`SELECT value FROM docs WHERE id = ?`, "existing",
	).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "original" {
		t.Fatalf("duplicate INSERT replaced existing value with %q", value)
	}
}

func TestSQLDriverEnforcesDurableKeyAndDocumentBoundsBeforeCommit(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			body STRING
		)`); err != nil {
		t.Fatal(err)
	}

	oversizeKey := strings.Repeat("k", 257)
	if _, err := db.Exec(
		`INSERT INTO docs (id, body) VALUES (?, ?)`,
		oversizeKey, "small",
	); !errors.Is(err, durable.ErrKeyTooLarge) {
		t.Fatalf("oversize primary key = %v, want ErrKeyTooLarge", err)
	}

	oversizeBody := strings.Repeat("x", 4<<20)
	if _, err := db.Exec(
		`INSERT INTO docs (id, body) VALUES (?, ?)`,
		"flat", oversizeBody,
	); !errors.Is(err, durable.ErrDocumentTooLarge) {
		t.Fatalf("oversize flat document = %v, want ErrDocumentTooLarge", err)
	}
	raw := `{"id":"raw","body":"` + oversizeBody + `"}`
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, raw,
	); !errors.Is(err, durable.ErrDocumentTooLarge) {
		t.Fatalf("oversize whole document = %v, want ErrDocumentTooLarge", err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)
}

func TestSQLDriverRejectsOversizeInsertBatchBeforeResolvingRows(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	statement := `INSERT INTO docs VALUES ` +
		strings.Repeat(`(?),`, 64) + `(?)`
	args := make([]any, 65)
	for i := range args {
		// The first row is deliberately invalid. Batch cardinality must be
		// rejected before any row is decoded or allocated.
		args[i] = `not-json`
	}
	if _, err := db.Exec(statement, args...); !errors.Is(err, durable.ErrBatchTooLarge) {
		t.Fatalf("65-row INSERT = %v, want early ErrBatchTooLarge", err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)
}

func TestSQLDriverMultiRowDeleteIsAtomicWithinBatch(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?), (?), (?)`,
		`{"id":"a","remove":true,"n":1}`,
		`{"id":"b","remove":true,"n":2}`,
		`{"id":"c","remove":true,"n":3}`,
		`{"id":"d","remove":false,"n":4}`,
	); err != nil {
		t.Fatal(err)
	}

	deleted, err := db.Exec(`DELETE FROM docs WHERE remove = TRUE AND n >= 2`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := deleted.RowsAffected(); err != nil || affected != 2 {
		t.Fatalf("DELETE RowsAffected = (%d, %v), want (2, nil)", affected, err)
	}
	assertSurfaceIDs(t, db, `SELECT id FROM docs ORDER BY id`, []string{"a", "d"})
}

func assertSurfaceCount(t *testing.T, db *stdsql.DB, statement string, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRow(statement).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s: count = %d, want %d", statement, got, want)
	}
}

func TestSQLDriverTreatsDollarKeyAsAnOrdinaryJSONField(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, "$key" STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","$key":"logical-a","value":"original"}`,
		`{"id":"b","$key":"logical-b","value":"other"}`,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		`UPDATE docs SET "$doc" = ? WHERE "$key" = ?`,
		`{"id":"a","$key":"logical-a","value":"updated"}`, "logical-a",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`DELETE FROM docs WHERE "$key" IN (?, ?)`,
		"logical-b", "absent",
	); err != nil {
		t.Fatal(err)
	}

	var value string
	if err := db.QueryRow(
		`SELECT value FROM docs WHERE id = ?`, "a",
	).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "updated" {
		t.Fatalf(`mutation through JSON field "$key" left value %q`, value)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 1)
}

func TestSQLDriverReservesOnlyWholeDocumentUpdateTarget(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE bad_doc ("$doc" STRING PRIMARY KEY)`,
	); err == nil || !strings.Contains(err.Error(), "whole replacement document") {
		t.Fatalf("CREATE TABLE with $doc error = %v, want reserved-target guidance", err)
	}

	if _, err := db.Exec(
		`CREATE TABLE docs ("$key" STRING PRIMARY KEY, value STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX by_key ON docs ("$key")`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs ("$key", value) VALUES (?, ?)`, "a", "ok",
	); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := db.QueryRow(
		`SELECT value FROM docs WHERE "$key" = ?`, "a",
	).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "ok" {
		t.Fatalf(`row keyed by JSON field "$key" = %q, want ok`, value)
	}
	if _, err := db.Prepare(
		`INSERT INTO docs (id, "$doc") VALUES (?, ?)`,
	); err == nil || !strings.Contains(err.Error(), "one complete document") {
		t.Fatalf("flat INSERT with $doc error = %v, want whole-document guidance", err)
	}
	if _, err := db.Prepare(
		`CREATE INDEX bad_doc ON docs ("$doc")`,
	); err == nil || !strings.Contains(err.Error(), "whole replacement document") {
		t.Fatalf("CREATE INDEX on $doc error = %v, want reserved-target guidance", err)
	}
}

func TestSQLDriverRejectsNonScalarPrimarySchemaBeforePublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`CREATE TABLE docs (id ANY PRIMARY KEY)`,
	); err == nil || !strings.Contains(err.Error(), "scalar") {
		t.Fatalf("non-scalar primary key error = %v, want scalar-schema refusal", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The refused definition must never reach the catalog. Reopen would reject
	// a persisted non-scalar primary schema, and reusing the name would collide
	// if the failed CREATE had published either in memory or on disk.
	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY)`,
	); err != nil {
		t.Fatalf("valid CREATE after refused schema: %v", err)
	}
}
