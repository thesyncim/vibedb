package driver

import (
	stdsql "database/sql"
	"errors"
	"testing"
)

func TestDatabaseSQLFromlessScalarDirectPreparedAndTransaction(t *testing.T) {
	db := openTestDB(t)

	var direct string
	var enabled bool
	if err := db.QueryRow(`SELECT 1 + 2 AS value, TRUE AS enabled`).Scan(
		&direct, &enabled,
	); err != nil {
		t.Fatal(err)
	}
	if direct != "3" || !enabled {
		t.Fatalf("direct FROM-less row = %q/%v, want 3/true", direct, enabled)
	}

	prepared, err := db.Prepare(
		`SELECT CASE WHEN ? = TRUE THEN CAST(? AS NUMERIC) ELSE -1 END AS value`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	var value string
	if err := prepared.QueryRow(true, "2.50").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "2.5" {
		t.Fatalf("prepared FROM-less value = %q, want 2.5", value)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.QueryRow(`SELECT ? || '!' AS value`, "transaction").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "transaction!" {
		t.Fatalf("transaction FROM-less value = %q", value)
	}
}

func TestDatabaseSQLFromlessScalarCTEAndSetComposition(t *testing.T) {
	db := openTestDB(t)
	for _, test := range []struct {
		source string
		want   string
	}{
		{`WITH seed AS (SELECT 1 AS n) SELECT n FROM seed`, "1"},
		{`SELECT 1 AS n UNION ALL VALUES (2) ORDER BY n DESC LIMIT 1`, "2"},
	} {
		var got string
		if err := db.QueryRow(test.source).Scan(&got); err != nil {
			t.Fatalf("%s: %v", test.source, err)
		}
		if got != test.want {
			t.Fatalf("%s = %q, want %q", test.source, got, test.want)
		}
	}
}

func TestDatabaseSQLFromlessScalarIgnoresUnreferencedPhysicalCTE(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE dormant_a (id STRING PRIMARY KEY)`,
		`CREATE TABLE dormant_b (id STRING PRIMARY KEY)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	const source = `WITH
		unused_a AS (SELECT id FROM dormant_a),
		unused_b AS (SELECT id FROM dormant_b)
		SELECT ? AS value`

	var got string
	if err := db.QueryRow(source, "direct").Scan(&got); err != nil {
		t.Fatalf("direct unused CTE: %v", err)
	}
	if got != "direct" {
		t.Fatalf("direct unused CTE value = %q", got)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.QueryRow(source, "transaction").Scan(&got); err != nil {
		t.Fatalf("transaction unused CTE: %v", err)
	}
	if got != "transaction" {
		t.Fatalf("transaction unused CTE value = %q", got)
	}
}

func TestDatabaseSQLFromlessScalarRejectsMissingUnreferencedPhysicalCTE(t *testing.T) {
	db := openTestDB(t)
	var got string
	err := db.QueryRow(`WITH unused AS (` +
		`SELECT id FROM missing_dormant) SELECT 1 AS value`).Scan(&got)
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("missing unused CTE = %v, want ErrTableNotFound", err)
	}
}

func TestDatabaseSQLFromlessScalarExecutesReachablePhysicalSubquery(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE predicate_docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO predicate_docs VALUES ('{"id":"present"}')`); err != nil {
		t.Fatal(err)
	}
	assert := func(label, want string, row *stdsql.Row) {
		t.Helper()
		var got string
		if err := row.Scan(&got); err != nil {
			t.Fatalf("%s predicate subquery: %v", label, err)
		}
		if got != want {
			t.Fatalf("%s predicate subquery value = %q, want %q", label, got, want)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, test := range []struct {
		name   string
		source string
		want   string
	}{
		{
			name: "FROM-less root",
			source: `SELECT 1 AS value WHERE EXISTS (` +
				`SELECT id FROM predicate_docs WHERE id = 'present')`,
			want: "1",
		},
		{
			name: "same collection nested",
			source: `SELECT id FROM predicate_docs WHERE EXISTS (` +
				`SELECT 1 WHERE EXISTS (` +
				`SELECT id FROM predicate_docs WHERE id = 'present'))`,
			want: "present",
		},
		{
			name: "reachable CTE body",
			source: `WITH probe AS (` +
				`SELECT 1 AS value WHERE EXISTS (` +
				`SELECT id FROM predicate_docs WHERE id = 'present')) ` +
				`SELECT value FROM probe`,
			want: "1",
		},
		{
			name: "reachable derived body",
			source: `SELECT value FROM (` +
				`SELECT 1 AS value WHERE EXISTS (` +
				`SELECT id FROM predicate_docs WHERE id = 'present')) AS probe`,
			want: "1",
		},
		{
			name: "FROM-less set leaf",
			source: `SELECT 1 AS value WHERE EXISTS (` +
				`SELECT id FROM predicate_docs WHERE id = 'present') ` +
				`UNION ALL VALUES (2) ORDER BY value DESC LIMIT 1`,
			want: "2",
		},
	} {
		assert("direct "+test.name, test.want, db.QueryRow(test.source))
		assert("transaction "+test.name, test.want, tx.QueryRow(test.source))
	}
}

func TestDatabaseSQLInsertFromlessNestedPhysicalSubquery(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE insert_predicate_docs (id STRING PRIMARY KEY)`,
		`CREATE TABLE insert_nested_target (id STRING PRIMARY KEY)`,
		`INSERT INTO insert_predicate_docs VALUES ('{"id":"present"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	const source = `INSERT INTO insert_nested_target ` +
		`SELECT CAST(? AS JSON) WHERE EXISTS (` +
		`SELECT 1 WHERE EXISTS (` +
		`SELECT id FROM insert_predicate_docs WHERE id = 'present'))`

	if _, err := db.Exec(source, `{"id":"direct"}`); err != nil {
		t.Fatalf("direct nested INSERT source: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(source, `{"id":"transaction"}`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("transaction nested INSERT source: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM insert_nested_target`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("nested INSERT rows = %d, want 2", count)
	}
}

func TestDatabaseSQLInsertFromlessScalarIgnoresUnreferencedPhysicalCTE(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE fromless_target (id STRING PRIMARY KEY)`,
		`CREATE TABLE dormant_a (id STRING PRIMARY KEY)`,
		`CREATE TABLE dormant_b (id STRING PRIMARY KEY)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	insert := func(executor interface {
		Exec(query string, args ...any) (stdsql.Result, error)
	}, id string) error {
		_, err := executor.Exec(
			`INSERT INTO fromless_target `+
				`WITH unused_a AS (SELECT id FROM dormant_a), `+
				`unused_b AS (SELECT id FROM dormant_b) `+
				`SELECT CAST(? AS JSON)`,
			`{"id":"`+id+`"}`,
		)
		return err
	}

	if err := insert(db, "direct"); err != nil {
		t.Fatalf("direct INSERT source unused CTE: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := insert(tx, "transaction"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("transaction INSERT source unused CTE: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM fromless_target`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("inserted rows = %d, want 2", count)
	}
}
