package driver

import (
	stdsql "database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestDatabaseSQLDerivedTablesDirectAndPrepared(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE customers (
			id STRING PRIMARY KEY,
			tier STRING NOT NULL,
			score INTEGER NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO customers VALUES (?), (?), (?), (?)`,
		`{"id":"c1","tier":"pro","score":30}`,
		`{"id":"c2","tier":"free","score":20}`,
		`{"id":"c3","tier":"pro","score":10}`,
		`{"id":"c4","tier":"free","score":40}`,
	); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		SELECT d.tier, COUNT(*) AS n
		FROM (SELECT tier FROM customers) AS d
		GROUP BY d.tier
		ORDER BY d.tier
		LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	var groups []string
	for rows.Next() {
		var tier string
		var count int64
		if err := rows.Scan(&tier, &count); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		groups = append(groups, fmt.Sprintf("%s:%d", tier, count))
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"free:2", "pro:2"}; !slices.Equal(groups, want) {
		t.Fatalf("derived grouped rows = %v, want %v", groups, want)
	}

	prepared, err := db.Prepare(`
		SELECT outer_d.id
		FROM (
			SELECT inner_d.id
			FROM (
				SELECT id, tier, score
				FROM customers
				WHERE tier = ?
			) AS inner_d
			WHERE inner_d.score >= ?
			ORDER BY inner_d.id
			LIMIT ?
		) AS outer_d
		ORDER BY outer_d.id DESC`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	for _, test := range []struct {
		name string
		args []any
		want []string
	}{
		{name: "pro", args: []any{"pro", 10, 2}, want: []string{"c3", "c1"}},
		{name: "free", args: []any{"free", 30, 1}, want: []string{"c4"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, err := prepared.Query(test.args...)
			if err != nil {
				t.Fatal(err)
			}
			got := scanDerivedIDs(t, rows)
			if !slices.Equal(got, test.want) {
				t.Fatalf("prepared derived rows = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDatabaseSQLDerivedTableMissingDependency(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE present (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}

	_, err := db.Query(`
		SELECT d.id
		FROM (
			SELECT p.id
			FROM present AS p
			WHERE EXISTS (SELECT id FROM missing)
		) AS d`)
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("derived missing dependency = %v, want ErrTableNotFound", err)
	}
	if got := err.Error(); !strings.Contains(got, `"missing"`) || strings.Contains(got, `""`) {
		t.Fatalf("derived missing dependency error = %q", got)
	}

	prepared, err := db.Prepare(
		`SELECT d.id FROM (SELECT id FROM present) AS d`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, err := db.Exec(`DROP TABLE present`); err != nil {
		t.Fatal(err)
	}
	rows, err := prepared.Query()
	if rows != nil {
		rows.Close()
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("prepared derived query after DROP = %v, want ErrTableNotFound", err)
	}
	if got := err.Error(); !strings.Contains(got, `"present"`) || strings.Contains(got, `""`) {
		t.Fatalf("prepared derived post-DROP error = %q", got)
	}
}

func TestPreparedExplainDerivedTableRevalidatesPhysicalDependencies(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	explain, err := db.Prepare(
		`EXPLAIN SELECT d.id FROM (SELECT id FROM docs) AS d`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer explain.Close()
	var plan string
	if err := explain.QueryRow().Scan(&plan); err != nil {
		t.Fatalf("derived EXPLAIN before DROP: %v", err)
	}
	if plan == "" {
		t.Fatal("derived EXPLAIN returned an empty plan")
	}
	if _, err := db.Exec(`DROP TABLE docs`); err != nil {
		t.Fatal(err)
	}
	rows, err := explain.Query()
	if rows != nil {
		rows.Close()
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("prepared derived EXPLAIN after DROP = %v, want ErrTableNotFound", err)
	}
}

func TestTransactionDerivedTableSnapshotAndReadYourWrites(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	for _, statement := range []string{
		`CREATE TABLE docs (PRIMARY KEY (id))`,
		`CREATE TABLE permitted (PRIMARY KEY (id))`,
		`INSERT INTO docs VALUES ('{"id":"base"}')`,
		`INSERT INTO permitted VALUES ('{"id":"base"}'), ('{"id":"pending"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// These commits occur after BEGIN. Neither physical collection may leak a
	// newer generation into the transaction's derived/predicate read.
	if _, err := db.Exec(
		`INSERT INTO docs VALUES ('{"id":"outside"}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO permitted VALUES ('{"id":"outside"}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`INSERT INTO docs VALUES ('{"id":"pending"}')`); err != nil {
		t.Fatal(err)
	}

	rows, err := tx.Query(`
		SELECT d.id
		FROM (
			SELECT id
			FROM docs
			WHERE id IN (SELECT id FROM permitted)
		) AS d
		ORDER BY d.id`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanDerivedIDs(t, rows), []string{"base", "pending"}; !slices.Equal(got, want) {
		t.Fatalf("transaction derived rows = %v, want %v", got, want)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	rows, err = db.Query(`
		SELECT d.id FROM (SELECT id FROM docs) AS d ORDER BY d.id`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanDerivedIDs(t, rows), []string{"base", "outside"}; !slices.Equal(got, want) {
		t.Fatalf("rows after rollback = %v, want %v", got, want)
	}
}

func TestDerivedTableDependenciesAreRecursiveStableAndUnique(t *testing.T) {
	tree, err := sqlast.Parse(`
		SELECT outer_d.id
		FROM (
			SELECT inner_d.id
			FROM (
				SELECT id FROM docs
				WHERE id IN (SELECT id FROM permitted)
			) AS inner_d
			WHERE EXISTS (SELECT id FROM docs)
		) AS outer_d
		WHERE outer_d.id IN (SELECT id FROM audit)`)
	if err != nil {
		t.Fatal(err)
	}
	got := joinTableNames(tree)
	want := []string{"docs", "permitted", "audit"}
	if !slices.Equal(got, want) {
		t.Fatalf("derived dependencies = %v, want %v", got, want)
	}
	for _, name := range got {
		if name == "" {
			t.Fatal("derived dependency walk retained an empty physical name")
		}
	}
}

func scanDerivedIDs(t *testing.T, rows *stdsql.Rows) []string {
	t.Helper()
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}
