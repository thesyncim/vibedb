package driver

import (
	"errors"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const recursiveCTEDriverSQL = `WITH RECURSIVE reachable(node) AS (
	SELECT src AS node FROM recursive_edges WHERE src = ?
	UNION
	SELECT e.dst AS node
	FROM reachable AS r
	JOIN recursive_edges AS e ON r.node = e.src
)
SELECT node FROM reachable ORDER BY node`

const recursiveCTEDriverDependencySQL = `WITH RECURSIVE
filtered(src, dst) AS MATERIALIZED (
	SELECT src, dst FROM recursive_edges WHERE dst <= ?
),
reachable(node) AS (
	SELECT src FROM filtered WHERE src = ?
	UNION
	SELECT e.dst FROM reachable r JOIN filtered e ON r.node = e.src
)
SELECT node FROM reachable ORDER BY node`

func TestDatabaseSQLRecursiveCTEPreparedTransitiveClosureAndPositionedRefusal(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE recursive_edges (` +
			`id STRING PRIMARY KEY, src INTEGER NOT NULL, dst INTEGER NOT NULL)`,
		`INSERT INTO recursive_edges VALUES ` +
			`('{"id":"e01","src":0,"dst":1}'),` +
			`('{"id":"e12","src":1,"dst":2}'),` +
			`('{"id":"e23","src":2,"dst":3}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	prepared, err := db.Prepare(recursiveCTEDriverSQL)
	if err != nil {
		t.Fatalf("prepare recursive CTE: %v", err)
	}
	defer prepared.Close()
	for _, test := range []struct {
		start int64
		want  []int64
	}{
		{start: 0, want: []int64{0, 1, 2, 3}},
		{start: 1, want: []int64{1, 2, 3}},
	} {
		rows, err := prepared.Query(test.start)
		if err != nil {
			t.Fatalf("execute recursive CTE from %d: %v", test.start, err)
		}
		var got []int64
		for rows.Next() {
			var node int64
			if err := rows.Scan(&node); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			got = append(got, node)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, test.want) {
			t.Fatalf("recursive closure from %d = %v, want %v",
				test.start, got, test.want)
		}
	}

	dependency, err := db.Prepare(recursiveCTEDriverDependencySQL)
	if err != nil {
		t.Fatalf("prepare dependent recursive CTE: %v", err)
	}
	defer dependency.Close()
	rows, err := dependency.Query(int64(3), int64(0))
	if err != nil {
		t.Fatalf("execute dependent recursive CTE: %v", err)
	}
	var dependent []int64
	for rows.Next() {
		var node int64
		if err := rows.Scan(&node); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		dependent = append(dependent, node)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if want := []int64{0, 1, 2, 3}; !slices.Equal(dependent, want) {
		t.Fatalf("dependent recursive closure = %v, want %v", dependent, want)
	}

	unsupportedSQL := `WITH RECURSIVE reachable(node) AS (` +
		`SELECT src FROM recursive_edges WHERE src = 0 ` +
		`UNION SELECT e.dst FROM reachable r ` +
		`JOIN recursive_edges e ON r.node = e.src` +
		`) SEARCH DEPTH FIRST BY node SET traversal_order ` +
		`SELECT node FROM reachable`
	statement, err := db.Prepare(unsupportedSQL)
	if statement != nil {
		statement.Close()
		t.Fatal("database/sql prepared unsupported recursive SEARCH clause")
	}
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("database/sql recursive refusal = %T %v, want typed feature refusal", err, err)
	}
	if want := strings.Index(unsupportedSQL, "SEARCH"); unsupported.Pos != want {
		t.Fatalf("database/sql recursive refusal position = %d, want %d",
			unsupported.Pos, want)
	}
}
