package driver

import (
	stdsql "database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func seedSQLSetTables(t *testing.T, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE set_a (id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`CREATE TABLE set_b (id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`INSERT INTO set_a VALUES ` +
			`('{"id":"a1","n":1}'), ('{"id":"a2","n":2}'), ('{"id":"a4","n":4}')`,
		`INSERT INTO set_b VALUES ` +
			`('{"id":"b2","n":2}'), ('{"id":"b3","n":3}'), ('{"id":"b4","n":4}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func scanSQLSetStrings(t testing.TB, rows *stdsql.Rows) []string {
	t.Helper()
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSQLSetDirectPreparedGroupedTailAndStableSchema(t *testing.T) {
	db := openTestDB(t)
	seedSQLSetTables(t, db)

	rows, err := db.Query(`
		(SELECT id AS stable_name FROM set_a ORDER BY stable_name DESC LIMIT 2)
		UNION ALL SELECT id AS ignored_name FROM set_b
		ORDER BY stable_name LIMIT 4 OFFSET 1`)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(columns, []string{"stable_name"}) {
		t.Fatalf("columns = %v, want stable first-operand name", columns)
	}
	if got, want := scanSQLSetStrings(t, rows), []string{"a4", "b2", "b3", "b4"}; !slices.Equal(got, want) {
		t.Fatalf("direct set rows = %v, want %v", got, want)
	}

	prepared, err := db.Prepare(`
		SELECT id AS item FROM set_a WHERE n >= ?
		UNION DISTINCT SELECT id FROM set_b WHERE n <= ?
		ORDER BY item DESC LIMIT ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	rows, err = prepared.Query(int64(2), int64(3), int64(3))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanSQLSetStrings(t, rows), []string{"b3", "b2", "a4"}; !slices.Equal(got, want) {
		t.Fatalf("prepared set rows = %v, want %v", got, want)
	}
	var plan string
	if err := db.QueryRow(`EXPLAIN SELECT id FROM set_a UNION ALL SELECT id FROM set_b`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, `"node":"set"`) ||
		!strings.Contains(plan, `"operation":"union all"`) {
		t.Fatalf("set EXPLAIN = %s", plan)
	}
}

func TestSQLSetPreparedAndExplainRevalidateDependencies(t *testing.T) {
	db := openTestDB(t)
	seedSQLSetTables(t, db)
	queryStatement, err := db.Prepare(
		`SELECT id FROM set_a UNION ALL SELECT id FROM set_b`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer queryStatement.Close()
	explainStatement, err := db.Prepare(
		`EXPLAIN SELECT id FROM set_a UNION ALL SELECT id FROM set_b`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer explainStatement.Close()
	tableStatement, err := db.Prepare(
		`TABLE set_a UNION ALL TABLE set_b`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tableStatement.Close()
	if _, err := db.Exec(`DROP TABLE set_b`); err != nil {
		t.Fatal(err)
	}
	for name, statement := range map[string]*stdsql.Stmt{
		"query": queryStatement, "explain": explainStatement,
		"table": tableStatement,
	} {
		rows, err := statement.Query()
		if rows != nil {
			_ = rows.Close()
		}
		if !errors.Is(err, ErrTableNotFound) {
			t.Fatalf("prepared set %s after DROP = %v", name, err)
		}
	}
	rows, err := db.Query(`EXPLAIN SELECT id FROM set_a UNION ALL SELECT id FROM set_b`)
	if rows != nil {
		_ = rows.Close()
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("plain set EXPLAIN after DROP = %v", err)
	}
	rows, err = db.Query(`EXPLAIN TABLE set_a UNION ALL TABLE set_b`)
	if rows != nil {
		_ = rows.Close()
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("plain TABLE set EXPLAIN after DROP = %v", err)
	}
}

func TestSQLSetValuesAndTableRootsPreparedTransactionsAndAllModes(t *testing.T) {
	db := openTestDB(t)
	seedSQLSetTables(t, db)

	for _, test := range []struct {
		operation string
		want      []string
	}{
		{"UNION ALL", []string{"1", "2", "2", "2", "3"}},
		{"UNION DISTINCT", []string{"1", "2", "3"}},
		{"INTERSECT ALL", []string{"2"}},
		{"INTERSECT DISTINCT", []string{"2"}},
		{"EXCEPT ALL", []string{"1", "2"}},
		{"EXCEPT DISTINCT", []string{"1"}},
	} {
		t.Run(test.operation, func(t *testing.T) {
			rows, err := db.Query(
				`VALUES (1), (2), (2) ` + test.operation +
					` VALUES (2), (3) ORDER BY column1`,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := scanSQLSetStrings(t, rows); !slices.Equal(got, test.want) {
				t.Fatalf("VALUES rows = %v, want %v", got, test.want)
			}
		})
	}

	rows, err := db.Query(`TABLE set_a`)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(columns, []string{"*"}) {
		t.Fatalf("TABLE columns = %v, want [*]", columns)
	}
	tableRows := scanSQLSetStrings(t, rows)
	if len(tableRows) != 3 || !strings.Contains(tableRows[0], `"id":"a1"`) {
		t.Fatalf("TABLE rows = %v", tableRows)
	}

	prepared, err := db.Prepare(
		`(VALUES (?) ORDER BY column1) UNION ALL ` +
			`SELECT n FROM set_a ORDER BY column1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	rows, err = prepared.Query(int64(0))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanSQLSetStrings(t, rows), []string{"0", "1", "2", "4"}; !slices.Equal(got, want) {
		t.Fatalf("prepared VALUES/SELECT rows = %v, want %v", got, want)
	}

	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(
		`INSERT INTO set_a VALUES ('{"id":"tx","n":9}')`,
	); err != nil {
		_ = transaction.Rollback()
		t.Fatal(err)
	}
	rows, err = transaction.Query(
		`VALUES (0) UNION ALL SELECT n FROM set_a ORDER BY column1`,
	)
	if err != nil {
		_ = transaction.Rollback()
		t.Fatal(err)
	}
	if got, want := scanSQLSetStrings(t, rows), []string{"0", "1", "2", "4", "9"}; !slices.Equal(got, want) {
		_ = transaction.Rollback()
		t.Fatalf("transaction VALUES/SELECT rows = %v, want %v", got, want)
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}

	invalid := `VALUES (1) UNION SELECT id, n FROM set_a`
	_, err = db.Prepare(invalid)
	var positioned *sqlast.ParseError
	if !errors.As(err, &positioned) || positioned.Pos != strings.Index(invalid, "UNION") {
		t.Fatalf("positioned VALUES arity error = %T %v", err, err)
	}

	var plan string
	if err := db.QueryRow(`EXPLAIN ANALYZE VALUES (1), (2) UNION ALL VALUES (3)`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, `"kind":"values"`) ||
		!strings.Contains(plan, `"rows":3`) {
		t.Fatalf("VALUES EXPLAIN ANALYZE = %s", plan)
	}
}

func TestSQLSetTransactionSnapshotAndReadYourWrites(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	seedSQLSetTables(t, db)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`INSERT INTO set_b VALUES ('{"id":"outside","n":9}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO set_a VALUES ('{"id":"pending","n":8}')`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(`
		SELECT id FROM set_a UNION ALL SELECT id FROM set_b ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanSQLSetStrings(t, rows),
		[]string{"a1", "a2", "a4", "b2", "b3", "b4", "pending"}; !slices.Equal(got, want) {
		t.Fatalf("transaction set rows = %v, want %v", got, want)
	}
}
