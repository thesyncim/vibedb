package driver

import (
	stdsql "database/sql"
	"errors"
	"testing"
)

func seedJoinTables(t *testing.T, db *stdsql.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE users (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE orders (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{
		`{"id":"u1","$key":"u1","name":"Alice"}`,
		`{"id":"u2","$key":"u2","name":"Bob"}`,
		`{"id":"u3","$key":"u3","name":"Cara"}`,
	} {
		if _, err := db.Exec(`INSERT INTO users VALUES (?)`, document); err != nil {
			t.Fatal(err)
		}
	}
	for _, document := range []string{
		`{"id":"o1","user_id":"u1","total":10}`,
		`{"id":"o2","user_id":"u1","total":20}`,
		`{"id":"o3","user_id":"u2","total":30}`,
		`{"id":"orphan","user_id":"absent","total":40}`,
	} {
		if _, err := db.Exec(`INSERT INTO orders VALUES (?)`, document); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLeftJoinPreservesAndNullExtendsDrivingRows(t *testing.T) {
	db := openTestDB(t)
	seedJoinTables(t, db)

	rows, err := db.Query(`
		SELECT u.name, o.total
		FROM users AS u
		LEFT JOIN orders AS o ON u.id = o.user_id
		ORDER BY u.name, o.total`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type pair struct {
		name  string
		total stdsql.NullInt64
	}
	var got []pair
	for rows.Next() {
		var row pair
		if err := rows.Scan(&row.name, &row.total); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []pair{
		{name: "Alice", total: stdsql.NullInt64{Int64: 10, Valid: true}},
		{name: "Alice", total: stdsql.NullInt64{Int64: 20, Valid: true}},
		{name: "Bob", total: stdsql.NullInt64{Int64: 30, Valid: true}},
		{name: "Cara"},
	}
	if len(got) != len(want) {
		t.Fatalf("left joined rows = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("left joined row %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	var all, matched int64
	if err := db.QueryRow(`
		SELECT COUNT(*), COUNT(o.total)
		FROM users AS u
		LEFT JOIN orders AS o ON u.id = o.user_id`).Scan(&all, &matched); err != nil {
		t.Fatal(err)
	}
	if all != 4 || matched != 3 {
		t.Fatalf("LEFT JOIN counts = (%d, %d), want (4, 3)", all, matched)
	}
}

func TestLeftJoinRejectsNullableSideWhereRatherThanMiscomputing(t *testing.T) {
	db := openTestDB(t)
	seedJoinTables(t, db)

	_, err := db.Query(`
		SELECT u.name
		FROM users AS u
		LEFT JOIN orders AS o ON u.id = o.user_id
		WHERE o.total IS NULL`)
	if err == nil {
		t.Fatal("nullable-side LEFT JOIN WHERE unexpectedly succeeded")
	}
}

func TestNaturalInnerJoinProjectsEveryMatchingPair(t *testing.T) {
	db := openTestDB(t)
	seedJoinTables(t, db)

	rows, err := db.Query(`
		SELECT u.name, o.total
		FROM users AS u
		JOIN orders AS o ON u.id = o.user_id
		ORDER BY o.total`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type pair struct {
		name  string
		total int64
	}
	var got []pair
	for rows.Next() {
		var row pair
		if err := rows.Scan(&row.name, &row.total); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []pair{{"Alice", 10}, {"Alice", 20}, {"Bob", 30}}
	if len(got) != len(want) {
		t.Fatalf("joined rows = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("joined row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestNaturalJoinInsideTransactionReadsItsOverlay(t *testing.T) {
	db := openTestDB(t)
	seedJoinTables(t, db)

	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer transaction.Rollback()
	if _, err := transaction.Exec(
		`INSERT INTO orders VALUES (?)`,
		`{"id":"pending","user_id":"u2","total":99}`,
	); err != nil {
		t.Fatal(err)
	}

	var name string
	if err := transaction.QueryRow(`
		SELECT u.name
		FROM orders AS o
		JOIN users AS u ON o.user_id = u.id
		WHERE o.id = 'pending'`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Bob" {
		t.Fatalf("joined pending row name = %q, want Bob", name)
	}
}

func TestNaturalJoinOverLazyEmptyTablePreservesAggregateShape(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE users (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE orders (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO users VALUES (?)`, `{"id":"u1","name":"Only"}`,
	); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM orders AS o
		JOIN users AS u ON o.user_id = u.id`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("empty joined COUNT(*) = %d, want 0", count)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM users AS u
		LEFT JOIN orders AS o ON u.id = o.user_id`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("LEFT JOIN over lazy empty table COUNT(*) = %d, want 1", count)
	}
}

func TestJoinTreatsDollarKeyAsAnOrdinaryJSONField(t *testing.T) {
	db := openTestDB(t)
	seedJoinTables(t, db)

	var count int64
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM orders AS o
		JOIN users AS u ON o.user_id = u."$key"`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf(`JOIN against JSON field "$key" count = %d, want 3`, count)
	}
}

func TestJoinMaterializationBudgetRejectsBeforeBuild(t *testing.T) {
	budget := joinMaterializationBudget{limit: driverMinimumQueryMemory}
	document := make([]byte, driverMinimumQueryMemory)
	err := budget.add("orders", []byte("o1"), document)
	if !errors.Is(err, ErrJoinMaterializationTooLarge) {
		t.Fatalf("budget error = %v, want ErrJoinMaterializationTooLarge", err)
	}
	if budget.used != 0 {
		t.Fatalf("rejected budget used = %d, want 0", budget.used)
	}
}
