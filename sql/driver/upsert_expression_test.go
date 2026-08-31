package driver

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestUpsertExpressionsAutocommitSimultaneousPerCandidateReturning(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE upsert_expression_rows (
			id STRING PRIMARY KEY,
			n INTEGER NOT NULL,
			delta INTEGER NOT NULL,
			mirror INTEGER NOT NULL,
			label STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO upsert_expression_rows VALUES (?), (?)`,
		`{"id":"a","n":10,"delta":0,"mirror":0,"label":"old-a"}`,
		`{"id":"b","n":20,"delta":0,"mirror":0,"label":"old-b"}`,
	); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		INSERT INTO upsert_expression_rows VALUES (?), (?), (?)
		ON CONFLICT DO UPDATE SET
			n = upsert_expression_rows.n + EXCLUDED.delta,
			delta = EXCLUDED.delta,
			mirror = upsert_expression_rows.n,
			label = upsert_expression_rows.label || ':' || EXCLUDED.label
		RETURNING id, n, delta, mirror, label`,
		`{"id":"a","n":100,"delta":2,"mirror":100,"label":"candidate-a"}`,
		`{"id":"b","n":200,"delta":3,"mirror":200,"label":"candidate-b"}`,
		`{"id":"c","n":7,"delta":4,"mirror":9,"label":"inserted-c"}`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []struct {
		id       string
		n, delta int64
		mirror   int64
		label    string
	}{
		{id: "a", n: 12, delta: 2, mirror: 10, label: "old-a:candidate-a"},
		{id: "b", n: 23, delta: 3, mirror: 20, label: "old-b:candidate-b"},
		{id: "c", n: 7, delta: 4, mirror: 9, label: "inserted-c"},
	}
	for i := range want {
		if !rows.Next() {
			t.Fatalf("RETURNING ended at row %d: %v", i, rows.Err())
		}
		var got struct {
			id       string
			n, delta int64
			mirror   int64
			label    string
		}
		if err := rows.Scan(
			&got.id, &got.n, &got.delta, &got.mirror, &got.label,
		); err != nil {
			t.Fatal(err)
		}
		if got != want[i] {
			t.Fatalf("RETURNING row %d = %+v, want %+v", i, got, want[i])
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected trailing RETURNING row or error: %v", rows.Err())
	}
}

func TestUpsertExpressionsExactWideNumberArithmetic(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE upsert_expression_exact (
			id STRING PRIMARY KEY,
			n NUMBER NOT NULL,
			delta NUMBER NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO upsert_expression_exact VALUES (?)`,
		`{"id":"wide","n":9007199254740993,"delta":0}`,
	); err != nil {
		t.Fatal(err)
	}

	var returned string
	if err := db.QueryRow(`
		INSERT INTO upsert_expression_exact VALUES (?)
		ON CONFLICT DO UPDATE SET
			n = upsert_expression_exact.n + EXCLUDED.delta
		RETURNING n`,
		`{"id":"wide","n":0,"delta":1}`,
	).Scan(&returned); err != nil {
		t.Fatal(err)
	}
	if returned != "9007199254740994" {
		t.Fatalf("exact conflict RETURNING value = %q, want 9007199254740994", returned)
	}

	var stored string
	if err := db.QueryRow(`
		SELECT n FROM upsert_expression_exact WHERE id = 'wide'`,
	).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "9007199254740994" {
		t.Fatalf("stored exact conflict value = %q, want 9007199254740994", stored)
	}
}

func TestUpsertExpressionsNoConflictIsLazyAndLaterErrorIsAtomic(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE upsert_expression_errors (
			id STRING PRIMARY KEY,
			n INTEGER NOT NULL,
			divisor INTEGER NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO upsert_expression_errors VALUES (?), (?)`,
		`{"id":"a","n":8,"divisor":1}`,
		`{"id":"b","n":9,"divisor":1}`,
	); err != nil {
		t.Fatal(err)
	}

	// The UPDATE arm is not evaluated for an inserted candidate.
	if _, err := db.Exec(`
		INSERT INTO upsert_expression_errors VALUES (?)
		ON CONFLICT DO UPDATE SET n = upsert_expression_errors.n / EXCLUDED.divisor`,
		`{"id":"c","n":7,"divisor":0}`,
	); err != nil {
		t.Fatalf("non-conflicting zero divisor was evaluated: %v", err)
	}

	// A later conflicting candidate failure must discard earlier computed
	// postimages and an intervening insert candidate.
	_, err := db.Exec(`
		INSERT INTO upsert_expression_errors VALUES (?), (?), (?)
		ON CONFLICT DO UPDATE SET n = upsert_expression_errors.n / EXCLUDED.divisor`,
		`{"id":"a","n":100,"divisor":2}`,
		`{"id":"d","n":4,"divisor":1}`,
		`{"id":"b","n":100,"divisor":0}`,
	)
	if !errors.Is(err, query.ErrScalarDivisionByZero) {
		t.Fatalf("later conflict expression error = %T %v, want division by zero", err, err)
	}
	for id, want := range map[string]int64{"a": 8, "b": 9, "c": 7} {
		var got int64
		if err := db.QueryRow(
			`SELECT n FROM upsert_expression_errors WHERE id = ?`, id,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("row %q after failed upsert = %d, want %d", id, got, want)
		}
	}
	assertSurfaceCount(
		t, db, `SELECT COUNT(*) FROM upsert_expression_errors WHERE id = 'd'`, 0,
	)

	_, err = db.Exec(`
		INSERT INTO upsert_expression_errors VALUES (?), (?)
		ON CONFLICT DO UPDATE SET n = upsert_expression_errors.n + EXCLUDED.n`,
		`{"id":"a","n":1,"divisor":1}`,
		`{"id":"a","n":2,"divisor":1}`,
	)
	if !errors.Is(err, ErrUpsertCardinality) {
		t.Fatalf("duplicate computed upsert = %T %v, want ErrUpsertCardinality", err, err)
	}
	var unchanged int64
	if err := db.QueryRow(
		`SELECT n FROM upsert_expression_errors WHERE id = 'a'`,
	).Scan(&unchanged); err != nil {
		t.Fatal(err)
	}
	if unchanged != 8 {
		t.Fatalf("duplicate candidate changed row a to %d, want 8", unchanged)
	}
}

func TestUpsertExpressionsMaintainIndexesAndRejectIdentityMoves(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE upsert_expression_indexed (` +
			`id STRING PRIMARY KEY, handle STRING NOT NULL, ` +
			`next_handle STRING NOT NULL, suffix STRING NOT NULL, ` +
			`score INTEGER NOT NULL, delta INTEGER NOT NULL)`,
		`CREATE INDEX upsert_expression_by_score ON upsert_expression_indexed (score)`,
		`CREATE UNIQUE INDEX upsert_expression_by_handle ON upsert_expression_indexed (handle)`,
		`INSERT INTO upsert_expression_indexed VALUES (` +
			`'{"id":"a","handle":"alpha","next_handle":"unused",` +
			`"suffix":"","score":1,"delta":0}')`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatalf("setup %q: %v", source, err)
		}
	}

	if _, err := db.Exec(`
		INSERT INTO upsert_expression_indexed VALUES (?)
		ON CONFLICT DO UPDATE SET
			handle = upsert_expression_indexed.handle || EXCLUDED.suffix,
			score = upsert_expression_indexed.score + EXCLUDED.delta`,
		`{"id":"a","handle":"candidate","next_handle":"unused",`+
			`"suffix":"-v2","score":100,"delta":10}`,
	); err != nil {
		t.Fatal(err)
	}
	for predicate, want := range map[string]int64{
		`score = 1`: 0, `score = 11`: 1,
	} {
		var got int64
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM upsert_expression_indexed WHERE ` + predicate,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("indexed predicate %q count = %d, want %d", predicate, got, want)
		}
	}
	if _, err := db.Exec(`INSERT INTO upsert_expression_indexed VALUES (?)`,
		`{"id":"b","handle":"alpha","next_handle":"unused",`+
			`"suffix":"","score":2,"delta":0}`,
	); err != nil {
		t.Fatalf("old unique value remained claimed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO upsert_expression_indexed VALUES (?)`,
		`{"id":"c","handle":"alpha-v2","next_handle":"unused",`+
			`"suffix":"","score":3,"delta":0}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("computed unique postimage was not claimed: %v", err)
	}

	_, err := db.Exec(`
		INSERT INTO upsert_expression_indexed VALUES (?)
		ON CONFLICT DO UPDATE SET
			handle = EXCLUDED.next_handle || '',
			score = upsert_expression_indexed.score + 100`,
		`{"id":"a","handle":"ignored","next_handle":"alpha",`+
			`"suffix":"","score":0,"delta":0}`,
	)
	if !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("computed unique collision = %v, want ErrUniqueConstraint", err)
	}
	assertUpsertExpressionIndexRow(t, db, "a", "alpha-v2", 11)

	_, err = db.Exec(`
		INSERT INTO upsert_expression_indexed VALUES (?)
		ON CONFLICT DO UPDATE SET
			id = upsert_expression_indexed.id || EXCLUDED.suffix,
			score = upsert_expression_indexed.score + 1`,
		`{"id":"a","handle":"ignored","next_handle":"unused",`+
			`"suffix":"-moved","score":0,"delta":0}`,
	)
	if !errors.Is(err, ErrUpdatePrimaryKey) {
		t.Fatalf("computed conflict primary-key move = %v, want ErrUpdatePrimaryKey", err)
	}
	assertUpsertExpressionIndexRow(t, db, "a", "alpha-v2", 11)
}

func TestUpsertExpressionsTransactionUsesVisibleCurrentRowAndSavepoints(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE upsert_expression_tx (` +
		`id STRING PRIMARY KEY, n INTEGER NOT NULL, mirror INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO upsert_expression_tx VALUES (?)`,
		`{"id":"a","n":5,"mirror":0}`,
	); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SAVEPOINT before_computed_upsert`); err != nil {
		t.Fatal(err)
	}
	var n, mirror int64
	if err := tx.QueryRow(`
		INSERT INTO upsert_expression_tx VALUES (?)
		ON CONFLICT DO UPDATE SET
			n = upsert_expression_tx.n + EXCLUDED.n,
			mirror = upsert_expression_tx.n
		RETURNING n, mirror`,
		`{"id":"a","n":3,"mirror":99}`,
	).Scan(&n, &mirror); err != nil {
		t.Fatal(err)
	}
	if n != 8 || mirror != 5 {
		t.Fatalf("transaction conflict postimage = (%d,%d), want (8,5)", n, mirror)
	}
	if _, err := tx.Exec(`ROLLBACK TO SAVEPOINT before_computed_upsert`); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`
		SELECT n, mirror FROM upsert_expression_tx WHERE id = 'a'`,
	).Scan(&n, &mirror); err != nil {
		t.Fatal(err)
	}
	if n != 5 || mirror != 0 {
		t.Fatalf("row after savepoint rollback = (%d,%d), want (5,0)", n, mirror)
	}

	if _, err := tx.Exec(`INSERT INTO upsert_expression_tx VALUES (?)`,
		`{"id":"b","n":10,"mirror":0}`,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`
		INSERT INTO upsert_expression_tx VALUES (?)
		ON CONFLICT DO UPDATE SET
			n = upsert_expression_tx.n + EXCLUDED.n,
			mirror = upsert_expression_tx.n
		RETURNING n, mirror`,
		`{"id":"b","n":2,"mirror":99}`,
	).Scan(&n, &mirror); err != nil {
		t.Fatal(err)
	}
	if n != 12 || mirror != 10 {
		t.Fatalf("staged-row conflict postimage = (%d,%d), want (12,10)", n, mirror)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`
		SELECT n, mirror FROM upsert_expression_tx WHERE id = 'b'`,
	).Scan(&n, &mirror); err != nil {
		t.Fatal(err)
	}
	if n != 12 || mirror != 10 {
		t.Fatalf("committed staged-row conflict = (%d,%d), want (12,10)", n, mirror)
	}
}

func TestUpsertExpressionsTransactionLaterFailureIsStatementAtomic(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE upsert_expression_tx_atomic (
			id STRING PRIMARY KEY,
			n INTEGER NOT NULL,
			divisor INTEGER NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO upsert_expression_tx_atomic VALUES (?), (?)`,
		`{"id":"a","n":8,"divisor":1}`,
		`{"id":"b","n":9,"divisor":1}`,
	); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
		INSERT INTO upsert_expression_tx_atomic VALUES (?), (?), (?)
		ON CONFLICT DO UPDATE SET
			n = upsert_expression_tx_atomic.n / EXCLUDED.divisor`,
		`{"id":"a","n":100,"divisor":2}`,
		`{"id":"c","n":4,"divisor":1}`,
		`{"id":"b","n":100,"divisor":0}`,
	)
	if !errors.Is(err, query.ErrScalarDivisionByZero) {
		t.Fatalf("transaction later conflict error = %T %v, want division by zero", err, err)
	}

	for id, want := range map[string]int64{"a": 8, "b": 9} {
		var got int64
		if err := tx.QueryRow(`
			SELECT n FROM upsert_expression_tx_atomic WHERE id = ?`, id,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("transaction row %q after failed statement = %d, want %d", id, got, want)
		}
	}
	var inserted int64
	if err := tx.QueryRow(`
		SELECT COUNT(*) FROM upsert_expression_tx_atomic WHERE id = 'c'`,
	).Scan(&inserted); err != nil {
		t.Fatal(err)
	}
	if inserted != 0 {
		t.Fatalf("interleaved insert survived failed transaction statement: count = %d", inserted)
	}

	// A statement-local failure must not poison the transaction. Commit a
	// subsequent write so the external view proves both properties together.
	if _, err := tx.Exec(`INSERT INTO upsert_expression_tx_atomic VALUES (?)`,
		`{"id":"z","n":1,"divisor":1}`,
	); err != nil {
		t.Fatalf("transaction was unusable after computed conflict failure: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	for id, want := range map[string]int64{"a": 8, "b": 9, "z": 1} {
		var got int64
		if err := db.QueryRow(`
			SELECT n FROM upsert_expression_tx_atomic WHERE id = ?`, id,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("committed row %q after failed statement = %d, want %d", id, got, want)
		}
	}
	assertSurfaceCount(
		t, db,
		`SELECT COUNT(*) FROM upsert_expression_tx_atomic WHERE id = 'c'`, 0,
	)
}

func TestUpsertExpressionsValidateBothNamespacesBeforeConflict(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE upsert_expression_validation (` +
		`id STRING PRIMARY KEY, n INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	const ambiguous = `INSERT INTO upsert_expression_validation VALUES (?) ` +
		`ON CONFLICT DO UPDATE SET n = n + EXCLUDED.n`
	prepared, err := db.Prepare(ambiguous)
	if prepared != nil {
		_ = prepared.Close()
	}
	var ambiguousColumn *sqlast.AmbiguousColumnError
	if !errors.As(err, &ambiguousColumn) {
		t.Fatalf("Prepare(bare current column) error = %T %v, want AmbiguousColumnError", err, err)
	}

	for _, source := range []string{
		`INSERT INTO upsert_expression_validation VALUES (?) ` +
			`ON CONFLICT DO UPDATE SET n = upsert_expression_validation.missing + EXCLUDED.n`,
		`INSERT INTO upsert_expression_validation VALUES (?) ` +
			`ON CONFLICT DO UPDATE SET n = upsert_expression_validation.n + EXCLUDED.missing`,
		`INSERT INTO upsert_expression_validation VALUES (?) ` +
			`ON CONFLICT DO UPDATE SET n = CASE ` +
			`WHEN EXCLUDED.missing > 0 THEN upsert_expression_validation.n ` +
			`ELSE EXCLUDED.n END`,
	} {
		prepared, err := db.Prepare(source)
		if prepared != nil {
			_ = prepared.Close()
		}
		var column *query.RelationColumnError
		if !errors.As(err, &column) {
			t.Fatalf("Prepare(%q) error = %T %v, want RelationColumnError", source, err, err)
		}
	}

	_, err = db.Exec(`
		INSERT INTO upsert_expression_validation VALUES (?)
		ON CONFLICT DO UPDATE SET n = upsert_expression_validation.n + ?`,
		`{"id":"new","n":1}`, struct{}{},
	)
	if err == nil {
		t.Fatal("invalid expression binding succeeded on the no-conflict branch")
	}
	assertSurfaceCount(
		t, db, `SELECT COUNT(*) FROM upsert_expression_validation`, 0,
	)
}

func TestApplyColumnAssignmentsWithExcludedFailsClosedOnComputedExpression(t *testing.T) {
	statement, err := sqlast.ParseStatement(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET n = docs.n + EXCLUDED.n`)
	if err != nil {
		t.Fatal(err)
	}
	assignments := statement.Insert.OnConflictUpdate.Assignments
	updated, err := ApplyColumnAssignmentsWithExcluded(
		[]byte(`{"id":"a","n":1}`),
		[]byte(`{"id":"a","n":2}`),
		assignments, nil, 1<<20,
	)
	if err == nil || !strings.Contains(err.Error(), "computed") {
		t.Fatalf("legacy conflict assignment result = %s, error = %v; want fail-closed computed error", updated, err)
	}
	if updated != nil {
		t.Fatalf("legacy conflict assignment returned a postimage on refusal: %s", updated)
	}
}

func assertUpsertExpressionIndexRow(
	t *testing.T,
	db interface {
		QueryRow(string, ...any) *sql.Row
	},
	id, wantHandle string,
	wantScore int64,
) {
	t.Helper()
	var handle string
	var score int64
	if err := db.QueryRow(`
		SELECT handle, score FROM upsert_expression_indexed WHERE id = ?`, id,
	).Scan(&handle, &score); err != nil {
		t.Fatal(err)
	}
	if handle != wantHandle || score != wantScore {
		t.Fatalf("indexed row %q = (%q,%d), want (%q,%d)",
			id, handle, score, wantHandle, wantScore)
	}
}
