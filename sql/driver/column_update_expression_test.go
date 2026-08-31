package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestUpdateExpressionsAutocommitSimultaneousPerRowReturning(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE update_expression_metrics (
			id STRING PRIMARY KEY,
			grp STRING NOT NULL,
			a INTEGER NOT NULL,
			b INTEGER NOT NULL,
			total INTEGER NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO update_expression_metrics VALUES (?), (?), (?)`,
		`{"id":"a","grp":"selected","a":10,"b":2,"total":0}`,
		`{"id":"b","grp":"selected","a":20,"b":3,"total":0}`,
		`{"id":"c","grp":"other","a":30,"b":4,"total":0}`,
	); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		UPDATE update_expression_metrics
		SET a = b, b = a, total = a + b
		WHERE grp = 'selected' ORDER BY id LIMIT 10
		RETURNING id, a, b, total`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []struct {
		id          string
		a, b, total int64
	}{
		{id: "a", a: 2, b: 10, total: 12},
		{id: "b", a: 3, b: 20, total: 23},
	}
	for i := range want {
		if !rows.Next() {
			t.Fatalf("RETURNING ended at row %d: %v", i, rows.Err())
		}
		var id string
		var a, b, total int64
		if err := rows.Scan(&id, &a, &b, &total); err != nil {
			t.Fatal(err)
		}
		if id != want[i].id || a != want[i].a || b != want[i].b || total != want[i].total {
			t.Fatalf(
				"RETURNING row %d = (%q,%d,%d,%d), want (%q,%d,%d,%d)",
				i, id, a, b, total, want[i].id, want[i].a, want[i].b, want[i].total,
			)
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected trailing RETURNING row or error: %v", rows.Err())
	}

	var a, b, total int64
	if err := db.QueryRow(`
		SELECT a, b, total FROM update_expression_metrics WHERE id = 'c'`,
	).Scan(&a, &b, &total); err != nil {
		t.Fatal(err)
	}
	if a != 30 || b != 4 || total != 0 {
		t.Fatalf("filtered row = (%d,%d,%d), want (30,4,0)", a, b, total)
	}
}

func TestUpdateExpressionsExactArithmeticAndAtomicErrors(t *testing.T) {
	t.Run("exact expression parameter", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`
			CREATE TABLE update_expression_exact (
				id STRING PRIMARY KEY,
				n NUMBER NOT NULL
			)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO update_expression_exact VALUES (?)`,
			`{"id":"wide","n":9007199254740993}`,
		); err != nil {
			t.Fatal(err)
		}

		var got string
		if err := db.QueryRow(`
			UPDATE update_expression_exact SET n = n + ? WHERE id = ? RETURNING n`,
			int64(1), "wide",
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != "9007199254740994" {
			t.Fatalf("exact UPDATE result = %q, want 9007199254740994", got)
		}
	})

	t.Run("lazy filter and later-row rollback", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`
			CREATE TABLE update_expression_division (
				id STRING PRIMARY KEY,
				grp STRING NOT NULL,
				n INTEGER NOT NULL,
				divisor INTEGER NOT NULL
			)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO update_expression_division VALUES (?), (?), (?)`,
			`{"id":"a","grp":"selected","n":8,"divisor":2}`,
			`{"id":"b","grp":"selected","n":9,"divisor":0}`,
			`{"id":"c","grp":"other","n":7,"divisor":0}`,
		); err != nil {
			t.Fatal(err)
		}

		var n int64
		if err := db.QueryRow(`
			UPDATE update_expression_division
			SET n = n / divisor WHERE id = 'a' RETURNING n`,
		).Scan(&n); err != nil {
			t.Fatalf("filtered-out zero divisor was evaluated: %v", err)
		}
		if n != 4 {
			t.Fatalf("first quotient = %d, want 4", n)
		}

		_, err := db.Exec(`
			UPDATE update_expression_division
			SET n = n / divisor
			WHERE grp = 'selected' ORDER BY id LIMIT 10`)
		if !errors.Is(err, query.ErrScalarDivisionByZero) {
			t.Fatalf("later-row division error = %T %v, want ErrScalarDivisionByZero", err, err)
		}
		rows, err := db.Query(`
			SELECT id, n FROM update_expression_division ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		want := []struct {
			id string
			n  int64
		}{{"a", 4}, {"b", 9}, {"c", 7}}
		for i := range want {
			if !rows.Next() {
				t.Fatalf("verification ended at row %d: %v", i, rows.Err())
			}
			var id string
			if err := rows.Scan(&id, &n); err != nil {
				t.Fatal(err)
			}
			if id != want[i].id || n != want[i].n {
				t.Fatalf("row %d after failed UPDATE = (%q,%d), want (%q,%d)",
					i, id, n, want[i].id, want[i].n)
			}
		}
		if rows.Next() || rows.Err() != nil {
			t.Fatalf("unexpected trailing verification row or error: %v", rows.Err())
		}
	})
}

func TestCanonicalComputedInteger(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		maxBytes int
		want     string
		wantOK   bool
	}{
		{name: "positive exponent", value: "1.2e1", maxBytes: 32, want: "12", wantOK: true},
		{name: "negative and fractional zeros", value: "-1.20e2", maxBytes: 32, want: "-120", wantOK: true},
		{name: "non-integral unchanged", value: "1.2", maxBytes: 32},
		{name: "zero canonicalized", value: "-0.0e1000", maxBytes: 32, want: "0", wantOK: true},
		{name: "huge exponent bounded", value: "1e1000", maxBytes: 32},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := canonicalComputedInteger([]byte(test.value), test.maxBytes)
			if ok != test.wantOK || string(got) != test.want {
				t.Fatalf("canonicalComputedInteger(%q, %d) = (%q,%v), want (%q,%v)",
					test.value, test.maxBytes, got, ok, test.want, test.wantOK)
			}
		})
	}
}

func TestUpdateExpressionsMaintainSecondaryAndUniqueIndexPostimages(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE update_expression_indexed (` +
			`id STRING PRIMARY KEY, handle STRING NOT NULL, ` +
			`next_handle STRING NOT NULL, score INTEGER NOT NULL)`,
		`CREATE INDEX update_expression_by_score ON update_expression_indexed (score)`,
		`CREATE UNIQUE INDEX update_expression_by_handle ON update_expression_indexed (handle)`,
		`INSERT INTO update_expression_indexed VALUES (` +
			`'{"id":"a","handle":"alpha","next_handle":"alpha","score":1}')`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatalf("setup %q: %v", source, err)
		}
	}

	if _, err := db.Exec(`
		UPDATE update_expression_indexed
		SET handle = handle || '-v2', score = score + 10
		WHERE id = 'a'`); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		predicate string
		want      int64
	}{
		{predicate: `score = 1`, want: 0},
		{predicate: `score = 11`, want: 1},
	} {
		var count int64
		if err := db.QueryRow(`SELECT COUNT(*) FROM update_expression_indexed WHERE ` + check.predicate).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != check.want {
			t.Fatalf("indexed %s count = %d, want %d", check.predicate, count, check.want)
		}
	}

	// The old unique value must be released and the computed postimage claimed.
	if _, err := db.Exec(`INSERT INTO update_expression_indexed VALUES (?)`,
		`{"id":"b","handle":"alpha","next_handle":"unused","score":2}`,
	); err != nil {
		t.Fatalf("old unique value remained claimed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO update_expression_indexed VALUES (?)`,
		`{"id":"c","handle":"alpha-v2","next_handle":"unused","score":3}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("computed unique postimage was not claimed: %v", err)
	}

	// A computed collision must leave both the primary row and secondary index
	// posting at their prior postimage.
	_, err := db.Exec(`
		UPDATE update_expression_indexed
		SET handle = next_handle, score = score + 100
		WHERE id = 'a'`)
	if !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("computed unique collision = %v, want ErrUniqueConstraint", err)
	}
	var handle string
	var score int64
	if err := db.QueryRow(`
		SELECT handle, score FROM update_expression_indexed WHERE id = 'a'`,
	).Scan(&handle, &score); err != nil {
		t.Fatal(err)
	}
	if handle != "alpha-v2" || score != 11 {
		t.Fatalf("row after computed unique collision = (%q,%d), want (alpha-v2,11)", handle, score)
	}
}

func TestUpdateExpressionPrimaryKeyIsImmutableAtomically(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE update_expression_pk (` +
		`id STRING PRIMARY KEY, n INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO update_expression_pk VALUES (?)`,
		`{"id":"a","n":1}`,
	); err != nil {
		t.Fatal(err)
	}

	_, err := db.Exec(`
		UPDATE update_expression_pk
		SET id = id || '-moved', n = n + 1
		WHERE id = 'a'`)
	if !errors.Is(err, ErrUpdatePrimaryKey) {
		t.Fatalf("computed primary-key move = %v, want ErrUpdatePrimaryKey", err)
	}
	var id string
	var n int64
	if err := db.QueryRow(`SELECT id, n FROM update_expression_pk`).Scan(&id, &n); err != nil {
		t.Fatal(err)
	}
	if id != "a" || n != 1 {
		t.Fatalf("row after primary-key refusal = (%q,%d), want (a,1)", id, n)
	}
}

func TestUpdateExpressionsTransactionReadYourWritesAndSavepointRollback(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE update_expression_tx (` +
		`id STRING PRIMARY KEY, n INTEGER NOT NULL, mirror INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO update_expression_tx VALUES (?)`,
		`{"id":"a","n":5,"mirror":0}`,
	); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SAVEPOINT before_expression`); err != nil {
		t.Fatal(err)
	}
	var n, mirror int64
	if err := tx.QueryRow(`
		UPDATE update_expression_tx
		SET n = n + 1, mirror = n
		WHERE id = 'a' RETURNING n, mirror`,
	).Scan(&n, &mirror); err != nil {
		t.Fatal(err)
	}
	if n != 6 || mirror != 5 {
		t.Fatalf("transaction RETURNING = (%d,%d), want simultaneous (6,5)", n, mirror)
	}
	if err := tx.QueryRow(`
		SELECT n, mirror FROM update_expression_tx WHERE id = 'a'`,
	).Scan(&n, &mirror); err != nil {
		t.Fatal(err)
	}
	if n != 6 || mirror != 5 {
		t.Fatalf("transaction read-your-writes = (%d,%d), want (6,5)", n, mirror)
	}
	if _, err := tx.Exec(`ROLLBACK TO SAVEPOINT before_expression`); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`
		SELECT n, mirror FROM update_expression_tx WHERE id = 'a'`,
	).Scan(&n, &mirror); err != nil {
		t.Fatal(err)
	}
	if n != 5 || mirror != 0 {
		t.Fatalf("row after savepoint rollback = (%d,%d), want (5,0)", n, mirror)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`
		SELECT n, mirror FROM update_expression_tx WHERE id = 'a'`,
	).Scan(&n, &mirror); err != nil {
		t.Fatal(err)
	}
	if n != 5 || mirror != 0 {
		t.Fatalf("committed row after savepoint rollback = (%d,%d), want (5,0)", n, mirror)
	}
}

func TestUpdateExpressionCaptureIsTypedPositionedAndVisitsNothing(t *testing.T) {
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})
	ctx := context.Background()
	create := runtimePrepare(t, session,
		`CREATE TABLE update_expression_capture (`+
			`id STRING PRIMARY KEY, state STRING NOT NULL)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session,
		`INSERT INTO update_expression_capture VALUES (?)`)
	if _, err := insert.Exec(ctx, []any{`{"id":"a","state":"old"}`}); err != nil {
		t.Fatal(err)
	}

	const source = `UPDATE update_expression_capture SET state = state || '!' WHERE id = 'a'`
	capture := runtimePrepare(t, session, source)
	visits := 0
	err := capture.CaptureMutationInto(ctx, nil, func(_, _ []byte) error {
		visits++
		return nil
	})
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("capture refusal = %T %v, want FeatureNotSupportedError", err, err)
	}
	wantPos := strings.Index(source, "||")
	if unsupported.Pos != wantPos || unsupported.Line != 1 || unsupported.Col != wantPos+1 {
		t.Fatalf("capture refusal position = %+v, want byte %d", unsupported.ParseError, wantPos)
	}
	if visits != 0 {
		t.Fatalf("capture visitor calls = %d, want 0", visits)
	}
}

func TestApplyColumnAssignmentsFailsClosedOnComputedExpression(t *testing.T) {
	statement, err := sqlast.ParseStatement(
		`UPDATE docs SET n = n + 1 WHERE id = 'a'`,
	)
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"id":"a","n":1}`)
	updated, err := ApplyColumnAssignments(
		document, statement.Update.Assignments, nil, 1<<20,
	)
	if err == nil || !strings.Contains(err.Error(), "computed") {
		t.Fatalf("legacy assignment result = %s, error = %v; want fail-closed computed-expression error", updated, err)
	}
	if updated != nil {
		t.Fatalf("legacy assignment returned a postimage on refusal: %s", updated)
	}
}

func TestMaterializePreparedUpdateAssignmentsIsSimultaneousAndReusable(t *testing.T) {
	const source = `UPDATE docs SET n = n + ?, mirror = n, label = ? WHERE id = ?`
	parsed, err := sqlast.ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := query.PrepareParsedDML(source, parsed)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	args := []any{int64(1), "changed", "row"}
	if err := statement.ValidateUpdateExpressionBindings(args); err != nil {
		t.Fatal(err)
	}
	var exec query.Exec
	for _, test := range []struct {
		old  string
		want string
	}{
		{
			old:  `{"id":"row","label":"old","mirror":0,"n":5}`,
			want: `{"id":"row","label":"changed","mirror":5,"n":6}`,
		},
		{
			old:  `{"id":"row","label":"again","mirror":7,"n":10}`,
			want: `{"id":"row","label":"changed","mirror":10,"n":11}`,
		},
	} {
		updated, materializeErr := MaterializePreparedUpdateAssignments(
			statement, &exec, []byte(test.old), args, 1<<20,
		)
		if materializeErr != nil || string(updated) != test.want {
			t.Fatalf("materialized %s = %s, %v; want %s", test.old, updated, materializeErr, test.want)
		}
	}
}
