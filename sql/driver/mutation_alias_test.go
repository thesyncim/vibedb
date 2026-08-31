package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestUpsertBareConflictColumnCatalogClassification(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE conflict_binding (
			id STRING PRIMARY KEY,
			n INTEGER,
			"CaseName" INTEGER,
			out INTEGER
		)`); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		rhs          string
		column       string
		matches      int
		wantSentinel error
	}{
		{
			name: "declared",
			rhs:  `n + EXCLUDED.n`, column: "n", matches: 2,
			wantSentinel: query.ErrAmbiguousColumn,
		},
		{
			name: "missing",
			rhs:  `missing + EXCLUDED.n`, column: "missing",
			wantSentinel: query.ErrUndefinedColumn,
		},
		{
			name: "quoted declared",
			rhs:  `"CaseName" + EXCLUDED.n`, column: "CaseName", matches: 2,
			wantSentinel: query.ErrAmbiguousColumn,
		},
		{
			name: "quoted case-sensitive miss",
			rhs:  `"casename" + EXCLUDED.n`, column: "casename",
			wantSentinel: query.ErrUndefinedColumn,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := `INSERT INTO conflict_binding AS target VALUES (?) ` +
				`ON CONFLICT DO UPDATE SET out = ` + test.rhs
			prepared, err := db.Prepare(source)
			if prepared != nil {
				_ = prepared.Close()
			}
			var column *query.RelationColumnError
			if !errors.As(err, &column) ||
				!errors.Is(err, test.wantSentinel) {
				t.Fatalf("Prepare error = %T %v, want RelationColumnError wrapping %v", err, err, test.wantSentinel)
			}
			wantPos := strings.Index(source, test.rhs)
			if column.Relation != "target" || column.Column != test.column ||
				column.Matches != test.matches || column.Pos != wantPos {
				t.Fatalf(
					"column error = %+v, want relation target column %q matches %d pos %d",
					column, test.column, test.matches, wantPos,
				)
			}
		})
	}

	const invalidTarget = `INSERT INTO conflict_binding AS target VALUES (?) ` +
		`ON CONFLICT DO UPDATE SET missing_target = n`
	prepared, err := db.Prepare(invalidTarget)
	if prepared != nil {
		_ = prepared.Close()
	}
	var target *query.RelationColumnError
	if !errors.As(err, &target) || !errors.Is(err, query.ErrUndefinedColumn) ||
		target.Column != "missing_target" || target.Matches != 0 ||
		target.Pos != strings.Index(invalidTarget, "missing_target") {
		t.Fatalf("invalid SET target precedence = %+v / %v", target, err)
	}

	const missingExcluded = `INSERT INTO conflict_binding AS target VALUES (?) ` +
		`ON CONFLICT DO UPDATE SET out = EXCLUDED.missing`
	prepared, err = db.Prepare(missingExcluded)
	if prepared != nil {
		_ = prepared.Close()
	}
	var excluded *query.RelationColumnError
	if !errors.As(err, &excluded) || !errors.Is(err, query.ErrUndefinedColumn) ||
		excluded.Relation != "EXCLUDED" || excluded.Column != "missing" {
		t.Fatalf("missing EXCLUDED column = %+v / %v", excluded, err)
	}
}

func TestUpsertConflictAssignmentsBindInAuthoredOrder(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE conflict_assignment_order (` +
		`id STRING PRIMARY KEY, n INTEGER, out INTEGER)`); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		firstRHS     string
		marker       string
		relation     string
		column       string
		matches      int
		wantSentinel error
	}{
		{
			name:     "first direct excluded miss before later target",
			firstRHS: `EXCLUDED.missing`, marker: "EXCLUDED.missing",
			relation: "EXCLUDED", column: "missing",
			wantSentinel: query.ErrUndefinedColumn,
		},
		{
			name:     "first scalar miss before later target",
			firstRHS: `target.missing + EXCLUDED.n`, marker: "target.missing",
			relation: "target", column: "missing",
			wantSentinel: query.ErrUndefinedColumn,
		},
		{
			name:     "first bare ambiguity before later target",
			firstRHS: `n + EXCLUDED.n`, marker: "n + EXCLUDED",
			relation: "target", column: "n", matches: 2,
			wantSentinel: query.ErrAmbiguousColumn,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := `INSERT INTO conflict_assignment_order AS target VALUES (?) ` +
				`ON CONFLICT DO UPDATE SET out = ` + test.firstRHS +
				`, missing_target = EXCLUDED.n`
			prepared, err := db.Prepare(source)
			if prepared != nil {
				_ = prepared.Close()
			}
			var column *query.RelationColumnError
			if !errors.As(err, &column) ||
				!errors.Is(err, test.wantSentinel) ||
				column.Relation != test.relation ||
				column.Column != test.column ||
				column.Matches != test.matches ||
				column.Pos != strings.Index(source, test.marker) {
				t.Fatalf(
					"assignment-order error = %+v / %v, want relation %q column %q matches %d at %d",
					column, err, test.relation, test.column, test.matches,
					strings.Index(source, test.marker),
				)
			}
		})
	}
}

func TestMutationTargetAliasesExecuteAgainstPhysicalTable(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE excluded (` +
			`id STRING PRIMARY KEY, n INTEGER, out INTEGER)`,
		`INSERT INTO excluded VALUES ` +
			`('{"id":"same","n":4,"out":0}'), ` +
			`('{"id":"z","n":8,"out":0}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	var out int64
	if err := db.QueryRow(`
		INSERT INTO excluded AS target VALUES (?)
		ON CONFLICT DO UPDATE SET out = target.n + EXCLUDED.out
		RETURNING target.out`,
		`{"id":"same","n":2,"out":3}`,
	).Scan(&out); err != nil {
		t.Fatal(err)
	}
	if out != 7 {
		t.Fatalf("aliased table-named-excluded upsert out = %d, want 7", out)
	}

	var id string
	var n int64
	if err := db.QueryRow(`
		UPDATE excluded AS target
		SET n = target.n + 1
		WHERE target.n >= 4
		ORDER BY target.id LIMIT 1
		RETURNING target.id, target.n`,
	).Scan(&id, &n); err != nil {
		t.Fatal(err)
	}
	if id != "same" || n != 5 {
		t.Fatalf("AS-aliased bounded UPDATE row = (%q,%d), want (same,5)", id, n)
	}
	if err := db.QueryRow(`
		UPDATE excluded target
		SET out = target.out + 1
		WHERE target.id = 'same'
		RETURNING target.out`,
	).Scan(&out); err != nil {
		t.Fatal(err)
	}
	if out != 8 {
		t.Fatalf("bare-aliased UPDATE out = %d, want 8", out)
	}
	if err := db.QueryRow(
		`SELECT n, out FROM excluded WHERE id = 'same'`,
	).Scan(&n, &out); err != nil {
		t.Fatal(err)
	}
	if n != 5 || out != 8 {
		t.Fatalf("physical table row = (%d,%d), want (5,8)", n, out)
	}
}

func TestUpsertAliasPreparedRevalidatesRecreatedTable(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	if err := testRuntimeExec(session, `
		CREATE TABLE prepared_alias (
			id STRING PRIMARY KEY,
			n INTEGER,
			out INTEGER
		)`, nil); err != nil {
		t.Fatal(err)
	}
	const source = `INSERT INTO prepared_alias AS target VALUES (?) ` +
		`ON CONFLICT DO UPDATE SET out = target.n + EXCLUDED.out`
	prepared := runtimePrepare(t, session, source)

	for _, statement := range []string{
		`DROP TABLE prepared_alias`,
		`CREATE TABLE prepared_alias (id STRING PRIMARY KEY, out INTEGER)`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := prepared.Exec(
		ctx, []any{`{"id":"not-published","out":3}`},
	); err == nil {
		t.Fatal("stale prepared upsert accepted a removed target column")
	} else {
		var column *query.RelationColumnError
		if !errors.As(err, &column) ||
			!errors.Is(err, query.ErrUndefinedColumn) ||
			column.Relation != "target" || column.Column != "n" ||
			column.Pos != strings.Index(source, "target.n") {
			t.Fatalf("stale prepared error = %+v / %v", column, err)
		}
	}
	count := runtimePrepare(t, session,
		`SELECT count(*) FROM prepared_alias`)
	cursor, err := count.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		_ = cursor.Close()
		t.Fatal("count returned no row")
	}
	if got, ok := cursor.Cell(0).Int64(); !ok || got != 0 {
		_ = cursor.Close()
		t.Fatalf("incompatible recreation row count = (%d, %v), want zero", got, ok)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}

	for _, statement := range []string{
		`DROP TABLE prepared_alias`,
		`CREATE TABLE prepared_alias (id STRING PRIMARY KEY, n INTEGER, out INTEGER)`,
		`INSERT INTO prepared_alias VALUES ('{"id":"same","n":4,"out":0}')`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	result, err := prepared.Exec(
		ctx, []any{`{"id":"same","n":2,"out":3}`},
	)
	if err != nil {
		t.Fatalf("compatible recreation reuse: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("compatible recreation affected %d rows, want 1", result.RowsAffected)
	}
	lookup := runtimePrepare(t, session,
		`SELECT n, out FROM prepared_alias WHERE id = 'same'`)
	row, err := lookup.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer row.Close()
	if !row.Next() {
		t.Fatal("compatible recreation returned no row")
	}
	n, nOK := row.Cell(0).Int64()
	out, outOK := row.Cell(1).Int64()
	if !nOK || n != 4 || !outOK || out != 7 {
		t.Fatalf("compatible recreation row = n(%d,%v) out(%d,%v), want 4,7", n, nOK, out, outOK)
	}
}

func TestUpsertBareConflictClassificationUsesTransactionIncarnation(t *testing.T) {
	tests := []struct {
		name         string
		before       string
		after        string
		matches      int
		wantSentinel error
	}{
		{
			name: "declared before missing after",
			before: `CREATE TABLE tx_binding (` +
				`id STRING PRIMARY KEY, n INTEGER, out INTEGER)`,
			after: `CREATE TABLE tx_binding (` +
				`id STRING PRIMARY KEY, out INTEGER)`,
			matches: 2, wantSentinel: query.ErrAmbiguousColumn,
		},
		{
			name: "missing before declared after",
			before: `CREATE TABLE tx_binding (` +
				`id STRING PRIMARY KEY, out INTEGER)`,
			after: `CREATE TABLE tx_binding (` +
				`id STRING PRIMARY KEY, n INTEGER, out INTEGER)`,
			matches: 0, wantSentinel: query.ErrUndefinedColumn,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			database, transactionSession := openRuntimeSession(t)
			defer database.Close()
			defer transactionSession.Close()
			ddlSession, err := database.NewSession(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer ddlSession.Close()
			if err := testRuntimeExec(ddlSession, test.before, nil); err != nil {
				t.Fatal(err)
			}
			if err := transactionSession.Begin(ctx, TxOptions{
				Isolation: IsolationRepeatableRead,
			}); err != nil {
				t.Fatal(err)
			}
			if err := testRuntimeExec(ddlSession,
				`DROP TABLE tx_binding`, nil,
			); err != nil {
				t.Fatal(err)
			}
			if err := testRuntimeExec(ddlSession, test.after, nil); err != nil {
				t.Fatal(err)
			}

			const source = `INSERT INTO tx_binding AS target VALUES (?) ` +
				`ON CONFLICT DO UPDATE SET out = n + EXCLUDED.out`
			prepared, err := transactionSession.Prepare(ctx, source)
			if prepared != nil {
				_ = prepared.Close()
			}
			var column *query.RelationColumnError
			if !errors.As(err, &column) ||
				!errors.Is(err, test.wantSentinel) ||
				column.Relation != "target" || column.Column != "n" ||
				column.Matches != test.matches ||
				column.Pos != strings.Index(source, "n + EXCLUDED") {
				t.Fatalf(
					"transaction-incarnation error = %+v / %v, want matches %d wrapping %v",
					column, err, test.matches, test.wantSentinel,
				)
			}
			if err := transactionSession.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
