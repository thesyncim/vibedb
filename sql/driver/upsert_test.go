package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibejson"
)

func TestInsertOnConflictDoUpdateWholeDocumentSchemaless(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"a","state":"old","removed":"yes"}`,
	); err != nil {
		t.Fatal(err)
	}

	result, err := db.Exec(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET "$doc" = EXCLUDED."$doc"`,
		`{"id":"a","state":"new","nested":{"kept":true}}`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("whole-document upsert affected = %d, %v; want 1", affected, err)
	}
	var state string
	var kept bool
	var removed any
	if err := db.QueryRow(`
		SELECT state, nested.kept, removed FROM docs WHERE id = 'a'
	`).Scan(&state, &kept, &removed); err != nil {
		t.Fatal(err)
	}
	if state != "new" || !kept || removed != nil {
		t.Fatalf("whole-document upsert = (%q, %v, %#v), want (new, true, nil)",
			state, kept, removed)
	}
}

func TestInsertOnConflictDoUpdateExcludedUsesEffectiveDuplicateValue(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"a","state":"old"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET state = EXCLUDED.state`,
		`{"id":"a","state":{},"state":"effective"}`,
	); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM docs WHERE id = 'a'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "effective" {
		t.Fatalf("effective duplicate EXCLUDED value = %q, want effective", state)
	}
}

func TestInsertOnConflictDoUpdateMixedValuesReturningAndRowsAffected(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			state STRING NOT NULL,
			score INTEGER NOT NULL,
			note STRING,
			marker STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"a","state":"old","score":1,"note":"old-note","marker":"old-marker"}`,
	); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		INSERT INTO docs VALUES (?), (?)
		ON CONFLICT DO UPDATE SET
			state = EXCLUDED.state,
			score = ?,
			note = NULL,
			marker = 'updated-literal'
		RETURNING id, state, score, note, marker`,
		`{"id":"a","state":"candidate-a","score":10,"note":"candidate-note","marker":"candidate-marker"}`,
		`{"id":"b","state":"inserted-b","score":2,"note":"inserted-note","marker":"inserted-marker"}`,
		int64(70),
	)
	if err != nil {
		t.Fatal(err)
	}
	type returnedRow struct {
		id, state, marker string
		score             int64
		note              []byte
	}
	var got []returnedRow
	for rows.Next() {
		var row returnedRow
		if err := rows.Scan(
			&row.id, &row.state, &row.score, &row.note, &row.marker,
		); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	want := []returnedRow{
		{id: "a", state: "candidate-a", score: 70, marker: "updated-literal"},
		{id: "b", state: "inserted-b", score: 2, note: []byte("inserted-note"), marker: "inserted-marker"},
	}
	if !slices.EqualFunc(got, want, func(a, b returnedRow) bool {
		return a.id == b.id && a.state == b.state && a.score == b.score &&
			slices.Equal(a.note, b.note) && a.marker == b.marker
	}) {
		t.Fatalf("mixed upsert RETURNING = %+v, want %+v", got, want)
	}

	result, err := db.Exec(`
		INSERT INTO docs VALUES (?), (?)
		ON CONFLICT DO UPDATE SET state = EXCLUDED.state, score = ?`,
		`{"id":"a","state":"second-a","score":11,"note":"a","marker":"a"}`,
		`{"id":"c","state":"inserted-c","score":3,"note":"c","marker":"c"}`,
		int64(80),
	)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 2 {
		t.Fatalf("mixed upsert affected = %d, %v; want 2", affected, err)
	}
}

func TestInsertOnConflictDoUpdateMaintainsLocalExactIndex(t *testing.T) {
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})
	ctx := context.Background()
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING NOT NULL)`,
		`CREATE INDEX by_state ON docs(state)`,
	} {
		prepared := runtimePrepare(t, session, statement)
		if _, err := prepared.Exec(ctx, nil); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
	}
	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	if _, err := insert.Exec(ctx, []any{`{"id":"a","state":"old"}`}); err != nil {
		t.Fatal(err)
	}
	if err := insert.Close(); err != nil {
		t.Fatal(err)
	}
	upsert := runtimePrepare(t, session, `
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET state = EXCLUDED.state`)
	if result, err := upsert.Exec(ctx, []any{`{"id":"a","state":"new"}`}); err != nil || result.RowsAffected != 1 {
		t.Fatalf("indexed upsert = (%+v, %v), want one affected row", result, err)
	}
	if err := upsert.Close(); err != nil {
		t.Fatal(err)
	}

	database.connector.db.mu.RLock()
	collection := database.connector.db.tables["docs"].collection
	database.connector.db.mu.RUnlock()
	if collection == nil {
		t.Fatal("indexed table was not materialized")
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	countIndexValue := func(value string) (int, string) {
		t.Helper()
		var entries [1]vibejson.IndexEntry
		needle, err := vibejson.BuildIndex([]byte(`"`+value+`"`), entries[:])
		if err != nil {
			t.Fatal(err)
		}
		masks, err := snapshot.AppendIndexMasks(nil, "by_state", needle)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		var document string
		if err := snapshot.RangeMasksRaw(masks, func(_, raw []byte) error {
			count++
			document = string(append([]byte(nil), raw...))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return count, document
	}
	if count, document := countIndexValue("old"); count != 0 {
		t.Fatalf("old exact-index posting count = %d, document %s; want 0", count, document)
	}
	if count, document := countIndexValue("new"); count != 1 || document != `{"id":"a","state":"new"}` {
		t.Fatalf("new exact-index posting = (%d, %s), want one updated document", count, document)
	}
}

func TestInsertOnConflictDoUpdateDuplicateCanonicalCandidateIsAtomic(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (id NUMBER PRIMARY KEY, value STRING NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":9,"value":"seed"}`,
	); err != nil {
		t.Fatal(err)
	}

	_, err := db.Exec(`
		INSERT INTO docs VALUES (?), (?)
		ON CONFLICT DO UPDATE SET value = EXCLUDED.value`,
		`{"id":1,"value":"first"}`,
		`{"id":1e0,"value":"second"}`,
	)
	if !errors.Is(err, ErrUpsertCardinality) ||
		!errors.Is(err, query.ErrCardinalityViolation) {
		t.Fatalf("duplicate canonical upsert error = %T %v, want ErrUpsertCardinality/ErrCardinalityViolation", err, err)
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 1)
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs WHERE id = 1`, 0)
	var value string
	if err := db.QueryRow(`SELECT value FROM docs WHERE id = 9`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "seed" {
		t.Fatalf("duplicate canonical upsert changed seed to %q", value)
	}
}

func TestInsertOnConflictDoUpdateFailureIsAtomic(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			required STRING NOT NULL,
			optional STRING
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","required":"old-a","optional":"old-optional-a"}`,
		`{"id":"b","required":"old-b","optional":"old-optional-b"}`,
	); err != nil {
		t.Fatal(err)
	}

	t.Run("later candidate", func(t *testing.T) {
		_, err := db.Exec(`
			INSERT INTO docs VALUES (?), (?)
			ON CONFLICT DO UPDATE SET required = EXCLUDED.required`,
			`{"id":"a","required":"candidate-a"}`,
			`{"id":"c"}`,
		)
		if err == nil {
			t.Fatal("upsert with invalid later candidate succeeded")
		}
		assertUpsertRequiredValues(t, db, map[string]string{
			"a": "old-a", "b": "old-b",
		})
		assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs WHERE id = 'c'`, 0)
	})

	t.Run("later post-image", func(t *testing.T) {
		_, err := db.Exec(`
			INSERT INTO docs VALUES (?), (?)
			ON CONFLICT DO UPDATE SET required = EXCLUDED.optional`,
			`{"id":"a","required":"candidate-a","optional":"new-a"}`,
			`{"id":"b","required":"candidate-b"}`,
		)
		if err == nil {
			t.Fatal("upsert with invalid later conflict post-image succeeded")
		}
		assertUpsertRequiredValues(t, db, map[string]string{
			"a": "old-a", "b": "old-b",
		})
	})
}

func TestInsertOnConflictDoUpdateValidatesActionWithoutConflict(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`INSERT INTO docs VALUES (?) ON CONFLICT DO UPDATE SET missing = 'x'`,
		`INSERT INTO docs VALUES (?) ON CONFLICT DO UPDATE SET state = EXCLUDED.missing`,
	} {
		prepared, err := db.Prepare(source)
		if prepared != nil {
			_ = prepared.Close()
		}
		if err == nil {
			t.Fatalf("Prepare(%q) accepted an undeclared conflict column", source)
		}
	}

	_, err := db.Exec(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET state = ?`,
		`{"id":"absent","state":"candidate"}`, struct{}{},
	)
	if err == nil {
		t.Fatal("invalid action binding succeeded on the insert branch")
	}
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs`, 0)
}

func TestInsertOnConflictDoUpdatePlacedRoutingIsAtomicAndImmutable(t *testing.T) {
	t.Run("cross-shard candidates", func(t *testing.T) {
		db, _, diff := openTwoShardDocs(t)
		_, err := db.Exec(`
			INSERT INTO docs VALUES (?), (?)
			ON CONFLICT DO UPDATE SET state = EXCLUDED.state`,
			tenantDoc(diff[0], "first"), tenantDoc(diff[1], "second"),
		)
		if !errors.Is(err, ErrCrossShardWrite) {
			t.Fatalf("cross-shard upsert error = %v, want ErrCrossShardWrite", err)
		}
		if got := countDocs(t, db); got != 0 {
			t.Fatalf("rows after refused cross-shard upsert = %d, want 0", got)
		}
	})

	t.Run("conflict post-image moves shard", func(t *testing.T) {
		db, _, diff := openTwoShardDocs(t)
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
			tenantDoc(diff[0], "old")); err != nil {
			t.Fatal(err)
		}
		_, err := db.Exec(`
			INSERT INTO docs VALUES (?)
			ON CONFLICT DO UPDATE SET tenant_id = ?`,
			tenantDoc(diff[0], "candidate"), diff[1],
		)
		if !errors.Is(err, ErrShardKeyImmutable) {
			t.Fatalf("shard-moving upsert error = %v, want ErrShardKeyImmutable", err)
		}
		var state string
		if err := db.QueryRow(
			`SELECT state FROM docs WHERE tenant_id = ?`, diff[0],
		).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state != "old" {
			t.Fatalf("state after refused shard move = %q, want old", state)
		}
	})
}

func TestInsertOnConflictDoUpdateTransactionVisibilityRollbackCommitAndDelete(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			state STRING NOT NULL,
			marker STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"a","state":"committed","marker":"base"}`,
	); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := tx.Exec(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET
			state = EXCLUDED.state, marker = 'rolled-back'`,
		`{"id":"a","state":"transaction","marker":"candidate"}`,
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	} else if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		_ = tx.Rollback()
		t.Fatalf("transactional upsert affected = %d, %v; want 1", affected, err)
	}
	assertUpsertState(t, tx, "a", "transaction", "rolled-back")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertUpsertState(t, db, "a", "committed", "base")

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"b","state":"staged","marker":"insert"}`,
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET marker = 'updated-staged'`,
		`{"id":"b","state":"ignored-candidate","marker":"ignored"}`,
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	assertUpsertState(t, tx, "b", "staged", "updated-staged")

	if _, err := tx.Exec(`DELETE FROM docs WHERE id = 'a'`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET marker = 'must-not-run'`,
		`{"id":"a","state":"reinserted","marker":"candidate-after-delete"}`,
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	assertUpsertState(t, tx, "a", "reinserted", "candidate-after-delete")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertUpsertState(t, db, "a", "reinserted", "candidate-after-delete")
	assertUpsertState(t, db, "b", "staged", "updated-staged")
}

func TestInsertOnConflictDoUpdateTransactionReturningSavepointAndConflict(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			state STRING NOT NULL,
			marker STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"a","state":"base","marker":"base"}`); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SAVEPOINT before_upsert`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var state, marker string
	if err := tx.QueryRow(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET state = EXCLUDED.state, marker = 'savepoint'
		RETURNING state, marker`,
		`{"id":"a","state":"inside","marker":"candidate"}`,
	).Scan(&state, &marker); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if state != "inside" || marker != "savepoint" {
		_ = tx.Rollback()
		t.Fatalf("transactional RETURNING = (%q, %q), want (inside, savepoint)", state, marker)
	}
	if _, err := tx.Exec(`ROLLBACK TO SAVEPOINT before_upsert`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	assertUpsertState(t, tx, "a", "base", "base")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO docs VALUES (?), (?)
		ON CONFLICT DO UPDATE SET state = EXCLUDED.state, marker = EXCLUDED.marker`,
		`{"id":"a","state":"loser","marker":"loser"}`,
		`{"id":"disjoint","state":"must-not-publish","marker":"tx"}`,
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE docs SET state = 'winner', marker = 'winner' WHERE id = 'a'`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("conflicting upsert Commit = %v, want ErrTransactionConflict", err)
	}
	assertUpsertState(t, db, "a", "winner", "winner")
	assertSurfaceCount(t, db, `SELECT COUNT(*) FROM docs WHERE id = 'disjoint'`, 0)
}

type upsertQueryRower interface {
	QueryRow(query string, args ...any) *stdsql.Row
}

func assertUpsertState(
	t *testing.T,
	queryer upsertQueryRower,
	id, wantState, wantMarker string,
) {
	t.Helper()
	var state, marker string
	if err := queryer.QueryRow(
		`SELECT state, marker FROM docs WHERE id = ?`, id,
	).Scan(&state, &marker); err != nil {
		t.Fatal(err)
	}
	if state != wantState || marker != wantMarker {
		t.Fatalf("row %q = (%q, %q), want (%q, %q)",
			id, state, marker, wantState, wantMarker)
	}
}

func assertUpsertRequiredValues(
	t *testing.T,
	db *stdsql.DB,
	want map[string]string,
) {
	t.Helper()
	rows, err := db.Query(`SELECT id, required FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[string]string, len(want))
	for rows.Next() {
		var id, required string
		if err := rows.Scan(&id, &required); err != nil {
			t.Fatal(err)
		}
		got[id] = required
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("rows after failed upsert = %v, want %v", got, want)
	}
	for id, required := range want {
		if got[id] != required {
			t.Fatalf("row %q after failed upsert = %q, want %q", id, got[id], required)
		}
	}
}
