package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func isolationCount(t testing.TB, tx *stdsql.Tx) int64 {
	t.Helper()
	var count int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestDatabaseSQLReadCommittedRefreshAndRepeatableReadCut(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, value) VALUES ('a', 'before')`); err != nil {
		t.Fatal(err)
	}

	rc, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, rc); got != 1 {
		t.Fatalf("READ COMMITTED first count = %d, want 1", got)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, value) VALUES ('b', 'outside')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE docs SET "$doc" = '{"id":"a","value":"after"}' WHERE id = 'a'`); err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, rc); got != 2 {
		t.Fatalf("READ COMMITTED refreshed count = %d, want 2", got)
	}
	var value string
	if err := rc.QueryRow(`SELECT value FROM docs WHERE id = 'a'`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "after" {
		t.Fatalf("READ COMMITTED refreshed value = %q, want after", value)
	}
	if err := rc.Rollback(); err != nil {
		t.Fatal(err)
	}

	rr, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, rr); got != 2 {
		t.Fatalf("REPEATABLE READ first count = %d, want 2", got)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, value) VALUES ('c', 'later')`); err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, rr); got != 2 {
		t.Fatalf("REPEATABLE READ second count = %d, want fixed 2", got)
	}
	if err := rr.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestReadCommittedPreservesOverlayAndSavepointsAcrossRefresh(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO docs (id) VALUES ('kept')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SAVEPOINT stable`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs (id) VALUES ('rolled')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id) VALUES ('outside')`); err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, tx); got != 3 {
		t.Fatalf("refreshed overlay count = %d, want 3", got)
	}
	if _, err := tx.Exec(`ROLLBACK TO stable`); err != nil {
		t.Fatal(err)
	}
	if got := isolationCount(t, tx); got != 2 {
		t.Fatalf("rollback-to after refresh count = %d, want 2", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestReadCommittedDirtyKeysKeepTheirStatementObservation(t *testing.T) {
	t.Run("freshly observed key", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO docs (id, value) VALUES ('key', 'initial')`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelReadCommitted,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`UPDATE docs SET "$doc" = '{"id":"key","value":"outside"}' WHERE id = 'key'`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(
			`UPDATE docs SET "$doc" = '{"id":"key","value":"transaction"}' WHERE id = 'key'`,
		); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("fresh-cut writer conflicted with an already observed write: %v", err)
		}
	})

	t.Run("earlier pending key", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('early', 'initial'), ('late', 'initial')`,
		); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelReadCommitted,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(
			`UPDATE docs SET "$doc" = '{"id":"early","value":"pending"}' WHERE id = 'early'`,
		); err != nil {
			t.Fatal(err)
		}
		for _, statement := range []string{
			`UPDATE docs SET "$doc" = '{"id":"early","value":"outside"}' WHERE id = 'early'`,
			`UPDATE docs SET "$doc" = '{"id":"late","value":"outside"}' WHERE id = 'late'`,
		} {
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.Exec(
			`UPDATE docs SET "$doc" = '{"id":"late","value":"pending"}' WHERE id = 'late'`,
		); err != nil {
			t.Fatal(err)
		}
		// Restaging the early overlay after another refreshed statement must not
		// replace the key's original observation with the newer revision.
		if _, err := tx.Exec(
			`UPDATE docs SET "$doc" = '{"id":"early","value":"restaged"}' WHERE id = 'early'`,
		); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("COMMIT = %v, want earlier-key ErrTransactionConflict", err)
		}
	})
}

func TestSerializableRejectsWriteSkew(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, on_call BOOL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, on_call) VALUES ('a', true), ('b', true)`); err != nil {
		t.Fatal(err)
	}
	begin := func() *stdsql.Tx {
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelSerializable,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	left, right := begin(), begin()
	for _, tx := range []*stdsql.Tx{left, right} {
		var count int64
		if err := tx.QueryRow(`SELECT COUNT(*) FROM docs WHERE on_call = true`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("on-call count = %d, want 2", count)
		}
	}
	if _, err := left.Exec(`UPDATE docs SET "$doc" = '{"id":"a","on_call":false}' WHERE id = 'a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Exec(`UPDATE docs SET "$doc" = '{"id":"b","on_call":false}' WHERE id = 'b'`); err != nil {
		t.Fatal(err)
	}
	if err := left.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := right.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("second serializable COMMIT = %v, want ErrTransactionConflict", err)
	}
}

func TestSerializableAllowsDisjointPrimaryPointMutations(t *testing.T) {
	begin := func(t *testing.T, db *stdsql.DB) *stdsql.Tx {
		t.Helper()
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelSerializable,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}

	t.Run("insert", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		left, right := begin(t, db), begin(t, db)
		if _, err := left.Exec(`INSERT INTO docs (id) VALUES ('left')`); err != nil {
			t.Fatal(err)
		}
		if _, err := right.Exec(`INSERT INTO docs (id) VALUES ('right')`); err != nil {
			t.Fatal(err)
		}
		if err := left.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := right.Commit(); err != nil {
			t.Fatalf("disjoint exact INSERT conflicted: %v", err)
		}
	})

	t.Run("update", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('left', 'old'), ('right', 'old')`,
		); err != nil {
			t.Fatal(err)
		}
		left, right := begin(t, db), begin(t, db)
		if _, err := left.Exec(
			`UPDATE docs SET "$doc" = '{"id":"left","value":"new"}' WHERE id = 'left'`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := right.Exec(
			`UPDATE docs SET "$doc" = '{"id":"right","value":"new"}' WHERE id = 'right'`,
		); err != nil {
			t.Fatal(err)
		}
		if err := left.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := right.Commit(); err != nil {
			t.Fatalf("disjoint exact UPDATE conflicted: %v", err)
		}
	})
}

func TestSerializablePrimaryPointReadsAreExactAndOwned(t *testing.T) {
	begin := func(t *testing.T, db *stdsql.DB) *stdsql.Tx {
		t.Helper()
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelSerializable,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	seed := func(t *testing.T, db *stdsql.DB) {
		t.Helper()
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('left', 'old'), ('right', 'old')`,
		); err != nil {
			t.Fatal(err)
		}
	}
	read := func(t *testing.T, tx *stdsql.Tx, key string) {
		t.Helper()
		var value string
		if err := tx.QueryRow(
			`SELECT value FROM docs WHERE id = ?`, key,
		).Scan(&value); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("unrelated write", func(t *testing.T) {
		db := openTestDB(t)
		seed(t, db)
		reader := begin(t, db)
		read(t, reader, "left")
		if _, err := db.Exec(
			`UPDATE docs SET "$doc" = '{"id":"right","value":"outside"}' WHERE id = 'right'`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Exec(
			`INSERT INTO docs (id, value) VALUES ('guard', 'tx')`,
		); err != nil {
			t.Fatal(err)
		}
		if err := reader.Commit(); err != nil {
			t.Fatalf("unrelated point write caused a false conflict: %v", err)
		}
	})

	t.Run("connection arena reuse", func(t *testing.T) {
		db := openTestDB(t)
		seed(t, db)
		reader := begin(t, db)
		read(t, reader, "left")
		read(t, reader, "right")
		if _, err := db.Exec(
			`UPDATE docs SET "$doc" = '{"id":"left","value":"outside"}' WHERE id = 'left'`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Exec(
			`INSERT INTO docs (id, value) VALUES ('guard', 'tx')`,
		); err != nil {
			t.Fatal(err)
		}
		if err := reader.Commit(); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("owned point-read COMMIT = %v, want ErrTransactionConflict", err)
		}
	})
}

func TestSerializableExactReadConflictsRemainFailClosed(t *testing.T) {
	begin := func(t *testing.T, db *stdsql.DB) *stdsql.Tx {
		t.Helper()
		tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
			Isolation: stdsql.LevelSerializable,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	stageGuard := func(t *testing.T, tx *stdsql.Tx) {
		t.Helper()
		if _, err := tx.Exec(`INSERT INTO docs (id, value) VALUES ('guard', 'tx')`); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("miss followed by insert", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		reader := begin(t, db)
		var value string
		if err := reader.QueryRow(
			`SELECT value FROM docs WHERE id = 'missing'`,
		).Scan(&value); !errors.Is(err, stdsql.ErrNoRows) {
			t.Fatalf("point miss = %v, want sql.ErrNoRows", err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('missing', 'outside')`,
		); err != nil {
			t.Fatal(err)
		}
		stageGuard(t, reader)
		if err := reader.Commit(); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("point-miss COMMIT = %v, want ErrTransactionConflict", err)
		}
	})

	t.Run("point DML miss followed by insert", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		reader := begin(t, db)
		result, err := reader.Exec(
			`UPDATE docs SET "$doc" = '{"id":"missing","value":"tx"}' WHERE id = 'missing'`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 0 {
			t.Fatalf("point UPDATE miss affected = %d, %v; want 0, nil", affected, err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('missing', 'outside')`,
		); err != nil {
			t.Fatal(err)
		}
		stageGuard(t, reader)
		if err := reader.Commit(); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("point-DML-miss COMMIT = %v, want ErrTransactionConflict", err)
		}
	})

	t.Run("ABA", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('watched', 'same')`,
		); err != nil {
			t.Fatal(err)
		}
		reader := begin(t, db)
		var value string
		if err := reader.QueryRow(
			`SELECT value FROM docs WHERE id = 'watched'`,
		).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DELETE FROM docs WHERE id = 'watched'`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('watched', 'same')`,
		); err != nil {
			t.Fatal(err)
		}
		stageGuard(t, reader)
		if err := reader.Commit(); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("ABA COMMIT = %v, want ErrTransactionConflict", err)
		}
	})

	t.Run("coarse promotion survives savepoint rollback", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('seed', 'watched')`,
		); err != nil {
			t.Fatal(err)
		}
		reader := begin(t, db)
		if _, err := reader.Exec(`SAVEPOINT before_reads`); err != nil {
			t.Fatal(err)
		}
		var value string
		if err := reader.QueryRow(
			`SELECT value FROM docs WHERE id = 'seed'`,
		).Scan(&value); err != nil {
			t.Fatal(err)
		}
		var count int64
		if err := reader.QueryRow(
			`SELECT COUNT(*) FROM docs WHERE value = 'watched'`,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Exec(`ROLLBACK TO before_reads`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO docs (id, value) VALUES ('phantom', 'watched')`,
		); err != nil {
			t.Fatal(err)
		}
		stageGuard(t, reader)
		if err := reader.Commit(); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("coarse/savepoint COMMIT = %v, want ErrTransactionConflict", err)
		}
	})
}

func TestSerializableSameTableSubqueryRemainsCoarse(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs (id, value) VALUES ('left', 'target'), ('gate', 'open')`,
	); err != nil {
		t.Fatal(err)
	}
	transaction, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelSerializable,
	})
	if err != nil {
		t.Fatal(err)
	}
	var id string
	if err := transaction.QueryRow(`
		SELECT d.id
		FROM (
			SELECT id FROM docs
			WHERE EXISTS (SELECT 1 FROM docs WHERE id = 'gate')
		) AS d
		WHERE d.id = 'left'`,
	).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM docs WHERE id = 'gate'`); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(
		`INSERT INTO docs (id, value) VALUES ('guard', 'tx')`,
	); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("same-table subquery COMMIT = %v, want ErrTransactionConflict", err)
	}
}

func TestSerializablePointClassifierRejectsNestedSameTableExecution(t *testing.T) {
	raw, err := (Driver{}).Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	connection := raw.(*conn)
	defer connection.Close()
	if _, err := connection.ExecContext(
		context.Background(),
		`CREATE TABLE docs (id STRING PRIMARY KEY)`, nil,
	); err != nil {
		t.Fatal(err)
	}
	prepared, err := connection.prepareContext(context.Background(), `
		SELECT d.id
		FROM (
			SELECT id FROM docs
			WHERE EXISTS (SELECT 1 FROM docs WHERE id = 'gate')
		) AS d
		WHERE d.id = 'left'`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if !prepared.pointCandidate {
		t.Fatal("nested same-table query did not retain its point-shaped outer predicate")
	}
	if prepared.query.RequiresCatalog() {
		t.Fatal("nested same-table query unexpectedly required a multi-table catalog")
	}
	state := &txTable{primaryKey: prepared.pointPath}
	transaction := &tx{
		isolation: IsolationSerializable,
		tables: map[string]*txTable{
			prepared.query.Collection(): state,
		},
	}
	if _, exact := transaction.serializablePointQuery(prepared); exact {
		t.Fatal("nested same-table execution was misclassified as an exact point read")
	}
}

func TestSerializableExactReadBoundsPromoteCoarse(t *testing.T) {
	t.Run("keys", func(t *testing.T) {
		state := &txTable{}
		transaction := &tx{isolation: IsolationSerializable}
		for i := 0; i < txSerializableReadKeys; i++ {
			transaction.trackSerializablePointRead(
				state, fmt.Sprintf("key-%04d", i),
			)
		}
		if state.serialReadCoarse || len(state.serialReadOrder) != txSerializableReadKeys {
			t.Fatalf(
				"exact key state = coarse %v, keys %d; want false, %d",
				state.serialReadCoarse, len(state.serialReadOrder),
				txSerializableReadKeys,
			)
		}
		transaction.trackSerializablePointRead(state, "overflow")
		assertSerializableReadPromoted(t, transaction, state)
	})

	t.Run("bytes", func(t *testing.T) {
		state := &txTable{}
		transaction := &tx{isolation: IsolationSerializable}
		transaction.trackSerializablePointRead(
			state, strings.Repeat("x", txSerializableReadBytes),
		)
		if state.serialReadCoarse || state.serialReadBytes != txSerializableReadBytes {
			t.Fatalf(
				"exact byte state = coarse %v, bytes %d; want false, %d",
				state.serialReadCoarse, state.serialReadBytes,
				txSerializableReadBytes,
			)
		}
		transaction.trackSerializablePointRead(state, "overflow")
		assertSerializableReadPromoted(t, transaction, state)
	})

	t.Run("read only", func(t *testing.T) {
		state := &txTable{}
		transaction := &tx{
			isolation: IsolationSerializable,
			readOnly:  true,
			tables:    map[string]*txTable{"docs": state},
		}
		transaction.trackSerializablePointRead(state, "ignored")
		transaction.markSerializableRead("docs")
		if state.serialReadCoarse || state.serialReadSet != nil ||
			state.serialReadOrder != nil || transaction.serialReadKeys != 0 ||
			transaction.serialReadBytes != 0 || transaction.serialReadRetained != 0 {
			t.Fatalf("read-only Serializable retained conflict state: %#v", state)
		}
	})
}

func assertSerializableReadPromoted(
	t *testing.T,
	transaction *tx,
	state *txTable,
) {
	t.Helper()
	if !state.serialReadCoarse {
		t.Fatal("exact dependency overflow did not promote to coarse")
	}
	if state.serialReadSet != nil || state.serialReadOrder != nil ||
		state.serialReadArena != nil || state.serialReadChunks != nil ||
		state.serialReadBytes != 0 || state.serialReadRetained != 0 {
		t.Fatalf("coarse dependency retained exact state: %#v", state)
	}
	if transaction.serialReadKeys != 0 || transaction.serialReadBytes != 0 ||
		transaction.serialReadRetained != 0 {
		t.Fatalf(
			"coarse dependency retained transaction accounting: keys=%d bytes=%d retained=%d",
			transaction.serialReadKeys, transaction.serialReadBytes,
			transaction.serialReadRetained,
		)
	}
}

func TestIsolationLevelRefusalsAndTypedReadCommitted(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadUncommitted,
	}); !errors.Is(err, ErrUnsupportedIsolation) {
		t.Fatalf("READ UNCOMMITTED = %v, want ErrUnsupportedIsolation", err)
	}
	if _, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelLinearizable,
	}); !errors.Is(err, ErrUnsupportedIsolation) {
		t.Fatalf("LINEARIZABLE = %v, want ErrUnsupportedIsolation", err)
	}

	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	other, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	create := runtimePrepare(t, session, `CREATE TABLE docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, other, `INSERT INTO docs VALUES (?)`)
	count := runtimePrepare(t, session, `SELECT COUNT(*) FROM docs`)
	if err := session.Begin(context.Background(), TxOptions{
		Isolation: IsolationReadCommitted,
	}); err != nil {
		t.Fatal(err)
	}
	assertTypedIsolationCount(t, count, 0)
	if _, err := insert.Exec(
		context.Background(), []any{[]byte(`{"id":"outside"}`)},
	); err != nil {
		t.Fatal(err)
	}
	assertTypedIsolationCount(t, count, 1)
	if err := session.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(context.Background(), TxOptions{
		Isolation: IsolationLevel(255),
	}); !errors.Is(err, ErrUnsupportedIsolation) {
		t.Fatalf("typed invalid isolation = %v, want ErrUnsupportedIsolation", err)
	}
}

func TestReadCommittedCatalogChangeFailsClosed(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`CREATE TABLE late (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	var count int64
	err = tx.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count)
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("statement after catalog change = %v, want ErrTransactionConflict", err)
	}
}

func TestReadCommittedPlainExplainUsesStatementCatalogFence(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var plan string
	if err := tx.QueryRow(`EXPLAIN SELECT id FROM docs`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE late (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	err = tx.QueryRow(`EXPLAIN SELECT id FROM docs`).Scan(&plan)
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("plain EXPLAIN after catalog change = %v, want ErrTransactionConflict", err)
	}
}

func TestPreparedPointPlanRevalidatesPrimaryKeyAfterRecreate(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`); err != nil {
		t.Fatal(err)
	}
	prepared, err := db.Prepare(`SELECT value FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for _, statement := range []string{
		`DROP TABLE docs`,
		`CREATE TABLE docs (code STRING PRIMARY KEY, id STRING, value STRING)`,
		`INSERT INTO docs (code, id, value) VALUES ('storage-key', 'lookup', 'found')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	var value string
	if err := prepared.QueryRow("lookup").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "found" {
		t.Fatalf("recreated-table query = %q, want found", value)
	}
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Stmt(prepared).QueryRow("lookup").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "found" {
		t.Fatalf("recreated-table transaction query = %q, want found", value)
	}
}

func assertTypedIsolationCount(t testing.TB, prepared *Prepared, want int64) {
	t.Helper()
	cursor, err := prepared.Query(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("COUNT returned no row")
	}
	got, ok := cursor.Cell(0).Int64()
	if !ok || got != want {
		t.Fatalf("COUNT = (%d, %v), want %d", got, ok, want)
	}
}
