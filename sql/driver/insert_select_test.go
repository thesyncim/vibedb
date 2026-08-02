package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

func setupInsertSelectTables(t testing.TB, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE insert_source (id STRING PRIMARY KEY, keep BOOL NOT NULL)`,
		`CREATE TABLE insert_target (id STRING PRIMARY KEY, keep BOOL NOT NULL)`,
		`INSERT INTO insert_source VALUES ` +
			`('{"id":"a","keep":true}'),` +
			`('{"id":"b","keep":false}'),` +
			`('{"id":"c","keep":true}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func TestInsertSelectTransactionOwnsKeysAcrossReuseCommitAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "insert-select-keys.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE key_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE key_target (id STRING PRIMARY KEY)`,
		`INSERT INTO key_source VALUES ('{"id":"aa"}'),('{"id":"bb"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := tx.Prepare(
		`INSERT INTO key_target SELECT * FROM key_source WHERE id = ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"aa", "bb"} {
		result, err := prepared.Exec(id)
		if err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			t.Fatalf("insert %s affected = %d", id, affected)
		}
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(`SELECT id FROM key_target ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"aa", "bb"}) {
		t.Fatalf("transaction keys = %v", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err = db.Query(`SELECT id FROM key_target ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"aa", "bb"}) {
		t.Fatalf("reopened keys = %v", got)
	}
}

func insertSelectIDs(t testing.TB, rows *stdsql.Rows) []string {
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

func TestInsertSelectValuesDocumentParametersAndScalarIsolation(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE values_target (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	prepared, err := db.Prepare(
		`INSERT INTO values_target (VALUES (?)) RETURNING id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := prepared.Query([]byte(`{"id":"bytes"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"bytes"}) {
		t.Fatalf("single VALUES document ids = %v", got)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}

	rows, err = db.Query(
		`INSERT INTO values_target `+
			`((VALUES (?)) UNION ALL (VALUES (?))) RETURNING id`,
		`{"id":"text"}`, []byte(`{"id":"second"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"text", "second"}) {
		t.Fatalf("compound VALUES document ids = %v", got)
	}

	// VALUES remains scalar outside an INSERT query-source document column.
	const documentText = `{"id":"still-scalar"}`
	var scalar string
	if err := db.QueryRow(`VALUES (?)`, documentText).Scan(&scalar); err != nil {
		t.Fatal(err)
	}
	if scalar != documentText {
		t.Fatalf("standalone VALUES scalar = %q, want %q", scalar, documentText)
	}

	badStatement := `INSERT INTO values_target (VALUES (?))`
	_, err = db.Exec(badStatement, true)
	var parameterError *query.InsertSelectDocumentParameterError
	if !errors.Is(err, query.ErrParameterType) ||
		!errors.As(err, &parameterError) {
		t.Fatalf("non-document parameter = %T %v", err, err)
	}
	if want := strings.Index(badStatement, "?"); parameterError.Position() != want {
		t.Fatalf("parameter error position = %d, want %d",
			parameterError.Position(), want)
	}

	invalidJSONStatement := `INSERT INTO values_target (VALUES (?))`
	_, err = db.Exec(invalidJSONStatement, `{"id":"broken"`)
	parameterError = nil
	if !errors.Is(err, query.ErrParameterType) ||
		!errors.As(err, &parameterError) ||
		parameterError.Position() != strings.Index(invalidJSONStatement, "?") {
		t.Fatalf("invalid JSON parameter = %T %v", err, err)
	}
}

func TestInsertSelectNestedValuesDocumentLineageDatabaseSQLAndRuntime(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE lineage_seed (id STRING PRIMARY KEY)`,
		`CREATE TABLE lineage_target (id STRING PRIMARY KEY)`,
		`INSERT INTO lineage_seed VALUES ('{"id":"seed"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	derived := `INSERT INTO lineage_target ` +
		`SELECT v.* FROM (` +
		`SELECT * FROM lineage_seed WHERE id = ? ` +
		`UNION ALL VALUES (?)) AS v RETURNING id`
	rows, err := db.Query(derived, "missing", []byte(`{"id":"derived"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"derived"}) {
		t.Fatalf("derived VALUES ids = %v", got)
	}

	cte := `INSERT INTO lineage_target ` +
		`WITH supplied(doc) AS ((VALUES (?))) ` +
		`SELECT doc FROM supplied RETURNING id`
	rows, err = db.Query(cte, `{"id":"cte"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"cte"}) {
		t.Fatalf("CTE VALUES ids = %v", got)
	}

	path := filepath.Join(t.TempDir(), "insert-lineage-runtime.vdb")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE lineage_seed (id STRING PRIMARY KEY)`,
		`CREATE TABLE lineage_target (id STRING PRIMARY KEY)`,
	} {
		prepared := runtimePrepare(t, session, statement)
		if _, err := prepared.Exec(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
	}
	prepared := runtimePrepare(t, session, derived)
	defer prepared.Close()
	if prepared.ParamKind(0) != ParamScalar ||
		prepared.ParamKind(1) != ParamDocument {
		t.Fatalf("nested roles = %s/%s, want scalar/document",
			prepared.ParamKind(0), prepared.ParamKind(1))
	}
	if prepared.ParamPosition(0) != -1 ||
		prepared.ParamPosition(1) != nthQuestionPosition(derived, 1) {
		t.Fatalf("nested positions = %d/%d",
			prepared.ParamPosition(0), prepared.ParamPosition(1))
	}
	var cursor Cursor
	err = prepared.QueryInto(context.Background(), []any{
		"missing", `{"id":"broken"`,
	}, &cursor)
	var parameterError *query.InsertSelectDocumentParameterError
	if !errors.Is(err, query.ErrParameterType) ||
		!errors.As(err, &parameterError) ||
		parameterError.Parameter != 2 ||
		parameterError.Position() != nthQuestionPosition(derived, 1) {
		t.Fatalf("nested invalid document = %T %v", err, err)
	}
}

func nthQuestionPosition(statement string, ordinal int) int {
	position := -1
	for range ordinal + 1 {
		next := strings.Index(statement[position+1:], "?")
		if next < 0 {
			return -1
		}
		position += next + 1
	}
	return position
}

func TestInsertSelectRecursiveChildValuesDocumentLineage(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE recursive_lineage_seed (id STRING PRIMARY KEY)`,
		`CREATE TABLE recursive_lineage_target (id STRING PRIMARY KEY)`,
		`INSERT INTO recursive_lineage_seed VALUES ('{"id":"seed"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	statement := `INSERT INTO recursive_lineage_target ` +
		`WITH RECURSIVE supplied(doc) AS (` +
		`SELECT v.* FROM (` +
		`SELECT * FROM recursive_lineage_seed UNION ALL VALUES (?)) AS v ` +
		`UNION SELECT doc FROM supplied` +
		`) SELECT doc FROM supplied RETURNING id`
	rows, err := db.Query(statement, []byte(`{"id":"bound"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"seed", "bound"}) {
		t.Fatalf("recursive child VALUES ids = %v", got)
	}
}

func TestInsertSelectPreparedReturningAndConflictDeterminism(t *testing.T) {
	db := openTestDB(t)
	setupInsertSelectTables(t, db)

	prepared, err := db.Prepare(
		`INSERT INTO insert_target ` +
			`SELECT * FROM insert_source WHERE keep = ? ORDER BY id ` +
			`RETURNING id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	rows, err := prepared.Query(true)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := insertSelectIDs(t, rows), []string{"a", "c"}; !slices.Equal(got, want) {
		t.Fatalf("RETURNING ids = %v, want %v", got, want)
	}

	result, err := db.Exec(
		`INSERT INTO insert_target ` +
			`SELECT * FROM insert_source UNION ALL SELECT * FROM insert_source ` +
			`ON CONFLICT DO NOTHING`,
	)
	if err != nil {
		t.Fatal(err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		t.Fatalf("DO NOTHING affected = %d, %v; want 1", affected, err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM insert_target`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("target count = %d, want 3", count)
	}
}

func TestInsertSelectCTEJoinTableAndCompoundSources(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE relation_source (id STRING PRIMARY KEY, group_id STRING NOT NULL)`,
		`CREATE TABLE relation_groups (id STRING PRIMARY KEY, enabled BOOL NOT NULL)`,
		`CREATE TABLE relation_target (id STRING PRIMARY KEY, group_id STRING NOT NULL)`,
		`INSERT INTO relation_source VALUES ` +
			`('{"id":"a","group_id":"g1"}'),` +
			`('{"id":"b","group_id":"g2"}'),` +
			`('{"id":"c","group_id":"g1"}')`,
		`INSERT INTO relation_groups VALUES ` +
			`('{"id":"g1","enabled":true}'),('{"id":"g2","enabled":false}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	rows, err := db.Query(
		`INSERT INTO relation_target `+
			`WITH picked AS (`+
			`SELECT s.* FROM relation_source AS s `+
			`JOIN relation_groups AS g ON s.group_id = g.id `+
			`WHERE g.enabled = ?`+
			`) SELECT * FROM picked `+
			`RETURNING id`,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("CTE/join source ids = %v", got)
	}
	result, err := db.Exec(
		`INSERT INTO relation_target ` +
			`TABLE relation_source ` +
			`ON CONFLICT DO NOTHING`,
	)
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Fatalf("TABLE source affected = %d, want 1", affected)
	}
	result, err = db.Exec(
		`INSERT INTO relation_target ` +
			`SELECT * FROM relation_source WHERE id = 'a' ` +
			`UNION ALL SELECT * FROM relation_source WHERE id = 'b' ` +
			`ON CONFLICT DO NOTHING`,
	)
	if err != nil {
		t.Fatal(err)
	}
	affected, _ = result.RowsAffected()
	if affected != 0 {
		t.Fatalf("compound source affected = %d, want 0", affected)
	}
}

func TestInsertSelectDefaultConflictAndDynamicTypeAreAtomic(t *testing.T) {
	db := openTestDB(t)
	setupInsertSelectTables(t, db)
	if _, err := db.Exec(
		`INSERT INTO insert_target VALUES ('{"id":"a","keep":true}')`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO insert_target SELECT * FROM insert_source`,
	); !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("default conflict = %v, want ErrDuplicatePrimaryKey", err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM insert_target`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("conflicting INSERT SELECT published a prefix: count=%d", count)
	}

	for _, statement := range []string{
		`CREATE TABLE shaped_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE shaped_target (id STRING PRIMARY KEY)`,
		`INSERT INTO shaped_source VALUES ` +
			`('{"id":"object","payload":{"id":"new"}}'),` +
			`('{"id":"scalar","payload":"bad"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	_, err := db.Exec(
		`INSERT INTO shaped_target SELECT payload FROM shaped_source ORDER BY id`,
	)
	if !errors.Is(err, query.ErrInsertSelectShape) ||
		!errors.Is(err, query.ErrParameterType) {
		t.Fatalf("dynamic source type error = %T %v", err, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM shaped_target`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("dynamic type failure published %d rows", count)
	}
}

func TestInsertSelectTransactionSnapshotOverlayAndRecovery(t *testing.T) {
	db := openTestDB(t)
	setupInsertSelectTables(t, db)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO insert_target VALUES ('{"id":"a","keep":true}')`,
	); err != nil {
		t.Fatal(err)
	}
	result, err := tx.Exec(
		`INSERT INTO insert_target SELECT * FROM insert_target ` +
			`ON CONFLICT DO NOTHING`,
	)
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 0 {
		t.Fatalf("target==source affected = %d, want 0", affected)
	}
	if _, err := tx.Exec(
		`INSERT INTO insert_target SELECT * FROM insert_source`,
	); !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("transaction conflict = %v", err)
	}
	var count int
	if err := tx.QueryRow(`SELECT count(*) FROM insert_target`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("failed statement changed transaction overlay: count=%d", count)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM insert_target`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("committed count = %d, want 1", count)
	}
}

func TestInsertSelectCanceledBeforePublicationAndPreparedRecovery(t *testing.T) {
	db := openTestDB(t)
	setupInsertSelectTables(t, db)
	prepared, err := db.Prepare(
		`INSERT INTO insert_target SELECT * FROM insert_source WHERE keep = ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepared.ExecContext(ctx, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled INSERT SELECT = %v", err)
	}
	result, err := prepared.ExecContext(context.Background(), true)
	if err != nil {
		t.Fatalf("prepared recovery: %v", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 2 {
		t.Fatalf("prepared recovery affected = %d, want 2", affected)
	}
}

func TestInsertSelectSchemaFailurePublishesNothing(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE schema_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE schema_target (id STRING PRIMARY KEY, required STRING NOT NULL)`,
		`INSERT INTO schema_source VALUES ` +
			`('{"id":"good","required":"yes"}'),('{"id":"bad"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO schema_target SELECT * FROM schema_source ORDER BY id DESC`,
	); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("schema error = %v, want ErrSchemaViolation", err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM schema_target`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("schema failure published %d rows", count)
	}
}

func TestInsertSelectPreparedMissingAndStaleViewDependencies(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE dependency_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE dependency_target (id STRING PRIMARY KEY)`,
		`INSERT INTO dependency_source VALUES ('{"id":"a"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	missingSQL := `INSERT INTO dependency_target ` +
		`WITH picked AS (SELECT * FROM dependency_source) SELECT * FROM picked`
	missing, err := db.Prepare(missingSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer missing.Close()
	if _, err := db.Exec(`DROP TABLE dependency_source`); err != nil {
		t.Fatal(err)
	}
	if _, err := missing.Exec(); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("missing prepared source = %T %v", err, err)
	} else {
		var positioned interface{ Position() int }
		if !errors.As(err, &positioned) ||
			positioned.Position() != strings.Index(missingSQL, "dependency_source") {
			t.Fatalf("missing CTE dependency position = %T %v", err, err)
		}
	}

	for _, statement := range []string{
		`CREATE TABLE view_source (id STRING PRIMARY KEY)`,
		`INSERT INTO view_source VALUES ('{"id":"outer","payload":{"id":"v"}}')`,
		`CREATE VIEW insert_source_view (doc) AS SELECT payload FROM view_source`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	stale, err := db.Prepare(
		`INSERT INTO dependency_target SELECT doc FROM insert_source_view`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close()
	result, err := db.Exec(
		`INSERT INTO dependency_target ` +
			`SELECT doc FROM insert_source_view RETURNING id`,
	)
	if err == nil {
		// Exec is intentionally the wrong database/sql entry point for a
		// RETURNING statement; prove the successful expanded execution through
		// Query below while retaining this API-boundary assertion.
		t.Fatalf("RETURNING source unexpectedly executed through Exec: %+v", result)
	}
	rows, err := db.Query(
		`INSERT INTO dependency_target ` +
			`SELECT doc FROM insert_source_view RETURNING id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := insertSelectIDs(t, rows); !slices.Equal(got, []string{"v"}) {
		t.Fatalf("view source RETURNING = %v", got)
	}
	if _, err := db.Exec(`DROP VIEW insert_source_view`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`CREATE VIEW insert_source_view (doc) AS SELECT payload FROM view_source`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := stale.Exec(); !errors.Is(err, ErrViewChanged) {
		t.Fatalf("stale prepared view source = %T %v", err, err)
	}
}

func TestInsertSelectResultLimitAndPrimaryKeyErrorsAreAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "insert-select-limits.vdb")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	exec := func(text string) {
		prepared, err := session.Prepare(context.Background(), text)
		if err != nil {
			t.Fatalf("prepare %s: %v", text, err)
		}
		defer prepared.Close()
		if _, err := prepared.Exec(context.Background(), nil); err != nil {
			t.Fatalf("exec %s: %v", text, err)
		}
	}
	exec(`CREATE TABLE limit_source (id STRING PRIMARY KEY)`)
	exec(`CREATE TABLE limit_target (id STRING PRIMARY KEY)`)
	exec(`INSERT INTO limit_source VALUES ('{"id":"a"}'),('{"id":"b"}')`)
	if err := session.SetResultLimits(1, -1); err != nil {
		t.Fatal(err)
	}
	limited, err := session.Prepare(
		context.Background(),
		`INSERT INTO limit_target SELECT * FROM limit_source`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limited.Exec(context.Background(), nil); !errors.Is(err, query.ErrResultBudget) {
		t.Fatalf("limited source = %T %v", err, err)
	}
	if err := limited.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.SetResultLimits(-1, -1); err != nil {
		t.Fatal(err)
	}
	exec(`CREATE TABLE invalid_source (id STRING PRIMARY KEY)`)
	exec(`INSERT INTO invalid_source VALUES ` +
		`('{"id":"missing","doc":{"value":1}}'),` +
		`('{"id":"null","doc":{"id":null}}')`)
	invalid, err := session.Prepare(
		context.Background(),
		`INSERT INTO limit_target SELECT doc FROM invalid_source ORDER BY id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invalid.Exec(context.Background(), nil); err == nil {
		t.Fatal("invalid primary-key source succeeded")
	}
	if err := invalid.Close(); err != nil {
		t.Fatal(err)
	}
	check, err := session.Prepare(
		context.Background(), `SELECT count(*) FROM limit_target`,
	)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := check.Query(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("missing count row")
	}
	count, ok := cursor.Cell(0).Int64()
	if !ok || count != 0 {
		t.Fatalf("target count = %d/%v, want 0", count, ok)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := check.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInsertSelectSourceAndStagingShareIntermediateBudget(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	for _, text := range []string{
		`CREATE TABLE budget_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE budget_target (id STRING PRIMARY KEY)`,
		`CREATE TABLE budget_tx_target (id STRING PRIMARY KEY)`,
		`INSERT INTO budget_source VALUES ('{"id":"a"}'), ('{"id":"b"}')`,
	} {
		prepared := runtimePrepare(t, session, text)
		if _, err := prepared.Exec(ctx, nil); err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
	}

	source := runtimePrepare(t, session, `SELECT * FROM budget_source ORDER BY id`)
	cursor, err := source.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	sourceBytes := session.conn.exec.Result.RetainedBytes()
	if sourceBytes <= 0 {
		t.Fatalf("source retained bytes = %d, want positive", sourceBytes)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	insert := runtimePrepare(t, session,
		`INSERT INTO budget_target SELECT * FROM budget_source ORDER BY id`)
	defer insert.Close()
	// The source itself fits. The first owned document/key/tape staging record
	// does not, proving INSERT cannot consume a fresh allowance after SELECT.
	if err := session.SetIntermediateLimit(sourceBytes + 1); err != nil {
		t.Fatal(err)
	}
	if _, err := insert.Exec(ctx, nil); !errors.Is(err, query.ErrIntermediateBudget) {
		t.Fatalf("shared intermediate limit = %T %v", err, err)
	}

	check := runtimePrepare(t, session, `SELECT count(*) FROM budget_target`)
	defer check.Close()
	result, err := check.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Next() {
		t.Fatal("missing target count")
	}
	count, ok := result.Cell(0).Int64()
	if !ok || count != 0 {
		t.Fatalf("target after budget refusal = %d/%v, want 0", count, ok)
	}
	if err := result.Close(); err != nil {
		t.Fatal(err)
	}

	txInsert := runtimePrepare(t, session,
		`INSERT INTO budget_tx_target SELECT * FROM budget_source ORDER BY id`)
	defer txInsert.Close()
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := txInsert.Exec(ctx, nil); !errors.Is(err, query.ErrIntermediateBudget) {
		t.Fatalf("transaction shared intermediate limit = %T %v", err, err)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	txCheck := runtimePrepare(t, session, `SELECT count(*) FROM budget_tx_target`)
	defer txCheck.Close()
	txResult, err := txCheck.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !txResult.Next() {
		t.Fatal("missing transaction target count")
	}
	txCount, txOK := txResult.Cell(0).Int64()
	if !txOK || txCount != 0 {
		t.Fatalf("transaction target after budget refusal = %d/%v, want 0", txCount, txOK)
	}
	if err := txResult.Close(); err != nil {
		t.Fatal(err)
	}

	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatal(err)
	}
	affected, err := insert.Exec(ctx, nil)
	if err != nil || affected.RowsAffected != 2 {
		t.Fatalf("reused insert = %+v, %v; want 2", affected, err)
	}
}

func preparedInsertSelectDoNothingWarm(tb testing.TB) *Prepared {
	tb.Helper()
	database, err := Open(filepath.Join(tb.TempDir(), "insert-select-alloc.vdb"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := database.Close(); err != nil {
			tb.Error(err)
		}
	})
	session, err := database.NewSession(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := session.Close(); err != nil {
			tb.Error(err)
		}
	})
	for _, statement := range []string{
		`CREATE TABLE bench_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE bench_target (id STRING PRIMARY KEY)`,
		`INSERT INTO bench_source VALUES ('{"id":"a"}'),('{"id":"b"}')`,
		`INSERT INTO bench_target VALUES ('{"id":"a"}'),('{"id":"b"}')`,
	} {
		prepared, err := session.Prepare(context.Background(), statement)
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := prepared.Exec(context.Background(), nil); err != nil {
			tb.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	prepared, err := session.Prepare(
		context.Background(),
		`INSERT INTO bench_target SELECT * FROM bench_source `+
			`ON CONFLICT DO NOTHING`,
	)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			tb.Error(err)
		}
	})
	for range 3 {
		if _, err := prepared.Exec(context.Background(), nil); err != nil {
			tb.Fatal(err)
		}
	}
	return prepared
}

func TestPreparedInsertSelectDoNothingWarmAllocations(t *testing.T) {
	prepared := preparedInsertSelectDoNothingWarm(t)
	ctx := context.Background()
	var result Result
	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		result, runErr = prepared.Exec(ctx, nil)
	})
	if runErr != nil || result.RowsAffected != 0 {
		t.Fatalf("result=%+v err=%v", result, runErr)
	}
	if allocs != 0 {
		t.Fatalf(
			"warmed direct-session INSERT SELECT DO NOTHING allocated %.2f times, want zero",
			allocs,
		)
	}
}

func preparedInsertSelectValuesDoNothingWarm(
	tb testing.TB,
) (*Prepared, []any) {
	tb.Helper()
	database, err := Open(filepath.Join(tb.TempDir(), "insert-select-values-alloc.vdb"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := database.Close(); err != nil {
			tb.Error(err)
		}
	})
	session, err := database.NewSession(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := session.Close(); err != nil {
			tb.Error(err)
		}
	})
	for _, statement := range []string{
		`CREATE TABLE bench_values_target (id STRING PRIMARY KEY)`,
		`INSERT INTO bench_values_target VALUES ('{"id":"stable"}')`,
	} {
		prepared, err := session.Prepare(context.Background(), statement)
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := prepared.Exec(context.Background(), nil); err != nil {
			tb.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	const statement = `INSERT INTO bench_values_target (VALUES (?)) ` +
		`ON CONFLICT DO NOTHING`
	prepared, err := session.Prepare(context.Background(), statement)
	if err != nil {
		tb.Fatal(err)
	}
	if got := prepared.ParamKind(0); got != ParamDocument {
		tb.Fatalf("VALUES source ParamKind = %s, want document", got)
	}
	invalid := []any{`{"id":"broken"`}
	_, err = prepared.Exec(context.Background(), invalid)
	var parameterError *query.InsertSelectDocumentParameterError
	if !errors.Is(err, query.ErrParameterType) ||
		!errors.As(err, &parameterError) ||
		parameterError.Position() != strings.Index(statement, "?") {
		tb.Fatalf("typed-runtime invalid JSON = %T %v", err, err)
	}

	document := `{"id":"stable"}`
	args := []any{&document}
	for range 3 {
		result, err := prepared.Exec(context.Background(), args)
		if err != nil || result.RowsAffected != 0 {
			tb.Fatalf("warm result=%+v err=%v", result, err)
		}
	}
	tb.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			tb.Error(err)
		}
	})
	return prepared, args
}

func TestPreparedInsertSelectValuesDoNothingWarmAllocations(t *testing.T) {
	prepared, args := preparedInsertSelectValuesDoNothingWarm(t)
	ctx := context.Background()
	var result Result
	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		result, runErr = prepared.Exec(ctx, args)
	})
	if runErr != nil || result.RowsAffected != 0 {
		t.Fatalf("result=%+v err=%v", result, runErr)
	}
	if allocs != 0 {
		t.Fatalf(
			"warmed direct-session INSERT VALUES source allocated %.2f times, want zero",
			allocs,
		)
	}
}

func TestPreparedInsertSelectNestedValuesDoNothingWarmAllocations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "insert-select-nested-values-alloc.vdb")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE nested_alloc_target (id STRING PRIMARY KEY)`,
		`INSERT INTO nested_alloc_target VALUES ('{"id":"stable"}')`,
	} {
		prepared := runtimePrepare(t, session, statement)
		if _, err := prepared.Exec(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
	}
	statement := `INSERT INTO nested_alloc_target ` +
		`WITH supplied(doc) AS ((VALUES (?))) ` +
		`SELECT doc FROM supplied ON CONFLICT DO NOTHING`
	prepared := runtimePrepare(t, session, statement)
	defer prepared.Close()
	if prepared.ParamKind(0) != ParamDocument ||
		prepared.ParamPosition(0) != strings.Index(statement, "?") {
		t.Fatalf("nested allocation metadata = %s/%d",
			prepared.ParamKind(0), prepared.ParamPosition(0))
	}
	document := `{"id":"stable"}`
	args := []any{&document}
	for range 3 {
		result, err := prepared.Exec(context.Background(), args)
		if err != nil || result.RowsAffected != 0 {
			t.Fatalf("warm result=%+v err=%v", result, err)
		}
	}
	var result Result
	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		result, runErr = prepared.Exec(context.Background(), args)
	})
	if runErr != nil || result.RowsAffected != 0 {
		t.Fatalf("result=%+v err=%v", result, runErr)
	}
	if allocs != 0 {
		t.Fatalf("warmed nested VALUES INSERT allocated %.2f times, want zero", allocs)
	}
}

func TestInsertSelectDocumentLineageAbsentValuesExecutionAllocatesZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "values-scalar-absent-alloc.vdb")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	prepared := runtimePrepare(t, session, `VALUES (?)`)
	defer prepared.Close()
	if prepared.ParamKind(0) != ParamScalar || prepared.ParamPosition(0) != -1 {
		t.Fatalf("standalone VALUES metadata = %s/%d, want scalar/-1",
			prepared.ParamKind(0), prepared.ParamPosition(0))
	}
	args := []any{"still scalar"}
	var cursor Cursor
	for range 3 {
		if err := prepared.QueryInto(context.Background(), args, &cursor); err != nil {
			t.Fatal(err)
		}
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		runErr = prepared.QueryInto(context.Background(), args, &cursor)
		if runErr == nil {
			runErr = cursor.Close()
		}
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf("standalone VALUES absent path allocated %.2f times, want zero", allocs)
	}
}

func BenchmarkPreparedInsertSelectValuesDoNothingWarm(b *testing.B) {
	prepared, args := preparedInsertSelectValuesDoNothingWarm(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := prepared.Exec(ctx, args)
		if err != nil || result.RowsAffected != 0 {
			b.Fatalf("result=%+v err=%v", result, err)
		}
	}
}

func BenchmarkPreparedInsertSelectDoNothingWarm(b *testing.B) {
	prepared := preparedInsertSelectDoNothingWarm(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := prepared.Exec(ctx, nil)
		if err != nil || result.RowsAffected != 0 {
			b.Fatalf("result=%+v err=%v", result, err)
		}
	}
}

func BenchmarkPreparedInsertValuesAbsentSourceWarm(b *testing.B) {
	database, err := Open(filepath.Join(b.TempDir(), "insert-values-bench.vdb"))
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()
	session, err := database.NewSession(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE bench_target (id STRING PRIMARY KEY)`,
		`INSERT INTO bench_target VALUES ('{"id":"a"}')`,
	} {
		prepared, err := session.Prepare(context.Background(), statement)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := prepared.Exec(context.Background(), nil); err != nil {
			b.Fatal(err)
		}
		prepared.Close()
	}
	prepared, err := session.Prepare(
		context.Background(),
		`INSERT INTO bench_target VALUES (?) ON CONFLICT DO NOTHING`,
	)
	if err != nil {
		b.Fatal(err)
	}
	defer prepared.Close()
	args := []any{`{"id":"a"}`}
	for range 3 {
		if _, err := prepared.Exec(context.Background(), args); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := prepared.Exec(context.Background(), args)
		if err != nil || result.RowsAffected != 0 {
			b.Fatalf("result=%+v err=%v", result, err)
		}
	}
}
