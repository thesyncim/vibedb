package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestDatabaseSQLCTEDirectAndPrepared(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE docs (` +
			`id STRING PRIMARY KEY, kind STRING NOT NULL, n INTEGER NOT NULL)`,
		`INSERT INTO docs VALUES ` +
			`('{"id":"a","kind":"x","n":1}'),` +
			`('{"id":"b","kind":"x","n":2}'),` +
			`('{"id":"c","kind":"y","n":3}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	rows, err := db.Query(`
		WITH active AS (
			SELECT id, n FROM docs WHERE kind = 'x'
		)
		SELECT id FROM active WHERE n >= 2 ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanCTEIDs(t, rows), []string{"b"}; !slices.Equal(got, want) {
		t.Fatalf("direct CTE rows = %v, want %v", got, want)
	}

	prepared, err := db.Prepare(`
		WITH active(identifier, score) AS MATERIALIZED (
			SELECT id, n FROM docs WHERE kind = ?
		)
		SELECT identifier FROM active
		WHERE score >= ? ORDER BY identifier`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for _, test := range []struct {
		kind string
		min  int
		want []string
	}{
		{kind: "x", min: 1, want: []string{"a", "b"}},
		{kind: "y", min: 3, want: []string{"c"}},
	} {
		rows, err := prepared.Query(test.kind, test.min)
		if err != nil {
			t.Fatal(err)
		}
		if got := scanCTEIDs(t, rows); !slices.Equal(got, test.want) {
			t.Fatalf("prepared CTE rows = %v, want %v", got, test.want)
		}
	}
}

func TestPreparedCTEAndExplainRevalidatePhysicalDependencies(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	queryStmt, err := db.Prepare(`
		WITH selected AS (SELECT id FROM docs)
		SELECT id FROM selected`)
	if err != nil {
		t.Fatal(err)
	}
	defer queryStmt.Close()
	explainStmt, err := db.Prepare(`
		EXPLAIN WITH selected AS MATERIALIZED (SELECT id FROM docs)
		SELECT id FROM selected`)
	if err != nil {
		t.Fatal(err)
	}
	defer explainStmt.Close()
	var plan string
	if err := explainStmt.QueryRow().Scan(&plan); err != nil || plan == "" {
		t.Fatalf("CTE EXPLAIN before DROP = (%q, %v)", plan, err)
	}
	if _, err := db.Exec(`DROP TABLE docs`); err != nil {
		t.Fatal(err)
	}
	for name, statement := range map[string]*stdsql.Stmt{
		"query": queryStmt, "explain": explainStmt,
	} {
		rows, err := statement.Query()
		if rows != nil {
			_ = rows.Close()
		}
		if !errors.Is(err, ErrTableNotFound) {
			t.Fatalf("prepared CTE %s after DROP = %v, want ErrTableNotFound", name, err)
		}
	}
	rows, err := db.Query(`
		EXPLAIN WITH selected AS (SELECT id FROM docs)
		SELECT id FROM selected`)
	if rows != nil {
		_ = rows.Close()
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("plain CTE EXPLAIN after DROP = %v, want ErrTableNotFound", err)
	}
}

func TestTransactionCTESnapshotAndReadYourWrites(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY)`,
		`CREATE TABLE permitted (id STRING PRIMARY KEY)`,
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
	if _, err := db.Exec(`INSERT INTO docs VALUES ('{"id":"outside"}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO permitted VALUES ('{"id":"outside"}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES ('{"id":"pending"}')`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(`
		WITH selected AS MATERIALIZED (
			SELECT id FROM docs WHERE id IN (SELECT id FROM permitted)
		)
		SELECT id FROM selected ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanCTEIDs(t, rows), []string{"base", "pending"}; !slices.Equal(got, want) {
		t.Fatalf("transaction CTE rows = %v, want %v", got, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestSinglePhysicalCTEPreservesDurableExactIndex(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	index := runtimePrepare(t, session,
		`CREATE INDEX docs_kind_exact ON docs (kind)`)
	if _, err := index.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	if _, err := insert.Exec(ctx, []any{`{"id":"a","kind":"x"}`}); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePrepare(t, session, `
		WITH selected AS (
			SELECT id FROM docs WHERE kind = ?
		)
		SELECT * FROM selected`)
	cursor, err := prepared.Query(ctx, []any{"x"})
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("single-source CTE returned no row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if got := prepared.statement.query.Collection(); got != "docs" {
		t.Fatalf("CTE physical driving collection = %q, want docs", got)
	}
	if prepared.statement.requiresCatalogSource() {
		t.Fatal("single physical CTE selected heap catalog materialization")
	}
	stats := session.conn.exec.Stats
	if !stats.IndexBounded || stats.IndexLookups == 0 {
		t.Fatalf("single physical CTE lost durable exact index: %+v", stats)
	}

	materialized := runtimePrepare(t, session, `
		WITH selected AS MATERIALIZED (
			SELECT id FROM docs WHERE id = ?
		)
		SELECT id FROM selected`)
	if got := materialized.statement.query.Collection(); got != "docs" {
		t.Fatalf("materialized CTE physical collection = %q, want docs", got)
	}
	if materialized.statement.requiresCatalogSource() {
		t.Fatal("materialized single-source CTE selected heap catalog materialization")
	}
}

func TestCanceledCTEExecutionRecoversWithoutPartialResult(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	if _, err := insert.Exec(ctx, []any{`{"id":"a"}`}); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePrepare(t, session, `
		WITH selected AS MATERIALIZED (SELECT id FROM docs)
		SELECT id FROM selected`)
	var cancel query.CancelFlag
	if err := session.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	cancel.Cancel()
	cursor, err := prepared.Query(ctx, nil)
	if !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("pre-canceled CTE = %v, want ErrCanceled", err)
	}
	if cursor != nil {
		_ = cursor.Close()
		t.Fatal("pre-canceled CTE published a partial cursor")
	}
	cancel.Reset()
	cursor, err = prepared.Query(ctx, nil)
	if err != nil {
		t.Fatalf("CTE after cancellation: %v", err)
	}
	if !cursor.Next() {
		t.Fatal("CTE after cancellation returned no row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
}

func scanCTEIDs(t *testing.T, rows *stdsql.Rows) []string {
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

func TestCTEPhysicalDependenciesAreRecursiveStableAndUnique(t *testing.T) {
	statement := `WITH
		base AS (
			SELECT id FROM docs
			WHERE id IN (SELECT id FROM permitted)
		),
		reused AS (
			SELECT id FROM base
			WHERE EXISTS (SELECT id FROM docs)
		)
	SELECT id FROM reused
	WHERE EXISTS (SELECT id FROM audit)`
	tree, err := sqlast.Parse(statement)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := selectPhysicalDependencies(tree)
	wantNames := []string{"docs", "permitted", "audit"}
	gotNames := make([]string, len(dependencies))
	for i := range dependencies {
		gotNames[i] = dependencies[i].name
	}
	if !slices.Equal(gotNames, wantNames) {
		t.Fatalf("CTE dependencies = %v, want %v", gotNames, wantNames)
	}
	for i := range dependencies {
		wantPos := strings.Index(statement, dependencies[i].name)
		if dependencies[i].pos != wantPos {
			t.Fatalf(
				"dependency %q position = %d, want first physical reference %d",
				dependencies[i].name, dependencies[i].pos, wantPos,
			)
		}
	}
}

func TestNestedSelfJoinRequiresCatalogDespiteOnePhysicalDependency(t *testing.T) {
	tree, err := sqlast.Parse(`
		SELECT d.id
		FROM (
			SELECT left_docs.id
			FROM docs AS left_docs
			JOIN docs AS right_docs ON left_docs.id = right_docs.id
		) AS d`)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := selectPhysicalDependencies(tree)
	if len(dependencies) != 1 || dependencies[0].name != "docs" {
		t.Fatalf("self-join dependencies = %+v, want one docs dependency", dependencies)
	}
	if !selectContainsJoin(tree) {
		t.Fatal("nested self-join was classified as direct single-source execution")
	}
}

func TestSinglePhysicalDerivedDependencyKeepsDirectSource(t *testing.T) {
	const text = `SELECT d.id FROM (` +
		`SELECT id FROM docs WHERE id = ?` +
		`) AS d`
	tree, err := sqlast.ParseStatement(text)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := query.PrepareParsedStatement(text, tree.Select)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Release()
	statement := stmt{
		tree:         tree,
		query:        prepared,
		dependencies: selectPhysicalDependencies(tree.Select),
		catalogJoin:  selectContainsJoin(tree.Select),
	}
	if prepared.RequiresCatalog() {
		t.Fatal("single-source derived statement unnecessarily requires a catalog")
	}
	if got := prepared.Collection(); got != "docs" {
		t.Fatalf("derived physical driving collection = %q, want docs", got)
	}
	if got := len(statement.dependencies); got != 1 {
		t.Fatalf("derived physical dependency count = %d, want 1", got)
	}
	if statement.requiresCatalogSource() {
		t.Fatal("single physical derived dependency selected heap catalog materialization")
	}
}

func TestAbsentCTECatalogClassificationIsAllocationFree(t *testing.T) {
	statement := prepareAbsentCTEClassificationStatement(t)
	defer statement.query.Release()
	if statement.tree.Select.With != nil || statement.dependencies != nil {
		t.Fatal("ordinary statement retained CTE/dependency state")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if statement.requiresCatalogSource() {
			panic("ordinary statement requires a catalog")
		}
	}); allocs != 0 {
		t.Fatalf("absent-CTE classification allocated %.2f times, want zero", allocs)
	}
}

func TestZeroPhysicalDependencyClassificationIsGuarded(t *testing.T) {
	// A zero-source relation must not turn a classifier refactor into an unchecked
	// dependencies[0] access at the driver boundary.
	const text = `SELECT id FROM docs`
	tree, err := sqlast.ParseStatement(text)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := query.PrepareParsedStatement(text, tree.Select)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Release()
	statement := stmt{
		tree:  tree,
		query: prepared,
	}
	if statement.requiresCatalogSource() {
		t.Fatal("zero-dependency statement unexpectedly requires a durable catalog")
	}
	if statement.transactionRequiresCatalogSource() {
		t.Fatal("zero-dependency transaction statement unexpectedly requires a catalog")
	}
	if _, err := new(conn).materializeJoinRows(nil, "", func(joinRowVisitor) error {
		return nil
	}); err == nil {
		t.Fatal("zero-dependency catalog fallback unexpectedly succeeded")
	}
}

func TestUnusedPhysicalCTEsHaveNoExecutableDependencies(t *testing.T) {
	const text = `WITH
		unused_a AS (SELECT id FROM never_created_a),
		unused_b AS (SELECT id FROM never_created_b)
		SELECT 1 AS value`
	tree, err := sqlast.ParseStatement(text)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := query.PrepareParsedStatement(text, tree.Select)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Release()
	if got := prepared.Collection(); got != "" {
		t.Fatalf("unused CTE driving collection = %q, want empty", got)
	}
	if prepared.RequiresCatalog() {
		t.Fatal("unused physical CTE definitions require a catalog")
	}
	if dependencies := selectPhysicalDependencies(tree.Select); len(dependencies) != 2 {
		t.Fatalf("unused lexical CTE dependencies = %+v, want two", dependencies)
	}
	if dependencies := selectExecutablePhysicalDependencies(tree.Select); len(dependencies) != 0 {
		t.Fatalf("unused executable CTE dependencies = %+v, want none", dependencies)
	}
	if !sourceIndependentStatement(prepared) {
		t.Fatal("unused physical CTE root was not classified source-independent")
	}
}

func TestCatalogSnapshotCaptureWarmAllocatesZero(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, text := range []string{
		`CREATE TABLE left_docs (id STRING PRIMARY KEY)`,
		`CREATE TABLE right_docs (id STRING PRIMARY KEY)`,
	} {
		prepared := runtimePrepare(t, session, text)
		if _, err := prepared.Exec(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	statement := stmt{
		conn: session.conn,
		dependencies: []physicalDependency{
			{name: "left_docs"},
			{name: "right_docs"},
		},
	}
	capture := func() {
		session.conn.db.mu.RLock()
		catalog, err := statement.snapshotCatalogDependenciesLocked(ctx)
		session.conn.db.mu.RUnlock()
		if err != nil {
			panic(err)
		}
		if catalog.Len() != 2 {
			panic("wrong coherent catalog size")
		}
		if err := catalog.Close(); err != nil {
			panic(err)
		}
	}
	capture()
	if allocs := testing.AllocsPerRun(1000, capture); allocs != 0 {
		t.Fatalf("warmed coherent catalog capture allocated %.2f times, want zero", allocs)
	}
}

func BenchmarkAbsentCTECatalogClassification(b *testing.B) {
	statement := prepareAbsentCTEClassificationStatement(b)
	defer statement.query.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if statement.requiresCatalogSource() {
			b.Fatal("ordinary statement requires a catalog")
		}
	}
}

type testOrBenchmark interface {
	Helper()
	Fatal(args ...any)
}

func prepareAbsentCTEClassificationStatement(tb testOrBenchmark) *stmt {
	tb.Helper()
	const text = `SELECT id FROM docs WHERE id = ?`
	tree, err := sqlast.ParseStatement(text)
	if err != nil {
		tb.Fatal(err)
	}
	prepared, err := query.PrepareParsedStatement(text, tree.Select)
	if err != nil {
		tb.Fatal(err)
	}
	return &stmt{tree: tree, query: prepared}
}
