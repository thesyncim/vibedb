package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestTypedRuntimeDDLIndexWriteSelectAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}

	create := runtimePrepare(t, session, `
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			name STRING NOT NULL,
			kind STRING,
			n INTEGER,
			score NUMBER,
			active BOOLEAN
		)`)
	if create.Kind().String() != "CREATE TABLE" || create.NumParams() != 0 {
		t.Fatalf("CREATE metadata = (%s, %d params)", create.Kind(), create.NumParams())
	}
	if result, err := create.Exec(ctx, nil); err != nil || result.RowsAffected != 0 {
		t.Fatalf("CREATE TABLE = (%+v, %v)", result, err)
	}

	index := runtimePrepare(t, session, `CREATE INDEX by_kind ON docs(kind)`)
	if _, err := index.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Exec(ctx, nil); !errors.Is(err, ErrIndexExists) {
		t.Fatalf("duplicate CREATE INDEX = %v", err)
	}

	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	if got := insert.ParamKind(0); got != ParamDocument {
		t.Fatalf("whole-document INSERT ParamKind = %s, want document", got)
	}
	if got := insert.ParamKind(1); got != ParamInvalid {
		t.Fatalf("out-of-range ParamKind = %s, want invalid", got)
	}
	doc := []byte(`{"id":"a","name":"Ada","kind":"person","n":7}`)
	args := []any{doc}
	if result, err := insert.Exec(ctx, args); err != nil || result.RowsAffected != 1 {
		t.Fatalf("INSERT = (%+v, %v)", result, err)
	}
	if len(args) != 1 || !slices.Equal(args[0].([]byte), doc) {
		t.Fatalf("runtime mutated caller arguments: %#v", args)
	}
	documentText := `{"id":"text-pointer","name":"Text","kind":"other"}`
	textArgs := []any{&documentText}
	if result, err := insert.Exec(ctx, textArgs); err != nil || result.RowsAffected != 1 {
		t.Fatalf("*string document INSERT = (%+v, %v)", result, err)
	}
	if textArgs[0] != &documentText {
		t.Fatal("runtime replaced caller's *string document argument")
	}
	documentBytes := []byte(
		`{"id":"bytes-pointer","name":"Bytes","kind":"other"}`,
	)
	byteArgs := []any{&documentBytes}
	if result, err := insert.Exec(ctx, byteArgs); err != nil || result.RowsAffected != 1 {
		t.Fatalf("*[]byte document INSERT = (%+v, %v)", result, err)
	}
	if byteArgs[0] != &documentBytes {
		t.Fatal("runtime replaced caller's *[]byte document argument")
	}

	flat := runtimePrepare(t, session,
		`INSERT INTO docs (id, name, kind, n, score, active)
		 VALUES (?, ?, ?, ?, ?, ?)`)
	for i := 0; i < flat.NumParams(); i++ {
		if got := flat.ParamKind(i); got != ParamScalar {
			t.Fatalf("flat INSERT parameter %d = %s, want scalar", i, got)
		}
	}
	id, name, kind := "b", "Grace", "person"
	n, active := int64(9), true
	score := query.Number("9007199254740993")
	if result, err := flat.Exec(ctx, []any{
		&id, &name, &kind, &n, &score, &active,
	}); err != nil || result.RowsAffected != 1 {
		t.Fatalf("flat INSERT = (%+v, %v)", result, err)
	}

	update := runtimePrepare(t, session,
		`UPDATE docs SET "$doc" = ? WHERE id = ?`)
	if update.ParamKind(0) != ParamDocument ||
		update.ParamKind(1) != ParamScalar {
		t.Fatalf("UPDATE parameter roles = (%s, %s), want (document, scalar)",
			update.ParamKind(0), update.ParamKind(1))
	}
	updatedText := `{"id":"text-pointer","name":"Text updated","kind":"other"}`
	if result, err := update.Exec(ctx, []any{
		&updatedText, "text-pointer",
	}); err != nil || result.RowsAffected != 1 {
		t.Fatalf("*string document UPDATE = (%+v, %v)", result, err)
	}
	updatedBytes := []byte(
		`{"id":"bytes-pointer","name":"Bytes updated","kind":"other"}`,
	)
	if result, err := update.Exec(ctx, []any{
		&updatedBytes, "bytes-pointer",
	}); err != nil || result.RowsAffected != 1 {
		t.Fatalf("*[]byte document UPDATE = (%+v, %v)", result, err)
	}

	selectByKind := runtimePrepare(t, session,
		`SELECT id, name, n FROM docs WHERE kind = ? ORDER BY id`)
	if got, want := selectByKind.Columns(), []string{"id", "name", "n"}; !slices.Equal(got, want) {
		t.Fatalf("Columns = %v, want %v", got, want)
	}
	schema := selectByKind.AppendSchema(nil)
	if len(schema) != 3 {
		t.Fatalf("schema columns = %d, want 3", len(schema))
	}
	for i := range schema {
		if schema[i].Ordinal != uint32(i) ||
			schema[i].Reduction != query.ReductionNone {
			t.Fatalf("schema[%d] = %+v", i, schema[i])
		}
	}

	queryArgs := []any{"person"}
	cursor, err := selectByKind.Query(ctx, queryArgs)
	if err != nil {
		t.Fatal(err)
	}
	if queryArgs[0] != "person" {
		t.Fatalf("Query mutated caller args: %#v", queryArgs)
	}
	copyCursor := cursor.Snapshot()
	if !copyCursor.Next() {
		t.Fatal("snapshot cursor is empty")
	}
	if cursor.Row() != -1 {
		t.Fatalf("advancing snapshot moved owning cursor to row %d", cursor.Row())
	}
	if !cursor.Next() {
		t.Fatal("owning cursor is empty")
	}
	if id, ok := cursor.Cell(0).Text(); !ok || id != "a" {
		t.Fatalf("first id = (%q, %v), want a", id, ok)
	}
	if !cursor.Next() {
		t.Fatal("missing second row")
	}
	if n, ok := cursor.Cell(2).Int64(); !ok || n != 9 {
		t.Fatalf("second n = (%d, %v), want 9", n, ok)
	}
	if cursor.Next() {
		t.Fatal("unexpected third row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatalf("idempotent Cursor.Close: %v", err)
	}
	if cursor.Next() || cursor.Row() != -1 || cursor.Cell(0).Kind() != query.TypeAny {
		t.Fatal("closed cursor remained usable")
	}

	for _, prepared := range []*Prepared{
		create, index, insert, flat, update, selectByKind,
	} {
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedSession, err := reopened.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSession.Close()
	persisted := runtimePrepare(t, reopenedSession,
		`SELECT name FROM docs WHERE id = ?`)
	defer persisted.Close()
	cursor, err = persisted.Query(ctx, []any{"a"})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("persisted row is absent after reopen")
	}
	if name, ok := cursor.Cell(0).Text(); !ok || name != "Ada" {
		t.Fatalf("persisted name = (%q, %v), want Ada", name, ok)
	}
}

func TestTypedRuntimeTransactionStateAndRollback(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	count := runtimePrepare(t, session,
		`SELECT COUNT(*) FROM docs WHERE id = ?`)

	if err := session.Commit(ctx); !errors.Is(err, ErrNoTransaction) {
		t.Fatalf("COMMIT without transaction = %v", err)
	}
	if err := session.Rollback(ctx); !errors.Is(err, ErrNoTransaction) {
		t.Fatalf("ROLLBACK without transaction = %v", err)
	}
	if _, err := session.conn.BeginTx(ctx, sqldriver.TxOptions{
		Isolation: sqldriver.IsolationLevel(1),
	}); !errors.Is(err, ErrUnsupportedIsolation) {
		t.Fatalf("unsupported isolation = %v", err)
	}

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if session.State() != SessionInTransaction {
		t.Fatalf("state after BEGIN = %s", session.State())
	}
	if _, err := insert.Exec(
		ctx, []any{[]byte(`{"id":"committed","name":"Ada"}`)},
	); err != nil {
		t.Fatal(err)
	}
	assertRuntimeCount(t, count, "committed", 1)
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if session.State() != SessionIdle {
		t.Fatalf("state after COMMIT = %s", session.State())
	}
	assertRuntimeCount(t, count, "committed", 1)

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := insert.Exec(
		ctx, []any{[]byte(`{"id":"rolled","name":"Grace"}`)},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Prepare(ctx, `SELECT FROM`); err == nil {
		t.Fatal("invalid SQL prepared inside a transaction")
	}
	if session.State() != SessionFailedTransaction {
		t.Fatalf("state after prepare error = %s", session.State())
	}
	if _, err := count.Query(ctx, []any{"rolled"}); !errors.Is(err, ErrTransactionFailed) {
		t.Fatalf("query in failed transaction = %v", err)
	}
	if err := session.Commit(ctx); !errors.Is(err, ErrTransactionFailed) {
		t.Fatalf("COMMIT failed transaction = %v", err)
	}
	if session.State() != SessionIdle {
		t.Fatalf("state after failed COMMIT rollback = %s", session.State())
	}
	assertRuntimeCount(t, count, "rolled", 0)

	if err := session.Begin(ctx, TxOptions{ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := insert.Exec(
		ctx, []any{[]byte(`{"id":"readonly","name":"No"}`)},
	); !errors.Is(err, ErrReadOnlyTransaction) {
		t.Fatalf("read-only INSERT = %v", err)
	}
	if session.State() != SessionFailedTransaction {
		t.Fatalf("state after read-only error = %s", session.State())
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := session.Rollback(canceled); err != nil {
		t.Fatalf("Rollback with canceled context = %v", err)
	}

	createExtra := runtimePrepare(t, session,
		`CREATE TABLE extra (id STRING PRIMARY KEY)`)
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := createExtra.Exec(ctx, nil); !errors.Is(err, ErrDDLInTransaction) {
		t.Fatalf("DDL in transaction = %v", err)
	}
	if session.State() != SessionFailedTransaction {
		t.Fatalf("state after DDL error = %s", session.State())
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTypedRuntimeSnapshotCatalogErrorsAreStable(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	other, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()

	base := runtimePrepare(t, other,
		`CREATE TABLE base (id STRING PRIMARY KEY)`)
	if _, err := base.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	late := runtimePrepare(t, other,
		`CREATE TABLE late (id STRING PRIMARY KEY)`)
	if _, err := late.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}

	lateRead := runtimePrepare(t, session, `SELECT id FROM late`)
	if _, err := lateRead.Query(ctx, nil); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("table created after BEGIN read = %v", err)
	}
	if session.State() != SessionFailedTransaction {
		t.Fatalf("late-table read state = %s", session.State())
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	later := runtimePrepare(t, other,
		`CREATE TABLE later (id STRING PRIMARY KEY)`)
	if _, err := later.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	lateWrite := runtimePrepare(t, session,
		`INSERT INTO later (id) VALUES (?)`)
	if _, err := lateWrite.Exec(ctx, []any{"x"}); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("table created after BEGIN write = %v", err)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	latest := runtimePrepare(t, other,
		`CREATE TABLE latest (id STRING PRIMARY KEY)`)
	if _, err := latest.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	lateJoin := runtimePrepare(t, session, `
		SELECT b.id
		FROM base AS b
		JOIN latest AS l ON b.id = l.id`)
	if _, err := lateJoin.Query(ctx, nil); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("table created after BEGIN join = %v", err)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTypedRuntimeOwnershipErrorsAndArgumentBounds(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := create.Exec(ctx, nil); !errors.Is(err, ErrTableExists) {
		t.Fatalf("duplicate CREATE TABLE = %v", err)
	}
	if _, err := session.Prepare(ctx, `SELECT * FROM missing`); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("missing relation = %v", err)
	}

	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	document := []byte(`{"id":"kept","value":"v"}`)
	callerArgs := []any{document}
	if _, err := insert.Exec(ctx, callerArgs); err != nil {
		t.Fatal(err)
	}
	if len(callerArgs) != 1 || !slices.Equal(callerArgs[0].([]byte), document) {
		t.Fatalf("Exec cleared caller args: %#v", callerArgs)
	}
	lateIndex := runtimePrepare(t, session,
		`CREATE INDEX late_value ON docs(value)`)
	if _, err := lateIndex.Exec(ctx, nil); err != nil {
		t.Fatalf("CREATE INDEX after materialization = %v", err)
	}
	if _, err := insert.Exec(ctx, []any{
		[]byte(`{"id":"later","value":"v"}`),
	}); err != nil {
		t.Fatalf("INSERT after online CREATE INDEX = %v", err)
	}
	lateLookup := runtimePrepare(t, session,
		`SELECT id FROM docs WHERE value = ?`)
	lateCursor, err := lateLookup.Query(ctx, []any{"v"})
	if err != nil {
		t.Fatal(err)
	}
	lateIDs := make(map[string]bool, 2)
	for lateCursor.Next() {
		id, ok := lateCursor.Cell(0).Text()
		if !ok {
			t.Fatalf("online indexed id = (%q, %v)", id, ok)
		}
		lateIDs[id] = true
	}
	if !lateIDs["kept"] || !lateIDs["later"] || len(lateIDs) != 2 {
		t.Fatalf("online indexed ids = %v, want kept and later", lateIDs)
	}
	if err := lateCursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lateLookup.Close(); err != nil {
		t.Fatal(err)
	}

	lookup := runtimePrepare(t, session,
		`SELECT value FROM docs WHERE id = ?`)
	oversized := []any{strings.Repeat("x", maxSQLParameterBytes+1)}
	if _, err := lookup.Query(ctx, oversized); err == nil {
		t.Fatal("oversized runtime parameter succeeded")
	}
	if len(oversized) != 1 ||
		len(oversized[0].(string)) != maxSQLParameterBytes+1 {
		t.Fatal("failed Query mutated caller args")
	}

	cursor, err := lookup.Query(ctx, []any{"kept"})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.NewSession(ctx); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("NewSession after Database.Close = %v", err)
	}
	if !cursor.Next() {
		t.Fatal("Database.Close invalidated an owned session cursor")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if cursor.Next() || cursor.Cell(0).Kind() != query.TypeAny {
		t.Fatal("Session.Close did not invalidate its cursor")
	}
	if session.State() != SessionClosed {
		t.Fatalf("closed session state = %s", session.State())
	}
	if _, err := lookup.Query(ctx, []any{"kept"}); !errors.Is(err, ErrPreparedClosed) {
		t.Fatalf("Prepared survived owning Session.Close: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("idempotent Session.Close = %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("last Session.Close retained catalog writer lock: %v", err)
	}
	reopenedSession, err := reopened.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reopenedLookup := runtimePrepare(t, reopenedSession,
		`SELECT id FROM docs WHERE value = ?`)
	reopenedCursor, err := reopenedLookup.Query(ctx, []any{"v"})
	if err != nil {
		t.Fatal(err)
	}
	reopenedRows := 0
	for reopenedCursor.Next() {
		reopenedRows++
	}
	if reopenedRows != 2 {
		t.Fatalf("reopened online index rows = %d, want 2", reopenedRows)
	}
	_ = reopenedCursor.Close()
	_ = reopenedLookup.Close()
	_ = reopenedSession.Close()
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedRuntimeParamKindLookupAllocatesZero(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	update := runtimePrepare(t, session,
		`UPDATE docs SET "$doc" = ? WHERE id = ?`)
	defer update.Close()
	if allocs := testing.AllocsPerRun(1000, func() {
		if update.ParamKind(0) != ParamDocument ||
			update.ParamKind(1) != ParamScalar {
			panic("wrong parameter roles")
		}
	}); allocs != 0 {
		t.Fatalf("ParamKind allocated %.2f times, want zero", allocs)
	}
}

func TestTypedRuntimePGShapedAndExtendedScalarBindings(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	create := runtimePrepare(t, session, `
		CREATE TABLE valueset (
			id NUMBER PRIMARY KEY,
			label STRING,
			active BOOLEAN,
			n NUMBER
		)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session,
		`INSERT INTO valueset (id, label, active, n) VALUES (?, ?, ?, ?)`)

	id := query.Number("9007199254740993")
	label := "pointer slots"
	active := true
	n := int64(7)
	if _, err := insert.Exec(ctx, []any{&id, &label, &active, &n}); err != nil {
		t.Fatalf("PG-shaped flat INSERT: %v", err)
	}
	lookup := runtimePrepare(t, session,
		`SELECT label, active, n FROM valueset WHERE id = ?`)
	cursor, err := lookup.Query(ctx, []any{&id})
	if err != nil {
		t.Fatalf("PG-shaped point query: %v", err)
	}
	if !cursor.Next() {
		t.Fatal("PG-shaped point query returned no row")
	}
	if got, ok := cursor.Cell(0).Text(); !ok || got != label {
		t.Fatalf("label = (%q, %v), want %q", got, ok, label)
	}
	if got, ok := cursor.Cell(1).Bool(); !ok || !got {
		t.Fatalf("active = (%v, %v), want true", got, ok)
	}
	if got, ok := cursor.Cell(2).Int64(); !ok || got != n {
		t.Fatalf("n = (%d, %v), want %d", got, ok, n)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}

	shortInsert := runtimePrepare(t, session,
		`INSERT INTO valueset (id) VALUES (?)`)
	for _, value := range []any{
		int8(-8),
		uint64(^uint64(0)),
		float32(0.1),
	} {
		if _, err := shortInsert.Exec(ctx, []any{value}); err != nil {
			t.Fatalf("flat INSERT of %T: %v", value, err)
		}
		cursor, err := lookup.Query(ctx, []any{value})
		if err != nil {
			t.Fatalf("point query of %T: %v", value, err)
		}
		if !cursor.Next() {
			t.Fatalf("point query of %T returned no row", value)
		}
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
	}

	nullID := query.Number("10")
	var nullLabel *string
	if _, err := insert.Exec(
		ctx, []any{&nullID, nullLabel, &active, &n},
	); err != nil {
		t.Fatalf("typed nil pointer as SQL NULL: %v", err)
	}
}

func TestTypedRuntimeQueryIntoWarmAllocatesZero(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	if _, err := insert.Exec(
		ctx, []any{[]byte(`{"id":"a","value":"kept"}`)},
	); err != nil {
		t.Fatal(err)
	}
	lookup := runtimePrepare(t, session,
		`SELECT value FROM docs WHERE id = ?`)
	key := "a"
	args := []any{&key}
	var cursor Cursor
	run := func() {
		if err := lookup.QueryInto(ctx, args, &cursor); err != nil {
			panic(err)
		}
		if !cursor.Next() {
			panic("point query returned no row")
		}
		if err := cursor.Close(); err != nil {
			panic(err)
		}
	}
	run()
	if allocs := testing.AllocsPerRun(1000, run); allocs != 0 {
		t.Fatalf("warmed QueryInto allocated %.2f times, want zero", allocs)
	}
}

func TestTypedRuntimeBorrowedValueCopyWarmAllocatesZero(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	document := []byte(`{"id":"borrowed"}`)
	kinds := []ParamKind{ParamDocument}
	documentText := string(document)
	for _, values := range [][]any{
		{document},
		{&document},
		{&documentText},
	} {
		if _, err := session.conn.runtimeValues(kinds, values); err != nil {
			t.Fatal(err)
		}
		if allocs := testing.AllocsPerRun(1000, func() {
			got, err := session.conn.runtimeValues(kinds, values)
			if err != nil || len(got) != 1 {
				panic("runtimeValues failed")
			}
		}); allocs != 0 {
			t.Fatalf("borrowed %T copy allocated %.2f times, want zero",
				values[0], allocs)
		}
	}
}

func TestZeroSessionReportsClosed(t *testing.T) {
	var session Session
	if session.State() != SessionClosed {
		t.Fatalf("zero Session state = %s, want closed", session.State())
	}
}

func TestSessionIntermediateLimitLifecycle(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	if err := session.SetIntermediateLimit(-2); err == nil {
		t.Fatal("SetIntermediateLimit accepted a value below -1")
	}
	if got := session.conn.exec.Options.IntermediateBytes; got != 0 {
		t.Fatalf("rejected intermediate limit changed option to %d", got)
	}
	if err := session.SetIntermediateLimit(4096); err != nil {
		t.Fatal(err)
	}
	if got := session.conn.exec.Options.IntermediateBytes; got != 4096 {
		t.Fatalf("intermediate limit = %d, want 4096", got)
	}

	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	selectAll := runtimePrepare(t, session, `SELECT id FROM docs`)
	cursor, err := selectAll.Query(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.SetIntermediateLimit(8192); !errors.Is(err, ErrCursorOpen) {
		t.Fatalf("SetIntermediateLimit with live cursor = %v", err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(context.Background(), TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := session.SetIntermediateLimit(8192); !errors.Is(err, ErrTransactionActive) {
		t.Fatalf("SetIntermediateLimit in transaction = %v", err)
	}
	if err := session.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatal(err)
	}
	if got := session.conn.exec.Options.IntermediateBytes; got != -1 {
		t.Fatalf("unlimited intermediate option = %d, want -1", got)
	}
}

func TestTypedRuntimeCancellationAndExternalFailureState(t *testing.T) {
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
	if _, err := insert.Exec(ctx, []any{[]byte(`{"id":"a"}`)}); err != nil {
		t.Fatal(err)
	}
	selectAll := runtimePrepare(t, session, `SELECT id FROM docs`)

	var flag query.CancelFlag
	if err := session.SetCancelFlag(&flag); err != nil {
		t.Fatal(err)
	}
	flag.Cancel()
	if _, err := selectAll.Query(ctx, nil); !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("pre-canceled query = %v", err)
	}
	if session.State() != SessionIdle {
		t.Fatalf("autocommit cancellation changed state to %s", session.State())
	}
	flag.Reset()
	cursor, err := selectAll.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.SetCancelFlag(nil); !errors.Is(err, ErrCursorOpen) {
		t.Fatalf("SetCancelFlag with a live cursor = %v", err)
	}
	if _, err := session.Prepare(ctx, `SELECT id FROM docs`); !errors.Is(err, ErrCursorOpen) {
		t.Fatalf("Prepare with a live cursor = %v", err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(ctx, TxOptions{}); !errors.Is(err, ErrTransactionActive) {
		t.Fatalf("nested BEGIN = %v", err)
	}
	if err := session.SetCancelFlag(nil); !errors.Is(err, ErrTransactionActive) {
		t.Fatalf("SetCancelFlag in transaction = %v", err)
	}
	session.MarkFailed()
	if session.State() != SessionFailedTransaction {
		t.Fatalf("MarkFailed state = %s", session.State())
	}
	session.MarkFailed()
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	session.MarkFailed()
	if session.State() != SessionIdle {
		t.Fatalf("idle MarkFailed changed state to %s", session.State())
	}

	if err := session.SetCancelFlag(&flag); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	flag.Cancel()
	if _, err := selectAll.Query(ctx, nil); !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("transactional canceled query = %v", err)
	}
	if session.State() != SessionFailedTransaction {
		t.Fatalf("transaction cancellation state = %s", session.State())
	}
	flag.Reset()
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.SetCancelFlag(nil); err != nil {
		t.Fatal(err)
	}
}

func openRuntimeSession(t *testing.T) (*Database, *Session) {
	t.Helper()
	database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	return database, session
}

func runtimePrepare(t *testing.T, session *Session, statement string) *Prepared {
	t.Helper()
	prepared, err := session.Prepare(context.Background(), statement)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prepared.Close() })
	return prepared
}

func assertRuntimeCount(t *testing.T, prepared *Prepared, key string, want int64) {
	t.Helper()
	cursor, err := prepared.Query(context.Background(), []any{key})
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
	if cursor.Next() {
		t.Fatal("COUNT returned more than one row")
	}
}
