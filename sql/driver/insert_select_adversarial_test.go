package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func TestTransactionInsertSelectCancelFlagIsAtomicAndRecovers(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	for _, statement := range []string{
		`CREATE TABLE cancel_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE cancel_filter (id STRING PRIMARY KEY)`,
		`CREATE TABLE cancel_target (id STRING PRIMARY KEY)`,
		`INSERT INTO cancel_source VALUES ('{"id":"a"}')`,
		`INSERT INTO cancel_filter VALUES ('{"id":"a"}')`,
	} {
		prepared := runtimePrepare(t, session, statement)
		if _, err := prepared.Exec(ctx, nil); err != nil {
			prepared.Close()
			t.Fatalf("%s: %v", statement, err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
	}

	insert := runtimePrepare(t, session, `
		INSERT INTO cancel_target
		WITH picked AS MATERIALIZED (
			SELECT s.*
			FROM cancel_source AS s
			JOIN cancel_filter AS f ON s.id = f.id
		)
		SELECT * FROM picked`)
	defer insert.Close()

	var cancel query.CancelFlag
	if err := session.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	cancel.Cancel()
	if _, err := insert.Exec(ctx, nil); !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("transaction INSERT SELECT cancellation = %T %v, want ErrCanceled", err, err)
	}
	state := session.conn.tx.tables["cancel_target"]
	if state == nil || len(state.pending) != 0 || len(state.order) != 0 ||
		state.stagedBytes != 0 {
		t.Fatalf(
			"canceled INSERT SELECT changed transaction overlay: state=%+v",
			state,
		)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	cancel.Reset()
	result, err := insert.Exec(ctx, nil)
	if err != nil || result.RowsAffected != 1 {
		t.Fatalf("INSERT SELECT after cancellation = %+v, %v; want one row", result, err)
	}
}

func TestTransactionMutationCancelBridgeNilPathAllocatesZero(t *testing.T) {
	ctx := context.Background()
	var wrapped context.Context
	allocations := testing.AllocsPerRun(1_000, func() {
		wrapped = withCooperativeCancellation(ctx, nil)
	})
	if wrapped != ctx {
		t.Fatal("nil cancellation flag changed the transaction context")
	}
	if allocations != 0 {
		t.Fatalf("nil transaction cancellation bridge = %.2f allocs/run, want zero", allocations)
	}
}

func TestPreparedInsertSelectPhysicalSourceReplacementIsViewChanged(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		name := "autocommit"
		if transaction {
			name = "transaction"
		}
		t.Run(name, func(t *testing.T) {
			db := openTestDB(t)
			db.SetMaxOpenConns(4)
			for _, statement := range []string{
				`CREATE TABLE insert_rebind_source (id STRING PRIMARY KEY)`,
				`CREATE TABLE insert_rebind_backing (id STRING PRIMARY KEY)`,
				`CREATE TABLE insert_rebind_target (id STRING PRIMARY KEY)`,
				`INSERT INTO insert_rebind_source VALUES ('{"id":"old"}')`,
				`INSERT INTO insert_rebind_backing VALUES ('{"id":"new"}')`,
			} {
				if _, err := db.Exec(statement); err != nil {
					t.Fatalf("setup %q: %v", statement, err)
				}
			}

			const source = `INSERT INTO insert_rebind_target SELECT * FROM insert_rebind_source`
			var preparer interface {
				Prepare(string) (*stdsql.Stmt, error)
			}
			var tx *stdsql.Tx
			if transaction {
				var err error
				tx, err = db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				preparer = tx
			} else {
				preparer = db
			}
			prepared, err := preparer.Prepare(source)
			if err != nil {
				if tx != nil {
					_ = tx.Rollback()
				}
				t.Fatal(err)
			}
			defer prepared.Close()

			for _, replacement := range []string{
				`DROP TABLE insert_rebind_source`,
				`CREATE VIEW insert_rebind_source (id) AS SELECT id FROM insert_rebind_backing`,
			} {
				if _, err := db.Exec(replacement); err != nil {
					if tx != nil {
						_ = tx.Rollback()
					}
					t.Fatalf("replace source %q: %v", replacement, err)
				}
			}

			_, err = prepared.Exec()
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.Is(err, ErrViewChanged) || !errors.As(err, &unsupported) {
				t.Fatalf("execute = %T %v, want ErrViewChanged/FeatureNotSupported", err, err)
			}
			wantPosition := strings.LastIndex(source, "insert_rebind_source")
			if unsupported.Pos != wantPosition {
				t.Fatalf("source position = %d, want %d", unsupported.Pos, wantPosition)
			}
			var hinted interface{ SQLHint() string }
			if !errors.As(err, &hinted) ||
				!strings.Contains(hinted.SQLHint(), "prepare the INSERT statement again") {
				t.Fatalf("hint = %v, want INSERT-specific reprepare guidance", err)
			}
			if tx != nil {
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}

			var count int
			if err := db.QueryRow(`SELECT count(*) FROM insert_rebind_target`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("stale prepared INSERT published %d rows", count)
			}
		})
	}
}

const insertSelectMemorySource = `
	WITH joined(doc) AS MATERIALIZED (
		SELECT s.*
		FROM memory_source AS s
		JOIN memory_gate AS g ON s.group_id = g.id
		WHERE g.enabled = true
	), combined(doc) AS MATERIALIZED (
		SELECT doc FROM joined
		UNION ALL
		SELECT s.* FROM memory_source AS s WHERE id = 'absent'
	)
	SELECT d.doc
	FROM (SELECT doc FROM combined) AS d`

func openInsertSelectMemorySession(tb testing.TB) (*Database, *Session) {
	tb.Helper()
	database, err := Open(filepath.Join(tb.TempDir(), "insert-select-memory.vdb"))
	if err != nil {
		tb.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		_ = database.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := session.Close(); err != nil {
			tb.Error(err)
		}
		if err := database.Close(); err != nil {
			tb.Error(err)
		}
	})
	return database, session
}

func execInsertSelectMemorySQL(
	tb testing.TB,
	session *Session,
	statements ...string,
) {
	tb.Helper()
	for _, text := range statements {
		prepared, err := session.Prepare(context.Background(), text)
		if err != nil {
			tb.Fatalf("prepare %q: %v", text, err)
		}
		_, execErr := prepared.Exec(context.Background(), nil)
		closeErr := prepared.Close()
		if execErr != nil || closeErr != nil {
			tb.Fatalf("exec %q: %v", text, errors.Join(execErr, closeErr))
		}
	}
}

func setupInsertSelectMemoryBudgetFixture(tb testing.TB, session *Session) {
	tb.Helper()
	// Keep each document below the durable inline threshold while making its
	// validation tape substantially larger than its raw payload. That shape
	// makes shared atomic staging, rather than the query's historical peak, the
	// observable final boundary.
	items := strings.Repeat(`{"n":1},`, 23) + `{"n":1}`
	execInsertSelectMemorySQL(tb, session,
		`CREATE TABLE memory_source (`+
			`id STRING PRIMARY KEY, group_id STRING NOT NULL)`,
		`CREATE TABLE memory_gate (`+
			`id STRING PRIMARY KEY, enabled BOOL NOT NULL)`,
		`INSERT INTO memory_gate VALUES ('{"id":"g","enabled":true}')`,
	)
	seed, err := session.Prepare(
		context.Background(), `INSERT INTO memory_source VALUES (?)`,
	)
	if err != nil {
		tb.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		document := `{"id":"` + id + `","group_id":"g","items":[` + items + `]}`
		if _, err := seed.Exec(context.Background(), []any{document}); err != nil {
			_ = seed.Close()
			tb.Fatalf("seed memory source %q: %v", id, err)
		}
	}
	if err := seed.Close(); err != nil {
		tb.Fatal(err)
	}
	for _, target := range []string{
		"memory_auto_probe", "memory_auto_below", "memory_auto_exact",
		"memory_tx_probe", "memory_tx_below", "memory_tx_exact",
	} {
		execInsertSelectMemorySQL(tb, session,
			`CREATE TABLE `+target+` (`+
				`id STRING PRIMARY KEY, group_id STRING NOT NULL)`)
	}
}

func prepareInsertSelectMemory(
	t testing.TB,
	session *Session,
	target string,
) *Prepared {
	t.Helper()
	prepared, err := session.Prepare(
		context.Background(), `INSERT INTO `+target+insertSelectMemorySource,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	return prepared
}

func retainedInsertSelectSource(
	t *testing.T,
	session *Session,
	prepared *Prepared,
	transaction bool,
) (int64, int64) {
	t.Helper()
	plan := prepared.statement.insertSource
	if plan == nil || plan.statement == nil {
		t.Fatal("prepared INSERT has no source plan")
	}
	var (
		cursor   query.Cursor
		retained int64
		err      error
	)
	if transaction {
		cursor, retained, err = session.conn.tx.runInsertSource(
			context.Background(), plan, nil,
		)
	} else {
		session.conn.db.mu.Lock()
		cursor, retained, err = session.conn.runInsertSourceLocked(
			context.Background(), plan, nil,
		)
		session.conn.db.mu.Unlock()
	}
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows == 0 {
		t.Fatal("source returned no rows")
	}
	root := session.conn.exec.Result.RetainedBytes()
	if root <= 0 || retained < root {
		t.Fatalf(
			"source retained/root = %d/%d",
			retained, root,
		)
	}
	return retained, root
}

func targetPublication(
	session *Session,
	tableName string,
) (materialized bool, generation uint64) {
	database := session.conn.db
	database.mu.RLock()
	table := database.tables[tableName]
	if table != nil && table.collection != nil {
		materialized = true
		generation = table.collection.Generation()
	}
	database.mu.RUnlock()
	return materialized, generation
}

func assertEmptyInsertSelectTarget(
	t *testing.T,
	session *Session,
	tableName string,
) {
	t.Helper()
	if materialized, generation := targetPublication(session, tableName); materialized {
		t.Fatalf(
			"target %q was published at generation %d after rejected statement",
			tableName, generation,
		)
	}
}

func discoverInsertSelectMemoryBoundary(
	t *testing.T,
	session *Session,
	prepared *Prepared,
	transaction bool,
) (limit int64, sawQuery, sawStaging bool) {
	t.Helper()
	target := prepared.statement.tree.Insert.Table
	limit = 1
	for attempt := 0; attempt < 64; attempt++ {
		if err := session.SetIntermediateLimit(limit); err != nil {
			t.Fatal(err)
		}
		if transaction {
			if err := session.Begin(context.Background(), TxOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		result, err := prepared.Exec(context.Background(), nil)
		if err == nil {
			if result.RowsAffected != 3 {
				t.Fatalf("exact-bound probe affected %d rows, want 3", result.RowsAffected)
			}
			if transaction {
				state := session.conn.tx.tables[target]
				if state == nil || len(state.pending) != 3 || len(state.order) != 3 ||
					state.stagedBytes == 0 {
					t.Fatalf("successful transaction staging = %+v", state)
				}
				if rollbackErr := session.Rollback(context.Background()); rollbackErr != nil {
					t.Fatal(rollbackErr)
				}
				assertEmptyInsertSelectTarget(t, session, target)
			}
			return limit, sawQuery, sawStaging
		}
		var budget *query.IntermediateBudgetError
		if !errors.Is(err, query.ErrIntermediateBudget) ||
			!errors.As(err, &budget) {
			t.Fatalf("limit %d = %T %v, want IntermediateBudgetError", limit, err, err)
		}
		if budget.Limit != limit || budget.Bytes <= limit {
			t.Fatalf("non-progressing budget refusal = %+v", budget)
		}
		if budget.Resource == insertSelectIntermediateResource {
			sawStaging = true
		} else {
			sawQuery = true
		}
		if transaction {
			state := session.conn.tx.tables[target]
			if state == nil || len(state.pending) != 0 || len(state.order) != 0 ||
				state.stagedBytes != 0 {
				t.Fatalf("rejected transaction changed overlay: %+v", state)
			}
			if rollbackErr := session.Rollback(context.Background()); rollbackErr != nil {
				t.Fatal(rollbackErr)
			}
		}
		assertEmptyInsertSelectTarget(t, session, target)
		limit = budget.Bytes
	}
	t.Fatal("intermediate boundary discovery did not converge")
	return 0, false, false
}

func assertInsertSelectExactMemoryBoundary(
	t *testing.T,
	session *Session,
	prepared *Prepared,
	limit int64,
	transaction bool,
	wantSuccess bool,
) {
	t.Helper()
	target := prepared.statement.tree.Insert.Table
	if err := session.SetIntermediateLimit(limit); err != nil {
		t.Fatal(err)
	}
	if transaction {
		if err := session.Begin(context.Background(), TxOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := prepared.Exec(context.Background(), nil)
	if !wantSuccess {
		var budget *query.IntermediateBudgetError
		if !errors.As(err, &budget) || !errors.Is(err, query.ErrIntermediateBudget) {
			t.Fatalf("below-bound execution = %T %v", err, err)
		}
		if budget.Bytes != limit+1 || budget.Limit != limit ||
			budget.Resource != insertSelectIntermediateResource {
			t.Fatalf("below-bound refusal = %+v, want exact %d/%d staging refusal",
				budget, limit+1, limit)
		}
		if transaction {
			state := session.conn.tx.tables[target]
			if state == nil || len(state.pending) != 0 || len(state.order) != 0 ||
				state.stagedBytes != 0 {
				t.Fatalf("below-bound transaction changed overlay: %+v", state)
			}
			if rollbackErr := session.Rollback(context.Background()); rollbackErr != nil {
				t.Fatal(rollbackErr)
			}
		}
		assertEmptyInsertSelectTarget(t, session, target)
		return
	}
	if err != nil || result.RowsAffected != 3 {
		t.Fatalf("at-bound execution = %+v, %v; want 3 rows", result, err)
	}
	if transaction {
		state := session.conn.tx.tables[target]
		if state == nil || len(state.pending) != 3 || len(state.order) != 3 ||
			state.stagedBytes == 0 {
			t.Fatalf("at-bound transaction staging = %+v", state)
		}
		if rollbackErr := session.Rollback(context.Background()); rollbackErr != nil {
			t.Fatal(rollbackErr)
		}
		assertEmptyInsertSelectTarget(t, session, target)
		return
	}
	if materialized, generation := targetPublication(session, target); !materialized || generation == 0 {
		t.Fatalf("at-bound autocommit did not publish: %t/%d", materialized, generation)
	}
}

func TestInsertSelectIntermediateBudgetOwnsNestedRootAndAtomicStaging(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		name := "autocommit"
		prefix := "memory_auto_"
		if transaction {
			name = "transaction"
			prefix = "memory_tx_"
		}
		t.Run(name, func(t *testing.T) {
			_, session := openInsertSelectMemorySession(t)
			setupInsertSelectMemoryBudgetFixture(t, session)
			probe := prepareInsertSelectMemory(t, session, prefix+"probe")
			below := prepareInsertSelectMemory(t, session, prefix+"below")
			exact := prepareInsertSelectMemory(t, session, prefix+"exact")

			if err := session.SetIntermediateLimit(-1); err != nil {
				t.Fatal(err)
			}
			if transaction {
				if err := session.Begin(context.Background(), TxOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			retained, root := retainedInsertSelectSource(t, session, probe, transaction)
			if retained <= root {
				t.Fatalf("retained/root = %d/%d", retained, root)
			}
			if transaction {
				if err := session.Rollback(context.Background()); err != nil {
					t.Fatal(err)
				}
			}

			boundary, sawQuery, sawStaging := discoverInsertSelectMemoryBoundary(
				t, session, probe, transaction,
			)
			if boundary <= retained || !sawQuery || !sawStaging {
				t.Fatalf(
					"boundary/source/query/staging = %d/%d/%t/%t",
					boundary, retained, sawQuery, sawStaging,
				)
			}
			assertInsertSelectExactMemoryBoundary(
				t, session, below, boundary-1, transaction, false,
			)
			assertInsertSelectExactMemoryBoundary(
				t, session, exact, boundary, transaction, true,
			)
		})
	}
}

func TestInsertSelectAdmissionPrecedesScratchGrowthAndPublication(t *testing.T) {
	_, session := openInsertSelectMemorySession(t)
	execInsertSelectMemorySQL(t, session,
		`CREATE TABLE memory_admission_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE memory_admission_target (id STRING PRIMARY KEY)`,
	)
	document := `{"id":"` + strings.Repeat("k", 96) + `","items":[` +
		strings.Repeat(`{"n":1},`, 127) + `{"n":1}]}`
	insertSource, err := session.Prepare(
		context.Background(), `INSERT INTO memory_admission_source VALUES (?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := insertSource.Exec(context.Background(), []any{document}); err != nil {
		t.Fatal(err)
	}
	if err := insertSource.Close(); err != nil {
		t.Fatal(err)
	}

	prepared, err := session.Prepare(context.Background(),
		`INSERT INTO memory_admission_target SELECT * FROM memory_admission_source`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatal(err)
	}
	retained, _ := retainedInsertSelectSource(t, session, prepared, false)
	if cap(session.conn.insertTape) != 0 || session.conn.pointDocs.Len() != 0 {
		t.Fatal("fixture unexpectedly warmed INSERT SELECT staging scratch")
	}
	if err := session.SetIntermediateLimit(retained); err != nil {
		t.Fatal(err)
	}
	_, err = prepared.Exec(context.Background(), nil)
	var budget *query.IntermediateBudgetError
	if !errors.As(err, &budget) || budget.Resource != insertSelectIntermediateResource ||
		budget.Limit != retained || budget.Bytes <= retained {
		t.Fatalf("validation refusal = %+v / %v", budget, err)
	}
	if cap(session.conn.insertTape) != 0 || len(session.conn.insertKeyRaw) != 0 ||
		len(session.conn.insertSeen) != 0 || session.conn.pointDocs.Len() != 0 ||
		len(session.conn.insertSeeds) != 0 {
		t.Fatalf(
			"rejected growth tape/key/map/segment/seeds = %d/%d/%d/%d/%d",
			cap(session.conn.insertTape), len(session.conn.insertKeyRaw),
			len(session.conn.insertSeen), session.conn.pointDocs.Len(),
			len(session.conn.insertSeeds),
		)
	}
	assertEmptyInsertSelectTarget(t, session, "memory_admission_target")

	// Exercise the exact pre-growth contracts directly. The key charge includes
	// the retained map entry, and the stage charge includes the owned document,
	// validation tape, publication record, and Segment handle.
	session.conn.db.mu.RLock()
	target := session.conn.db.tables["memory_admission_target"]
	session.conn.db.mu.RUnlock()
	if target == nil {
		t.Fatal("missing admission target")
	}
	limits, err := tableMutationLimits(target)
	if err != nil {
		t.Fatal(err)
	}
	var tape []vibejson.IndexEntry
	validation := insertSelectIntermediateBudget{limit: 0}
	err = validateDocumentWithIntermediateBudget(
		target.schema, []byte(document), limits.MaxDocumentBytes,
		&tape, &validation,
	)
	if !errors.As(err, &budget) || budget.Bytes <= 0 || cap(tape) != 0 {
		t.Fatalf("pre-growth validation = tape %d, error %#v", cap(tape), err)
	}
	validation.limit = budget.Bytes
	for attempt := 0; ; attempt++ {
		if attempt == 8 {
			t.Fatal("validation boundary did not converge")
		}
		err = validateDocumentWithIntermediateBudget(
			target.schema, []byte(document), limits.MaxDocumentBytes,
			&tape, &validation,
		)
		if err == nil {
			break
		}
		if !errors.As(err, &budget) || budget.Bytes <= validation.limit {
			t.Fatalf("validation boundary = tape %d, error %#v", len(tape), err)
		}
		validation.limit = budget.Bytes
	}
	if len(tape) == 0 {
		t.Fatal("exact validation boundary retained no tape")
	}

	keyProbe := insertSelectIntermediateBudget{limit: -1}
	_, encodedKey, keyCharge, err := appendDocumentKeyBudgeted(
		nil, []byte(document), target.meta.PrimaryKey, target.primary,
		limits.MaxKeyBytes, &keyProbe,
	)
	if err != nil || keyCharge <= int64(len(encodedKey)) {
		t.Fatalf("key/map probe = key %d, charge %d, error %v",
			len(encodedKey), keyCharge, err)
	}
	keyBudget := insertSelectIntermediateBudget{limit: keyCharge - 1}
	keyRaw, _, _, err := appendDocumentKeyBudgeted(
		nil, []byte(document), target.meta.PrimaryKey, target.primary,
		limits.MaxKeyBytes, &keyBudget,
	)
	if !errors.As(err, &budget) || budget.Bytes != keyCharge ||
		budget.Limit != keyCharge-1 || len(keyRaw) != 0 || cap(keyRaw) != 0 {
		t.Fatalf("pre-growth key/map admission = key %d/%d, error %#v",
			len(keyRaw), cap(keyRaw), err)
	}

	stageCharge := insertSelectStagedRowBytes([]byte(document), encodedKey, tape)
	stageBudget := insertSelectIntermediateBudget{limit: stageCharge - 1}
	var segment store.Segment
	if err := stageBudget.admit(stageCharge); !errors.As(err, &budget) ||
		budget.Bytes != stageCharge || segment.Len() != 0 {
		t.Fatalf("pre-growth Segment admission = len %d, error %#v", segment.Len(), err)
	}
	stageBudget.limit = stageCharge
	if err := stageBudget.admit(stageCharge); err != nil {
		t.Fatal(err)
	}
	if _, err := segment.Append([]byte(document)); err != nil || segment.Len() != 1 {
		t.Fatalf("exact Segment boundary = len %d, error %v", segment.Len(), err)
	}

	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatal(err)
	}
	result, err := prepared.Exec(context.Background(), nil)
	if err != nil || result.RowsAffected != 1 {
		t.Fatalf("recovery = %+v, %v", result, err)
	}
}

func setupLargeConflictInsertSelect(
	tb testing.TB,
) (*Session, *Prepared, *Prepared, string) {
	tb.Helper()
	_, session := openInsertSelectMemorySession(tb)
	execInsertSelectMemorySQL(tb, session,
		`CREATE TABLE memory_conflict_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE memory_conflict_target (id STRING PRIMARY KEY)`,
		`INSERT INTO memory_conflict_source VALUES ('{"id":"same"}')`,
	)
	large := `{"id":"same","payload":"` + strings.Repeat("x", 64<<10) + `"}`
	seed, err := session.Prepare(
		context.Background(), `INSERT INTO memory_conflict_target VALUES (?)`,
	)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := seed.Exec(context.Background(), []any{large}); err != nil {
		tb.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		tb.Fatal(err)
	}
	selectInsert, err := session.Prepare(context.Background(),
		`INSERT INTO memory_conflict_target SELECT * FROM memory_conflict_source `+
			`ON CONFLICT DO NOTHING`)
	if err != nil {
		tb.Fatal(err)
	}
	valueInsert, err := session.Prepare(context.Background(),
		`INSERT INTO memory_conflict_target VALUES (?) ON CONFLICT DO NOTHING`)
	if err != nil {
		_ = selectInsert.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := selectInsert.Close(); err != nil {
			tb.Error(err)
		}
		if err := valueInsert.Close(); err != nil {
			tb.Error(err)
		}
	})
	return session, selectInsert, valueInsert, large
}

func runLargeConflictNoCopy(
	tb testing.TB,
	session *Session,
	prepared *Prepared,
	transaction bool,
	runs int,
) {
	tb.Helper()
	if transaction {
		if err := session.Begin(context.Background(), TxOptions{}); err != nil {
			tb.Fatal(err)
		}
	}
	session.conn.pointRaw = nil
	for range runs {
		result, err := prepared.Exec(context.Background(), nil)
		if err != nil || result.RowsAffected != 0 {
			tb.Fatalf("large conflict result = %+v, %v", result, err)
		}
		if len(session.conn.pointRaw) != 0 || cap(session.conn.pointRaw) != 0 {
			tb.Fatalf("conflict check copied payload into %d/%d bytes",
				len(session.conn.pointRaw), cap(session.conn.pointRaw))
		}
	}
}

func TestInsertSelectLargeConflictDoesNotCopyPayloadAndWarmPathsAllocateZero(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		name := "autocommit"
		if transaction {
			name = "transaction"
		}
		t.Run(name, func(t *testing.T) {
			session, prepared, _, large := setupLargeConflictInsertSelect(t)
			runLargeConflictNoCopy(t, session, prepared, transaction, 3)
			var result Result
			var runErr error
			allocations := testing.AllocsPerRun(100, func() {
				result, runErr = prepared.Exec(context.Background(), nil)
			})
			if runErr != nil || result.RowsAffected != 0 || allocations != 0 {
				t.Fatalf("warm no-op = %+v, %v, %.2f allocs", result, runErr, allocations)
			}
			if cap(session.conn.pointRaw) != 0 {
				t.Fatalf("warm no-op retained %d payload bytes", cap(session.conn.pointRaw))
			}
			if transaction {
				state := session.conn.tx.tables["memory_conflict_target"]
				if state == nil || len(state.pending) != 0 || len(state.order) != 0 ||
					state.stagedBytes != 0 {
					t.Fatalf("no-op changed transaction overlay: %+v", state)
				}
				if err := session.Rollback(context.Background()); err != nil {
					t.Fatal(err)
				}
			}

			session.conn.db.mu.RLock()
			target := session.conn.db.tables["memory_conflict_target"]
			session.conn.db.mu.RUnlock()
			limits, err := tableMutationLimits(target)
			if err != nil {
				t.Fatal(err)
			}
			key, err := documentKey(
				[]byte(large), target.meta.PrimaryKey, target.primary, limits.MaxKeyBytes,
			)
			if err != nil {
				t.Fatal(err)
			}
			raw, found, err := target.collection.AppendRaw(nil, []byte(key))
			if err != nil || !found || string(raw) != large {
				t.Fatalf("large target changed: found=%t bytes=%d error=%v",
					found, len(raw), err)
			}
		})
	}

	t.Run("absent INSERT SELECT branch", func(t *testing.T) {
		_, session := openInsertSelectMemorySession(t)
		prepared, err := session.Prepare(context.Background(), `VALUES (?)`)
		if err != nil {
			t.Fatal(err)
		}
		defer prepared.Close()
		args := []any{"ordinary scalar"}
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
		allocations := testing.AllocsPerRun(100, func() {
			runErr = prepared.QueryInto(context.Background(), args, &cursor)
			if runErr == nil {
				runErr = cursor.Close()
			}
		})
		if runErr != nil || allocations != 0 {
			t.Fatalf("absent-source path = %v, %.2f allocs", runErr, allocations)
		}
	})
}

func TestAutocommitInsertSelectMemoryCancellationIsAtomicAndRecovers(t *testing.T) {
	_, session := openInsertSelectMemorySession(t)
	setupInsertSelectMemoryBudgetFixture(t, session)
	prepared := prepareInsertSelectMemory(t, session, "memory_auto_probe")
	var cancel query.CancelFlag
	if err := session.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	cancel.Cancel()
	if _, err := prepared.Exec(context.Background(), nil); !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("canceled autocommit = %T %v", err, err)
	}
	if session.conn.exec.Result.RetainedBytes() != 0 ||
		session.conn.pointDocs.Len() != 0 || len(session.conn.insertSeeds) != 0 {
		t.Fatalf("cancellation retained result/segment/seeds = %d/%d/%d",
			session.conn.exec.Result.RetainedBytes(), session.conn.pointDocs.Len(),
			len(session.conn.insertSeeds))
	}
	assertEmptyInsertSelectTarget(t, session, "memory_auto_probe")
	cancel.Reset()
	result, err := prepared.Exec(context.Background(), nil)
	if err != nil || result.RowsAffected != 3 {
		t.Fatalf("post-cancel recovery = %+v, %v", result, err)
	}
}

func BenchmarkInsertSelectLargeConflictNoCopyWarm(b *testing.B) {
	for _, transaction := range []bool{false, true} {
		name := "autocommit"
		if transaction {
			name = "transaction"
		}
		b.Run(name, func(b *testing.B) {
			session, prepared, _, _ := setupLargeConflictInsertSelect(b)
			runLargeConflictNoCopy(b, session, prepared, transaction, 3)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := prepared.Exec(context.Background(), nil)
				if err != nil || result.RowsAffected != 0 {
					b.Fatalf("result = %+v, %v", result, err)
				}
			}
			b.StopTimer()
			if cap(session.conn.pointRaw) != 0 {
				b.Fatalf("conflict benchmark copied %d payload bytes", cap(session.conn.pointRaw))
			}
			if transaction {
				if err := session.Rollback(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkInsertSelectMemoryAbsentBranchWarm(b *testing.B) {
	_, session := openInsertSelectMemorySession(b)
	prepared, err := session.Prepare(context.Background(), `VALUES (?)`)
	if err != nil {
		b.Fatal(err)
	}
	defer prepared.Close()
	args := []any{"ordinary scalar"}
	var cursor Cursor
	for range 3 {
		if err := prepared.QueryInto(context.Background(), args, &cursor); err != nil {
			b.Fatal(err)
		}
		if err := cursor.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := prepared.QueryInto(context.Background(), args, &cursor); err != nil {
			b.Fatal(err)
		}
		if err := cursor.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
