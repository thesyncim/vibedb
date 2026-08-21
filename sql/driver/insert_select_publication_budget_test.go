package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func TestRunInsertSelectReturningSharesAndTranslatesExactBudget(t *testing.T) {
	_, session := openInsertSelectPublicationSession(t)
	prepared := runtimePrepare(t, session, `
		INSERT INTO publication_probe_target
		SELECT * FROM publication_source ORDER BY id
		RETURNING id, payload, id AS id_copy, payload AS payload_copy`)
	defer prepared.Close()

	segment := appendPublicationDocuments(t, nil,
		`{"id":"alpha","payload":"0123456789abcdef"}`,
		`{"id":"beta","payload":"fedcba9876543210"}`,
	)
	defer segment.Reset()

	const (
		base       = int64(113)
		sourceRoot = int64(47)
	)
	probe := insertSelectIntermediateBudget{limit: -1, used: base + sourceRoot}
	original := query.ExecOptions{IntermediateBytes: 701}
	exec := query.Exec{Options: original}
	cursor, err := runInsertSelectReturning(
		prepared.statement.query, &exec, query.FromSegment(segment),
		&probe, sourceRoot,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rows := queryCursorRows(cursor); rows != 2 {
		t.Fatalf("RETURNING rows = %d, want 2", rows)
	}
	if exec.Options != original {
		t.Fatalf("executor options were not restored: got %+v want %+v", exec.Options, original)
	}
	required := probe.used
	if required <= base {
		t.Fatalf("RETURNING retained charge = %d, want positive", required-base)
	}
	// The exact final account is base plus RETURNING, not base plus the now
	// invalid source root plus RETURNING.
	if required == base+sourceRoot {
		t.Fatalf("RETURNING did not replace source root charge: account=%d", required)
	}
	exec.Release()

	for _, tc := range []struct {
		name  string
		limit int64
		ok    bool
	}{
		{name: "one byte below", limit: required - 1},
		{name: "exact boundary", limit: required, ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := insertSelectIntermediateBudget{
				limit: tc.limit, used: base + sourceRoot,
			}
			exec := query.Exec{Options: original}
			got, err := runInsertSelectReturning(
				prepared.statement.query, &exec, query.FromSegment(segment),
				&budget, sourceRoot,
			)
			defer exec.Release()
			if tc.ok {
				rows := queryCursorRows(got)
				if err != nil || rows != 2 || budget.used != required {
					t.Fatalf("exact boundary = rows %d, budget %d, error %v; want 2/%d/nil",
						rows, budget.used, err, required)
				}
			} else {
				var intermediate *query.IntermediateBudgetError
				if !errors.Is(err, query.ErrIntermediateBudget) ||
					!errors.As(err, &intermediate) {
					t.Fatalf("below boundary = %T %v, want IntermediateBudgetError", err, err)
				}
				if intermediate.Resource != insertSelectIntermediateResource ||
					intermediate.Bytes != required || intermediate.Limit != tc.limit {
					t.Fatalf("translated budget = %+v, want resource %q bytes %d limit %d",
						intermediate, insertSelectIntermediateResource, required, tc.limit)
				}
			}
			if exec.Options != original {
				t.Fatalf("executor options after %s = %+v, want %+v", tc.name, exec.Options, original)
			}
		})
	}

	returningBytes := required - base
	if returningBytes <= 1 {
		t.Fatalf("RETURNING retained bytes = %d, want > 1", returningBytes)
	}
	t.Run("result bytes win equal boundary", func(t *testing.T) {
		limit := returningBytes - 1
		budget := insertSelectIntermediateBudget{
			limit: base + limit, used: base + sourceRoot,
		}
		options := query.ExecOptions{
			IntermediateBytes: 701,
			ResultBytes:       limit,
		}
		exec := query.Exec{Options: options}
		_, err := runInsertSelectReturning(
			prepared.statement.query, &exec, query.FromSegment(segment),
			&budget, sourceRoot,
		)
		defer exec.Release()
		var resultBudget *query.ResultBudgetError
		if !errors.Is(err, query.ErrResultBudget) ||
			!errors.As(err, &resultBudget) ||
			resultBudget.Bytes != returningBytes ||
			resultBudget.ByteLimit != limit {
			t.Fatalf("equal byte boundary = %T %+v, want ResultBudgetError %d/%d",
				err, resultBudget, returningBytes, limit)
		}
		if exec.Options != options {
			t.Fatalf("options after equal boundary = %+v, want %+v", exec.Options, options)
		}
	})

	t.Run("result rows remain caller visible", func(t *testing.T) {
		budget := insertSelectIntermediateBudget{
			limit: required, used: base + sourceRoot,
		}
		options := query.ExecOptions{
			IntermediateBytes: 701,
			ResultRows:        1,
		}
		exec := query.Exec{Options: options}
		_, err := runInsertSelectReturning(
			prepared.statement.query, &exec, query.FromSegment(segment),
			&budget, sourceRoot,
		)
		defer exec.Release()
		var resultBudget *query.ResultBudgetError
		if !errors.Is(err, query.ErrResultBudget) ||
			!errors.As(err, &resultBudget) ||
			resultBudget.Rows != 2 || resultBudget.RowLimit != 1 {
			t.Fatalf("row boundary = %T %+v, want ResultBudgetError 2/1",
				err, resultBudget)
		}
		if exec.Options != options {
			t.Fatalf("options after row boundary = %+v, want %+v", exec.Options, options)
		}
	})

	t.Run("zero remainder invalidates source result", func(t *testing.T) {
		budget := insertSelectIntermediateBudget{
			limit: base, used: base + sourceRoot,
		}
		exec := query.Exec{Options: original}
		_, err := runInsertSelectReturning(
			prepared.statement.query, &exec, query.FromSegment(segment),
			&budget, sourceRoot,
		)
		defer exec.Release()
		var intermediate *query.IntermediateBudgetError
		if !errors.As(err, &intermediate) ||
			intermediate.Resource != insertSelectIntermediateResource ||
			intermediate.Limit != base || intermediate.Bytes <= base {
			t.Fatalf("zero remainder = %T %+v, want translated refusal above %d",
				err, intermediate, base)
		}
		if exec.Result.RowCount != 0 || exec.Result.RetainedBytes() != 0 {
			t.Fatalf("zero-remainder failure exposed source/result = rows %d bytes %d",
				exec.Result.RowCount, exec.Result.RetainedBytes())
		}
		if exec.Options != original {
			t.Fatalf("options after zero remainder = %+v, want %+v", exec.Options, original)
		}
	})
}

func TestInsertSelectReturningPublicationBudgetIsAtomic(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		name := "autocommit"
		if transaction {
			name = "transaction"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_, session := openInsertSelectPublicationSession(t)
			target := "publication_auto_target"
			if transaction {
				target = "publication_tx_target"
			}
			prepared := runtimePrepare(t, session, fmt.Sprintf(`
				INSERT INTO %s
				SELECT * FROM publication_source ORDER BY id
				RETURNING id, payload, id AS id_copy, payload AS payload_copy`, target))
			defer prepared.Close()

			required := probeInsertSelectReturningBudget(t, session, prepared, transaction)
			if required < 2 {
				t.Fatalf("required budget = %d, want >= 2", required)
			}
			if err := session.SetIntermediateLimit(required - 1); err != nil {
				t.Fatal(err)
			}
			if transaction {
				if err := session.Begin(ctx, TxOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			var returned Cursor
			err := prepared.QueryInto(ctx, nil, &returned)
			var intermediate *query.IntermediateBudgetError
			if !errors.Is(err, query.ErrIntermediateBudget) ||
				!errors.As(err, &intermediate) {
				t.Fatalf("one-byte-below execution = %T %v, want IntermediateBudgetError", err, err)
			}
			if intermediate.Resource != insertSelectIntermediateResource ||
				intermediate.Bytes != required || intermediate.Limit != required-1 {
				t.Fatalf("publication budget = %+v, want resource %q bytes %d limit %d",
					intermediate, insertSelectIntermediateResource, required, required-1)
			}
			if transaction {
				state := session.conn.tx.tables[target]
				if state == nil || len(state.pending) != 0 || len(state.order) != 0 ||
					state.stagedBytes != 0 {
					t.Fatalf("failed RETURNING changed transaction overlay: %+v", state)
				}
				if err := session.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
			}
			assertPublicationTargetCount(t, session, target, 0)
		})
	}
}

func TestTransactionInsertSelectStagesExactNativeRecordSize(t *testing.T) {
	document := []byte(`{"id":"native","payload":"record"}`)
	key := "\x01native"
	tape := make([]vibejson.IndexEntry, 7)
	txRecord := int64(unsafe.Sizeof(stagedTxMutation{}))
	seedRecord := int64(unsafe.Sizeof(seedDocument{}))
	want := int64(len(document)+len(key)) +
		int64(len(tape))*int64(unsafe.Sizeof(vibejson.IndexEntry{})) +
		txRecord + int64(unsafe.Sizeof(vibejson.Index{}))
	got := insertSelectStagedRowBytesFor(document, key, tape, txRecord)
	if got != want {
		t.Fatalf("native transaction staging charge = %d, want %d", got, want)
	}
	seed := insertSelectStagedRowBytesFor(document, key, tape, seedRecord)
	if got-seed != txRecord-seedRecord {
		t.Fatalf("transaction/seed charge delta = %d, want native record delta %d (tx=%d seed=%d)",
			got-seed, txRecord-seedRecord, txRecord, seedRecord)
	}

	ctx := context.Background()
	_, session := openInsertSelectPublicationSession(t)
	prepared := runtimePrepare(t, session, `
		INSERT INTO publication_tx_target
		SELECT * FROM publication_source ORDER BY id`)
	defer prepared.Close()
	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	tx := session.conn.tx
	if err := tx.beginMutationStatement(
		ctx, prepared.statement.mutation, prepared.statement,
	); err != nil {
		t.Fatal(err)
	}
	state := tx.tables["publication_tx_target"]
	var account insertSelectStageAccount
	staged, err := tx.stageInsertSelect(
		ctx, prepared.statement.mutation, nil, state,
		prepared.statement.insertSource, nil, &account,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(staged) != 2 || !account.active {
		t.Fatalf("probe staged/account = %d/%+v, want 2/active", len(staged), account)
	}
	exact := account.budget.used
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.SetIntermediateLimit(exact - 1); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := session.conn.tx.beginMutationStatement(
		ctx, prepared.statement.mutation, prepared.statement,
	); err != nil {
		t.Fatal(err)
	}
	var below insertSelectStageAccount
	_, err = session.conn.tx.stageInsertSelect(
		ctx, prepared.statement.mutation, nil,
		session.conn.tx.tables["publication_tx_target"],
		prepared.statement.insertSource, nil, &below,
	)
	var intermediate *query.IntermediateBudgetError
	if !errors.As(err, &intermediate) || intermediate.Bytes != exact ||
		intermediate.Limit != exact-1 {
		t.Fatalf("native record boundary = %T %+v, want bytes %d limit %d",
			err, intermediate, exact, exact-1)
	}
	if state := session.conn.tx.tables["publication_tx_target"]; len(state.pending) != 0 || len(state.order) != 0 || state.stagedBytes != 0 {
		t.Fatalf("failed native staging changed overlay: %+v", state)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPlacedInsertStreamingRoutingIsAtomicAndZeroAllocation(t *testing.T) {
	binding, router, same, diff := twoShardFixture(t)
	native := distribution.NewNativeMapperWithBucketBits(binding.spec.Arity, binding.spec.EffectiveBucketBits())
	binding.mapper = native
	binding.native = native
	sameDocs := [][]byte{doc(t, same[0]), doc(t, same[1])}
	seeds := []seedDocument{
		{key: "a", document: sameDocs[0]},
		{key: "b", document: sameDocs[1]},
	}
	staged := []stagedTxMutation{
		{key: "a", document: sameDocs[0]},
		{key: "b", document: sameDocs[1]},
	}
	c := conn{routing: router}

	if err := c.routeInsertSeedsWithBinding(binding, seeds); err != nil {
		t.Fatal(err)
	}
	var routeErr error
	allocs := testing.AllocsPerRun(1_000, func() {
		routeErr = c.routeInsertSeedsWithBinding(binding, seeds)
	})
	if routeErr != nil || allocs != 0 {
		t.Fatalf("warmed placed seed streaming = %v, %.2f allocs/run",
			routeErr, allocs)
	}

	if err := c.routeInsertStagedWithBinding(binding, staged); err != nil {
		t.Fatal(err)
	}
	allocs = testing.AllocsPerRun(1_000, func() {
		routeErr = c.routeInsertStagedWithBinding(binding, staged)
	})
	if routeErr != nil || allocs != 0 {
		t.Fatalf("warmed placed transaction streaming = %v, %.2f allocs/run",
			routeErr, allocs)
	}

	different := []seedDocument{
		{key: "left", document: doc(t, diff[0])},
		{key: "right", document: doc(t, diff[1])},
	}
	if err := c.routeInsertSeedsWithBinding(binding, different); !errors.Is(err, ErrCrossShardWrite) {
		t.Fatalf("cross-shard route = %v, want ErrCrossShardWrite", err)
	}
	if string(different[0].document) != string(doc(t, diff[0])) ||
		string(different[1].document) != string(doc(t, diff[1])) {
		t.Fatal("cross-shard refusal mutated staged publication records")
	}

	var unplaced conn
	allocs = testing.AllocsPerRun(1_000, func() {
		routeErr = unplaced.routeInsertSeedsWithBinding(nil, seeds)
	})
	if routeErr != nil || allocs != 0 || unplaced.routing != nil {
		t.Fatalf("unplaced seed route = %v, %.2f allocs, router=%p",
			routeErr, allocs, unplaced.routing)
	}
	allocs = testing.AllocsPerRun(1_000, func() {
		routeErr = unplaced.routeInsertStagedWithBinding(nil, staged)
	})
	if routeErr != nil || allocs != 0 || unplaced.routing != nil {
		t.Fatalf("unplaced transaction route = %v, %.2f allocs, router=%p",
			routeErr, allocs, unplaced.routing)
	}

	got := insertSelectStagedRowBytesFor(
		sameDocs[0], "a", nil, int64(unsafe.Sizeof(seedDocument{})),
	)
	want := int64(len(sameDocs[0])+len("a")) +
		int64(unsafe.Sizeof(seedDocument{})) +
		int64(unsafe.Sizeof(vibejson.Index{}))
	if got != want {
		t.Fatalf("unplaced staging charge = %d, want %d with no routing charge", got, want)
	}

	escapedBinding := publicationRoutingBinding(t)
	escapedDocument := []byte(`{"tenant_id":"sta\u0062le"}`)
	escapedSeeds := []seedDocument{{
		key: "stable", document: escapedDocument,
	}}
	var escaped conn
	if err := escaped.routeInsertSeedsWithBinding(
		escapedBinding, escapedSeeds,
	); err != nil {
		t.Fatal(err)
	}
	allocs = testing.AllocsPerRun(1_000, func() {
		routeErr = escaped.routeInsertSeedsWithBinding(
			escapedBinding, escapedSeeds,
		)
	})
	if routeErr != nil || allocs != 0 {
		t.Fatalf("escaped shard-key streaming = %v, %.2f allocs/run",
			routeErr, allocs)
	}
}

func TestDocumentShardKeyCompositeEscapesKeepStableScratchAliases(t *testing.T) {
	binding := publicationRoutingBinding(t)
	binding.placement.Columns = []string{"/first", "/second"}
	binding.spec.Arity = 2
	binding.pointers = make([]vibejson.CompiledPointer, 2)
	for i, pointer := range binding.placement.Columns {
		compiled, err := vibejson.CompilePointer(pointer)
		if err != nil {
			t.Fatal(err)
		}
		binding.pointers[i] = compiled
	}
	binding.native = distribution.NewNativeMapper(2)
	binding.mapper = binding.native
	document := []byte(`{"first":"alpha\u002dbeta","second":"gamma\u002ddelta"}`)
	var scalarScratch [distribution.KeyspaceWidth]distribution.Scalar
	key, text, err := documentShardKey(
		document, binding, scalarScratch[:0], nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := binding.native.PointFor(key)
	if err != nil {
		t.Fatal(err)
	}
	want, err := binding.native.PointFor([]distribution.Scalar{
		distribution.NewString("alpha-beta"),
		distribution.NewString("gamma-delta"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("composite escaped point = %x, want %x", got, want)
	}
	allocs := testing.AllocsPerRun(1_000, func() {
		key, text, err = documentShardKey(
			document, binding, scalarScratch[:0], text[:0],
		)
		if err != nil || len(key) != 2 {
			t.Fatalf("warmed composite key = %d, %v", len(key), err)
		}
	})
	if allocs != 0 {
		t.Fatalf("composite escaped shard key allocated %.2f/run", allocs)
	}
}

func TestPlacedInsertSelectCrossShardRefusalPublishesNothing(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		name := "autocommit"
		if transaction {
			name = "transaction"
		}
		t.Run(name, func(t *testing.T) {
			cfg, _, diff := twoShardClusterConfig(t, "stream_target")
			db := openTestCluster(t, cfg)
			for _, statement := range []string{
				`CREATE TABLE stream_source (tenant_id STRING PRIMARY KEY, state STRING)`,
				`CREATE TABLE stream_target (tenant_id STRING PRIMARY KEY, state STRING)`,
			} {
				if _, err := db.Exec(statement); err != nil {
					t.Fatalf("setup %q: %v", statement, err)
				}
			}
			if _, err := db.Exec(`INSERT INTO stream_source VALUES (?), (?)`,
				tenantDoc(diff[0], "left"), tenantDoc(diff[1], "right")); err != nil {
				t.Fatal(err)
			}
			const insert = `INSERT INTO stream_target SELECT * FROM stream_source ORDER BY tenant_id`
			if transaction {
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				_, err = tx.Exec(insert)
				if !errors.Is(err, ErrCrossShardWrite) {
					_ = tx.Rollback()
					t.Fatalf("transaction cross-shard INSERT SELECT = %v, want ErrCrossShardWrite", err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.Exec(insert); !errors.Is(err, ErrCrossShardWrite) {
				t.Fatalf("autocommit cross-shard INSERT SELECT = %v, want ErrCrossShardWrite", err)
			}
			var count int
			if err := db.QueryRow(`SELECT count(*) FROM stream_target`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("cross-shard refusal published %d rows", count)
			}
		})
	}
}

func BenchmarkInsertSelectPublicationStreamingRouting(b *testing.B) {
	binding := publicationRoutingBinding(b)
	document := []byte(`{"tenant_id":"stable"}`)
	seeds := []seedDocument{{key: "stable", document: document}}
	staged := []stagedTxMutation{{key: "stable", document: document}}

	b.Run("placed seeds warmed", func(b *testing.B) {
		c := conn{routing: distribution.NewRouter()}
		if err := c.routeInsertSeedsWithBinding(binding, seeds); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := c.routeInsertSeedsWithBinding(binding, seeds); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("placed transaction warmed", func(b *testing.B) {
		c := conn{routing: distribution.NewRouter()}
		if err := c.routeInsertStagedWithBinding(binding, staged); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := c.routeInsertStagedWithBinding(binding, staged); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("unplaced", func(b *testing.B) {
		var c conn
		b.ReportAllocs()
		for range b.N {
			if err := c.routeInsertSeedsWithBinding(nil, seeds); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func openInsertSelectPublicationSession(tb *testing.T) (*Database, *Session) {
	tb.Helper()
	database, session := openRuntimeSession(tb)
	for _, statement := range []string{
		`CREATE TABLE publication_source (id STRING PRIMARY KEY, payload STRING)`,
		`CREATE TABLE publication_probe_target (id STRING PRIMARY KEY, payload STRING)`,
		`CREATE TABLE publication_auto_target (id STRING PRIMARY KEY, payload STRING)`,
		`CREATE TABLE publication_tx_target (id STRING PRIMARY KEY, payload STRING)`,
		`INSERT INTO publication_source VALUES
			('{"id":"alpha","payload":"0123456789abcdef"}'),
			('{"id":"beta","payload":"fedcba9876543210"}')`,
	} {
		prepared := runtimePrepare(tb, session, statement)
		if _, err := prepared.Exec(context.Background(), nil); err != nil {
			prepared.Close()
			tb.Fatalf("setup %q: %v", statement, err)
		}
		if err := prepared.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	return database, session
}

func probeInsertSelectReturningBudget(
	tb *testing.T,
	session *Session,
	prepared *Prepared,
	transaction bool,
) int64 {
	tb.Helper()
	ctx := context.Background()
	if err := session.SetIntermediateLimit(-1); err != nil {
		tb.Fatal(err)
	}
	if transaction {
		if err := session.Begin(ctx, TxOptions{}); err != nil {
			tb.Fatal(err)
		}
		tx := session.conn.tx
		if err := tx.beginMutationStatement(
			ctx, prepared.statement.mutation, prepared.statement,
		); err != nil {
			tb.Fatal(err)
		}
		state := tx.tables[prepared.statement.mutation.Collection()]
		var account insertSelectStageAccount
		staged, err := tx.stageInsertSelect(
			ctx, prepared.statement.mutation, nil, state,
			prepared.statement.insertSource, nil, &account,
		)
		if err != nil {
			tb.Fatal(err)
		}
		if len(staged) == 0 || !account.active {
			tb.Fatalf("transaction probe staged/account = %d/%+v", len(staged), account)
		}
		if _, err := runInsertSelectReturning(
			prepared.statement.query, &session.conn.exec,
			query.FromSegment(&session.conn.pointDocs),
			&account.budget, account.sourceRootBytes,
		); err != nil {
			tb.Fatal(err)
		}
		required := account.budget.used
		if err := session.Rollback(ctx); err != nil {
			tb.Fatal(err)
		}
		return required
	}

	c := session.conn
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	cursor, sourceRetained, err := c.runInsertSourceLocked(
		ctx, prepared.statement.insertSource, nil,
	)
	if err != nil {
		tb.Fatal(err)
	}
	budget, err := newInsertSelectIntermediateBudget(c.exec.Options, sourceRetained)
	if err != nil {
		tb.Fatal(err)
	}
	sourceRoot := c.exec.Result.RetainedBytes()
	c.pointDocs.Reset()
	c.insertKeyRaw = c.insertKeyRaw[:0]
	target := c.db.tables[prepared.statement.mutation.Collection()]
	limits, err := tableMutationLimits(target)
	if err != nil {
		tb.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		document := cursor.Cell(0).Payload()
		if err := validateDocumentWithIntermediateBudget(
			target.schema, document, limits.MaxDocumentBytes,
			&c.insertTape, &budget,
		); err != nil {
			tb.Fatal(err)
		}
		var key string
		var keyCharge int64
		c.insertKeyRaw, key, keyCharge, err = appendDocumentKeyBudgeted(
			c.insertKeyRaw, document, target.meta.PrimaryKey,
			target.primary, limits.MaxKeyBytes, &budget,
		)
		if err != nil {
			tb.Fatal(err)
		}
		_ = keyCharge
		if err := budget.admit(insertSelectStagedRowBytesFor(
			document, key, c.insertTape,
			int64(unsafe.Sizeof(seedDocument{})),
		)); err != nil {
			tb.Fatal(err)
		}
		if _, err := c.pointDocs.Append(document); err != nil {
			tb.Fatal(err)
		}
		rows++
	}
	if rows != 2 {
		tb.Fatalf("autocommit probe rows = %d, want 2", rows)
	}
	if _, err := runInsertSelectReturning(
		prepared.statement.query, &c.exec, query.FromSegment(&c.pointDocs),
		&budget, sourceRoot,
	); err != nil {
		tb.Fatal(err)
	}
	return budget.used
}

func assertPublicationTargetCount(
	tb *testing.T,
	session *Session,
	target string,
	want int64,
) {
	tb.Helper()
	prepared := runtimePrepare(tb, session, `SELECT count(*) FROM `+target)
	defer prepared.Close()
	cursor, err := prepared.Query(context.Background(), nil)
	if err != nil {
		tb.Fatal(err)
	}
	if !cursor.Next() {
		tb.Fatal("missing target count")
	}
	got, ok := cursor.Cell(0).Int64()
	if !ok || got != want {
		tb.Fatalf("%s count = %d/%t, want %d", target, got, ok, want)
	}
	if err := cursor.Close(); err != nil {
		tb.Fatal(err)
	}
}

func queryCursorRows(cursor query.Cursor) int {
	rows := 0
	for cursor.Next() {
		rows++
	}
	return rows
}

func appendPublicationDocuments(
	tb testing.TB,
	dst *store.Segment,
	documents ...string,
) *store.Segment {
	tb.Helper()
	if dst == nil {
		dst = &store.Segment{}
	}
	for _, document := range documents {
		if _, err := dst.Append([]byte(document)); err != nil {
			tb.Fatal(err)
		}
	}
	return dst
}

func publicationRoutingBinding(tb testing.TB) *placementBinding {
	tb.Helper()
	manifest, err := distribution.NewManifest("publication", 1, []distribution.Shard{{
		ID: "s0", AllocationGeneration: 1,
		Range: distribution.KeyRange{
			Start: distribution.KeyspacePoint{},
			End:   distribution.KeyspaceEnd{Max: true},
		},
		Leaders: []distribution.EndpointID{"e0"},
	}})
	if err != nil {
		tb.Fatal(err)
	}
	pointer, err := vibejson.CompilePointer("/tenant_id")
	if err != nil {
		tb.Fatal(err)
	}
	native := distribution.NewNativeMapper(1)
	return &placementBinding{
		placement: distribution.TablePlacement{
			Table: "t", Distribution: "publication", Columns: []string{"/tenant_id"},
		},
		spec: distribution.DistributionSpec{
			Name: "publication", Arity: 1,
			MapperVersion: distribution.NativeMapperVersion,
		},
		mapper:   native,
		native:   native,
		manifest: manifest,
		pointers: []vibejson.CompiledPointer{pointer},
	}
}
