package driver

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/query"
)

func TestReplicatedReadReuseLayoutTokensOutliveEpochs(t *testing.T) {
	seen := make(map[*catalogLayoutIdentity]bool)
	for i := 0; i < 4096; i++ {
		epoch := newCatalogLayoutEpoch(nil, nil)
		token := layoutIdentityToken(epoch)
		if token == nil || seen[token] {
			t.Fatal("a live cache identity was reused for a different epoch")
		}
		seen[token] = true
		if i%128 == 0 {
			runtime.GC()
		}
	}
	if unsafe.Sizeof(catalogLayoutIdentity{}) == 0 {
		t.Fatal("zero-size identities do not guarantee distinct addresses")
	}
}

func TestReplicatedReadReusePreparationOwnsSourceAndAccountsObjects(t *testing.T) {
	db, session := openRuntimeSession(t)
	defer db.Close()
	defer session.Close()
	create := runtimePrepare(t, session, `CREATE TABLE docs (id STRING PRIMARY KEY, score INTEGER)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	_ = create.Close()

	const text = `SELECT id, score FROM docs WHERE id >= ? ORDER BY id LIMIT 64`
	backing := strings.Repeat("x", 1<<20) + text
	input := backing[len(backing)-len(text):]
	reader := &ReplicatedReadSession{conn: conn{db: session.conn.db}}
	reader.session = Session{conn: &reader.conn, state: SessionIdle}
	defer reader.Close()
	prepared, err := prepareReplicatedRead(context.Background(), reader, input, []ParamType{ParamTypeText})
	if err != nil {
		t.Fatal(err)
	}
	statement := prepared.statement
	if statement.text != input || unsafe.StringData(statement.text) == unsafe.StringData(input) {
		t.Fatal("cached statement retained the caller's SQL backing")
	}
	// AST identifiers are copied into the parser's counted arenas. The lexer
	// and query diagnostic source still borrow the original parse input.
	parserSource := reflect.ValueOf(statement.parser).Elem().FieldByName("lx").FieldByName("src").String()
	querySource := reflect.ValueOf(statement.query).Elem().FieldByName("text").String()
	if unsafe.StringData(parserSource) != unsafe.StringData(statement.text) ||
		unsafe.StringData(querySource) != unsafe.StringData(statement.text) {
		t.Fatal("parser/query diagnostic source did not use the owned SQL clone")
	}
	runtime.KeepAlive(backing)
	if reader.conn.retainReadPreparation {
		t.Fatal("private preparation mode remained enabled")
	}
	if err := statement.resetReadForReuse(); err != nil {
		t.Fatal(err)
	}
	slot := &replicatedReadReuseSlot{reader: reader, prepared: prepared}
	got, ok := retainedReplicatedReadSlotBytes(slot)
	if !ok {
		t.Fatal("restricted preparation was not accountable")
	}
	statementBytes, ok := statement.readReuseRetainedBytes()
	if !ok {
		t.Fatal("statement was not accountable")
	}
	minimum := statementBytes + int64(unsafe.Sizeof(*reader)) + int64(unsafe.Sizeof(*prepared)) +
		int64(unsafe.Sizeof(replicatedReadReuseCache{})) + int64(unsafe.Sizeof(catalogLayoutIdentity{}))
	if got < minimum {
		t.Fatalf("retained bytes %d omit owned fixed objects: minimum %d", got, minimum)
	}
	var cancel query.CancelFlag
	reader.conn.exec.Options.Cancel = &cancel
	reader.conn.exec.Options.ResultBytes = 1234
	if err := reader.resetForReadReuse(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reader.conn.exec, query.Exec{}) {
		t.Fatal("idle reader retained execution options or request cancellation")
	}
	before := got
	statement.paramPositions = make([]int, 0, 1024)
	got, ok = retainedReplicatedReadSlotBytes(slot)
	if !ok || got-before != int64(1024*unsafe.Sizeof(int(0))) {
		t.Fatalf("capacity accounting delta=%d, accepted=%v", got-before, ok)
	}
	ordinary := runtimePrepare(t, session, text)
	defer ordinary.Close()
	if ordinary.statement.parser != nil || ordinary.statement.text != "" {
		t.Fatal("ordinary preparation retained cache-only parser/source storage")
	}
}

// Trigger publication precisely after cache lookup, at the constructor's first
// context checkpoint. No production synchronization hooks are necessary.
type readReuseReviewContext struct {
	context.Context
	once func()
}

func (c *readReuseReviewContext) Done() <-chan struct{} {
	if hook := c.once; hook != nil {
		c.once = nil
		hook()
	}
	return nil
}

func TestReplicatedReadReusePublicationBetweenLookupAndAttach(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "hit"}[warm], func(t *testing.T) {
			f := newReplicatedReadReuseFixture(t)
			params := []ParamType{ParamTypeText, ParamTypeText}
			if warm {
				lease := acquireReplicatedReadReuseData(t, f, &f.cut, replicatedReadReuseScoreSQL, params, replicatedReadReuseOptions())
				if err := lease.Finish(nil); err != nil {
					t.Fatal(err)
				}
			}
			ctx := &readReuseReviewContext{Context: context.Background(), once: func() {
				core := f.claim.database
				core.mu.Lock()
				core.layoutEpoch = newCatalogLayoutEpoch(core.tables, core.catalog.Views)
				core.mu.Unlock()
			}}
			lease, err := f.claim.AcquireReplicatedDataRead(ctx, &f.cut, replicatedReadReuseScoreSQL, params, false, replicatedReadReuseOptions())
			if warm {
				if lease != nil || !errors.Is(err, ErrReplicatedReadReuseUnsupported) {
					if lease != nil {
						_ = lease.Abort(nil)
					}
					t.Fatalf("stale hit returned lease=%v error=%v", lease, err)
				}
			} else {
				if err != nil || lease == nil {
					t.Fatalf("cold race: %v", err)
				}
				if lease.slot.cacheable {
					_ = lease.Abort(nil)
					t.Fatal("cold preparation was relabeled with a newer epoch")
				}
				if err := queryReplicatedReadReuseScore(t, lease, []any{"row-00000000", "row-00000064"}, f.rows); err != nil {
					_ = lease.Abort(err)
					t.Fatal(err)
				}
				if err := lease.Finish(nil); err != nil {
					t.Fatal(err)
				}
			}
			if stats := f.claim.replicatedReadReuseStats(); stats.RetainedSlots != 0 || stats.RetainedBytes != 0 {
				t.Fatalf("raced preparation was retained: %+v", stats)
			}
		})
	}
}

func TestReplicatedReadReuseAcquirePanicRetiresReservation(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "hit"}[warm], func(t *testing.T) {
			f := newReplicatedReadReuseFixture(t)
			params := []ParamType{ParamTypeText, ParamTypeText}
			if warm {
				lease := acquireReplicatedReadReuseData(t, f, &f.cut, replicatedReadReuseScoreSQL, params, replicatedReadReuseOptions())
				if err := lease.Finish(nil); err != nil {
					t.Fatal(err)
				}
			}
			const cause = "injected constructor panic"
			ctx := &readReuseReviewContext{Context: context.Background(), once: func() { panic(cause) }}
			func() {
				defer func() {
					if got := recover(); got != cause {
						t.Fatalf("panic=%v, want %q", got, cause)
					}
				}()
				_, _ = f.claim.AcquireReplicatedDataRead(ctx, &f.cut, replicatedReadReuseScoreSQL, params, false, query.ExecOptions{})
			}()
			cache := f.claim.readReuse
			cache.mu.Lock()
			defer cache.mu.Unlock()
			for i := range cache.slots {
				if slot := &cache.slots[i]; slot.active || slot.reader != nil {
					t.Fatalf("panic retained slot %d", i)
				}
			}
			if cache.retained != 0 {
				t.Fatalf("panic retained %d bytes", cache.retained)
			}
		})
	}
}

func TestReplicatedReadReuseResetDeclinePreservesSuccessfulRead(t *testing.T) {
	db, session := openRuntimeSession(t)
	defer db.Close()
	defer session.Close()
	create := runtimePrepare(t, session, `CREATE TABLE docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	_ = create.Close()
	reader := &ReplicatedReadSession{conn: conn{db: session.conn.db}}
	reader.session = Session{conn: &reader.conn, state: SessionIdle}
	defer reader.Close()
	// Driver bounds admit this range, while the deliberately stricter query
	// reset helper declines descending order. It must remain valid SQL.
	prepared, err := prepareReplicatedRead(context.Background(), reader,
		`SELECT id FROM docs WHERE id >= ? ORDER BY id DESC LIMIT 64`, []ParamType{ParamTypeText})
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.statement.replicatedReadReuseEligible() {
		t.Fatal("test missed the post-execution decline")
	}
	var cursor Cursor
	if err := prepared.QueryInto(context.Background(), []any{"a"}, &cursor); err != nil {
		t.Fatal(err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	cache := new(replicatedReadReuseCache)
	slot, _, err := cache.reserve(replicatedReadReuseKey{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.install(slot, replicatedReadReuseKey{}, reader, prepared, true); err != nil {
		t.Fatal(err)
	}
	lease := cache.leaseFor(slot)
	if err := lease.Finish(nil); err != nil {
		t.Fatalf("successful SQL became a retention error: %v", err)
	}
	if slot.active || slot.reader != nil || cache.retained != 0 {
		t.Fatal("declined preparation remained cached")
	}
}

func TestReplicatedReadReuseAbortClosesUnpublishedRows(t *testing.T) {
	db, session := openRuntimeSession(t)
	defer db.Close()
	defer session.Close()
	create := runtimePrepare(t, session, `CREATE TABLE docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	_ = create.Close()
	reader := &ReplicatedReadSession{conn: conn{db: session.conn.db}}
	reader.session = Session{conn: &reader.conn, state: SessionIdle}
	defer reader.Close()
	prepared, err := prepareReplicatedRead(context.Background(), reader,
		`SELECT id FROM docs WHERE id = ?`, []ParamType{ParamTypeText})
	if err != nil {
		t.Fatal(err)
	}
	var cursor Cursor
	if err := prepared.QueryInto(context.Background(), []any{"a"}, &cursor); err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	// Reproduce the panic window between opening internal rows and publishing
	// their public Cursor. Session.Close alone cannot discover these rows.
	reader.session.current = nil
	if !reader.conn.open {
		t.Fatal("test did not open internal rows")
	}
	cache := new(replicatedReadReuseCache)
	slot, _, err := cache.reserve(replicatedReadReuseKey{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.install(slot, replicatedReadReuseKey{}, reader, prepared, true); err != nil {
		t.Fatal(err)
	}
	lease := cache.leaseFor(slot)
	_ = lease.Abort(nil)
	if !reader.conn.closed || reader.conn.db != nil || reader.conn.open || reader.conn.tx != nil {
		t.Fatal("aborted unpublished rows prevented connection cleanup")
	}
	if slot.active || slot.reader != nil {
		t.Fatal("aborted slot remained live")
	}
}

type readReusePanicExecutionContext struct {
	context.Context
	connection *conn
	done       chan struct{}
}

func (c *readReusePanicExecutionContext) Done() <-chan struct{} { return c.done }
func (c *readReusePanicExecutionContext) Err() error {
	// beginContextCancellation installs its local flag only after all setup
	// checkpoints. The next checkpoint is inside the running SQL operation.
	if c.connection.exec.Options.Cancel != nil {
		panic("injected execution panic")
	}
	return nil
}

func TestReplicatedReadReuseQueryPanicStopsContextWatcher(t *testing.T) {
	db, session := openRuntimeSession(t)
	defer db.Close()
	defer session.Close()
	create := runtimePrepare(t, session, `CREATE TABLE docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	_ = create.Close()
	prepared := runtimePrepare(t, session, `SELECT id FROM docs WHERE id = ?`)
	defer prepared.Close()
	ctx := &readReusePanicExecutionContext{Context: context.Background(), connection: session.conn, done: make(chan struct{})}
	defer close(ctx.done)
	func() {
		defer func() {
			if got := recover(); got != "injected execution panic" {
				t.Fatalf("panic=%v", got)
			}
		}()
		var cursor Cursor
		_ = prepared.QueryInto(ctx, []any{"a"}, &cursor)
	}()
	// Restoration happens only after the watcher has been stopped and joined.
	if session.conn.exec.Options.Cancel != nil {
		t.Fatal("panicking SQL retained its cancellation watcher and local flag")
	}
}
