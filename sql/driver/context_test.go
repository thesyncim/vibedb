package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/conformance"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

type observedDoneContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

type observedErrContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

type cancelOnNthErrContext struct {
	context.Context
	cancel context.CancelFunc
	at     int32
	calls  atomic.Int32
}

type deadlineSignalContext struct {
	context.Context
	done chan struct{}
}

func (c *deadlineSignalContext) Done() <-chan struct{} { return c.done }

func (c *deadlineSignalContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *cancelOnNthErrContext) Err() error {
	if c.calls.Add(1) == c.at {
		c.cancel()
	}
	return c.Context.Err()
}

func cancelMutationBeforePublication(t *testing.T, statement *stdsql.Stmt, name string) {
	t.Helper()
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelOnNthErrContext{
		Context: base,
		cancel:  cancel,
		// Cancellation-scope setup and connection admission consume the first
		// observations. The fourth is inside mutation execution, before its
		// publication boundary, and is architecture/scheduler independent.
		at: 4,
	}
	_, err := statement.ExecContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("active %s = %v, want context.Canceled", name, err)
	}
	if got := ctx.calls.Load(); got < ctx.at {
		t.Fatalf("active %s used only %d checkpoints, cancellation target was %d",
			name, got, ctx.at)
	}
}

func (c *observedErrContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.observed) })
	return err
}

type cancelOnDoneContext struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

func (c *cancelOnDoneContext) Done() <-chan struct{} {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.Context.Done()
}

func TestContextLockBackgroundPathDoesNotAllocate(t *testing.T) {
	var mu sync.RWMutex
	var connectorMu sync.Mutex
	allocs := testing.AllocsPerRun(1_000, func() {
		if err := mutexLockContext(context.Background(), &connectorMu); err != nil {
			panic(err)
		}
		connectorMu.Unlock()
		if err := lockContext(context.Background(), &mu); err != nil {
			panic(err)
		}
		mu.Unlock()
		if err := rlockContext(context.Background(), &mu); err != nil {
			panic(err)
		}
		mu.RUnlock()
	})
	if allocs != 0 {
		t.Fatalf("background catalog lock path allocated %.2f times, want zero", allocs)
	}
}

func TestContendedContextLockReturnsOnCancellation(t *testing.T) {
	var mu sync.RWMutex
	mu.Lock()
	defer mu.Unlock()

	base, cancel := context.WithCancel(context.Background())
	ctx := &observedDoneContext{
		Context: base, observed: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		done <- lockContext(ctx, &mu)
	}()
	<-ctx.observed
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lockContext = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("contended lock did not return after context cancellation")
	}
}

func TestConnectorConnectReturnsOnCancellationWhileOwnershipLockIsHeld(t *testing.T) {
	raw, err := (Driver{}).OpenConnector(t.TempDir() + "/catalog.vdb")
	if err != nil {
		t.Fatal(err)
	}
	connector := raw.(*dbConnector)
	t.Cleanup(func() {
		if err := connector.Close(); err != nil {
			t.Error(err)
		}
	})

	connector.mu.Lock()
	locked := true
	defer func() {
		if locked {
			connector.mu.Unlock()
		}
	}()

	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &observedErrContext{
		Context: base, observed: make(chan struct{}),
	}
	type connectResult struct {
		conn sqldriver.Conn
		err  error
	}
	done := make(chan connectResult, 1)
	go func() {
		connection, connectErr := connector.Connect(ctx)
		done <- connectResult{conn: connection, err: connectErr}
	}()
	// The initial Err check has completed with a live context. Cancellation
	// therefore happens after Connect entered, while the ownership mutex is
	// still unavailable.
	<-ctx.observed
	cancel()

	select {
	case result := <-done:
		if result.conn != nil {
			_ = result.conn.Close()
			t.Fatal("canceled Connect returned a connection")
		}
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("Connect = %v, want context.Canceled", result.err)
		}
	case <-time.After(time.Second):
		// Release the lock before failing so a broken implementation cannot
		// strand its goroutine or the connector's writer lease in the test
		// process.
		connector.mu.Unlock()
		locked = false
		result := <-done
		if result.conn != nil {
			_ = result.conn.Close()
		}
		t.Fatal("contended Connect did not return after context cancellation")
	}

	connector.mu.Unlock()
	locked = false
}

func TestExecContextCancellationWhileCatalogLockedPublishesNothing(t *testing.T) {
	connection := directTestConn(t)
	c := connection.(*conn)

	c.db.mu.Lock()
	locked := true
	defer func() {
		if locked {
			c.db.mu.Unlock()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := connection.(sqldriver.ExecerContext).ExecContext(
			ctx,
			`CREATE TABLE canceled (id STRING PRIMARY KEY)`,
			nil,
		)
		done <- err
	}()

	// A result before cancellation means the supposedly blocked acquisition
	// escaped the exclusive catalog lock.
	select {
	case err := <-done:
		t.Fatalf("ExecContext returned before cancellation: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked ExecContext = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked ExecContext did not return promptly after cancellation")
	}

	c.db.mu.Unlock()
	locked = false
	c.db.mu.RLock()
	_, exists := c.db.tables["canceled"]
	c.db.mu.RUnlock()
	if exists {
		t.Fatal("canceled CREATE TABLE was published after prepublication cancellation")
	}
}

func TestBeginTxCancellationReleasesPartiallyCapturedSnapshots(t *testing.T) {
	connection := directTestConn(t)
	for _, table := range []string{"left_docs", "right_docs"} {
		directExec(t, connection,
			`CREATE TABLE `+table+` (id STRING PRIMARY KEY)`, nil)
		directExec(t, connection,
			`INSERT INTO `+table+` VALUES (?)`,
			[]sqldriver.NamedValue{{
				Ordinal: 1, Value: `{"id":"kept"}`,
			}})
	}

	base, cancel := context.WithCancel(context.Background())
	ctx := &cancelOnDoneContext{
		Context: base,
		cancel:  cancel,
		// lockContext observes Done once. The first table checkpoint is the
		// second observation; cancel at the second table checkpoint, after one
		// durable snapshot lease has been captured.
		remaining: 3,
	}
	transaction, err := connection.(sqldriver.ConnBeginTx).BeginTx(
		ctx, sqldriver.TxOptions{})
	if transaction != nil {
		_ = transaction.Rollback()
		t.Fatal("canceled BeginTx returned a transaction")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BeginTx = %v, want context.Canceled", err)
	}
	if connection.(*conn).tx != nil {
		t.Fatal("canceled BeginTx was installed on the connection")
	}

	// Collection.Close reports outstanding snapshot leases. A clean connection
	// close therefore proves the partially built transaction released the
	// snapshot it captured before observing cancellation.
	if err := connection.Close(); err != nil {
		t.Fatalf("close after canceled BeginTx: %v", err)
	}
}

func TestExecutionContextInterfacesExposeCooperativeCancellation(t *testing.T) {
	connection := directTestConn(t)
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`, nil)
	prepared, err := connection.Prepare(`SELECT id FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	if _, ok := connection.(sqldriver.QueryerContext); ok {
		t.Fatal("connection QueryerContext would need transient-statement row ownership")
	}
	if _, ok := prepared.(sqldriver.StmtQueryContext); !ok {
		t.Fatal("statement does not advertise cancellable QueryContext")
	}
	if _, ok := prepared.(sqldriver.StmtExecContext); !ok {
		t.Fatal("statement does not advertise cancellable ExecContext")
	}
	if _, ok := connection.(sqldriver.ExecerContext); !ok {
		t.Fatal("connection lost cancellable ExecContext")
	}
}

func TestContextCancellationScopeCancelsAndDoesNotPoisonConnection(t *testing.T) {
	connection := directTestConn(t)
	c := connection.(*conn)
	previous := new(query.CancelFlag)
	c.exec.Options.Cancel = previous

	ctx, cancel := context.WithCancel(context.Background())
	scope, err := c.beginContextCancellation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if scope == nil || c.exec.Options.Cancel != previous {
		t.Fatal("cancellable context did not reuse the installed cancellation flag")
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for !scope.flag.Canceled() {
		if time.Now().After(deadline) {
			t.Fatal("context watcher did not cancel the executor flag")
		}
		time.Sleep(time.Microsecond)
	}
	if err := scope.finish(query.ErrCanceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("mapped cancellation = %v, want context.Canceled", err)
	}
	if c.exec.Options.Cancel != previous || previous.Canceled() {
		t.Fatal("finished context operation did not restore the prior clean flag")
	}
	deadlineCtx := &deadlineSignalContext{
		Context: context.Background(), done: make(chan struct{}),
	}
	deadlineScope, err := c.beginContextCancellation(deadlineCtx)
	if err != nil {
		t.Fatal(err)
	}
	close(deadlineCtx.done)
	if err := deadlineScope.finish(query.ErrCanceled); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mapped deadline cancellation = %v, want DeadlineExceeded", err)
	}
	if c.exec.Options.Cancel != previous || previous.Canceled() {
		t.Fatal("finished deadline operation did not restore the prior clean flag")
	}
	if scope, err := c.beginContextCancellation(context.Background()); err != nil || scope != nil {
		t.Fatalf("background context scope = (%v, %v), want nil, nil", scope, err)
	}
	if c.exec.Options.Cancel != previous {
		t.Fatal("background context changed the executor flag")
	}
	allocations := testing.AllocsPerRun(1_000, func() {
		scope, err := c.beginContextCancellation(context.Background())
		if err != nil || scope != nil {
			panic("background cancellation scope changed during allocation gate")
		}
	})
	if allocations != 0 {
		t.Fatalf("background cancellation scope allocated %.2f times, want zero",
			allocations)
	}
}

func TestCooperativeCancelFlagInterruptsContendedCatalogLock(t *testing.T) {
	var mu sync.RWMutex
	mu.Lock()

	var flag query.CancelFlag
	ctx := withCooperativeCancellation(context.Background(), &flag)
	done := make(chan error, 1)
	go func() {
		done <- lockContext(ctx, &mu)
	}()

	// Let lockContext enter its bounded polling path before cancellation. This
	// models pgwire, whose protocol CancelRequest arms the shared atomic flag
	// while Prepared.Exec is running with context.Background.
	time.Sleep(5 * contextLockPoll)
	flag.Cancel()
	select {
	case err := <-done:
		if !errors.Is(err, query.ErrCanceled) {
			t.Fatalf("lockContext = %v, want %v", err, query.ErrCanceled)
		}
	case <-time.After(time.Second):
		mu.Unlock()
		t.Fatal("cooperative cancellation did not interrupt lock acquisition")
	}
	mu.Unlock()
}

func TestRuntimeCancelFlagInterruptsPreparedDDLWaitingForCatalogLock(t *testing.T) {
	database, session := openRuntimeSession(t)
	prepared, err := session.Prepare(context.Background(),
		`CREATE TABLE canceled (id STRING PRIMARY KEY)`)
	if err != nil {
		_ = session.Close()
		_ = database.Close()
		t.Fatal(err)
	}
	safeToClose := true
	defer func() {
		if safeToClose {
			_ = prepared.Close()
			_ = session.Close()
			_ = database.Close()
		}
	}()
	var flag query.CancelFlag
	if err := session.SetCancelFlag(&flag); err != nil {
		t.Fatal(err)
	}

	session.conn.db.mu.Lock()
	locked := true
	defer func() {
		if locked {
			session.conn.db.mu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, err := prepared.Exec(context.Background(), nil)
		done <- err
	}()
	time.Sleep(5 * contextLockPoll)
	flag.Cancel()
	select {
	case err := <-done:
		if !errors.Is(err, query.ErrCanceled) {
			t.Fatalf("Prepared.Exec = %v, want %v", err, query.ErrCanceled)
		}
	case <-time.After(time.Second):
		// Release the artificial blocker and join Exec before deferred session
		// cleanup. A failing test must not race Session.Close against the same
		// single-consumer runtime object and obscure the cancellation failure.
		session.conn.db.mu.Unlock()
		locked = false
		select {
		case err := <-done:
			t.Fatalf(
				"runtime cancellation waited for catalog unlock, then returned %v", err,
			)
		case <-time.After(time.Second):
			safeToClose = false
			t.Fatal("runtime cancellation remained stuck after catalog unlock")
		}
	}
	session.conn.db.mu.Unlock()
	locked = false
	flag.Reset()

	session.conn.db.mu.RLock()
	_, exists := session.conn.db.tables["canceled"]
	session.conn.db.mu.RUnlock()
	if exists {
		t.Fatal("canceled runtime DDL was published")
	}
}

func TestPrepareObservesInstalledCancellationFlagAndConnectionIsReusable(t *testing.T) {
	connection := directTestConn(t)
	c := connection.(*conn)
	directExec(t, connection, `CREATE TABLE docs (id STRING PRIMARY KEY)`, nil)
	var cancel query.CancelFlag
	c.exec.Options.Cancel = &cancel
	cancel.Cancel()
	large := "SELECT * FROM docs /*" + strings.Repeat("x", 1<<20) + "*/"
	prepared, err := c.PrepareContext(context.Background(), large)
	if prepared != nil {
		_ = prepared.Close()
	}
	if !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("PrepareContext with installed cancellation = %v, want %v",
			err, query.ErrCanceled)
	}

	cancel.Reset()
	prepared, err = c.PrepareContext(context.Background(), "SELECT * FROM docs")
	if err != nil {
		t.Fatalf("connection after canceled parse: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLCancellationContractIsAtomicAndReusable(t *testing.T) {
	contract, ok := conformance.CancellationFor(conformance.DatabaseSQL)
	if !ok || !contract.NoPartialWrite || !contract.Reusable {
		t.Fatalf("database/sql cancellation contract = %+v, %v", contract, ok)
	}
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"kept"}`); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if rows, err := db.QueryContext(ctx, `SELECT id FROM docs`); !errors.Is(err, context.Canceled) {
		if rows != nil {
			_ = rows.Close()
		}
		t.Fatalf("canceled QueryContext = %v, want context.Canceled", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM docs WHERE id = ?`, "kept"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ExecContext = %v, want context.Canceled", err)
	}

	deadlineCtx, deadlineCancel := context.WithDeadline(
		context.Background(), time.Now().Add(-time.Second),
	)
	defer deadlineCancel()
	if _, err := db.QueryContext(deadlineCtx, `SELECT id FROM docs`); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired QueryContext = %v, want DeadlineExceeded", err)
	}

	var id string
	if err := db.QueryRow(`SELECT id FROM docs WHERE id = ?`, "kept").Scan(&id); err != nil {
		t.Fatalf("connection after cancellation: %v", err)
	}
	if id != "kept" {
		t.Fatalf("canceled delete changed row to %q", id)
	}
}

func TestDatabaseSQLCancelsActiveScanAndMutationBeforePublication(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(1)
	for _, table := range []string{"left_docs", "right_docs"} {
		if _, err := db.Exec(`CREATE TABLE ` + table +
			` (id STRING PRIMARY KEY, grp STRING NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		const batch = 64
		insertSQL := `INSERT INTO ` + table + ` VALUES ` +
			strings.TrimSuffix(strings.Repeat("(?),", batch), ",")
		insert, err := db.Prepare(insertSQL)
		if err != nil {
			t.Fatal(err)
		}
		rows := 1024
		if table == "left_docs" {
			rows = 8192
		}
		for first := 0; first < rows; first += batch {
			args := make([]any, batch)
			for index := range batch {
				group := "all"
				if table == "left_docs" && first+index < 32 {
					group = "victim"
				}
				args[index] = fmt.Sprintf(
					`{"id":"%s-%04d","grp":"%s"}`,
					table, first+index, group,
				)
			}
			if _, err := insert.Exec(args...); err != nil {
				_ = insert.Close()
				t.Fatal(err)
			}
		}
		if err := insert.Close(); err != nil {
			t.Fatal(err)
		}
	}

	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()
	// BatchRows is an existing execution control, not a test hook. Setting it
	// to one makes the scan cross many real durable batch/cancellation
	// checkpoints, giving the test a deterministic active-command window.
	if err := sqlConn.Raw(func(raw any) error {
		raw.(*conn).exec.Options.BatchRows = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		rows, err := sqlConn.QueryContext(ctx, `
			SELECT COUNT(*)
			FROM left_docs AS l
			JOIN right_docs AS r ON l.grp = r.grp`)
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			_ = rows.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		cancel()
		t.Fatalf("join completed before its cancellation window: %v", err)
	case <-time.After(2 * time.Millisecond):
		cancel()
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active QueryContext = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active QueryContext did not stop at a bounded checkpoint")
	}

	deleteStmt, err := sqlConn.PrepareContext(context.Background(),
		`DELETE FROM left_docs WHERE grp = 'victim'`)
	if err != nil {
		t.Fatal(err)
	}
	defer deleteStmt.Close()
	cancelMutationBeforePublication(t, deleteStmt, "autocommit DELETE")

	// Repeat the mutation-side cancellation inside an explicit transaction. A
	// canceled statement must leave no partial overlay behind, while the
	// transaction itself remains usable for a later statement and commit.
	transaction, err := sqlConn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer transaction.Rollback()
	txDelete, err := transaction.PrepareContext(context.Background(),
		`DELETE FROM left_docs WHERE grp = 'victim'`)
	if err != nil {
		t.Fatal(err)
	}
	defer txDelete.Close()
	cancelMutationBeforePublication(t, txDelete, "transaction DELETE")
	if err := txDelete.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.ExecContext(context.Background(),
		`INSERT INTO left_docs VALUES (?)`,
		`{"id":"after-cancel","grp":"committed"}`,
	); err != nil {
		t.Fatalf("transaction after canceled statement: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit after canceled statement: %v", err)
	}

	var total, victims int
	if err := sqlConn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM left_docs`).Scan(&total); err != nil {
		t.Fatalf("query after active cancellation: %v", err)
	}
	if err := sqlConn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM left_docs WHERE grp = 'victim'`).Scan(&victims); err != nil {
		t.Fatalf("victim query after active cancellation: %v", err)
	}
	if total != 8193 || victims != 32 {
		t.Fatalf("rows after canceled deletes = total %d victims %d, want 8193/32",
			total, victims)
	}
}

func TestNamedValueRejectsParametersLargerThanAnySQLDocument(t *testing.T) {
	c := &conn{}
	tooLarge := strings.Repeat("1", maxSQLParameterBytes+1)
	tests := []struct {
		name  string
		value any
	}{
		{name: "string", value: tooLarge},
		{name: "bytes", value: []byte(tooLarge)},
		{name: "json number", value: json.Number(tooLarge)},
		{name: "query number", value: query.Number(tooLarge)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := sqldriver.NamedValue{Ordinal: 1, Value: test.value}
			err := c.CheckNamedValue(&value)
			if !errors.Is(err, durable.ErrDocumentTooLarge) {
				t.Fatalf("CheckNamedValue(%T) = %v, want ErrDocumentTooLarge",
					test.value, err)
			}
		})
	}

	atLimit := sqldriver.NamedValue{
		Ordinal: 1,
		Value:   strings.Repeat("x", maxSQLParameterBytes),
	}
	if err := c.CheckNamedValue(&atLimit); err != nil {
		t.Fatalf("maximum-size parameter: %v", err)
	}
}
