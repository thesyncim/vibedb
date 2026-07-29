package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestExecutionContextInterfacesDoNotOverclaimCancellation(t *testing.T) {
	connection := directTestConn(t)
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`, nil)
	prepared, err := connection.Prepare(`SELECT id FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	if _, ok := connection.(sqldriver.QueryerContext); ok {
		t.Fatal("connection advertises QueryerContext without an interruptible query executor")
	}
	if _, ok := prepared.(sqldriver.StmtQueryContext); ok {
		t.Fatal("statement advertises StmtQueryContext without an interruptible query executor")
	}
	if _, ok := prepared.(sqldriver.StmtExecContext); ok {
		t.Fatal("statement advertises StmtExecContext without an interruptible transaction executor")
	}
	if _, ok := connection.(sqldriver.ExecerContext); !ok {
		t.Fatal("connection lost its bounded, prepublication-cancellable ExecContext")
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
