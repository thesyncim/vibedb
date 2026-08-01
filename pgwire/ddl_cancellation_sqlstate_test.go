package pgwire

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// ddlReplacementGateContext pauses a typed-runtime DROP INDEX only after it
// has entered replacement copying. That operation holds the catalog mutex, so
// a pgwire DDL issued while the gate is closed is deterministically parked in
// the driver's cancellable catalog-lock path rather than racing a fast DDL on
// the test machine.
type ddlReplacementGateContext struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newDDLReplacementGateContext() *ddlReplacementGateContext {
	return &ddlReplacementGateContext{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (*ddlReplacementGateContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (*ddlReplacementGateContext) Done() <-chan struct{} { return nil }

func (c *ddlReplacementGateContext) Err() error {
	if ddlCurrentStackContains("sql/driver.(*database).buildReplacementStorageLocked") {
		c.enterOnce.Do(func() {
			close(c.entered)
			<-c.release
		})
	}
	return nil
}

func (*ddlReplacementGateContext) Value(any) any { return nil }

func (c *ddlReplacementGateContext) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

type ddlExecutionGateContext struct {
	frame       string
	match       uint32
	matches     atomic.Uint32
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newDDLExecutionGateContext(
	frame string,
	match uint32,
) *ddlExecutionGateContext {
	return &ddlExecutionGateContext{
		frame: frame, match: match,
		entered: make(chan struct{}), release: make(chan struct{}),
	}
}

func (*ddlExecutionGateContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (*ddlExecutionGateContext) Done() <-chan struct{} { return nil }

func (c *ddlExecutionGateContext) Err() error {
	if ddlCurrentStackContains(c.frame) && c.matches.Add(1) == c.match {
		c.enterOnce.Do(func() {
			close(c.entered)
			<-c.release
		})
	}
	return nil
}

func (*ddlExecutionGateContext) Value(any) any { return nil }

func (c *ddlExecutionGateContext) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func ddlCurrentStackContains(want string) bool {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, want) {
			return true
		}
		if !more {
			return false
		}
	}
}

func TestDDLInstalledCancelFlagStopsPrePublicationBuilds(t *testing.T) {
	tests := []struct {
		name                  string
		statement             string
		setup                 []string
		frame                 string
		match                 uint32
		blockCatalogAfterGate bool
		verify                func(*testing.T, *testClient)
	}{
		{
			name:                  "create index scan",
			statement:             `CREATE INDEX canceled_idx ON docs(extra)`,
			frame:                 "store/durable.(*Collection).CreateIndexContext",
			match:                 2,
			blockCatalogAfterGate: true,
			verify: func(t *testing.T, c *testClient) {
				t.Helper()
				if got := commandTagOf(t, c.query(
					`CREATE INDEX canceled_idx ON docs(extra)`,
				)); got != "CREATE INDEX" {
					t.Fatalf("CREATE INDEX after cancellation tag = %q", got)
				}
			},
		},
		{
			name:      "drop index copy",
			statement: `DROP INDEX target_idx ON docs`,
			setup:     []string{`CREATE INDEX target_idx ON docs(extra)`},
			frame: "sql/driver.(*database)." +
				"buildReplacementStorageLocked",
			match: 1,
			verify: func(t *testing.T, c *testClient) {
				t.Helper()
				if got := commandTagOf(t, c.query(
					`DROP INDEX target_idx ON docs`,
				)); got != "DROP INDEX" {
					t.Fatalf("DROP INDEX after cancellation tag = %q", got)
				}
			},
		},
		{
			name:      "truncate candidate",
			statement: `TRUNCATE TABLE docs`,
			frame: "sql/driver.(*database)." +
				"replaceTableStorageLockedContext",
			match: 2,
			verify: func(t *testing.T, c *testClient) {
				t.Helper()
				rows := rowsOf(t, c.query(`SELECT COUNT(*) FROM docs`))
				if len(rows) != 1 || len(rows[0]) != 1 ||
					string(rows[0][0]) != "1" {
					t.Fatalf("rows after canceled TRUNCATE = %q, want [[1]]", rows)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, server := connectSQLCatalogWithServer(t)
			ddlSetUpCancellationTable(t, c, test.setup)

			runtimeSession, err := server.db.NewSession(context.Background())
			if err != nil {
				t.Fatalf("open cancellation runtime session: %v", err)
			}
			var cancel query.CancelFlag
			if err := runtimeSession.SetCancelFlag(&cancel); err != nil {
				_ = runtimeSession.Close()
				t.Fatalf("install cancellation flag: %v", err)
			}
			prepared, err := runtimeSession.Prepare(
				context.Background(), test.statement,
			)
			if err != nil {
				_ = runtimeSession.Close()
				t.Fatalf("prepare %q: %v", test.statement, err)
			}

			gate := newDDLExecutionGateContext(test.frame, test.match)
			done := make(chan error, 1)
			go func() {
				_, execErr := prepared.Exec(gate, nil)
				done <- execErr
			}()
			finished := false
			var cleanupOnce sync.Once
			cleanup := func() {
				cleanupOnce.Do(func() {
					gate.unblock()
					if !finished {
						select {
						case <-done:
							finished = true
						case <-time.After(5 * time.Second):
							t.Error("canceled DDL execution did not finish during cleanup")
						}
					}
					if closeErr := prepared.Close(); closeErr != nil {
						t.Errorf("close canceled DDL: %v", closeErr)
					}
					if closeErr := runtimeSession.Close(); closeErr != nil {
						t.Errorf("close cancellation runtime session: %v", closeErr)
					}
				})
			}
			t.Cleanup(cleanup)
			select {
			case <-gate.entered:
			case execErr := <-done:
				finished = true
				gate.unblock()
				t.Fatalf("%s completed before its pre-publication gate: %v",
					test.statement, execErr)
			case <-time.After(5 * time.Second):
				gate.unblock()
				t.Fatalf("%s did not reach its pre-publication gate", test.statement)
			}

			finishBlocker := func() {}
			if test.blockCatalogAfterGate {
				// Hold the catalog mutex after CreateIndexContext has started but
				// before it returns cancellation. A canceled online build must not
				// enter an unconditional identity-check RLock and be rewritten as a
				// later serialization conflict when this replacement publishes.
				blockerGate := newDDLReplacementGateContext()
				finishBlocker = ddlStartReplacementBlocker(
					t, server.db, blockerGate,
				)
			}
			cancel.Cancel()
			gate.unblock()
			select {
			case execErr := <-done:
				finished = true
				if execErr == nil {
					t.Fatalf("%s succeeded after cancellation", test.statement)
				}
				if got := asPGError(execErr).code; got != sqlstateQueryCanceled {
					t.Fatalf("%s cancellation SQLSTATE = %s, want %s: %v",
						test.statement, got, sqlstateQueryCanceled, execErr)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s did not stop after CancelFlag", test.statement)
			}
			finishBlocker()
			cancel.Reset()
			cleanup()
			test.verify(t, c)
		})
	}
}

func TestDDLCancelRequestStopsPrePublicationCatalogWork(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		setup     []string
		verify    func(*testing.T, *testClient)
	}{
		{
			name:      "create index",
			statement: `CREATE INDEX canceled_idx ON docs(extra)`,
			verify: func(t *testing.T, c *testClient) {
				t.Helper()
				if got := commandTagOf(t, c.query(
					`CREATE INDEX canceled_idx ON docs(extra)`,
				)); got != "CREATE INDEX" {
					t.Fatalf("CREATE INDEX after cancellation tag = %q", got)
				}
			},
		},
		{
			name:      "drop index",
			statement: `DROP INDEX target_idx ON docs`,
			setup:     []string{`CREATE INDEX target_idx ON docs(extra)`},
			verify: func(t *testing.T, c *testClient) {
				t.Helper()
				if got := commandTagOf(t, c.query(
					`DROP INDEX target_idx ON docs`,
				)); got != "DROP INDEX" {
					t.Fatalf("DROP INDEX after cancellation tag = %q", got)
				}
			},
		},
		{
			name:      "truncate",
			statement: `TRUNCATE TABLE docs`,
			verify: func(t *testing.T, c *testClient) {
				t.Helper()
				rows := rowsOf(t, c.query(`SELECT COUNT(*) FROM docs`))
				if len(rows) != 1 || len(rows[0]) != 1 ||
					string(rows[0][0]) != "1" {
					t.Fatalf("rows after canceled TRUNCATE = %q, want [[1]]", rows)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, server := connectSQLCatalogWithServer(t)
			ddlSetUpCancellationTable(t, c, test.setup)

			const statementName = "cancel-ddl"
			const portalName = "cancel-ddl-portal"
			ddlPreparePortal(t, c, statementName, portalName, test.statement)
			server.mu.Lock()
			wireSession := server.sessions[c.pid]
			server.mu.Unlock()
			if wireSession == nil {
				t.Fatal("pgwire DDL session disappeared before execution")
			}

			gate := newDDLReplacementGateContext()
			finishBlocker := ddlStartReplacementBlocker(t, server.db, gate)

			c.send(msgExecute, executeMsg(portalName, 0))
			c.drainWrites()
			ddlWaitForCatalogLock(t)

			wireSession.cancelMu.Lock()
			cancelActive := wireSession.cancelActive
			wireSession.cancelMu.Unlock()
			if !cancelActive {
				t.Fatal("pgwire DDL did not retain an active cancellation window")
			}
			ddlSendWireCancelRequest(t, server, c.pid, c.secret)
			msgs := c.until(msgErrorResponse)
			expectError(t, msgs, sqlstateQueryCanceled)
			if has(msgs, msgCommandComplete) {
				t.Fatalf("canceled DDL emitted CommandComplete: %s", tags(msgs))
			}
			// net.Pipe has no socket buffer: pipelining Sync behind the
			// deliberately blocked Execute would stall the test client's writer
			// until its deadline. Sending Sync after ErrorResponse is a valid
			// extended-protocol recovery sequence and exercises the same server
			// resynchronization path without relying on transport buffering.
			c.send(msgSync, nil)
			ready := c.until(msgReadyForQuery)
			assertReadyStatus(t, ready, statusIdle)

			finishBlocker()
			test.verify(t, c)
			if msgs := c.query(`SELECT 1`); has(msgs, msgErrorResponse) {
				t.Fatalf("cancellation poisoned the next statement: %s", tags(msgs))
			}
		})
	}
}

func ddlSendWireCancelRequest(
	t *testing.T,
	server *Server,
	pid int32,
	secret int32,
) {
	t.Helper()
	client, backend := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		server.ServeConn(backend)
		close(done)
	}()
	var packet [16]byte
	binary.BigEndian.PutUint32(packet[0:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[4:8], codeCancelRequest)
	binary.BigEndian.PutUint32(packet[8:12], uint32(pid))
	binary.BigEndian.PutUint32(packet[12:16], uint32(secret))
	if _, err := client.Write(packet[:]); err != nil {
		t.Fatalf("write CancelRequest packet: %v", err)
	}
	if err := client.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set CancelRequest read deadline: %v", err)
	}
	var response [1]byte
	n, readErr := client.Read(response[:])
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("CancelRequest connection did not finish")
	}
	if n != 0 || readErr != io.EOF {
		t.Fatalf(
			"CancelRequest response = (%d bytes, %v), want zero bytes then EOF",
			n, readErr,
		)
	}
}

func ddlSetUpCancellationTable(
	t *testing.T,
	c *testClient,
	extra []string,
) {
	t.Helper()
	statements := []string{
		`CREATE TABLE docs (` +
			`id STRING PRIMARY KEY, kind STRING, extra STRING)`,
		`INSERT INTO docs VALUES (` +
			`'{"id":"one","kind":"hold","extra":"target"}')`,
		`CREATE INDEX hold_idx ON docs(kind)`,
	}
	statements = append(statements, extra...)
	for _, statement := range statements {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("setup %q: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
}

func ddlPreparePortal(
	t *testing.T,
	c *testClient,
	statementName string,
	portalName string,
	statement string,
) {
	t.Helper()
	c.send(msgParse, parseMsg(statementName, statement))
	c.send(msgBind, bindMsg(portalName, statementName, nil, nil, nil))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("prepare DDL %q: %s", statement,
			formatError(find(t, msgs, msgErrorResponse).body))
	}
}

func ddlStartReplacementBlocker(
	t *testing.T,
	database *sqldriver.Database,
	gate *ddlReplacementGateContext,
) func() {
	t.Helper()
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatalf("open DDL blocker session: %v", err)
	}
	prepared, err := session.Prepare(
		context.Background(), `DROP INDEX hold_idx ON docs`,
	)
	if err != nil {
		_ = session.Close()
		t.Fatalf("prepare DDL blocker: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, execErr := prepared.Exec(gate, nil)
		done <- execErr
	}()

	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			gate.unblock()
			select {
			case execErr := <-done:
				if execErr != nil {
					t.Errorf("DDL blocker: %v", execErr)
				}
			case <-time.After(10 * time.Second):
				t.Error("DDL blocker did not finish after release")
			}
			if closeErr := prepared.Close(); closeErr != nil {
				t.Errorf("close DDL blocker statement: %v", closeErr)
			}
			if closeErr := session.Close(); closeErr != nil {
				t.Errorf("close DDL blocker session: %v", closeErr)
			}
		})
	}
	t.Cleanup(finish)

	select {
	case <-gate.entered:
		return finish
	case execErr := <-done:
		t.Fatalf("DDL blocker finished before replacement-copy gate: %v", execErr)
	case <-time.After(5 * time.Second):
		t.Fatal("DDL blocker did not reach replacement copying")
	}
	return finish
}

func ddlWaitForCatalogLock(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		stacks := ddlAllGoroutineStacks()
		for _, stack := range strings.Split(stacks, "\n\n") {
			if strings.Contains(stack, "sql/driver.lockContext") &&
				strings.Contains(stack, "pgwire.(*session).executeRuntimeExec") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("pgwire DDL did not block on the catalog lock; relevant stacks:\n%s",
				ddlStacksContaining(stacks, "executeRuntimeExec"))
		}
		runtime.Gosched()
	}
}

func ddlAllGoroutineStacks() string {
	size := 256 << 10
	for {
		buffer := make([]byte, size)
		n := runtime.Stack(buffer, true)
		if n < len(buffer) {
			return string(buffer[:n])
		}
		size *= 2
	}
}

func ddlStacksContaining(stacks string, want string) string {
	var matching strings.Builder
	for _, stack := range strings.Split(stacks, "\n\n") {
		if strings.Contains(stack, want) {
			matching.WriteString(stack)
			matching.WriteString("\n\n")
		}
	}
	return matching.String()
}

func TestDDLUnsupportedVariantsPreserveSQLSTATETaxonomy(t *testing.T) {
	c := connectSQLCatalog(t)

	for _, statement := range []string{
		`DROP VIEW docs`,
		`DROP MATERIALIZED VIEW docs`,
		`DROP FUNCTION public.f(integer, text)`,
		`DROP TRIGGER trg ON docs`,
		`DROP DATABASE old_db WITH (FORCE)`,
		`DROP TABLE docs CASCADE`,
		`DROP INDEX by_kind RESTRICT`,
		`DROP INDEX by_kind ON docs RESTRICT`,
		`TRUNCATE TABLE docs RESTART IDENTITY`,
		`TRUNCATE TABLE docs CASCADE`,
		`TRUNCATE TABLE docs RESTART IDENTITY CASCADE`,
	} {
		t.Run("unsupported/"+statement, func(t *testing.T) {
			expectError(t, c.query(statement), sqlstateFeatureNotSupported)
		})
	}

	for _, statement := range []string{
		`DROP`,
		`DROP VIEW`,
		`DROP MATERIALIZED docs`,
		`DROP FUNCTION f(`,
		`DROP FUNCTION f(@)`,
		`DROP FUNCTION f(,)`,
		`DROP FUNCTION f(integer,)`,
		`DROP TRIGGER trg`,
		`DROP DATABASE first, second`,
		`DROP DATABASE old_db CASCADE`,
		`DROP TABLE`,
		`DROP TABLE docs,`,
		`DROP INDEX`,
		`DROP INDEX by_kind,`,
		`TRUNCATE`,
		`TRUNCATE TABLE`,
		`TRUNCATE TABLE docs RESTART`,
		`TRUNCATE TABLE docs CONTINUE CASCADE`,
	} {
		t.Run("malformed/"+statement, func(t *testing.T) {
			expectError(t, c.query(statement), sqlstateSyntaxError)
		})
	}
}
