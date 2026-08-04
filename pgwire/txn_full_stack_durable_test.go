package pgwire

import (
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

// TestTxnFullStackMultiTableDurableAllOrNothingAcrossReopen drives a
// two-table transaction over the wire, kills the commit via the decision-log
// create-fence fault seam (process-kill shape), reopens the durable catalog,
// and asserts all-or-nothing recovery. A second incarnation then commits a
// two-table transaction cleanly and proves both sides survive reopen.
//
// Named distinctly from main's in-flight/full_stack_durable_test.go helpers so
// the two files can coexist at merge time. Main's file covers single-table
// round-trip reopen; this file owns the multi-table crash and 40003 cases.
func TestTxnFullStackMultiTableDurableAllOrNothingAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")

	c, stop := openTxnFullStack(t, path)
	requireWireOK(t, c.query(`CREATE TABLE a (id STRING PRIMARY KEY, v STRING NOT NULL)`))
	requireWireOK(t, c.query(`CREATE TABLE b (id STRING PRIMARY KEY, v STRING NOT NULL)`))

	storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{
		Phase: storeio.TxnMarkerFaultCreateFileSync,
	})
	t.Cleanup(func() {
		storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
	})

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	requireWireOK(t, c.query(`INSERT INTO a (id, v) VALUES ('x', 'ax')`))
	requireWireOK(t, c.query(`INSERT INTO b (id, v) VALUES ('x', 'bx')`))
	faulted := c.query(`COMMIT`)
	if !has(faulted, msgErrorResponse) {
		t.Fatalf("mint-faulted COMMIT succeeded: %s", tags(faulted))
	}
	if !storeio.TxnMarkerCreateFaulted() {
		t.Fatal("create-fence fault did not fire")
	}
	assertReadyStatus(t, faulted, statusIdle)

	// Live catalog must already show abort-of-both before reopen.
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM a ORDER BY id`)); len(got) != 0 {
		t.Fatalf("pre-reopen a = %v, want empty", got)
	}
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM b ORDER BY id`)); len(got) != 0 {
		t.Fatalf("pre-reopen b = %v, want empty", got)
	}

	stop()
	storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})

	c2, stop2 := openTxnFullStack(t, path)
	if got := jsonStringColumn(t, requireSelect(t, c2,
		`SELECT id FROM a ORDER BY id`)); len(got) != 0 {
		t.Fatalf("reopened a = %v, want empty", got)
	}
	if got := jsonStringColumn(t, requireSelect(t, c2,
		`SELECT id FROM b ORDER BY id`)); len(got) != 0 {
		t.Fatalf("reopened b = %v, want empty", got)
	}

	assertReadyStatus(t, requireQueryReady(t, c2, `BEGIN`), statusInTx)
	requireWireOK(t, c2.query(`INSERT INTO a (id, v) VALUES ('ok', 'a1')`))
	requireWireOK(t, c2.query(`INSERT INTO b (id, v) VALUES ('ok', 'b1')`))
	assertReadyStatus(t, requireQueryReady(t, c2, `COMMIT`), statusIdle)
	stop2()

	c3, _ := openTxnFullStack(t, path)
	if got := jsonStringColumn(t, requireSelect(t, c3,
		`SELECT id FROM a ORDER BY id`)); !stringSlicesEqual(got, []string{"ok"}) {
		t.Fatalf("committed reopen a = %v, want [ok]", got)
	}
	if got := jsonStringColumn(t, requireSelect(t, c3,
		`SELECT id FROM b ORDER BY id`)); !stringSlicesEqual(got, []string{"ok"}) {
		t.Fatalf("committed reopen b = %v, want [ok]", got)
	}
}

// TestTxnFullStackMultiTableCommitOutcomeUnknown extends the 40003 coverage
// with a multi-table COMMIT that hits a decision-log sync fault. The session
// remains reusable at the protocol layer; reopen proves all-or-nothing.
func TestTxnFullStackMultiTableCommitOutcomeUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	c, stop := openTxnFullStack(t, path)

	requireWireOK(t, c.query(`CREATE TABLE a (id STRING PRIMARY KEY, v STRING NOT NULL)`))
	requireWireOK(t, c.query(`CREATE TABLE b (id STRING PRIMARY KEY, v STRING NOT NULL)`))

	restore := durable.InstallTxnMarkerSyncFaultForFacadeTest()
	t.Cleanup(restore)

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	requireWireOK(t, c.query(`INSERT INTO a (id, v) VALUES ('1', 'a1')`))
	requireWireOK(t, c.query(`INSERT INTO b (id, v) VALUES ('1', 'b1')`))
	unknown := c.query(`COMMIT`)
	fields := expectError(t, unknown, sqlstateStatementCompletionUnknown)
	if fields['S'] != "ERROR" {
		t.Fatalf("40003 severity = %q, want ERROR", fields['S'])
	}
	assertReadyStatus(t, unknown, statusIdle)

	// Protocol delivery of the next statement must not hang or FATAL; the
	// catalog may independently refuse with the same sticky unknown outcome.
	follow := c.query(`SELECT 1`)
	if has(follow, msgErrorResponse) {
		expectError(t, follow, sqlstateStatementCompletionUnknown)
	} else {
		requireWireOK(t, follow)
	}
	assertReadyStatus(t, follow, statusIdle)

	stop()
	restore()

	// Reopen: both participants agree — all committed or all aborted.
	c2, _ := openTxnFullStack(t, path)
	aKeys := jsonStringColumn(t, requireSelect(t, c2, `SELECT id FROM a ORDER BY id`))
	bKeys := jsonStringColumn(t, requireSelect(t, c2, `SELECT id FROM b ORDER BY id`))
	aHas := stringSlicesEqual(aKeys, []string{"1"})
	bHas := stringSlicesEqual(bKeys, []string{"1"})
	aEmpty := len(aKeys) == 0
	bEmpty := len(bKeys) == 0
	if aHas != bHas || aEmpty != bEmpty || (!aHas && !aEmpty) {
		t.Fatalf("torn reopen: a=%v b=%v", aKeys, bKeys)
	}
}

func openTxnFullStack(t *testing.T, path string) (*testClient, func()) {
	t.Helper()
	db, err := sqldriver.Open(path)
	if err != nil {
		t.Fatalf("open durable SQL catalog: %v", err)
	}
	server, err := NewServer(db, Options{Auth: Trust()})
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewServer: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = server.Close()
		_ = db.Close()
		t.Fatalf("listen on ephemeral loopback port: %v", err)
	}
	serveDone := make(chan struct{})
	var serveErr error
	go func() {
		defer close(serveDone)
		serveErr = server.Serve(listener)
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = server.Close()
		<-serveDone
		_ = db.Close()
		t.Fatalf("dial pgwire server: %v", err)
	}
	c := newTestClient(t, conn)
	c.readTimeout = 15 * time.Second
	c.startup(map[string]string{"user": "tester", "database": "app"})

	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(c.outbox)
			_ = conn.Close()
			if err := server.Close(); err != nil {
				t.Errorf("Server.Close: %v", err)
			}
			select {
			case <-serveDone:
			case <-time.After(5 * time.Second):
				t.Error("the Serve goroutine did not exit after Server.Close")
			}
			if serveErr != nil && !errors.Is(serveErr, ErrServerClosed) {
				t.Errorf("Serve returned %v, want ErrServerClosed", serveErr)
			}
			// A catalog poisoned by ErrCommitOutcomeUnknown may refuse Close
			// with that sticky error; the reopen path is the reconciliation.
			if err := db.Close(); err != nil &&
				!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
				t.Errorf("close durable SQL catalog: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return c, stop
}
