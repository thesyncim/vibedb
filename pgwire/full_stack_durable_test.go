package pgwire

import (
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// TestFullStackDurableRoundTripAcrossReopen exercises the whole stack a real
// client crosses — pgwire wire protocol, the database/sql driver catalog, and
// the durable store — in one always-on test, then proves that boundary holds
// across a process-lifetime restart.
//
// The rest of the repository's cross-boundary coverage lives in the env-gated
// integration/pgclient module, which is a separate Go module and does not run
// under a plain "go test ./..." of the root module. Nothing else in the root
// module drives pgwire -> sql/driver -> durable store and then reopens the same
// directory, so a regression that only surfaced when a committed wire write
// failed to survive a restart — or when a rolled-back one did survive — could
// pass every root-module test. This test closes that gap without an env gate,
// an external client dependency, or a bound port that another test could clash
// with: it speaks the protocol over loopback on an ephemeral port with the
// package's own wire-client utilities.
//
// The guarantee it pins is the durable profile's contract as the README states
// it: a successful mutation is persisted to the recovery journal before it
// becomes visible, so a COMMIT acknowledged over the wire is on disk. After the
// server is closed and the catalog is closed cleanly, a fresh process opening
// the same directory must see every committed row — the index-backed ones
// included — and must not see the write from the transaction that rolled back.
func TestFullStackDurableRoundTripAcrossReopen(t *testing.T) {
	// One directory, opened twice: first to write, then to prove the writes
	// outlived the writer. t.TempDir is per-test, so nothing here shares state
	// with another package running concurrently.
	path := filepath.Join(t.TempDir(), "catalog.vdb")

	// --- first incarnation: build the stack and write through it -----------
	c, stop := openFullStack(t, path)

	// CREATE TABLE with an index. The index is not decoration: the reopen
	// assertions below query through it, so a lost or corrupt index survives
	// this test only as a failure.
	wireOK(t, c.query(`CREATE TABLE docs (`+
		`id STRING PRIMARY KEY, grp STRING NOT NULL, n INTEGER NOT NULL)`))
	wireOK(t, c.query(`CREATE INDEX docs_by_grp ON docs(grp)`))

	// INSERTs including RETURNING. The returned keys prove the server ran the
	// mutation and reported the produced rows, not merely a command tag.
	seed := c.query(`INSERT INTO docs VALUES ` +
		`('{"id":"a","grp":"seed","n":1}'),` +
		`('{"id":"b","grp":"seed","n":2}') RETURNING id`)
	wireOK(t, seed)
	if tag := commandTagOf(t, seed); tag != "INSERT 0 2" {
		t.Fatalf("seed INSERT tag = %q, want %q", tag, "INSERT 0 2")
	}
	if got := wireKeys(t, seed); !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("seed RETURNING keys = %v, want [a b]", got)
	}

	// A SELECT that uses the index. EXPLAIN ANALYZE reports the access path the
	// planner actually took at runtime, so the assertion is a measured claim
	// that the index answered the predicate rather than a full scan.
	if got := wireKeys(t, mustSelect(t, c,
		`SELECT id FROM docs WHERE grp = 'seed' ORDER BY id`)); !equalStrings(
		got, []string{"a", "b"}) {
		t.Fatalf("indexed SELECT keys = %v, want [a b]", got)
	}
	if path := actualAccessPath(t, c,
		`EXPLAIN ANALYZE SELECT id FROM docs WHERE grp = 'seed' ORDER BY id`,
	); path != "exact-index" {
		t.Fatalf("indexed SELECT actual access path = %q, want %q",
			path, "exact-index")
	}

	// A transaction that commits. The committed row must survive the reopen.
	assertReadyStatus(t, mustQueryReady(t, c, `BEGIN`), statusInTx)
	committed := c.query(`INSERT INTO docs VALUES ` +
		`('{"id":"committed","grp":"tx","n":10}') RETURNING id`)
	wireOK(t, committed)
	if got := wireKeys(t, committed); !equalStrings(got, []string{"committed"}) {
		t.Fatalf("committed RETURNING keys = %v, want [committed]", got)
	}
	assertReadyStatus(t, mustQueryReady(t, c, `COMMIT`), statusIdle)

	// A transaction that rolls back. The staged row must not survive — nor even
	// be visible in this same connection after the ROLLBACK.
	assertReadyStatus(t, mustQueryReady(t, c, `BEGIN`), statusInTx)
	wireOK(t, c.query(`INSERT INTO docs VALUES `+
		`('{"id":"rolled","grp":"tx","n":11}')`))
	assertReadyStatus(t, mustQueryReady(t, c, `ROLLBACK`), statusIdle)

	// Before the reopen, the live catalog must already reflect the outcome: the
	// index-backed tx group contains only the committed row.
	if got := wireKeys(t, mustSelect(t, c,
		`SELECT id FROM docs WHERE grp = 'tx' ORDER BY id`)); !equalStrings(
		got, []string{"committed"}) {
		t.Fatalf("pre-reopen tx group = %v, want [committed]", got)
	}

	// Shut down the server and close the catalog cleanly. Server.Close waits for
	// every session goroutine to return, which is the guarantee that no session
	// is still reading the store when the catalog handle closes underneath it.
	stop()

	// --- second incarnation: prove the writes outlived the writer ----------
	c2, _ := openFullStack(t, path)

	// Every committed row survived, in key order, across the reopen.
	if got := wireKeys(t, mustSelect(t, c2,
		`SELECT id FROM docs ORDER BY id`)); !equalStrings(
		got, []string{"a", "b", "committed"}) {
		t.Fatalf("reopened catalog keys = %v, want [a b committed]", got)
	}

	// The rolled-back write did not survive — it is absent, not merely masked.
	if got := wireKeys(t, mustSelect(t, c2,
		`SELECT id FROM docs WHERE id = 'rolled'`)); len(got) != 0 {
		t.Fatalf("reopened catalog retained the rolled-back row: %v", got)
	}

	// The index survived the reopen as an index: it still answers its predicate,
	// and still does so by an index lookup rather than a rebuilt full scan.
	if got := wireKeys(t, mustSelect(t, c2,
		`SELECT id FROM docs WHERE grp = 'tx' ORDER BY id`)); !equalStrings(
		got, []string{"committed"}) {
		t.Fatalf("reopened indexed SELECT keys = %v, want [committed]", got)
	}
	if path := actualAccessPath(t, c2,
		`EXPLAIN ANALYZE SELECT id FROM docs WHERE grp = 'seed' ORDER BY id`,
	); path != "exact-index" {
		t.Fatalf("reopened indexed SELECT actual access path = %q, want %q",
			path, "exact-index")
	}
}

// openFullStack opens a durable SQL catalog on path via the sql/driver package,
// serves it with a pgwire Server on an ephemeral loopback port, and returns a
// wire client that has completed startup. The returned stop closes the client,
// the server, and the catalog in that order and is idempotent; it is also
// registered as a cleanup, so a fatal assertion still releases the durable
// writer lease this catalog holds.
//
// The stack is torn down explicitly rather than only at cleanup because the
// durable store is writer-locked: the second incarnation cannot open the same
// directory until the first has closed its handle.
func openFullStack(t *testing.T, path string) (*testClient, func()) {
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
	// A real durable open, index build, and reopen on a loaded machine is still
	// sub-second in practice, but the default read timeout is tuned for the
	// in-process net.Pipe suite; widen it so contention on the shared box turns
	// into a slow pass rather than a spurious deadline.
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
			// serveErr is written before serveDone closes, so this read is
			// ordered after that write.
			if serveErr != nil && !errors.Is(serveErr, ErrServerClosed) {
				t.Errorf("Serve returned %v, want ErrServerClosed", serveErr)
			}
			if err := db.Close(); err != nil {
				t.Errorf("close durable SQL catalog: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return c, stop
}

// wireOK fails the test if a simple-query response carried an ErrorResponse.
func wireOK(t *testing.T, msgs []backendMessage) {
	t.Helper()
	if has(msgs, msgErrorResponse) {
		t.Fatalf("wire command failed: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
}

// mustQueryReady runs one simple query, requires it to succeed, and returns its
// messages so the caller can assert the ReadyForQuery transaction status.
func mustQueryReady(t *testing.T, c *testClient, sql string) []backendMessage {
	t.Helper()
	msgs := c.query(sql)
	wireOK(t, msgs)
	return msgs
}

// mustSelect runs one SELECT and requires it to succeed.
func mustSelect(t *testing.T, c *testClient, sql string) []backendMessage {
	t.Helper()
	msgs := c.query(sql)
	wireOK(t, msgs)
	return msgs
}

// wireKeys decodes a single-column result whose cells are JSON string values —
// the shape both the RETURNING id and SELECT id statements here produce — into
// the plain key strings, in wire order.
func wireKeys(t *testing.T, msgs []backendMessage) []string {
	t.Helper()
	rows := rowsOf(t, msgs)
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) != 1 {
			t.Fatalf("expected one column, got %d: %q", len(row), row)
		}
		var key string
		if err := json.Unmarshal(row[0], &key); err != nil {
			t.Fatalf("result cell %q is not a JSON string: %v", row[0], err)
		}
		keys = append(keys, key)
	}
	return keys
}

// actualAccessPath runs an EXPLAIN ANALYZE and returns the access path the
// planner measured itself taking at runtime, decoded from the single QUERY PLAN
// row. It is how a test states "this SELECT used the index" as a measured fact
// rather than an assumption about the planner.
func actualAccessPath(t *testing.T, c *testClient, explainSQL string) string {
	t.Helper()
	msgs := c.query(explainSQL)
	wireOK(t, msgs)
	rows := rowsOf(t, msgs)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("EXPLAIN ANALYZE returned %d rows, want one QUERY PLAN row", len(rows))
	}
	var planText string
	if err := json.Unmarshal(rows[0][0], &planText); err != nil {
		t.Fatalf("QUERY PLAN cell is not a JSON string: %q: %v", rows[0][0], err)
	}
	var plan struct {
		Plan struct {
			Analyze struct {
				ActualAccessPath string `json:"actual_access_path"`
			} `json:"analyze"`
		} `json:"plan"`
	}
	if err := json.Unmarshal([]byte(planText), &plan); err != nil {
		t.Fatalf("QUERY PLAN is not the expected JSON shape: %s: %v", planText, err)
	}
	if plan.Plan.Analyze.ActualAccessPath == "" {
		t.Fatalf("QUERY PLAN carried no measured access path: %s", planText)
	}
	return plan.Plan.Analyze.ActualAccessPath
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
