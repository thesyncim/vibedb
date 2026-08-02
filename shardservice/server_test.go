package shardservice

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// testOwner is the static identity every server test configures its shard with.
func testOwner() Ownership {
	return Ownership{
		Distribution:   "tenant_data",
		Shard:          "-80",
		Epoch:          7,
		RoutingVersion: 3,
	}
}

// ownedRequest returns a request that admits against testOwner, carrying sql.
func ownedRequest(sql string, params ...Param) *ShardRequest {
	own := testOwner()
	return &ShardRequest{
		SQL:            sql,
		Params:         params,
		Distribution:   own.Distribution,
		Shard:          own.Shard,
		RoutingVersion: own.RoutingVersion,
		OwnershipEpoch: own.Epoch,
	}
}

// openDB opens a fresh durable catalog under path.
func openDB(t *testing.T, path string) *sqldriver.Database {
	t.Helper()
	db, err := sqldriver.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newServer builds a server over a fresh database and testOwner. It returns the
// server and its database so a test can restart against the same path.
func newServer(t *testing.T, opts Options) (*Server, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shard.vdb")
	db := openDB(t, path)
	srv, err := NewServer(db, testOwner(), opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv, path
}

// dial serves one in-process connection and returns the client half.
func dial(t *testing.T, srv *Server) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	go srv.ServeConn(server)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// roundTrip sends one request and reads one response over conn.
func roundTrip(t *testing.T, conn net.Conn, req *ShardRequest) *ShardResponse {
	t.Helper()
	if err := EncodeRequest(conn, req); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	resp, err := DecodeResponse(conn)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	return resp
}

// exec sends a statement expected to complete, failing on any error frame.
func exec(t *testing.T, conn net.Conn, req *ShardRequest) *ShardResponse {
	t.Helper()
	resp := roundTrip(t, conn, req)
	if resp.Kind == ResponseError {
		t.Fatalf("statement %q returned error frame: %s: %s",
			req.SQL, resp.ErrorKind, resp.ErrorMessage)
	}
	return resp
}

// cellText returns the JSON bytes of one non-null cell as a string.
func cellText(t *testing.T, resp *ShardResponse, row, col int) string {
	t.Helper()
	if resp.Kind != ResponseRows {
		t.Fatalf("response kind = %s, want Rows", resp.Kind)
	}
	if row >= len(resp.Rows) || col >= len(resp.Rows[row]) {
		t.Fatalf("cell (%d,%d) out of range for %dx%d result",
			row, col, len(resp.Rows), len(resp.Columns))
	}
	c := resp.Rows[row][col]
	if c.Null {
		t.Fatalf("cell (%d,%d) is null", row, col)
	}
	return string(c.Bytes)
}

const ddlDocs = `CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL, n INTEGER)`

// TestServerExecuteLifecycle drives the full request lifecycle over one
// connection: DDL, DML, and a parameterized SELECT that streams back rows.
func TestServerExecuteLifecycle(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	create := exec(t, conn, ownedRequest(ddlDocs))
	if create.Kind != ResponseCompletion {
		t.Fatalf("CREATE TABLE kind = %s, want Completion", create.Kind)
	}

	insert := exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))
	if insert.Kind != ResponseCompletion || insert.RowsAffected != 1 {
		t.Fatalf("INSERT = %+v, want completion affecting 1 row", insert)
	}
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("b"), StringParam("Grace"), NumberParam("9")))

	sel := exec(t, conn, ownedRequest(
		`SELECT id, name, n FROM docs WHERE n >= ? ORDER BY id`,
		NumberParam("7")))
	if sel.Kind != ResponseRows {
		t.Fatalf("SELECT kind = %s, want Rows", sel.Kind)
	}
	wantCols := []string{"id", "name", "n"}
	if len(sel.Columns) != len(wantCols) {
		t.Fatalf("columns = %d, want %d", len(sel.Columns), len(wantCols))
	}
	for i, name := range wantCols {
		if sel.Columns[i].Name != name {
			t.Fatalf("column %d = %q, want %q", i, sel.Columns[i].Name, name)
		}
		if sel.Columns[i].TypeOID != pgOIDJSON {
			t.Fatalf("column %d OID = %d, want %d", i, sel.Columns[i].TypeOID, pgOIDJSON)
		}
	}
	if len(sel.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(sel.Rows))
	}
	if got := cellText(t, sel, 0, 0); got != `"a"` {
		t.Fatalf("row 0 id = %s, want \"a\"", got)
	}
	if got := cellText(t, sel, 1, 2); got != `9` {
		t.Fatalf("row 1 n = %s, want 9", got)
	}
}

// TestServerAdmissionBeforeExecution proves admission runs before any execution:
// a request the shard does not own is refused with the typed frame and its
// destructive SQL never runs.
func TestServerAdmissionBeforeExecution(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))

	// A not-owner request carrying a DROP must be refused, not executed.
	drop := ownedRequest(`DROP TABLE docs`)
	drop.Shard = "80-"
	resp := roundTrip(t, conn, drop)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorNotOwner {
		t.Fatalf("wrong-shard DROP = %+v, want NotOwner error frame", resp)
	}

	// The table and its row must survive, proving DROP never ran.
	sel := exec(t, conn, ownedRequest(`SELECT id FROM docs`))
	if sel.Kind != ResponseRows || len(sel.Rows) != 1 {
		t.Fatalf("post-refusal SELECT = %+v, want the surviving row", sel)
	}
}

// TestServerAdmissionErrors maps every admission divergence to its typed frame.
func TestServerAdmissionErrors(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	tests := []struct {
		name   string
		mutate func(*ShardRequest)
		want   ErrorKind
	}{
		{"wrong_distribution", func(r *ShardRequest) { r.Distribution = "other" }, ErrorNotOwner},
		{"wrong_shard", func(r *ShardRequest) { r.Shard = "80-" }, ErrorNotOwner},
		{"stale_routing_version", func(r *ShardRequest) { r.RoutingVersion = 2 }, ErrorRoutingVersion},
		{"stale_epoch", func(r *ShardRequest) { r.OwnershipEpoch = 6 }, ErrorOwnershipEpoch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := ownedRequest(`SELECT id FROM docs`)
			tc.mutate(req)
			resp := roundTrip(t, conn, req)
			if resp.Kind != ResponseError || resp.ErrorKind != tc.want {
				t.Fatalf("resp = %+v, want %s error frame", resp, tc.want)
			}
		})
	}
}

// TestServerResourceLimit proves a per-request row cap surfaces as a typed
// resource-limit frame rather than a partial or oversized result.
func TestServerResourceLimit(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	for _, id := range []string{"a", "b", "c"} {
		exec(t, conn, ownedRequest(
			`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
			StringParam(id), StringParam("x"), NumberParam("1")))
	}

	req := ownedRequest(`SELECT id FROM docs`)
	req.MaxRows = 1
	resp := roundTrip(t, conn, req)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorResourceLimit {
		t.Fatalf("capped SELECT = %+v, want ResourceLimit error frame", resp)
	}

	// The connection remains usable: a request within the cap still succeeds.
	within := ownedRequest(`SELECT id FROM docs WHERE id = ?`, StringParam("a"))
	within.MaxRows = 1
	if got := exec(t, conn, within); got.Kind != ResponseRows || len(got.Rows) != 1 {
		t.Fatalf("within-cap SELECT = %+v, want one row", got)
	}
}

// TestServerDeadline proves an already-elapsed request budget surfaces as a
// typed deadline frame rather than executing.
func TestServerDeadline(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))

	req := ownedRequest(`SELECT id, name, n FROM docs ORDER BY id`)
	req.Deadline = time.Nanosecond
	resp := roundTrip(t, conn, req)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorDeadlineExceeded {
		t.Fatalf("expired SELECT = %+v, want DeadlineExceeded error frame", resp)
	}
}

// TestServerMalformedRequest proves a body-level malformation is answered with a
// malformed-request frame and the same connection keeps serving.
func TestServerMalformedRequest(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	// A request naming more parameters than its body carries is a body-level
	// malformation the codec rejects without desynchronizing the stream.
	bad := ownedRequest(`SELECT id FROM docs`)
	bad.Params = []Param{NullParam()}
	raw := encodeRequest(t, bad)
	// Overwrite the trailing parameter-kind byte with an out-of-range enumerator.
	raw[len(raw)-1] = 0xFF
	if _, err := conn.Write(raw); err != nil {
		t.Fatalf("write malformed frame: %v", err)
	}
	resp, err := DecodeResponse(conn)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorMalformedRequest {
		t.Fatalf("malformed request = %+v, want MalformedRequest error frame", resp)
	}

	// The stream stayed aligned: a well-formed request still succeeds.
	if got := exec(t, conn, ownedRequest(`SELECT id FROM docs`)); got.Kind != ResponseRows {
		t.Fatalf("post-malformed SELECT = %+v, want Rows", got)
	}
}

// TestServerRestart proves durability across a shard restart: data written by one
// server is readable by a fresh server opened over the same catalog path.
func TestServerRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.vdb")

	db1 := openDB(t, path)
	srv1, err := NewServer(db1, testOwner(), Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	conn1 := dial(t, srv1)
	exec(t, conn1, ownedRequest(ddlDocs))
	exec(t, conn1, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))
	_ = conn1.Close()
	if err := srv1.Close(); err != nil {
		t.Fatalf("srv1.Close: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("db1.Close: %v", err)
	}

	db2 := openDB(t, path)
	srv2, err := NewServer(db2, testOwner(), Options{})
	if err != nil {
		t.Fatalf("reopen NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv2.Close() })
	conn2 := dial(t, srv2)
	sel := exec(t, conn2, ownedRequest(`SELECT id, name FROM docs`))
	if sel.Kind != ResponseRows || len(sel.Rows) != 1 {
		t.Fatalf("post-restart SELECT = %+v, want the persisted row", sel)
	}
	if got := cellText(t, sel, 0, 0); got != `"a"` {
		t.Fatalf("post-restart id = %s, want \"a\"", got)
	}
}

// TestServerCloseGraceful proves Close returns after in-flight connections drain.
func TestServerCloseGraceful(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s")
	}
	// A second Close is a no-op.
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestServeAndClose proves Serve accepts over a real listener and unblocks with
// ErrServerClosed on Close.
func TestServeAndClose(t *testing.T) {
	srv, _ := newServer(t, Options{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(l) }()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	exec(t, conn, ownedRequest(ddlDocs))
	_ = conn.Close()

	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-serveErr:
		if !errors.Is(err, ErrServerClosed) {
			t.Fatalf("Serve returned %v, want ErrServerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after Close")
	}
}

// TestNewServerValidation covers the constructor's rejections and defaults.
func TestNewServerValidation(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "v.vdb"))

	if _, err := NewServer(nil, testOwner(), Options{}); err == nil {
		t.Fatal("NewServer(nil db) = nil error")
	}
	if _, err := NewServer(db, Ownership{Shard: "-80"}, Options{}); err == nil {
		t.Fatal("NewServer(empty distribution) = nil error")
	}
	if _, err := NewServer(db, testOwner(), Options{MaxConnections: -2}); err == nil {
		t.Fatal("NewServer(MaxConnections=-2) = nil error")
	}
	srv, err := NewServer(db, testOwner(), Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	if srv.opts.MaxConnections != DefaultMaxConnections {
		t.Fatalf("MaxConnections default = %d, want %d",
			srv.opts.MaxConnections, DefaultMaxConnections)
	}
	if srv.opts.MaxResultRows != DefaultMaxResultRows {
		t.Fatalf("MaxResultRows default = %d, want %d",
			srv.opts.MaxResultRows, DefaultMaxResultRows)
	}
}

// TestServerReadPolicyStrong proves the honored strong read policy is served and
// the reserved values remain wire-valid (zero value is safe).
func TestServerReadPolicyStrong(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	req := ownedRequest(`SELECT id FROM docs`)
	req.ReadPolicy = ReadStrong
	if got := exec(t, conn, req); got.Kind != ResponseRows {
		t.Fatalf("strong read = %+v, want Rows", got)
	}
}
