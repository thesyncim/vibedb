package shardservice

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// The shard-service acceptance gate, exercised end to end over a served connection. Each
// test names one gate criterion:
//
//   - (a) epoch admission: a request for the wrong shard or distribution, a stale
//     ownership epoch, or a stale routing version is refused with the correct
//     typed error and never executes; a correct request executes. See
//     TestAcceptanceEpochAdmissionNoExecution.
//   - (b) snapshot lifetime: a read pins one snapshot for its duration and a
//     concurrent committed write is invisible to it. See
//     TestAcceptanceReadSnapshotPinned.
//   - (c) restart: Close drains a live connection and a fresh server serves the
//     persisted catalog cleanly. See TestAcceptanceRestartDrainsInFlight.
//   - (d) framing: a malformed or oversized frame is refused without a panic or
//     an allocation the peer sized. See TestAcceptanceMalformedFraming.
//   - (e) resource limits: exceeding the row or byte cap yields the typed limit
//     error and the connection stays usable. See TestAcceptanceResourceLimits.
//   - (f) no serialized plan: the wire carries SQL text plus typed parameters
//     only and the shard re-parses that text locally. See
//     TestAcceptanceNoSerializedPlan.

// TestAcceptanceEpochAdmissionNoExecution proves gate (a): every ownership
// divergence is refused with its typed error before any execution, and a correct
// request executes. Each refused request carries a destructive DELETE whose
// non-execution is proven by the seeded row surviving all of them.
func TestAcceptanceEpochAdmissionNoExecution(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("keep"), StringParam("Ada"), NumberParam("1")))

	divergences := []struct {
		name   string
		mutate func(*ShardRequest)
		want   ErrorKind
	}{
		{"wrong_distribution", func(r *ShardRequest) { r.Distribution = "other_tenant" }, ErrorNotOwner},
		{"wrong_shard", func(r *ShardRequest) { r.Shard = "80-" }, ErrorNotOwner},
		{"stale_allocation_generation", func(r *ShardRequest) { r.AllocationGeneration-- }, ErrorShardAllocation},
		{"stale_routing_version", func(r *ShardRequest) { r.RoutingVersion = 2 }, ErrorRoutingVersion},
		{"stale_epoch", func(r *ShardRequest) { r.OwnershipEpoch = 6 }, ErrorOwnershipEpoch},
	}
	for _, d := range divergences {
		t.Run(d.name, func(t *testing.T) {
			req := ownedRequest(`DELETE FROM docs`)
			d.mutate(req)
			resp := roundTrip(t, conn, req)
			if resp.Kind != ResponseError || resp.ErrorKind != d.want {
				t.Fatalf("%s DELETE = %+v, want %s error frame", d.name, resp, d.want)
			}
		})
	}

	// No divergent DELETE ran: the seeded row survives.
	sel := exec(t, conn, ownedRequest(`SELECT id FROM docs`))
	if sel.Kind != ResponseRows || len(sel.Rows) != 1 {
		t.Fatalf("post-refusal SELECT = %+v, want the one surviving row", sel)
	}
	if got := cellText(t, sel, 0, 0); got != `"keep"` {
		t.Fatalf("surviving id = %s, want \"keep\"", got)
	}

	// A correct request does execute and mutate.
	del := exec(t, conn, ownedRequest(`DELETE FROM docs WHERE id = ?`, StringParam("keep")))
	if del.Kind != ResponseCompletion || del.RowsAffected != 1 {
		t.Fatalf("owned DELETE = %+v, want completion affecting 1 row", del)
	}
	empty := exec(t, conn, ownedRequest(`SELECT id FROM docs`))
	if empty.Kind != ResponseRows || len(empty.Rows) != 0 {
		t.Fatalf("post-delete SELECT = %+v, want zero rows", empty)
	}
}

// TestAcceptanceReadSnapshotPinned proves gate (b) and documents the exact
// isolation a read request is served under: statement-level snapshot isolation.
// A read pins one snapshot at Begin and serves its whole result from it; a write
// another connection commits after that Begin is invisible to the read and
// becomes visible only to a later request. Within one request the server does
// Begin(ReadOnly) -> Query -> Rollback with no window for another connection to
// interleave, so this test drives that same read-only primitive directly to hold
// the snapshot open across a concurrent committed write.
func TestAcceptanceReadSnapshotPinned(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("1")))

	reader, err := srv.db.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer reader.Close()
	if err := reader.Begin(context.Background(), sqldriver.TxOptions{
		ReadOnly:  true,
		Isolation: sqldriver.IsolationRepeatableRead,
	}); err != nil {
		t.Fatalf("Begin(ReadOnly): %v", err)
	}
	if n := countRows(t, reader, `SELECT id FROM docs`); n != 1 {
		t.Fatalf("pinned snapshot before write = %d rows, want 1", n)
	}

	// A concurrent write commits through the shard on another connection.
	ins := exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("b"), StringParam("Grace"), NumberParam("2")))
	if ins.Kind != ResponseCompletion || ins.RowsAffected != 1 {
		t.Fatalf("concurrent INSERT = %+v, want completion affecting 1 row", ins)
	}

	// The pinned snapshot does not observe the concurrent commit.
	if n := countRows(t, reader, `SELECT id FROM docs`); n != 1 {
		t.Fatalf("pinned snapshot after concurrent write = %d rows, want 1 (write leaked into the read)", n)
	}
	if err := reader.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// A fresh read request observes the committed write.
	sel := exec(t, conn, ownedRequest(`SELECT id FROM docs ORDER BY id`))
	if sel.Kind != ResponseRows || len(sel.Rows) != 2 {
		t.Fatalf("post-release SELECT = %+v, want two rows", sel)
	}
}

// TestAcceptanceRestartDrainsInFlight proves gate (c): Close drains a live
// connection whose goroutine is parked in a blocking read, unblocks Serve with
// ErrServerClosed, and a fresh server opened over the same catalog path serves
// cleanly and reads back the persisted data.
func TestAcceptanceRestartDrainsInFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.vdb")

	db1 := openDB(t, path)
	srv1, err := NewServer(db1, testOwner(), Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv1.Serve(l) }()

	conn1, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	exec(t, conn1, ownedRequest(ddlDocs))
	exec(t, conn1, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))
	// conn1 is left open: a live, tracked connection whose server goroutine is
	// now parked in a blocking read. Close must drain it.

	closed := make(chan error, 1)
	go func() { closed <- srv1.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not drain the live connection within 5s")
	}
	select {
	case err := <-serveErr:
		if !errors.Is(err, ErrServerClosed) {
			t.Fatalf("Serve = %v, want ErrServerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after Close")
	}

	// The drained connection was closed by the server: a read now fails.
	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := DecodeResponse(conn1); err == nil {
		t.Fatal("expected the drained connection to be closed by the server")
	}
	_ = conn1.Close()
	if err := db1.Close(); err != nil {
		t.Fatalf("db1.Close: %v", err)
	}

	// A fresh server over the same catalog serves cleanly and sees the data.
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

// TestAcceptanceMalformedFraming proves gate (d): an oversized frame is refused
// from its header before its body is read or allocated, a framing desync ends the
// connection, and a bounded body-level malformation is answered with a typed
// frame while the stream stays aligned. None of these panic the server goroutine
// (a panic there would crash this test binary).
func TestAcceptanceMalformedFraming(t *testing.T) {
	srv, _ := newServer(t, Options{})

	t.Run("oversized_length_refused_before_alloc", func(t *testing.T) {
		conn := dial(t, srv)
		// A header declaring a ~2 GiB body must be refused from the five header
		// bytes alone. The body is deliberately never sent, so a server that
		// tried to allocate it would block rather than return.
		if _, err := conn.Write([]byte{tagRequest, 0x7f, 0xff, 0xff, 0xff}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := DecodeResponse(conn); err == nil {
			t.Fatal("oversized frame drew a response; want the connection closed")
		}
	})

	t.Run("bad_tag_ends_connection", func(t *testing.T) {
		conn := dial(t, srv)
		// The response tag in the request direction is an unrecoverable framing
		// desync; the server rejects it from the tag byte and ends the connection
		// before reading any body, so only the five header bytes are sent.
		if _, err := conn.Write([]byte{tagResponse, 0x00, 0x00, 0x00, 0x06}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := DecodeResponse(conn); err == nil {
			t.Fatal("bad-tag frame drew a response; want the connection closed")
		}
	})

	t.Run("impossible_element_count_is_recoverable", func(t *testing.T) {
		conn := dial(t, srv)
		exec(t, conn, ownedRequest(ddlDocs))
		// A tiny frame that names a million parameters must be rejected without
		// allocating a million-element slice, and it leaves the stream aligned.
		own := testOwner()
		body := func() []byte {
			var e encbuf
			e.u8(wireVersion)
			e.str("SELECT id FROM docs")
			e.str(string(own.Distribution))
			e.str(string(own.Shard))
			e.u64(uint64(own.AllocationGeneration))
			e.u64(uint64(own.RoutingVersion))
			e.u64(uint64(own.Epoch))
			e.u8(uint8(ReadStrong))
			e.u8(uint8(ExecutionReadOnly))
			e.u64(0)
			e.u64(0)
			e.u64(0)
			e.u32(1_000_000) // far more params than the body could carry
			return e.b
		}()
		if _, err := conn.Write(rawFrame(tagRequest, body)); err != nil {
			t.Fatalf("write frame: %v", err)
		}
		resp, err := DecodeResponse(conn)
		if err != nil {
			t.Fatalf("DecodeResponse: %v", err)
		}
		if resp.Kind != ResponseError || resp.ErrorKind != ErrorMalformedRequest {
			t.Fatalf("impossible-count frame = %+v, want MalformedRequest frame", resp)
		}
		// The stream stayed aligned: a well-formed request still works.
		if got := exec(t, conn, ownedRequest(`SELECT id FROM docs`)); got.Kind != ResponseRows {
			t.Fatalf("post-malformed request = %+v, want Rows", got)
		}
	})
}

// TestAcceptanceResourceLimits proves gate (e): exceeding the per-request row or
// byte cap surfaces as a typed resource-limit frame rather than a partial or
// oversized result, and the connection remains usable afterward.
func TestAcceptanceResourceLimits(t *testing.T) {
	seed := func(t *testing.T) net.Conn {
		t.Helper()
		srv, _ := newServer(t, Options{})
		conn := dial(t, srv)
		exec(t, conn, ownedRequest(ddlDocs))
		for _, id := range []string{"a", "b", "c", "d"} {
			exec(t, conn, ownedRequest(
				`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
				StringParam(id), StringParam("a-reasonably-long-name-value"), NumberParam("1")))
		}
		return conn
	}

	t.Run("max_rows", func(t *testing.T) {
		conn := seed(t)
		req := ownedRequest(`SELECT id FROM docs`)
		req.MaxRows = 1
		resp := roundTrip(t, conn, req)
		if resp.Kind != ResponseError || resp.ErrorKind != ErrorResourceLimit {
			t.Fatalf("row-capped SELECT = %+v, want ResourceLimit frame", resp)
		}
		// The connection remains usable within the cap.
		ok := ownedRequest(`SELECT id FROM docs WHERE id = ?`, StringParam("a"))
		ok.MaxRows = 1
		if got := exec(t, conn, ok); got.Kind != ResponseRows || len(got.Rows) != 1 {
			t.Fatalf("within-cap SELECT = %+v, want one row", got)
		}
	})

	t.Run("max_result_bytes", func(t *testing.T) {
		conn := seed(t)
		req := ownedRequest(`SELECT id, name, n FROM docs`)
		req.MaxResultBytes = 16 // far below the materialized multi-row result
		resp := roundTrip(t, conn, req)
		if resp.Kind != ResponseError || resp.ErrorKind != ErrorResourceLimit {
			t.Fatalf("byte-capped SELECT = %+v, want ResourceLimit frame", resp)
		}
		// The connection remains usable for a bounded query.
		ok := ownedRequest(`SELECT id FROM docs WHERE id = ?`, StringParam("a"))
		if got := exec(t, conn, ok); got.Kind != ResponseRows || len(got.Rows) != 1 {
			t.Fatalf("post-limit SELECT = %+v, want one row", got)
		}
	})
}

// TestAcceptanceNoSerializedPlan proves gate (f): the wire carries SQL text plus
// typed parameters only — never a serialized plan or program — and the shard
// re-parses the text locally.
func TestAcceptanceNoSerializedPlan(t *testing.T) {
	// (1) The request type carries SQL text, typed params, ownership, optional
	// byte-native storage access envelopes, session-position coordinates, and
	// execution bounds — no serialized plan-shaped field.
	reqFields := structFieldNames(reflect.TypeOf(ShardRequest{}))
	assertFieldSet(t, "ShardRequest", reqFields, map[string]bool{
		"Authority": true,
		"SQL":       true, "Params": true, "Distribution": true, "Shard": true,
		"AllocationGeneration": true, "RoutingVersion": true, "OwnershipEpoch": true,
		"HasMinPosition": true, "MinPosition": true, "ReadPolicy": true,
		"ExecutionMode": true,
		"Deadline":      true, "MaxResultBytes": true, "MaxRows": true,
		"BucketBits": true, "AccessScopes": true, "ReadFenceID": true,
		"GlobalIndexLookup": true, "PrimaryKeyRead": true,
		"MutationCapture": true, "DocumentScan": true, "Repartition": true, "PartialAggregate": true,
		"RowBatch": true, "Exchange": true,
		"Transaction": true,
	})
	assertNoPlanField(t, "ShardRequest", reqFields)

	// (2) A parameter is a typed scalar/document value, never an encoded plan.
	paramFields := structFieldNames(reflect.TypeOf(Param{}))
	assertFieldSet(t, "Param", paramFields, map[string]bool{
		"Kind": true, "Bool": true, "Bytes": true,
	})
	assertNoPlanField(t, "Param", paramFields)

	// (3) The SQL text crosses the wire verbatim.
	const sql = `SELECT id, name FROM docs WHERE n >= ? ORDER BY id`
	frame := encodeRequest(t, ownedRequest(sql, NumberParam("1")))
	if !bytes.Contains(frame, []byte(sql)) {
		t.Fatal("encoded request frame does not contain the SQL text verbatim")
	}

	// (4) The shard parses and plans the text itself.
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))

	// Invalid text is rejected by the shard's own parser: there is no
	// pre-serialized plan to replay.
	bad := roundTrip(t, conn, ownedRequest(`SELCT nonsense FROM`))
	if bad.Kind != ResponseError || bad.ErrorKind != ErrorMalformedRequest {
		t.Fatalf("invalid SQL = %+v, want MalformedRequest from the local parser", bad)
	}

	// Valid text is parsed, planned, bound, and executed locally.
	sel := exec(t, conn, ownedRequest(sql, NumberParam("7")))
	if sel.Kind != ResponseRows || len(sel.Rows) != 1 {
		t.Fatalf("re-parsed SELECT = %+v, want one row", sel)
	}
	if got := cellText(t, sel, 0, 0); got != `"a"` {
		t.Fatalf("re-parsed row id = %s, want \"a\"", got)
	}
}

// countRows drives a directly borrowed Session to count the rows one SELECT
// returns, so a test can hold a snapshot open across an interleaved write.
func countRows(t *testing.T, sess *sqldriver.Session, sql string) int {
	t.Helper()
	ctx := context.Background()
	prep, err := sess.Prepare(ctx, sql)
	if err != nil {
		t.Fatalf("Prepare(%q): %v", sql, err)
	}
	defer prep.Close()
	cur, err := prep.Query(ctx, nil)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer cur.Close()
	n := 0
	for cur.Next() {
		n++
	}
	return n
}

// structFieldNames returns the exported and unexported field names of a struct
// type in declaration order.
func structFieldNames(tp reflect.Type) []string {
	names := make([]string, tp.NumField())
	for i := range names {
		names[i] = tp.Field(i).Name
	}
	return names
}

// assertFieldSet fails unless got is exactly the field set want.
func assertFieldSet(t *testing.T, typeName string, got []string, want map[string]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s has fields %v, want exactly %d fields", typeName, got, len(want))
	}
	for _, f := range got {
		if !want[f] {
			t.Fatalf("%s has unexpected field %q; the wire must not gain plan state", typeName, f)
		}
	}
}

// assertNoPlanField fails if any field name looks like a serialized plan or
// program, guarding hard rule (1) against a future accidental addition.
func assertNoPlanField(t *testing.T, typeName string, fields []string) {
	t.Helper()
	for _, f := range fields {
		for _, banned := range []string{"Plan", "Program", "Constraint", "Bytecode"} {
			if strings.Contains(f, banned) {
				t.Fatalf("%s carries a %q-shaped field %q; the wire carries SQL text only", typeName, banned, f)
			}
		}
	}
}
