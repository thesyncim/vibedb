package pgwire

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/store"
)

// A protocol client written for the tests, and the fixtures every test in this
// package runs against.
//
// The client speaks the wire protocol over a net.Pipe, so the suite needs no
// port, no external binary, and no PostgreSQL. That is not only convenience: a
// net.Pipe delivers nothing the peer did not write, so a test that reads a
// message the server has not sent blocks rather than passing on a timing
// accident, and every test here therefore proves the server actually produced
// what it says it did. A read deadline turns that block into a failure with a
// message. Writes go through a buffer, for the reason on newTestClient.
//
// It is a deliberately dumb client. It does not model the protocol's state
// machine, does not retry, and does not interpret anything it is not asked
// about, because a test client that shared the server's understanding of the
// protocol would agree with the server's bugs.

// backendMessage is one decoded server message.
type backendMessage struct {
	tag  byte
	body []byte
}

// A testClient is a frontend speaking to a session over one connection.
type testClient struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
	// outbox is the client's send buffer; see newTestClient.
	outbox chan writeRequest
	// params are the ParameterStatus values the server reported.
	params map[string]string
	// pid and secret are the backend key, kept so a cancel test can use it.
	pid, secret int32
}

// A writeRequest is one queued write, optionally with an acknowledgement the
// caller waits on.
type writeRequest struct {
	data []byte
	ack  chan struct{}
}

// newTestClient wraps one end of a connection, giving the client a send buffer.
//
// The buffer is not a convenience, it is fidelity. A net.Pipe is unbuffered and
// synchronous: a write blocks until the peer reads it. A real socket has a send
// buffer, which is what lets a client pipeline Parse/Bind/Execute/Sync in one
// burst while the server is writing an error back. Without a buffer here, a
// server that correctly flushes an error mid-pipeline — which PostgreSQL does,
// and which this one does so a client's Flush is answered — deadlocks against a
// test client that is still writing. The deadlock would be the harness's, and
// pinning the server's behaviour to it would be pinning the wrong thing.
func newTestClient(t *testing.T, conn net.Conn) *testClient {
	c := &testClient{
		t:      t,
		conn:   conn,
		br:     bufio.NewReader(conn),
		outbox: make(chan writeRequest, 64),
		params: map[string]string{},
	}
	go func() {
		for req := range c.outbox {
			if req.data != nil {
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				// A write failure is not reported: a test that closed the
				// connection, or a server that went away, is an ordinary thing
				// for these tests to arrange, and the assertion belongs to
				// whichever read discovers it.
				_, _ = conn.Write(req.data)
			}
			if req.ack != nil {
				close(req.ack)
			}
		}
	}()
	return c
}

// dial starts a server on one end of a net.Pipe and returns a client on the
// other. The session goroutine is joined at test cleanup, which is what makes a
// leaked session a test failure rather than a slow leak.
func dial(t *testing.T, srv *Server) *testClient {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeConn(server)
	}()
	c := newTestClient(t, client)
	t.Cleanup(func() {
		close(c.outbox)
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the session goroutine did not exit after the client closed")
		}
	})
	return c
}

// send writes one frontend message.
func (c *testClient) send(tag byte, body []byte) {
	c.t.Helper()
	var head [5]byte
	head[0] = tag
	binary.BigEndian.PutUint32(head[1:], uint32(len(body)+4))
	c.write(append(head[:], body...))
}

// sendRaw writes bytes verbatim, for the tests that need a malformed frame.
func (c *testClient) sendRaw(b []byte) { c.write(b) }

func (c *testClient) write(b []byte) {
	c.outbox <- writeRequest{data: append([]byte(nil), b...)}
}

// drainWrites blocks until everything queued has been handed to the connection.
// A test that closes the connection to simulate a client vanishing needs it, so
// that the close does not race ahead of the bytes it is meant to follow.
func (c *testClient) drainWrites() {
	ack := make(chan struct{})
	c.outbox <- writeRequest{ack: ack}
	<-ack
}

// startup performs the startup packet exchange and reads through
// ReadyForQuery, recording the reported parameters and the backend key.
func (c *testClient) startup(params map[string]string) {
	c.t.Helper()
	c.sendStartup(params)
	for {
		m := c.recv()
		switch m.tag {
		case msgAuthentication:
			if code := int32(binary.BigEndian.Uint32(m.body)); code != authOK {
				c.t.Fatalf("expected AuthenticationOk, got authentication code %d", code)
			}
		case msgParameterStatus:
			f := fields{b: m.body}
			name, value := f.cstring(), f.cstring()
			c.params[name] = value
		case msgBackendKeyData:
			f := fields{b: m.body}
			c.pid, c.secret = f.int32(), f.int32()
		case msgReadyForQuery:
			if m.body[0] != statusIdle {
				c.t.Fatalf("ReadyForQuery status is %q, want %q", m.body[0], statusIdle)
			}
			return
		case msgErrorResponse:
			c.t.Fatalf("startup failed: %s", formatError(m.body))
		default:
			c.t.Fatalf("unexpected message %q during startup", string(rune(m.tag)))
		}
	}
}

func (c *testClient) sendStartup(params map[string]string) {
	c.t.Helper()
	body := binary.BigEndian.AppendUint32(nil, protocolVersion30)
	for k, v := range params {
		body = append(body, k...)
		body = append(body, 0)
		body = append(body, v...)
		body = append(body, 0)
	}
	body = append(body, 0)
	c.write(binary.BigEndian.AppendUint32(nil, uint32(len(body)+4)))
	c.write(body)
}

// recv reads one backend message.
func (c *testClient) recv() backendMessage {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var head [5]byte
	if _, err := io.ReadFull(c.br, head[:]); err != nil {
		c.t.Fatalf("read message header: %v", err)
	}
	size := int(binary.BigEndian.Uint32(head[1:])) - 4
	if size < 0 || size > 1<<24 {
		c.t.Fatalf("server sent an implausible message length %d", size)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(c.br, body); err != nil {
		c.t.Fatalf("read message body: %v", err)
	}
	return backendMessage{tag: head[0], body: body}
}

// until reads messages until one with the given tag arrives, returning
// everything up to and including it.
func (c *testClient) until(tag byte) []backendMessage {
	c.t.Helper()
	var out []backendMessage
	for {
		m := c.recv()
		out = append(out, m)
		if m.tag == tag {
			return out
		}
	}
}

// query runs a simple query and returns every message through ReadyForQuery.
func (c *testClient) query(sql string) []backendMessage {
	c.t.Helper()
	c.send(msgQuery, append([]byte(sql), 0))
	return c.until(msgReadyForQuery)
}

// terminate sends Terminate.
func (c *testClient) terminate() { c.send(msgTerminate, nil) }

// --- message builders ------------------------------------------------------

func parseMsg(name, sql string, oids ...int32) []byte {
	b := append([]byte(name), 0)
	b = append(b, sql...)
	b = append(b, 0)
	b = binary.BigEndian.AppendUint16(b, uint16(len(oids)))
	for _, oid := range oids {
		b = binary.BigEndian.AppendUint32(b, uint32(oid))
	}
	return b
}

// bindMsg builds a Bind. A nil parameter is SQL NULL.
func bindMsg(portal, stmt string, paramFormats []int16, params [][]byte, resultFormats []int16) []byte {
	b := append([]byte(portal), 0)
	b = append(b, stmt...)
	b = append(b, 0)
	b = binary.BigEndian.AppendUint16(b, uint16(len(paramFormats)))
	for _, f := range paramFormats {
		b = binary.BigEndian.AppendUint16(b, uint16(f))
	}
	b = binary.BigEndian.AppendUint16(b, uint16(len(params)))
	for _, p := range params {
		if p == nil {
			b = binary.BigEndian.AppendUint32(b, ^uint32(0)) // -1
			continue
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(p)))
		b = append(b, p...)
	}
	b = binary.BigEndian.AppendUint16(b, uint16(len(resultFormats)))
	for _, f := range resultFormats {
		b = binary.BigEndian.AppendUint16(b, uint16(f))
	}
	return b
}

func describeMsg(target byte, name string) []byte {
	return append([]byte{target}, append([]byte(name), 0)...)
}

func executeMsg(portal string, maxRows int32) []byte {
	b := append([]byte(portal), 0)
	return binary.BigEndian.AppendUint32(b, uint32(maxRows))
}

func closeMsg(target byte, name string) []byte {
	return append([]byte{target}, append([]byte(name), 0)...)
}

// --- message decoders ------------------------------------------------------

// decodedRow is a DataRow's values, nil for NULL.
type decodedRow [][]byte

func decodeDataRow(t *testing.T, body []byte) decodedRow {
	t.Helper()
	f := fields{b: body}
	n := f.int16()
	row := make(decodedRow, 0, n)
	for range n {
		size := f.int32()
		if size == -1 {
			row = append(row, nil)
			continue
		}
		row = append(row, append([]byte(nil), f.slice(int(size))...))
	}
	if err := f.end(); err != nil {
		t.Fatalf("malformed DataRow: %v", err)
	}
	return row
}

// describedColumn is one RowDescription field.
type describedColumn struct {
	name   string
	oid    int32
	size   int16
	format int16
}

func decodeRowDescription(t *testing.T, body []byte) []describedColumn {
	t.Helper()
	f := fields{b: body}
	n := f.int16()
	cols := make([]describedColumn, 0, n)
	for range n {
		var c describedColumn
		c.name = f.cstring()
		f.int32() // table OID
		f.int16() // attribute number
		c.oid = f.int32()
		c.size = int16(f.int16())
		f.int32() // type modifier
		c.format = int16(f.int16())
		cols = append(cols, c)
	}
	if err := f.end(); err != nil {
		t.Fatalf("malformed RowDescription: %v", err)
	}
	return cols
}

// errorFields decodes an ErrorResponse into its field map.
func errorFields(body []byte) map[byte]string {
	out := map[byte]string{}
	f := fields{b: body}
	for {
		key := f.uint8()
		if f.bad() || key == 0 {
			return out
		}
		out[key] = f.cstring()
	}
}

func formatError(body []byte) string {
	fs := errorFields(body)
	return fmt.Sprintf("%s %s: %s (P=%s H=%s)", fs['S'], fs['C'], fs['M'], fs['P'], fs['H'])
}

// --- assertions ------------------------------------------------------------

// find returns the first message with the given tag, or fails.
func find(t *testing.T, msgs []backendMessage, tag byte) backendMessage {
	t.Helper()
	for _, m := range msgs {
		if m.tag == tag {
			return m
		}
	}
	t.Fatalf("no %q message in %s", string(rune(tag)), tags(msgs))
	return backendMessage{}
}

func has(msgs []backendMessage, tag byte) bool {
	for _, m := range msgs {
		if m.tag == tag {
			return true
		}
	}
	return false
}

func tags(msgs []backendMessage) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, m := range msgs {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(strconv.QuoteRune(rune(m.tag)))
	}
	b.WriteByte(']')
	return b.String()
}

func rowsOf(t *testing.T, msgs []backendMessage) []decodedRow {
	t.Helper()
	var out []decodedRow
	for _, m := range msgs {
		if m.tag == msgDataRow {
			out = append(out, decodeDataRow(t, m.body))
		}
	}
	return out
}

// expectError asserts that msgs carries an ErrorResponse with the given
// SQLSTATE, and returns its fields.
func expectError(t *testing.T, msgs []backendMessage, code string) map[byte]string {
	t.Helper()
	m := find(t, msgs, msgErrorResponse)
	fs := errorFields(m.body)
	if fs['C'] != code {
		t.Fatalf("SQLSTATE is %q, want %q; message: %s", fs['C'], code, formatError(m.body))
	}
	return fs
}

func commandTagOf(t *testing.T, msgs []backendMessage) string {
	t.Helper()
	m := find(t, msgs, msgCommandComplete)
	f := fields{b: m.body}
	return f.cstring()
}

// --- fixtures --------------------------------------------------------------

// corpus carries the shapes the type mapping has a decision to make about:
// explicit nulls, absent fields, an empty string, containers, an integer past
// float64's mantissa, and a non-integer number.
var corpus = []string{
	`{"id":1,"name":"amy","age":30,"tier":"pro","tags":["a","b"],"meta":{"x":1}}`,
	`{"id":2,"name":"bob","age":21,"tier":"free","tags":[]}`,
	`{"id":3,"name":"cy","age":null,"tier":"pro"}`,
	`{"id":4,"name":"","age":17,"tier":"free","big":9007199254740993}`,
	`{"id":5,"name":"dee","age":45,"big":9007199254740992,"ratio":0.1}`,
	`{"id":6,"tier":"pro","age":30,"flag":true}`,
	`{"id":7,"name":"eve","age":21,"tier":"free","flag":false,"ratio":2.5}`,
}

// testDatabase builds a heap database holding docs under the given collection
// name.
func testDatabase(t testing.TB, name string, docs []string) *store.Database {
	t.Helper()
	db := &store.Database{}
	collection, err := db.CreateCollection(name, store.Options{})
	if err != nil {
		t.Fatalf("CreateCollection(%q): %v", name, err)
	}
	for i, doc := range docs {
		if _, err := collection.Put(strconv.Itoa(i+1), []byte(doc)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	return db
}

// newTestServer builds a server over the corpus in a collection named "users".
func newTestServer(t *testing.T, opts Options) *Server {
	t.Helper()
	srv := NewServer(FromDatabase(testDatabase(t, "users", corpus)), opts)
	t.Cleanup(func() {
		if err := srv.Close(); err != nil && !errors.Is(err, ErrServerClosed) {
			t.Errorf("Server.Close: %v", err)
		}
	})
	return srv
}

// connect is the whole of the usual setup: a server, a client, a completed
// startup.
func connect(t *testing.T) *testClient {
	t.Helper()
	c := dial(t, newTestServer(t, Options{}))
	c.startup(map[string]string{"user": "tester", "database": "app"})
	return c
}
