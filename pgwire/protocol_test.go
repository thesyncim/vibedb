package pgwire

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/conformance"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Given a PostgreSQL client speaking protocol 3.0 over a net.Pipe, when it
// performs each exchange the specification defines, then this server answers
// with the message sequence the specification requires and with values that
// round-trip losslessly through the declared type.
//
// The tests are organized by the promise they check rather than by the function
// they call: startup and negotiation, the simple query protocol, the extended
// query protocol, the type mapping, error classification, session commands,
// lifecycle, and concurrency.

// --- startup ---------------------------------------------------------------

func TestStartupReportsTheParametersClientsRead(t *testing.T) {
	c := connect(t)
	for _, name := range []string{
		"server_version", "server_encoding", "client_encoding",
		"standard_conforming_strings", "DateStyle", "integer_datetimes",
		"session_authorization",
	} {
		if _, ok := c.params[name]; !ok {
			t.Errorf("the server did not report ParameterStatus for %q", name)
		}
	}
	if c.params["client_encoding"] != "UTF8" {
		t.Errorf("client_encoding is %q, want UTF8", c.params["client_encoding"])
	}
	if c.params["session_authorization"] != "tester" {
		t.Errorf("session_authorization is %q, want the startup user",
			c.params["session_authorization"])
	}
	if c.pid == 0 && c.secret == 0 {
		t.Error("the server sent no BackendKeyData")
	}
}

func TestStartupRefusesSSLInTheClear(t *testing.T) {
	c := dial(t, newTestServer(t, Options{}))
	// An SSLRequest is a bare length and code with no parameters.
	var packet [8]byte
	binary.BigEndian.PutUint32(packet[0:], 8)
	binary.BigEndian.PutUint32(packet[4:], codeSSLRequest)
	c.sendRaw(packet[:])
	reply := make([]byte, 1)
	if _, err := c.br.Read(reply); err != nil {
		t.Fatalf("reading the SSL negotiation reply: %v", err)
	}
	if reply[0] != 'N' {
		t.Fatalf("SSL negotiation replied %q, want 'N' (unavailable)", reply[0])
	}
	// The connection must remain usable in the clear afterwards.
	c.startup(map[string]string{"user": "tester"})
}

func sendEncryptionRequest(c *testClient, code int32, extra []byte) {
	packet := binary.BigEndian.AppendUint32(nil, uint32(8+len(extra)))
	packet = binary.BigEndian.AppendUint32(packet, uint32(code))
	packet = append(packet, extra...)
	c.sendRaw(packet)
}

func expectEncryptionUnavailable(t *testing.T, c *testClient) {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("setting encryption-negotiation read deadline: %v", err)
	}
	reply := make([]byte, 1)
	if _, err := io.ReadFull(c.br, reply); err != nil {
		t.Fatalf("reading encryption-negotiation reply: %v", err)
	}
	if reply[0] != 'N' {
		t.Fatalf("encryption negotiation replied %q, want 'N'", reply[0])
	}
}

func expectStartupProtocolViolation(t *testing.T, c *testClient) {
	t.Helper()
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("malformed startup produced %q, want ErrorResponse", string(rune(m.tag)))
	}
	if fields := errorFields(m.body); fields['C'] != sqlstateProtocolViolation ||
		fields['S'] != "FATAL" {
		t.Fatalf("malformed startup was not a fatal protocol violation: %s",
			formatError(m.body))
	}
}

func TestStartupEncryptionNegotiationIsStrictAndFinite(t *testing.T) {
	t.Run("one fallback in either order", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			codes []int32
		}{
			{name: "SSL then GSS", codes: []int32{codeSSLRequest, codeGSSENCRequest}},
			{name: "GSS then SSL", codes: []int32{codeGSSENCRequest, codeSSLRequest}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c := dial(t, newTestServer(t, Options{}))
				for _, code := range tc.codes {
					sendEncryptionRequest(c, code, nil)
					expectEncryptionUnavailable(t, c)
				}
				c.startup(map[string]string{"user": "tester"})
			})
		}
	})

	t.Run("a mechanism cannot reset the read deadline forever", func(t *testing.T) {
		for _, code := range []int32{codeSSLRequest, codeGSSENCRequest} {
			name := "SSL"
			if code == codeGSSENCRequest {
				name = "GSS"
			}
			t.Run(name, func(t *testing.T) {
				c := dial(t, newTestServer(t, Options{ReadTimeout: -1}))
				sendEncryptionRequest(c, code, nil)
				expectEncryptionUnavailable(t, c)
				sendEncryptionRequest(c, code, nil)
				expectStartupProtocolViolation(t, c)
			})
		}
	})

	t.Run("encryption requests are exactly eight bytes", func(t *testing.T) {
		for _, code := range []int32{codeSSLRequest, codeGSSENCRequest} {
			c := dial(t, newTestServer(t, Options{}))
			sendEncryptionRequest(c, code, []byte{0})
			expectStartupProtocolViolation(t, c)
		}
	})
}

func TestStartupPacketRejectsBytesAfterItsTerminalNUL(t *testing.T) {
	c := dial(t, newTestServer(t, Options{}))
	body := binary.BigEndian.AppendUint32(nil, uint32(protocolVersion30))
	body = append(body, "user\x00tester\x00\x00"...)
	body = append(body, 0x7f)
	packet := binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))
	c.sendRaw(append(packet, body...))
	expectStartupProtocolViolation(t, c)
}

func TestStartupRefusesAnUnknownProtocolVersion(t *testing.T) {
	c := dial(t, newTestServer(t, Options{}))
	body := binary.BigEndian.AppendUint32(nil, 4<<16) // protocol 4.0
	body = append(body, 0)
	c.sendRaw(binary.BigEndian.AppendUint32(nil, uint32(len(body)+4)))
	c.sendRaw(body)
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("expected ErrorResponse, got %q", string(rune(m.tag)))
	}
	fs := errorFields(m.body)
	if fs['C'] != sqlstateFeatureNotSupported || fs['S'] != "FATAL" {
		t.Fatalf("wrong refusal for an unknown protocol version: %s", formatError(m.body))
	}
}

func TestStartupRefusesAnUnsupportedRuntimeParameter(t *testing.T) {
	c := dial(t, newTestServer(t, Options{}))
	c.sendStartup(map[string]string{"user": "tester", "role": "admin"})
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("expected the connection to be refused, got %q", string(rune(m.tag)))
	}
	fs := errorFields(m.body)
	if fs['C'] != sqlstateFeatureNotSupported {
		t.Fatalf("wrong SQLSTATE for an unsupported startup parameter: %s", formatError(m.body))
	}
	if !strings.Contains(fs['H'], "no role") {
		t.Errorf("the refusal does not explain why role cannot work: %q", fs['H'])
	}
}

func TestStartupWithoutAUserIsRefused(t *testing.T) {
	c := dial(t, newTestServer(t, Options{}))
	c.sendStartup(map[string]string{"database": "app"})
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("expected ErrorResponse, got %q", string(rune(m.tag)))
	}
	if fs := errorFields(m.body); fs['C'] != sqlstateInvalidAuthorization {
		t.Fatalf("wrong SQLSTATE for a startup with no user: %s", formatError(m.body))
	}
}

func TestStartupChecksTheDatabaseNameWhenOneIsConfigured(t *testing.T) {
	c := dial(t, newTestServer(t, Options{Database: "app"}))
	c.sendStartup(map[string]string{"user": "tester", "database": "other"})
	m := c.recv()
	if fs := errorFields(m.body); m.tag != msgErrorResponse ||
		fs['C'] != sqlstateInvalidAuthorization {
		t.Fatalf("connecting to the wrong database was not refused: %q", string(rune(m.tag)))
	}
}

// --- simple query ----------------------------------------------------------

func TestSimpleQueryReturnsRowsAndOneReadyForQuery(t *testing.T) {
	c := connect(t)
	msgs := c.query(`SELECT name, age FROM users WHERE tier = 'pro' ORDER BY age`)

	if n := countTag(msgs, msgReadyForQuery); n != 1 {
		t.Fatalf("the server sent %d ReadyForQuery messages for one Query, want exactly 1", n)
	}
	cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if len(cols) != 2 || cols[0].name != "name" || cols[1].name != "age" {
		t.Fatalf("RowDescription is %+v, want columns name and age", cols)
	}
	for _, col := range cols {
		if col.format != formatText {
			t.Errorf("column %q is described as format %d; the simple query protocol has no "+
				"way to request binary, so every column must be text", col.name, col.format)
		}
	}
	rows := rowsOf(t, msgs)
	// tier=pro is amy(30), cy(null), and the untitled document with age 30.
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3: %v", len(rows), rows)
	}
	if tag := commandTagOf(t, msgs); tag != "SELECT 3" {
		t.Fatalf("CommandComplete tag is %q, want %q", tag, "SELECT 3")
	}
}

func TestSimpleQueryLikeAndILike(t *testing.T) {
	c := connect(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{name: "LIKE", sql: `SELECT name FROM users WHERE name LIKE '_y'`, want: `"cy"`},
		{name: "ILIKE", sql: `SELECT name FROM users WHERE name ILIKE 'A%'`, want: `"amy"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msgs := c.query(tc.sql)
			if has(msgs, msgErrorResponse) {
				t.Fatalf("%s: %s", tc.sql,
					formatError(find(t, msgs, msgErrorResponse).body))
			}
			rows := rowsOf(t, msgs)
			if len(rows) != 1 || len(rows[0]) != 1 || string(rows[0][0]) != tc.want {
				t.Fatalf("%s rows = %q, want [[%s]]", tc.sql, rows, tc.want)
			}
			if tag := commandTagOf(t, msgs); tag != "SELECT 1" {
				t.Fatalf("%s tag = %q, want SELECT 1", tc.sql, tag)
			}
		})
	}
}

func TestSimpleQueryRunsSeveralStatementsAndStopsAtTheFirstError(t *testing.T) {
	c := connect(t)
	msgs := c.query(`SELECT name FROM users LIMIT 1; SELECT nope FROM absent; SELECT name FROM users LIMIT 1`)

	if n := countTag(msgs, msgReadyForQuery); n != 1 {
		t.Fatalf("got %d ReadyForQuery messages, want 1", n)
	}
	if n := countTag(msgs, msgCommandComplete); n != 1 {
		t.Fatalf("got %d CommandComplete messages; the statement after the error must not run",
			n)
	}
	expectError(t, msgs, sqlstateUndefinedTable)
}

func TestSimpleQueryDoesNotSplitOnASemicolonInsideALiteral(t *testing.T) {
	c := connect(t)
	msgs := c.query(`SELECT name FROM users WHERE name = 'a;b'`)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("a semicolon inside a string literal split the statement: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
	if tag := commandTagOf(t, msgs); tag != "SELECT 0" {
		t.Fatalf("CommandComplete tag is %q, want %q", tag, "SELECT 0")
	}
}

func TestEmptyQueryGetsEmptyQueryResponse(t *testing.T) {
	c := connect(t)
	for _, text := range []string{"", "   ", "-- just a comment", ";"} {
		msgs := c.query(text)
		if !has(msgs, msgEmptyQuery) {
			t.Errorf("query %q produced %s, want an EmptyQueryResponse", text, tags(msgs))
		}
		if has(msgs, msgCommandComplete) {
			t.Errorf("query %q produced a CommandComplete as well as an empty response", text)
		}
	}
}

func TestUnterminatedBlockCommentIsNeverAnEmptyQuery(t *testing.T) {
	c := connect(t)

	msgs := c.query("/* unterminated")
	expectError(t, msgs, sqlstateSyntaxError)
	if has(msgs, msgEmptyQuery) {
		t.Fatal("simple Query classified an unterminated block comment as empty SQL")
	}

	c.send(msgParse, parseMsg("bad-comment", "/* unterminated"))
	c.send(msgSync, nil)
	msgs = c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateSyntaxError)
	if has(msgs, msgParseComplete) {
		t.Fatal("extended Parse accepted an unterminated block comment")
	}
}

func TestSimpleQueryStatementIterationIsBounded(t *testing.T) {
	t.Run("separator run allocates nothing", func(t *testing.T) {
		src := strings.Repeat(";", 1<<20)
		allocations := testing.AllocsPerRun(10, func() {
			iter := statementIterator{src: src}
			count := 0
			for {
				_, ok, err := iter.next()
				if err != nil {
					panic(err)
				}
				if !ok {
					break
				}
				count++
			}
			if count != 1 {
				panic("all-empty query did not yield exactly one statement")
			}
		})
		if allocations != 0 {
			t.Fatalf("iterating one million separators allocated %.2f times, want zero",
				allocations)
		}
	})

	t.Run("statement count", func(t *testing.T) {
		c := connect(t)
		// Comments are non-empty input segments that classify as empty SQL.
		// They keep the test's response small while exercising the same
		// per-statement loop as ordinary SELECTs.
		msgs := c.query(strings.Repeat("/**/;", maxSimpleStatements+1))
		expectError(t, msgs, sqlstateProgramLimitExceeded)
		if n := countTag(msgs, msgEmptyQuery); n != 0 {
			t.Fatalf("executed %d statements after preflight rejected the message, want 0", n)
		}
		if n := countTag(msgs, msgReadyForQuery); n != 1 {
			t.Fatalf("bounded simple query produced %d ReadyForQuery messages, want 1", n)
		}
	})
}

func TestLiteralSelectCompatibilityShimIsBounded(t *testing.T) {
	c := connect(t)
	// This path is answered by the no-FROM compatibility shim rather than the
	// core SQL parser. It must carry the parser's structural limit itself.
	sql := "SELECT " + strings.Repeat("1,", maxResultColumns) + "1"
	msgs := c.query(sql)
	expectError(t, msgs, sqlstateProgramLimitExceeded)
	if has(msgs, msgRowDescription) || has(msgs, msgDataRow) {
		t.Fatal("an over-wide compatibility SELECT emitted partial result metadata or data")
	}
}

func TestLiteralSelectCompatibilityShimRejectsInt8Overflow(t *testing.T) {
	c := connect(t)
	msgs := c.query(`SELECT 9223372036854775808`)
	expectError(t, msgs, sqlstateNumericValueOutOfRange)
	if has(msgs, msgRowDescription) || has(msgs, msgDataRow) {
		t.Fatal("an out-of-range fixed int8 emitted partial metadata or data")
	}
}

// --- extended query --------------------------------------------------------

func TestExtendedQueryRoundTrip(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("byTier", `SELECT name, age FROM users WHERE tier = ? ORDER BY age`))
	c.send(msgDescribe, describeMsg(targetStatement, "byTier"))
	c.send(msgBind, bindMsg("p1", "byTier", nil, [][]byte{[]byte("free")}, nil))
	c.send(msgExecute, executeMsg("p1", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)

	want := []byte{msgParseComplete, msgParameterDesc, msgRowDescription,
		msgBindComplete, msgDataRow, msgDataRow, msgDataRow, msgCommandComplete,
		msgReadyForQuery}
	if got := tagBytes(msgs); !bytes.Equal(got, want) {
		t.Fatalf("message sequence is %s, want %s", tags(msgs), tags(msgsOf(want)))
	}
	pd := find(t, msgs, msgParameterDesc)
	f := fields{b: pd.body}
	if n := f.int16(); n != 1 {
		t.Fatalf("ParameterDescription declares %d parameters, want 1", n)
	}
	if oid := f.int32(); oid != 0 {
		t.Fatalf("ParameterDescription declares OID %d; a placeholder in this dialect has no "+
			"type and must be reported as unspecified", oid)
	}
}

func TestParameterDescriptionPreservesDeclaredWireOIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		oids []int32
		want []int32
	}{
		{
			name: "out of order and repeated",
			oids: []int32{oidText, oidInt8},
			want: []int32{oidText, oidInt8},
		},
		{
			name: "unspecified tail",
			oids: []int32{oidText},
			want: []int32{oidText, 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := connect(t)
			c.send(msgParse, parseMsg("typed",
				`SELECT id FROM users WHERE age = $2 AND tier = $1 AND age >= $2`,
				tc.oids...))
			c.send(msgDescribe, describeMsg(targetStatement, "typed"))
			c.send(msgSync, nil)
			msgs := c.until(msgReadyForQuery)
			if has(msgs, msgErrorResponse) {
				t.Fatalf("typed Parse/Describe failed: %s",
					formatError(find(t, msgs, msgErrorResponse).body))
			}
			got := decodeParameterDescription(t,
				find(t, msgs, msgParameterDesc).body)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("ParameterDescription OIDs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDeclaredJSONParametersUseJSONScalarSemantics(t *testing.T) {
	docs := []string{
		`{"id":1,"value":"21"}`,
		`{"id":2,"value":"A\nB"}`,
		`{"id":3,"value":21}`,
		`{"id":4,"value":true}`,
		`{"id":5,"value":null}`,
	}
	for _, tc := range []struct {
		name     string
		oid      int32
		format   int16
		raw      []byte
		wantID   string
		wantCode string
	}{
		{name: "quoted string", oid: oidJSON, raw: []byte(`"21"`), wantID: "1"},
		{name: "escaped string", oid: oidJSON, raw: []byte(`"A\nB"`), wantID: "2"},
		{name: "exact number", oid: oidJSON, raw: []byte(`21`), wantID: "3"},
		{name: "boolean", oid: oidJSON, raw: []byte(`true`), wantID: "4"},
		{name: "null", oid: oidJSON, raw: []byte(`null`)},
		{name: "jsonb text", oid: oidJSONB, raw: []byte(` "21" `), wantID: "1"},
		{
			name:   "json binary",
			oid:    oidJSON,
			format: formatBinary,
			raw:    []byte(`"21"`),
			wantID: "1",
		},
		{
			name:   "jsonb binary",
			oid:    oidJSONB,
			format: formatBinary,
			raw:    append([]byte{1}, []byte(`"21"`)...),
			wantID: "1",
		},
		{
			name:     "jsonb binary version",
			oid:      oidJSONB,
			format:   formatBinary,
			raw:      append([]byte{2}, []byte(`"21"`)...),
			wantCode: sqlstateInvalidParameterValue,
		},
		{
			name:     "malformed scalar",
			oid:      oidJSON,
			raw:      []byte(`"unterminated`),
			wantCode: sqlstateInvalidParameterValue,
		},
		{
			name:     "array refused",
			oid:      oidJSON,
			raw:      []byte(`[1,2]`),
			wantCode: sqlstateFeatureNotSupported,
		},
		{
			name:     "object refused",
			oid:      oidJSONB,
			raw:      []byte(`{"x":1}`),
			wantCode: sqlstateFeatureNotSupported,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := NewServer(testDatabase(t, "docs", docs),
				Options{Auth: Trust()})
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			t.Cleanup(func() { _ = srv.Close() })
			c := dial(t, srv)
			c.startup(map[string]string{"user": "tester"})
			c.send(msgParse, parseMsg("", `SELECT id FROM docs WHERE value = $1`, tc.oid))
			c.send(msgBind, bindMsg("", "", []int16{tc.format}, [][]byte{tc.raw}, nil))
			c.send(msgExecute, executeMsg("", 0))
			c.send(msgSync, nil)
			msgs := c.until(msgReadyForQuery)
			if tc.wantCode != "" {
				expectError(t, msgs, tc.wantCode)
				if has(msgs, msgDataRow) {
					t.Fatal("a rejected JSON Bind emitted a row")
				}
				return
			}
			if has(msgs, msgErrorResponse) {
				t.Fatalf("declared JSON scalar failed: %s",
					formatError(find(t, msgs, msgErrorResponse).body))
			}
			rows := rowsOf(t, msgs)
			if tc.wantID == "" {
				if len(rows) != 0 {
					t.Fatalf("JSON null matched %v, want no SQL equality match", rows)
				}
				return
			}
			if len(rows) != 1 || string(rows[0][0]) != tc.wantID {
				t.Fatalf("declared JSON scalar returned %v, want id %s", rows, tc.wantID)
			}
		})
	}
}

func TestExtendedQueryPortalSuspension(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SELECT id FROM users ORDER BY id`))
	c.send(msgBind, bindMsg("", "", nil, nil, nil))
	c.send(msgExecute, executeMsg("", 2))
	c.send(msgExecute, executeMsg("", 3))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if n := countTag(msgs, msgPortalSuspended); n != 2 {
		t.Fatalf("three Execute messages produced %d suspensions, want 2: %s", n, tags(msgs))
	}
	rows := rowsOf(t, msgs)
	if len(rows) != 7 {
		t.Fatalf("resumed portal produced %d rows, want all 7", len(rows))
	}
	for i := range rows {
		if string(rows[i][0]) != strconv.Itoa(i+1) {
			t.Fatalf("row %d = %q, want %d", i, rows[i][0], i+1)
		}
	}
	if tag := commandTagOf(t, msgs); tag != "SELECT 7" {
		t.Fatalf("CommandComplete tag is %q, want portal total SELECT 7", tag)
	}

	// Sync commits the implicit transaction and closes its non-holdable portal.
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs = c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateInvalidCursorName)
}

func TestSimpleQueryDestroysUnnamedExtendedObjects(t *testing.T) {
	c := connect(t)
	// A named portal can be built from the unnamed statement, and the unnamed
	// portal can independently be built from a named statement. A simple Query
	// destroys both unnamed objects; destroying the statement also destroys
	// every portal that depends on it.
	c.send(msgParse, parseMsg("", `SELECT id FROM users`))
	c.send(msgBind, bindMsg("fromUnnamed", "", nil, nil, nil))
	c.send(msgParse, parseMsg("named", `SELECT id FROM users`))
	c.send(msgBind, bindMsg("", "named", nil, nil, nil))
	c.send(msgSync, nil)
	if msgs := c.until(msgReadyForQuery); has(msgs, msgErrorResponse) {
		t.Fatalf("setup failed: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}

	if msgs := c.query(`SELECT 1`); has(msgs, msgErrorResponse) {
		t.Fatalf("simple Query failed: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}

	c.send(msgDescribe, describeMsg(targetStatement, ""))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateInvalidStatementName)

	c.send(msgDescribe, describeMsg(targetPortal, "fromUnnamed"))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateInvalidCursorName)

	c.send(msgDescribe, describeMsg(targetPortal, ""))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateInvalidCursorName)

	// The named statement did not depend on either unnamed object and remains.
	c.send(msgDescribe, describeMsg(targetStatement, "named"))
	c.send(msgSync, nil)
	if msgs := c.until(msgReadyForQuery); has(msgs, msgErrorResponse) {
		t.Fatalf("simple Query destroyed a named statement: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
}

func TestASuspendedPortalIsRefusedAfterAnotherExecution(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("a", `SELECT id FROM users ORDER BY id`))
	c.send(msgParse, parseMsg("b", `SELECT id FROM users ORDER BY id`))
	c.send(msgBind, bindMsg("pa", "a", nil, nil, nil))
	c.send(msgBind, bindMsg("pb", "b", nil, nil, nil))
	c.send(msgExecute, executeMsg("pa", 1))
	c.send(msgExecute, executeMsg("pb", 0))
	c.send(msgExecute, executeMsg("pa", 1))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	fs := expectError(t, msgs, sqlstateObjectNotInPrereqState)
	if !strings.Contains(fs['H'], "one result set per connection") {
		t.Errorf("the refusal does not explain the one-result-set rule: %q", fs['H'])
	}
}

func TestAnErrorDiscardsMessagesUntilSync(t *testing.T) {
	c := connect(t)
	// Bind names a statement that was never parsed; the Execute that follows
	// must not run, and the Sync must still produce exactly one ReadyForQuery.
	c.send(msgBind, bindMsg("", "missing", nil, nil, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgParse, parseMsg("x", `SELECT name FROM users`))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)

	expectError(t, msgs, sqlstateInvalidStatementName)
	if n := countTag(msgs, msgErrorResponse); n != 1 {
		t.Fatalf("got %d ErrorResponse messages; only the first failure should be reported", n)
	}
	if has(msgs, msgParseComplete) {
		t.Fatal("a Parse after an error was executed instead of being discarded")
	}
	if n := countTag(msgs, msgReadyForQuery); n != 1 {
		t.Fatalf("got %d ReadyForQuery messages after Sync, want 1", n)
	}
	// The session recovers: the discarded Parse never happened, so it works now.
	c.send(msgParse, parseMsg("x", `SELECT name FROM users`))
	c.send(msgSync, nil)
	if msgs := c.until(msgReadyForQuery); !has(msgs, msgParseComplete) {
		t.Fatalf("the session did not recover after Sync: %s", tags(msgs))
	}
}

func TestNamedStatementsAndPortalsHaveIndependentLifetimes(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("s", `SELECT id FROM users`))
	c.send(msgParse, parseMsg("s", `SELECT id FROM users`))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateDuplicateStatement)

	// The unnamed statement, by contrast, is replaceable.
	c.send(msgParse, parseMsg("", `SELECT id FROM users`))
	c.send(msgParse, parseMsg("", `SELECT name FROM users`))
	c.send(msgSync, nil)
	if msgs := c.until(msgReadyForQuery); countTag(msgs, msgParseComplete) != 2 {
		t.Fatalf("re-parsing the unnamed statement failed: %s", tags(msgs))
	}

	// Closing a statement drops the portals built from it.
	c.send(msgBind, bindMsg("p", "s", nil, nil, nil))
	c.send(msgClose, closeMsg(targetStatement, "s"))
	c.send(msgExecute, executeMsg("p", 0))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateInvalidCursorName)
}

func TestDescribeAPortalReportsTheFormatsItWillUse(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SELECT name FROM users LIMIT 1`))
	c.send(msgBind, bindMsg("", "", nil, nil, []int16{formatBinary}))
	c.send(msgDescribe, describeMsg(targetPortal, ""))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if cols[0].format != formatBinary {
		t.Fatalf("Describe of a portal bound for binary reported format %d", cols[0].format)
	}
}

func TestCloseOfAMissingObjectSucceeds(t *testing.T) {
	c := connect(t)
	c.send(msgClose, closeMsg(targetStatement, "never-created"))
	c.send(msgClose, closeMsg(targetPortal, "never-created"))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if countTag(msgs, msgCloseComplete) != 2 || has(msgs, msgErrorResponse) {
		t.Fatalf("closing an object that does not exist must succeed: %s", tags(msgs))
	}
}

func TestDescribeAStatementWithNoRowsGivesNoData(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SET application_name = 'x'`))
	c.send(msgDescribe, describeMsg(targetStatement, ""))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if !has(msgs, msgNoData) {
		t.Fatalf("Describe of a SET reported %s, want NoData", tags(msgs))
	}
}

// --- the type mapping ------------------------------------------------------

func TestProjectedColumnsAreDeclaredJSONAndRoundTrip(t *testing.T) {
	c := connect(t)
	msgs := c.query(`SELECT id, name, age, tags, meta, big, ratio, flag FROM users ORDER BY id`)
	cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	for _, col := range cols {
		if col.oid != oidJSON {
			t.Fatalf("column %q is declared OID %d, want json (%d)", col.name, col.oid, oidJSON)
		}
		if col.size != -1 {
			t.Errorf("column %q declares a fixed size %d for a variable-width type",
				col.name, col.size)
		}
	}
	rows := rowsOf(t, msgs)
	for i, row := range rows {
		for j, value := range row {
			if value == nil {
				continue
			}
			if !json.Valid(value) {
				t.Fatalf("row %d column %q is not valid JSON: %q", i, cols[j].name, value)
			}
		}
	}
	// The exactness claim, checked rather than asserted: 9007199254740993 must
	// arrive as its own digits, and 9007199254740992 as different digits, even
	// though both are one float64.
	big := map[string]string{}
	for _, row := range rows {
		if row[5] != nil {
			big[string(row[0])] = string(row[5])
		}
	}
	if big["4"] != "9007199254740993" || big["5"] != "9007199254740992" {
		t.Fatalf("exact decimal was lost in transit: %v", big)
	}
	if len(rows) != 7 {
		t.Fatalf("got %d rows, want 7", len(rows))
	}
	// A string is JSON-quoted, which is what keeps it distinguishable from a
	// number, and the empty string stays distinguishable from NULL.
	names := map[string]string{}
	for _, row := range rows {
		if row[1] == nil {
			names[string(row[0])] = "<null>"
			continue
		}
		names[string(row[0])] = string(row[1])
	}
	if names["1"] != `"amy"` {
		t.Fatalf("a string column arrived as %q, want JSON-quoted", names["1"])
	}
	if names["4"] != `""` {
		t.Fatalf("the empty string arrived as %q, want an empty JSON string", names["4"])
	}
	if names["6"] != "<null>" {
		t.Fatalf("an absent path arrived as %q, want NULL", names["6"])
	}
}

func TestBinaryFormatForJSONIsTheSameBytesAsText(t *testing.T) {
	c := connect(t)
	textRows := binaryOrTextRows(t, c, formatText)
	binaryRows := binaryOrTextRows(t, c, formatBinary)
	if len(textRows) != len(binaryRows) {
		t.Fatalf("row counts differ between formats: %d and %d", len(textRows), len(binaryRows))
	}
	for i := range textRows {
		for j := range textRows[i] {
			if !bytes.Equal(textRows[i][j], binaryRows[i][j]) {
				t.Fatalf("row %d column %d differs between formats: text %q, binary %q",
					i, j, textRows[i][j], binaryRows[i][j])
			}
		}
	}
}

func binaryOrTextRows(t *testing.T, c *testClient, format int16) []decodedRow {
	t.Helper()
	c.send(msgParse, parseMsg("", `SELECT id, name, tags, ratio FROM users ORDER BY id`))
	c.send(msgBind, bindMsg("", "", nil, nil, []int16{format}))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	return rowsOf(t, c.until(msgReadyForQuery))
}

func TestCountIsDeclaredInt8AndEncodesBinaryCorrectly(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SELECT tier, COUNT(*) FROM users GROUP BY tier ORDER BY tier`))
	c.send(msgBind, bindMsg("", "", nil, nil, []int16{formatText, formatBinary}))
	c.send(msgDescribe, describeMsg(targetPortal, ""))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)

	cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if cols[1].oid != oidInt8 || cols[1].size != 8 {
		t.Fatalf("COUNT is declared OID %d size %d, want int8 (%d) size 8",
			cols[1].oid, cols[1].size, oidInt8)
	}
	total := int64(0)
	for _, row := range rowsOf(t, msgs) {
		if len(row[1]) != 8 {
			t.Fatalf("a binary int8 value is %d bytes, want 8: %q", len(row[1]), row[1])
		}
		total += int64(binary.BigEndian.Uint64(row[1]))
	}
	if total != int64(len(corpus)) {
		t.Fatalf("the group counts sum to %d, want %d", total, len(corpus))
	}
}

func TestNullAndAbsentAreBothNULL(t *testing.T) {
	c := connect(t)
	msgs := c.query(`SELECT id, age FROM users WHERE age IS NULL ORDER BY id`)
	rows := rowsOf(t, msgs)
	// Document 3 has an explicit null age; document 6 has one and 2 does not —
	// only 3 holds an explicit null, so IS NULL selects it alone among the
	// documents that mention age, and the engine treats absent identically.
	if len(rows) == 0 {
		t.Fatal("IS NULL matched nothing")
	}
	for _, row := range rows {
		if row[1] != nil {
			t.Fatalf("a row selected by IS NULL carried a non-NULL value %q", row[1])
		}
	}
}

// --- bound parameters ------------------------------------------------------

func TestUntypedTextParametersAreReadAsJSONScalars(t *testing.T) {
	c := connect(t)
	cases := []struct {
		sql   string
		param string
		want  int
	}{
		{`SELECT id FROM users WHERE age = ?`, "30", 2},
		{`SELECT id FROM users WHERE big = ?`, "9007199254740993", 1},
		{`SELECT id FROM users WHERE ratio = ?`, "1e-1", 1},
		{`SELECT id FROM users WHERE tier = ?`, "pro", 3},
		{`SELECT id FROM users WHERE flag = ?`, "true", 1},
	}
	for _, tc := range cases {
		c.send(msgParse, parseMsg("", tc.sql))
		c.send(msgBind, bindMsg("", "", nil, [][]byte{[]byte(tc.param)}, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
		msgs := c.until(msgReadyForQuery)
		if got := countTag(msgs, msgDataRow); got != tc.want {
			t.Errorf("%s bound to %q returned %d rows, want %d: %s",
				tc.sql, tc.param, got, tc.want, tags(msgs))
		}
	}
}

func TestADeclaredParameterTypeDisambiguatesTheBinding(t *testing.T) {
	c := connect(t)
	// Declared text: the digits stay a string, and no numeric age matches.
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE age = ?`, oidText))
	c.send(msgBind, bindMsg("", "", nil, [][]byte{[]byte("30")}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	if n := countTag(c.until(msgReadyForQuery), msgDataRow); n != 0 {
		t.Fatalf("a parameter declared text matched %d numeric rows, want 0", n)
	}
	// Declared int8 in binary format: unambiguous, and matches.
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE age = ?`, oidInt8))
	c.send(msgBind, bindMsg("", "", []int16{formatBinary},
		[][]byte{binary.BigEndian.AppendUint64(nil, 30)}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	if n := countTag(c.until(msgReadyForQuery), msgDataRow); n != 2 {
		t.Fatalf("a binary int8 parameter matched %d rows, want 2", n)
	}
}

func TestLikeBindingErrorsCarryStableSQLStates(t *testing.T) {
	t.Run("non-string pattern", func(t *testing.T) {
		c := connect(t)
		c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE name LIKE ?`, oidInt8))
		c.send(msgBind, bindMsg("", "", nil, [][]byte{[]byte("30")}, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
		expectError(t, c.until(msgReadyForQuery), sqlstateDatatypeMismatch)
	})
	t.Run("malformed bound pattern", func(t *testing.T) {
		c := connect(t)
		c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE name LIKE ?`, oidText))
		c.send(msgBind, bindMsg("", "", nil, [][]byte{[]byte(`a\`)}, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
		expectError(t, c.until(msgReadyForQuery), sqlstateInvalidParameterValue)
	})
}

func TestANullParameterBinds(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE age = ?`))
	c.send(msgBind, bindMsg("", "", nil, [][]byte{nil}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("binding NULL failed: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}
	if n := countTag(msgs, msgDataRow); n != 0 {
		t.Fatalf("comparison against NULL returned %d rows, want 0 under SQL's three-valued "+
			"logic", n)
	}
}

func TestAWrongParameterCountIsRefusedBeforeExecution(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE age = ?`))
	c.send(msgBind, bindMsg("", "", nil, nil, nil))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateProtocolViolation)
}

func TestBinaryParameterWithNoDeclaredTypeIsRefused(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE age = ?`))
	c.send(msgBind, bindMsg("", "", []int16{formatBinary}, [][]byte{{0, 0, 0, 30}}, nil))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateFeatureNotSupported)
}

func TestNonFiniteBinaryFloatParametersAreRefusedAtBind(t *testing.T) {
	for _, tc := range []struct {
		name string
		oid  int32
		raw  []byte
	}{
		{name: "float4 NaN", oid: oidFloat4,
			raw: binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(math.NaN())))},
		{name: "float8 positive infinity", oid: oidFloat8,
			raw: binary.BigEndian.AppendUint64(nil, math.Float64bits(math.Inf(1)))},
		{name: "float8 negative infinity", oid: oidFloat8,
			raw: binary.BigEndian.AppendUint64(nil, math.Float64bits(math.Inf(-1)))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := connect(t)
			c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE ratio = ?`, tc.oid))
			c.send(msgBind, bindMsg("", "", []int16{formatBinary}, [][]byte{tc.raw}, nil))
			c.send(msgSync, nil)
			expectError(t, c.until(msgReadyForQuery), sqlstateInvalidParameterValue)
		})
	}
}

func TestTextInputsMustMatchTheReportedUTF8Encoding(t *testing.T) {
	t.Run("SQL", func(t *testing.T) {
		c := connect(t)
		expectError(t, c.query("SELECT "+string([]byte{0xff})+" FROM users"),
			sqlstateCharacterNotInRepertoire)
	})
	t.Run("parameter", func(t *testing.T) {
		c := connect(t)
		c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE name = ?`))
		c.send(msgBind, bindMsg("", "", nil, [][]byte{{0xff}}, nil))
		c.send(msgSync, nil)
		expectError(t, c.until(msgReadyForQuery), sqlstateCharacterNotInRepertoire)
	})
	t.Run("startup", func(t *testing.T) {
		c := dial(t, newTestServer(t, Options{}))
		c.sendStartup(map[string]string{"user": string([]byte{0xff})})
		m := c.recv()
		if fs := errorFields(m.body); m.tag != msgErrorResponse ||
			fs['C'] != sqlstateCharacterNotInRepertoire || fs['S'] != "FATAL" {
			t.Fatalf("invalid startup UTF-8 was not refused: %s", formatError(m.body))
		}
	})
}

func TestSimpleQueryExplainAndAnalyzeReturnOnePlanRow(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "explain",
			sql:  `EXPLAIN SELECT name FROM users WHERE tier = 'pro'`,
			want: []string{`"node":"scan"`, `"collection":"users"`},
		},
		{
			name: "explain analyze",
			sql: `EXPLAIN ANALYZE SELECT name FROM users ` +
				`WHERE tier = 'pro' ORDER BY name LIMIT 2`,
			want: []string{`"node":"scan"`, `"collection":"users"`, `"analyze":{`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := connect(t)
			msgs := c.query(tc.sql)
			wantTags := []byte{
				msgRowDescription, msgDataRow, msgCommandComplete, msgReadyForQuery,
			}
			if got := tagBytes(msgs); !bytes.Equal(got, wantTags) {
				t.Fatalf("message sequence is %s, want %s",
					tags(msgs), tags(msgsOf(wantTags)))
			}

			cols := decodeRowDescription(t, msgs[0].body)
			if len(cols) != 1 || cols[0].name != "QUERY PLAN" {
				t.Fatalf("RowDescription is %+v, want one QUERY PLAN column", cols)
			}
			rows := rowsOf(t, msgs)
			if len(rows) != 1 || len(rows[0]) != 1 {
				t.Fatalf("EXPLAIN rows = %q, want one row with one plan", rows)
			}
			var plan string
			if err := json.Unmarshal(rows[0][0], &plan); err != nil {
				t.Fatalf("QUERY PLAN wire value is not a JSON string: %q: %v",
					rows[0][0], err)
			}
			if !json.Valid([]byte(plan)) {
				t.Fatalf("QUERY PLAN is not valid JSON: %s", plan)
			}
			for _, want := range tc.want {
				if !strings.Contains(plan, want) {
					t.Errorf("QUERY PLAN missing %s: %s", want, plan)
				}
			}
			if got := commandTagOf(t, msgs); got != "SELECT 1" {
				t.Fatalf("CommandComplete tag = %q, want SELECT 1", got)
			}
		})
	}
}

// --- error classification --------------------------------------------------

func TestErrorClassification(t *testing.T) {
	c := connect(t)
	cases := []struct {
		sql  string
		code string
	}{
		{`SELECT name FROM nosuch`, sqlstateUndefinedTable},
		{`SELECT FROM users`, sqlstateSyntaxError},
		{`SELECT name FROM users WHERE name LIKE 'a%' ESCAPE '!'`, sqlstateSyntaxError},
		{`INSERT INTO users VALUES (1)`, sqlstateSyntaxError},
		{`CREATE TABLE t (a int)`, sqlstateInternalError},
		{`COPY users TO STDOUT`, sqlstateFeatureNotSupported},
		{`DECLARE c CURSOR FOR SELECT 1`, sqlstateFeatureNotSupported},
		{`WITH RECURSIVE x(id) AS (` +
			`SELECT id FROM users UNION ALL SELECT id FROM x` +
			`) SEARCH DEPTH FIRST BY id SET ord SELECT id FROM x`, sqlstateFeatureNotSupported},
		{`banana`, sqlstateSyntaxError},
		{`SET statement_timeout = 100`, sqlstateFeatureNotSupported},
		{`SET search_path = private`, sqlstateInvalidParameterValue},
		{`SHOW nonexistent_setting`, sqlstateUndefinedObject},
	}
	for _, tc := range cases {
		msgs := c.query(tc.sql)
		expectErrorSoft(t, msgs, tc.code, tc.sql)
	}
}

func TestUnsupportedSQLTaxonomyMatchesDatabaseSQL(t *testing.T) {
	db, err := stdsql.Open("vibedb", filepath.Join(t.TempDir(), "unsupported.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := connect(t)
	for _, tc := range conformance.UnsupportedSQLCases {
		t.Run(tc.ID, func(t *testing.T) {
			text := tc.Statement
			reason := tc.ReasonContains
			if tc.ID == "cte" {
				// Ordinary and bounded recursive CTE execution are supported.
				// Keep this cross-adapter taxonomy case on a valid standard
				// recursive SEARCH clause that has no physical lowering yet.
				text = `WITH RECURSIVE x(id) AS (` +
					`SELECT id FROM docs UNION ALL SELECT id FROM x` +
					`) SEARCH DEPTH FIRST BY id SET ord SELECT id FROM x`
				reason = "SEARCH"
			}
			statement, prepareErr := db.Prepare(text)
			if statement != nil {
				_ = statement.Close()
				t.Fatal("database/sql prepared an unsupported statement")
			}
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(prepareErr, &unsupported) {
				t.Fatalf("database/sql error = %T %v, want typed feature refusal",
					prepareErr, prepareErr)
			}
			if !strings.Contains(unsupported.Msg, reason) {
				t.Fatalf("database/sql reason = %q, want %q", unsupported.Msg, reason)
			}
			fields := expectError(t, c.query(text), sqlstateFeatureNotSupported)
			if fields['M'] != unsupported.Msg {
				t.Fatalf("pgwire reason = %q, database/sql reason = %q",
					fields['M'], unsupported.Msg)
			}
		})
	}
}

func TestASyntaxErrorCarriesAPosition(t *testing.T) {
	c := connect(t)
	sql := `SELECT name FROM users WHERE name LIKE 'a%' ESCAPE '!'`
	fs := expectError(t, c.query(sql), sqlstateSyntaxError)
	pos, err := strconv.Atoi(fs['P'])
	if err != nil {
		t.Fatalf("the error carries no usable position %q: %v", fs['P'], err)
	}
	// The position is 1-based, so it indexes the statement text directly.
	if pos < 1 || pos > len(sql)+1 {
		t.Fatalf("position %d is outside the statement", pos)
	}
	if !strings.HasPrefix(sql[pos-1:], "ESCAPE") {
		t.Fatalf("position %d points at %q, want the ESCAPE keyword", pos, sql[pos-1:])
	}
	if !strings.Contains(fs['M'], "ESCAPE") {
		t.Errorf("the message does not name the refused construct: %q", fs['M'])
	}
}

func TestAPositionCountsCharactersNotBytes(t *testing.T) {
	// A non-ASCII quoted identifier before the failure makes byte and character
	// offsets disagree; the protocol's P field counts characters.
	sql := `SELECT "é" FROM users WHERE "é" LIKE 'x' ESCAPE '!'`
	_, err := sqlast.Parse(sql)
	if err == nil {
		t.Fatal("expected the statement to be refused")
	}
	pg := asPGErrorIn(err, sql)
	runes := []rune(sql)
	if pg.position < 1 || pg.position > len(runes)+1 {
		t.Fatalf("position %d is outside the statement's %d characters", pg.position, len(runes))
	}
	if !strings.HasPrefix(string(runes[pg.position-1:]), "ESCAPE") {
		t.Fatalf("character position %d points at %q, want ESCAPE",
			pg.position, string(runes[pg.position-1:]))
	}
}

// TestCatalogQueriesAreRefused checks the claim doc.go makes about psql and BI
// tools rather than asserting it.
//
// Each statement below is the shape a real catalog probe takes — psql's \d, a
// JDBC DatabaseMetaData column lookup, and an ORM's table list — and each is
// refused by the parser for a reason this dialect states. If the parser ever
// grows the constructs they need, this test fails and the documentation gets
// revisited instead of quietly becoming wrong.
func TestCatalogQueriesAreRefused(t *testing.T) {
	probes := map[string]string{
		"psql \\d": `SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c ` +
			`LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ` +
			`WHERE c.relname OPERATOR(pg_catalog.~) '^(users)$' COLLATE pg_catalog.default ` +
			`AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3`,
		"psql \\d columns": `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) ` +
			`FROM pg_catalog.pg_attribute a WHERE a.attrelid = '16384' AND a.attnum > 0 ` +
			`AND NOT a.attisdropped ORDER BY a.attnum`,
		"JDBC getColumns": `SELECT n.nspname, c.relname, a.attname, a.atttypid, ` +
			`a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull ` +
			`FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) ` +
			`JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid) ` +
			`JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) WHERE c.relkind in ('r','v')`,
		"information_schema": `SELECT table_name FROM information_schema.tables ` +
			`WHERE table_schema = ANY (current_schemas(false))`,
		"regclass cast": `SELECT 'users'::regclass::oid`,
	}
	for name, sql := range probes {
		_, err := sqlast.Parse(sql)
		if err == nil {
			t.Errorf("%s parsed; doc.go claims this dialect refuses catalog queries", name)
			continue
		}
		var parse *sqlast.ParseError
		if !errors.As(err, &parse) {
			t.Errorf("%s was refused with %T, want a positioned *sql.ParseError", name, err)
			continue
		}
		t.Logf("%s refused at %d:%d: %s", name, parse.Line, parse.Col, parse.Msg)
	}
}

// --- session commands ------------------------------------------------------

func TestSessionCommandShimsRejectMalformedOrTrailingSQL(t *testing.T) {
	c := connect(t)
	for _, sql := range []string{
		`SHOW application_name trailing`,
		`RESET application_name trailing`,
		`DISCARD`,
		`DISCARD PLANS`,
		`DISCARD ALL trailing`,
		`SET TIME ZONE`,
		`SET NAMES`,
		`SET application_name = 'x' /* unterminated`,
	} {
		expectErrorSoft(t, c.query(sql), sqlstateSyntaxError, sql)
	}

	// Simple Query legitimately splits this text into two statements. Extended
	// Parse, however, must represent exactly one statement and must not let the
	// permissive SET compatibility parser absorb a second command as its value.
	c.send(msgParse, parseMsg("two-statements",
		`SET application_name = 'changed'; SHOW application_name`))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateSyntaxError)
	if has(msgs, msgParseComplete) {
		t.Fatal("extended Parse accepted two statements through the SET shim")
	}

	rows := rowsOf(t, c.query(`SHOW application_name`))
	if len(rows) != 1 || len(rows[0][0]) != 0 {
		t.Fatalf("a rejected SET changed application_name to %v", rows)
	}
}

func TestSetAcceptsWhatItCanAndRefusesTheRest(t *testing.T) {
	c := connect(t)
	for _, sql := range []string{
		`SET extra_float_digits = 3`,
		`SET application_name = 'test'`,
		`SET client_encoding TO 'UTF8'`,
		`SET SESSION DateStyle = 'ISO, MDY'`,
		`SET TIME ZONE 'UTC'`,
		`SET NAMES 'UTF8'`,
		`SET standard_conforming_strings = on`,
	} {
		msgs := c.query(sql)
		if has(msgs, msgErrorResponse) {
			t.Errorf("%s was refused: %s", sql,
				formatError(find(t, msgs, msgErrorResponse).body))
			continue
		}
		if tag := commandTagOf(t, msgs); tag != "SET" {
			t.Errorf("%s completed with tag %q, want SET", sql, tag)
		}
	}
	// A change to a reported parameter is announced.
	msgs := c.query(`SET application_name = 'announced'`)
	status := find(t, msgs, msgParameterStatus)
	f := fields{b: status.body}
	if name, value := f.cstring(), f.cstring(); name != "application_name" || value != "announced" {
		t.Errorf("ParameterStatus reported %q=%q after the SET", name, value)
	}
	// A value this server cannot serve is refused rather than accepted.
	expectError(t, c.query(`SET client_encoding = 'LATIN1'`), sqlstateInvalidParameterValue)
}

func TestShowReportsTheSessionValue(t *testing.T) {
	c := connect(t)
	c.query(`SET application_name = 'shown'`)
	msgs := c.query(`SHOW application_name`)
	rows := rowsOf(t, msgs)
	if len(rows) != 1 || string(rows[0][0]) != "shown" {
		t.Fatalf("SHOW returned %v, want the value just set", rows)
	}
	cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if cols[0].oid != oidText {
		t.Fatalf("SHOW's column is declared OID %d, want text (%d)", cols[0].oid, oidText)
	}
	if tag := commandTagOf(t, msgs); tag != "SHOW" {
		t.Fatalf("SHOW completed with tag %q", tag)
	}
	if n := len(rowsOf(t, c.query(`SHOW ALL`))); n == 0 {
		t.Fatal("SHOW ALL returned no rows")
	}
}

func TestResetRestoresTheInitialValue(t *testing.T) {
	c := connect(t)
	c.query(`SET application_name = 'temporary'`)
	c.query(`RESET application_name`)
	rows := rowsOf(t, c.query(`SHOW application_name`))
	if len(rows) != 1 || len(rows[0][0]) != 0 {
		t.Fatalf("RESET left application_name as %v", rows)
	}
	c.query(`SET application_name = 'again'`)
	c.query(`RESET ALL`)
	rows = rowsOf(t, c.query(`SHOW application_name`))
	if len(rows[0][0]) != 0 {
		t.Fatalf("RESET ALL left application_name as %q", rows[0][0])
	}
}

func TestTheFixedSelectShimAnswersAHandshake(t *testing.T) {
	c := connect(t)
	cases := []struct {
		sql  string
		want string
	}{
		{`SELECT 1`, "1"},
		{`SELECT 1 AS ping`, "1"},
		{`SELECT version()`, versionString},
		{`SELECT current_database()`, "app"},
		{`SELECT current_schema()`, "public"},
		{`SELECT current_user`, "tester"},
		{`SELECT pg_catalog.version()`, versionString},
		{`SELECT 'lib/pq ping test'`, "lib/pq ping test"},
	}
	for _, tc := range cases {
		msgs := c.query(tc.sql)
		if has(msgs, msgErrorResponse) {
			t.Errorf("%s failed: %s", tc.sql,
				formatError(find(t, msgs, msgErrorResponse).body))
			continue
		}
		rows := rowsOf(t, msgs)
		if len(rows) != 1 || string(rows[0][0]) != tc.want {
			t.Errorf("%s returned %v, want %q", tc.sql, rows, tc.want)
		}
	}
	// A SELECT with a FROM is not the shim's. It reaches scalar lowering and
	// therefore has the collection's cardinality, not the shim's singleton
	// handshake result.
	if _, recognized, err := (shimFunctions{}).parseFixedSelect(`SELECT 1 FROM users`); err != nil || recognized {
		t.Fatalf("fixed SELECT routing = recognized %v, error %v", recognized, err)
	}
	stored := c.query(`SELECT 1 FROM users`)
	if has(stored, msgErrorResponse) {
		t.Fatalf("stored constant SELECT failed: %s",
			formatError(find(t, stored, msgErrorResponse).body))
	}
	rows := rowsOf(t, stored)
	if len(rows) != len(corpus) || commandTagOf(t, stored) != "SELECT 7" {
		t.Fatalf("stored constant SELECT rows/tag = %q/%q, want %d/SELECT 7",
			rows, commandTagOf(t, stored), len(corpus))
	}
	for row := range rows {
		if len(rows[row]) != 1 || string(rows[row][0]) != "1" {
			t.Fatalf("stored constant SELECT row %d = %q, want [1]", row, rows[row])
		}
	}
}

func TestFixedBooleanUsesPostgreSQLBoolInTextAndBinary(t *testing.T) {
	c := connect(t)

	text := c.query(`SELECT TRUE`)
	cols := decodeRowDescription(t, find(t, text, msgRowDescription).body)
	if len(cols) != 1 || cols[0].oid != oidBool || cols[0].size != 1 {
		t.Fatalf("SELECT TRUE described %+v, want one bool column", cols)
	}
	if rows := rowsOf(t, text); len(rows) != 1 ||
		len(rows[0]) != 1 || string(rows[0][0]) != "t" {
		t.Fatalf("SELECT TRUE text rows = %v, want t", rows)
	}

	c.send(msgParse, parseMsg("", `SELECT FALSE`))
	c.send(msgBind, bindMsg("", "", nil, nil, []int16{formatBinary}))
	c.send(msgDescribe, describeMsg(targetPortal, ""))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	binaryMessages := c.until(msgReadyForQuery)
	binaryCols := decodeRowDescription(
		t, find(t, binaryMessages, msgRowDescription).body)
	if len(binaryCols) != 1 || binaryCols[0].oid != oidBool ||
		binaryCols[0].format != formatBinary {
		t.Fatalf("SELECT FALSE binary described %+v, want binary bool", binaryCols)
	}
	if rows := rowsOf(t, binaryMessages); len(rows) != 1 ||
		len(rows[0]) != 1 || !bytes.Equal(rows[0][0], []byte{0}) {
		t.Fatalf("SELECT FALSE binary rows = %v, want one zero byte", rows)
	}
}

func TestDiscardActuallyDiscards(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("kept", `SELECT id FROM users`))
	c.send(msgSync, nil)
	c.until(msgReadyForQuery)

	msgs := c.query(`DISCARD ALL`)
	if tag := commandTagOf(t, msgs); tag != "DISCARD" {
		t.Fatalf("DISCARD completed with tag %q", tag)
	}
	c.send(msgDescribe, describeMsg(targetStatement, "kept"))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateInvalidStatementName)
}

// --- lifecycle and concurrency ---------------------------------------------

func TestTerminateEndsTheSessionCleanly(t *testing.T) {
	c := connect(t)
	c.query(`SELECT 1`)
	c.terminate()
	// The server must close its side; the cleanup registered by dial fails the
	// test if the goroutine does not return.
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.br.ReadByte(); err == nil {
		t.Fatal("the server sent something after Terminate")
	}
}

func TestAMalformedMessageIsRefusedAsAProtocolViolation(t *testing.T) {
	c := connect(t)
	// A Describe naming neither a statement nor a portal.
	c.send(msgDescribe, append([]byte{'X'}, 0))
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("expected ErrorResponse, got %q", string(rune(m.tag)))
	}
	fs := errorFields(m.body)
	if fs['C'] != sqlstateProtocolViolation || fs['S'] != "FATAL" {
		t.Fatalf("a malformed message produced %s, want a FATAL 08P01", formatError(m.body))
	}
}

func TestAnOversizedMessageIsRefusedWithoutAllocating(t *testing.T) {
	c := connect(t)
	// Declare a two-gigabyte body and send none of it.
	var head [5]byte
	head[0] = msgQuery
	binary.BigEndian.PutUint32(head[1:], 1<<31-1)
	c.sendRaw(head[:])
	// The server must drop the connection rather than wait for two gigabytes.
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.br.ReadByte(); err == nil {
		t.Fatal("the server accepted an absurd message length")
	}
}

func TestSessionRetainedInputsHaveAggregateBounds(t *testing.T) {
	t.Run("prepared SQL", func(t *testing.T) {
		s := &session{
			statements:     map[string]*prepared{},
			portals:        map[string]*portal{},
			statementBytes: maxPreparedInputBytes,
			msg: frontendMessage{
				name:  "next",
				query: "SELECT 1",
			},
		}
		err := s.handleParse()
		var pg *pgError
		if !errors.As(err, &pg) || pg.code != sqlstateProgramLimitExceeded {
			t.Fatalf("prepared SQL beyond the session budget = %v", err)
		}
	})
	t.Run("prepared parameter OIDs", func(t *testing.T) {
		const oidCount = maxParameters
		charge := preparedInputCharge("s", "", oidCount)
		if want := 1 + 4*oidCount; charge != want {
			t.Fatalf("prepared OID charge = %d, want %d", charge, want)
		}
		s := &session{
			statements: map[string]*prepared{},
			portals:    map[string]*portal{},
			// Leave one byte less than this Parse requires.
			statementBytes: maxPreparedInputBytes - charge + 1,
			msg: frontendMessage{
				name:      "s",
				paramOIDs: make([]int32, oidCount),
			},
		}
		err := s.handleParse()
		var pg *pgError
		if !errors.As(err, &pg) || pg.code != sqlstateProgramLimitExceeded {
			t.Fatalf("prepared OIDs beyond the session budget = %v", err)
		}
	})
	t.Run("portal arguments", func(t *testing.T) {
		stmt := &prepared{name: "s", wireParams: 1}
		s := &session{
			statements:  map[string]*prepared{"s": stmt},
			portals:     map[string]*portal{},
			portalBytes: maxPortalBytes,
			msg: frontendMessage{
				name:   "s",
				portal: "next",
				params: [][]byte{[]byte("x")},
			},
		}
		err := s.handleBind()
		var pg *pgError
		if !errors.As(err, &pg) || pg.code != sqlstateProgramLimitExceeded {
			t.Fatalf("portal arguments beyond the session budget = %v", err)
		}
	})
	t.Run("compiler high-water", func(t *testing.T) {
		stmt := &prepared{name: "s", wireParams: 1}
		s := &session{
			statements:         map[string]*prepared{"s": stmt},
			portals:            map[string]*portal{},
			statementBindBytes: maxPreparedBindBytes,
			msg: frontendMessage{
				name:   "s",
				portal: "next",
				params: [][]byte{[]byte("x")},
			},
		}
		err := s.handleBind()
		var pg *pgError
		if !errors.As(err, &pg) || pg.code != sqlstateProgramLimitExceeded {
			t.Fatalf("compiler storage beyond the session budget = %v", err)
		}
		if len(s.portals) != 0 {
			t.Fatal("a Bind rejected by compiler accounting published its portal")
		}
	})
}

func TestManyWidePreparedPlansHitAggregateBoundAndCloseReleasesIt(t *testing.T) {
	c := connect(t)
	projection := strings.TrimSuffix(strings.Repeat("id,", maxResultColumns), ",")
	wide := "SELECT " + projection + " FROM users"
	var admitted []string
	for i := 0; i < maxStatements; i++ {
		name := fmt.Sprintf("w%03d", i)
		c.send(msgParse, parseMsg(name, wide))
		c.send(msgSync, nil)
		msgs := c.until(msgReadyForQuery)
		if has(msgs, msgErrorResponse) {
			expectError(t, msgs, sqlstateProgramLimitExceeded)
			break
		}
		admitted = append(admitted, name)
	}
	if len(admitted) == 0 || len(admitted) >= maxStatements {
		t.Fatalf("wide-plan aggregate bound admitted %d statements; want a finite prefix",
			len(admitted))
	}

	// Releasing one plan returns exactly its admission charge, so an
	// equivalently shaped and equally named replacement must fit immediately.
	c.send(msgClose, closeMsg(targetStatement, admitted[0]))
	c.send(msgSync, nil)
	if msgs := c.until(msgReadyForQuery); has(msgs, msgErrorResponse) {
		t.Fatalf("closing an admitted wide plan failed: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
	c.send(msgParse, parseMsg("x000", wide))
	c.send(msgSync, nil)
	if msgs := c.until(msgReadyForQuery); has(msgs, msgErrorResponse) {
		t.Fatalf("released wide-plan budget was not reusable: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
}

func TestDirectQuestionMarkParametersRespectTheWireInt16Bound(t *testing.T) {
	c := connect(t)
	// Parsing the protocol's maximal predicate is intentionally adversarial
	// and takes several seconds under -race on slower builders. Keep the
	// ordinary client's five-second missing-message detector everywhere else.
	c.readTimeout = 20 * time.Second
	sql := `SELECT id FROM users WHERE age = ?` +
		strings.Repeat(` AND age = ?`, maxParameters)
	c.send(msgParse, parseMsg("", sql))
	// Pipeline Describe to prove an oversized count can never reach
	// ParameterDescription's signed Int16 field.
	c.send(msgDescribe, describeMsg(targetStatement, ""))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateProgramLimitExceeded)
	if has(msgs, msgParseComplete) || has(msgs, msgParameterDesc) {
		t.Fatal("an over-wide parameter vector was published or described")
	}
}

func TestRepeatedNumberedParameterCopiesWireValueOnce(t *testing.T) {
	const occurrences = 4096
	order := make([]int, occurrences)
	for i := range order {
		order[i] = 1
	}
	raw := bytes.Repeat([]byte("9"), 4096)
	m := &frontendMessage{params: [][]byte{raw}}
	stmt := &prepared{paramOrder: order, wireParams: 1}
	args, wireArgs, slots, store, decoded, err := bindArgs(
		nil, nil, nil, nil, nil, m, stmt, nil)
	if err != nil {
		t.Fatalf("bindArgs: %v", err)
	}
	if len(args) != occurrences || len(wireArgs) != 1 || len(slots) != 1 {
		t.Fatalf("argument vectors have lengths %d/%d/%d, want %d/1/1",
			len(args), len(wireArgs), len(slots), occurrences)
	}
	if len(store) != len(raw) {
		t.Fatalf("repeated $1 retained %d bytes, want one %d-byte wire value",
			len(store), len(raw))
	}
	if len(decoded) != 0 {
		t.Fatalf("untyped repeated $1 retained %d decoded bytes, want none", len(decoded))
	}
	for i, arg := range args {
		if arg != wireArgs[0] {
			t.Fatalf("placeholder %d does not alias the one decoded wire value", i)
		}
	}
	charges := fillLiteralCharges(nil, wireArgs)
	if len(charges) != 1 {
		t.Fatalf("literal charge cache has %d entries, want one per wire value", len(charges))
	}

	// The portal copy is one value, but lowering currently retains literal
	// bytes per occurrence in the prepared statement's compiler. That separate
	// high-water charge must still reject a shape whose execution would
	// amplify past the session bound.
	if got := boundLiteralCharge(charges, stmt); got <= maxPreparedBindBytes {
		t.Fatalf("execution charge for repeated $1 = %d, want over %d",
			got, maxPreparedBindBytes)
	}
}

func TestNumberedParameterRewriteCancelsInsideLargeQuotedRun(t *testing.T) {
	src := "$1, '" + strings.Repeat("x", 1<<20) + "'"
	checks := 0
	out, order, highest, err := rewriteNumberedParameters(src, func() error {
		checks++
		if checks == 6 {
			return query.ErrCanceled
		}
		return nil
	})
	if !errors.Is(err, query.ErrCanceled) || out != "" || order != nil || highest != 0 {
		t.Fatalf("canceled rewrite = (%q, %v, %d, %v)", out, order, highest, err)
	}
	if checks != 6 {
		t.Fatalf("rewrite cancellation checks = %d, want 6", checks)
	}
	if _, _, _, err := rewriteNumberedParameters(`SELECT $1`, nil); err != nil {
		t.Fatalf("rewrite was not reusable after cancellation: %v", err)
	}
}

func TestBindArgumentsCancelDuringLargeWireCopyAndRemainReusable(t *testing.T) {
	raw := bytes.Repeat([]byte("x"), 1<<20)
	m := &frontendMessage{params: [][]byte{raw}}
	stmt := &prepared{wireParams: 1}
	checks := 0
	_, _, _, _, _, err := bindArgs(
		nil, nil, nil, nil, nil, m, stmt, func() error {
			checks++
			if checks == 8 {
				return query.ErrCanceled
			}
			return nil
		},
	)
	if !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("large Bind cancellation = %v, want %v", err, query.ErrCanceled)
	}
	args, _, _, store, _, err := bindArgs(
		nil, nil, nil, nil, nil, m, stmt, nil,
	)
	if err != nil || len(args) != 1 || len(store) != len(raw) {
		t.Fatalf("Bind after cancellation = args %d, store %d, error %v",
			len(args), len(store), err)
	}
}

func TestCompilerLiteralChargeAccountsExactEscapingOnce(t *testing.T) {
	const n = 1024
	controls := strings.Repeat("\x01", n)
	if got, want := encodedJSONStringLen(controls), 2+6*n; got != want {
		t.Fatalf("encoded control-string length = %d, want %d", got, want)
	}
	if got, want := compilerLiteralCharge(controls), 1<<10+4*n+8*(2+6*n); got != want {
		t.Fatalf("control-string compiler charge = %d, want %d", got, want)
	}
	number := query.Number(strings.Repeat("9", n))
	if got, want := compilerLiteralCharge(number), 1<<10+12*n; got != want {
		t.Fatalf("exact-number compiler charge = %d, want %d", got, want)
	}

	values := []any{controls}
	charges := make([]int, 0, 1)
	if allocs := testing.AllocsPerRun(100, func() {
		charges = fillLiteralCharges(charges[:0], values)
	}); allocs != 0 {
		t.Fatalf("warm literal accounting allocated %.2f times, want zero", allocs)
	}
}

func TestWarmUnnamedBindIsAllocationFreeForEveryParameterShape(t *testing.T) {
	intWire := binary.BigEndian.AppendUint64(nil, uint64(42))
	for _, tc := range []struct {
		name   string
		raw    []byte
		oid    int32
		format int16
		role   sqldriver.ParamKind
	}{
		{name: "string", raw: []byte("ordinary text"), oid: oidText},
		{name: "exact number", raw: []byte("1e-1"), oid: oidNumeric},
		{name: "boolean", raw: []byte("true"), oid: oidBool},
		{name: "binary integer", raw: intWire, oid: oidInt8, format: formatBinary},
		{name: "declared JSON string", raw: []byte(`"A\nB"`), oid: oidJSON},
		{
			name:   "binary JSONB string",
			raw:    append([]byte{1}, []byte(`"A\nB"`)...),
			oid:    oidJSONB,
			format: formatBinary,
		},
		{
			name: "whole JSON document", raw: []byte(`{"id":"a","nested":[1,true]}`),
			oid: oidJSON, role: sqldriver.ParamDocument,
		},
		{
			name: "binary JSONB document",
			raw:  append([]byte{1}, []byte(`{"id":"a","nested":[1,true]}`)...),
			oid:  oidJSONB, format: formatBinary, role: sqldriver.ParamDocument,
		},
		{name: "null", raw: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := &prepared{
				wireParams: 1, paramOIDs: []int32{tc.oid},
				paramKinds: []sqldriver.ParamKind{tc.role},
			}
			s := &session{
				w:          newWriter(io.Discard, 1<<20),
				statements: map[string]*prepared{"": stmt},
				portals:    map[string]*portal{},
				msg: frontendMessage{
					params:       [][]byte{tc.raw},
					paramFormats: []int16{tc.format},
				},
			}
			if err := s.handleBind(); err != nil {
				t.Fatalf("warm Bind: %v", err)
			}
			if allocs := testing.AllocsPerRun(100, func() {
				if err := s.handleBind(); err != nil {
					panic(err)
				}
			}); allocs != 0 {
				t.Fatalf("warm unnamed Bind allocated %.2f times, want zero", allocs)
			}
		})
	}
}

func TestOneResultRowCannotAmplifyPastTheMessageBound(t *testing.T) {
	blob := strings.Repeat("x", 1<<20)
	doc := `{"id":1,"blob":"` + blob + `"}`
	srv, err := NewServer(testDatabase(t, "docs", []string{doc}),
		Options{Auth: Trust()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})

	columns := make([]string, 17)
	for i := range columns {
		columns[i] = "blob"
	}
	msgs := c.query("SELECT " + strings.Join(columns, ",") + " FROM docs")
	expectError(t, msgs, sqlstateProgramLimitExceeded)
	if has(msgs, msgDataRow) {
		t.Fatal("an oversized result row was partially emitted")
	}
}

func TestLaterOversizedResultRowIsRejectedBeforeAnyDataRow(t *testing.T) {
	blob := strings.Repeat("x", 1<<20)
	docs := []string{
		`{"id":1,"blob":"small"}`,
		`{"id":2,"blob":"` + blob + `"}`,
	}
	srv, err := NewServer(testDatabase(t, "docs", docs),
		Options{Auth: Trust()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})

	columns := make([]string, 17)
	for i := range columns {
		columns[i] = "blob"
	}
	msgs := c.query("SELECT " + strings.Join(columns, ",") + " FROM docs")
	expectError(t, msgs, sqlstateProgramLimitExceeded)
	if has(msgs, msgDataRow) {
		t.Fatal("a row preceding an oversized later DataRow was emitted")
	}
}

func TestWarmDataRowPreflightIsAllocationFree(t *testing.T) {
	db := testDatabase(t, "docs", []string{
		`{"blob":"small"}`,
		`{"blob":"also small"}`,
	})
	session, err := db.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	stmt, err := session.Prepare(context.Background(), `SELECT blob FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var runtimeCursor sqldriver.Cursor
	if err := stmt.QueryInto(context.Background(), nil, &runtimeCursor); err != nil {
		t.Fatal(err)
	}
	defer runtimeCursor.Close()
	cursor := runtimeCursor.Snapshot()
	cols := columnsFor(nil, stmt.Columns(), stmt.AppendSchema(nil))
	if err := checkDataRows(cursor, cols, nil); err != nil {
		t.Fatalf("warm DataRow preflight: %v", err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := checkDataRows(cursor, cols, nil); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm DataRow preflight allocated %.2f times, want zero", allocs)
	}
}

func TestConfiguredResultBudgetsFailBeforeAnyDataRow(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{
			name: "rows",
			opts: Options{
				Auth:           Trust(),
				MaxResultRows:  1,
				MaxResultBytes: UnlimitedResults,
			},
		},
		{
			name: "bytes",
			opts: Options{
				Auth:           Trust(),
				MaxResultRows:  UnlimitedResults,
				MaxResultBytes: 1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := NewServer(testDatabase(t, "users", corpus), tc.opts)
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			t.Cleanup(func() { _ = srv.Close() })
			c := dial(t, srv)
			c.startup(map[string]string{"user": "tester"})
			msgs := c.query(`SELECT id FROM users`)
			expectError(t, msgs, sqlstateProgramLimitExceeded)
			if has(msgs, msgDataRow) {
				t.Fatal("a result rejected by its execution budget emitted a partial DataRow")
			}
		})
	}
}

func TestBackendResultMessagesAreAdmittedBeforeEncoding(t *testing.T) {
	value := strings.Repeat("x", maxDataRowBytes/maxResultColumns+1)
	cols := make([]column, maxResultColumns)
	values := make([]*string, maxResultColumns)
	for i := range cols {
		cols[i] = column{name: value, typ: typeText}
		values[i] = &value
	}

	t.Run("RowDescription", func(t *testing.T) {
		var sink bytes.Buffer
		w := newWriter(&sink, 1024)
		err := w.rowDescription(cols, nil)
		var pg *pgError
		if !errors.As(err, &pg) || pg.code != sqlstateProgramLimitExceeded {
			t.Fatalf("oversized RowDescription = %v", err)
		}
		if len(w.buf) != 0 {
			t.Fatal("oversized RowDescription partially entered the writer")
		}
	})

	t.Run("fixed DataRow", func(t *testing.T) {
		var sink bytes.Buffer
		w := newWriter(&sink, 1024)
		err := w.fixedRow(cols, values, nil)
		var pg *pgError
		if !errors.As(err, &pg) || pg.code != sqlstateProgramLimitExceeded {
			t.Fatalf("oversized fixed DataRow = %v", err)
		}
		if len(w.buf) != 0 {
			t.Fatal("oversized fixed DataRow partially entered the writer")
		}
	})
}

func TestErrorResponseFieldsAreBoundedAndRemainUTF8(t *testing.T) {
	var sink bytes.Buffer
	w := newWriter(&sink, 1024)
	oversized := strings.Repeat("界", maxErrorFieldBytes)
	w.errorResponse(newError(sqlstateInvalidParameterValue, oversized).
		withHint(oversized))
	if len(w.buf) > 2*maxErrorFieldBytes+128 {
		t.Fatalf("bounded ErrorResponse retained %d bytes", len(w.buf))
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	wire := sink.Bytes()
	if len(wire) < 5 || wire[0] != msgErrorResponse {
		t.Fatalf("malformed ErrorResponse: %x", wire)
	}
	fields := errorFields(wire[5:])
	for _, tag := range []byte{'M', 'H'} {
		value := fields[tag]
		if len(value) > maxErrorFieldBytes || !utf8.ValidString(value) ||
			!strings.HasSuffix(value, "...") {
			t.Errorf("field %c = %d bytes, valid=%v, suffix=%v",
				tag, len(value), utf8.ValidString(value), strings.HasSuffix(value, "..."))
		}
	}
}

func TestManySessionsRunConcurrentlyAgainstOneStore(t *testing.T) {
	srv := newTestServer(t, Options{})
	const sessions = 8
	const queries = 20
	var wg sync.WaitGroup
	for range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dial(t, srv)
			c.startup(map[string]string{"user": "tester", "database": "app"})
			for i := range queries {
				sql := fmt.Sprintf(`SELECT id, name FROM users WHERE age >= %d ORDER BY id`,
					i%40)
				msgs := c.query(sql)
				if has(msgs, msgErrorResponse) {
					t.Errorf("concurrent query failed: %s",
						formatError(find(t, msgs, msgErrorResponse).body))
					return
				}
			}
		}()
	}
	wg.Wait()
}

type controlledListener struct {
	entered    chan struct{}
	acceptConn chan net.Conn
	acceptErr  chan error
	closed     chan struct{}
	enterOnce  sync.Once
	closeOnce  sync.Once
}

func newControlledListener() *controlledListener {
	return &controlledListener{
		entered:    make(chan struct{}),
		acceptConn: make(chan net.Conn),
		acceptErr:  make(chan error, 1),
		closed:     make(chan struct{}),
	}
}

func (l *controlledListener) Accept() (net.Conn, error) {
	l.enterOnce.Do(func() { close(l.entered) })
	select {
	case conn := <-l.acceptConn:
		return conn, nil
	case err := <-l.acceptErr:
		return nil, err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *controlledListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (*controlledListener) Addr() net.Addr { return controlledListenerAddr{} }

type controlledListenerAddr struct{}

func (controlledListenerAddr) Network() string { return "pgwire-test" }
func (controlledListenerAddr) String() string  { return "pgwire-test" }

func awaitListenerAccept(t *testing.T, listener *controlledListener) {
	t.Helper()
	select {
	case <-listener.entered:
	case <-time.After(time.Second):
		t.Fatal("Serve did not enter Listener.Accept")
	}
}

func awaitServeResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after its listener stopped")
		return nil
	}
}

func TestNilServeInputsNeverEnterListenerOrConnectionAccounting(t *testing.T) {
	var reported error
	srv, err := NewServer(testDatabase(t, "users", corpus),
		Options{
			Auth: Trust(),
			OnError: func(err error) {
				reported = err
			},
		})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if err := srv.Serve(nil); !errors.Is(err, errNilListener) {
		t.Fatalf("Serve(nil) = %v, want actionable nil-listener error", err)
	}
	srv.ServeConn(nil)
	if !errors.Is(reported, errNilConnection) {
		t.Fatalf("ServeConn(nil) reported %v, want nil-connection error", reported)
	}

	srv.mu.Lock()
	listeners, conns := len(srv.listeners), len(srv.conns)
	srv.mu.Unlock()
	if listeners != 0 || conns != 0 {
		t.Fatalf("nil endpoints entered accounting: %d listeners, %d connections",
			listeners, conns)
	}

	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close after nil endpoints: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked: a nil endpoint leaked WaitGroup accounting")
	}
}

func TestServeOwnsTheListenerAcrossEveryExit(t *testing.T) {
	newServer := func(t *testing.T) *Server {
		t.Helper()
		srv, err := NewServer(testDatabase(t, "users", corpus),
			Options{Auth: Trust()})
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		return srv
	}

	t.Run("server close", func(t *testing.T) {
		srv := newServer(t)
		listener := newControlledListener()
		done := make(chan error, 1)
		go func() { done <- srv.Serve(listener) }()
		awaitListenerAccept(t, listener)

		if err := srv.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := awaitServeResult(t, done); !errors.Is(err, ErrServerClosed) {
			t.Fatalf("Serve after Close = %v, want ErrServerClosed", err)
		}
		select {
		case <-listener.closed:
		default:
			t.Fatal("Server.Close did not close the listener owned by Serve")
		}
		srv.mu.Lock()
		n := len(srv.listeners)
		srv.mu.Unlock()
		if n != 0 {
			t.Fatalf("Serve retained %d listeners after returning", n)
		}
	})

	t.Run("accept failure", func(t *testing.T) {
		srv := newServer(t)
		listener := newControlledListener()
		done := make(chan error, 1)
		go func() { done <- srv.Serve(listener) }()
		awaitListenerAccept(t, listener)

		acceptErr := errors.New("accept failed")
		listener.acceptErr <- acceptErr
		if err := awaitServeResult(t, done); !errors.Is(err, acceptErr) {
			t.Fatalf("Serve after Accept failure = %v, want %v", err, acceptErr)
		}
		select {
		case <-listener.closed:
		default:
			t.Fatal("Serve returned without closing its listener")
		}
		if err := srv.Close(); err != nil {
			t.Fatalf("Close after Serve returned: %v", err)
		}
	})
}

func TestOnErrorMayCloseTheServerSynchronously(t *testing.T) {
	for _, entry := range []string{"ServeConn", "Serve"} {
		t.Run(entry, func(t *testing.T) {
			callbackDone := make(chan error, 1)
			var srv *Server
			var err error
			srv, err = NewServer(testDatabase(t, "users", corpus),
				Options{
					Auth: Trust(),
					OnError: func(error) {
						callbackDone <- srv.Close()
					},
				})
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}

			client, server := net.Pipe()
			serveDone := make(chan error, 1)
			switch entry {
			case "ServeConn":
				go func() {
					srv.ServeConn(server)
					serveDone <- nil
				}()
			case "Serve":
				listener := newControlledListener()
				go func() { serveDone <- srv.Serve(listener) }()
				awaitListenerAccept(t, listener)
				listener.acceptConn <- server
			}

			// EOF before a startup packet is an ordinary session-terminating
			// error and invokes OnError.
			if err := client.Close(); err != nil {
				t.Fatalf("closing test client: %v", err)
			}
			select {
			case err := <-callbackDone:
				if err != nil {
					t.Fatalf("Close from OnError: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("OnError calling Close deadlocked on its own connection")
			}
			select {
			case err := <-serveDone:
				if entry == "Serve" && !errors.Is(err, ErrServerClosed) {
					t.Fatalf("Serve after callback Close = %v, want ErrServerClosed", err)
				}
			case <-time.After(time.Second):
				t.Fatalf("%s did not return after callback Close", entry)
			}
		})
	}
}

func TestServerCloseEndsEverySession(t *testing.T) {
	srv, err := NewServer(testDatabase(t, "users", corpus),
		Options{Auth: Trust()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close waits for the session goroutine, so by here it has returned.
	_ = c.conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.br.ReadByte(); err == nil {
		t.Fatal("the connection was still live after Close")
	}
}

func TestNewServerRequiresAuthenticationAndAppliesFiniteDefaults(t *testing.T) {
	database := testDatabase(t, "users", corpus)
	if _, err := NewServer(database, Options{}); err == nil {
		t.Fatal("NewServer accepted an implicit authentication policy")
	}
	srv, err := NewServer(database, Options{Auth: Trust()})
	if err != nil {
		t.Fatalf("NewServer with explicit Trust: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	if srv.opts.MaxConnections != DefaultMaxConnections ||
		srv.opts.ReadTimeout != DefaultReadTimeout ||
		srv.opts.WriteTimeout != DefaultWriteTimeout ||
		srv.opts.IdleTimeout != DefaultIdleTimeout ||
		srv.opts.MaxResultRows != DefaultMaxResultRows ||
		srv.opts.MaxResultBytes != DefaultMaxResultBytes ||
		srv.opts.MaxIntermediateBytes != DefaultMaxIntermediateBytes {
		t.Fatalf("zero resource fields did not select finite defaults: %+v", srv.opts)
	}

	unbounded, err := NewServer(database, Options{
		Auth:                 Trust(),
		MaxConnections:       UnlimitedConnections,
		ReadTimeout:          -1,
		WriteTimeout:         -1,
		IdleTimeout:          -1,
		MaxResultRows:        UnlimitedResults,
		MaxResultBytes:       UnlimitedResults,
		MaxIntermediateBytes: UnlimitedResults,
	})
	if err != nil {
		t.Fatalf("explicit opt-outs: %v", err)
	}
	t.Cleanup(func() { _ = unbounded.Close() })
	if unbounded.opts.MaxConnections != UnlimitedConnections ||
		unbounded.opts.ReadTimeout != 0 ||
		unbounded.opts.WriteTimeout != 0 ||
		unbounded.opts.IdleTimeout != 0 ||
		unbounded.opts.MaxIntermediateBytes != UnlimitedResults {
		t.Fatalf("explicit unbounded settings were not preserved: %+v", unbounded.opts)
	}
	if invalid, err := NewServer(database, Options{
		Auth: Trust(), MaxIntermediateBytes: -2,
	}); err == nil || invalid != nil {
		t.Fatalf("invalid intermediate limit = (%v, %v), want (nil, error)", invalid, err)
	}
}

func TestNewServerRejectsNilDatabaseBeforeServing(t *testing.T) {
	if _, err := NewServer(nil, Options{Auth: Trust()}); err == nil {
		t.Fatal("NewServer accepted a nil database")
	}
}

func TestMaxConnectionsIsEnforced(t *testing.T) {
	srv := newTestServer(t, Options{MaxConnections: 1})
	first := dial(t, srv)
	first.startup(map[string]string{"user": "tester"})

	second := dial(t, srv)
	second.sendStartup(map[string]string{"user": "tester"})
	for {
		m := second.recv()
		if m.tag == msgErrorResponse {
			if fs := errorFields(m.body); fs['C'] != sqlstateTooManyConnections {
				t.Fatalf("wrong refusal past the connection limit: %s", formatError(m.body))
			}
			return
		}
		if m.tag == msgReadyForQuery {
			t.Fatal("a second connection was admitted past MaxConnections of 1")
		}
	}
}

func TestSlowPartialStartupSocketsHaveAHardAggregateBound(t *testing.T) {
	srv := newTestServer(t, Options{
		MaxConnections: 1,
		// Keep the intentionally partial ordinary startup packets open until
		// this test closes them. This demonstrates that the pre-startup cushion
		// is not dedicated to CancelRequest; production's zero value selects a
		// finite read deadline, while this explicit opt-out permits indefinite
		// occupation.
		ReadTimeout: -1,
	})
	const admitted = 1 + cancelRequestSlots
	clients := make([]net.Conn, 0, admitted)
	for range admitted {
		client, server := net.Pipe()
		clients = append(clients, client)
		go srv.ServeConn(server)
	}
	t.Cleanup(func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv.mu.Lock()
		n := len(srv.conns)
		srv.mu.Unlock()
		if n == admitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d partial-startup sockets reached admission, want %d", n, admitted)
		}
		time.Sleep(time.Millisecond)
	}

	overflowClient, overflowServer := net.Pipe()
	defer overflowClient.Close()
	done := make(chan struct{})
	go func() {
		srv.ServeConn(overflowServer)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a socket beyond the pre-startup aggregate bound retained a goroutine")
	}
}

func TestCancelDuringDataRowStreamingStopsTheCurrentQuery(t *testing.T) {
	blob := strings.Repeat("x", 32<<10)
	docs := make([]string, 8)
	for i := range docs {
		docs[i] = fmt.Sprintf(`{"id":%d,"blob":"%s"}`, i, blob)
	}
	srv, err := NewServer(
		testDatabase(t, "docs", docs),
		Options{Auth: Trust()},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})

	c.send(msgQuery, append([]byte(`SELECT blob FROM docs`), 0))
	if m := c.recv(); m.tag != msgRowDescription {
		t.Fatalf("first result message = %q, want RowDescription", string(rune(m.tag)))
	}
	if m := c.recv(); m.tag != msgDataRow {
		t.Fatalf("second result message = %q, want DataRow", string(rune(m.tag)))
	}
	// Each row is larger than the server's write buffer. The next row is now
	// either about to be written or blocked in net.Pipe, so the cancel is
	// deterministically observed at a following row boundary.
	srv.cancelRequest(c.pid, c.secret)
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateQueryCanceled)
	if rows := countTag(msgs, msgDataRow); rows >= len(docs)-1 {
		t.Fatalf("streaming cancel delivered all %d remaining rows", rows)
	}
	if has(c.query(`SELECT 1`), msgErrorResponse) {
		t.Fatal("a streaming cancel poisoned the next statement")
	}
}

func TestLargeWriteRefreshesAnExpiredPreviousWriteDeadline(t *testing.T) {
	blob := strings.Repeat("x", 32<<10)
	doc := `{"blob":"` + blob + `"}`
	srv, err := NewServer(testDatabase(t, "docs", []string{doc}), Options{
		Auth:         Trust(),
		WriteTimeout: 250 * time.Millisecond,
		IdleTimeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})

	// Startup's final flush armed an absolute write deadline. Let it expire.
	// The next DataRow is larger than bufio.Writer, so Write itself reaches the
	// net.Pipe before session.flush runs and must refresh the deadline there.
	time.Sleep(300 * time.Millisecond)
	msgs := c.query(`SELECT blob FROM docs`)
	if has(msgs, msgErrorResponse) || !has(msgs, msgDataRow) {
		t.Fatalf("large result after idle did not complete: %s", tags(msgs))
	}
}

func TestWriterRefreshesDeadlineOnlyBeforeUnderlyingWrites(t *testing.T) {
	var sink bytes.Buffer
	w := newWriter(&sink, 16)
	refreshes := 0
	w.beforeWrite = func() { refreshes++ }

	w.write(make([]byte, 16))
	if refreshes != 0 {
		t.Fatalf("a fully buffered write refreshed the deadline %d times, want zero",
			refreshes)
	}
	w.write([]byte{1})
	if refreshes != 1 {
		t.Fatalf("a write that forced a bufio flush refreshed %d times, want one",
			refreshes)
	}
	if err := w.flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if refreshes != 2 {
		t.Fatalf("the explicit flush left refresh count %d, want two", refreshes)
	}
}

func TestCancelRequestWithAWrongSecretIsIgnored(t *testing.T) {
	srv := newTestServer(t, Options{})
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})
	srv.cancelRequest(c.pid, c.secret+1)
	if has(c.query(`SELECT 1`), msgErrorResponse) {
		t.Fatal("a cancel with the wrong secret was honoured")
	}
}

// --- helpers ---------------------------------------------------------------

func countTag(msgs []backendMessage, tag byte) int {
	n := 0
	for _, m := range msgs {
		if m.tag == tag {
			n++
		}
	}
	return n
}

func tagBytes(msgs []backendMessage) []byte {
	out := make([]byte, len(msgs))
	for i, m := range msgs {
		out[i] = m.tag
	}
	return out
}

func msgsOf(tagList []byte) []backendMessage {
	out := make([]backendMessage, len(tagList))
	for i, tag := range tagList {
		out[i] = backendMessage{tag: tag}
	}
	return out
}

// expectErrorSoft reports rather than aborts, so one table entry's failure does
// not hide the rest.
func expectErrorSoft(t *testing.T, msgs []backendMessage, code, what string) map[byte]string {
	t.Helper()
	for _, m := range msgs {
		if m.tag != msgErrorResponse {
			continue
		}
		fs := errorFields(m.body)
		if fs['C'] != code {
			t.Errorf("%s produced SQLSTATE %s, want %s: %s", what, fs['C'], code,
				formatError(m.body))
		}
		return fs
	}
	t.Errorf("%s produced no error at all: %s", what, tags(msgs))
	return nil
}

// --- numbered parameters ---------------------------------------------------

// Given that every PostgreSQL client library writes $1 rather than this
// dialect's '?', when a statement using numbered parameters arrives, then it is
// rewritten in front of the parser, its arguments are read in the order the
// numbers name, and a parse error still points at the byte the client wrote.

func TestNumberedParametersAreAccepted(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE tier = $1 AND age = $2`))
	c.send(msgDescribe, describeMsg(targetStatement, ""))
	c.send(msgBind, bindMsg("", "", nil, [][]byte{[]byte("free"), []byte("21")}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("a $n statement failed: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
	f := fields{b: find(t, msgs, msgParameterDesc).body}
	if n := f.int16(); n != 2 {
		t.Fatalf("ParameterDescription declares %d parameters for $1 and $2", n)
	}
	if n := countTag(msgs, msgDataRow); n != 2 {
		t.Fatalf("the query returned %d rows, want 2", n)
	}
}

func TestNumberedParametersMayBeOutOfOrderAndRepeated(t *testing.T) {
	c := connect(t)
	// $2 is read first and $1 second, so a server that ignored the numbering
	// would compare tier against 21 and age against 'free' and match nothing.
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE age = $2 AND tier = $1`))
	c.send(msgBind, bindMsg("", "", nil, [][]byte{[]byte("free"), []byte("21")}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	if n := countTag(c.until(msgReadyForQuery), msgDataRow); n != 2 {
		t.Fatalf("an out-of-order $n statement returned %d rows, want 2", n)
	}

	// One parameter read by two placeholders.
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE age >= $1 AND age <= $1`))
	c.send(msgDescribe, describeMsg(targetStatement, ""))
	c.send(msgBind, bindMsg("", "", nil, [][]byte{[]byte("30")}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	f := fields{b: find(t, msgs, msgParameterDesc).body}
	if n := f.int16(); n != 1 {
		t.Fatalf("a statement with $1 twice declares %d parameters, want 1", n)
	}
	if n := countTag(msgs, msgDataRow); n != 2 {
		t.Fatalf("a repeated $1 returned %d rows, want the 2 documents with age 30", n)
	}
}

func TestADollarInsideALiteralIsNotAParameter(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("", `SELECT id FROM users WHERE name = '$1' AND age = $1`))
	c.send(msgDescribe, describeMsg(targetStatement, ""))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("a $1 inside a string literal broke the rewrite: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
	f := fields{b: find(t, msgs, msgParameterDesc).body}
	if n := f.int16(); n != 1 {
		t.Fatalf("the statement declares %d parameters; the one inside the literal is text", n)
	}
}

func TestMixingPlaceholderSpellingsIsRefused(t *testing.T) {
	c := connect(t)
	msgs := c.query(`SELECT id FROM users WHERE age = ? AND tier = $1`)
	expectError(t, msgs, sqlstateSyntaxError)
}

func TestTheRewritePreservesErrorPositions(t *testing.T) {
	// "$12" is three bytes and "?" is one, so a rewrite that shortened the text
	// would report a position two bytes short of the offending token.
	sql := `SELECT id FROM users WHERE age = $12 AND name LIKE 'x' ESCAPE '!'`
	c := connect(t)
	fs := expectError(t, c.query(sql), sqlstateSyntaxError)
	pos, err := strconv.Atoi(fs['P'])
	if err != nil {
		t.Fatalf("no position on the error: %q", fs['P'])
	}
	if !strings.HasPrefix(sql[pos-1:], "ESCAPE") {
		t.Fatalf("position %d points at %q in the original statement, want ESCAPE",
			pos, sql[pos-1:])
	}
}

func TestRewriteNumberedParametersPreservesLength(t *testing.T) {
	cases := []struct {
		sql   string
		order []int
	}{
		{`SELECT a FROM b WHERE c = $1`, []int{1}},
		{`SELECT a FROM b WHERE c = $12 AND d = $345`, []int{12, 345}},
		{`SELECT a FROM b WHERE c = $2 AND d = $1`, []int{2, 1}},
		{`SELECT a FROM b WHERE c = $1 AND d = $1`, []int{1, 1}},
		// A '$1' inside a comment or a string literal is text, so the mapping
		// records only the one outside.
		{"-- $1 in a comment\n SELECT a FROM b WHERE c = $1", []int{1}},
		{`SELECT a FROM b WHERE c = '/* $1 */' AND d = $1`, []int{1}},
		{`SELECT a FROM b /* $9 */ WHERE c = $1`, []int{1}},
	}
	for _, tc := range cases {
		out, order, highest, err := rewriteNumberedParameters(tc.sql, nil)
		if err != nil {
			t.Errorf("%q: %v", tc.sql, err)
			continue
		}
		if len(out) != len(tc.sql) {
			t.Errorf("%q rewrote to %d bytes from %d; positions would shift",
				tc.sql, len(out), len(tc.sql))
		}
		if !slices.Equal(order, tc.order) {
			t.Errorf("%q mapped to %v, want %v (rewrote to %q)", tc.sql, order, tc.order, out)
		}
		if want := slices.Max(tc.order); highest != want {
			t.Errorf("%q reports %d parameters, want %d", tc.sql, highest, want)
		}
	}
	// A statement with no numbered parameter is returned untouched.
	src := `SELECT a FROM b WHERE c = ?`
	out, order, _, err := rewriteNumberedParameters(src, nil)
	if err != nil || out != src || order != nil {
		t.Fatalf("a '?' statement was rewritten: %q %v %v", out, order, err)
	}
}

func TestUnimplementedSubprotocolsAreRefusedAsMissingFeatures(t *testing.T) {
	for _, tag := range []byte{msgCopyData, msgCopyDone, msgCopyFail, msgFunctionCall} {
		c := connect(t)
		c.send(tag, []byte{0})
		m := c.recv()
		if m.tag != msgErrorResponse {
			t.Fatalf("message %q produced %q, want ErrorResponse",
				string(rune(tag)), string(rune(m.tag)))
		}
		fs := errorFields(m.body)
		if fs['C'] != sqlstateFeatureNotSupported {
			t.Errorf("message %q produced SQLSTATE %s, want %s: %s",
				string(rune(tag)), fs['C'], sqlstateFeatureNotSupported, formatError(m.body))
		}
	}
}

func TestAnAuthenticationReplyAfterAuthenticationIsAProtocolViolation(t *testing.T) {
	c := connect(t)
	c.send(msgPasswordOrSASL, []byte("late"))
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("a late authentication reply produced %q", string(rune(m.tag)))
	}
	if fs := errorFields(m.body); fs['C'] != sqlstateProtocolViolation {
		t.Fatalf("wrong SQLSTATE for a late authentication reply: %s", formatError(m.body))
	}
}

func TestAnOverLongStatementNameIsRefused(t *testing.T) {
	c := connect(t)
	name := strings.Repeat("x", maxIdentifier+1)
	c.send(msgParse, parseMsg(name, `SELECT id FROM users`))
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("an over-long statement name produced %q", string(rune(m.tag)))
	}
	if fs := errorFields(m.body); fs['C'] != sqlstateProtocolViolation {
		t.Fatalf("wrong SQLSTATE for an over-long name: %s", formatError(m.body))
	}
}

func TestAClientVanishingMidMessageEndsTheSession(t *testing.T) {
	srv := newTestServer(t, Options{})
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		srv.ServeConn(server)
		done <- nil
	}()
	// A complete startup, then half a Query message, then a hard close.
	c := newTestClient(t, client)
	c.startup(map[string]string{"user": "tester"})
	var head [5]byte
	head[0] = msgQuery
	binary.BigEndian.PutUint32(head[1:], 100)
	c.sendRaw(head[:])
	c.sendRaw([]byte("SELECT"))
	c.drainWrites()
	_ = client.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the session goroutine did not exit after the client vanished mid-message")
	}
}

// --- regressions found by review -------------------------------------------
//
// Each of these pins a bug that was present and is not a hypothetical: three of
// them produced silently wrong rows or values rather than an error, which is
// the failure mode this package is written to avoid.

// A Bind supplying fewer format codes than parameters used to be accepted, and
// formatFor answers text for an index past the array — so the parameters past
// the codes were decoded as text even though the client encoded them binary.
func TestBindRejectsAMismatchedFormatCodeCount(t *testing.T) {
	c := connect(t)
	sql := `SELECT id FROM users WHERE age = ? AND tier = ? AND name = ?`
	values := [][]byte{[]byte("30"), []byte("pro"), []byte("amy")}
	c.send(msgParse, parseMsg("s", sql))
	c.send(msgSync, nil)
	c.until(msgReadyForQuery)

	// The protocol's three legal shapes: no codes, one code for every
	// parameter, and exactly one code per parameter.
	for _, codes := range [][]int16{
		nil,
		{formatText},
		{formatText, formatText, formatText},
	} {
		c.send(msgBind, bindMsg("", "s", codes, values, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
		if msgs := c.until(msgReadyForQuery); has(msgs, msgErrorResponse) {
			t.Fatalf("%d format codes for 3 parameters was rejected: %s", len(codes),
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}

	// Two codes for three parameters is none of them. Accepting it is how the
	// third parameter ends up decoded as text whatever the client encoded,
	// because formatFor answers text for an index past the array.
	c.send(msgBind, bindMsg("", "s", []int16{formatText, formatText}, values, nil))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateProtocolViolation)

	// The same rule governs result format codes.
	c.send(msgBind, bindMsg("", "s", nil, values, []int16{formatText, formatText}))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateProtocolViolation)
}

// A failed re-Bind of the unnamed portal used to overwrite the previous
// portal's arguments in place and leave it reachable, so the next Execute ran
// with a mixture of the old values and the new ones.
func TestAFailedRebindDoesNotCorruptThePreviousPortal(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("s", `SELECT id FROM users WHERE age = $1 AND tier = $2`))
	c.send(msgBind, bindMsg("", "s", nil, [][]byte{[]byte("21"), []byte("free")}, nil))
	c.send(msgSync, nil)
	c.until(msgReadyForQuery)

	// A re-Bind whose second parameter cannot be decoded: binary with no
	// declared type. The first parameter has already been converted when it
	// fails.
	c.send(msgBind, bindMsg("", "s", []int16{formatText, formatBinary},
		[][]byte{[]byte("45"), {0, 0, 0, 1}}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateFeatureNotSupported)
	// The Execute must not have produced rows from a half-rebound portal. It is
	// discarded by the error state, and the portal itself is gone.
	if has(msgs, msgDataRow) {
		t.Fatalf("a portal left by a failed Bind returned rows: %v", rowsOf(t, msgs))
	}
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateInvalidCursorName)
}

// An error in the extended protocol used to sit in the write buffer until the
// next Sync, so a client following the documented "Flush and examine the
// result" pattern waited for a message that had been written and not sent.
func TestAnErrorIsDeliveredBeforeFlush(t *testing.T) {
	c := connect(t)
	c.send(msgBind, bindMsg("", "never-parsed", nil, nil, nil))
	c.send(msgFlush, nil)
	m := c.recv()
	if m.tag != msgErrorResponse {
		t.Fatalf("Flush after a failed Bind produced %q, want the ErrorResponse",
			string(rune(m.tag)))
	}
	c.send(msgSync, nil)
	if m := c.recv(); m.tag != msgReadyForQuery {
		t.Fatalf("Sync produced %q, want ReadyForQuery", string(rune(m.tag)))
	}
}

// A simple Query arriving in the extended protocol's error state used to run,
// and to answer with a ReadyForQuery of its own — telling a pipelining client
// the batch had resynchronized while later messages were still being discarded.
func TestASimpleQueryIsDiscardedInTheErrorState(t *testing.T) {
	c := connect(t)
	c.send(msgBind, bindMsg("", "never-parsed", nil, nil, nil))
	c.send(msgQuery, append([]byte(`SELECT id FROM users`), 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateInvalidStatementName)
	if has(msgs, msgDataRow) {
		t.Fatal("a Query in the error state was executed")
	}
	if n := countTag(msgs, msgReadyForQuery); n != 1 {
		t.Fatalf("got %d ReadyForQuery messages, want the one Sync produced", n)
	}
}

// An empty statement between two semicolons used to produce its own
// EmptyQueryResponse, so a client counting one reply per statement got one too
// many.
func TestEmptyStatementsBetweenSemicolonsAreDropped(t *testing.T) {
	c := connect(t)
	msgs := c.query(`SELECT id FROM users LIMIT 1;;SELECT id FROM users LIMIT 1;`)
	if has(msgs, msgEmptyQuery) {
		t.Fatalf("an empty statement between semicolons produced a reply: %s", tags(msgs))
	}
	if n := countTag(msgs, msgCommandComplete); n != 2 {
		t.Fatalf("got %d CommandComplete messages for two statements", n)
	}
	// A wholly empty query string is still an empty query.
	if !has(c.query(`;`), msgEmptyQuery) {
		t.Fatal("a bare semicolon did not produce an EmptyQueryResponse")
	}
}

// The connection limit used to be checked after authentication, so the PBKDF2
// work an unauthenticated peer could demand was unbounded.
func TestTheConnectionLimitIsCheckedBeforeAuthentication(t *testing.T) {
	verifier, err := NewVerifier("correct-horse")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	attempts := 0
	srv := newTestServer(t, Options{
		MaxConnections: 1,
		Auth: SCRAM(func(string) (Verifier, bool) {
			attempts++
			return verifier, true
		}),
	})
	first := dial(t, srv)
	sc := &scramClient{t: t, c: first, user: "alice", password: "correct-horse", gs2: "n"}
	if m := sc.authenticate(); m.tag != msgAuthentication {
		t.Fatalf("the first connection failed: %s", formatError(m.body))
	}
	before := attempts

	second := dial(t, srv)
	second.sendStartup(map[string]string{"user": "alice"})
	m := second.recv()
	if fs := errorFields(m.body); m.tag != msgErrorResponse ||
		fs['C'] != sqlstateTooManyConnections {
		t.Fatalf("the second connection was not refused: %q", string(rune(m.tag)))
	}
	if attempts != before {
		t.Fatal("a connection past the limit still reached the authentication mechanism")
	}
}

// A frontend message larger than the retained buffer used to stay reachable
// through the decoded message's parameter slice, whose stale entries past len
// the garbage collector still scans.
func TestAnOversizedBodyIsNotPinnedByTheDecodedMessage(t *testing.T) {
	var m frontendMessage
	big := make([]byte, retainedBuffer*2)
	body := bindMsg("", "", nil, [][]byte{big}, nil)
	if err := decodeFrontend(&m, msgBind, body); err != nil {
		t.Fatalf("decoding a large Bind: %v", err)
	}
	if len(m.params) != 1 || len(m.params[0]) != len(big) {
		t.Fatalf("the large parameter did not decode: %d values", len(m.params))
	}
	// A later, small message must leave nothing pointing at the large body.
	if err := decodeFrontend(&m, msgSync, nil); err != nil {
		t.Fatalf("decoding Sync: %v", err)
	}
	for _, view := range m.params[:cap(m.params)] {
		if view != nil {
			t.Fatalf("a %d-byte view into a released body survived the next message", len(view))
		}
	}
	if m.name != "" || m.portal != "" || m.query != "" {
		t.Fatal("a borrowed string view into a released body survived the next message")
	}
}

func TestWarmNamedBindDecodeIsAllocationFree(t *testing.T) {
	body := bindMsg("portal", "statement", []int16{formatText},
		[][]byte{[]byte("value")}, []int16{formatBinary})
	var m frontendMessage
	if err := decodeFrontend(&m, msgBind, body); err != nil {
		t.Fatalf("warm decode: %v", err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := decodeFrontend(&m, msgBind, body); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm named Bind decode allocated %.2f times, want zero", allocs)
	}
}

func TestEveryExecutionBudgetMapsToProgramLimitExceeded(t *testing.T) {
	for _, err := range []error{
		&query.ResultBudgetError{Rows: 2, RowLimit: 1},
		&query.AggregateBudgetError{Requested: 2, Limit: 1},
		&query.JoinPairBudgetError{Pairs: 2, Bytes: 2, Limit: 1},
		&query.WorkBudgetError{Resource: "sort", Bytes: 2, Limit: 1},
		&query.SpillBudgetError{Bytes: 2, Limit: 1},
	} {
		if got := asPGError(err); got.code != sqlstateProgramLimitExceeded {
			t.Errorf("%T mapped to SQLSTATE %s, want %s",
				err, got.code, sqlstateProgramLimitExceeded)
		}
	}
}
