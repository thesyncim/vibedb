package gateway

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type sqlDiagnosticWireSession struct {
	conn   net.Conn
	reader *bufio.Reader
}

func openSQLDiagnosticWireSession(t *testing.T, server *pgwire.Server) sqlDiagnosticWireSession {
	t.Helper()
	client, backend := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.ServeConn(backend)
	}()
	t.Cleanup(func() {
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("pgwire diagnostic session did not close")
		}
	})
	reader := bufio.NewReader(client)
	typedProtocolStartup(t, client, reader)
	return sqlDiagnosticWireSession{conn: client, reader: reader}
}

func (session sqlDiagnosticWireSession) query(t *testing.T, sqlText string) map[byte][][]byte {
	t.Helper()
	body := typedProtocolCString(nil, sqlText)
	typedProtocolPipeline(session.conn, typedProtocolFrame('Q', body))
	return sqlDiagnosticProtocolMessages(t, session.conn, session.reader)
}

func sqlDiagnosticProtocolMessages(t *testing.T, conn net.Conn, reader *bufio.Reader) map[byte][][]byte {
	t.Helper()
	messages := make(map[byte][][]byte)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		var header [5]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			t.Fatal(err)
		}
		length := int(binary.BigEndian.Uint32(header[1:])) - 4
		if length < 0 || length > 1<<20 {
			t.Fatalf("backend message length = %d", length)
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			t.Fatal(err)
		}
		messages[header[0]] = append(messages[header[0]], body)
		if header[0] == 'Z' {
			return messages
		}
	}
}

func sqlDiagnosticErrorFields(t *testing.T, messages map[byte][][]byte) map[byte]string {
	t.Helper()
	errors := messages['E']
	if len(errors) != 1 {
		t.Fatalf("ErrorResponse count = %d, want 1; messages=%v", len(errors), reflect.ValueOf(messages).MapKeys())
	}
	body := errors[0]
	fields := make(map[byte]string)
	for index := 0; index < len(body); {
		field := body[index]
		index++
		if field == 0 {
			if index != len(body) {
				t.Fatalf("ErrorResponse has %d trailing bytes", len(body)-index)
			}
			return fields
		}
		end := index
		for end < len(body) && body[end] != 0 {
			end++
		}
		if end == len(body) {
			t.Fatal("unterminated ErrorResponse field")
		}
		fields[field] = string(body[index:end])
		index = end + 1
	}
	t.Fatal("ErrorResponse has no terminal zero")
	return nil
}

func sqlDiagnosticReadyIdle(t *testing.T, messages map[byte][][]byte) {
	t.Helper()
	ready := messages['Z']
	if len(ready) != 1 || len(ready[0]) != 1 || ready[0][0] != 'I' {
		t.Fatalf("ReadyForQuery = %q, want one idle response", ready)
	}
}

func sqlDiagnosticSingleRow(t *testing.T, messages map[byte][][]byte) string {
	t.Helper()
	if len(messages['E']) != 0 {
		t.Fatalf("recovery returned ErrorResponse: %q", messages['E'])
	}
	rows := messages['D']
	if len(rows) != 1 || len(rows[0]) < 6 || binary.BigEndian.Uint16(rows[0][:2]) != 1 {
		t.Fatalf("recovery DataRow = %q", rows)
	}
	length := int(int32(binary.BigEndian.Uint32(rows[0][2:6])))
	if length < 0 || 6+length != len(rows[0]) {
		t.Fatalf("recovery DataRow length = %d for %d-byte body", length, len(rows[0]))
	}
	return string(rows[0][6:])
}

func seedSQLDiagnosticEmbeddedDatabase(t *testing.T, database *sqldriver.Database) {
	t.Helper()
	session, err := database.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`INSERT INTO messages (tenant_id, n) VALUES ('a1', 1)`,
	} {
		prepared, err := session.Prepare(t.Context(), statement)
		if err != nil {
			t.Fatalf("Prepare %q: %v", statement, err)
		}
		_, execErr := prepared.Exec(t.Context(), nil)
		closeErr := prepared.Close()
		if execErr != nil || closeErr != nil {
			t.Fatalf("Exec/Close %q = (%v, %v)", statement, execErr, closeErr)
		}
	}
}

func TestPostgreSQLGatewayRuntimeDiagnosticsMatchEmbeddedAndRecover(t *testing.T) {
	client, _ := newTwoShardCluster(t, 3)
	t.Cleanup(func() { _ = client.Close() })
	executor := NewExecutor(client, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{})
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	gatewayServer, err := pgwire.NewServerWithBackend(
		&PostgreSQLBackend{
			Executor: executor,
			Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
				return authority, nil
			},
		},
		pgwire.Options{Auth: pgwire.Trust(), Database: "app"},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gatewayServer.Close() })

	database, err := sqldriver.Open(filepath.Join(t.TempDir(), "embedded.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	seedSQLDiagnosticEmbeddedDatabase(t, database)
	embeddedServer, err := pgwire.NewServer(
		database, pgwire.Options{Auth: pgwire.Trust(), Database: "app"},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = embeddedServer.Close() })

	gateway := openSQLDiagnosticWireSession(t, gatewayServer)
	embedded := openSQLDiagnosticWireSession(t, embeddedServer)
	tests := []struct {
		name       string
		sql        string
		state      string
		position   int
		redactText string
	}{
		{
			name: "division_by_zero", sql: `SELECT n / 0 FROM messages WHERE tenant_id = 'a1'`,
			state: "22012", position: 10,
		},
		{
			name: "numeric_range", sql: `SELECT CAST(CAST(n AS TEXT) || 'e999999999999999999999' AS NUMERIC) FROM messages WHERE tenant_id = 'a1'`,
			state: "22003", position: 8,
		},
		{
			name: "invalid_typed_input", sql: `SELECT CAST(tenant_id AS BOOLEAN) FROM messages WHERE tenant_id = 'a1'`,
			state: "22P02", position: 8, redactText: "a1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gatewayMessages := gateway.query(t, test.sql)
			embeddedMessages := embedded.query(t, test.sql)
			sqlDiagnosticReadyIdle(t, gatewayMessages)
			sqlDiagnosticReadyIdle(t, embeddedMessages)
			gatewayFields := sqlDiagnosticErrorFields(t, gatewayMessages)
			embeddedFields := sqlDiagnosticErrorFields(t, embeddedMessages)
			for _, field := range []byte{'C', 'M', 'H', 'P'} {
				if gatewayFields[field] != embeddedFields[field] {
					t.Fatalf("field %c: gateway=%q embedded=%q", field, gatewayFields[field], embeddedFields[field])
				}
			}
			if gatewayFields['C'] != test.state || gatewayFields['P'] != strconv.Itoa(test.position) {
				t.Fatalf("gateway diagnostic = C:%q M:%q H:%q P:%q, want %s/P=%d",
					gatewayFields['C'], gatewayFields['M'], gatewayFields['H'], gatewayFields['P'], test.state, test.position)
			}
			if strings.HasPrefix(gatewayFields['M'], "VDBSQL:") {
				t.Fatalf("wire envelope leaked into pgwire message: %q", gatewayFields['M'])
			}
			if test.redactText != "" && strings.Contains(gatewayFields['M'], test.redactText) {
				t.Fatalf("invalid runtime value leaked: %q", gatewayFields['M'])
			}

			for name, session := range map[string]sqlDiagnosticWireSession{
				"gateway": gateway, "embedded": embedded,
			} {
				recovery := session.query(t, `SELECT n FROM messages WHERE tenant_id = 'a1'`)
				sqlDiagnosticReadyIdle(t, recovery)
				if value := sqlDiagnosticSingleRow(t, recovery); value != "1" {
					t.Fatalf("%s recovery row = %q, want 1", name, value)
				}
			}
		})
	}
}
