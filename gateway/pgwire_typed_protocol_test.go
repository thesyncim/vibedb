package gateway

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type typedProtocolObservation struct {
	parameters []int32
	resultOID  int32
	rows       []string
}

func typedProtocolFrame(tag byte, body []byte) []byte {
	frame := make([]byte, 5, 5+len(body))
	frame[0] = tag
	binary.BigEndian.PutUint32(frame[1:], uint32(len(body)+4))
	return append(frame, body...)
}

func typedProtocolCString(dst []byte, value string) []byte {
	dst = append(dst, value...)
	return append(dst, 0)
}

func typedProtocolPipeline(conn net.Conn, frames ...[]byte) {
	var payload []byte
	for _, frame := range frames {
		payload = append(payload, frame...)
	}
	go func() { _, _ = conn.Write(payload) }()
}

func typedProtocolMessages(t *testing.T, conn net.Conn, reader *bufio.Reader) map[byte][][]byte {
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
		if header[0] == 'E' {
			t.Fatalf("PostgreSQL ErrorResponse: %q", body)
		}
		messages[header[0]] = append(messages[header[0]], body)
		if header[0] == 'Z' {
			return messages
		}
	}
}

func typedProtocolStartup(t *testing.T, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	body := binary.BigEndian.AppendUint32(nil, 196608)
	body = typedProtocolCString(body, "user")
	body = typedProtocolCString(body, "tester")
	body = typedProtocolCString(body, "database")
	body = typedProtocolCString(body, "app")
	body = append(body, 0)
	packet := binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))
	packet = append(packet, body...)
	go func() { _, _ = conn.Write(packet) }()
	typedProtocolMessages(t, conn, reader)
}

func observeTypedProtocol(
	t *testing.T,
	server *pgwire.Server,
	sqlText string,
	declared []int32,
	params [][]byte,
) typedProtocolObservation {
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
			t.Error("pgwire session did not close")
		}
	})
	reader := bufio.NewReader(client)
	typedProtocolStartup(t, client, reader)

	parse := typedProtocolCString(nil, "typed")
	parse = typedProtocolCString(parse, sqlText)
	parse = binary.BigEndian.AppendUint16(parse, uint16(len(declared)))
	for _, oid := range declared {
		parse = binary.BigEndian.AppendUint32(parse, uint32(oid))
	}
	describe := typedProtocolCString([]byte{'S'}, "typed")
	typedProtocolPipeline(client,
		typedProtocolFrame('P', parse),
		typedProtocolFrame('D', describe),
		typedProtocolFrame('S', nil),
	)
	description := typedProtocolMessages(t, client, reader)
	parameterBody := description['t'][0]
	parameterCount := int(binary.BigEndian.Uint16(parameterBody[:2]))
	observation := typedProtocolObservation{parameters: make([]int32, parameterCount)}
	for index := range observation.parameters {
		offset := 2 + index*4
		observation.parameters[index] = int32(binary.BigEndian.Uint32(parameterBody[offset:]))
	}
	rowBody := description['T'][0]
	offset := 2
	for rowBody[offset] != 0 {
		offset++
	}
	offset++
	offset += 4 + 2
	observation.resultOID = int32(binary.BigEndian.Uint32(rowBody[offset:]))

	bind := []byte{0}
	bind = typedProtocolCString(bind, "typed")
	bind = binary.BigEndian.AppendUint16(bind, 1)
	bind = binary.BigEndian.AppendUint16(bind, 1)
	bind = binary.BigEndian.AppendUint16(bind, uint16(len(params)))
	for _, param := range params {
		if param == nil {
			bind = binary.BigEndian.AppendUint32(bind, ^uint32(0))
			continue
		}
		bind = binary.BigEndian.AppendUint32(bind, uint32(len(param)))
		bind = append(bind, param...)
	}
	bind = binary.BigEndian.AppendUint16(bind, 0)
	execute := binary.BigEndian.AppendUint32([]byte{0}, 0)
	typedProtocolPipeline(client,
		typedProtocolFrame('B', bind),
		typedProtocolFrame('E', execute),
		typedProtocolFrame('S', nil),
	)
	execution := typedProtocolMessages(t, client, reader)
	for _, row := range execution['D'] {
		columns := int(binary.BigEndian.Uint16(row[:2]))
		if columns != 1 {
			t.Fatalf("DataRow columns = %d", columns)
		}
		length := int(int32(binary.BigEndian.Uint32(row[2:6])))
		if length < 0 || 6+length > len(row) {
			t.Fatalf("DataRow length = %d", length)
		}
		observation.rows = append(observation.rows, string(row[6:6+length]))
	}
	return observation
}

func TestPostgreSQLGatewayTypedProtocolMatchesEmbeddedBackend(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return authority, nil
		},
	}
	gatewayServer, err := pgwire.NewServerWithBackend(
		backend, pgwire.Options{Auth: pgwire.Trust(), Database: "app"},
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
	embeddedServer, err := pgwire.NewServer(
		database, pgwire.Options{Auth: pgwire.Trust(), Database: "app"},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = embeddedServer.Close() })

	tests := []struct {
		name     string
		sql      string
		declared []int32
		params   [][]byte
		want     typedProtocolObservation
	}{
		{
			name:   "inferred_bool_binary_bind",
			sql:    "SELECT BOOL 't' UNION ALL SELECT $1",
			params: [][]byte{{0}},
			want:   typedProtocolObservation{parameters: []int32{16}, resultOID: 16, rows: []string{"t", "f"}},
		},
		{
			name:   "inferred_text_binary_bind",
			sql:    "SELECT TEXT 'x' UNION ALL SELECT $1",
			params: [][]byte{[]byte("y")},
			want:   typedProtocolObservation{parameters: []int32{25}, resultOID: 25, rows: []string{"x", "y"}},
		},
		{
			name: "declared_bool_drives_set_analysis",
			sql: "SELECT CASE BOOL 't' WHEN $1 THEN BOOL 't' ELSE BOOL 'f' END " +
				"UNION ALL SELECT $2",
			declared: []int32{16, 0},
			params:   [][]byte{{1}, {0}},
			want:     typedProtocolObservation{parameters: []int32{16, 16}, resultOID: 16, rows: []string{"t", "f"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gateway := observeTypedProtocol(
				t, gatewayServer, test.sql, test.declared, test.params,
			)
			embedded := observeTypedProtocol(
				t, embeddedServer, test.sql, test.declared, test.params,
			)
			if !reflect.DeepEqual(gateway, test.want) {
				t.Fatalf("gateway = %+v, want %+v", gateway, test.want)
			}
			if !reflect.DeepEqual(embedded, gateway) {
				t.Fatalf("embedded = %+v, gateway %+v", embedded, gateway)
			}
		})
	}
}
