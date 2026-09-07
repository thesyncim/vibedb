package gateway

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestPostgreSQLGatewayAcceptsMaxRowsLiteralInsertOverSyntheticCharge(t *testing.T) {
	const rows = 1024
	var writes int
	server := newParameterBookkeepingServer(t, &writes)
	statement := parameterBookkeepingLiteralInsert(rows)
	if len(statement) <= (16<<20)/128 {
		t.Fatalf("regression SQL is too short to exercise the old multiplier: %d bytes", len(statement))
	}

	wire := openSQLDiagnosticWireSession(t, server)
	parse := typedProtocolCString(nil, "literal-batch")
	parse = typedProtocolCString(parse, statement)
	parse = append(parse, 0, 0) // no declared parameter OIDs
	bind := typedProtocolCString(nil, "")
	bind = typedProtocolCString(bind, "literal-batch")
	bind = append(bind, 0, 0, 0, 0, 0, 0) // no formats, values, or result formats
	execute := typedProtocolCString(nil, "")
	execute = append(execute, 0, 0, 0, 0) // execute all available rows
	typedProtocolPipeline(
		wire.conn,
		typedProtocolFrame('P', parse),
		typedProtocolFrame('B', bind),
		typedProtocolFrame('E', execute),
		typedProtocolFrame('S', nil),
	)
	messages := typedProtocolMessages(t, wire.conn, wire.reader)
	if writes != 1 {
		t.Fatalf("literal insert callback count = %d, want 1", writes)
	}
	if len(messages['C']) != 1 || strings.TrimSuffix(string(messages['C'][0]), "\x00") != "INSERT 0 1024" {
		t.Fatalf("literal insert command completion = %q", messages['C'])
	}
}

func TestPostgreSQLGatewayKnownRetentionReleasesNamedAggregateCharge(t *testing.T) {
	server := newParameterBookkeepingServer(t, nil)
	statement := parameterBookkeepingLiteralInsert(1024)
	wire := openSQLDiagnosticWireSession(t, server)
	var admitted []string
	for ordinal := 0; ordinal < 64; ordinal++ {
		name := fmt.Sprintf("literal-%02d", ordinal)
		messages := parameterBookkeepingBatch(t, wire.conn, wire.reader,
			typedProtocolFrame('P', parameterBookkeepingParse(name, statement)),
			typedProtocolFrame('S', nil),
		)
		if len(messages['E']) != 0 {
			break
		}
		if len(messages['1']) != 1 {
			t.Fatalf("named Parse %q messages = %v", name, messages)
		}
		admitted = append(admitted, name)
	}
	if len(admitted) < 2 || len(admitted) >= 64 {
		t.Fatalf("known retention admitted %d named statements; want a finite aggregate prefix", len(admitted))
	}

	closeBody := typedProtocolCString([]byte{'S'}, admitted[0])
	messages := parameterBookkeepingBatch(t, wire.conn, wire.reader,
		typedProtocolFrame('C', closeBody), typedProtocolFrame('S', nil))
	if len(messages['E']) != 0 {
		t.Fatalf("closing known-retention statement failed: %v", messages['E'])
	}
	replacement := parameterBookkeepingBatch(t, wire.conn, wire.reader,
		typedProtocolFrame('P', parameterBookkeepingParse("replacement", statement)),
		typedProtocolFrame('S', nil),
	)
	if len(replacement['E']) != 0 || len(replacement['1']) != 1 {
		t.Fatalf("replacement after known-retention close = %v", replacement)
	}
}

func TestPostgreSQLGatewayKnownRetentionUnnamedReplacementReleasesCharge(t *testing.T) {
	const rows = 1024
	var writes int
	server := newParameterBookkeepingServer(t, &writes)
	statement := parameterBookkeepingLiteralInsert(rows)
	wire := openSQLDiagnosticWireSession(t, server)
	for attempt := 0; attempt < 64; attempt++ {
		messages := parameterBookkeepingBatch(t, wire.conn, wire.reader,
			typedProtocolFrame('P', parameterBookkeepingParse("", statement)),
			typedProtocolFrame('S', nil),
		)
		if len(messages['E']) != 0 || len(messages['1']) != 1 {
			t.Fatalf("unnamed Parse attempt %d = %v", attempt, messages)
		}
	}
	bind := typedProtocolCString(nil, "batch")
	bind = typedProtocolCString(bind, "")
	bind = append(bind, 0, 0, 0, 0, 0, 0)
	execute := typedProtocolCString(nil, "batch")
	execute = append(execute, 0, 0, 0, 0)
	messages := parameterBookkeepingBatch(t, wire.conn, wire.reader,
		typedProtocolFrame('B', bind), typedProtocolFrame('E', execute),
		typedProtocolFrame('S', nil),
	)
	if len(messages['E']) != 0 || writes != 1 || len(messages['C']) != 1 ||
		strings.TrimSuffix(string(messages['C'][0]), "\x00") != "INSERT 0 1024" {
		t.Fatalf("unnamed replacement execution messages=%v writes=%d", messages, writes)
	}
}

func newParameterBookkeepingServer(t *testing.T, writes *int) *pgwire.Server {
	t.Helper()
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return authority, nil
		},
		Write: func(_ context.Context, _ serviceauthz.Authority, query Query) (*Result, error) {
			if writes != nil {
				*writes++
			}
			if len(query.Params) != 0 {
				return nil, fmt.Errorf("literal insert retained %d wire parameters", len(query.Params))
			}
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 1024}, nil
		},
	}
	server, err := pgwire.NewServerWithBackend(
		backend, pgwire.Options{Auth: pgwire.Trust(), Database: "app"},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func parameterBookkeepingParse(name, statement string) []byte {
	parse := typedProtocolCString(nil, name)
	parse = typedProtocolCString(parse, statement)
	return append(parse, 0, 0)
}

func parameterBookkeepingLiteralInsert(rows int) string {
	var source strings.Builder
	source.Grow(rows * 352)
	source.WriteString("INSERT INTO messages (id,bucket,score,payload) VALUES ")
	for row := 0; row < rows; row++ {
		if row != 0 {
			source.WriteByte(',')
		}
		fmt.Fprintf(&source, "('key-%08d',%d,%d,'%s')", row, row%16, row%100,
			parameterBookkeepingPayload(row))
	}
	return source.String()
}

const parameterBookkeepingPayloadAlphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"

func parameterBookkeepingPayload(row int) string {
	var value [256]byte
	x := parameterBookkeepingMixOrdinal(uint64(row) + 0x1d2b79f5aa33cc77)
	for i := range value {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 0x2545f4914f6cdd1d
		value[i] = parameterBookkeepingPayloadAlphabet[(x>>58)&63]
	}
	return string(value[:])
}

func parameterBookkeepingMixOrdinal(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func parameterBookkeepingBatch(
	t *testing.T, conn net.Conn, reader *bufio.Reader, frames ...[]byte,
) map[byte][][]byte {
	t.Helper()
	typedProtocolPipeline(conn, frames...)
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
