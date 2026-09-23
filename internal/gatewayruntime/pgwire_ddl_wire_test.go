package gatewayruntime

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
)

type ddlWireResult struct {
	tag, code, message string
	rows               [][]string
}

func openDDLWire(t *testing.T, ctx context.Context, address string) net.Conn {
	t.Helper()
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	if err := connection.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	packet := binary.BigEndian.AppendUint32(nil, 0)
	packet = binary.BigEndian.AppendUint32(packet, 196608)
	packet = append(packet, []byte("user\x00local\x00database\x00vibedb\x00\x00")...)
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	if _, err := connection.Write(packet); err != nil {
		t.Fatal(err)
	}
	if result := readDDLWire(t, connection); result.code != "" {
		t.Fatalf("startup: %+v", result)
	}
	return connection
}

func ddlWireQuery(t *testing.T, connection net.Conn, sql string, extended bool) ddlWireResult {
	t.Helper()
	var packet []byte
	frame := func(tag byte, payload []byte) {
		packet = append(packet, tag)
		packet = binary.BigEndian.AppendUint32(packet, uint32(len(payload)+4))
		packet = append(packet, payload...)
	}
	if extended {
		parse := append([]byte{0}, sql...)
		parse = append(parse, 0, 0, 0)
		frame('P', parse)
		frame('B', []byte{0, 0, 0, 0, 0, 0, 0, 0})
		frame('D', []byte{'P', 0})
		frame('E', []byte{0, 0, 0, 0, 0})
		frame('S', nil)
	} else {
		frame('Q', append([]byte(sql), 0))
	}
	if _, err := connection.Write(packet); err != nil {
		t.Fatal(err)
	}
	return readDDLWire(t, connection)
}

func readDDLWire(t *testing.T, connection net.Conn) ddlWireResult {
	t.Helper()
	var result ddlWireResult
	for {
		var header [5]byte
		if _, err := io.ReadFull(connection, header[:]); err != nil {
			t.Fatal(err)
		}
		size := int(binary.BigEndian.Uint32(header[1:])) - 4
		if size < 0 || size > 4<<20 {
			t.Fatalf("invalid wire frame: %d", size)
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(connection, payload); err != nil {
			t.Fatal(err)
		}
		switch header[0] {
		case 'Z':
			return result
		case 'C':
			result.tag = strings.TrimSuffix(string(payload), "\x00")
		case 'E':
			for len(payload) > 1 {
				end := bytes.IndexByte(payload[1:], 0)
				if end < 0 {
					t.Fatal("invalid ErrorResponse")
				}
				if payload[0] == 'C' {
					result.code = string(payload[1 : 1+end])
				}
				if payload[0] == 'M' {
					result.message = string(payload[1 : 1+end])
				}
				payload = payload[end+2:]
			}
		case 'D':
			if len(payload) < 2 {
				t.Fatal("invalid DataRow")
			}
			count := int(binary.BigEndian.Uint16(payload))
			payload = payload[2:]
			row := make([]string, count)
			for i := range row {
				if len(payload) < 4 {
					t.Fatal("invalid DataRow field")
				}
				n := int(int32(binary.BigEndian.Uint32(payload)))
				payload = payload[4:]
				if n < 0 {
					continue
				}
				if n > len(payload) {
					t.Fatal("truncated DataRow")
				}
				row[i] = string(payload[:n])
				payload = payload[n:]
			}
			result.rows = append(result.rows, row)
		}
	}
}
