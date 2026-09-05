//go:build linux

package gatewayruntime

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rf3testfixture"
)

// This is the public SQL endpoint, not the offline --table-schema helper.
// Its small wire client has no JDBC/psql dependency in the Go test suite.
func TestPostgreSQLDevOnlineCreateTableAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"vibedb", "vibedb-shard", "vibedb-gateway"} {
		replicaProcessBuild(t, ctx, filepath.Join(bin, command), "./cmd/"+command)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	args := []string{"cluster", "dev", "--root", filepath.Join(root, "state"), "--pg-listen", address, "--diagnostics-on-exit"}
	start := func() *rf3testfixture.ExternalProcess {
		process := &rf3testfixture.ExternalProcess{Binary: filepath.Join(bin, "vibedb"), Args: args}
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			replicaProcessStop(t, process)
			if t.Failed() {
				t.Log(process.Diagnostics())
			}
		})
		if err := process.WaitReady(ctx, "VibeDB development RF3 physical cluster ready:"); err != nil {
			t.Fatalf("startup: %v\n%s", err, process.Diagnostics())
		}
		return process
	}
	process := start()
	connection := openDDLWire(t, ctx, address)
	defer connection.Close()
	const ddl = `CREATE TABLE employees (
id TEXT PRIMARY KEY,
name TEXT NOT NULL,
team TEXT NOT NULL,
city TEXT,
score INTEGER NOT NULL,
active BOOLEAN NOT NULL
);`
	result := ddlWireQuery(t, connection, ddl, true)
	if result.code != "" || result.tag != "CREATE TABLE" {
		t.Fatalf("exact CREATE: %+v", result)
	}
	for first := 1; first <= 1000; first += 64 {
		var text strings.Builder
		text.WriteString("INSERT INTO employees (id,name,team,city,score,active) VALUES ")
		last := min(first+63, 1000)
		for i := first; i <= last; i++ {
			if i != first {
				text.WriteByte(',')
			}
			fmt.Fprintf(&text, "('employee-%04d','Employee %d','Platform','Lisbon',%d,true)", i, i, i%100)
		}
		result := ddlWireQuery(t, connection, text.String(), false)
		if result.code != "" || result.tag != fmt.Sprintf("INSERT 0 %d", last-first+1) {
			t.Fatalf("insert batch %d: %+v", first, result)
		}
	}
	check := func(connection net.Conn) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		result := ddlWireResult{}
		for {
			result = ddlWireQuery(t, connection, "SELECT COUNT(*) FROM public.employees", false)
			if result.code == "" || !strings.Contains(result.message, "no reachable leader") ||
				time.Now().After(deadline) {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		if result.code != "" || len(result.rows) != 1 || result.rows[0][0] != "1000" {
			t.Fatalf("count: %+v", result)
		}
		result = ddlWireQuery(t, connection, `SELECT id,name,team,city,score,active FROM public.employees WHERE id='employee-0001'`, true)
		if result.code != "" || len(result.rows) != 1 || len(result.rows[0]) != 6 || result.rows[0][1] != `"Employee 1"` {
			t.Fatalf("six columns: %+v", result)
		}
	}
	check(connection)
	result = ddlWireQuery(t, connection, ddl, true)
	if result.code != "42P07" {
		t.Fatalf("duplicate table: %+v", result)
	}
	result = ddlWireQuery(t, connection, "CREATE TABLE IF NOT EXISTS public.employees (id TEXT PRIMARY KEY)", true)
	if result.code != "" || result.tag != "CREATE TABLE" {
		t.Fatalf("IF NOT EXISTS: %+v", result)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	replicaProcessStop(t, process)
	start()
	connection = openDDLWire(t, ctx, address)
	defer connection.Close()
	check(connection)
}

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
