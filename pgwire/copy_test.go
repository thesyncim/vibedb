package pgwire

import (
	"path/filepath"
	"strings"
	"testing"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestPGWireCopyFromStdinAndToStdout(t *testing.T) {
	db, err := sqldriver.Open(filepath.Join(t.TempDir(), "copy.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv, err := NewServer(db, Options{Database: "test", Auth: Trust()})
	if err != nil {
		t.Fatal(err)
	}
	c := dial(t, srv)
	c.startup(map[string]string{"user": "postgres", "database": "test"})

	msgs := c.query(`CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, qty INTEGER, active BOOLEAN)`)
	for _, m := range msgs {
		if m.tag == msgErrorResponse {
			t.Fatalf("CREATE TABLE failed: %s", string(m.body))
		}
	}

	// 1. COPY FROM STDIN (text format with NULL and escapes)
	c.send(msgQuery, append([]byte(`COPY items (id, name, qty, active) FROM STDIN`), 0))
	resp := c.recv()
	if resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse ('G'), got %q (%s)", resp.tag, string(resp.body))
	}
	c.send(msgCopyData, []byte("a\thello\\tworld\t10\tt\n"))
	c.send(msgCopyData, []byte("b\t\\N\t20\tf\n"))
	c.send(msgCopyData, []byte("\\.\n"))
	c.send(msgCopyDone, nil)
	doneMsgs := c.until(msgReadyForQuery)
	if tag := copyCommandTag(t, doneMsgs); tag != "COPY 2" {
		t.Fatalf("COPY FROM STDIN tag = %q, want %q", tag, "COPY 2")
	}

	// 2. COPY FROM STDIN (CSV with HEADER)
	c.send(msgQuery, append([]byte(`COPY items (id, name, qty, active) FROM STDIN WITH (FORMAT csv, HEADER true)`), 0))
	resp = c.recv()
	if resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse for CSV, got %q", resp.tag)
	}
	c.send(msgCopyData, []byte("id,name,qty,active\n"))
	c.send(msgCopyData, []byte("c,\"gamma, inc\",30,true\n"))
	c.send(msgCopyDone, nil)
	doneMsgs = c.until(msgReadyForQuery)
	if tag := copyCommandTag(t, doneMsgs); tag != "COPY 1" {
		t.Fatalf("COPY CSV tag = %q, want %q", tag, "COPY 1")
	}

	// 3. COPY TO STDOUT (text format)
	outMsgs := c.query(`COPY (SELECT id, name, qty, active FROM items ORDER BY id) TO STDOUT`)
	var outText strings.Builder
	sawOutResp, sawOutDone := false, false
	for _, m := range outMsgs {
		switch m.tag {
		case msgErrorResponse:
			t.Fatalf("COPY TO STDOUT error: %s", string(m.body))
		case msgCopyOutResponse:
			sawOutResp = true
		case msgCopyData:
			outText.Write(m.body)
		case msgCopyDone:
			sawOutDone = true
		}
	}
	if !sawOutResp || !sawOutDone {
		t.Fatalf("missing CopyOut framing: resp=%v done=%v", sawOutResp, sawOutDone)
	}
	if tag := copyCommandTag(t, outMsgs); tag != "COPY 3" {
		t.Fatalf("COPY TO STDOUT tag = %q, want %q", tag, "COPY 3")
	}
	wantText := "a\thello\\tworld\t10\tt\nb\t\\N\t20\tf\nc\tgamma, inc\t30\tt\n"
	if outText.String() != wantText {
		t.Fatalf("COPY TO STDOUT text =\n%q\nwant:\n%q", outText.String(), wantText)
	}

	// 4. COPY TO STDOUT (CSV with HEADER)
	csvMsgs := c.query(`COPY (SELECT id, name, qty FROM items ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER true)`)
	var outCSV strings.Builder
	for _, m := range csvMsgs {
		if m.tag == msgErrorResponse {
			t.Fatalf("COPY CSV TO STDOUT error: %s", string(m.body))
		}
		if m.tag == msgCopyData {
			outCSV.Write(m.body)
		}
	}
	wantCSV := "id,name,qty\na,hello\tworld,10\nb,,20\nc,\"gamma, inc\",30\n"
	if outCSV.String() != wantCSV {
		t.Fatalf("COPY CSV TO STDOUT =\n%q\nwant:\n%q", outCSV.String(), wantCSV)
	}

	// 5. CopyFail aborts atomically
	c.send(msgQuery, append([]byte(`COPY items (id, name, qty, active) FROM STDIN`), 0))
	resp = c.recv()
	if resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse for CopyFail, got %q", resp.tag)
	}
	c.send(msgCopyData, []byte("d\tshould_rollback\t40\tt\n"))
	c.send(msgCopyFail, append([]byte("client aborted"), 0))
	failMsgs := c.until(msgReadyForQuery)
	sawErr := false
	for _, m := range failMsgs {
		if m.tag == msgErrorResponse {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatalf("expected error on CopyFail, got %+v", failMsgs)
	}
	// Verify 'd' was rolled back
	checkMsgs := c.query(`SELECT id FROM items WHERE id = 'd'`)
	rows := 0
	for _, m := range checkMsgs {
		if m.tag == msgDataRow {
			rows++
		}
	}
	if rows != 0 {
		t.Fatalf("expected CopyFail row to roll back, got %d rows", rows)
	}
}

func copyCommandTag(t *testing.T, msgs []backendMessage) string {
	t.Helper()
	for _, m := range msgs {
		if m.tag == msgErrorResponse {
			t.Fatalf("unexpected error: %s", string(m.body))
		}
		if m.tag == msgCommandComplete {
			return strings.TrimRight(string(m.body), "\x00")
		}
	}
	return ""
}
