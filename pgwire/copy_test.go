package pgwire

import (
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
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

func TestCopyTextEscapesCustomDelimiterAndNull(t *testing.T) {
	db, err := sqldriver.Open(filepath.Join(t.TempDir(), "copy_text.vdb"))
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

	if msgs := c.query(`CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT, n NUMBER, flag BOOLEAN)`); has(msgs, msgErrorResponse) {
		t.Fatalf("CREATE TABLE failed: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}

	// COPY FROM with custom delimiter '|', custom NULL 'NULL', HEADER, escapes (\b,\f,\n,\r,\t,\v,\\, octal \101, hex \x42, escaped delimiter \|), CRLF, and split chunks.
	c.send(msgQuery, append([]byte(`COPY t (id, val, n, flag) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL 'NULL', HEADER true)`), 0))
	if resp := c.recv(); resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse, got %q", resp.tag)
	}
	c.send(msgCopyData, []byte("id|val|n|flag\r\n"))
	c.send(msgCopyData, []byte(`r1|a\bb\fc\nd\re\tf\vg\\h\|i\101\x42|3.5|YES`+"\r\n"))
	// Split a single row across two CopyData chunks.
	c.send(msgCopyData, []byte("r2|NU"))
	c.send(msgCopyData, []byte("LL|42|off\n"))
	// Literal escaped \N when NULL is 'NULL' and literal NULL via \NULL (not NULL).
	c.send(msgCopyData, []byte(`r3|\NULL|-12.25|0`+"\n"))
	c.send(msgCopyData, []byte("\\.\n"))
	c.send(msgCopyDone, nil)
	doneMsgs := c.until(msgReadyForQuery)
	if tag := copyCommandTag(t, doneMsgs); tag != "COPY 3" {
		t.Fatalf("tag = %q, want COPY 3", tag)
	}

	// COPY TO STDOUT with custom delimiter '|', custom NULL 'NULL', HEADER.
	outMsgs := c.query(`COPY (SELECT id, val, n, flag FROM t ORDER BY id) TO STDOUT WITH (FORMAT text, DELIMITER '|', NULL 'NULL', HEADER true)`)
	var out strings.Builder
	for _, m := range outMsgs {
		if m.tag == msgErrorResponse {
			t.Fatalf("COPY TO error: %s", formatError(m.body))
		}
		if m.tag == msgCopyData {
			out.Write(m.body)
		}
	}
	want := "id|val|n|flag\n" +
		`r1|a\bb\fc\nd\re\tf\vg\\h\|iAB|3.5|t` + "\n" +
		"r2|NULL|42|f\n" +
		"r3|NULL|-12.25|f\n"
	if out.String() != want {
		t.Fatalf("COPY TO text =\n%q\nwant:\n%q", out.String(), want)
	}
}

func TestCopyCSVQuotingNewlinesForceNullAndCustomQuote(t *testing.T) {
	db, err := sqldriver.Open(filepath.Join(t.TempDir(), "copy_csv.vdb"))
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

	if msgs := c.query(`CREATE TABLE csvt (id TEXT PRIMARY KEY, a TEXT, b TEXT)`); has(msgs, msgErrorResponse) {
		t.Fatalf("CREATE TABLE failed: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}

	// Test:
	// - doubled quotes ""
	// - embedded delimiter and embedded newline split across CopyData chunks
	// - quoted empty string "" is NOT NULL by default, unless FORCE_NULL
	// - unquoted empty string is NULL by default, unless FORCE_NOT_NULL
	c.send(msgQuery, append([]byte(`COPY csvt (id, a, b) FROM STDIN WITH (FORMAT csv, HEADER true, FORCE_NULL (a), FORCE_NOT_NULL (b))`), 0))
	if resp := c.recv(); resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse, got %q", resp.tag)
	}
	c.send(msgCopyData, []byte("id,a,b\n"))
	// Row 1: doubled quotes + embedded newline split across chunks.
	c.send(msgCopyData, []byte("1,\"line1\n"))
	c.send(msgCopyData, []byte("line2, \"\"quoted\"\"\",\"val\"\"b\"\n"))
	// Row 2: a is quoted empty "" -> FORCE_NULL makes it NULL; b is unquoted empty -> FORCE_NOT_NULL makes it "".
	c.send(msgCopyData, []byte("2,\"\",\n"))
	c.send(msgCopyDone, nil)
	doneMsgs := c.until(msgReadyForQuery)
	if tag := copyCommandTag(t, doneMsgs); tag != "COPY 2" {
		t.Fatalf("tag = %q, want COPY 2", tag)
	}

	// Verify via SQL SELECT: row 2 has a IS NULL and b = ''.
	nullRows := rowsOf(t, c.query(`SELECT id FROM csvt WHERE a IS NULL AND b = ''`))
	if len(nullRows) != 1 || string(nullRows[0][0]) != `"2"` {
		t.Fatalf("FORCE_NULL / FORCE_NOT_NULL check failed: %q", nullRows)
	}

	// Export with custom QUOTE '''' and ESCAPE '\' and NULL 'NIL'.
	outMsgs := c.query(`COPY (SELECT id, a, b FROM csvt ORDER BY id) TO STDOUT WITH (FORMAT csv, QUOTE '''', ESCAPE '\', NULL 'NIL')`)
	var out strings.Builder
	for _, m := range outMsgs {
		if m.tag == msgErrorResponse {
			t.Fatalf("COPY TO CSV error: %s", formatError(m.body))
		}
		if m.tag == msgCopyData {
			out.Write(m.body)
		}
	}
	want := "1,'line1\nline2, \"quoted\"',val\"b\n2,NIL,\n"
	if out.String() != want {
		t.Fatalf("COPY TO custom CSV =\n%q\nwant:\n%q", out.String(), want)
	}
}

func TestCopyJSONDocumentRoundTripAndValidation(t *testing.T) {
	db, err := sqldriver.Open(filepath.Join(t.TempDir(), "copy_json.vdb"))
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

	if msgs := c.query(`CREATE TABLE docs (id TEXT PRIMARY KEY)`); has(msgs, msgErrorResponse) {
		t.Fatalf("CREATE TABLE failed: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}

	// COPY FROM CSV into "$doc" with JSON objects & arrays.
	c.send(msgQuery, append([]byte(`COPY docs ("$doc") FROM STDIN WITH (FORMAT csv)`), 0))
	if resp := c.recv(); resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse, got %q", resp.tag)
	}
	c.send(msgCopyData, []byte(`"{""id"":""d1"",""tags"":[1,2,3],""meta"":{""ok"":true}}"`+"\n"))
	c.send(msgCopyDone, nil)
	doneMsgs := c.until(msgReadyForQuery)
	if tag := copyCommandTag(t, doneMsgs); tag != "COPY 1" {
		t.Fatalf("tag = %q, want COPY 1", tag)
	}

	// COPY TO CSV of "$doc" should quote the JSON object cleanly.
	outMsgs := c.query(`COPY (SELECT "$doc" FROM docs ORDER BY id) TO STDOUT WITH (FORMAT csv)`)
	var out strings.Builder
	for _, m := range outMsgs {
		if m.tag == msgErrorResponse {
			t.Fatalf("COPY TO JSON CSV error: %s", formatError(m.body))
		}
		if m.tag == msgCopyData {
			out.Write(m.body)
		}
	}
	// Parse the exported CSV line back and verify it is valid JSON with expected fields.
	var p copyLineParser
	cmd := &copyCommand{format: copyFormatCSV, delimiter: ',', quote: '"', escape: '"'}
	line := strings.TrimSuffix(out.String(), "\n")
	if err := p.parseLine([]byte(line), cmd, nil, nil); err != nil {
		t.Fatalf("parse exported JSON CSV: %v", err)
	}
	if len(p.fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(p.fields))
	}
	if err := vibejson.Validate(p.fields[0].data); err != nil {
		t.Fatalf("exported JSON invalid: %v (%q)", err, p.fields[0].data)
	}

	// Unit test convertCopyFieldSlot on OBJECT/ARRAY columns (valid + invalid JSON).
	var slot copyValueSlot
	objCol := sqldriver.ColumnInfo{Types: sqlast.TypeObject}
	val, err := convertCopyFieldSlot([]byte(`{"k":[1,true,"x"]}`), objCol, &slot)
	if err != nil {
		t.Fatalf("valid JSON object rejected: %v", err)
	}
	rawVal, ok := val.(vibejson.RawValue)
	if !ok || string(rawVal.Src) != `{"k":[1,true,"x"]}` {
		t.Fatalf("unexpected RawValue: %#v", val)
	}
	if _, err := convertCopyFieldSlot([]byte(`{"k":invalid}`), objCol, &slot); err == nil {
		t.Fatal("expected error for invalid JSON object")
	}
}

func TestCopyCSVParseErrors(t *testing.T) {
	cmd := &copyCommand{format: copyFormatCSV, delimiter: ',', quote: '"', escape: '"'}
	var p copyLineParser
	for _, bad := range []string{
		`"unterminated`,
		`a,"b"x,c`,
		`a,b"c,d`,
	} {
		if err := p.parseLine([]byte(bad), cmd, nil, nil); err == nil {
			t.Errorf("expected error for invalid CSV %q", bad)
		}
	}
	// Trailing lone backslash in TEXT mode must be rejected.
	textCmd := &copyCommand{format: copyFormatText, delimiter: '\t', nullStr: `\N`}
	if err := p.parseLine([]byte("a\tb\\"), textCmd, nil, nil); err == nil {
		t.Error("expected error for trailing lone backslash in TEXT mode")
	}
}

func TestCopyOptionValidationAndBoundaries(t *testing.T) {
	for _, badSQL := range []string{
		`COPY t FROM STDIN WITH (FORMAT text, DELIMITER '\')`,
		`COPY t FROM STDIN WITH (FORMAT text, DELIMITER E'\n')`,
		`COPY t FROM STDIN WITH (FORMAT text, DELIMITER E'\r')`,
		`COPY t FROM STDIN WITH (FORMAT csv, DELIMITER ',', QUOTE ',')`,
		`COPY t FROM STDIN WITH (FORMAT csv, DELIMITER ',', ESCAPE ',')`,
		`COPY t FROM STDIN WITH (FORMAT csv, QUOTE E'\n')`,
		`COPY t FROM STDIN WITH (FORMAT csv, ESCAPE E'\r')`,
		`COPY t FROM STDIN WITH (FORMAT csv, NULL 'a,b')`,
		`COPY t FROM STDIN WITH (FORMAT text, NULL E'a\nb')`,
	} {
		if _, err := parseCopyCommand(badSQL); err == nil {
			t.Errorf("parseCopyCommand(%q) unexpectedly succeeded", badSQL)
		}
	}

	// Verify 64-bit integer boundaries and non-finite float rejection (32-bit safe).
	var slot copyValueSlot
	intCol := sqldriver.ColumnInfo{Types: sqlast.TypeInteger}
	for _, want := range []int64{int64(math.MinInt64), 0, int64(math.MaxInt64)} {
		raw := []byte(strconv.FormatInt(want, 10))
		v, err := convertCopyFieldSlot(raw, intCol, &slot)
		if err != nil {
			t.Fatalf("convertCopyFieldSlot(%d): %v", want, err)
		}
		if got := *v.(*int64); got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	}
	numCol := sqldriver.ColumnInfo{Types: sqlast.TypeNumber}
	for _, badNum := range []string{"NaN", "Inf", "+Inf", "-Inf", "Infinity"} {
		if _, err := convertCopyFieldSlot([]byte(badNum), numCol, &slot); err == nil {
			t.Errorf("expected error for non-finite number %q", badNum)
		}
	}
}

func TestCopyCSVEdgeCasesChunkBoundariesForceWildcardAndSingleColNull(t *testing.T) {
	db, err := sqldriver.Open(filepath.Join(t.TempDir(), "copy_csv_edge.vdb"))
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

	// 1. Single-column CSV empty line must produce 1 field (NULL by default, "" under FORCE_NOT_NULL)
	//    rather than being silently dropped.
	var p copyLineParser
	csvCmd := &copyCommand{format: copyFormatCSV, delimiter: ',', quote: '"', escape: '"', nullStr: ""}
	if err := p.parseLine([]byte(""), csvCmd, nil, nil); err != nil {
		t.Fatalf("parseLine empty CSV: %v", err)
	}
	if len(p.fields) != 1 || !p.fields[0].isNull {
		t.Fatalf("empty CSV line = %+v, want 1 NULL field", p.fields)
	}
	if err := p.parseLine([]byte(""), csvCmd, nil, []bool{true}); err != nil {
		t.Fatalf("parseLine empty CSV FORCE_NOT_NULL: %v", err)
	}
	if len(p.fields) != 1 || p.fields[0].isNull || len(p.fields[0].data) != 0 {
		t.Fatalf("empty CSV line FORCE_NOT_NULL = %+v, want 1 empty non-NULL field", p.fields)
	}
	if msgs := c.query(`CREATE TABLE solo (id TEXT PRIMARY KEY)`); has(msgs, msgErrorResponse) {
		t.Fatalf("CREATE TABLE solo failed: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}
	c.send(msgQuery, append([]byte(`COPY solo (id) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (id))`), 0))
	if resp := c.recv(); resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse, got %q", resp.tag)
	}
	c.send(msgCopyData, []byte("first\n\nlast\n"))
	c.send(msgCopyDone, nil)
	if tag := copyCommandTag(t, c.until(msgReadyForQuery)); tag != "COPY 3" {
		t.Fatalf("single-col CSV tag = %q, want COPY 3", tag)
	}
	emptyID := rowsOf(t, c.query(`SELECT id FROM solo WHERE id = ''`))
	if len(emptyID) != 1 {
		t.Fatalf("expected 1 empty-string row in solo, got %d", len(emptyID))
	}

	// 2. Multi-column CSV: chunk boundaries right after closing quote, multi-line header,
	//    FORCE_NULL *, FORCE_NOT_NULL *, and legacy FORCE NOT NULL.
	if msgs := c.query(`CREATE TABLE edge (id TEXT PRIMARY KEY, a TEXT, b TEXT)`); has(msgs, msgErrorResponse) {
		t.Fatalf("CREATE TABLE edge failed: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}
	c.send(msgQuery, append([]byte(`COPY edge (id, a, b) FROM STDIN WITH (FORMAT csv, HEADER true, FORCE_NULL *)`), 0))
	if resp := c.recv(); resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse, got %q", resp.tag)
	}
	// Multi-line quoted header.
	c.send(msgCopyData, []byte("\"id\ncol\",a,b\n"))
	// Chunk splits right after closing quote before delimiter, and right after closing quote before doubled quote.
	c.send(msgCopyData, []byte(`1,"abc"`))
	c.send(msgCopyData, []byte(`,"def"`))
	c.send(msgCopyData, []byte("\n"))
	c.send(msgCopyData, []byte(`2,"foo"`))
	c.send(msgCopyData, []byte(`"bar",""`)) // "" in col b becomes NULL via FORCE_NULL *; no trailing \n before CopyDone
	c.send(msgCopyDone, nil)
	if tag := copyCommandTag(t, c.until(msgReadyForQuery)); tag != "COPY 2" {
		t.Fatalf("chunked CSV tag = %q, want COPY 2", tag)
	}
	r2 := rowsOf(t, c.query(`SELECT a FROM edge WHERE id = '2' AND b IS NULL`))
	if len(r2) != 1 || string(r2[0][0]) != `"foo\"bar"` {
		t.Fatalf("row 2 check failed: %q", r2)
	}

	// 3. Legacy CSV FORCE NOT NULL syntax + trailing delimiter.
	c.send(msgQuery, append([]byte(`COPY edge (id, a, b) FROM STDIN CSV FORCE NOT NULL a, b`), 0))
	if resp := c.recv(); resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse, got %q", resp.tag)
	}
	c.send(msgCopyData, []byte("3,,\n"))
	c.send(msgCopyDone, nil)
	if tag := copyCommandTag(t, c.until(msgReadyForQuery)); tag != "COPY 1" {
		t.Fatalf("legacy FORCE NOT NULL tag = %q, want COPY 1", tag)
	}
	r3 := rowsOf(t, c.query(`SELECT id FROM edge WHERE id = '3' AND a = '' AND b = ''`))
	if len(r3) != 1 {
		t.Fatalf("legacy FORCE NOT NULL check failed: %q", r3)
	}

	// 4. Multi-column CSV empty line must return error (22P04), not silently skip.
	c.send(msgQuery, append([]byte(`COPY edge (id, a, b) FROM STDIN WITH (FORMAT csv)`), 0))
	if resp := c.recv(); resp.tag != msgCopyInResponse {
		t.Fatalf("expected CopyInResponse, got %q", resp.tag)
	}
	c.send(msgCopyData, []byte("\n"))
	c.send(msgCopyDone, nil)
	errMsgs := c.until(msgReadyForQuery)
	if !has(errMsgs, msgErrorResponse) {
		t.Fatalf("expected error for blank line in 3-col CSV, got %+v", errMsgs)
	}
}

func TestCatalogDiscoveryShapesLoadedWithVibeJSON(t *testing.T) {
	if len(discoveryShapes) == 0 {
		t.Fatal("discoveryShapes is empty")
	}
	for i, s := range discoveryShapes {
		if s.Name == "" || s.SQL == "" || len(s.Columns) == 0 || len(s.tokens) == 0 {
			t.Fatalf("shape[%d] incomplete: name=%q cols=%d tokens=%d", i, s.Name, len(s.Columns), len(s.tokens))
		}
	}
}

func BenchmarkCopyFromText(b *testing.B) {
	cmd := &copyCommand{format: copyFormatText, delimiter: '\t', nullStr: `\N`}
	cols := []sqldriver.ColumnInfo{
		{Types: sqlast.TypeString},
		{Types: sqlast.TypeString},
		{Types: sqlast.TypeInteger},
		{Types: sqlast.TypeNumber},
		{Types: sqlast.TypeBool},
	}
	line := []byte("user-000123\thello\\tworld\\nnext\t42\t99.95\tt")
	var p copyLineParser
	p.fields = make([]copyField, 0, len(cols))
	slots := make([]copyValueSlot, len(cols))
	args := make([]any, len(cols))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.parseLine(line, cmd, nil, nil); err != nil {
			b.Fatal(err)
		}
		for j, f := range p.fields {
			if f.isNull {
				args[j] = nil
				continue
			}
			v, err := convertCopyFieldSlot(f.data, cols[j], &slots[j])
			if err != nil {
				b.Fatal(err)
			}
			args[j] = v
		}
	}
}

func BenchmarkCopyFromCSV(b *testing.B) {
	cmd := &copyCommand{format: copyFormatCSV, delimiter: ',', quote: '"', escape: '"', nullStr: ""}
	cols := []sqldriver.ColumnInfo{
		{Types: sqlast.TypeString},
		{Types: sqlast.TypeString},
		{Types: sqlast.TypeInteger},
		{Types: sqlast.TypeNumber},
		{Types: sqlast.TypeBool},
	}
	line := []byte(`user-000123,"gamma, ""inc""",42,99.95,true`)
	var p copyLineParser
	p.fields = make([]copyField, 0, len(cols))
	slots := make([]copyValueSlot, len(cols))
	args := make([]any, len(cols))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.parseLine(line, cmd, nil, nil); err != nil {
			b.Fatal(err)
		}
		for j, f := range p.fields {
			if f.isNull {
				args[j] = nil
				continue
			}
			v, err := convertCopyFieldSlot(f.data, cols[j], &slots[j])
			if err != nil {
				b.Fatal(err)
			}
			args[j] = v
		}
	}
}

func BenchmarkCopyToText(b *testing.B) {
	db, err := sqldriver.Open(filepath.Join(b.TempDir(), "bench_copy_text.vdb"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ctx := b.Context()
	rawSess, err := db.NewSession(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer rawSess.Close()
	sess := &embeddedSession{rawSess}
	for _, ddl := range []string{
		`CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, qty INTEGER, active BOOLEAN)`,
		`INSERT INTO t (id, name, qty, active) VALUES ('a', 'hello	world', 42, true)`,
	} {
		s, err := sess.Prepare(ctx, ddl)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := s.Exec(ctx, nil); err != nil {
			s.Close()
			b.Fatal(err)
		}
		s.Close()
	}
	stmt, err := sess.Prepare(ctx, `SELECT id, name, qty, active FROM t`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	var rows BackendRows
	if err := stmt.QueryInto(ctx, nil, &rows); err != nil {
		b.Fatal(err)
	}
	if !rows.Next() {
		b.Fatal("expected 1 row")
	}
	cells := []query.Cell{rows.Cell(0), rows.Cell(1), rows.Cell(2), rows.Cell(3)}
	defer rows.Close()

	cmd := &copyCommand{format: copyFormatText, delimiter: '\t', nullStr: `\N`}
	lineBuf := make([]byte, 0, 256)
	cellScratch := make([]byte, 0, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lineBuf = lineBuf[:0]
		for j, cell := range cells {
			if j > 0 {
				lineBuf = append(lineBuf, cmd.delimiter)
			}
			lineBuf, cellScratch = appendCopyCell(lineBuf, cellScratch, cell, cmd)
		}
		lineBuf = append(lineBuf, '\n')
	}
}

func BenchmarkCopyToCSV(b *testing.B) {
	db, err := sqldriver.Open(filepath.Join(b.TempDir(), "bench_copy_csv.vdb"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ctx := b.Context()
	rawSess, err := db.NewSession(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer rawSess.Close()
	sess := &embeddedSession{rawSess}
	for _, ddl := range []string{
		`CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, qty INTEGER, active BOOLEAN)`,
		`INSERT INTO t (id, name, qty, active) VALUES ('a', 'gamma, "inc"', 42, true)`,
	} {
		s, err := sess.Prepare(ctx, ddl)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := s.Exec(ctx, nil); err != nil {
			s.Close()
			b.Fatal(err)
		}
		s.Close()
	}
	stmt, err := sess.Prepare(ctx, `SELECT id, name, qty, active FROM t`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	var rows BackendRows
	if err := stmt.QueryInto(ctx, nil, &rows); err != nil {
		b.Fatal(err)
	}
	if !rows.Next() {
		b.Fatal("expected 1 row")
	}
	cells := []query.Cell{rows.Cell(0), rows.Cell(1), rows.Cell(2), rows.Cell(3)}
	defer rows.Close()

	cmd := &copyCommand{format: copyFormatCSV, delimiter: ',', quote: '"', escape: '"', nullStr: ""}
	lineBuf := make([]byte, 0, 256)
	cellScratch := make([]byte, 0, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lineBuf = lineBuf[:0]
		for j, cell := range cells {
			if j > 0 {
				lineBuf = append(lineBuf, cmd.delimiter)
			}
			lineBuf, cellScratch = appendCopyCell(lineBuf, cellScratch, cell, cmd)
		}
		lineBuf = append(lineBuf, '\n')
	}
}
