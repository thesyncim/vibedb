package pgwire

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func seedProtocolSetTables(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE set_a (id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`CREATE TABLE set_b (id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`INSERT INTO set_a VALUES ` +
			`({"id":"a1","n":1}), ({"id":"a2","n":2}), ({"id":"a4","n":4})`,
		`INSERT INTO set_b VALUES ` +
			`({"id":"b2","n":2}), ({"id":"b3","n":3}), ({"id":"b4","n":4})`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
}

func TestLeadingParenthesizedQueryIsNeverClassifiedEmpty(t *testing.T) {
	for _, source := range []string{
		`(SELECT id FROM a) UNION SELECT id FROM b`,
		"/* lead */\n(SELECT id FROM a)",
		`VALUES (1)`,
		`TABLE a`,
	} {
		kind, reason := classify(source)
		if kind != kindSelect || reason != "" {
			t.Fatalf("classify(%q) = %v/%q, want SELECT", source, kind, reason)
		}
	}
}

func TestSQLSetValuesTableRootsSimpleExtendedAndPositionedRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	seedProtocolSetTables(t, c)

	msgs := c.query(`VALUES (1, 'one'), (2, NULL) ORDER BY column1 DESC`)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("bare VALUES: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}
	description := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if len(description) != 2 || description[0].name != "column1" ||
		description[1].name != "column2" ||
		description[0].oid != oidJSON || description[1].oid != oidJSON {
		t.Fatalf("VALUES RowDescription = %+v", description)
	}
	rows := rowsOf(t, msgs)
	if len(rows) != 2 || string(rows[0][0]) != "2" || rows[0][1] != nil ||
		string(rows[1][0]) != "1" || string(rows[1][1]) != `"one"` {
		t.Fatalf("VALUES rows = %q", rows)
	}
	if got := commandTagOf(t, msgs); got != "SELECT 2" {
		t.Fatalf("VALUES command tag = %q", got)
	}

	msgs = extendedSQL(c,
		`(VALUES ($1) ORDER BY column1) UNION ALL `+
			`SELECT n FROM set_a ORDER BY column1`,
		[][]byte{[]byte("0")},
	)
	rows = rowsOf(t, msgs)
	if len(rows) != 4 || string(rows[0][0]) != "0" ||
		string(rows[1][0]) != "1" || string(rows[2][0]) != "2" ||
		string(rows[3][0]) != "4" {
		t.Fatalf("extended leading VALUES rows = %q", rows)
	}

	msgs = c.query(`TABLE set_a`)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("bare TABLE: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}
	description = decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	rows = rowsOf(t, msgs)
	if len(description) != 1 || description[0].name != "*" || len(rows) != 3 ||
		!strings.Contains(string(rows[0][0]), `"id":"a1"`) {
		t.Fatalf("TABLE description/rows = %+v/%q", description, rows)
	}

	statement := `/* préfix */ VALUES (1) UNION SELECT id, n FROM set_a`
	fields := expectError(t, c.query(statement), sqlstateSyntaxError)
	bytePosition := strings.Index(statement, "UNION")
	want := strconv.Itoa(len([]rune(statement[:bytePosition])) + 1)
	if fields['P'] != want {
		t.Fatalf("VALUES arity position = %q, want %q", fields['P'], want)
	}
	unsupported := `/* préfix */ VALUES (missing) UNION VALUES (1)`
	fields = expectError(t, c.query(unsupported), sqlstateFeatureNotSupported)
	bytePosition = strings.Index(unsupported, "missing")
	want = strconv.Itoa(len([]rune(unsupported[:bytePosition])) + 1)
	if fields['P'] != want {
		t.Fatalf("VALUES unsupported position = %q, want %q", fields['P'], want)
	}
	if got := commandTagOf(t, c.query(`VALUES (7)`)); got != "SELECT 1" {
		t.Fatalf("post-VALUES-error recovery tag = %q", got)
	}

	planRows := rowsOf(t, c.query(
		`EXPLAIN ANALYZE VALUES (1), (2) UNION ALL VALUES (3)`,
	))
	if len(planRows) != 1 || !strings.Contains(
		string(planRows[0][0]), `\"kind\":\"values\"`,
	) || !strings.Contains(string(planRows[0][0]), `\"rows\":3`) {
		t.Fatalf("VALUES EXPLAIN ANALYZE rows = %q", planRows)
	}
}

func TestSQLSetValuesIntermediateBudgetNoPartialAndRecovery(t *testing.T) {
	srv := newTestServer(t, Options{MaxIntermediateBytes: 1})
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})
	for _, msgs := range [][]backendMessage{
		c.query(`VALUES (1), (2) UNION ALL VALUES (3)`),
		extendedSQL(c, `VALUES ($1), (2) UNION ALL VALUES (3)`, [][]byte{[]byte("1")}),
	} {
		expectError(t, msgs, sqlstateProgramLimitExceeded)
		if has(msgs, msgDataRow) {
			t.Fatal("VALUES budget failure emitted a partial DataRow")
		}
	}
	// The same one-byte intermediate limit would reject another VALUES root.
	// Use the protocol's fixed SELECT path to prove synchronization instead.
	if got := commandTagOf(t, c.query(`SELECT 1`)); got != "SELECT 1" {
		t.Fatalf("post-VALUES-budget recovery tag = %q", got)
	}
}

func TestSQLSetSimpleExtendedAndDuplicateRowDescription(t *testing.T) {
	c := connectSQLCatalog(t)
	seedProtocolSetTables(t, c)

	msgs := c.query(`
		(SELECT id AS item FROM set_a ORDER BY item DESC LIMIT 2)
		UNION ALL SELECT id FROM set_b
		ORDER BY item LIMIT 4 OFFSET 1`)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("simple set: %s", formatError(find(t, msgs, msgErrorResponse).body))
	}
	rows := rowsOf(t, msgs)
	if len(rows) != 4 || string(rows[0][0]) != `"a4"` ||
		string(rows[1][0]) != `"b2"` || string(rows[2][0]) != `"b3"` ||
		string(rows[3][0]) != `"b4"` {
		t.Fatalf("simple set rows = %q tags=%s", rows, tags(msgs))
	}

	msgs = extendedSQL(c, `
		SELECT id AS item FROM set_a WHERE n >= $1
		UNION DISTINCT SELECT id FROM set_b WHERE n <= $2
		ORDER BY item DESC LIMIT $3`,
		[][]byte{[]byte("2"), []byte("3"), []byte("3")},
	)
	rows = rowsOf(t, msgs)
	if len(rows) != 3 || string(rows[0][0]) != `"b3"` ||
		string(rows[1][0]) != `"b2"` || string(rows[2][0]) != `"a4"` {
		t.Fatalf("extended set rows = %q", rows)
	}

	msgs = c.query(`
		SELECT id AS id, n AS id FROM set_a
		UNION ALL SELECT id, n FROM set_b LIMIT 1`)
	description := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if len(description) != 2 || description[0].name != "id" || description[1].name != "id" {
		t.Fatalf("duplicate set RowDescription = %+v, want [id id]", description)
	}
	if planRows := rowsOf(t, c.query(
		`EXPLAIN SELECT id FROM set_a INTERSECT SELECT id FROM set_b`,
	)); len(planRows) != 1 || !strings.Contains(string(planRows[0][0]), `\"node\":\"set\"`) {
		t.Fatalf("set EXPLAIN rows = %q", planRows)
	}
}

func TestSQLSetAritySQLStateUTF8PositionAndRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	seedProtocolSetTables(t, c)
	statement := `/* préfix */ SELECT d.* FROM (` +
		`SELECT id, n FROM set_a) d UNION SELECT id FROM set_b`
	fields := expectError(t, c.query(statement), sqlstateSyntaxError)
	bytePosition := strings.Index(statement, "UNION")
	want := strconv.Itoa(len([]rune(statement[:bytePosition])) + 1)
	if fields['P'] != want {
		t.Fatalf("set arity position = %q, want %q", fields['P'], want)
	}
	if got := commandTagOf(t, c.query(`SELECT id FROM set_a WHERE id = 'a1'`)); got != "SELECT 1" {
		t.Fatalf("post-arity recovery tag = %q", got)
	}
}

func TestSQLSetIntermediateBudgetNoPartialRowsAndRecovery(t *testing.T) {
	srv := newTestServer(t, Options{MaxIntermediateBytes: 1})
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})
	statement := `SELECT id FROM users UNION ALL SELECT id FROM users`
	for _, msgs := range [][]backendMessage{
		c.query(statement),
		extendedSQL(c, statement, nil),
	} {
		expectError(t, msgs, sqlstateProgramLimitExceeded)
		if has(msgs, msgDataRow) {
			t.Fatal("set intermediate-budget failure emitted a partial DataRow")
		}
	}
	if got := commandTagOf(t, c.query(`SELECT 1`)); got != "SELECT 1" {
		t.Fatalf("post-set-budget recovery tag = %q", got)
	}
}

func TestSQLSetCancellationNoPartialRowsAndRecovery(t *testing.T) {
	c, server := connectSQLCatalogWithServer(t)
	var statement strings.Builder
	for leaf := 0; leaf < 256; leaf++ {
		if leaf != 0 {
			statement.WriteString(" UNION ALL ")
		}
		statement.WriteString("SELECT id FROM users")
	}
	server.mu.Lock()
	session := server.sessions[c.pid]
	server.mu.Unlock()
	if session == nil {
		t.Fatal("set cancellation session is not registered")
	}
	c.send(msgQuery, append([]byte(statement.String()), 0))
	deadline := time.Now().Add(2 * time.Second)
	for {
		session.cancelMu.Lock()
		active := session.cancelActive
		session.cancelMu.Unlock()
		if active {
			server.cancelRequest(c.pid, c.secret)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("set query never entered its cancellation window")
		}
		runtime.Gosched()
	}
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateQueryCanceled)
	if has(msgs, msgDataRow) {
		t.Fatal("canceled set query emitted a partial DataRow")
	}
	if got := commandTagOf(t, c.query(`SELECT 1`)); got != "SELECT 1" {
		t.Fatalf("post-set-cancellation recovery tag = %q", got)
	}
}
