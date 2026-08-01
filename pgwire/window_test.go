package pgwire

import (
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func seedWindowProtocol(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE events (` +
			`id STRING PRIMARY KEY, team STRING, score NUMBER, value NUMBER)`,
		`INSERT INTO events VALUES ` +
			`({"id":"a","team":"x","score":1,"value":0.1}),` +
			`({"id":"b","team":"x","score":1.0,"value":0.20}),` +
			`({"id":"c","team":"x","score":2,"value":1}),` +
			`({"id":"d","team":"y","score":1,"value":7})`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}
}

func TestWindowSimpleExtendedPeerDefaultsExactDecimalsAndRowDescription(t *testing.T) {
	c := connectSQLCatalog(t)
	seedWindowProtocol(t, c)

	messages := c.query(`
		SELECT id,
			SUM(value) OVER (PARTITION BY team ORDER BY score) AS peer_sum,
			ROW_NUMBER() OVER (PARTITION BY team ORDER BY score) AS position
		FROM events ORDER BY id`)
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 3 || description[0].name != "id" ||
		description[1].name != "peer_sum" || description[2].name != "position" ||
		description[0].oid != oidJSON || description[1].oid != oidJSON ||
		description[2].oid != oidInt8 || description[2].size != 8 ||
		description[0].format != formatText || description[1].format != formatText ||
		description[2].format != formatText {
		t.Fatalf("window RowDescription = %+v", description)
	}
	rows := rowsOf(t, messages)
	want := [][]string{
		{`"a"`, `0.3`, `1`},
		{`"b"`, `0.3`, `2`},
		{`"c"`, `1.3`, `3`},
		{`"d"`, `7`, `1`},
	}
	assertWindowProtocolRows(t, rows, want)
	if got := commandTagOf(t, messages); got != "SELECT 4" {
		t.Fatalf("simple window tag = %q, want SELECT 4", got)
	}

	messages = extendedSQL(c, `
		SELECT id,
			LAG(value, $1, NULL) OVER (PARTITION BY team ORDER BY score) AS previous,
			NTILE($2) OVER (PARTITION BY team ORDER BY score) AS tile,
			SUM(value) OVER (
				PARTITION BY team ORDER BY score
				ROWS BETWEEN $3 PRECEDING AND CURRENT ROW
			) AS rolling
		FROM events ORDER BY id`,
		[][]byte{[]byte("1"), []byte("2"), []byte("1")},
	)
	description = decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 4 || description[0].name != "id" ||
		description[1].name != "previous" || description[2].name != "tile" ||
		description[3].name != "rolling" ||
		description[0].oid != oidJSON || description[1].oid != oidJSON ||
		description[2].oid != oidInt8 || description[3].oid != oidJSON {
		t.Fatalf("extended window RowDescription = %+v", description)
	}
	rows = rowsOf(t, messages)
	if len(rows) != 4 || rows[0][1] != nil || string(rows[0][2]) != "1" ||
		string(rows[0][3]) != "0.1" || string(rows[1][1]) != "0.1" ||
		string(rows[1][2]) != "1" || string(rows[1][3]) != "0.3" ||
		string(rows[2][2]) != "2" || string(rows[2][3]) != "1.2" {
		t.Fatalf("extended window rows = %q", rows)
	}
}

func TestWindowPreparedTransactionReuseAndRuntimeSQLStateRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	seedWindowProtocol(t, c)
	statement := `
		SELECT id, ROW_NUMBER() OVER (PARTITION BY team ORDER BY score) AS position
		FROM events WHERE team = $1 ORDER BY id`
	c.send(msgParse, parseMsg("window-reuse", statement))
	c.send(msgSync, nil)
	if messages := c.until(msgReadyForQuery); has(messages, msgErrorResponse) ||
		!has(messages, msgParseComplete) {
		t.Fatalf("window Parse: %s", tags(messages))
	}
	execute := func() [][]byte {
		c.send(msgBind, bindMsg("", "window-reuse", nil, [][]byte{[]byte("x")}, nil))
		c.send(msgExecute, executeMsg("", 0))
		c.send(msgSync, nil)
		messages := c.until(msgReadyForQuery)
		if has(messages, msgErrorResponse) {
			t.Fatalf("window execute: %s", formatError(find(t, messages, msgErrorResponse).body))
		}
		rows := rowsOf(t, messages)
		ids := make([][]byte, len(rows))
		for i := range rows {
			ids[i] = rows[i][0]
		}
		return ids
	}
	if rows := execute(); len(rows) != 3 {
		t.Fatalf("prepared window rows = %q, want 3", rows)
	}
	if messages := c.query(`BEGIN`); has(messages, msgErrorResponse) {
		t.Fatalf("BEGIN: %s", tags(messages))
	}
	if messages := c.query(`INSERT INTO events VALUES (` +
		`{"id":"pending","team":"x","score":3,"value":2})`); has(messages, msgErrorResponse) {
		t.Fatalf("transaction INSERT: %s", tags(messages))
	}
	if rows := execute(); len(rows) != 4 || string(rows[3]) != `"pending"` {
		t.Fatalf("transaction prepared window rows = %q", rows)
	}
	if messages := c.query(`ROLLBACK`); has(messages, msgErrorResponse) {
		t.Fatalf("ROLLBACK: %s", tags(messages))
	}
	if rows := execute(); len(rows) != 3 {
		t.Fatalf("prepared window rows after rollback = %q, want 3", rows)
	}

	if got := asPGError(&query.WindowArgumentError{Clause: "NTILE bucket count"}).code; got != sqlstateInvalidParameterValue {
		t.Fatalf("WindowArgumentError SQLSTATE = %s, want %s",
			got, sqlstateInvalidParameterValue)
	}
	messages := extendedSQL(c,
		`SELECT NTILE($1) OVER (ORDER BY score) AS tile FROM events`,
		[][]byte{[]byte("0")},
	)
	expectError(t, messages, sqlstateInvalidParameterValue)
	if has(messages, msgDataRow) || has(messages, msgCommandComplete) {
		t.Fatalf("invalid NTILE emitted rows/completion: %s", tags(messages))
	}
	if got := commandTagOf(t, c.query(`SELECT id FROM events WHERE id = 'a'`)); got != "SELECT 1" {
		t.Fatalf("post-window-error recovery tag = %q, want SELECT 1", got)
	}
}

func TestWindowProtocolBudgetAndCancellationPublishNoRowsAndRecover(t *testing.T) {
	budgetServer := newTestServer(t, Options{MaxIntermediateBytes: 1})
	budgetClient := dial(t, budgetServer)
	budgetClient.startup(map[string]string{"user": "tester"})
	statement := `SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS position FROM users`
	for _, messages := range [][]backendMessage{
		budgetClient.query(statement),
		extendedSQL(budgetClient, statement, nil),
	} {
		expectError(t, messages, sqlstateProgramLimitExceeded)
		if has(messages, msgDataRow) {
			t.Fatal("window budget failure emitted a partial DataRow")
		}
	}
	if got := commandTagOf(t, budgetClient.query(`SELECT 1`)); got != "SELECT 1" {
		t.Fatalf("post-window-budget recovery tag = %q, want SELECT 1", got)
	}

	c, server := connectSQLCatalogWithServer(t)
	seedWindowProtocol(t, c)
	server.mu.Lock()
	session := server.sessions[c.pid]
	server.mu.Unlock()
	if session == nil {
		t.Fatal("window protocol session is not registered")
	}
	var cancel query.CancelFlag
	if err := session.sql.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.sql.SetCancelFlag(&session.queryCancel); err != nil {
			t.Error(err)
		}
	}()

	c.send(msgParse, parseMsg("cancel-window", `
		SELECT id, SUM(value) OVER (PARTITION BY team ORDER BY score) AS running
		FROM events`))
	c.send(msgSync, nil)
	if messages := c.until(msgReadyForQuery); has(messages, msgErrorResponse) {
		t.Fatalf("cancel-window Parse: %s", tags(messages))
	}
	cancel.Cancel()
	c.send(msgBind, bindMsg("", "cancel-window", nil, nil, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	expectError(t, messages, sqlstateQueryCanceled)
	if has(messages, msgDataRow) {
		t.Fatal("canceled window emitted a partial DataRow")
	}
	cancel.Reset()
	if got := commandTagOf(t, c.query(`SELECT id FROM events WHERE id = 'a'`)); got != "SELECT 1" {
		t.Fatalf("post-window-cancel recovery tag = %q, want SELECT 1", got)
	}
}

func assertWindowProtocolRows(t testing.TB, rows []decodedRow, want [][]string) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("window rows = %q, want %v", rows, want)
	}
	for i := range want {
		if len(rows[i]) != len(want[i]) {
			t.Fatalf("window row %d width = %d, want %d", i, len(rows[i]), len(want[i]))
		}
		for j := range want[i] {
			if string(rows[i][j]) != want[i][j] {
				t.Fatalf("window row %d column %d = %q, want %q; all rows %s",
					i, j, rows[i][j], want[i][j], strings.TrimSpace(stringRows(rows)))
			}
		}
	}
}

func stringRows(rows []decodedRow) string {
	var result strings.Builder
	for i := range rows {
		if i != 0 {
			result.WriteByte(' ')
		}
		result.WriteByte('[')
		for j := range rows[i] {
			if j != 0 {
				result.WriteByte(' ')
			}
			result.Write(rows[i][j])
		}
		result.WriteByte(']')
	}
	return result.String()
}
