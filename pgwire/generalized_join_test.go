package pgwire

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func seedGeneralizedJoinProtocol(t *testing.T, c *testClient) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE accounts (` +
			`id STRING PRIMARY KEY, tenant STRING, name STRING)`,
		`CREATE TABLE orders (` +
			`id STRING PRIMARY KEY, account_id STRING, tenant STRING, total INTEGER)`,
		`CREATE TABLE payments (` +
			`id STRING PRIMARY KEY, order_id STRING, state STRING)`,
		`INSERT INTO accounts VALUES ` +
			`({"id":"a1","tenant":"t1","name":"Ada"}),` +
			`({"id":"a2","tenant":"t1","name":"Bob"}),` +
			`({"id":"a3","tenant":"t2","name":"Cara"})`,
		`INSERT INTO orders VALUES ` +
			`({"id":"o1","account_id":"a1","tenant":"t1","total":10}),` +
			`({"id":"o2","account_id":"a1","tenant":"t1","total":20}),` +
			`({"id":"o3","account_id":"a2","tenant":"t1","total":30}),` +
			`({"id":"orphan","account_id":"absent","tenant":"t9","total":40})`,
		`INSERT INTO payments VALUES ` +
			`({"id":"p1","order_id":"o1","state":"settled"}),` +
			`({"id":"p2","order_id":"o2","state":"failed"}),` +
			`({"id":"p3","order_id":"orphan","state":"settled"})`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
}

func TestGeneralizedJoinSimpleExtendedAndDuplicateRowDescription(t *testing.T) {
	c := connectSQLCatalog(t)
	seedGeneralizedJoinProtocol(t, c)
	statement := `
		WITH eligible AS (SELECT id, name FROM accounts WHERE tenant = 't1')
		SELECT eligible.name, o.id, p.state
		FROM eligible
		JOIN (SELECT id, account_id, total FROM orders WHERE total >= $1) AS o
			ON eligible.id = o.account_id AND o.total <= $2
		LEFT JOIN payments AS p
			ON o.id = p.order_id AND p.state = 'settled'
		ORDER BY o.id`
	msgs := extendedSQL(c, statement, [][]byte{[]byte("10"), []byte("30")})
	rows := rowsOf(t, msgs)
	if len(rows) != 3 ||
		string(rows[0][0]) != `"Ada"` || string(rows[0][1]) != `"o1"` || string(rows[0][2]) != `"settled"` ||
		string(rows[1][0]) != `"Ada"` || string(rows[1][1]) != `"o2"` || rows[1][2] != nil ||
		string(rows[2][0]) != `"Bob"` || string(rows[2][1]) != `"o3"` || rows[2][2] != nil {
		t.Fatalf("extended generalized join rows = %q", rows)
	}

	msgs = c.query(`
		SELECT a.id, o.id
		FROM accounts AS a
		FULL JOIN orders AS o ON a.id = o.account_id`)
	if got := len(rowsOf(t, msgs)); got != 5 {
		t.Fatalf("simple FULL JOIN row count = %d, want 5", got)
	}
	msgs = c.query(`SELECT a.id, p.id FROM accounts AS a CROSS JOIN payments AS p`)
	if got := len(rowsOf(t, msgs)); got != 9 {
		t.Fatalf("simple CROSS JOIN row count = %d, want 9", got)
	}

	msgs = c.query(`
		SELECT a.id AS id, o.id AS id
		FROM accounts AS a
		JOIN orders AS o USING (tenant, id)`)
	description := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	if len(description) != 2 || description[0].name != "id" || description[1].name != "id" {
		t.Fatalf("duplicate generalized join RowDescription = %+v, want [id id]", description)
	}
}

func TestGeneralizedJoinPreparedPostDropRevalidation(t *testing.T) {
	c := connectSQLCatalog(t)
	seedGeneralizedJoinProtocol(t, c)
	statement := `
		WITH eligible AS (SELECT id FROM accounts)
		SELECT eligible.id, p.state
		FROM eligible
		JOIN (SELECT id, account_id FROM orders) AS o
			ON eligible.id = o.account_id
		LEFT JOIN payments AS p ON o.id = p.order_id`
	c.send(msgParse, parseMsg("generalized-join", statement))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if has(msgs, msgErrorResponse) || !has(msgs, msgParseComplete) {
		t.Fatalf("generalized join Parse: %s", tags(msgs))
	}
	if msgs := c.query(`DROP TABLE payments`); has(msgs, msgErrorResponse) {
		t.Fatalf("DROP TABLE payments: %s", tags(msgs))
	}
	c.send(msgBind, bindMsg("generalized-join-portal", "generalized-join", nil, nil, nil))
	c.send(msgExecute, executeMsg("generalized-join-portal", 0))
	c.send(msgSync, nil)
	msgs = c.until(msgReadyForQuery)
	fields := expectError(t, msgs, sqlstateUndefinedTable)
	want := strconv.Itoa(len([]rune(statement[:strings.Index(statement, "payments")])) + 1)
	if fields['P'] != want {
		t.Fatalf("post-DROP joined dependency position = %q, want %q", fields['P'], want)
	}
	if got := commandTagOf(t, c.query(`SELECT id FROM accounts WHERE id = 'a1'`)); got != "SELECT 1" {
		t.Fatalf("post-DROP prepared recovery tag = %q, want SELECT 1", got)
	}
}

func TestGeneralizedJoinTransactionSnapshotAndReadYourWrites(t *testing.T) {
	c, server := connectSQLCatalogWithServer(t)
	seedGeneralizedJoinProtocol(t, c)
	outside := dial(t, server)
	outside.startup(map[string]string{"user": "outside", "database": "app"})
	if msgs := c.query(`BEGIN ISOLATION LEVEL REPEATABLE READ`); has(msgs, msgErrorResponse) {
		t.Fatalf("BEGIN: %s", tags(msgs))
	}
	if msgs := outside.query(`INSERT INTO orders VALUES (` +
		`{"id":"outside","account_id":"a1","tenant":"t1","total":50})`); has(msgs, msgErrorResponse) {
		t.Fatalf("outside INSERT: %s", tags(msgs))
	}
	if msgs := c.query(`INSERT INTO orders VALUES (` +
		`{"id":"pending","account_id":"a1","tenant":"t1","total":60})`); has(msgs, msgErrorResponse) {
		t.Fatalf("transaction INSERT: %s", tags(msgs))
	}
	msgs := c.query(`
		WITH selected AS MATERIALIZED (
			SELECT id, account_id FROM orders
		)
		SELECT o.id
		FROM accounts AS a
		JOIN selected AS o ON a.id = o.account_id
		WHERE a.id = 'a1'
		ORDER BY o.id`)
	rows := rowsOf(t, msgs)
	if len(rows) != 3 || string(rows[0][0]) != `"o1"` ||
		string(rows[1][0]) != `"o2"` || string(rows[2][0]) != `"pending"` {
		t.Fatalf("transaction generalized join rows = %q, want [o1 o2 pending]", rows)
	}
	if msgs := c.query(`ROLLBACK`); has(msgs, msgErrorResponse) {
		t.Fatalf("ROLLBACK: %s", tags(msgs))
	}
}

func TestGeneralizedJoinUTF8ColumnErrorsAndRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	seedGeneralizedJoinProtocol(t, c)
	for _, test := range []struct {
		name, statement, marker, code string
	}{
		{
			name: "undefined joined output",
			statement: `/* préfix */ SELECT o.missing FROM accounts AS a ` +
				`JOIN (SELECT id, account_id FROM orders) AS o ` +
				`ON a.id = o.account_id`,
			marker: "missing", code: sqlstateUndefinedColumn,
		},
		{
			name: "ambiguous output",
			statement: `/* préfix */ SELECT d.id FROM (` +
				`SELECT id, account_id AS id FROM orders` +
				`) AS d ` +
				`JOIN payments AS p ON d.id = p.order_id`,
			marker: "d.id = p.order_id", code: sqlstateAmbiguousColumn,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fields := expectError(t, c.query(test.statement), test.code)
			bytePosition := strings.Index(test.statement, test.marker)
			want := strconv.Itoa(len([]rune(test.statement[:bytePosition])) + 1)
			if fields['P'] != want {
				t.Fatalf("position = %q, want %q", fields['P'], want)
			}
		})
	}
	if got := commandTagOf(t, c.query(`SELECT id FROM accounts WHERE id = 'a1'`)); got != "SELECT 1" {
		t.Fatalf("post-column-error recovery tag = %q, want SELECT 1", got)
	}
}

func TestGeneralizedJoinIntermediateLimitNoPartialRowsAndRecovery(t *testing.T) {
	srv := newTestServer(t, Options{MaxIntermediateBytes: 1})
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})
	if msgs := c.query(`CREATE TABLE orders (` +
		`id STRING PRIMARY KEY, user_id STRING)`); has(msgs, msgErrorResponse) {
		t.Fatalf("CREATE TABLE orders: %s", tags(msgs))
	}
	if msgs := c.query(`INSERT INTO orders VALUES ` +
		`({"id":"o1","user_id":"1"})`); has(msgs, msgErrorResponse) {
		t.Fatalf("INSERT orders: %s", tags(msgs))
	}
	statement := `
		SELECT u._pgwire_key, o.id
		FROM users AS u
		JOIN (SELECT id, user_id FROM orders) AS o
			ON u._pgwire_key = o.user_id`
	for _, msgs := range [][]backendMessage{
		c.query(statement),
		extendedSQL(c, statement, nil),
	} {
		expectError(t, msgs, sqlstateProgramLimitExceeded)
		if has(msgs, msgDataRow) {
			t.Fatal("generalized join budget failure emitted a partial DataRow")
		}
	}
	if got := commandTagOf(t, c.query(`SELECT 1`)); got != "SELECT 1" {
		t.Fatalf("post-budget recovery tag = %q, want SELECT 1", got)
	}
}

func TestGeneralizedJoinCancellationPublishesNoRowsAndRecovers(t *testing.T) {
	c, server := connectSQLCatalogWithServer(t)
	for _, statement := range []string{
		`CREATE TABLE cancel_left (id STRING PRIMARY KEY)`,
		`CREATE TABLE cancel_right (id STRING PRIMARY KEY)`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement, tags(msgs))
		}
	}
	for _, table := range []string{"cancel_left", "cancel_right"} {
		for base := 0; base < 1024; base += 64 {
			var insert strings.Builder
			insert.WriteString("INSERT INTO ")
			insert.WriteString(table)
			insert.WriteString(" VALUES ")
			for i := range 64 {
				if i != 0 {
					insert.WriteByte(',')
				}
				insert.WriteString(`({"id":"`)
				insert.WriteString(strconv.Itoa(base + i))
				insert.WriteString(`"})`)
			}
			if msgs := c.query(insert.String()); has(msgs, msgErrorResponse) {
				t.Fatalf("seed %s: %s", table,
					formatError(find(t, msgs, msgErrorResponse).body))
			}
		}
	}
	server.mu.Lock()
	sess := server.sessions[c.pid]
	server.mu.Unlock()
	if sess == nil {
		t.Fatal("generalized join session is not registered")
	}
	if err := sess.sql.SetIntermediateLimit(UnlimitedResults); err != nil {
		t.Fatalf("disable intermediate limit for cancellation fixture: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		sess.cancelMu.Lock()
		active := sess.cancelActive
		sess.cancelMu.Unlock()
		if !active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("seed command did not leave its cancellation window")
		}
		runtime.Gosched()
	}

	c.send(msgQuery, append([]byte(
		`SELECT COUNT(*) FROM cancel_left AS l JOIN cancel_right AS r `+
			`ON l.id < r.id AND l.id > r.id`,
	), 0))
	deadline = time.Now().Add(2 * time.Second)
	for {
		active := false
		sess.cancelMu.Lock()
		active = sess.cancelActive
		sess.cancelMu.Unlock()
		if active {
			server.cancelRequest(c.pid, c.secret)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("generalized join never entered its cancellation window")
		}
		runtime.Gosched()
	}
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateQueryCanceled)
	if has(msgs, msgDataRow) {
		t.Fatal("canceled generalized join emitted a partial DataRow")
	}
	if got := commandTagOf(t, c.query(`SELECT 1`)); got != "SELECT 1" {
		t.Fatalf("post-cancellation recovery tag = %q, want SELECT 1", got)
	}
}
