package pgwire

import (
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/conformance"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestSQLCatalogSimpleAndExtendedLifecycle(t *testing.T) {
	c := connectSQLCatalog(t)

	if got := commandTagOf(t, c.query(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			name STRING NOT NULL,
			kind STRING,
			n INTEGER
		)`)); got != "CREATE TABLE" {
		t.Fatalf("CREATE TABLE tag = %q", got)
	}
	if got := commandTagOf(t,
		c.query(`CREATE INDEX by_kind ON docs(kind)`)); got != "CREATE INDEX" {
		t.Fatalf("CREATE INDEX tag = %q", got)
	}
	expectError(t, c.query(`CREATE INDEX by_kind ON docs(kind)`),
		sqlstateDuplicateObject)
	if got := commandTagOf(t,
		c.query(`CREATE INDEX IF NOT EXISTS by_kind ON docs(kind)`)); got != "CREATE INDEX" {
		t.Fatalf("CREATE INDEX IF NOT EXISTS tag = %q", got)
	}

	insert := extendedSQL(c, `INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"a","name":"Ada","kind":"person","n":7}`)})
	if got := commandTagOf(t, insert); got != "INSERT 0 1" {
		t.Fatalf("whole-document INSERT tag = %q", got)
	}
	if got := decodeParameterDescription(t,
		find(t, insert, msgParameterDesc).body); !slices.Equal(got, []int32{oidJSON}) {
		t.Fatalf("document ParameterDescription = %v, want [%d]", got, oidJSON)
	}

	flat := extendedSQL(c,
		`INSERT INTO docs (id, name, kind, n) VALUES ($1, $2, $3, $4)`,
		[][]byte{[]byte("b"), []byte("Grace"), []byte("person"), []byte("9")})
	if got := commandTagOf(t, flat); got != "INSERT 0 1" {
		t.Fatalf("flat INSERT tag = %q", got)
	}
	if got := decodeParameterDescription(t,
		find(t, flat, msgParameterDesc).body); !slices.Equal(got, []int32{0, 0, 0, 0}) {
		t.Fatalf("flat ParameterDescription = %v, want four unspecified scalars", got)
	}

	rows := rowsOf(t, c.query(
		`SELECT id, name, n FROM docs WHERE kind = 'person' ORDER BY id`))
	if len(rows) != 2 ||
		string(rows[0][0]) != `"a"` || string(rows[0][1]) != `"Ada"` ||
		string(rows[1][0]) != `"b"` || string(rows[1][2]) != "9" {
		t.Fatalf("indexed SELECT rows = %q", rows)
	}

	update := extendedSQL(c,
		`UPDATE docs SET "$doc" = $1 WHERE id = $2`,
		[][]byte{
			[]byte(`{"id":"a","name":"Augusta","kind":"person","n":8}`),
			[]byte("a"),
		})
	if got := commandTagOf(t, update); got != "UPDATE 1" {
		t.Fatalf("UPDATE tag = %q", got)
	}
	if got := decodeParameterDescription(t,
		find(t, update, msgParameterDesc).body); !slices.Equal(got, []int32{oidJSON, 0}) {
		t.Fatalf("UPDATE ParameterDescription = %v, want [json unspecified]", got)
	}

	remove := extendedSQL(c, `DELETE FROM docs WHERE id = $1`,
		[][]byte{[]byte("b")})
	if got := commandTagOf(t, remove); got != "DELETE 1" {
		t.Fatalf("DELETE tag = %q", got)
	}
	rows = rowsOf(t, c.query(`SELECT name, n FROM docs WHERE id = 'a'`))
	if len(rows) != 1 || string(rows[0][0]) != `"Augusta"` ||
		string(rows[0][1]) != "8" {
		t.Fatalf("updated row = %q", rows)
	}
	if got := commandTagOf(t,
		c.query(`CREATE INDEX by_n_online ON docs(n)`)); got != "CREATE INDEX" {
		t.Fatalf("online CREATE INDEX tag = %q", got)
	}
	rows = rowsOf(t, c.query(`SELECT id FROM docs WHERE n = 8`))
	if len(rows) != 1 || string(rows[0][0]) != `"a"` {
		t.Fatalf("online indexed rows = %q", rows)
	}
}

func TestSQLCatalogTruncateAndDropIndexProtocol(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING)`,
		`CREATE INDEX by_kind ON docs (kind)`,
		`INSERT INTO docs VALUES ('{"id":"a","kind":"x"}')`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}

	if got := commandTagOf(t, c.query(`DROP INDEX by_kind ON docs`)); got != "DROP INDEX" {
		t.Fatalf("DROP INDEX tag = %q", got)
	}
	expectError(t, c.query(`DROP INDEX by_kind ON docs`), sqlstateUndefinedObject)
	if got := commandTagOf(t, c.query(`CREATE INDEX by_kind ON docs (kind)`)); got != "CREATE INDEX" {
		t.Fatalf("recreated index tag = %q", got)
	}

	truncated := extendedSQL(c, `TRUNCATE TABLE docs`, nil)
	if got := commandTagOf(t, truncated); got != "TRUNCATE TABLE" {
		t.Fatalf("extended TRUNCATE tag = %q", got)
	}
	rows := rowsOf(t, c.query(`SELECT COUNT(*) FROM docs`))
	if len(rows) != 1 || len(rows[0]) != 1 || string(rows[0][0]) != "0" {
		t.Fatalf("rows after TRUNCATE = %q, want [[0]]", rows)
	}

	dropped := extendedSQL(c, `DROP INDEX IF EXISTS by_kind ON docs`, nil)
	if got := commandTagOf(t, dropped); got != "DROP INDEX" {
		t.Fatalf("extended DROP INDEX tag = %q", got)
	}

	for _, statement := range []string{
		`CREATE TABLE alpha (id STRING PRIMARY KEY)`,
		`CREATE TABLE beta (id STRING PRIMARY KEY)`,
		`CREATE INDEX shared_name ON alpha (id)`,
		`CREATE INDEX shared_name ON beta (id)`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	expectError(t, c.query(`DROP INDEX shared_name`), sqlstateAmbiguousAlias)
}

func TestSQLCatalogIndexedImplicitAndExplicitBatches(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING NOT NULL)`,
		`CREATE INDEX by_kind ON docs(kind)`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	requireOK := func(label string, msgs []backendMessage) {
		t.Helper()
		if has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", label,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	assertIDs := func(kind string, want ...string) {
		t.Helper()
		rows := rowsOf(t, c.query(
			`SELECT id FROM docs WHERE kind = '`+kind+`' ORDER BY id`))
		if len(rows) != len(want) {
			t.Fatalf("kind %q returned %q, want %v", kind, rows, want)
		}
		for i := range rows {
			if got := string(rows[i][0]); got != `"`+want[i]+`"` {
				t.Fatalf("kind %q row %d = %s, want %q", kind, i, got, want[i])
			}
		}
	}

	// An explicit transaction is also the table's first write, exercising atomic
	// indexed batch materialization rather than a different first-write fallback.
	assertReadyStatus(t, c.query("BEGIN"), statusInTx)
	requireOK("first indexed insert", extendedSQL(c,
		`INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"a","kind":"seed"}`)}))
	requireOK("second indexed insert", extendedSQL(c,
		`INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"b","kind":"seed"}`)}))
	commit := c.query("COMMIT")
	requireOK("first indexed COMMIT", commit)
	assertReadyStatus(t, commit, statusIdle)
	assertIDs("seed", "a", "b")

	// One simple Query message is an implicit transaction. Its multiple indexed
	// keys must become visible together when the message commits.
	implicit := c.query(`
		INSERT INTO docs (id, kind) VALUES ('c', 'inserted');
		INSERT INTO docs (id, kind) VALUES ('d', 'inserted');
		DELETE FROM docs WHERE id = 'b'`)
	requireOK("implicit indexed batch", implicit)
	assertReadyStatus(t, implicit, statusIdle)
	assertIDs("seed", "a")
	assertIDs("inserted", "c", "d")

	assertReadyStatus(t, c.query("BEGIN"), statusInTx)
	requireOK("indexed update", extendedSQL(c,
		`UPDATE docs SET "$doc" = $1 WHERE id = $2`,
		[][]byte{
			[]byte(`{"id":"a","kind":"updated"}`),
			[]byte("a"),
		}))
	requireOK("indexed delete", extendedSQL(c,
		`DELETE FROM docs WHERE id = $1`, [][]byte{[]byte("c")}))
	requireOK("indexed multi-row insert", extendedSQL(c,
		`INSERT INTO docs VALUES ($1), ($2)`,
		[][]byte{
			[]byte(`{"id":"e","kind":"new"}`),
			[]byte(`{"id":"f","kind":"new"}`),
		}))
	commit = c.query("COMMIT")
	requireOK("mixed indexed COMMIT", commit)
	assertReadyStatus(t, commit, statusIdle)
	assertIDs("seed")
	assertIDs("updated", "a")
	assertIDs("inserted", "d")
	assertIDs("new", "e", "f")

	assertReadyStatus(t, c.query("BEGIN"), statusInTx)
	requireOK("rollback indexed delete", extendedSQL(c,
		`DELETE FROM docs WHERE id = $1`, [][]byte{[]byte("d")}))
	requireOK("rollback indexed insert", extendedSQL(c,
		`INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"g","kind":"rolled-back"}`)}))
	rollback := c.query("ROLLBACK")
	requireOK("indexed ROLLBACK", rollback)
	assertReadyStatus(t, rollback, statusIdle)
	assertIDs("inserted", "d")
	assertIDs("rolled-back")
}

func TestSQLCatalogSubqueryRows(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE orders (id STRING PRIMARY KEY, customer STRING)`,
		`CREATE TABLE customers (id STRING PRIMARY KEY, tier STRING)`,
	} {
		if got := commandTagOf(t, c.query(statement)); got != "CREATE TABLE" {
			t.Fatalf("%s tag = %q", statement, got)
		}
	}
	for _, insert := range []struct {
		sql string
		doc string
	}{
		{`INSERT INTO orders VALUES ($1)`, `{"id":"o1","customer":"c1"}`},
		{`INSERT INTO orders VALUES ($1)`, `{"id":"o2","customer":"c2"}`},
		{`INSERT INTO customers VALUES ($1)`, `{"id":"c1","tier":"pro"}`},
		{`INSERT INTO customers VALUES ($1)`, `{"id":"c2","tier":"free"}`},
	} {
		if got := commandTagOf(t,
			extendedSQL(c, insert.sql, [][]byte{[]byte(insert.doc)})); got != "INSERT 0 1" {
			t.Fatalf("insert tag = %q", got)
		}
	}

	rows := rowsOf(t, extendedSQL(c,
		`SELECT id FROM orders WHERE customer IN (`+
			`SELECT id FROM customers WHERE tier = $1)`,
		[][]byte{[]byte("pro")}))
	if len(rows) != 1 || string(rows[0][0]) != `"o1"` {
		t.Fatalf("subquery rows = %q, want o1", rows)
	}
}

func TestSQLCatalogInsertReturning(t *testing.T) {
	c := connectSQLCatalog(t)
	if got := commandTagOf(t, c.query(
		`CREATE TABLE users (id STRING PRIMARY KEY, name STRING NOT NULL)`,
	)); got != "CREATE TABLE" {
		t.Fatalf("CREATE TABLE tag = %q", got)
	}

	returned := extendedSQL(c,
		`INSERT INTO users VALUES ($1), ($2) RETURNING id, name AS display_name`,
		[][]byte{
			[]byte(`{"id":"a","name":"Ada"}`),
			[]byte(`{"id":"b","name":"Grace"}`),
		},
	)
	rows := rowsOf(t, returned)
	if len(rows) != 2 ||
		string(rows[0][0]) != `"a"` || string(rows[0][1]) != `"Ada"` ||
		string(rows[1][0]) != `"b"` || string(rows[1][1]) != `"Grace"` {
		t.Fatalf("INSERT RETURNING rows = %q", rows)
	}
	if got := commandTagOf(t, returned); got != "INSERT 0 2" {
		t.Fatalf("INSERT RETURNING tag = %q, want INSERT 0 2", got)
	}
	description := decodeRowDescription(
		t, find(t, returned, msgRowDescription).body,
	)
	if len(description) != 2 ||
		description[0].name != "id" ||
		description[1].name != "display_name" {
		t.Fatalf("INSERT RETURNING columns = %v", description)
	}

	whole := c.query(
		`INSERT INTO users VALUES ({"id":"c","name":"Lin"}) RETURNING *`,
	)
	rows = rowsOf(t, whole)
	if len(rows) != 1 ||
		string(rows[0][0]) != `{"id":"c","name":"Lin"}` {
		t.Fatalf("simple INSERT RETURNING * rows = %q", rows)
	}
	if got := commandTagOf(t, whole); got != "INSERT 0 1" {
		t.Fatalf("simple INSERT RETURNING tag = %q", got)
	}

	duplicate := extendedSQL(c,
		`INSERT INTO users VALUES ($1), ($2) RETURNING id`,
		[][]byte{
			[]byte(`{"id":"fresh","name":"Fresh"}`),
			[]byte(`{"id":"a","name":"Duplicate"}`),
		},
	)
	expectError(t, duplicate, sqlstateUniqueViolation)
	if rows := rowsOf(t, c.query(
		`SELECT id FROM users WHERE id = 'fresh'`,
	)); len(rows) != 0 {
		t.Fatalf("failed INSERT RETURNING published a row: %q", rows)
	}
}

func TestSQLCatalogLeftJoinNullExtension(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, setup := range []struct {
		statement string
		tag       string
	}{
		{`CREATE TABLE users (id STRING PRIMARY KEY, name STRING NOT NULL)`, "CREATE TABLE"},
		{`CREATE TABLE orders (id STRING PRIMARY KEY, user_id STRING, total INTEGER)`, "CREATE TABLE"},
		{`INSERT INTO users VALUES ({"id":"u1","name":"Ada"}), ({"id":"u2","name":"Lin"})`, "INSERT 0 2"},
		{`INSERT INTO orders VALUES ({"id":"o1","user_id":"u1","total":10})`, "INSERT 0 1"},
	} {
		if got := commandTagOf(t, c.query(setup.statement)); got != setup.tag {
			t.Fatalf("%s tag = %q, want %q", setup.statement, got, setup.tag)
		}
	}

	rows := rowsOf(t, c.query(`
		SELECT u.name, o.total
		FROM users AS u
		LEFT JOIN orders AS o ON u.id = o.user_id
		ORDER BY u.name`))
	if len(rows) != 2 ||
		string(rows[0][0]) != `"Ada"` || string(rows[0][1]) != "10" ||
		string(rows[1][0]) != `"Lin"` || rows[1][1] != nil {
		t.Fatalf("LEFT JOIN rows = %q", rows)
	}
}

func TestSQLCatalogTransactionsAndFailedState(t *testing.T) {
	c := connectSQLCatalog(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)

	assertReadyStatus(t, c.query("BEGIN"), statusInTx)
	insert := extendedSQL(c, `INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"rolled","name":"visible"}`)})
	assertReadyStatus(t, insert, statusInTx)
	if rows := rowsOf(t, c.query(
		`SELECT name FROM docs WHERE id = 'rolled'`)); len(rows) != 1 {
		t.Fatalf("transaction did not read its own write: %q", rows)
	}
	assertReadyStatus(t, c.query("ROLLBACK"), statusIdle)
	if rows := rowsOf(t, c.query(
		`SELECT name FROM docs WHERE id = 'rolled'`)); len(rows) != 0 {
		t.Fatalf("ROLLBACK left rows behind: %q", rows)
	}

	extendedSQL(c, `INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"kept","name":"first"}`)})
	assertReadyStatus(t, c.query("BEGIN"), statusInTx)
	duplicate := extendedSQL(c, `INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"kept","name":"duplicate"}`)})
	expectError(t, duplicate, sqlstateUniqueViolation)
	assertReadyStatus(t, duplicate, statusFailedT)

	failedRead := c.query(`SELECT name FROM docs WHERE id = 'kept'`)
	expectError(t, failedRead, sqlstateFailedTransaction)
	assertReadyStatus(t, failedRead, statusFailedT)
	failedFixed := c.query(`SELECT 1`)
	expectError(t, failedFixed, sqlstateFailedTransaction)
	if has(failedFixed, msgDataRow) {
		t.Fatal("a fixed compatibility SELECT executed inside a failed transaction")
	}
	assertReadyStatus(t, failedFixed, statusFailedT)

	commit := c.query("COMMIT")
	if got := commandTagOf(t, commit); got != "ROLLBACK" {
		t.Fatalf("COMMIT in failed transaction tag = %q, want ROLLBACK", got)
	}
	assertReadyStatus(t, commit, statusIdle)

	assertReadyStatus(t, c.query("BEGIN READ ONLY"), statusInTx)
	readOnlyWrite := extendedSQL(c, `INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"readonly","name":"no"}`)})
	expectError(t, readOnlyWrite, sqlstateReadOnlyTransaction)
	assertReadyStatus(t, readOnlyWrite, statusFailedT)
	assertReadyStatus(t, c.query("ROLLBACK"), statusIdle)
}

func TestSQLCatalogSimpleBatchAtomicityAndDDLGuard(t *testing.T) {
	c := connectSQLCatalog(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)

	msgs := c.query(`
		INSERT INTO docs (id, name) VALUES ('batch', 'first');
		INSERT INTO docs (id, name) VALUES ('batch', 'duplicate')`)
	expectError(t, msgs, sqlstateUniqueViolation)
	assertReadyStatus(t, msgs, statusIdle)
	if rows := rowsOf(t, c.query(
		`SELECT id FROM docs WHERE id = 'batch'`)); len(rows) != 0 {
		t.Fatalf("failed simple batch published its first write: %q", rows)
	}

	msgs = c.query(`
		CREATE TABLE should_not_exist (id STRING PRIMARY KEY);
		CREATE TABLE neither_should_this (id STRING PRIMARY KEY)`)
	expectError(t, msgs, sqlstateFeatureNotSupported)
	missing := c.query(`SELECT * FROM should_not_exist`)
	expectError(t, missing, sqlstateUndefinedTable)

	// Fail closed before execution when explicit controls could otherwise
	// commit in the middle of a Query and leave a later error unable to undo
	// the published rows.
	msgs = c.query(`
		BEGIN;
		INSERT INTO docs (id, name) VALUES ('mid-commit', 'must not publish');
		COMMIT;
		ROLLBACK`)
	expectError(t, msgs, sqlstateFeatureNotSupported)
	assertReadyStatus(t, msgs, statusIdle)
	if rows := rowsOf(t, c.query(
		`SELECT id FROM docs WHERE id = 'mid-commit'`)); len(rows) != 0 {
		t.Fatalf("unsafe transaction-control batch published rows: %q", rows)
	}
}

func TestSQLCatalogDocumentValidationAndParameterRoleConflict(t *testing.T) {
	c := connectSQLCatalog(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)

	invalid := extendedSQL(c, `INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"broken"`)})
	expectError(t, invalid, sqlstateInvalidParameterValue)

	schema := extendedSQL(c, `INSERT INTO docs VALUES ($1)`,
		[][]byte{[]byte(`{"id":"missing-name"}`)})
	expectError(t, schema, sqlstateCheckViolation)

	c.send(msgParse, parseMsg("conflict",
		`UPDATE docs SET "$doc" = $1 WHERE id = $1`))
	c.send(msgSync, nil)
	conflict := c.until(msgReadyForQuery)
	expectError(t, conflict, sqlstateDatatypeMismatch)
}

func TestSQLCatalogExtendedBatchRollsBackAsOneTransaction(t *testing.T) {
	c := connectSQLCatalog(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)

	statement := `INSERT INTO docs VALUES ($1)`
	c.send(msgParse, parseMsg("first", statement))
	c.send(msgBind, bindMsg("p1", "first", nil,
		[][]byte{[]byte(`{"id":"same","name":"first"}`)}, nil))
	c.send(msgExecute, executeMsg("p1", 0))
	c.send(msgParse, parseMsg("second", statement))
	c.send(msgBind, bindMsg("p2", "second", nil,
		[][]byte{[]byte(`{"id":"same","name":"duplicate"}`)}, nil))
	c.send(msgExecute, executeMsg("p2", 0))
	c.send(msgSync, nil)

	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateUniqueViolation)
	assertReadyStatus(t, msgs, statusIdle)
	if rows := rowsOf(t, c.query(
		`SELECT id FROM docs WHERE id = 'same'`)); len(rows) != 0 {
		t.Fatalf("failed extended batch published its first write: %q", rows)
	}
}

func TestSQLCatalogExtendedDDLSoleExecutionBoundary(t *testing.T) {
	c := connectSQLCatalog(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)

	// DML before DDL is still reversible: the DDL is refused before execution
	// and Sync rolls the earlier staged row back.
	c.send(msgParse, parseMsg("staged",
		`INSERT INTO docs (id, name) VALUES ('before-ddl', 'staged')`))
	c.send(msgBind, bindMsg("staged-portal", "staged", nil, nil, nil))
	c.send(msgExecute, executeMsg("staged-portal", 0))
	c.send(msgParse, parseMsg("late-ddl",
		`CREATE TABLE not_created (id STRING PRIMARY KEY)`))
	c.send(msgBind, bindMsg("late-ddl-portal", "late-ddl", nil, nil, nil))
	c.send(msgExecute, executeMsg("late-ddl-portal", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateFeatureNotSupported)
	assertReadyStatus(t, msgs, statusIdle)
	if rows := rowsOf(t, c.query(
		`SELECT id FROM docs WHERE id = 'before-ddl'`)); len(rows) != 0 {
		t.Fatalf("DML preceding refused DDL was published: %q", rows)
	}
	expectError(t, c.query(`SELECT * FROM not_created`), sqlstateUndefinedTable)

	// DDL itself has no transaction-overlay lane and publishes at Execute. A
	// client violating the sole-execution boundary afterwards gets an error,
	// but that later command cannot retroactively erase the completed DDL.
	c.send(msgParse, parseMsg("first-ddl",
		`CREATE TABLE created_first (id STRING PRIMARY KEY)`))
	c.send(msgBind, bindMsg("first-ddl-portal", "first-ddl", nil, nil, nil))
	c.send(msgExecute, executeMsg("first-ddl-portal", 0))
	c.send(msgParse, parseMsg("after-ddl",
		`INSERT INTO docs (id, name) VALUES ('after-ddl', 'refused')`))
	c.send(msgBind, bindMsg("after-ddl-portal", "after-ddl", nil, nil, nil))
	c.send(msgExecute, executeMsg("after-ddl-portal", 0))
	c.send(msgSync, nil)
	msgs = c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateFeatureNotSupported)
	assertReadyStatus(t, msgs, statusIdle)
	if !commandTagsContain(msgs, "CREATE TABLE") {
		t.Fatal("completed DDL did not emit CREATE TABLE before the later refusal")
	}
	if selected := c.query(`SELECT COUNT(*) FROM created_first`); has(selected, msgErrorResponse) {
		t.Fatalf("DDL completed before later refusal was lost: %s",
			formatError(find(t, selected, msgErrorResponse).body))
	}
	if rows := rowsOf(t, c.query(
		`SELECT id FROM docs WHERE id = 'after-ddl'`)); len(rows) != 0 {
		t.Fatalf("DML following DDL refusal was published: %q", rows)
	}
}

func TestSQLCatalogExtendedPipelinedTransactionControl(t *testing.T) {
	c := connectSQLCatalog(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)

	c.send(msgParse, parseMsg("begin", `BEGIN`))
	c.send(msgBind, bindMsg("begin-portal", "begin", nil, nil, nil))
	c.send(msgExecute, executeMsg("begin-portal", 0))
	statement := `INSERT INTO docs VALUES ($1)`
	c.send(msgParse, parseMsg("write", statement))
	c.send(msgBind, bindMsg("write-portal", "write", nil,
		[][]byte{[]byte(`{"id":"pipelined","name":"committed"}`)}, nil))
	c.send(msgExecute, executeMsg("write-portal", 0))
	c.send(msgParse, parseMsg("commit", `COMMIT`))
	c.send(msgBind, bindMsg("commit-portal", "commit", nil, nil, nil))
	c.send(msgExecute, executeMsg("commit-portal", 0))
	c.send(msgParse, parseMsg("after-commit", statement))
	c.send(msgBind, bindMsg("after-commit-portal", "after-commit", nil,
		[][]byte{[]byte(`{"id":"pipelined","name":"post-commit duplicate"}`)}, nil))
	c.send(msgExecute, executeMsg("after-commit-portal", 0))
	c.send(msgSync, nil)

	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateUniqueViolation)
	if !commandTagsContain(msgs, "COMMIT") {
		t.Fatal("the explicit COMMIT before a later failure was not completed")
	}
	assertReadyStatus(t, msgs, statusIdle)
	if rows := rowsOf(t, c.query(
		`SELECT name FROM docs WHERE id = 'pipelined'`)); len(rows) != 1 ||
		string(rows[0][0]) != `"committed"` {
		t.Fatalf("pipelined COMMIT did not publish its row: %q", rows)
	}

	// A failure before COMMIT enters the extended-protocol error state, so the
	// already-pipelined COMMIT is discarded through Sync and the explicit
	// transaction remains failed rather than publishing partial work.
	c.send(msgParse, parseMsg("begin2", `BEGIN`))
	c.send(msgBind, bindMsg("begin2-portal", "begin2", nil, nil, nil))
	c.send(msgExecute, executeMsg("begin2-portal", 0))
	c.send(msgParse, parseMsg("duplicate", statement))
	c.send(msgBind, bindMsg("duplicate-portal", "duplicate", nil,
		[][]byte{[]byte(`{"id":"pipelined","name":"duplicate"}`)}, nil))
	c.send(msgExecute, executeMsg("duplicate-portal", 0))
	c.send(msgParse, parseMsg("commit2", `COMMIT`))
	c.send(msgBind, bindMsg("commit2-portal", "commit2", nil, nil, nil))
	c.send(msgExecute, executeMsg("commit2-portal", 0))
	c.send(msgSync, nil)

	msgs = c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateUniqueViolation)
	assertReadyStatus(t, msgs, statusFailedT)
	if has(msgs, msgCommandComplete) &&
		commandTagsContain(msgs, "COMMIT") {
		t.Fatal("COMMIT after a failed pipelined Execute was not discarded")
	}
	if got := commandTagOf(t, c.query("COMMIT")); got != "ROLLBACK" {
		t.Fatalf("failed pipelined transaction COMMIT tag = %q, want ROLLBACK", got)
	}
}

func TestSQLCatalogTransactionEndInvalidatesSuspendedPortal(t *testing.T) {
	c := connectSQLCatalog(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)
	c.query(`
		INSERT INTO docs (id, name) VALUES ('a', 'A');
		INSERT INTO docs (id, name) VALUES ('b', 'B')`)
	assertReadyStatus(t, c.query("BEGIN"), statusInTx)

	c.send(msgParse, parseMsg("all", `SELECT id FROM docs ORDER BY id`))
	c.send(msgBind, bindMsg("rows", "all", nil, nil, nil))
	c.send(msgExecute, executeMsg("rows", 1))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if !has(msgs, msgPortalSuspended) {
		t.Fatalf("limited SELECT did not suspend: %s", tags(msgs))
	}
	assertReadyStatus(t, msgs, statusInTx)

	assertReadyStatus(t, c.query("ROLLBACK"), statusIdle)
	c.send(msgExecute, executeMsg("rows", 1))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateInvalidCursorName)

	c.send(msgParse, parseMsg("auto", `SELECT id FROM docs ORDER BY id`))
	c.send(msgBind, bindMsg("auto-rows", "auto", nil, nil, nil))
	c.send(msgExecute, executeMsg("auto-rows", 1))
	c.send(msgSync, nil)
	msgs = c.until(msgReadyForQuery)
	if !has(msgs, msgPortalSuspended) {
		t.Fatalf("implicit limited SELECT did not suspend: %s", tags(msgs))
	}
	assertReadyStatus(t, msgs, statusIdle)
	c.send(msgExecute, executeMsg("auto-rows", 1))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateInvalidCursorName)
}

func TestSQLCatalogParseRejectsExtraParameterTypes(t *testing.T) {
	c := connectSQLCatalog(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY)`)
	c.send(msgParse, parseMsg("typed",
		`SELECT id FROM docs WHERE id = $1`, oidText, oidInt8))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateProtocolViolation)
}

func TestSessionCancellationHasOneResettableSignal(t *testing.T) {
	var s session
	s.cancel()
	if s.queryCancel.Canceled() {
		t.Fatal("a CancelRequest received while idle armed the next statement")
	}
	s.beginCancelable()
	s.cancel()
	if !s.queryCancel.Canceled() {
		t.Fatal("CancelRequest did not arm the executor signal")
	}
	if !s.takeCancel() {
		t.Fatal("an armed cancellation was not consumed")
	}
	if s.queryCancel.Canceled() || s.takeCancel() {
		t.Fatal("one CancelRequest survived its statement and poisoned the next one")
	}
	s.endCancelable()
	s.cancel()
	if s.queryCancel.Canceled() {
		t.Fatal("a CancelRequest after command completion armed the next statement")
	}

	// The exact end-of-command race is the dangerous one: delivery must either
	// arm the ending command before its reset or observe it as idle. It must
	// never leave the next command armed.
	for range 256 {
		s.beginCancelable()
		start := make(chan struct{})
		done := make(chan struct{})
		go func() {
			<-start
			s.cancel()
			close(done)
		}()
		close(start)
		s.endCancelable()
		<-done
		if s.queryCancel.Canceled() {
			t.Fatal("a cancellation racing with command completion poisoned the next statement")
		}
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		s.beginCancelable()
		s.endCancelable()
	}); allocs != 0 {
		t.Fatalf("warm cancellation-window entry/exit allocated %.2f times, want zero", allocs)
	}
}

func TestShutdownCancellationSurvivesACommandWindowOpening(t *testing.T) {
	var s session
	// Reproduce Close snapshotting this session after the message was read but
	// immediately before simpleQuery/handleExecute opens its cancel window.
	s.shutdownCancel()
	s.beginCancelable()
	if !s.queryCancel.Canceled() {
		t.Fatal("command entry cleared a latched server-shutdown cancellation")
	}
	s.endCancelable()
}

func TestSQLCatalogCancellationRollsBackAndDoesNotPoisonNextStatement(t *testing.T) {
	contract, ok := conformance.CancellationFor(conformance.PGWire)
	if !ok || contract.Error != "SQLSTATE 57014" ||
		!contract.NoPartialWrite || !contract.Reusable {
		t.Fatalf("pgwire cancellation contract = %+v, %v", contract, ok)
	}
	c, server := connectSQLCatalogWithServer(t)
	c.query(`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL)`)

	// PostgreSQL ignores a CancelRequest while the backend is waiting for its
	// next command. It must not be retained as a cancellation of that future
	// command.
	server.cancelRequest(c.pid, c.secret)
	if msgs := c.query(`SELECT 1`); has(msgs, msgErrorResponse) {
		t.Fatalf("idle CancelRequest poisoned the next statement: %s", tags(msgs))
	}

	// Stage one write, then fill the same implicit transaction with fixed
	// SELECTs up to the protocol statement bound. Their RowDescription,
	// DataRow, and CommandComplete stream exceeds the session writer buffer, so
	// net.Pipe stops the server well before the batch can finish and keeps the
	// cancellation window open until this test starts reading. Unlike many
	// distinct writes, this shape cannot hit the storage batch-document bound
	// before the cancel checkpoint.
	var batch strings.Builder
	batch.WriteString(`INSERT INTO docs (id, name) VALUES ('canceled', 'must roll back');`)
	for range maxSimpleStatements - 1 {
		batch.WriteString(`SELECT version();`)
	}
	c.send(msgQuery, append([]byte(batch.String()), 0))
	c.drainWrites()

	server.mu.Lock()
	sess := server.sessions[c.pid]
	server.mu.Unlock()
	if sess == nil {
		t.Fatal("authenticated session is absent from the cancellation registry")
	}
	deadline := time.Now().Add(time.Second)
	for {
		sess.cancelMu.Lock()
		active := sess.cancelActive
		sess.cancelMu.Unlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SQL batch did not enter its cancellation window")
		}
		runtime.Gosched()
	}
	server.cancelRequest(c.pid, c.secret)
	msgs := c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateQueryCanceled)
	if completed := countTag(msgs, msgCommandComplete); completed >= maxSimpleStatements {
		t.Fatalf("cancel arrived only after all %d statements completed", completed)
	}
	assertReadyStatus(t, msgs, statusIdle)
	if rows := rowsOf(t, c.query(
		`SELECT id FROM docs WHERE id = 'canceled'`)); len(rows) != 0 {
		t.Fatalf("canceled simple DML published rows: %q", rows)
	}
	if got := commandTagOf(t, c.query(`
		INSERT INTO docs (id, name) VALUES ('after', 'works')`)); got != "INSERT 0 1" {
		t.Fatalf("statement after cancellation tag = %q, want INSERT 0 1", got)
	}

	// An explicit transaction has a different cleanup boundary: cancellation
	// moves it to failed state, only ROLLBACK is accepted, and that rollback
	// must discard writes staged before the canceled command.
	assertReadyStatus(t, c.query(`BEGIN`), statusInTx)
	assertReadyStatus(t, c.query(`
		INSERT INTO docs (id, name) VALUES ('tx-canceled', 'must roll back')`),
		statusInTx)
	batch.Reset()
	for range maxSimpleStatements {
		batch.WriteString(`SELECT version();`)
	}
	c.send(msgQuery, append([]byte(batch.String()), 0))
	c.drainWrites()
	deadline = time.Now().Add(time.Second)
	for {
		sess.cancelMu.Lock()
		active := sess.cancelActive
		sess.cancelMu.Unlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("explicit transaction did not enter its cancellation window")
		}
		runtime.Gosched()
	}
	server.cancelRequest(c.pid, c.secret)
	msgs = c.until(msgReadyForQuery)
	expectError(t, msgs, sqlstateQueryCanceled)
	assertReadyStatus(t, msgs, statusFailedT)
	failed := c.query(`SELECT 1`)
	expectError(t, failed, sqlstateFailedTransaction)
	assertReadyStatus(t, failed, statusFailedT)
	assertReadyStatus(t, c.query(`ROLLBACK`), statusIdle)
	if rows := rowsOf(t, c.query(
		`SELECT id FROM docs WHERE id = 'tx-canceled'`)); len(rows) != 0 {
		t.Fatalf("rollback after explicit cancellation retained staged row: %q", rows)
	}
}

func TestPreparedDerivedChargeIncludesRetainedCapacities(t *testing.T) {
	stmt := &prepared{
		sql:                `SELECT id FROM docs WHERE id = $1`,
		paramKinds:         make([]sqldriver.ParamKind, 3, 4),
		paramTypes:         make([]sqldriver.ParamType, 2, 4),
		paramTypePositions: make([]int, 2, 4),
		paramOrder:         make([]int, 2, 8),
	}
	want := preparedPlanFixedBytes +
		preparedPlanByteMultiplier*len(stmt.sql) +
		8*4 + 8*4 + 8*4 + 8*8 + len(stmt.sql)
	if got := preparedDerivedCharge(stmt); got != want {
		t.Fatalf("prepared derived charge = %d, want %d", got, want)
	}
}

func TestCommitOutcomeUnknownUsesPostgreSQLCompletionUnknown(t *testing.T) {
	if got := asPGError(durable.ErrCommitOutcomeUnknown).code; got != sqlstateStatementCompletionUnknown {
		t.Fatalf("commit outcome SQLSTATE = %s, want %s",
			got, sqlstateStatementCompletionUnknown)
	}
}

func connectSQLCatalog(t *testing.T) *testClient {
	t.Helper()
	c, _ := connectSQLCatalogWithServer(t)
	return c
}

func connectSQLCatalogWithServer(t *testing.T) (*testClient, *Server) {
	t.Helper()
	database, err := sqldriver.Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatalf("open SQL catalog: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQL catalog: %v", err)
		}
	})
	server, err := NewServer(database, Options{Auth: Trust()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	})
	c := dial(t, server)
	c.startup(map[string]string{"user": "tester", "database": "app"})
	return c, server
}

func extendedSQL(c *testClient, statement string, params [][]byte) []backendMessage {
	c.t.Helper()
	c.send(msgParse, parseMsg("", statement))
	c.send(msgDescribe, describeMsg(targetStatement, ""))
	c.send(msgBind, bindMsg("", "", nil, params, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	return c.until(msgReadyForQuery)
}

func assertReadyStatus(t *testing.T, msgs []backendMessage, want byte) {
	t.Helper()
	ready := find(t, msgs, msgReadyForQuery)
	if len(ready.body) != 1 || ready.body[0] != want {
		t.Fatalf("ReadyForQuery status = %q, want %q", ready.body, []byte{want})
	}
}

func commandTagsContain(msgs []backendMessage, want string) bool {
	for _, msg := range msgs {
		if msg.tag == msgCommandComplete && string(msg.body[:len(msg.body)-1]) == want {
			return true
		}
	}
	return false
}
