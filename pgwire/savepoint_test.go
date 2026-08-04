package pgwire

import (
	"encoding/json"
	"testing"
)

func TestSavepointFailedTransactionRecoveryTranscript(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`CREATE TABLE docs (
		id STRING PRIMARY KEY, name STRING NOT NULL)`))

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	requireWireOK(t, c.query(`INSERT INTO docs (id, name) VALUES ('keep', 'ok')`))
	assertReadyStatus(t, requireQueryReady(t, c, `SAVEPOINT safe`), statusInTx)

	requireWireOK(t, c.query(`INSERT INTO docs (id, name) VALUES ('temp', 'scratch')`))
	dup := c.query(`INSERT INTO docs (id, name) VALUES ('keep', 'dup')`)
	expectError(t, dup, sqlstateUniqueViolation)
	assertReadyStatus(t, dup, statusFailedT)

	// SAVEPOINT and RELEASE in a failed transaction are 25P02; the session
	// stays failed until ROLLBACK TO or a full ROLLBACK/COMMIT-as-rollback.
	sp := c.query(`SAVEPOINT x`)
	expectError(t, sp, sqlstateFailedTransaction)
	assertReadyStatus(t, sp, statusFailedT)
	rel := c.query(`RELEASE safe`)
	expectError(t, rel, sqlstateFailedTransaction)
	assertReadyStatus(t, rel, statusFailedT)

	recoverMsgs := requireQueryReady(t, c, `ROLLBACK TO SAVEPOINT safe`)
	if got := commandTagOf(t, recoverMsgs); got != "ROLLBACK" {
		t.Fatalf("ROLLBACK TO tag = %q, want ROLLBACK", got)
	}
	assertReadyStatus(t, recoverMsgs, statusInTx)

	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM docs ORDER BY id`)); !stringSlicesEqual(got, []string{"keep"}) {
		t.Fatalf("after ROLLBACK TO keys = %v, want [keep]", got)
	}
	assertReadyStatus(t, requireQueryReady(t, c, `COMMIT`), statusIdle)
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM docs ORDER BY id`)); !stringSlicesEqual(got, []string{"keep"}) {
		t.Fatalf("committed keys = %v, want [keep]", got)
	}
}

func TestSavepointPgxStyleNestedBeginFlow(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`CREATE TABLE docs (
		id STRING PRIMARY KEY, name STRING NOT NULL)`))

	// pgx implements database/sql nested Begin as SAVEPOINT / RELEASE /
	// ROLLBACK TO over the extended protocol.
	assertReadyStatus(t, extendedSQL(c, `BEGIN`, nil), statusInTx)
	assertReadyStatus(t, extendedSQL(c, `SAVEPOINT sp_1`, nil), statusInTx)
	requireWireOK(t, extendedSQL(c,
		`INSERT INTO docs (id, name) VALUES ('outer', 'o')`, nil))
	assertReadyStatus(t, extendedSQL(c, `SAVEPOINT sp_2`, nil), statusInTx)
	requireWireOK(t, extendedSQL(c,
		`INSERT INTO docs (id, name) VALUES ('inner', 'i')`, nil))
	assertReadyStatus(t, extendedSQL(c, `ROLLBACK TO sp_2`, nil), statusInTx)
	assertReadyStatus(t, extendedSQL(c, `RELEASE sp_1`, nil), statusInTx)
	assertReadyStatus(t, extendedSQL(c, `COMMIT`, nil), statusIdle)

	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM docs ORDER BY id`)); !stringSlicesEqual(got, []string{"outer"}) {
		t.Fatalf("nested-begin keys = %v, want [outer]", got)
	}
}

func TestSavepointSerializationFailureRetryLoop(t *testing.T) {
	c, srv := connectSQLCatalogWithServer(t)
	requireWireOK(t, c.query(`CREATE TABLE docs (
		id STRING PRIMARY KEY, name STRING NOT NULL)`))
	requireWireOK(t, c.query(`INSERT INTO docs (id, name) VALUES ('k', 'v0')`))

	blocker := dial(t, srv)
	blocker.startup(map[string]string{"user": "tester", "database": "app"})

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	requireWireOK(t, c.query(
		`UPDATE docs SET "$doc" = '{"id":"k","name":"loser"}' WHERE id = 'k'`))

	assertReadyStatus(t, requireQueryReady(t, blocker, `BEGIN`), statusInTx)
	requireWireOK(t, blocker.query(
		`UPDATE docs SET "$doc" = '{"id":"k","name":"winner"}' WHERE id = 'k'`))
	assertReadyStatus(t, requireQueryReady(t, blocker, `COMMIT`), statusIdle)

	conflict := c.query(`COMMIT`)
	expectError(t, conflict, sqlstateSerializationFailure)
	assertReadyStatus(t, conflict, statusIdle)

	// Caller retry loop: open a fresh transaction and re-apply the write.
	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	requireWireOK(t, c.query(
		`UPDATE docs SET "$doc" = '{"id":"k","name":"retried"}' WHERE id = 'k'`))
	assertReadyStatus(t, requireQueryReady(t, c, `COMMIT`), statusIdle)
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT name FROM docs WHERE id = 'k'`)); !stringSlicesEqual(got, []string{"retried"}) {
		t.Fatalf("retried value = %v, want [retried]", got)
	}
}

func TestSavepointSQLSTATEGoldenErrors(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`CREATE TABLE docs (
		id STRING PRIMARY KEY, name STRING NOT NULL)`))

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	assertReadyStatus(t, requireQueryReady(t, c, `SAVEPOINT known`), statusInTx)

	unknownRelease := c.query(`RELEASE missing`)
	expectError(t, unknownRelease, sqlstateInvalidSavepointSpecification)
	assertReadyStatus(t, unknownRelease, statusFailedT)

	// Unknown ROLLBACK TO from the failed state stays failed (3B001) rather
	// than recovering; only a successful ROLLBACK TO returns status T.
	unknownRollback := c.query(`ROLLBACK TO missing`)
	expectError(t, unknownRollback, sqlstateInvalidSavepointSpecification)
	assertReadyStatus(t, unknownRollback, statusFailedT)

	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK TO known`), statusInTx)
	requireWireOK(t, c.query(`INSERT INTO docs (id, name) VALUES ('a', '1')`))
	requireWireOK(t, c.query(`SAVEPOINT mark`))
	dup := c.query(`INSERT INTO docs (id, name) VALUES ('a', '2')`)
	expectError(t, dup, sqlstateUniqueViolation)
	assertReadyStatus(t, dup, statusFailedT)
	inFailed := c.query(`SAVEPOINT after_fail`)
	expectError(t, inFailed, sqlstateFailedTransaction)
	assertReadyStatus(t, inFailed, statusFailedT)
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK`), statusIdle)
}

func TestSavepointChainedTransactionStillRefused(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`CREATE TABLE docs (
		id STRING PRIMARY KEY, name STRING NOT NULL)`))

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	requireWireOK(t, c.query(`INSERT INTO docs (id, name) VALUES ('a', '1')`))
	chained := c.query(`COMMIT AND CHAIN`)
	fields := expectError(t, chained, sqlstateFeatureNotSupported)
	if fields['M'] == "" {
		t.Fatal("chained COMMIT refused without a message")
	}
	assertReadyStatus(t, chained, statusFailedT)
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK`), statusIdle)

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	rollbackChained := c.query(`ROLLBACK AND CHAIN`)
	expectError(t, rollbackChained, sqlstateFeatureNotSupported)
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK`), statusIdle)
}

func TestSavepointMultiStatementNonTerminal(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`CREATE TABLE docs (
		id STRING PRIMARY KEY, name STRING NOT NULL)`))

	// Preflight admits savepoints as non-terminal transaction-block members.
	assertReadyStatus(t, requireQueryReady(t, c, `
		BEGIN;
		SAVEPOINT s;
		INSERT INTO docs (id, name) VALUES ('a', '1');
		COMMIT`), statusIdle)
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM docs ORDER BY id`)); !stringSlicesEqual(got, []string{"a"}) {
		t.Fatalf("keys = %v, want [a]", got)
	}

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	assertReadyStatus(t, requireQueryReady(t, c, `SAVEPOINT s`), statusInTx)
	requireWireOK(t, c.query(`INSERT INTO docs (id, name) VALUES ('b', '2')`))
	dup := c.query(`INSERT INTO docs (id, name) VALUES ('a', 'dup')`)
	expectError(t, dup, sqlstateUniqueViolation)
	assertReadyStatus(t, dup, statusFailedT)
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK TO s`), statusInTx)
	assertReadyStatus(t, requireQueryReady(t, c, `COMMIT`), statusIdle)
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM docs ORDER BY id`)); !stringSlicesEqual(got, []string{"a"}) {
		t.Fatalf("keys after ROLLBACK TO = %v, want [a]", got)
	}
}

func requireWireOK(t *testing.T, msgs []backendMessage) {
	t.Helper()
	if has(msgs, msgErrorResponse) {
		t.Fatalf("wire command failed: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
}

func requireQueryReady(t *testing.T, c *testClient, sql string) []backendMessage {
	t.Helper()
	msgs := c.query(sql)
	requireWireOK(t, msgs)
	return msgs
}

func requireSelect(t *testing.T, c *testClient, sql string) []backendMessage {
	t.Helper()
	msgs := c.query(sql)
	requireWireOK(t, msgs)
	return msgs
}

func jsonStringColumn(t *testing.T, msgs []backendMessage) []string {
	t.Helper()
	rows := rowsOf(t, msgs)
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) != 1 {
			t.Fatalf("expected one column, got %d: %q", len(row), row)
		}
		var key string
		if err := json.Unmarshal(row[0], &key); err != nil {
			t.Fatalf("result cell %q is not a JSON string: %v", row[0], err)
		}
		keys = append(keys, key)
	}
	return keys
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
