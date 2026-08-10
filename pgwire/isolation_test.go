package pgwire

import "testing"

func TestPGWireIsolationModesRefreshRepeatAndSerialize(t *testing.T) {
	c, srv := connectSQLCatalogWithServer(t)
	other := dial(t, srv)
	other.startup(map[string]string{"user": "tester", "database": "app"})
	requireWireOK(t, c.query(`CREATE TABLE docs (
		id STRING PRIMARY KEY, value STRING, on_call BOOL)`))
	requireWireOK(t, c.query(
		`INSERT INTO docs (id, value, on_call) VALUES ('a', 'before', true)`))

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM docs ORDER BY id`)); !stringSlicesEqual(got, []string{"a"}) {
		t.Fatalf("READ COMMITTED first rows = %v, want [a]", got)
	}
	requireWireOK(t, other.query(
		`INSERT INTO docs (id, value, on_call) VALUES ('b', 'outside', true)`))
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM docs ORDER BY id`)); !stringSlicesEqual(got, []string{"a", "b"}) {
		t.Fatalf("READ COMMITTED refreshed rows = %v, want [a b]", got)
	}
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK`), statusIdle)

	assertReadyStatus(t, requireQueryReady(t, c,
		`BEGIN ISOLATION LEVEL REPEATABLE READ`), statusInTx)
	requireWireOK(t, other.query(
		`INSERT INTO docs (id, value, on_call) VALUES ('c', 'later', true)`))
	if got := jsonStringColumn(t, requireSelect(t, c,
		`SELECT id FROM docs ORDER BY id`)); !stringSlicesEqual(got, []string{"a", "b"}) {
		t.Fatalf("REPEATABLE READ rows = %v, want fixed [a b]", got)
	}
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK`), statusIdle)

	assertReadyStatus(t, requireQueryReady(t, c,
		`BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE`), statusInTx)
	assertReadyStatus(t, requireQueryReady(t, other,
		`BEGIN READ WRITE ISOLATION LEVEL SERIALIZABLE`), statusInTx)
	requireSelect(t, c, `SELECT COUNT(*) FROM docs WHERE on_call = true`)
	requireSelect(t, other, `SELECT COUNT(*) FROM docs WHERE on_call = true`)
	requireWireOK(t, c.query(
		`UPDATE docs SET "$doc" = '{"id":"a","value":"left","on_call":false}' WHERE id = 'a'`))
	requireWireOK(t, other.query(
		`UPDATE docs SET "$doc" = '{"id":"b","value":"right","on_call":false}' WHERE id = 'b'`))
	assertReadyStatus(t, requireQueryReady(t, c, `COMMIT`), statusIdle)
	conflict := other.query(`COMMIT`)
	expectError(t, conflict, sqlstateSerializationFailure)
	assertReadyStatus(t, conflict, statusIdle)
}

func TestPGWireIsolationModeRefusalAndReadOnly(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`CREATE TABLE docs (id STRING PRIMARY KEY)`))
	assertReadyStatus(t, requireQueryReady(t, c,
		`BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY`), statusInTx)
	write := c.query(`INSERT INTO docs (id) VALUES ('nope')`)
	expectError(t, write, sqlstateReadOnlyTransaction)
	assertReadyStatus(t, write, statusFailedT)
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK`), statusIdle)

	unsupported := c.query(`BEGIN ISOLATION LEVEL READ UNCOMMITTED`)
	expectError(t, unsupported, sqlstateFeatureNotSupported)
	assertReadyStatus(t, unsupported, statusIdle)
}
