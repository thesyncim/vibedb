package pgwire

import (
	"strconv"
	"strings"
	"testing"
)

func TestPGWirePreparedInsertSelectPhysicalSourceReplacementIsPositioned0A000(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE wire_insert_rebind_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE wire_insert_rebind_backing (id STRING PRIMARY KEY)`,
		`CREATE TABLE wire_insert_rebind_target (id STRING PRIMARY KEY)`,
		`INSERT INTO wire_insert_rebind_source VALUES ({"id":"old"})`,
		`INSERT INTO wire_insert_rebind_backing VALUES ({"id":"new"})`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("setup %q: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}

	const preparedName = "insert-source-generation"
	const preparedSQL = `INSERT INTO wire_insert_rebind_target SELECT * FROM wire_insert_rebind_source`
	c.send(msgParse, parseMsg(preparedName, preparedSQL))
	c.send(msgSync, nil)
	if messages := c.until(msgReadyForQuery); has(messages, msgErrorResponse) ||
		!has(messages, msgParseComplete) {
		t.Fatalf("prepare INSERT SELECT: %s", tags(messages))
	}
	for _, statement := range []string{
		`DROP TABLE wire_insert_rebind_source`,
		`CREATE VIEW wire_insert_rebind_source (id) AS SELECT id FROM wire_insert_rebind_backing`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("replace source %q: %s", statement, tags(messages))
		}
	}

	c.send(msgBind, bindMsg("insert-source-portal", preparedName, nil, nil, nil))
	c.send(msgExecute, executeMsg("insert-source-portal", 0))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	fields := expectError(t, messages, sqlstateFeatureNotSupported)
	assertReadyStatus(t, messages, statusIdle)
	wantPosition := strconv.Itoa(strings.LastIndex(preparedSQL, "wire_insert_rebind_source") + 1)
	if fields['P'] != wantPosition {
		t.Fatalf("position = %q, want %q", fields['P'], wantPosition)
	}
	if !strings.Contains(fields['M'], "prepared INSERT query source") {
		t.Fatalf("message = %q, want INSERT source rebind", fields['M'])
	}
	if !strings.Contains(fields['H'], "prepare the statement again") {
		t.Fatalf("hint = %q, want established reprepare guidance", fields['H'])
	}

	rows := rowsOf(t, c.query(`SELECT count(*) FROM wire_insert_rebind_target`))
	if len(rows) != 1 || string(rows[0][0]) != "0" {
		t.Fatalf("stale prepared INSERT published rows: %q", rows)
	}
}
