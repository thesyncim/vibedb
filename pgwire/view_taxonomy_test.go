package pgwire

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPGWireViewObjectTypeAndStalePlanTaxonomy(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY)`,
		`CREATE VIEW selected AS SELECT id FROM docs`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("setup %q: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}

	for _, statement := range []string{
		`DROP VIEW docs`,
		`DROP VIEW IF EXISTS docs`,
	} {
		messages := c.query(statement)
		fields := expectError(t, messages, sqlstateWrongObjectType)
		assertReadyStatus(t, messages, statusIdle)
		want := strconv.Itoa(utf8.RuneCountInString(
			statement[:strings.LastIndex(statement, "docs")],
		) + 1)
		if fields['P'] != want || !strings.Contains(fields['H'], "DROP TABLE") {
			t.Fatalf("wrong-object fields = %#v, want position %s and DROP TABLE hint", fields, want)
		}
	}

	const preparedName = "view-generation"
	const preparedSQL = `SELECT id FROM selected`
	c.send(msgParse, parseMsg(preparedName, preparedSQL))
	c.send(msgSync, nil)
	if messages := c.until(msgReadyForQuery); has(messages, msgErrorResponse) ||
		!has(messages, msgParseComplete) {
		t.Fatalf("prepare view plan: %s", tags(messages))
	}
	for _, statement := range []string{
		`DROP VIEW selected`,
		`CREATE VIEW selected AS SELECT id FROM docs`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("replace view %q: %s", statement, tags(messages))
		}
	}
	c.send(msgBind, bindMsg("view-generation-portal", preparedName, nil, nil, nil))
	c.send(msgExecute, executeMsg("view-generation-portal", 0))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	fields := expectError(t, messages, sqlstateFeatureNotSupported)
	assertReadyStatus(t, messages, statusIdle)
	if !strings.Contains(fields['H'], "prepare the statement again") {
		t.Fatalf("stale view plan hint = %q", fields['H'])
	}
	if fields['P'] != strconv.Itoa(strings.LastIndex(preparedSQL, "selected")+1) {
		t.Fatalf("stale view plan position = %q", fields['P'])
	}
	if got := commandTagOf(t, c.query(`SELECT count(*) FROM selected`)); got != "SELECT 1" {
		t.Fatalf("stale-plan session reuse tag = %q, want SELECT 1", got)
	}
}

func TestPGWireViewTargetsUseWrongObjectTypeMatrix(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE object_base (id STRING PRIMARY KEY)`,
		`CREATE VIEW object_view AS SELECT id FROM object_base`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("setup %q: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}
	tests := []struct {
		statement string
		hint      string
	}{
		{`DROP TABLE object_view`, "DROP VIEW"},
		{`DROP TABLE IF EXISTS object_view`, "DROP VIEW"},
		{`TRUNCATE object_view`, "truncate a base table"},
		{`CREATE INDEX object_view_id ON object_view (id)`, "index on a base table"},
		{`DROP INDEX IF EXISTS missing_index ON object_view`, "index from its base table"},
		{`INSERT INTO object_view VALUES ('{"id":"write"}')`, "insert into a base table"},
		{`UPDATE object_view SET "$doc" = '{"id":"write"}' WHERE id = 'base'`, "update a base table"},
		{`DELETE FROM object_view WHERE id = 'base'`, "delete from a base table"},
	}
	for _, test := range tests {
		messages := c.query(test.statement)
		fields := expectError(t, messages, sqlstateWrongObjectType)
		assertReadyStatus(t, messages, statusIdle)
		want := strconv.Itoa(utf8.RuneCountInString(
			test.statement[:strings.LastIndex(test.statement, "object_view")],
		) + 1)
		if fields['P'] != want || !strings.Contains(fields['H'], test.hint) {
			t.Fatalf("%s fields = %#v, want position %s and hint %q",
				test.statement, fields, want, test.hint)
		}
	}
	if got := commandTagOf(t, c.query(`SELECT count(*) FROM object_base`)); got != "SELECT 1" {
		t.Fatalf("wrong-object session reuse tag = %q, want SELECT 1", got)
	}
}

func TestPGWirePreparedTableTargetRevalidatesViewObjectType(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE wire_object_base (id STRING PRIMARY KEY)`,
		`CREATE TABLE wire_object_race (id STRING PRIMARY KEY)`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("setup %q: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}
	const preparedName = "wrong-object-generation"
	const preparedSQL = `DELETE FROM wire_object_race WHERE id = 'base'`
	c.send(msgParse, parseMsg(preparedName, preparedSQL))
	c.send(msgSync, nil)
	if messages := c.until(msgReadyForQuery); has(messages, msgErrorResponse) ||
		!has(messages, msgParseComplete) {
		t.Fatalf("prepare table target: %s", tags(messages))
	}
	for _, statement := range []string{
		`DROP TABLE wire_object_race`,
		`CREATE VIEW wire_object_race AS SELECT id FROM wire_object_base`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("replace relation %q: %s", statement, tags(messages))
		}
	}
	c.send(msgBind, bindMsg("wrong-object-portal", preparedName, nil, nil, nil))
	c.send(msgExecute, executeMsg("wrong-object-portal", 0))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	fields := expectError(t, messages, sqlstateWrongObjectType)
	assertReadyStatus(t, messages, statusIdle)
	if fields['P'] != strconv.Itoa(strings.Index(preparedSQL, "wire_object_race")+1) ||
		!strings.Contains(fields['H'], "delete from a base table") {
		t.Fatalf("prepared wrong-object fields = %#v", fields)
	}
	if got := commandTagOf(t, c.query(`SELECT count(*) FROM wire_object_race`)); got != "SELECT 1" {
		t.Fatalf("prepared wrong-object session reuse tag = %q, want SELECT 1", got)
	}
}
