package pgwire

import (
	"strconv"
	"strings"
	"testing"
)

func TestPGWireDMLViewRefusalIsPositioned(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY)`,
		`CREATE TABLE target (id STRING PRIMARY KEY)`,
		`CREATE VIEW selected AS SELECT id FROM docs`,
	} {
		if messages := c.query(statement); has(messages, msgErrorResponse) {
			t.Fatalf("setup %q: %s", statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}
	dml := `DELETE FROM target WHERE EXISTS (SELECT id FROM selected)`
	messages := c.query(dml)
	fields := expectError(t, messages, sqlstateFeatureNotSupported)
	assertReadyStatus(t, messages, statusIdle)
	want := strconv.Itoa(strings.LastIndex(dml, "selected") + 1)
	if fields['P'] != want {
		t.Fatalf("DML view refusal position = %q, want %q", fields['P'], want)
	}
	if got := commandTagOf(t, c.query(`SELECT count(*) FROM target`)); got != "SELECT 1" {
		t.Fatalf("simple session reuse tag = %q, want SELECT 1", got)
	}
}

func TestPGWirePreparedDMLNestedTableReplacementIsPositioned0A000(t *testing.T) {
	for _, returning := range []bool{false, true} {
		name := "exec"
		if returning {
			name = "returning"
		}
		t.Run(name, func(t *testing.T) {
			c := connectSQLCatalog(t)
			for _, statement := range []string{
				`CREATE TABLE mutation_target (id STRING PRIMARY KEY)`,
				`CREATE TABLE nested_source (id STRING PRIMARY KEY)`,
				`CREATE TABLE replacement_base (id STRING PRIMARY KEY)`,
				`INSERT INTO mutation_target VALUES ('{"id":"target"}')`,
				`INSERT INTO nested_source VALUES ('{"id":"target"}')`,
			} {
				if messages := c.query(statement); has(messages, msgErrorResponse) {
					t.Fatalf("setup %q: %s", statement,
						formatError(find(t, messages, msgErrorResponse).body))
				}
			}

			preparedSQL := `DELETE FROM mutation_target WHERE id IN (SELECT id FROM nested_source)`
			if returning {
				preparedSQL += ` RETURNING id`
			}
			const preparedName = "nested-source-generation"
			c.send(msgParse, parseMsg(preparedName, preparedSQL))
			c.send(msgSync, nil)
			if messages := c.until(msgReadyForQuery); has(messages, msgErrorResponse) ||
				!has(messages, msgParseComplete) {
				t.Fatalf("prepare nested source: %s", tags(messages))
			}
			for _, statement := range []string{
				`DROP TABLE nested_source`,
				`CREATE VIEW nested_source AS SELECT id FROM replacement_base`,
			} {
				if messages := c.query(statement); has(messages, msgErrorResponse) {
					t.Fatalf("replace relation %q: %s", statement, tags(messages))
				}
			}

			c.send(msgBind, bindMsg("nested-source-portal", preparedName, nil, nil, nil))
			c.send(msgExecute, executeMsg("nested-source-portal", 0))
			c.send(msgSync, nil)
			messages := c.until(msgReadyForQuery)
			fields := expectError(t, messages, sqlstateFeatureNotSupported)
			assertReadyStatus(t, messages, statusIdle)
			wantPosition := strconv.Itoa(strings.LastIndex(preparedSQL, "nested_source") + 1)
			if fields['P'] != wantPosition {
				t.Fatalf("position = %q, want %q", fields['P'], wantPosition)
			}
			if !strings.Contains(fields['M'], "prepared DELETE query source") {
				t.Fatalf("message = %q, want DELETE source rebind", fields['M'])
			}
			if !strings.Contains(fields['H'], "prepare the statement again") {
				t.Fatalf("hint = %q, want established reprepare guidance", fields['H'])
			}
			if got := commandTagOf(t, c.query(`SELECT count(*) FROM mutation_target`)); got != "SELECT 1" {
				t.Fatalf("extended session reuse tag = %q, want SELECT 1", got)
			}
		})
	}
}
