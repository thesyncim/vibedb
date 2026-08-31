package pgwire

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

const postgresUndefinedOperatorHint = "No operator matches the given name and argument types. " +
	"You might need to add explicit type casts."

func TestPGWirePathComparisonNullSemanticsAndUndefinedOperatorPosition(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE wire_path_docs (id STRING PRIMARY KEY, a NUMBER, b NUMBER)`,
		`INSERT INTO wire_path_docs VALUES ` +
			`('{"id":"equal","a":1,"b":1}'),` +
			`('{"id":"less","a":1,"b":2}'),` +
			`('{"id":"null-left","a":null,"b":1}'),` +
			`('{"id":"null-right","a":1,"b":null}')`,
		`CREATE TABLE wire_path_dynamic (id STRING PRIMARY KEY, a ANY, b ANY)`,
		`INSERT INTO wire_path_dynamic VALUES ('{"id":"bad","a":1,"b":"1","keep":true}')`,
		`CREATE VIEW wire_path_bad AS ` +
			`SELECT id FROM wire_path_dynamic WHERE a != b`,
		`CREATE TABLE wire_join_left (id STRING PRIMARY KEY, x ANY, keep BOOL)`,
		`INSERT INTO wire_join_left VALUES ('{"id":"l","x":1,"keep":false}')`,
		`CREATE TABLE wire_join_right (id STRING PRIMARY KEY, x ANY)`,
		`INSERT INTO wire_join_right VALUES ('{"id":"r","x":"1"}')`,
	} {
		messages := c.query(statement)
		if has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", statement, formatError(find(t, messages, msgErrorResponse).body))
		}
	}

	rows := rowsOf(t, c.query(`SELECT id FROM wire_path_docs WHERE a = b ORDER BY id`))
	if len(rows) != 1 || string(rows[0][0]) != `"equal"` {
		t.Fatalf("equality rows = %q, want equal only", rows)
	}
	rows = rowsOf(t, c.query(`SELECT id FROM wire_path_docs WHERE NOT (a = b) ORDER BY id`))
	if len(rows) != 1 || string(rows[0][0]) != `"less"` {
		t.Fatalf("negated rows = %q, want less only", rows)
	}
	rows = rowsOf(t, c.query(`SELECT id FROM wire_path_docs `+
		`WHERE CASE WHEN a = b THEN TRUE ELSE FALSE END = TRUE ORDER BY id`))
	if len(rows) != 1 || string(rows[0][0]) != `"equal"` {
		t.Fatalf("searched CASE rows = %q, want equal only", rows)
	}

	source := `SELECT id FROM wire_path_dynamic WHERE a <= b`
	fields := expectError(t, c.query(source), sqlstateUndefinedFunction)
	if fields['M'] != `operator does not exist: numeric <= text` {
		t.Fatalf("message = %q", fields['M'])
	}
	if fields['H'] != postgresUndefinedOperatorHint {
		t.Fatalf("hint = %q", fields['H'])
	}
	if want := strconv.Itoa(strings.Index(source, "<=") + 1); fields['P'] != want {
		t.Fatalf("position = %q, want %q", fields['P'], want)
	}
	unicodeSource := `SELECT id AS "é" FROM wire_path_dynamic WHERE a <= b`
	fields = expectError(t, c.query(unicodeSource), sqlstateUndefinedFunction)
	if want := strconv.Itoa(
		utf8.RuneCountInString(unicodeSource[:strings.Index(unicodeSource, "<=")]) + 1,
	); fields['P'] != want {
		t.Fatalf("Unicode position = %q, want %q", fields['P'], want)
	}
	caseSource := `SELECT CASE WHEN a != b THEN 1 ELSE 0 END FROM wire_path_dynamic`
	fields = expectError(t, c.query(caseSource), sqlstateUndefinedFunction)
	if fields['M'] != `operator does not exist: numeric <> text` {
		t.Fatalf("CASE message = %q", fields['M'])
	}
	if want := strconv.Itoa(strings.Index(caseSource, "!=") + 1); fields['P'] != want {
		t.Fatalf("CASE position = %q, want %q", fields['P'], want)
	}
	for _, filteredSource := range []string{
		`SELECT id FROM wire_path_dynamic WHERE keep = FALSE AND a <= b`,
		`SELECT id FROM wire_path_dynamic WHERE keep = TRUE OR a <= b`,
		`SELECT id FROM wire_path_dynamic WHERE a <= b LIMIT 0`,
	} {
		fields = expectError(t, c.query(filteredSource), sqlstateUndefinedFunction)
		if fields['M'] != `operator does not exist: numeric <= text` ||
			fields['H'] != postgresUndefinedOperatorHint {
			t.Fatalf("prefiltered fields = %#v", fields)
		}
		if want := strconv.Itoa(strings.Index(filteredSource, "<=") + 1); fields['P'] != want {
			t.Fatalf("prefiltered position = %q, want %q", fields['P'], want)
		}
	}

	fields = expectError(
		t, c.query(`SELECT id FROM wire_path_bad`), sqlstateUndefinedFunction,
	)
	if fields['M'] != `operator does not exist: numeric <> text` {
		t.Fatalf("durable-view message = %q", fields['M'])
	}
	if fields['P'] != "" {
		t.Fatalf("durable-view definition offset leaked as outer position %q", fields['P'])
	}

	fields = expectError(t, c.query(
		`SELECT l.id FROM wire_join_left l JOIN wire_join_right r USING (x)`,
	), sqlstateUndefinedFunction)
	if fields['M'] != `operator does not exist: numeric = text` {
		t.Fatalf("USING message = %q", fields['M'])
	}
	if fields['P'] != "" {
		t.Fatalf("USING synthesized operator published position %q", fields['P'])
	}
	for _, joinSource := range []string{
		`SELECT l.id FROM wire_join_left l JOIN wire_join_right r ON l.x = r.x WHERE l.keep = TRUE`,
		`SELECT l.id FROM wire_join_left l LEFT JOIN wire_join_right r ON l.x = r.x WHERE l.keep = TRUE`,
		`SELECT l.id FROM wire_join_left l JOIN wire_join_right r ON l.x = r.x LIMIT 0`,
		`SELECT l.id FROM wire_join_left l LEFT JOIN wire_join_right r ON l.x = r.x LIMIT 0`,
	} {
		fields = expectError(t, c.query(joinSource), sqlstateUndefinedFunction)
		if fields['M'] != `operator does not exist: numeric = text` {
			t.Fatalf("prefiltered JOIN message = %q", fields['M'])
		}
		if want := strconv.Itoa(strings.Index(joinSource, "=") + 1); fields['P'] != want {
			t.Fatalf("prefiltered JOIN position = %q, want %q", fields['P'], want)
		}
	}
	assertReadyStatus(t, c.query(`SELECT id FROM wire_path_docs WHERE a = b`), statusIdle)
}
