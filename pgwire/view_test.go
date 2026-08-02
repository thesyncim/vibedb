package pgwire

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPGWireOrdinaryViewsSimpleExtendedAndTypedBoundaries(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, setup := range []struct {
		statement string
		tag       string
	}{
		{
			`CREATE TABLE view_docs (` +
				`id STRING PRIMARY KEY, kind STRING NOT NULL, n NUMBER NOT NULL)`,
			"CREATE TABLE",
		},
		{
			`INSERT INTO view_docs VALUES ` +
				`({"id":"a","kind":"x","n":1}),` +
				`({"id":"b","kind":"x","n":2}),` +
				`({"id":"c","kind":"y","n":3})`,
			"INSERT 0 3",
		},
		{
			`CREATE VIEW view_open AS ` +
				`SELECT id, n FROM view_docs WHERE kind = 'x'`,
			"CREATE VIEW",
		},
	} {
		messages := c.query(setup.statement)
		if has(messages, msgErrorResponse) {
			t.Fatalf("%s: %s", setup.statement,
				formatError(find(t, messages, msgErrorResponse).body))
		}
		if got := commandTagOf(t, messages); got != setup.tag {
			t.Fatalf("%s tag = %q, want %q", setup.statement, got, setup.tag)
		}
	}

	messages := c.query(`SELECT id, n FROM view_open ORDER BY id`)
	rows := rowsOf(t, messages)
	if len(rows) != 2 || string(rows[0][0]) != `"a"` ||
		string(rows[0][1]) != "1" || string(rows[1][0]) != `"b"` ||
		string(rows[1][1]) != "2" {
		t.Fatalf("simple view rows = %q", rows)
	}
	if got := commandTagOf(t, messages); got != "SELECT 2" {
		t.Fatalf("simple view tag = %q", got)
	}

	messages = extendedSQL(
		c,
		`SELECT id FROM view_open WHERE n >= $1 ORDER BY id`,
		[][]byte{[]byte("2")},
	)
	rows = rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != `"b"` {
		t.Fatalf("extended view rows = %q", rows)
	}
	if got := commandTagOf(t, messages); got != "SELECT 1" {
		t.Fatalf("extended view tag = %q", got)
	}

	messages = extendedSQL(
		c,
		`CREATE VIEW view_high (doc_id, score) AS `+
			`SELECT id, n FROM view_open WHERE n >= 2`,
		nil,
	)
	if has(messages, msgErrorResponse) {
		t.Fatalf("extended CREATE VIEW: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	if got := commandTagOf(t, messages); got != "CREATE VIEW" {
		t.Fatalf("extended CREATE VIEW tag = %q", got)
	}
	rows = rowsOf(t, c.query(`SELECT doc_id FROM view_high`))
	if len(rows) != 1 || string(rows[0][0]) != `"b"` {
		t.Fatalf("nested alias view rows = %q", rows)
	}

	fields := expectError(
		t, c.query(`DROP VIEW view_open`), sqlstateDependentObjectsStillExist,
	)
	if !strings.Contains(fields['M'], "view_high") {
		t.Fatalf("dependent DROP message = %q", fields['M'])
	}
	if got := commandTagOf(t, c.query(`DROP VIEW view_high RESTRICT`)); got != "DROP VIEW" {
		t.Fatalf("DROP VIEW tag = %q", got)
	}
	if got := commandTagOf(t, c.query(`DROP VIEW view_open`)); got != "DROP VIEW" {
		t.Fatalf("DROP VIEW tag = %q", got)
	}
}

func TestPGWireMaterializedViewRefusalsHaveUTF8PositionsAndRecover(t *testing.T) {
	c := connectSQLCatalog(t)
	if messages := c.query(`CREATE TABLE docs (id STRING PRIMARY KEY)`); has(messages, msgErrorResponse) {
		t.Fatalf("setup: %s", formatError(find(t, messages, msgErrorResponse).body))
	}
	tests := []struct {
		statement string
		marker    string
	}{
		{
			`/* préfix */ CREATE MATERIALIZED VIEW m AS SELECT id FROM docs`,
			"MATERIALIZED",
		},
		{
			`/* préfix */ DROP MATERIALIZED VIEW IF EXISTS m RESTRICT`,
			"MATERIALIZED",
		},
		{
			`/* préfix */ DROP VIEW m CASCADE`,
			"CASCADE",
		},
		{
			`/* préfix */ REFRESH MATERIALIZED VIEW m`,
			"REFRESH",
		},
	}
	for _, test := range tests {
		position := strings.Index(test.statement, test.marker)
		want := strconv.Itoa(utf8.RuneCountInString(test.statement[:position]) + 1)
		for name, messages := range map[string][]backendMessage{
			"simple":   c.query(test.statement),
			"extended": extendedSQL(c, test.statement, nil),
		} {
			fields := expectError(t, messages, sqlstateFeatureNotSupported)
			if fields['P'] != want {
				t.Fatalf("%s %s position = %q, want %q",
					name, test.marker, fields['P'], want)
			}
			if has(messages, msgDataRow) || has(messages, msgCommandComplete) {
				t.Fatalf("%s refusal published protocol output: %s", name, tags(messages))
			}
		}
	}
	if got := commandTagOf(t, c.query(`SELECT id FROM docs`)); got != "SELECT 0" {
		t.Fatalf("post-refusal recovery tag = %q", got)
	}
}

func TestPGWireViewDefinitionErrorsRebaseIntoCreateStatement(t *testing.T) {
	c := connectSQLCatalog(t)
	if messages := c.query(`CREATE TABLE docs (id STRING PRIMARY KEY)`); has(messages, msgErrorResponse) {
		t.Fatalf("setup: %s", formatError(find(t, messages, msgErrorResponse).body))
	}
	tests := []struct {
		statement string
		marker    string
		code      string
	}{
		{
			`/* préfix */ CREATE VIEW missing_view AS SELECT id FROM absent_table`,
			"absent_table", sqlstateUndefinedTable,
		},
		{
			`/* préfix */ CREATE VIEW cyclic_view AS SELECT id FROM cyclic_view`,
			"cyclic_view AS SELECT id FROM cyclic_view", sqlstateInvalidObjectDefinition,
		},
		{
			`/* préfix */ CREATE VIEW wild_view AS SELECT * FROM docs`,
			"SELECT", sqlstateFeatureNotSupported,
		},
		{
			`/* préfix */ CREATE VIEW excess_view (first, second) AS SELECT id FROM docs`,
			"SELECT", sqlstateInvalidTableDefinition,
		},
		{
			`/* préfix */ CREATE VIEW duplicate_prefix (id) AS SELECT id, id FROM docs`,
			"SELECT", sqlstateDuplicateColumn,
		},
	}
	for _, test := range tests {
		bytePosition := strings.LastIndex(test.statement, test.marker)
		if test.marker == "cyclic_view AS SELECT id FROM cyclic_view" {
			bytePosition += strings.LastIndex(test.marker, "cyclic_view")
		}
		want := strconv.Itoa(
			utf8.RuneCountInString(test.statement[:bytePosition]) + 1,
		)
		for name, messages := range map[string][]backendMessage{
			"simple":   c.query(test.statement),
			"extended": extendedSQL(c, test.statement, nil),
		} {
			fields := expectError(t, messages, test.code)
			if fields['P'] != want {
				t.Fatalf("%s %s position = %q, want %q: %q",
					name, test.code, fields['P'], want, fields['M'])
			}
			if has(messages, msgCommandComplete) || has(messages, msgDataRow) {
				t.Fatalf("%s failed CREATE published output: %s", name, tags(messages))
			}
		}
	}
	if got := commandTagOf(t, c.query(`SELECT id FROM docs`)); got != "SELECT 0" {
		t.Fatalf("post-definition-error recovery tag = %q", got)
	}
}
