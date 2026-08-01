package pgwire

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestDerivedTableErrorClassification(t *testing.T) {
	tests := []struct {
		err  error
		code string
	}{
		{
			err:  &query.RelationColumnError{Relation: "d", Column: "missing"},
			code: sqlstateUndefinedColumn,
		},
		{
			err: &query.RelationColumnError{
				Relation: "d", Column: "id", Matches: 2,
			},
			code: sqlstateAmbiguousColumn,
		},
		{
			err: fmt.Errorf("materialize nested plan: %w",
				&query.IntermediateBudgetError{
					Resource: "derived relation", Bytes: 2, Limit: 1,
				}),
			code: sqlstateProgramLimitExceeded,
		},
	}
	for _, test := range tests {
		if got := asPGError(test.err).code; got != test.code {
			t.Errorf("asPGError(%T) = %q, want %q", test.err, got, test.code)
		}
	}
}

func TestDerivedTableFeatureRefusalsKeepSQLStatePositionAndRecovery(t *testing.T) {
	c := connect(t)
	for _, test := range []struct {
		name      string
		statement string
		marker    string
	}{
		{
			name: "column alias list",
			statement: `SELECT d.id FROM (` +
				`SELECT id FROM users` +
				`) AS d(id)`,
			marker: "(id)",
		},
		{
			name: "lateral",
			statement: `SELECT d.id FROM LATERAL (` +
				`SELECT id FROM users` +
				`) AS d`,
			marker: "LATERAL",
		},
		{
			name: "derived join input",
			statement: `SELECT d.id FROM users AS u JOIN (` +
				`SELECT id FROM users` +
				`) AS d ON u.id = d.id`,
			marker: "(SELECT",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fields := expectError(t, c.query(test.statement),
				sqlstateFeatureNotSupported)
			wantPosition := strconv.Itoa(strings.Index(test.statement, test.marker) + 1)
			if fields['P'] != wantPosition {
				t.Fatalf("ErrorResponse position = %q, want %q", fields['P'], wantPosition)
			}
		})
	}

	statement := `SELECT d.id FROM (SELECT id FROM users) AS d(id)`
	c.send(msgParse, parseMsg("unsupported-derived", statement))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	fields := expectError(t, msgs, sqlstateFeatureNotSupported)
	wantPosition := strconv.Itoa(strings.Index(statement, "(id)") + 1)
	if fields['P'] != wantPosition {
		t.Fatalf("extended ErrorResponse position = %q, want %q",
			fields['P'], wantPosition)
	}
	if has(msgs, msgParseComplete) {
		t.Fatalf("unsupported Parse emitted ParseComplete: %s", tags(msgs))
	}

	msgs = c.query(`SELECT id FROM users WHERE id = 1`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("recovery tag = %q, want SELECT 1", got)
	}
}

func TestDerivedTableSimpleAndExtendedProtocol(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING, n INTEGER)`,
		`INSERT INTO docs VALUES ` +
			`({"id":"a","kind":"x","n":1}),` +
			`({"id":"b","kind":"x","n":2}),` +
			`({"id":"c","kind":"y","n":3})`,
	} {
		msgs := c.query(statement)
		if has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}

	rows := rowsOf(t, c.query(
		`SELECT d.id, d.n FROM (`+
			`SELECT id, kind, n FROM docs WHERE kind = 'x' ORDER BY id LIMIT 2`+
			`) d WHERE d.n >= 2 ORDER BY d.id DESC`,
	))
	if len(rows) != 1 || string(rows[0][0]) != `"b"` || string(rows[0][1]) != "2" {
		t.Fatalf("simple derived rows = %q, want [[b 2]]", rows)
	}

	msgs := extendedSQL(c,
		`SELECT d.id FROM (`+
			`SELECT id, kind, n FROM docs WHERE kind = $1`+
			`) d WHERE d.n >= $2 ORDER BY d.id`,
		[][]byte{[]byte("x"), []byte("1")},
	)
	rows = rowsOf(t, msgs)
	if len(rows) != 2 || len(rows[0]) != 1 || len(rows[1]) != 1 ||
		string(rows[0][0]) != `"a"` || string(rows[1][0]) != `"b"` {
		t.Fatalf("extended derived rows = %q, want [[a] [b]]", rows)
	}
	if got := commandTagOf(t, msgs); got != "SELECT 2" {
		t.Fatalf("extended derived tag = %q, want SELECT 2", got)
	}
}

func TestConfiguredIntermediateBudgetFailsBeforeAnyDataRowAndRecovers(t *testing.T) {
	srv := newTestServer(t, Options{MaxIntermediateBytes: 1})
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})
	statement := `SELECT d.id FROM (SELECT id FROM users) AS d`

	msgs := c.query(statement)
	expectError(t, msgs, sqlstateProgramLimitExceeded)
	if has(msgs, msgDataRow) {
		t.Fatal("intermediate-budget failure emitted a partial simple-query DataRow")
	}

	msgs = extendedSQL(c, statement, nil)
	expectError(t, msgs, sqlstateProgramLimitExceeded)
	if has(msgs, msgDataRow) {
		t.Fatal("intermediate-budget failure emitted a partial extended-query DataRow")
	}

	msgs = c.query(`SELECT id FROM users WHERE id = 1`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("post-budget recovery tag = %q, want SELECT 1", got)
	}
}

func TestDerivedTableColumnSQLStatesAndRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, n INTEGER)`,
		`INSERT INTO docs VALUES ({"id":"a","n":1})`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("setup %s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}

	expectError(t,
		c.query(`SELECT d.missing FROM (SELECT id FROM docs) d`),
		sqlstateUndefinedColumn,
	)
	expectError(t,
		c.query(`SELECT d.id FROM (SELECT id, id FROM docs) d`),
		sqlstateAmbiguousColumn,
	)
	for _, test := range []struct {
		name      string
		statement string
		code      string
	}{
		{
			name:      "undefined-derived-column",
			statement: `SELECT d.missing FROM (SELECT id FROM docs) d`,
			code:      sqlstateUndefinedColumn,
		},
		{
			name:      "ambiguous-derived-column",
			statement: `SELECT d.id FROM (SELECT id, id FROM docs) d`,
			code:      sqlstateAmbiguousColumn,
		},
	} {
		c.send(msgParse, parseMsg(test.name, test.statement))
		c.send(msgSync, nil)
		msgs := c.until(msgReadyForQuery)
		expectError(t, msgs, test.code)
		if has(msgs, msgParseComplete) {
			t.Fatalf("%s emitted ParseComplete: %s", test.name, tags(msgs))
		}
	}
	msgs := c.query(`SELECT d.id FROM (SELECT id FROM docs) d`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("recovery tag = %q, want SELECT 1", got)
	}
}
