package pgwire

import (
	"strconv"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestCTEGrammarSQLStatesAndExactPositions(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		marker    string
		code      string
	}{
		{
			name: "duplicate name",
			statement: `WITH a AS (SELECT id FROM users), ` +
				`a AS (SELECT id FROM users) SELECT id FROM a`,
			marker: `a AS (SELECT id FROM users) SELECT`,
			code:   sqlstateDuplicateAlias,
		},
		{
			name: "too many column aliases",
			statement: `WITH a(id, extra) AS (` +
				`SELECT id FROM users) SELECT id FROM a`,
			marker: `extra`,
			code:   sqlstateInvalidColumnReference,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := sqlast.Parse(test.statement)
			if err == nil {
				t.Fatal("invalid CTE parsed")
			}
			pg := asPGErrorIn(err, test.statement)
			if pg.code != test.code {
				t.Fatalf("SQLSTATE = %q, want %q: %v", pg.code, test.code, err)
			}
			want := strings.Index(test.statement, test.marker) + 1
			if pg.position != want {
				t.Fatalf("position = %d, want %d", pg.position, want)
			}
		})
	}
}

func TestCTEMissingPhysicalDependencyPositionAndProtocolRecovery(t *testing.T) {
	c := connect(t)
	statement := `WITH visible AS (` +
		`SELECT id FROM missing_relation` +
		`) SELECT id FROM visible`
	wantPosition := strconv.Itoa(strings.Index(statement, "missing_relation") + 1)

	fields := expectError(t, c.query(statement), sqlstateUndefinedTable)
	if fields['P'] != wantPosition {
		t.Fatalf("simple missing dependency position = %q, want %q",
			fields['P'], wantPosition)
	}

	c.send(msgParse, parseMsg("missing-cte-dependency", statement))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	fields = expectError(t, msgs, sqlstateUndefinedTable)
	if fields['P'] != wantPosition {
		t.Fatalf("extended missing dependency position = %q, want %q",
			fields['P'], wantPosition)
	}
	if has(msgs, msgParseComplete) {
		t.Fatalf("failed CTE Parse emitted ParseComplete: %s", tags(msgs))
	}

	msgs = c.query(`SELECT id FROM users WHERE id = 1`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("post-error recovery tag = %q, want SELECT 1", got)
	}
}

func TestCTEMissingDependencyUTF8CharacterPosition(t *testing.T) {
	c := connect(t)
	statement := `WITH "é" AS (` +
		`SELECT id FROM missing_relation` +
		`) SELECT id FROM "é"`
	want := strconv.Itoa(len([]rune(statement[:strings.Index(
		statement, "missing_relation",
	)])) + 1)
	fields := expectError(t, c.query(statement), sqlstateUndefinedTable)
	if fields['P'] != want {
		t.Fatalf("UTF-8 dependency position = %q, want %q", fields['P'], want)
	}
	c.send(msgParse, parseMsg("utf8-missing-cte-dependency", statement))
	c.send(msgSync, nil)
	fields = expectError(t, c.until(msgReadyForQuery), sqlstateUndefinedTable)
	if fields['P'] != want {
		t.Fatalf("extended UTF-8 dependency position = %q, want %q",
			fields['P'], want)
	}

	msgs := c.query(`SELECT id FROM users WHERE id = 1`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("UTF-8 error recovery tag = %q, want SELECT 1", got)
	}
}

func TestCTESimpleExtendedAndDuplicateRowDescription(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE docs (` +
			`id STRING PRIMARY KEY, kind STRING NOT NULL, n INTEGER NOT NULL)`,
		`INSERT INTO docs VALUES ` +
			`({"id":"a","kind":"x","n":1}),` +
			`({"id":"b","kind":"x","n":2}),` +
			`({"id":"c","kind":"y","n":3})`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}

	msgs := c.query(`
		WITH active AS (
			SELECT id, n FROM docs WHERE kind = 'x'
		)
		SELECT id FROM active WHERE n >= 2 ORDER BY id`)
	rows := rowsOf(t, msgs)
	if len(rows) != 1 || len(rows[0]) != 1 || string(rows[0][0]) != `"b"` {
		t.Fatalf("simple CTE rows = %q, want [[b]]", rows)
	}

	msgs = extendedSQL(c, `
		WITH active(identifier, score) AS MATERIALIZED (
			SELECT id, n FROM docs WHERE kind = $1
		)
		SELECT identifier FROM active WHERE score >= $2 ORDER BY identifier`,
		[][]byte{[]byte("x"), []byte("1")},
	)
	rows = rowsOf(t, msgs)
	if len(rows) != 2 || string(rows[0][0]) != `"a"` ||
		string(rows[1][0]) != `"b"` {
		t.Fatalf("extended CTE rows = %q, want [[a] [b]]", rows)
	}

	msgs = c.query(`
		WITH duplicated AS (SELECT id, id FROM docs WHERE id = 'a')
		SELECT * FROM duplicated`)
	description := decodeRowDescription(
		t, find(t, msgs, msgRowDescription).body,
	)
	if len(description) != 2 || description[0].name != "id" ||
		description[1].name != "id" {
		t.Fatalf("duplicate CTE RowDescription = %+v, want [id id]", description)
	}
}

func TestCTEIntermediateBudgetNoPartialRowsAndRecovery(t *testing.T) {
	srv := newTestServer(t, Options{MaxIntermediateBytes: 1})
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester"})
	statement := `WITH selected AS MATERIALIZED (` +
		`SELECT id FROM users` +
		`) SELECT id FROM selected`

	for _, msgs := range [][]backendMessage{
		c.query(statement),
		extendedSQL(c, statement, nil),
	} {
		expectError(t, msgs, sqlstateProgramLimitExceeded)
		if has(msgs, msgDataRow) {
			t.Fatal("CTE intermediate-budget failure emitted a partial DataRow")
		}
	}

	msgs := c.query(`SELECT id FROM users WHERE id = 1`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("post-CTE-budget recovery tag = %q, want SELECT 1", got)
	}
}

func TestCTEColumnSQLStatesExactPositionsAndRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, n INTEGER)`,
		`INSERT INTO docs VALUES ({"id":"a","n":1})`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	for _, test := range []struct {
		name      string
		statement string
		marker    string
		code      string
	}{
		{
			name: "undefined",
			statement: `/* préfix */ WITH c AS (SELECT id FROM docs) ` +
				`SELECT c.missing FROM c`,
			marker: "missing",
			code:   sqlstateUndefinedColumn,
		},
		{
			name: "ambiguous",
			statement: `/* préfix */ WITH c AS (SELECT id, id FROM docs) ` +
				`SELECT c.id FROM c`,
			marker: "c.id FROM c",
			code:   sqlstateAmbiguousColumn,
		},
		{
			name: "runtime alias arity",
			statement: `/* préfix */ WITH wide(first, second) AS (` +
				`SELECT * FROM docs) SELECT first FROM wide`,
			marker: "second",
			code:   sqlstateInvalidColumnReference,
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

	msgs := c.query(`WITH c AS (SELECT id FROM docs) SELECT id FROM c`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("post-CTE-column-error recovery tag = %q, want SELECT 1", got)
	}
}
