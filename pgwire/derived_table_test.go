package pgwire

import (
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
			err: &query.IntermediateBudgetError{
				Resource: "derived relation", Bytes: 2, Limit: 1,
			},
			code: sqlstateProgramLimitExceeded,
		},
	}
	for _, test := range tests {
		if got := asPGError(test.err).code; got != test.code {
			t.Errorf("asPGError(%T) = %q, want %q", test.err, got, test.code)
		}
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
	msgs := c.query(`SELECT d.id FROM (SELECT id FROM docs) d`)
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("recovery tag = %q, want SELECT 1", got)
	}
}
