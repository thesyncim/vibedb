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
