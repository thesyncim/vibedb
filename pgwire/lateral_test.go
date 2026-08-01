package pgwire

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

const correlatedLateralWireSQL = `SELECT a.id, d.id
FROM accounts a
CROSS JOIN LATERAL (
	SELECT i.id FROM items i WHERE i.owner = a.id
) d`

func TestPGWireCorrelatedLateralExecutesAndRemainingRefusalIsPositioned(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, statement := range []string{
		`CREATE TABLE accounts (id STRING PRIMARY KEY)`,
		`CREATE TABLE items (id STRING PRIMARY KEY, owner STRING)`,
		`INSERT INTO accounts VALUES ({"id":"a1"})`,
		`INSERT INTO items VALUES ({"id":"i1","owner":"a1"})`,
	} {
		if msgs := c.query(statement); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", statement,
				formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	for _, msgs := range [][]backendMessage{
		c.query(correlatedLateralWireSQL),
		extendedSQL(c, correlatedLateralWireSQL, nil),
	} {
		rows := rowsOf(t, msgs)
		if len(rows) != 1 || len(rows[0]) != 2 ||
			string(rows[0][0]) != `"a1"` || string(rows[0][1]) != `"i1"` {
			t.Fatalf("correlated LATERAL rows = %q, want [[a1 i1]]", rows)
		}
		if got := commandTagOf(t, msgs); got != "SELECT 1" {
			t.Fatalf("correlated LATERAL command tag = %q, want SELECT 1", got)
		}
	}

	unsupported := `SELECT a.id, d.id FROM accounts a JOIN LATERAL (` +
		`SELECT i.id, i.owner FROM items i WHERE i.owner = a.id) d USING (id)`
	fields := expectError(t, c.query(unsupported), sqlstateFeatureNotSupported)
	if !strings.Contains(fields['M'], "LATERAL") || !strings.Contains(fields['M'], "USING") {
		t.Fatalf("wire refusal = %q", fields['M'])
	}
	bytePos := strings.Index(unsupported, "USING")
	wantPosition := utf8.RuneCountInString(unsupported[:bytePos]) + 1
	if fields['P'] != strconv.Itoa(wantPosition) {
		t.Fatalf("wire position = %q, want %d", fields['P'], wantPosition)
	}

	msgs := c.query(`SELECT a.id, d.id FROM accounts a ` +
		`CROSS JOIN LATERAL (SELECT i.id FROM items i) d`)
	if has(msgs, msgErrorResponse) {
		t.Fatalf("decorrelated LATERAL: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
}
