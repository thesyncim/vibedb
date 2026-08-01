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

func TestPGWireCorrelatedLateralRefusalIs0A000AndPositioned(t *testing.T) {
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
	fields := expectError(t, c.query(correlatedLateralWireSQL), sqlstateFeatureNotSupported)
	if !strings.Contains(fields['M'], "correlated LATERAL execution") {
		t.Fatalf("wire refusal = %q", fields['M'])
	}
	bytePos := strings.Index(correlatedLateralWireSQL, "LATERAL")
	wantPosition := utf8.RuneCountInString(correlatedLateralWireSQL[:bytePos]) + 1
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
