package pgwire

import "testing"

func TestScalarConditionalWireRowsAndRecovery(t *testing.T) {
	c := connect(t)
	messages := c.query(`SELECT COALESCE(NULL, 'fallback'), GREATEST(1, NULL, 5), LEAST(7, 2), NULLIF(4, 4)`)
	if has(messages, msgErrorResponse) {
		t.Fatalf("conditional query: %q", messages)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != "fallback" || string(rows[0][1]) != "5" || string(rows[0][2]) != "2" || rows[0][3] != nil {
		t.Fatalf("conditional rows = %q", rows)
	}
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if description[0].oid != oidText {
		t.Fatalf("conditional metadata = %+v", description)
	}
	messages = extendedSQL(c, `SELECT COALESCE(NULLIF($1::TEXT, ''), 'fallback'), GREATEST($2::NUMERIC, 0)`, [][]byte{[]byte(""), []byte("-1")})
	if has(messages, msgErrorResponse) {
		t.Fatalf("conditional extended query: %q", messages)
	}
	rows = rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != "fallback" || string(rows[0][1]) != "0" {
		t.Fatalf("conditional extended rows = %q", rows)
	}
	expectError(t, c.query(`SELECT COALESCE(NULL, CAST('bad' AS BOOLEAN))`), sqlstateInvalidTextRepresentation)
	messages = c.query(`SELECT COALESCE(TRUE, CAST('bad' AS BOOLEAN))`)
	if has(messages, msgErrorResponse) || string(rowsOf(t, messages)[0][0]) != "t" {
		t.Fatalf("conditional recovery = %q", messages)
	}
}
