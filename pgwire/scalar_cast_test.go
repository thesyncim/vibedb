package pgwire

import (
	"bytes"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestScalarCastSQLStatesMetadataFormatsAndRecovery(t *testing.T) {
	c := connect(t)
	messages := c.query(`SELECT CAST('1.25' AS NUMERIC) AS n, 'yes'::BOOLEAN AS b,
		CAST('{"x":1}' AS JSON) AS j, 12::TEXT AS t FROM users WHERE id = 1`)
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 4 || description[0].oid != oidJSON || description[1].oid != oidBool ||
		description[2].oid != oidJSON || description[3].oid != oidText {
		t.Fatalf("CAST RowDescription = %+v", description)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != "1.25" || string(rows[0][1]) != "t" ||
		string(rows[0][2]) != `{"x":1}` || string(rows[0][3]) != "12" {
		t.Fatalf("CAST rows = %q", rows)
	}

	for _, test := range []struct {
		sql  string
		code string
	}{
		{`SELECT CAST('wat' AS BOOLEAN) FROM users`, sqlstateInvalidTextRepresentation},
		{`SELECT CAST('1x' AS NUMERIC) FROM users`, sqlstateInvalidTextRepresentation},
		{`SELECT CAST('wat' AS JSON) FROM users`, sqlstateInvalidTextRepresentation},
		{`SELECT CAST('1e999999999999999999999' AS NUMERIC) FROM users`, sqlstateNumericValueOutOfRange},
		{`SELECT CAST(true AS NUMERIC) FROM users`, sqlstateDatatypeMismatch},
		{`SELECT CAST(id AS jsonb) FROM users`, sqlstateFeatureNotSupported},
		{`SELECT id::jsonb FROM users`, sqlstateFeatureNotSupported},
	} {
		fields := expectError(t, c.query(test.sql), test.code)
		if fields['P'] == "" {
			t.Fatalf("%q omitted error position", test.sql)
		}
		if strings.Contains(fields['M'], "wat") {
			t.Fatalf("%q leaked CAST input in error message %q", test.sql, fields['M'])
		}
	}

	messages = extendedSQL(c, `SELECT $1::BOOLEAN FROM users WHERE id = 1`, [][]byte{[]byte("Tr")})
	description = decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 1 || description[0].oid != oidBool {
		t.Fatalf("extended CAST RowDescription = %+v", description)
	}
	rows = rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != "t" {
		t.Fatalf("extended CAST rows = %q", rows)
	}

	c.send(msgParse, parseMsg("cast_bool", `SELECT 'off'::BOOLEAN FROM users WHERE id = 1`))
	c.send(msgBind, bindMsg("cast_bool_portal", "cast_bool", nil, nil, []int16{formatBinary}))
	c.send(msgDescribe, describeMsg(targetPortal, "cast_bool_portal"))
	c.send(msgExecute, executeMsg("cast_bool_portal", 0))
	c.send(msgSync, nil)
	binary := c.until(msgReadyForQuery)
	binaryDescription := decodeRowDescription(t, find(t, binary, msgRowDescription).body)
	if len(binaryDescription) != 1 || binaryDescription[0].oid != oidBool ||
		binaryDescription[0].format != formatBinary {
		t.Fatalf("binary CAST RowDescription = %+v", binaryDescription)
	}
	if binaryRows := rowsOf(t, binary); len(binaryRows) != 1 ||
		len(binaryRows[0]) != 1 || !bytes.Equal(binaryRows[0][0], []byte{0}) {
		t.Fatalf("binary CAST rows = %v", binaryRows)
	}

	for _, source := range []string{
		`SELECT CAST('not-numeric' AS NUMERIC), 1 / 0 FROM users WHERE 1 = 0`,
		`SELECT CAST('not-numeric' AS NUMERIC), 1 / 0 FROM users OFFSET 7`,
	} {
		lazy := c.query(source)
		if has(lazy, msgErrorResponse) || len(rowsOf(t, lazy)) != 0 {
			t.Fatalf("lazy projection %q failed or returned rows: %s", source, tags(lazy))
		}
	}

	if pg := asPGErrorIn(&query.ScalarInvalidTextError{Pos: 3, Target: "NUMERIC"}, "ééCAST"); pg.code != sqlstateInvalidTextRepresentation || pg.position != 3 {
		t.Fatalf("invalid-text mapping = %s/%d", pg.code, pg.position)
	}
}
