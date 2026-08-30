package pgwire

import (
	"bytes"
	"strconv"
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

func TestPostgreSQLTypedStringConstantsWireMetadataValuesAndErrors(t *testing.T) {
	c := connect(t)
	messages := c.query(`SELECT BOOL 'tr', BOOLEAN 'off', TEXT 'x',
		TEXT 't'::BOOL::TEXT FROM users WHERE id = 1`)
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	wantNames := []string{"bool", "bool", "text", "text"}
	wantOIDs := []int32{oidBool, oidBool, oidText, oidText}
	if len(description) != len(wantNames) {
		t.Fatalf("typed-constant RowDescription = %+v", description)
	}
	for i := range description {
		if description[i].name != wantNames[i] || description[i].oid != wantOIDs[i] {
			t.Fatalf("typed-constant column[%d] = %+v, want %q/OID %d",
				i, description[i], wantNames[i], wantOIDs[i])
		}
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || len(rows[0]) != 4 ||
		string(rows[0][0]) != "t" || string(rows[0][1]) != "f" ||
		string(rows[0][2]) != "x" || string(rows[0][3]) != "true" {
		t.Fatalf("typed-constant rows = %q", rows)
	}

	ordered := c.query(`SELECT BOOL 't' FROM users WHERE id = 1 ORDER BY bool`)
	if has(ordered, msgErrorResponse) || len(rowsOf(t, ordered)) != 1 {
		t.Fatalf("generated BOOL label did not resolve in ORDER BY: %s", tags(ordered))
	}

	valuesMessages := c.query(`VALUES (BOOL 't', TEXT 'x')`)
	valuesDescription := decodeRowDescription(t, find(t, valuesMessages, msgRowDescription).body)
	if len(valuesDescription) != 2 || valuesDescription[0].name != "column1" ||
		valuesDescription[0].oid != oidBool || valuesDescription[1].name != "column2" ||
		valuesDescription[1].oid != oidText {
		t.Fatalf("typed VALUES RowDescription = %+v", valuesDescription)
	}
	valuesRows := rowsOf(t, valuesMessages)
	if len(valuesRows) != 1 || string(valuesRows[0][0]) != "t" || string(valuesRows[0][1]) != "x" {
		t.Fatalf("typed VALUES rows = %q", valuesRows)
	}

	boundValues := extendedSQL(c,
		`VALUES (BOOL 't', TEXT 'x'), ($1, $2)`,
		[][]byte{[]byte("off"), []byte("bound")},
	)
	boundDescription := decodeRowDescription(t, find(t, boundValues, msgRowDescription).body)
	if len(boundDescription) != 2 || boundDescription[0].oid != oidBool ||
		boundDescription[1].oid != oidText {
		t.Fatalf("bound typed VALUES RowDescription = %+v", boundDescription)
	}
	boundRows := rowsOf(t, boundValues)
	if len(boundRows) != 2 || string(boundRows[0][0]) != "t" ||
		string(boundRows[0][1]) != "x" || string(boundRows[1][0]) != "f" ||
		string(boundRows[1][1]) != "bound" {
		t.Fatalf("bound typed VALUES rows = %q", boundRows)
	}
	expectError(t, extendedSQL(c,
		`VALUES (BOOL 't'), ($1)`, [][]byte{[]byte("o")},
	), sqlstateInvalidTextRepresentation)

	for _, source := range []string{
		`SELECT BOOL 'o' FROM users WHERE 1 = 0`,
		`SELECT BOOLEAN 'not-bool' FROM users OFFSET 999`,
	} {
		fields := expectError(t, c.query(source), sqlstateInvalidTextRepresentation)
		if fields['P'] == "" {
			t.Fatalf("%q omitted typed-input position", source)
		}
		if strings.Contains(fields['M'], "not-bool") || strings.Contains(fields['M'], "'o'") {
			t.Fatalf("%q leaked typed input in %q", source, fields['M'])
		}
	}

	for _, source := range []string{
		`SELECT BOOL 't'::NUMERIC`,
		`SELECT BOOL 't'::JSON`,
		`SELECT CASE WHEN false THEN BOOL 't'::NUMERIC ELSE 1 END`,
	} {
		fields := expectError(t, c.query(source), sqlstateCannotCoerce)
		if fields['P'] == "" {
			t.Fatalf("%q omitted cannot-coerce position", source)
		}
	}

	for _, source := range []string{
		`SELECT pg_catalog.bool 't'`,
		`SELECT bool(1) 't'`,
		`SELECT bool[] 't'`,
		`SELECT BOOL E't'`,
		`SELECT BOOL U&'t'`,
		`SELECT VARCHAR 'x'`,
		`SELECT CHAR 'x'`,
		`SELECT CHARACTER 'x'`,
	} {
		fields := expectError(t, c.query(source), sqlstateFeatureNotSupported)
		if fields['P'] == "" {
			t.Fatalf("%q omitted unsupported typed-constant position", source)
		}
	}

	for _, test := range []struct {
		source string
		code   string
		at     string
	}{
		{
			source: `SELECT id FROM users WHERE EXISTS (SELECT BOOL 'o')`,
			code:   sqlstateInvalidTextRepresentation,
			at:     "'o'",
		},
		{
			source: `SELECT id FROM users WHERE id = (SELECT BOOL 't'::NUMERIC)`,
			code:   sqlstateCannotCoerce,
			at:     "NUMERIC",
		},
	} {
		fields := expectError(t, c.query(test.source), test.code)
		wantPosition := strconv.Itoa(strings.LastIndex(test.source, test.at) + 1)
		if fields['P'] != wantPosition {
			t.Fatalf("nested typed error %q position = %q, want %s",
				test.source, fields['P'], wantPosition)
		}
	}
}
