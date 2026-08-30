package pgwire

import (
	"strings"
	"testing"
)

func TestPostgreSQLTypedCaseCommonTypeWireMetadataErrorsAndRecovery(t *testing.T) {
	c := connect(t)
	messages := c.query(`SELECT
		CASE WHEN TRUE THEN BOOL 't' ELSE 'off' END AS a,
		CASE WHEN TRUE THEN 'no' ELSE BOOLEAN 't' END AS b,
		CASE WHEN TRUE THEN TEXT 'typed' ELSE 'plain' END AS c
		FROM users WHERE id = 1`)
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 3 || description[0].oid != oidBool ||
		description[1].oid != oidBool || description[2].oid != oidText {
		t.Fatalf("typed CASE RowDescription = %+v", description)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || len(rows[0]) != 3 ||
		string(rows[0][0]) != "t" || string(rows[0][1]) != "f" ||
		string(rows[0][2]) != "typed" {
		t.Fatalf("typed CASE rows = %q", rows)
	}

	for _, source := range []string{
		`SELECT CASE WHEN TRUE THEN BOOL 't' ELSE 'not-bool' END FROM users`,
		`SELECT CASE WHEN FALSE THEN 'not-bool' ELSE BOOLEAN 'f' END FROM users`,
	} {
		fields := expectError(t, c.query(source), sqlstateInvalidTextRepresentation)
		if fields['P'] == "" {
			t.Fatalf("%q omitted typed CASE input position", source)
		}
		if strings.Contains(fields['M'], "not-bool") {
			t.Fatalf("%q leaked typed CASE input in %q", source, fields['M'])
		}
	}

	recovered := c.query(`SELECT CASE WHEN TRUE THEN BOOL 't' ELSE 'off' END FROM users WHERE id = 1`)
	if has(recovered, msgErrorResponse) || len(rowsOf(t, recovered)) != 1 {
		t.Fatalf("typed CASE session did not recover: %s", tags(recovered))
	}
}

func TestPostgreSQLTypedSimpleCaseSelectorCoercesUnknownWhenOnWire(t *testing.T) {
	c := connect(t)
	messages := c.query(`SELECT CASE BOOL 't'
		WHEN 'off' THEN BOOL 'f' WHEN 'yes' THEN BOOL 't' ELSE BOOL 'f' END
		FROM users WHERE id = 1`)
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 1 || description[0].oid != oidBool {
		t.Fatalf("typed simple CASE RowDescription = %+v", description)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || len(rows[0]) != 1 || string(rows[0][0]) != "t" {
		t.Fatalf("typed simple CASE rows = %q", rows)
	}

	source := `SELECT CASE BOOL 't' WHEN BOOL 't' THEN 1 WHEN 'not-bool' THEN 2 ELSE 0 END FROM users`
	fields := expectError(t, c.query(source), sqlstateInvalidTextRepresentation)
	if fields['P'] == "" || strings.Contains(fields['M'], "not-bool") {
		t.Fatalf("typed simple CASE error fields = %#v", fields)
	}
}

func TestPostgreSQLNestedTypedCaseMetadataCastAndOperatorErrors(t *testing.T) {
	c := connect(t)
	messages := c.query(`SELECT
		CASE WHEN TRUE THEN CASE WHEN FALSE THEN BOOL 'f' ELSE 'yes' END ELSE 'off' END,
		CASE WHEN TRUE THEN CASE WHEN FALSE THEN TEXT 'x' ELSE 'inner' END ELSE 'outer' END
		FROM users WHERE id = 1`)
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 2 || description[0].oid != oidBool ||
		description[1].oid != oidText {
		t.Fatalf("nested typed CASE RowDescription = %+v", description)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != "t" || string(rows[0][1]) != "inner" {
		t.Fatalf("nested typed CASE rows = %q", rows)
	}

	expectError(t, c.query(`SELECT (CASE WHEN TRUE THEN
		CASE WHEN FALSE THEN BOOL 'f' ELSE 'yes' END ELSE 'off' END)::NUMERIC`),
		sqlstateCannotCoerce)
	for _, source := range []string{
		`SELECT CASE NULL WHEN BOOL 't' THEN 1 ELSE 0 END`,
		`SELECT CASE $1 WHEN BOOL 't' THEN 1 ELSE 0 END`,
	} {
		fields := expectError(t, c.query(source), sqlstateUndefinedFunction)
		if fields['P'] == "" {
			t.Fatalf("%q omitted undefined-operator position", source)
		}
	}

	recovered := c.query(`SELECT CASE WHEN TRUE THEN BOOL 't' ELSE 'off' END
		FROM users WHERE id = 1`)
	if has(recovered, msgErrorResponse) || len(rowsOf(t, recovered)) != 1 {
		t.Fatalf("nested typed CASE error did not recover: %s", tags(recovered))
	}
}

func TestPostgreSQLTypedCaseParameterDescriptionAndExecution(t *testing.T) {
	const source = `SELECT
		CASE BOOL 't' WHEN $1 THEN BOOL 't' ELSE BOOL 'f' END,
		CASE WHEN FALSE THEN BOOL 'f' ELSE $2 END,
		CASE TEXT 'x' WHEN $3 THEN TEXT 'matched' ELSE $4 END
		FROM users WHERE id = 1`
	c := connect(t)
	c.send(msgParse, parseMsg("typed_case_params", source))
	c.send(msgDescribe, describeMsg(targetStatement, "typed_case_params"))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	if has(messages, msgErrorResponse) {
		t.Fatalf("typed CASE Parse/Describe failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	got := decodeParameterDescription(t, find(t, messages, msgParameterDesc).body)
	want := []int32{oidBool, oidBool, oidText, oidText}
	if len(got) != len(want) {
		t.Fatalf("typed CASE ParameterDescription = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("typed CASE ParameterDescription = %v, want %v", got, want)
		}
	}

	messages = extendedSQL(c, source, [][]byte{
		[]byte("yes"), []byte("off"), []byte("x"), []byte("unused"),
	})
	if has(messages, msgErrorResponse) {
		t.Fatalf("typed CASE Execute failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || len(rows[0]) != 3 ||
		string(rows[0][0]) != "t" || string(rows[0][1]) != "f" ||
		string(rows[0][2]) != "matched" {
		t.Fatalf("typed CASE parameter rows = %q", rows)
	}

	expectError(t, extendedSQL(c, source, [][]byte{
		[]byte("o"), []byte("off"), []byte("x"), []byte("unused"),
	}), sqlstateInvalidTextRepresentation)
}

func TestPostgreSQLTypedCaseRepeatedParameterCannotResolveAsBoolAndText(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("typed_case_conflict", `SELECT
		CASE BOOL 't' WHEN $1 THEN 1 ELSE 0 END,
		CASE TEXT 'x' WHEN $1 THEN 1 ELSE 0 END`))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateAmbiguousParameter)
}
