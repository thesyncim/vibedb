package pgwire

import (
	"fmt"
	"strings"
	"testing"
)

func TestRepeatedWireParameterTargetDefaultYieldsToContextualInference(t *testing.T) {
	const source = `SELECT $1, CASE BOOL 't' WHEN $1 THEN 1 ELSE 0 END`
	c := connect(t)
	c.send(msgParse, parseMsg("target_default_conflict", source))
	c.send(msgSync, nil)
	fields := expectError(t, c.until(msgReadyForQuery), sqlstateAmbiguousParameter)
	wantPosition := strings.Index(source, "$1") + 1
	if fields['P'] != fmt.Sprint(wantPosition) ||
		fields['D'] != "boolean versus text" {
		t.Fatalf("target-default conflict fields = %v, want P=%d D=%q",
			fields, wantPosition, "boolean versus text")
	}
}

func TestIntrinsicTypedConflictsKeepPostgreSQLSQLStates(t *testing.T) {
	t.Run("simple CASE operator", func(t *testing.T) {
		const source = `SELECT CASE BOOL 't' WHEN TEXT 'x' THEN 1 ELSE 0 END`
		c := connect(t)
		fields := expectError(t, c.query(source), sqlstateUndefinedFunction)
		wantPosition := strings.Index(source, "TEXT") + 1
		if fields['P'] != fmt.Sprint(wantPosition) {
			t.Fatalf("intrinsic CASE position = %q, want %d; fields %v",
				fields['P'], wantPosition, fields)
		}
	})

	t.Run("VALUES set common type", func(t *testing.T) {
		c := connect(t)
		fields := expectError(t,
			c.query(`VALUES (BOOL 't') INTERSECT VALUES (TEXT 'x')`),
			sqlstateDatatypeMismatch,
		)
		if fields['P'] != "" {
			t.Fatalf("intrinsic VALUES mismatch unexpectedly had P=%q; fields %v",
				fields['P'], fields)
		}
	})
}

func TestTypedWindowSchemaAndRowsPreserveInputDomains(t *testing.T) {
	const source = `SELECT q.v,
		FIRST_VALUE(q.v) OVER () AS first_v,
		ROW_NUMBER() OVER () AS rn
	FROM (SELECT $1 AS v) AS q`
	c := connect(t)
	c.send(msgParse, parseMsg("typed_window", source, oidBool))
	c.send(msgDescribe, describeMsg(targetStatement, "typed_window"))
	c.send(msgBind, bindMsg("", "typed_window", nil, [][]byte{[]byte("yes")}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	if has(messages, msgErrorResponse) {
		t.Fatalf("typed window failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	parameters := decodeParameterDescription(t,
		find(t, messages, msgParameterDesc).body)
	if len(parameters) != 1 || parameters[0] != oidBool {
		t.Fatalf("typed window ParameterDescription = %v, want [%d]",
			parameters, oidBool)
	}
	description := decodeRowDescription(t,
		find(t, messages, msgRowDescription).body)
	if len(description) != 3 ||
		description[0].oid != oidBool || description[0].size != 1 ||
		description[1].oid != oidBool || description[1].size != 1 ||
		description[2].oid != oidInt8 || description[2].size != 8 {
		t.Fatalf("typed window RowDescription = %+v, want bool/bool/int8", description)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 1 || len(rows[0]) != 3 ||
		string(rows[0][0]) != "t" || string(rows[0][1]) != "t" ||
		string(rows[0][2]) != "1" {
		t.Fatalf("typed window rows = %q, want [[t t 1]]", rows)
	}
}
