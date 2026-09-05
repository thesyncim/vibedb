package query

import (
	"errors"
	"testing"
)

func TestScalarConditionalWarmExecutionAndFailureRecovery(t *testing.T) {
	statement, err := PrepareStatement(`SELECT GREATEST(0, COALESCE(n, 0) + ?),
		COALESCE(name, 'unknown', CAST('bad' AS BOOLEAN)::TEXT), NULLIF(n, ?)
		FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromSegment(mustSegment(t, `{"id":1,"n":4,"name":"a"}`, `{"id":2,"n":null}`, `{"id":3,"n":-4,"name":"b"}`))
	args := []any{int64(2), int64(4)}
	var exec Exec
	exec.Options.Workers = 1
	run := func() {
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			panic(err)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows != 3 {
			panic("missing conditional rows")
		}
	}
	run()
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("conditional warm execution allocated %v times", allocs)
	}
	bad := FromSegment(mustSegment(t, `{"id":1,"n":{},"name":"a"}`))
	if _, err := statement.RunInto(&exec, bad, args); !errors.Is(err, ErrScalarType) {
		t.Fatalf("conditional type error = %v", err)
	}
	run()
	assertScalarCaseSlotsCleared(t, statement.scalarStatement())
}

func TestScalarConditionalJSONRepresentationAndTypedBoolean(t *testing.T) {
	for _, tc := range []struct{ source, want string }{
		{`SELECT COALESCE('null'::JSON, '{"fallback":1}'::JSON)`, "null"},
		{`SELECT COALESCE('1e0'::JSON, '2'::JSON)`, "1e0"},
		{`SELECT COALESCE(NULL, '{"a":1,"a":2}'::JSON)`, `{"a":1,"a":2}`},
		{`SELECT COALESCE(BOOL 't', 'off')`, "true"},
		{`SELECT GREATEST(BOOL 'f', 'on')`, "true"},
	} {
		stmt, err := PrepareStatement(tc.source)
		if err != nil {
			t.Fatalf("%s: %v", tc.source, err)
		}
		var exec Exec
		cursor, err := stmt.RunInto(&exec, FromSegment(mustSegment(t, `{}`)), nil)
		if err != nil || !cursor.Next() || string(cursor.Cell(0).JSON()) != tc.want {
			t.Fatalf("%s: result %v, error %v", tc.source, cursor, err)
		}
		stmt.Release()
	}
	if _, err := PrepareStatement(`SELECT COALESCE(BOOL 't', 'invalid')`); err == nil {
		t.Fatal("unselected invalid typed Boolean accepted")
	}
}
