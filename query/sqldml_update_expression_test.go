package query

import (
	"testing"
)

func TestDMLUpdateExpressionsEvaluateOneOldRowSimultaneously(t *testing.T) {
	statement, err := PrepareDML(
		`UPDATE docs SET a = b, b = a, total = a + b, note = 'direct' WHERE id = ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if !statement.HasUpdateExpressions() {
		t.Fatal("computed UPDATE did not retain an assignment projection")
	}
	args := []any{"row"}
	if err := statement.ValidateUpdateExpressionBindings(args); err != nil {
		t.Fatalf("ValidateUpdateExpressionBindings: %v", err)
	}
	var exec Exec
	cursor, err := statement.EvaluateUpdateExpressions(
		&exec, []byte(`{"id":"row","a":1,"b":2}`), args,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("computed UPDATE projection returned no row")
	}
	for column, want := range []string{"2", "1", "3"} {
		if got := cursor.Cell(column).String(); got != want {
			t.Fatalf("computed column %d = %s, want %s", column, got, want)
		}
	}
	if cursor.Next() {
		t.Fatal("computed UPDATE projection returned more than one row")
	}
}

func TestDMLUpdateExpressionsPreserveNullAndExactDecimal(t *testing.T) {
	statement, err := PrepareDML(
		`UPDATE docs SET exact = exact + 1, missing = missing || 'x', nil = nil + 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	cursor, err := statement.EvaluateUpdateExpressions(
		&exec,
		[]byte(`{"exact":9007199254740992,"nil":null}`),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("computed UPDATE projection returned no row")
	}
	for column, want := range []string{"9.007199254740993e15", "null", "null"} {
		if got := cursor.Cell(column).String(); got != want {
			t.Fatalf("computed column %d = %s, want %s", column, got, want)
		}
	}
}

func TestDMLUpdateExpressionAndFilterParameterMetadataShareFrame(t *testing.T) {
	const source = `UPDATE users SET enabled = CASE BOOL 't' WHEN ? THEN enabled ELSE false END WHERE id IN (SELECT TEXT 'x' UNION ALL SELECT ? FROM other)`
	statement, err := PrepareDML(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if got := statement.NumParams(); got != 2 {
		t.Fatalf("NumParams = %d, want 2", got)
	}
	if got := statement.ParameterType(0); got != ParameterTypeBool {
		t.Fatalf("SET parameter type = %s, want boolean", got)
	}
	if got := statement.ParameterType(1); got != ParameterTypeText {
		t.Fatalf("WHERE parameter type = %s, want text", got)
	}
}

func TestDMLDirectUpdateAssignmentsKeepEvaluatorAbsent(t *testing.T) {
	statement, err := PrepareDML(`UPDATE docs SET enabled = true, note = ?, gone = NULL`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.HasUpdateExpressions() {
		t.Fatal("direct UPDATE assignments unexpectedly compiled an expression projection")
	}
	if err := statement.ValidateUpdateExpressionBindings([]any{"ok"}); err != nil {
		t.Fatalf("direct assignment expression validation = %v", err)
	}
}
