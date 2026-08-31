package query

import (
	"errors"
	"strings"
	"testing"
)

const updateExpressionTestDocumentLimit = 1 << 20

func TestDMLUpdateTargetAliasKeepsPhysicalCollectionAndProjectionSource(t *testing.T) {
	const source = `UPDATE docs AS d SET total = d.total + 1 WHERE d.id = ? RETURNING d.total`
	statement, err := PrepareDML(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	tree := statement.Tree()
	if statement.Collection() != "docs" || tree.Update.Alias != "d" ||
		len(tree.Update.Filter.From) != 1 ||
		tree.Update.Filter.From[0].Name != "docs" ||
		tree.Update.Filter.From[0].Alias != "d" ||
		!tree.Update.Filter.From[0].HasAlias {
		t.Fatalf(
			"aliased UPDATE identity = collection %q alias %q from %+v",
			statement.Collection(), tree.Update.Alias, tree.Update.Filter.From,
		)
	}

	var exec Exec
	cursor, err := statement.EvaluateUpdateExpressions(
		&exec, []byte(`{"id":"row","total":2}`), []any{"row"},
		updateExpressionTestDocumentLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || cursor.Cell(0).String() != "3" || cursor.Next() {
		t.Fatalf("aliased UPDATE projection result = %+v", exec.Result)
	}

	returning, err := PrepareParsedStatement(source, tree.Update.Returning)
	if err != nil {
		t.Fatal(err)
	}
	defer returning.Release()
	if returning.Collection() != "docs" ||
		len(tree.Update.Returning.From) != 1 ||
		tree.Update.Returning.From[0].Name != "docs" ||
		tree.Update.Returning.From[0].Alias != "d" ||
		!tree.Update.Returning.From[0].HasAlias {
		t.Fatalf(
			"aliased UPDATE RETURNING identity = collection %q from %+v",
			returning.Collection(), tree.Update.Returning.From,
		)
	}
}

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
		updateExpressionTestDocumentLimit,
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
		updateExpressionTestDocumentLimit,
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

func TestDMLUpdateExpressionsBoundSourceAndResetExecutionState(t *testing.T) {
	statement, err := PrepareDML(`UPDATE docs SET value = value + 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	document := []byte(`{"value":1}`)
	exec := Exec{
		Stats: ExecStats{RowsTotal: 41, IndexBounded: true},
	}
	exec.Result.RowCount = 7
	if _, err := statement.EvaluateUpdateExpressions(
		&exec, document, nil, len(document)-1,
	); err == nil || !strings.Contains(err.Error(), "source document has 11 bytes, limit 10") {
		t.Fatalf("oversize source error = %v", err)
	}
	if exec.Stats != (ExecStats{}) {
		t.Fatalf("oversize source retained stale stats: %+v", exec.Stats)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("oversize source retained %d result rows", exec.Result.RowCount)
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	exec.Stats = ExecStats{RowsTotal: 73, IndexBounded: true}
	exec.Result.RowCount = 9
	if _, err := statement.EvaluateUpdateExpressions(
		&exec, document, nil, len(document),
	); !errors.Is(err, ErrCanceled) {
		t.Fatalf("pre-canceled evaluation error = %T %v, want ErrCanceled", err, err)
	}
	if exec.Stats != (ExecStats{}) {
		t.Fatalf("pre-canceled evaluation retained stale stats: %+v", exec.Stats)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("pre-canceled evaluation retained %d result rows", exec.Result.RowCount)
	}
}
