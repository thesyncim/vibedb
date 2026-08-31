package query

import (
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
)

func TestDMLConflictExpressionsPrepareFromSQLNamespaces(t *testing.T) {
	statement, err := PrepareDML(`
		INSERT INTO metrics (id, total, delta, enabled) VALUES (?, ?, ?, ?)
		ON CONFLICT DO UPDATE SET
			total = metrics.total + EXCLUDED.delta,
			enabled = CASE WHEN EXCLUDED.enabled THEN metrics.enabled ELSE false END`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if !statement.HasConflictUpdateExpressions() ||
		statement.ConflictUpdateExpressionCount() != 2 {
		t.Fatalf(
			"SQL conflict projection = (has=%t count=%d)",
			statement.HasConflictUpdateExpressions(),
			statement.ConflictUpdateExpressionCount(),
		)
	}
	if err := statement.ValidateConflictUpdateExpressionBindings(
		[]any{"row", int64(10), int64(2), true},
	); err != nil {
		t.Fatalf("ValidateConflictUpdateExpressionBindings: %v", err)
	}

	var exec Exec
	cursor, err := statement.EvaluateConflictUpdateExpressions(
		&exec,
		[]byte(`{"id":"row","total":10,"delta":1,"enabled":false}`),
		[]byte(`{"id":"row","total":100,"delta":2,"enabled":true}`),
		[]any{"row", int64(10), int64(2), true},
		1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("SQL conflict projection returned no row")
	}
	for column, want := range []string{"1.2e1", "false"} {
		if got := cursor.Cell(column).String(); got != want {
			t.Fatalf("SQL conflict column %d = %s, want %s", column, got, want)
		}
	}
	if cursor.Next() {
		t.Fatal("SQL conflict projection returned more than one row")
	}
}

func TestDMLConflictExpressionsEvaluateCurrentAndExcludedSimultaneously(t *testing.T) {
	currentA := conflictTestPath(0, "a", 101)
	excludedB := conflictTestPath(1, "b", 105)
	excludedFlag := conflictTestPath(1, "flag", 141)
	currentB := conflictTestPath(0, "b", 169)
	excludedA := conflictTestPath(1, "a", 177)
	assignments := []sqlast.UpdateAssignment{
		{
			Column: "total",
			Value:  sqlast.Operand{Kind: sqlast.OperandExpression, Pos: 101},
			Expr: &sqlast.ScalarExpr{
				Kind:  sqlast.ScalarBinary,
				Op:    sqlast.ScalarAdd,
				Left:  &sqlast.ScalarExpr{Kind: sqlast.ScalarPath, Path: currentA, Pos: currentA.Pos},
				Right: &sqlast.ScalarExpr{Kind: sqlast.ScalarPath, Path: excludedB, Pos: excludedB.Pos},
				Pos:   103,
			},
			Pos: 95,
		},
		{
			Column: "chosen",
			Value:  sqlast.Operand{Kind: sqlast.OperandExpression, Pos: 135},
			Expr: &sqlast.ScalarExpr{
				Kind: sqlast.ScalarCase,
				Whens: []sqlast.ScalarWhen{{
					Predicate: &sqlast.Expr{
						Kind: sqlast.ExprCompare,
						Path: excludedFlag,
						Op:   sqlast.OpEq,
						Value: sqlast.Operand{
							Kind: sqlast.OperandBool, Bool: true, Pos: 158,
						},
						Column: -1,
						Pos:    141,
					},
					Result: &sqlast.ScalarExpr{
						Kind: sqlast.ScalarPath, Path: currentB, Pos: currentB.Pos,
					},
					Pos: 136,
				}},
				Else: &sqlast.ScalarExpr{
					Kind: sqlast.ScalarPath, Path: excludedA, Pos: excludedA.Pos,
				},
				Pos: 135,
			},
			Pos: 127,
		},
	}
	tree := conflictTestStatement(assignments, 0)
	statement, err := PrepareParsedDML("INSERT conflict expression test", tree)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	if !statement.HasConflictUpdateExpressions() ||
		statement.ConflictUpdateExpressionCount() != 2 ||
		statement.FirstConflictUpdateExpressionPosition() != 103 {
		t.Fatalf(
			"conflict expression metadata = (has=%t count=%d pos=%d)",
			statement.HasConflictUpdateExpressions(),
			statement.ConflictUpdateExpressionCount(),
			statement.FirstConflictUpdateExpressionPosition(),
		)
	}
	// Preparation must not leak the executor's envelope representation into the
	// parser-owned tree exposed through DMLStatement.Tree.
	for _, test := range []struct {
		name   string
		path   *sqlast.PathExpr
		source int
		key    string
	}{
		{"current a", currentA, 0, "a"},
		{"excluded b", excludedB, 1, "b"},
		{"excluded flag", excludedFlag, 1, "flag"},
		{"current b", currentB, 0, "b"},
		{"excluded a", excludedA, 1, "a"},
	} {
		if test.path.Source != test.source || len(test.path.Segments) != 1 ||
			test.path.Segments[0].Key != test.key || test.path.Segments[0].IsIndex {
			t.Fatalf("%s path was mutated during prepare: %#v", test.name, test.path)
		}
	}

	var exec Exec
	cursor, err := statement.EvaluateConflictUpdateExpressions(
		&exec,
		[]byte(`{"a":1,"b":2,"flag":false}`),
		[]byte(`{"a":10,"b":20,"flag":true}`),
		nil,
		1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("conflict expression projection returned no row")
	}
	for column, want := range []string{"2.1e1", "2"} {
		if got := cursor.Cell(column).String(); got != want {
			t.Fatalf("computed conflict column %d = %s, want %s", column, got, want)
		}
	}
	if cursor.Next() {
		t.Fatal("conflict expression projection returned more than one row")
	}
}

func TestDMLConflictExpressionParameterMetadataUsesGlobalOrdinals(t *testing.T) {
	selector := &sqlast.ScalarExpr{
		Kind: sqlast.ScalarCast,
		Cast: sqlast.ScalarCastBoolean,
		Left: &sqlast.ScalarExpr{
			Kind: sqlast.ScalarLiteral,
			Value: sqlast.Operand{
				Kind: sqlast.OperandBool, Bool: true, Pos: 82,
			},
			Pos: 82,
		},
		TypedConstant: true,
		Pos:           77,
		TargetPos:     77,
	}
	parameter := &sqlast.ScalarExpr{
		Kind: sqlast.ScalarLiteral,
		Value: sqlast.Operand{
			Kind: sqlast.OperandParam, Ordinal: 2, Pos: 91,
		},
		Pos: 91,
	}
	expression := &sqlast.ScalarExpr{
		Kind: sqlast.ScalarCase,
		Left: selector,
		Whens: []sqlast.ScalarWhen{{
			Match:  parameter,
			Result: conflictTestScalarPath(0, "enabled", 98),
			Pos:    86,
		}},
		Else: &sqlast.ScalarExpr{
			Kind:  sqlast.ScalarLiteral,
			Value: sqlast.Operand{Kind: sqlast.OperandBool, Pos: 112},
			Pos:   112,
		},
		Pos: 72,
	}
	statement, err := PrepareParsedDML(
		"INSERT global conflict parameter test",
		conflictTestStatement([]sqlast.UpdateAssignment{{
			Column: "enabled",
			Value:  sqlast.Operand{Kind: sqlast.OperandExpression, Pos: 72},
			Expr:   expression,
			Pos:    64,
		}}, 3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	if statement.NumParams() != 3 ||
		statement.ParameterType(0) != ParameterTypeUnspecified ||
		statement.ParameterType(1) != ParameterTypeUnspecified ||
		statement.ParameterType(2) != ParameterTypeBool ||
		statement.ParameterTypePosition(2) != 91 {
		t.Fatalf(
			"global conflict parameter metadata = count %d types [%s %s %s] pos %d",
			statement.NumParams(), statement.ParameterType(0),
			statement.ParameterType(1), statement.ParameterType(2),
			statement.ParameterTypePosition(2),
		)
	}
	if err := statement.ValidateConflictUpdateExpressionBindings(
		[]any{"candidate", int64(7), true},
	); err != nil {
		t.Fatalf("valid global bindings: %v", err)
	}
}

func TestDMLConflictExpressionsBoundBothInputsAndResetExecutionState(t *testing.T) {
	statement, err := PrepareParsedDML(
		"INSERT bounded conflict expression test",
		conflictTestStatement([]sqlast.UpdateAssignment{{
			Column: "value",
			Value:  sqlast.Operand{Kind: sqlast.OperandExpression, Pos: 31},
			Expr: &sqlast.ScalarExpr{
				Kind:  sqlast.ScalarBinary,
				Op:    sqlast.ScalarAdd,
				Left:  conflictTestScalarPath(0, "value", 31),
				Right: conflictTestScalarPath(1, "delta", 39),
				Pos:   36,
			},
			Pos: 23,
		}}, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	current := []byte(`{"value":1}`)
	excluded := []byte(`{"delta":2}`)
	excludedLarge := []byte(`{"delta":200}`)
	exec := Exec{Stats: ExecStats{RowsTotal: 41, IndexBounded: true}}
	exec.Result.RowCount = 7
	if _, err := statement.EvaluateConflictUpdateExpressions(
		&exec, current, excluded, nil, len(current)-1,
	); err == nil || !strings.Contains(err.Error(), "current document") {
		t.Fatalf("oversize current error = %v", err)
	}
	if exec.Stats != (ExecStats{}) || exec.Result.RowCount != 0 {
		t.Fatalf("oversize current retained state: stats=%+v rows=%d", exec.Stats, exec.Result.RowCount)
	}

	exec.Stats = ExecStats{RowsTotal: 73, IndexBounded: true}
	exec.Result.RowCount = 9
	if _, err := statement.EvaluateConflictUpdateExpressions(
		&exec, current, excludedLarge, nil, len(current),
	); err == nil || !strings.Contains(err.Error(), "EXCLUDED document") {
		t.Fatalf("oversize EXCLUDED error = %v", err)
	}
	if exec.Stats != (ExecStats{}) || exec.Result.RowCount != 0 {
		t.Fatalf("oversize EXCLUDED retained state: stats=%+v rows=%d", exec.Stats, exec.Result.RowCount)
	}

	if _, err := statement.EvaluateConflictUpdateExpressions(
		&exec, []byte("1,2"), []byte("3"), nil, len(current),
	); err == nil || !strings.Contains(err.Error(), "invalid ON CONFLICT current document") {
		t.Fatalf("current namespace injection error = %v", err)
	}
	if _, err := statement.EvaluateConflictUpdateExpressions(
		&exec, []byte("1"), []byte("2,3"), nil, len(current),
	); err == nil || !strings.Contains(err.Error(), "invalid ON CONFLICT EXCLUDED document") {
		t.Fatalf("EXCLUDED namespace injection error = %v", err)
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	exec.Stats = ExecStats{RowsTotal: 99, IndexBounded: true}
	exec.Result.RowCount = 11
	if _, err := statement.EvaluateConflictUpdateExpressions(
		&exec, current, excluded, nil, len(excluded),
	); !errors.Is(err, ErrCanceled) {
		t.Fatalf("pre-canceled conflict evaluation = %T %v, want ErrCanceled", err, err)
	}
	if exec.Stats != (ExecStats{}) || exec.Result.RowCount != 0 {
		t.Fatalf("canceled conflict evaluation retained state: stats=%+v rows=%d", exec.Stats, exec.Result.RowCount)
	}

	exec.Options.Cancel = nil
	cursor, err := statement.EvaluateConflictUpdateExpressions(
		&exec, current, excluded, nil, len(excluded),
	)
	if err != nil {
		t.Fatalf("reuse after cancellation: %v", err)
	}
	if !cursor.Next() || cursor.Cell(0).String() != "3" || cursor.Next() {
		t.Fatalf("reuse result = %v", exec.Result)
	}
	wantEnvelope := "[" + string(current) + "," + string(excluded) + "]"
	if got := string(statement.assignmentExpressions.envelope); got != wantEnvelope {
		t.Fatalf("retained envelope = %q, want %q", got, wantEnvelope)
	}
}

func TestDMLDirectConflictAssignmentsKeepEvaluatorAbsent(t *testing.T) {
	statement, err := PrepareParsedDML(
		"INSERT direct conflict assignment test",
		conflictTestStatement([]sqlast.UpdateAssignment{{
			Column: "value",
			Value: sqlast.Operand{
				Kind: sqlast.OperandExcluded, Text: "value", Pos: 42,
			},
			Pos: 34,
		}}, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.HasConflictUpdateExpressions() ||
		statement.ConflictUpdateExpressionCount() != 0 ||
		statement.FirstConflictUpdateExpressionPosition() != -1 {
		t.Fatalf(
			"direct conflict assignment retained evaluator: has=%t count=%d pos=%d",
			statement.HasConflictUpdateExpressions(),
			statement.ConflictUpdateExpressionCount(),
			statement.FirstConflictUpdateExpressionPosition(),
		)
	}
	if err := statement.ValidateConflictUpdateExpressionBindings(nil); err != nil {
		t.Fatalf("direct conflict binding validation: %v", err)
	}
}

func TestDMLConflictExpressionEnvelopePreservesDocumentDepthLimit(t *testing.T) {
	statement, err := PrepareParsedDML(
		"INSERT deep conflict expression test",
		conflictTestStatement([]sqlast.UpdateAssignment{{
			Column: "value",
			Value:  sqlast.Operand{Kind: sqlast.OperandExpression, Pos: 31},
			Expr:   conflictTestScalarPath(0, "value", 31),
			Pos:    23,
		}}, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	current := []byte(
		`{"value":1,"deep":` +
			strings.Repeat("[", vibejson.DefaultMaxDepth-1) + "0" +
			strings.Repeat("]", vibejson.DefaultMaxDepth-1) + "}",
	)
	if err := vibejson.Validate(current); err != nil {
		t.Fatalf("max-depth test document is invalid: %v", err)
	}
	excluded := []byte(`{"value":2}`)
	var exec Exec
	cursor, err := statement.EvaluateConflictUpdateExpressions(
		&exec, current, excluded, nil, len(current),
	)
	if err != nil {
		t.Fatalf("envelope rejected an individually valid max-depth row: %v", err)
	}
	if !cursor.Next() || cursor.Cell(0).String() != "1" || cursor.Next() {
		t.Fatalf("max-depth conflict projection = %+v", exec.Result)
	}
}

func conflictTestStatement(
	assignments []sqlast.UpdateAssignment,
	params int,
) *sqlast.Statement {
	return &sqlast.Statement{
		Kind: sqlast.KindInsert,
		Insert: &sqlast.InsertStmt{
			Table: "docs",
			Rows: []sqlast.InsertRow{{
				Values: []sqlast.Operand{{
					Kind: sqlast.OperandJSON, Text: `{"id":"candidate"}`,
				}},
			}},
			OnConflictUpdate: &sqlast.InsertConflictUpdate{
				Assignments: assignments,
				Pos:         20,
				SetPos:      30,
			},
			Params: params,
			Pos:    12,
		},
	}
}

func conflictTestScalarPath(source int, key string, pos int) *sqlast.ScalarExpr {
	return &sqlast.ScalarExpr{
		Kind: sqlast.ScalarPath,
		Path: conflictTestPath(source, key, pos),
		Pos:  pos,
	}
}

func conflictTestPath(source int, key string, pos int) *sqlast.PathExpr {
	return &sqlast.PathExpr{
		Source: source,
		Segments: []sqlast.Segment{{
			Key: key,
		}},
		Pos: pos,
	}
}
