package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestUpdateAssignmentsRetainSimultaneousScalarExpressions(t *testing.T) {
	source := `UPDATE metrics SET n = n + ? * 2, s = s || '!', a = b, b = a WHERE id = ?`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Update == nil || len(statement.Update.Assignments) != 4 {
		t.Fatalf("assignments = %#v", statement.Update)
	}
	assignments := statement.Update.Assignments
	for i := range assignments {
		if assignments[i].Expr == nil || assignments[i].Value.Kind != OperandExpression {
			t.Fatalf("assignment %d did not use the expression sentinel: %#v", i, assignments[i])
		}
	}

	number := assignments[0].Expr
	if number.Kind != ScalarBinary || number.Op != ScalarAdd ||
		number.Left == nil || number.Left.Kind != ScalarPath || number.Left.Path.Spec() != "n" ||
		number.Right == nil || number.Right.Kind != ScalarBinary || number.Right.Op != ScalarMultiply ||
		number.Right.Left == nil || number.Right.Left.Kind != ScalarLiteral ||
		number.Right.Left.Value.Kind != OperandParam || number.Right.Left.Value.Ordinal != 0 ||
		number.Right.Right == nil || number.Right.Right.Kind != ScalarLiteral ||
		number.Right.Right.Value.Kind != OperandNumber || number.Right.Right.Value.Text != "2" {
		t.Fatalf("numeric expression = %#v", number)
	}
	concat := assignments[1].Expr
	if concat.Kind != ScalarBinary || concat.Op != ScalarConcat ||
		concat.Left == nil || concat.Left.Path == nil || concat.Left.Path.Spec() != "s" ||
		concat.Right == nil || concat.Right.Value.Kind != OperandString || concat.Right.Value.Text != "!" {
		t.Fatalf("concat expression = %#v", concat)
	}
	if assignments[2].Expr.Kind != ScalarPath || assignments[2].Expr.Path.Spec() != "b" ||
		assignments[3].Expr.Kind != ScalarPath || assignments[3].Expr.Path.Spec() != "a" {
		t.Fatalf("swap expressions = %#v", assignments[2:])
	}
	if statement.Params() != 2 || statement.Update.Params != 2 {
		t.Fatalf("parameter count = statement %d, update %d", statement.Params(), statement.Update.Params)
	}
	if got, want := dumpAny(statement), `update metrics set "n"=(+ path(0:n) (* ?0 n2)), "s"=(|| path(0:s) s"!"), "a"=path(0:b), "b"=path(0:a) where (cmp = 0:id ?1) params=2`; got != want {
		t.Fatalf("dump\n got %s\nwant %s", got, want)
	}
}

func TestUpdateAssignmentDirectOperandFastPath(t *testing.T) {
	statement, err := ParseStatement(
		`UPDATE t SET a = ?, b = NULL, c = 'x', d = 7, e = TRUE, f = TEXT 'folded'`,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []OperandKind{
		OperandParam, OperandNull, OperandString, OperandNumber, OperandBool, OperandString,
	}
	for i := range statement.Update.Assignments {
		assignment := statement.Update.Assignments[i]
		if assignment.Expr != nil || assignment.Value.Kind != want[i] {
			t.Fatalf("assignment %d = %#v, want direct kind %d", i, assignment, want[i])
		}
	}
	if got := statement.Update.Assignments[5].Value.Text; got != "folded" {
		t.Fatalf("folded typed constant = %q", got)
	}
}

func TestUpdateAssignmentsAcceptCastCaseAndUnaryExpressions(t *testing.T) {
	source := `UPDATE ledger SET total = CAST(-amount AS NUMERIC), label = CASE WHEN active = TRUE THEN label || '!' ELSE ? END`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	assignments := statement.Update.Assignments
	if len(assignments) != 2 || assignments[0].Expr == nil || assignments[1].Expr == nil {
		t.Fatalf("assignments = %#v", assignments)
	}
	cast := assignments[0].Expr
	if cast.Kind != ScalarCast || cast.Cast != ScalarCastNumeric || cast.Left == nil ||
		cast.Left.Kind != ScalarUnary || cast.Left.Op != ScalarNegative ||
		cast.Left.Left == nil || cast.Left.Left.Path == nil || cast.Left.Left.Path.Spec() != "amount" {
		t.Fatalf("CAST expression = %#v", cast)
	}
	conditional := assignments[1].Expr
	if conditional.Kind != ScalarCase || len(conditional.Whens) != 1 ||
		conditional.Whens[0].Predicate == nil || conditional.Whens[0].Result == nil ||
		conditional.Whens[0].Result.Kind != ScalarBinary ||
		conditional.Else == nil || conditional.Else.Kind != ScalarLiteral ||
		conditional.Else.Value.Kind != OperandParam || conditional.Else.Value.Ordinal != 0 {
		t.Fatalf("CASE expression = %#v", conditional)
	}
}

func TestUpdateAssignmentUnsupportedScalarShapesAreTypedAndPositioned(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		marker  string
		message string
	}{
		{
			name:    "aggregate",
			source:  `UPDATE t SET n = SUM(n)`,
			marker:  "SUM",
			message: "aggregate",
		},
		{
			name:    "scalar function",
			source:  `UPDATE t SET label = lower(label)`,
			marker:  "lower",
			message: "not a supported function",
		},
		{
			name:    "direct scalar subquery",
			source:  `UPDATE t SET n = (SELECT n FROM other)`,
			marker:  "(",
			message: "subqueries",
		},
		{
			name:    "searched CASE subquery",
			source:  `UPDATE t SET n = CASE WHEN EXISTS (SELECT n FROM other) THEN 1 ELSE 0 END`,
			marker:  "EXISTS",
			message: "subqueries",
		},
		{
			name:    "function inside CASE predicate",
			source:  `UPDATE t SET n = CASE WHEN lower(label) = 'x' THEN 1 ELSE 0 END`,
			marker:  "(",
			message: "not a supported function",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseStatement(test.source)
			var unsupported *FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want *FeatureNotSupportedError", err, err)
			}
			if want := strings.Index(test.source, test.marker); unsupported.Pos != want {
				t.Fatalf("position = %d, want %d", unsupported.Pos, want)
			}
			if !strings.Contains(unsupported.Msg, test.message) {
				t.Fatalf("message = %q, want %q", unsupported.Msg, test.message)
			}
		})
	}
}

func TestUpdateAssignmentScalarDepthIsBounded(t *testing.T) {
	source := `UPDATE t SET n = ` + strings.Repeat("-(", maxExprDepth+1) + `n` +
		strings.Repeat(")", maxExprDepth+1)
	_, err := ParseStatement(source)
	if err == nil || !strings.Contains(err.Error(), "nest at most 64 levels") {
		t.Fatalf("deep UPDATE expression error = %v", err)
	}
}

func TestUpdateAssignmentPositionsSurviveParserReuse(t *testing.T) {
	var parser Parser
	var statement Statement
	for _, test := range []struct {
		source   string
		column   string
		operator string
	}{
		{source: `UPDATE first SET n = n + ? WHERE id = 1`, column: "n", operator: "+"},
		{source: `UPDATE second SET label = label || '!'`, column: "label", operator: "||"},
	} {
		if err := parser.ParseStatement(&statement, test.source); err != nil {
			t.Fatal(err)
		}
		assignment := statement.Update.Assignments[0]
		targetPos := strings.Index(test.source, test.column+" =")
		if assignment.Pos != targetPos {
			t.Fatalf("assignment position = %d, want %d in %q", assignment.Pos, targetPos, test.source)
		}
		operatorPos := strings.Index(test.source, test.operator)
		if assignment.Expr == nil || assignment.Expr.Kind != ScalarBinary ||
			assignment.Expr.Pos != operatorPos {
			t.Fatalf("expression position/tree = %#v, want operator at %d in %q",
				assignment.Expr, operatorPos, test.source)
		}
		pathPos := strings.LastIndex(test.source[:operatorPos], test.column)
		if assignment.Expr.Left == nil || assignment.Expr.Left.Path == nil ||
			assignment.Expr.Left.Path.Pos != pathPos {
			t.Fatalf("left path position/tree = %#v, want %d in %q",
				assignment.Expr.Left, pathPos, test.source)
		}
	}
}
