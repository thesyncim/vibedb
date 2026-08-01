package sql

import "testing"

// checkSetStatementInvariantsScoped pins the boundary a lowerer relies on:
// Set is not optional decoration around an otherwise executable SelectStmt.
// The outward ordinary fields are a shallow schema mirror only; Root is the
// complete executable syntax and owns every placeholder range.
func checkSetStatementInvariantsScoped(
	t *testing.T,
	statement *SelectStmt,
	outer *LateralSpec,
) {
	t.Helper()
	expression := statement.Set
	if expression == nil || expression.Root == nil || expression.First == nil {
		t.Fatalf("incomplete set-expression sidecar: %+v", expression)
	}
	if expression.Root.First != expression.First {
		t.Fatal("set root lost stable syntactic first operand identity")
	}
	if expression.First.Set != nil {
		t.Fatal("set first operand recursively carries the enclosing sidecar")
	}
	if expression.Root.ParamBase != 0 {
		t.Fatalf("set root ParamBase = %d, want 0", expression.Root.ParamBase)
	}
	if statement.Params != expression.Params {
		t.Fatalf("outward Params = %d, set Params = %d", statement.Params, expression.Params)
	}
	if expression.ArityDeferred != expression.Root.ArityDeferred {
		t.Fatalf("set arity deferred = %v, root = %v",
			expression.ArityDeferred, expression.Root.ArityDeferred)
	}
	if len(expression.Outputs) != len(expression.First.Columns) {
		t.Fatalf("set has %d output names for %d first-operand columns",
			len(expression.Outputs), len(expression.First.Columns))
	}
	for i := range expression.Outputs {
		output := &expression.Outputs[i]
		column := &expression.First.Columns[i]
		if output.Pos != column.Pos {
			t.Fatalf("output[%d] position = %d, first column = %d", i, output.Pos, column.Pos)
		}
		if column.Alias != "" && (output.Name != column.Alias || output.Deferred) {
			t.Fatalf("output[%d] = %+v, want exact alias %q", i, output, column.Alias)
		}
	}
	checkSetFirstMirror(t, statement, expression.First)

	end := checkSetExprInvariants(t, expression.Root, 0, outer)
	end = checkSetTailInvariants(t, expression.Tail, end, expression.Outputs)
	if end != expression.Params {
		t.Fatalf("set reports %d placeholders and owns range [0,%d)", expression.Params, end)
	}
}

func checkSetFirstMirror(t *testing.T, statement, first *SelectStmt) {
	t.Helper()
	if statement.With != first.With || statement.Distinct != first.Distinct ||
		statement.Where != first.Where || statement.Having != first.Having ||
		statement.Limit != first.Limit || statement.Offset != first.Offset {
		t.Fatal("outward SelectStmt is not a shallow first-operand metadata mirror")
	}
	if !sameResultColumnRun(statement.Columns, first.Columns) ||
		!sameTableRefRun(statement.From, first.From) ||
		!samePathRun(statement.GroupBy, first.GroupBy) ||
		!sameOrderRun(statement.OrderBy, first.OrderBy) ||
		!sameWindowRun(statement.Windows, first.Windows) {
		t.Fatal("outward SelectStmt does not share first-operand metadata storage")
	}
}

func sameResultColumnRun(a, b []ResultColumn) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

func sameTableRefRun(a, b []TableRef) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

func samePathRun(a, b []*PathExpr) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

func sameOrderRun(a, b []OrderTerm) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

func sameWindowRun(a, b []NamedWindow) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

func checkSetExprInvariants(
	t *testing.T,
	expr *SetExpr,
	wantBase int,
	outer *LateralSpec,
) int {
	t.Helper()
	if expr == nil {
		t.Fatal("nil set-expression node")
	}
	if expr.ParamBase != wantBase || expr.Params < 0 {
		t.Fatalf("set node at %d owns [%d,%d), want base %d",
			expr.Pos, expr.ParamBase, expr.ParamBase+expr.Params, wantBase)
	}
	switch expr.Kind {
	case SetSelectExpr:
		if expr.Select == nil || expr.First != expr.Select ||
			expr.Left != nil || expr.Right != nil || expr.Child != nil || expr.Tail != nil {
			t.Fatalf("invalid set leaf at %d: %+v", expr.Pos, expr)
		}
		if expr.Select.Set != nil {
			t.Fatal("a set leaf is not one ordinary SELECT")
		}
		if expr.Select.ParamBase != wantBase || expr.Select.Params != expr.Params {
			t.Fatalf("leaf range [%d,%d) disagrees with SELECT [%d,%d)",
				expr.ParamBase, expr.ParamBase+expr.Params,
				expr.Select.ParamBase, expr.Select.ParamBase+expr.Select.Params)
		}
		columns, deferred := setSelectArity(expr.Select)
		if expr.Columns != columns || expr.ArityDeferred != deferred {
			t.Fatalf("leaf arity = (%d,%v), want (%d,%v)",
				expr.Columns, expr.ArityDeferred, columns, deferred)
		}
		checkStatementInvariantsScoped(t, expr.Select, outer)
		return wantBase + expr.Params

	case SetBinaryExpr:
		if expr.Operation > SetExceptDistinct || expr.Left == nil || expr.Right == nil ||
			expr.Select != nil || expr.Child != nil || expr.Tail != nil ||
			expr.First != expr.Left.First {
			t.Fatalf("invalid binary set node at %d: %+v", expr.Pos, expr)
		}
		leftEnd := checkSetExprInvariants(t, expr.Left, wantBase, outer)
		rightEnd := checkSetExprInvariants(t, expr.Right, leftEnd, outer)
		if expr.Params != rightEnd-wantBase {
			t.Fatalf("binary node Params = %d, children own %d", expr.Params, rightEnd-wantBase)
		}
		if !expr.Left.ArityDeferred && !expr.Right.ArityDeferred &&
			expr.Left.Columns != expr.Right.Columns {
			t.Fatalf("accepted incompatible binary widths %d and %d",
				expr.Left.Columns, expr.Right.Columns)
		}
		if expr.ArityDeferred != (expr.Left.ArityDeferred || expr.Right.ArityDeferred) {
			t.Fatal("binary deferred-arity bit disagrees with its children")
		}
		return rightEnd

	case SetGroupExpr:
		if expr.Child == nil || expr.First != expr.Child.First ||
			expr.Select != nil || expr.Left != nil || expr.Right != nil || expr.End <= expr.Pos {
			t.Fatalf("invalid authored set group at %d: %+v", expr.Pos, expr)
		}
		end := checkSetExprInvariants(t, expr.Child, wantBase, outer)
		end = checkSetTailInvariants(t, expr.Tail, end, nil)
		if expr.Params != end-wantBase {
			t.Fatalf("group Params = %d, child and tail own %d", expr.Params, end-wantBase)
		}
		if expr.Columns != expr.Child.Columns || expr.ArityDeferred != expr.Child.ArityDeferred {
			t.Fatal("group changed its child's ordinal arity")
		}
		return end

	default:
		t.Fatalf("unknown set-expression kind %d", expr.Kind)
		return wantBase
	}
}

func checkSetTailInvariants(
	t *testing.T,
	tail *SetTail,
	wantBase int,
	outputs []SetOutputColumn,
) int {
	t.Helper()
	if tail == nil {
		return wantBase
	}
	if tail.ParamBase != wantBase || tail.Params < 0 {
		t.Fatalf("set tail owns [%d,%d), want base %d",
			tail.ParamBase, tail.ParamBase+tail.Params, wantBase)
	}
	for i := range tail.OrderBy {
		term := &tail.OrderBy[i]
		if outputs != nil {
			if term.Output <= 0 || term.Output > len(outputs) ||
				term.Name != outputs[term.Output-1].Name {
				t.Fatalf("set ORDER BY[%d] does not name a first-operand output: %+v", i, term)
			}
		}
	}
	next := wantBase
	next = checkSetTailOperand(t, tail.Limit, next)
	next = checkSetTailOperand(t, tail.Offset, next)
	if tail.Params != next-wantBase {
		t.Fatalf("set tail reports %d placeholders and owns %d", tail.Params, next-wantBase)
	}
	return next
}

func checkSetTailOperand(t *testing.T, operand *Operand, ordinal int) int {
	t.Helper()
	if operand == nil || operand.Kind == OperandNumber {
		return ordinal
	}
	if operand.Kind != OperandParam || operand.Ordinal != ordinal {
		t.Fatalf("set tail operand = %+v, want placeholder %d", operand, ordinal)
	}
	return ordinal + 1
}
