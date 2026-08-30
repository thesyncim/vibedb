package sql

import (
	"errors"
	"testing"
)

func TestPostgreSQLTypedConstantsNormalizeJoinPathComparisons(t *testing.T) {
	tests := []struct {
		name  string
		on    string
		op    CmpOp
		kind  OperandKind
		path  string
		text  string
		value bool
	}{
		{name: "boolean right", on: `r.keep = BOOL 't'`, op: OpEq, kind: OperandBool, path: "keep", value: true},
		{name: "boolean left equal", on: `BOOL 't' = r.keep`, op: OpEq, kind: OperandBool, path: "keep", value: true},
		{name: "boolean left not equal", on: `BOOL 'f' <> r.keep`, op: OpNe, kind: OperandBool, path: "keep"},
		{name: "boolean left less", on: `BOOL 'f' < r.keep`, op: OpGt, kind: OperandBool, path: "keep"},
		{name: "boolean left less equal", on: `BOOL 'f' <= r.keep`, op: OpGe, kind: OperandBool, path: "keep"},
		{name: "boolean left greater", on: `BOOL 't' > r.keep`, op: OpLt, kind: OperandBool, path: "keep", value: true},
		{name: "boolean left greater equal", on: `BOOL 't' >= r.keep`, op: OpLe, kind: OperandBool, path: "keep", value: true},
		{name: "text left", on: `TEXT 'm' < r.label`, op: OpGt, kind: OperandString, path: "label", text: "m"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := Parse(`SELECT l.k FROM l JOIN r ON ` + tc.on)
			if err != nil {
				t.Fatal(err)
			}
			expr := statement.From[1].On.Expr
			if expr == nil || expr.Kind != ExprCompare || expr.Op != tc.op ||
				expr.ScalarLeft != nil || expr.ScalarRight != nil || expr.RightPath != nil {
				t.Fatalf("normalized ON expression = %+v, want path/value comparison %s", expr, tc.op)
			}
			if expr.Path == nil || expr.Path.Source != 1 || len(expr.Path.Segments) != 1 ||
				expr.Path.Segments[0].Key != tc.path {
				t.Fatalf("normalized path = %+v", expr.Path)
			}
			if expr.Value.Kind != tc.kind || expr.Value.Bool != tc.value || expr.Value.Text != tc.text {
				t.Fatalf("normalized value = %+v", expr.Value)
			}
		})
	}
}

func TestPostgreSQLTypedConstantJoinWarmParseIsAllocationFree(t *testing.T) {
	const source = `SELECT l.k FROM l JOIN r ON BOOL 'f' < r.keep AND l.k = r.k`
	var parser Parser
	var statement SelectStmt
	for range 2 {
		if err := parser.Parse(&statement, source); err != nil {
			t.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if err := parser.Parse(&statement, source); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warmed typed-constant JOIN parse allocated %.2f/run", allocs)
	}
}

func TestPostgreSQLTypedConstantJoinCastChainsNormalizeBothSides(t *testing.T) {
	for _, test := range []struct {
		on    string
		kind  OperandKind
		text  string
		value bool
	}{
		{on: `r.keep = TEXT 't'::BOOL`, kind: OperandBool, value: true},
		{on: `r.label = BOOL 't'::TEXT`, kind: OperandString, text: "true"},
		{on: `TEXT 't'::BOOL = r.keep`, kind: OperandBool, value: true},
		{on: `BOOL 't'::TEXT = r.label`, kind: OperandString, text: "true"},
	} {
		statement, err := Parse(`SELECT l.k FROM l JOIN r ON ` + test.on)
		if err != nil {
			t.Fatalf("ON %s: %v", test.on, err)
		}
		expr := statement.From[1].On.Expr
		if expr == nil || expr.Kind != ExprCompare || expr.RightPath != nil ||
			expr.ScalarLeft != nil || expr.ScalarRight != nil ||
			expr.Value.Kind != test.kind || expr.Value.Text != test.text ||
			expr.Value.Bool != test.value {
			t.Fatalf("ON %s normalized expression = %+v", test.on, expr)
		}
	}

	_, err := Parse(`SELECT l.k FROM l JOIN r ON r.keep = TEXT 'o'::BOOL`)
	var unsupported *FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("non-foldable JOIN typed chain = %T %v, want explicit ON residual refusal", err, err)
	}
}
