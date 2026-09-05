package sql

import "testing"

func TestBooleanPredicateConstantRetainsStructuralNegation(t *testing.T) {
	for _, tc := range []struct {
		predicate    string
		value, known bool
	}{
		{"true", true, true}, {"false", false, true}, {"NOT true", false, true},
		{"NOT NOT false", false, true}, {"NOT (NOT true)", true, true},
		{"NULL", false, false}, {"1=0", false, false}, {"n>1", false, false},
		{"NOT (n>1)", false, false}, {"false AND n=1", false, false},
	} {
		statement, err := ParseStatement("SELECT n FROM metrics WHERE " + tc.predicate)
		if err != nil {
			t.Fatal(err)
		}
		value, known := BooleanPredicateConstant(statement.Select.Where)
		if known != tc.known || known && value != tc.value {
			t.Fatalf("%s got=(%v,%v) want=(%v,%v)", tc.predicate, value, known, tc.value, tc.known)
		}
	}
	constant := &Expr{Kind: ExprConstant, Negated: true, Value: Operand{Kind: OperandBool, Bool: true}}
	if value, known := BooleanPredicateConstant(constant); !known || value {
		t.Fatal("ignored leaf negation")
	}
	constant.Value.Kind = OperandNumber
	if _, known := BooleanPredicateConstant(constant); known {
		t.Fatal("non-Boolean constant accepted")
	}
}
