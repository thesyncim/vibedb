package query

import (
	"math"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
)

func TestNumberLiteralUsesExactJSONNumberGrammar(t *testing.T) {
	var compiler compiler
	defer compiler.release()

	for _, spelling := range []Number{
		"", "+1", "01", "-01", "1.", ".1", "1e", "1e+",
		"NaN", "Infinity", " 1", "1 ",
	} {
		if _, err := compiler.numberLiteral(spelling); err == nil {
			t.Errorf("Number(%q) compiled, want JSON-number rejection", spelling)
		}
	}
	for _, spelling := range []Number{
		"0", "-0", "1", "-1", "1.25", "1e9", "1E+9", "1e-9",
	} {
		if _, err := compiler.numberLiteral(spelling); err != nil {
			t.Errorf("Number(%q): %v", spelling, err)
		}
	}
}

func TestRawNumberLiteralUsesBorrowedExactSpelling(t *testing.T) {
	var compiler compiler
	defer compiler.release()

	literal, err := compiler.makeLiteral(vibejson.RawValue{Src: []byte("9007199254740993")})
	if err != nil {
		t.Fatal(err)
	}
	if literal.kind != kindNumber || string(literal.num) != "9007199254740993" {
		t.Fatalf("literal = %#v, want exact numeric spelling", literal)
	}
	if _, err := compiler.makeLiteral(vibejson.RawValue{Src: []byte("01")}); err == nil {
		t.Fatal("invalid raw JSON number compiled")
	}
}

func TestRawNumberBindsAsRowCount(t *testing.T) {
	var statement Statement
	operand := sqlast.Operand{Kind: sqlast.OperandParam, Ordinal: 0}
	got, err := statement.count(
		operand, []any{vibejson.RawValue{Src: []byte("17")}}, "LIMIT",
	)
	if err != nil || got != 17 {
		t.Fatalf("count = %d, %v; want 17", got, err)
	}
	for _, raw := range []string{"-1", "1.5", "null", `"1"`} {
		if _, err := statement.count(
			operand, []any{vibejson.RawValue{Src: []byte(raw)}}, "LIMIT",
		); err == nil {
			t.Fatalf("count accepted %q", raw)
		}
	}
}

func TestNonFiniteFloatLiteralsAreRejected(t *testing.T) {
	var compiler compiler
	for _, value := range []any{
		math.NaN(), math.Inf(1), math.Inf(-1),
		float32(math.Inf(1)), float32(math.Inf(-1)),
	} {
		if _, err := compiler.makeLiteral(value); err == nil {
			t.Errorf("makeLiteral(%v) succeeded, want non-finite rejection", value)
		}
	}
}
