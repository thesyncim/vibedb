package query

import (
	"math"
	"testing"
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
