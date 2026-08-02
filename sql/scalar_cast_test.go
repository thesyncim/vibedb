package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestScalarCastASTTargetsNestingAndPrecedence(t *testing.T) {
	source := `SELECT CAST(a + 1 AS NUMERIC) * 2 AS n,
		CAST(CAST(flag AS TeXt) AS JSON) AS j,
		CAST(name AS BOOLEAN) AS b FROM docs`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.Columns) != 3 {
		t.Fatalf("columns = %d", len(statement.Columns))
	}
	numeric := statement.Columns[0].Scalar
	if numeric == nil || numeric.Kind != ScalarBinary || numeric.Op != ScalarMultiply ||
		numeric.Left.Kind != ScalarCast || numeric.Left.Cast != ScalarCastNumeric ||
		numeric.Left.Left.Kind != ScalarBinary || numeric.Left.Left.Op != ScalarAdd {
		t.Fatalf("numeric CAST tree = %#v", numeric)
	}
	json := statement.Columns[1].Scalar
	if json.Kind != ScalarCast || json.Cast != ScalarCastJSON ||
		json.Left.Kind != ScalarCast || json.Left.Cast != ScalarCastText {
		t.Fatalf("nested CAST tree = %#v", json)
	}
	boolean := statement.Columns[2].Scalar
	if boolean.Kind != ScalarCast || boolean.Cast != ScalarCastBoolean ||
		boolean.TargetPos != strings.Index(source, "BOOLEAN") {
		t.Fatalf("boolean CAST tree = %#v", boolean)
	}
	wantDump := "select (* cast(numeric (+ path(0:a) n1)) n2) as n " +
		"cast(json cast(text path(0:flag))) as j cast(boolean path(0:name)) as b from docs"
	if got := dumpStmt(statement); got != wantDump {
		t.Fatalf("CAST dump = %q, want %q", got, wantDump)
	}
}

func TestScalarCastUnsupportedTargetsAndFormsArePositioned0A000(t *testing.T) {
	tests := []struct {
		source string
		at     string
	}{
		{`SELECT CAST(a AS jsonb) FROM docs`, "jsonb"},
		{`SELECT CAST(a AS numeric(10, 2)) FROM docs`, "(10"},
		{`SELECT CAST(a AS double precision) FROM docs`, "double"},
		{`SELECT CAST(a AS "text") FROM docs`, `"text"`},
		{`SELECT a::text FROM docs`, "::"},
		{`SELECT '1'::numeric FROM docs`, "::"},
	}
	for _, test := range tests {
		_, err := Parse(test.source)
		var unsupported *FeatureNotSupportedError
		want := strings.Index(test.source, test.at)
		if !errors.As(err, &unsupported) || unsupported.Pos != want {
			t.Fatalf("%q error = %T %v at %d, want 0A000-class at %d",
				test.source, err, err, unsupportedPosition(unsupported), want)
		}
	}
}

func unsupportedPosition(err *FeatureNotSupportedError) int {
	if err == nil {
		return -1
	}
	return err.Pos
}

func TestScalarCastNestingBoundAndParserReuse(t *testing.T) {
	var source strings.Builder
	source.WriteString("SELECT ")
	for range maxExprDepth + 2 {
		source.WriteString("CAST(")
	}
	source.WriteString("a")
	for range maxExprDepth + 2 {
		source.WriteString(" AS TEXT)")
	}
	source.WriteString(" FROM docs")
	if _, err := Parse(source.String()); err == nil {
		t.Fatal("deep CAST nesting was accepted")
	}

	var parser Parser
	var casted, plain SelectStmt
	if err := parser.Parse(&casted, `SELECT CAST(a AS text) FROM docs`); err != nil {
		t.Fatal(err)
	}
	if err := parser.Parse(&plain, `SELECT a FROM docs`); err != nil {
		t.Fatal(err)
	}
	if plain.Columns[0].Scalar != nil {
		t.Fatalf("parser reuse retained CAST state: %#v", plain.Columns[0])
	}
}
