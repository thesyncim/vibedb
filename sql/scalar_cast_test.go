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

func TestPostgreSQLScalarCastShorthandPrecedenceChainingAndPositions(t *testing.T) {
	shorthand := `SELECT a::text AS a, (n + 1)::numeric * 2 AS n,
		flag::bool::text AS flag, -'1'::numeric AS neg FROM docs`
	explicit := `SELECT CAST(a AS text) AS a, CAST(n + 1 AS numeric) * 2 AS n,
		CAST(CAST(flag AS bool) AS text) AS flag, -CAST('1' AS numeric) AS neg FROM docs`
	shortStatement, err := Parse(shorthand)
	if err != nil {
		t.Fatal(err)
	}
	explicitStatement, err := Parse(explicit)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dumpStmt(shortStatement), dumpStmt(explicitStatement); got != want {
		t.Fatalf("shorthand tree = %q, explicit tree = %q", got, want)
	}
	if len(shortStatement.Columns) != 4 {
		t.Fatalf("columns = %d, want 4", len(shortStatement.Columns))
	}
	chained := shortStatement.Columns[2].Scalar
	if chained == nil || chained.Kind != ScalarCast || chained.Cast != ScalarCastText ||
		chained.Left == nil || chained.Left.Kind != ScalarCast || chained.Left.Cast != ScalarCastBoolean ||
		chained.TargetPos != strings.Index(shorthand, "text AS flag") ||
		chained.Left.TargetPos != strings.Index(shorthand, "bool::text") {
		t.Fatalf("chained shorthand = %#v", chained)
	}
	negative := shortStatement.Columns[3].Scalar
	if negative == nil || negative.Kind != ScalarUnary || negative.Op != ScalarNegative ||
		negative.Left == nil || negative.Left.Kind != ScalarCast || negative.Left.Cast != ScalarCastNumeric {
		t.Fatalf("TYPECAST did not bind above UMINUS: %#v", negative)
	}
}

func TestPostgreSQLTypecastKeepsJSONOperatorBelowUnary(t *testing.T) {
	statement, err := Parse(`SELECT ("$doc"->>'n')::numeric FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	root := statement.Columns[0].Scalar
	if root == nil || root.Kind != ScalarCast || root.Cast != ScalarCastNumeric ||
		root.Left == nil || root.Left.Kind != ScalarCast || root.Left.Cast != ScalarCastText {
		t.Fatalf("parenthesized JSON extraction cast = %#v", root)
	}

	// PostgreSQL 18.6 assigns a generic operator such as ->> lower precedence
	// than unary minus. Preserve that existing boundary while adding the
	// higher-precedence TYPECAST production.
	_, err = Parse(`SELECT -"$doc"->>'n' FROM docs`)
	var unsupported *FeatureNotSupportedError
	if !errors.As(err, &unsupported) ||
		!strings.Contains(unsupported.Msg, "JSON ->> requires a stored JSON path") {
		t.Fatalf("unary/JSON precedence error = %T %v", err, err)
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
		{`SELECT a::jsonb FROM docs`, "jsonb"},
		{`SELECT a::numeric(10, 2) FROM docs`, "(10"},
		{`SELECT a::"text" FROM docs`, `"text"`},
		{`SELECT a::text[] FROM docs`, "["},
		{`SELECT a::public.text FROM docs`, "public"},
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

	source.Reset()
	source.WriteString("SELECT a")
	for range maxExprDepth + 1 {
		source.WriteString("::text")
	}
	source.WriteString(" FROM docs")
	if _, err := Parse(source.String()); err == nil {
		t.Fatal("deep :: nesting was accepted")
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
