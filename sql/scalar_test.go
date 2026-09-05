package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestScalarExpressionASTPrecedenceParametersAndReuse(t *testing.T) {
	var parser Parser
	var compound SelectStmt
	err := parser.Parse(&compound, `SELECT -a + b * ? || 'x' AS value FROM docs WHERE a + ? >= b * 2`)
	if err != nil {
		t.Fatal(err)
	}
	if compound.Params != 2 || len(compound.Columns) != 1 || compound.Columns[0].Scalar == nil {
		t.Fatalf("scalar AST/params = %#v / %d", compound.Columns[0].Scalar, compound.Params)
	}
	root := compound.Columns[0].Scalar
	if root.Kind != ScalarBinary || root.Op != ScalarConcat ||
		root.Left.Op != ScalarAdd || root.Left.Right.Op != ScalarMultiply ||
		root.Left.Right.Right.Value.Ordinal != 0 {
		t.Fatalf("precedence tree = %#v", root)
	}
	if compound.Where == nil || compound.Where.Kind != ExprScalarCompare ||
		compound.Where.ScalarLeft.Right.Value.Ordinal != 1 {
		t.Fatalf("scalar predicate = %#v", compound.Where)
	}

	var plain SelectStmt
	err = parser.Parse(&plain, `SELECT a FROM docs WHERE a = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Columns[0].Scalar != nil || plain.Where.Kind != ExprCompare || plain.Params != 0 {
		t.Fatalf("parser reuse retained scalar state: %#v", plain)
	}
}

func TestScalarUnsupportedContextIsTypedAndUTF8Positioned(t *testing.T) {
	source := `SELECT docs.é + 1 FROM docs JOIN more ON docs.id = more.id AND docs.é + 1 = 2`
	_, err := Parse(source)
	var unsupported *FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("error = %T %v", err, err)
	}
	want := strings.LastIndex(source, "docs.é")
	if unsupported.Pos != want {
		t.Fatalf("position = %d, want UTF-8 byte %d", unsupported.Pos, want)
	}
}

func TestScalarMinusWildcardExponentAndUnaryGrammar(t *testing.T) {
	for _, source := range []string{
		`SELECT a-1 FROM docs`,
		`SELECT a -1 FROM docs`,
		`SELECT a - 1 FROM docs`,
	} {
		statement, err := Parse(source)
		if err != nil {
			t.Fatalf("%q: %v", source, err)
		}
		expr := statement.Columns[0].Scalar
		if expr == nil || expr.Op != ScalarSubtract || expr.Right.Kind != ScalarLiteral ||
			expr.Right.Value.Kind != OperandNumber || expr.Right.Value.Text != "1" {
			t.Fatalf("%q produced %#v", source, expr)
		}
	}

	statement, err := Parse(`SELECT - - -a, a*2, 1e-2 + 3E+4 FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.Columns) != 3 || statement.Columns[0].Scalar.Op != ScalarNegative ||
		statement.Columns[0].Scalar.Left.Op != ScalarNegative ||
		statement.Columns[0].Scalar.Left.Left.Op != ScalarNegative ||
		statement.Columns[1].Scalar.Op != ScalarMultiply ||
		statement.Columns[2].Scalar.Op != ScalarAdd ||
		statement.Columns[2].Scalar.Left.Value.Text != "1e-2" ||
		statement.Columns[2].Scalar.Right.Value.Text != "3E+4" {
		t.Fatalf("unary/multiply/exponent AST = %#v", statement.Columns)
	}

	wildcard, err := Parse(`SELECT * FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	if wildcard.Columns[0].Scalar != nil || wildcard.Columns[0].Path == nil ||
		len(wildcard.Columns[0].Path.Segments) != 0 {
		t.Fatalf("wildcard was parsed as multiplication: %#v", wildcard.Columns[0])
	}
	commented, err := Parse("SELECT ---a is a comment\n a FROM docs")
	if err != nil {
		t.Fatal(err)
	}
	if commented.Columns[0].Scalar != nil || commented.Columns[0].Path.Spec() != "a" {
		t.Fatalf("unspaced -- did not retain line-comment semantics: %#v", commented.Columns[0])
	}
}

func TestScalarOrderByRetainsExpression(t *testing.T) {
	for _, source := range []string{
		`SELECT a FROM docs ORDER BY a + 1`,
		`SELECT a FROM docs ORDER BY -a`,
	} {
		statement, err := Parse(source)
		if err != nil || statement.OrderBy[0].Scalar == nil || statement.OrderBy[0].Path != nil || statement.OrderBy[0].Output != 0 {
			t.Fatalf("%q statement=%+v err=%v", source, statement, err)
		}
	}
}

func TestScalarOrderByOutputAliasAndAmbiguity(t *testing.T) {
	statement, err := Parse(`SELECT a + 1 AS score, score FROM docs ORDER BY score DESC`)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.OrderBy) != 1 || statement.OrderBy[0].Path != nil ||
		statement.OrderBy[0].Output != 1 || !statement.OrderBy[0].Desc {
		t.Fatalf("computed alias ORDER BY = %#v", statement.OrderBy)
	}

	qualified, err := Parse(`SELECT a + 1 AS score FROM docs AS d ORDER BY d.score`)
	if err != nil {
		t.Fatal(err)
	}
	if qualified.OrderBy[0].Output != 0 || qualified.OrderBy[0].Path == nil {
		t.Fatalf("qualified input path was resolved as an output alias: %#v", qualified.OrderBy[0])
	}
	quoted, err := Parse(`SELECT a + 1 AS "Score" FROM docs ORDER BY "Score"`)
	if err != nil || quoted.OrderBy[0].Output != 1 {
		t.Fatalf("quoted computed alias ORDER BY = %#v, err=%v", quoted.OrderBy, err)
	}

	const duplicate = `SELECT a + 1 AS score, b + 1 AS score FROM docs ORDER BY score`
	_, err = Parse(duplicate)
	var ambiguous *AmbiguousOutputError
	if !errors.As(err, &ambiguous) || ambiguous.Name != "score" ||
		ambiguous.Pos != strings.LastIndex(duplicate, "score") {
		t.Fatalf("duplicate output alias error = %T %+v", err, err)
	}
}

func TestOrderByOutputPositionsAndBounds(t *testing.T) {
	statement, err := Parse(`SELECT a, b + 1 AS score FROM docs ORDER BY 2 DESC, 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.OrderBy) != 2 || statement.OrderBy[0].Output != 2 ||
		!statement.OrderBy[0].Desc || statement.OrderBy[1].Output != 1 ||
		statement.OrderBy[1].Desc {
		t.Fatalf("positional ORDER BY = %#v", statement.OrderBy)
	}
	quoted, err := Parse(`SELECT a, b AS "1", c AS x, d AS x FROM docs ORDER BY 1, "1"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(quoted.OrderBy) != 2 || quoted.OrderBy[0].Output != 1 ||
		quoted.OrderBy[1].Output != 0 || quoted.OrderBy[1].Path != quoted.Columns[1].Path {
		t.Fatalf("numeric/quoted alias precedence = %#v", quoted.OrderBy)
	}

	for _, source := range []string{
		`SELECT a FROM docs ORDER BY -1`,
		`SELECT a FROM docs ORDER BY 0`,
		`SELECT a FROM docs ORDER BY 2`,
		`SELECT a FROM docs ORDER BY 1.0`,
		`SELECT a FROM docs ORDER BY 999999999999999999999999999`,
	} {
		_, err := Parse(source)
		var invalid *InvalidOrderPositionError
		if !errors.As(err, &invalid) || invalid.Pos != strings.Index(source, "ORDER BY")+len("ORDER BY ") ||
			invalid.Outputs != 1 {
			t.Fatalf("%q error = %T %+v", source, err, err)
		}
	}

	for _, source := range []string{
		`UPDATE docs SET "$doc" = ? ORDER BY 1 LIMIT 1`,
		`DELETE FROM docs ORDER BY 1 LIMIT 1`,
	} {
		_, err := ParseStatement(source)
		if err == nil || !strings.Contains(err.Error(), "mutation") {
			t.Fatalf("mutation ordinal %q error = %v", source, err)
		}
	}
	for _, source := range []string{
		`SELECT * FROM docs ORDER BY 1`,
		`SELECT d.* FROM docs AS d ORDER BY 1`,
	} {
		_, err := Parse(source)
		var unsupported *FeatureNotSupportedError
		if !errors.As(err, &unsupported) || !strings.Contains(unsupported.Msg, "wildcard") {
			t.Fatalf("wildcard ordinal %q error = %T %v", source, err, err)
		}
	}
	_, err = Parse(`SELECT * FROM docs ORDER BY 0`)
	var invalidWildcard *InvalidOrderPositionError
	if !errors.As(err, &invalidWildcard) {
		t.Fatalf("invalid wildcard ordinal precedence = %T %v", err, err)
	}
	_, err = Parse(`SELECT a FROM docs ORDER BY 1 + 0`)
	var unsupportedExpression *FeatureNotSupportedError
	if !errors.As(err, &unsupportedExpression) ||
		!strings.Contains(unsupportedExpression.Msg, "must stand alone") {
		t.Fatalf("numeric ORDER expression error = %T %v", err, err)
	}
}

func TestScalarFormerRefusalsProduceExplicitAST(t *testing.T) {
	for _, source := range []string{
		`SELECT a FROM t WHERE 1 = 1`,
		`SELECT a FROM t WHERE b + 1 = 2`,
		`SELECT a FROM t WHERE b || 'x' = 'yx'`,
		`SELECT ? FROM t`,
	} {
		statement, err := Parse(source)
		if err != nil {
			t.Fatalf("%q: %v", source, err)
		}
		if source == `SELECT ? FROM t` {
			if statement.Params != 1 || statement.Columns[0].Scalar == nil ||
				statement.Columns[0].Scalar.Kind != ScalarLiteral {
				t.Fatalf("parameter projection AST = %#v", statement.Columns[0])
			}
			continue
		}
		if statement.Where == nil || statement.Where.Kind != ExprScalarCompare {
			t.Fatalf("%q predicate AST = %#v", source, statement.Where)
		}
	}

	fromless, err := Parse(`SELECT ?`)
	if err != nil {
		t.Fatalf("FROM-less scalar projection: %v", err)
	}
	if fromless.Params != 1 || len(fromless.From) != 0 ||
		len(fromless.Columns) != 1 || fromless.Columns[0].Scalar == nil {
		t.Fatalf("FROM-less scalar projection AST = %#v", fromless)
	}
}

func TestFromlessSelectAcceptsOnlySourceIndependentScalars(t *testing.T) {
	for _, source := range []string{
		`SELECT 1`,
		`SELECT TRUE AS enabled, NULL AS absent, 'x' || ? AS label`,
		`SELECT CASE WHEN ? = 1 THEN CAST('2' AS NUMERIC) ELSE -3 END AS value`,
	} {
		statement, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		if len(statement.From) != 0 {
			t.Fatalf("Parse(%q) FROM = %#v, want none", source, statement.From)
		}
		for i := range statement.Columns {
			if statement.Columns[i].Scalar == nil {
				t.Fatalf("Parse(%q) column %d = %#v, want scalar", source, i, statement.Columns[i])
			}
		}
	}

	for _, test := range []struct {
		source string
		pos    int
	}{
		{`SELECT field`, 7},
		{`SELECT *`, 7},
		{`SELECT field + 1`, 7},
		{`SELECT COUNT(*)`, 7},
		{`SELECT ROW_NUMBER() OVER ()`, 7},
		{`SELECT 1 WHERE field = 1`, 15},
	} {
		_, err := Parse(test.source)
		var unsupported *FeatureNotSupportedError
		if !errors.As(err, &unsupported) || unsupported.Pos != test.pos {
			t.Fatalf("Parse(%q) = %T %v, want positioned unsupported at %d",
				test.source, err, err, test.pos)
		}
	}
}
