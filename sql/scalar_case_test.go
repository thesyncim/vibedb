package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestScalarCaseASTParametersCanonicalDumpAndReuse(t *testing.T) {
	source := `SELECT CASE WHEN active AND score + ? >= 10 THEN CAST(score AS TEXT)
		WHEN active IS NULL THEN 'unknown' ELSE 'inactive' END AS state,
		CASE kind WHEN 'a' THEN 1 WHEN ? THEN 2 END AS rank FROM docs`
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, source); err != nil {
		t.Fatal(err)
	}
	if statement.Params != 2 || len(statement.Columns) != 2 {
		t.Fatalf("CASE statement = %d params / %d columns", statement.Params, len(statement.Columns))
	}
	searched := statement.Columns[0].Scalar
	if searched == nil || searched.Kind != ScalarCase || searched.Left != nil ||
		len(searched.Whens) != 2 || searched.Whens[0].Predicate.Kind != ExprAnd ||
		searched.Whens[0].Result.Kind != ScalarCast || searched.Else == nil {
		t.Fatalf("searched CASE AST = %#v", searched)
	}
	simple := statement.Columns[1].Scalar
	if simple == nil || simple.Kind != ScalarCase || simple.Left == nil ||
		len(simple.Whens) != 2 || simple.Whens[0].Match.Value.Text != "a" ||
		simple.Whens[1].Match.Value.Ordinal != 1 || simple.Else != nil {
		t.Fatalf("simple CASE AST = %#v", simple)
	}
	want := "select case when (and (scalar-truth path(0:active)) " +
		"(scalarcmp >= (+ path(0:score) ?0) n10)) then cast(text path(0:score)) " +
		"when (isnull 0:active) then s\"unknown\" else s\"inactive\" end as state " +
		"case path(0:kind) when s\"a\" then n1 when ?1 then n2 end as rank from docs params=2"
	if got := dumpStmt(&statement); got != want {
		t.Fatalf("CASE dump = %q\nwant      = %q", got, want)
	}

	var plain SelectStmt
	if err := parser.Parse(&plain, `SELECT id FROM docs`); err != nil {
		t.Fatal(err)
	}
	if plain.Columns[0].Scalar != nil || plain.Params != 0 {
		t.Fatalf("parser reuse retained CASE state: %#v", plain.Columns[0])
	}
}

func TestScalarCasePostgreSQLTypedResultsCoerceUnknownStringsDuringAnalysis(t *testing.T) {
	source := `SELECT
		CASE WHEN TRUE THEN BOOL 't' ELSE 'off' END,
		CASE WHEN TRUE THEN 'yes' ELSE BOOLEAN 'f' END,
		CASE WHEN TRUE THEN TEXT 'x' ELSE 'plain' END,
		CASE WHEN TRUE THEN NULL ELSE BOOL 'f' END
	FROM docs`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.Columns) != 4 {
		t.Fatalf("typed CASE columns = %d, want 4", len(statement.Columns))
	}

	first := statement.Columns[0].Scalar
	if first == nil || first.Kind != ScalarCase || first.Else == nil ||
		first.Else.Kind != ScalarLiteral || first.Else.Value.Kind != OperandBool ||
		first.Else.Value.Bool {
		t.Fatalf("typed BOOL CASE fallback = %#v, want coerced false", first)
	}
	second := statement.Columns[1].Scalar
	if second == nil || second.Whens[0].Result.Kind != ScalarLiteral ||
		second.Whens[0].Result.Value.Kind != OperandBool ||
		!second.Whens[0].Result.Value.Bool {
		t.Fatalf("inverse typed BOOL CASE arm = %#v, want coerced true", second)
	}
	text := statement.Columns[2].Scalar
	if text == nil || text.Else == nil || text.Else.Value.Kind != OperandString ||
		text.Else.Value.Text != "plain" {
		t.Fatalf("typed TEXT CASE fallback = %#v, want unchanged text", text)
	}
	null := statement.Columns[3].Scalar
	if null == nil || null.Whens[0].Result.Kind != ScalarNull ||
		null.Else == nil || !null.Else.TypedConstant {
		t.Fatalf("typed BOOL/NULL CASE = %#v", null)
	}

	// The common-type pass is opt-in: without a typed-string result, an
	// ordinary string arm retains the parser's established OperandString shape.
	ordinary, err := Parse(`SELECT CASE WHEN TRUE THEN TRUE ELSE 'off' END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	if got := ordinary.Columns[0].Scalar.Else.Value.Kind; got != OperandString {
		t.Fatalf("ordinary CASE fallback kind = %d, want OperandString", got)
	}
}

func TestScalarCasePostgreSQLTypedBooleanRejectsInvalidUnknownEvenWhenDead(t *testing.T) {
	for _, source := range []string{
		`SELECT CASE WHEN TRUE THEN BOOL 't' ELSE 'not-bool' END FROM docs`,
		`SELECT CASE WHEN FALSE THEN 'not-bool' ELSE BOOLEAN 'f' END FROM docs`,
	} {
		_, err := Parse(source)
		var typed *InvalidTextRepresentationError
		wantPos := strings.Index(source, "'not-bool'")
		if !errors.As(err, &typed) || typed.Target != "boolean" || typed.Pos != wantPos {
			t.Fatalf("%q error = %T %v, want boolean input error at %d",
				source, err, err, wantPos)
		}
		if strings.Contains(err.Error(), "not-bool") {
			t.Fatalf("%q leaked rejected CASE input in %q", source, err)
		}
	}

	// A conflicting concrete result prevents boolean from becoming the CASE
	// common type. PostgreSQL reports the type-category conflict before it ever
	// invokes boolin for the unrelated unknown string.
	conflict := `SELECT CASE WHEN TRUE THEN BOOL 't' WHEN FALSE THEN 1 ELSE 'not-bool' END FROM docs`
	statement, err := Parse(conflict)
	if err != nil {
		t.Fatalf("concrete type conflict was preempted by unknown coercion: %v", err)
	}
	if got := statement.Columns[0].Scalar.Else.Value.Kind; got != OperandString {
		t.Fatalf("conflicting CASE fallback kind = %d, want OperandString", got)
	}
}

func TestScalarSimpleCasePostgreSQLTypedSelectorCoercesUnknownWhenOnly(t *testing.T) {
	statement, err := Parse(`SELECT CASE BOOL 't'
		WHEN 'off' THEN 0 WHEN 'yes' THEN 1 ELSE 2 END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	expr := statement.Columns[0].Scalar
	if expr == nil || expr.Kind != ScalarCase || expr.Left == nil ||
		len(expr.Whens) != 2 || expr.Whens[0].Match.Value.Kind != OperandBool ||
		expr.Whens[0].Match.Value.Bool || expr.Whens[1].Match.Value.Kind != OperandBool ||
		!expr.Whens[1].Match.Value.Bool {
		t.Fatalf("typed-selector simple CASE = %#v", expr)
	}

	for _, source := range []string{
		`SELECT CASE BOOL 't' WHEN 'not-bool' THEN 1 ELSE 0 END FROM docs`,
		`SELECT CASE BOOL 't' WHEN BOOL 't' THEN 1 WHEN 'not-bool' THEN 2 ELSE 0 END FROM docs`,
	} {
		_, err = Parse(source)
		var typed *InvalidTextRepresentationError
		if !errors.As(err, &typed) || typed.Pos != strings.Index(source, "'not-bool'") {
			t.Fatalf("%q error = %T %v, want analysis-time boolean input error",
				source, err, err)
		}
	}

	// PostgreSQL fixes an unknown simple-CASE selector as TEXT before it
	// transforms the WHEN comparisons. A later BOOL value therefore cannot
	// drive the selector backward to boolean, and text = boolean has no
	// operator catalog entry.
	inverse := `SELECT CASE 't' WHEN BOOL 't' THEN 1 ELSE 0 END FROM docs`
	_, err = Parse(inverse)
	var undefined *UndefinedOperatorError
	if !errors.As(err, &undefined) || undefined.Left != "text" ||
		undefined.Right != "boolean" || undefined.Pos != strings.Index(inverse, "BOOL") {
		t.Fatalf("inverse simple CASE error = %T %v, want text=boolean at BOOL", err, err)
	}
}

func TestScalarCasePostgreSQLNestedTypedResolutionAndOuterCastGraph(t *testing.T) {
	source := `SELECT CASE WHEN TRUE THEN
		CASE WHEN FALSE THEN BOOL 'f' ELSE 'yes' END
		ELSE 'off' END FROM docs`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	outer := statement.Columns[0].Scalar
	inner := outer.Whens[0].Result
	if outer.Else == nil || outer.Else.Kind != ScalarLiteral ||
		outer.Else.Value.Kind != OperandBool || outer.Else.Value.Bool ||
		inner == nil || inner.Kind != ScalarCase || inner.Else == nil ||
		inner.Else.Value.Kind != OperandBool || !inner.Else.Value.Bool {
		t.Fatalf("nested typed CASE coercion = %#v", outer)
	}

	for _, cast := range []string{"NUMERIC", "JSON"} {
		query := `SELECT (CASE WHEN TRUE THEN CASE WHEN FALSE THEN BOOL 'f' ` +
			`ELSE 'yes' END ELSE 'off' END)::` + cast + ` FROM docs`
		_, err = Parse(query)
		var cannot *CannotCoerceError
		if !errors.As(err, &cannot) || cannot.Source != "boolean" ||
			cannot.Target != strings.ToLower(cast) ||
			cannot.Pos != strings.LastIndex(query, cast) {
			t.Fatalf("nested CASE ::%s error = %T %v", cast, err, err)
		}
	}
}

func TestScalarSimpleCasePostgreSQLUnknownSelectorTypedOperatorResolution(t *testing.T) {
	for _, source := range []string{
		`SELECT CASE NULL WHEN BOOL 't' THEN 1 ELSE 0 END FROM docs`,
		`SELECT CASE ? WHEN BOOL 't' THEN 1 ELSE 0 END FROM docs`,
		`SELECT CASE 't' WHEN BOOL 't' THEN 1 ELSE 0 END FROM docs`,
	} {
		_, err := Parse(source)
		var undefined *UndefinedOperatorError
		if !errors.As(err, &undefined) || undefined.Left != "text" ||
			undefined.Right != "boolean" || undefined.Pos != strings.Index(source, "BOOL") {
			t.Fatalf("%q error = %T %v, want text=boolean at BOOL", source, err, err)
		}
	}

	for _, source := range []string{
		`SELECT CASE BOOL 't' WHEN ? THEN 1 ELSE 0 END FROM docs`,
		`SELECT CASE TEXT 'x' WHEN ? THEN 1 ELSE 0 END FROM docs`,
		`SELECT CASE ? WHEN TEXT 'x' THEN 1 ELSE 0 END FROM docs`,
	} {
		statement, err := Parse(source)
		if err != nil {
			t.Fatalf("%q: %v", source, err)
		}
		expr := statement.Columns[0].Scalar
		var parameter *ScalarExpr
		if expr.Left.Kind == ScalarLiteral && expr.Left.Value.Kind == OperandParam {
			parameter = expr.Left
		} else {
			parameter = expr.Whens[0].Match
		}
		if parameter == nil || parameter.Kind != ScalarLiteral ||
			parameter.Value.Kind != OperandParam {
			t.Fatalf("%q parameter AST = %#v", source, parameter)
		}
	}
}

func TestScalarCasePostgreSQLTypedCommonTypeWarmParseAllocatesZero(t *testing.T) {
	const source = `SELECT
		CASE WHEN TRUE THEN BOOL 't' ELSE 'off' END,
		CASE WHEN FALSE THEN 'yes' ELSE BOOLEAN 'f' END,
		CASE BOOL 't' WHEN 'off' THEN TEXT 'no' WHEN 'yes' THEN TEXT 'yes' END
	FROM docs`
	var parser Parser
	var statement SelectStmt
	for i := 0; i < 2; i++ {
		if err := parser.Parse(&statement, source); err != nil {
			t.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if err := parser.Parse(&statement, source); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warmed typed CASE parse allocated %.2f/run, want zero", allocs)
	}
}

func TestScalarCaseDepthItemBoundsAndUTF8Errors(t *testing.T) {
	var deep strings.Builder
	deep.WriteString("SELECT ")
	for range maxExprDepth + 2 {
		deep.WriteString("CASE WHEN TRUE THEN ")
	}
	deep.WriteString("1")
	for range maxExprDepth + 2 {
		deep.WriteString(" END")
	}
	deep.WriteString(" FROM docs")
	if _, err := Parse(deep.String()); err == nil {
		t.Fatal("deep CASE nesting was accepted")
	}

	var wide strings.Builder
	wide.WriteString("SELECT CASE ")
	for i := 0; i <= maxClauseItems; i++ {
		wide.WriteString("WHEN FALSE THEN 0 ")
	}
	wide.WriteString("END FROM docs")
	if _, err := Parse(wide.String()); err == nil || !strings.Contains(err.Error(), "at most 1024") {
		t.Fatalf("wide CASE error = %v", err)
	}

	source := `SELECT CASE WHEN é IN (1, 2) THEN 1 ELSE 0 END FROM docs`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	if got := statement.Columns[0].Scalar.Whens[0].Predicate.Pos; got != strings.Index(source, "é") {
		t.Fatalf("predicate byte position = %d, want %d", got, strings.Index(source, "é"))
	}

	for _, invalid := range []string{
		`SELECT CASE END FROM docs`,
		`SELECT CASE WHEN TRUE 1 END FROM docs`,
		`SELECT CASE WHEN TRUE THEN 1 ELSE END FROM docs`,
		`SELECT CASE WHEN TRUE THEN 1 FROM docs`,
	} {
		if _, err := Parse(invalid); err == nil {
			t.Fatalf("invalid CASE accepted: %s", invalid)
		}
	}
}

func TestScalarCaseAggregateAndGroupedDependencyValidation(t *testing.T) {
	if _, err := Parse(`SELECT CASE WHEN TRUE THEN SUM(value) ELSE 0 END FROM docs`); err != nil {
		t.Fatalf("aggregate CASE result: %v", err)
	}
	_, err := Parse(`SELECT team, CASE WHEN score > 0 THEN SUM(value) ELSE 0 END FROM docs GROUP BY team`)
	if err == nil || !strings.Contains(err.Error(), "score") {
		t.Fatalf("ungrouped CASE predicate error = %v", err)
	}
	_, err = Parse(`SELECT CASE WHEN SUM(value) > 0 THEN 1 ELSE 0 END FROM docs`)
	var unsupported *FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("aggregate searched condition error = %T %v", err, err)
	}
}

func TestScalarCaseParserCancellationIdentityAndRecovery(t *testing.T) {
	var source strings.Builder
	source.WriteString("SELECT CASE ")
	for range maxClauseItems {
		source.WriteString("WHEN FALSE THEN 0 ")
	}
	source.WriteString("ELSE 1 END FROM docs")

	want := errors.New("stop CASE parse")
	checks := 0
	var parser Parser
	parser.SetCancellationCheck(func() error {
		checks++
		if checks == 3 {
			return want
		}
		return nil
	})
	var statement SelectStmt
	if err := parser.Parse(&statement, source.String()); err != want {
		t.Fatalf("CASE cancellation = %T %v, want identity %p", err, err, want)
	}
	if checks > maxClauseItems*8 {
		t.Fatalf("CASE cancellation took %d checks", checks)
	}
	parser.SetCancellationCheck(nil)
	if err := parser.Parse(&statement, `SELECT CASE WHEN TRUE THEN 1 ELSE 0 END FROM docs`); err != nil {
		t.Fatalf("CASE parser recovery: %v", err)
	}
}

func TestScalarCaseReturningParameterIsPreciselyRefused(t *testing.T) {
	tests := []string{
		`INSERT INTO docs VALUES (?) RETURNING CASE WHEN ? THEN id ELSE 'x' END`,
		`UPDATE docs SET "$doc" = ? RETURNING CASE id WHEN ? THEN 'x' ELSE 'y' END`,
		`DELETE FROM docs RETURNING CASE WHEN CAST(? AS BOOLEAN) THEN id ELSE 'x' END`,
	}
	var parser Parser
	var statement Statement
	for _, source := range tests {
		err := parser.ParseStatement(&statement, source)
		var unsupported *FeatureNotSupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("RETURNING parameter %q = %T %v", source, err, err)
		}
		want := strings.LastIndex(source, "?")
		if unsupported.Pos != want || !strings.Contains(unsupported.Msg, "distinct bind frame") {
			t.Fatalf("RETURNING parameter %q = position/message %d/%q, want %d/bind-frame refusal",
				source, unsupported.Pos, unsupported.Msg, want)
		}
		if statement != (Statement{}) {
			t.Fatalf("rejected RETURNING retained partial AST: %+v", statement)
		}
	}
	if err := parser.ParseStatement(
		&statement,
		`INSERT INTO docs VALUES (?) RETURNING CASE WHEN TRUE THEN id ELSE 'x' END`,
	); err != nil {
		t.Fatalf("parameter-free CASE RETURNING after refusal: %v", err)
	}
}
