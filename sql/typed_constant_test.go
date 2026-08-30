package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestPostgreSQLTypedStringConstantsASTLabelsChainingAndPositions(t *testing.T) {
	source := `SELECT BOOL't', BOOLEAN 'of', TEXT ' x ',
		(BOOL 'yes')::TEXT, TEXT 't'::BOOL::TEXT, "bool" 'on', "text" 'q' FROM docs`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	wantAliases := []string{"bool", "bool", "text", "text", "text", "bool", "text"}
	if len(statement.Columns) != len(wantAliases) {
		t.Fatalf("columns = %d", len(statement.Columns))
	}
	for i := range statement.Columns {
		column := &statement.Columns[i]
		if column.Alias != wantAliases[i] || column.Scalar == nil {
			t.Fatalf("column[%d] = %+v, want alias %q and scalar", i, column, wantAliases[i])
		}
	}

	first := statement.Columns[0].Scalar
	if first.Kind != ScalarCast || !first.TypedConstant || first.Cast != ScalarCastBoolean ||
		first.TargetPos != strings.Index(source, "BOOL") || first.Pos != strings.Index(source, "'t'") ||
		first.Left == nil || first.Left.Kind != ScalarLiteral ||
		first.Left.Value.Kind != OperandBool || !first.Left.Value.Bool || first.Left.Value.Pos != first.Pos {
		t.Fatalf("BOOL typed constant = %#v", first)
	}
	second := statement.Columns[1].Scalar
	if second.Cast != ScalarCastBoolean || !second.TypedConstant || second.Left.Value.Bool {
		t.Fatalf("BOOLEAN typed constant = %#v", second)
	}
	third := statement.Columns[2].Scalar
	if third.Cast != ScalarCastText || !third.TypedConstant || third.Left.Value.Text != " x " {
		t.Fatalf("TEXT typed constant = %#v", third)
	}
	chained := statement.Columns[4].Scalar
	if chained.Kind != ScalarCast || chained.Cast != ScalarCastText || chained.TypedConstant ||
		chained.Left == nil || chained.Left.Kind != ScalarCast || chained.Left.Cast != ScalarCastBoolean ||
		chained.Left.TypedConstant || chained.Left.Left == nil || !chained.Left.Left.TypedConstant {
		t.Fatalf("typed constant cast chain = %#v", chained)
	}
}

func TestPostgreSQLTypedStringConstantsPreserveNativeFieldPaths(t *testing.T) {
	const source = `SELECT bool, boolean, text FROM docs
		WHERE bool = TRUE AND boolean = FALSE AND text = 'x'`
	var parser Parser
	var parsed SelectStmt
	if err := parser.Parse(&parsed, source); err != nil {
		t.Fatal(err)
	}
	if parser.scalar != nil {
		t.Fatal("BOOL/TEXT field fallback allocated the cold scalar parser state")
	}
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"bool", "boolean", "text"} {
		column := &statement.Columns[i]
		if column.Scalar != nil || column.Path == nil || column.Path.Spec() != want {
			t.Fatalf("column[%d] = %+v, want native path %q", i, column, want)
		}
	}
	for _, expr := range statement.Where.Kids {
		if expr.Kind != ExprCompare || expr.Path == nil || expr.ScalarLeft != nil {
			t.Fatalf("field predicate left cold scalar state: %+v", expr)
		}
	}
}

func TestPostgreSQLTypedStringConstantsNormalizeEveryOperandLane(t *testing.T) {
	statement, err := Parse(`SELECT flag FROM docs WHERE
		flag = BOOL 't' AND name = TEXT 'x' AND
		flag IN (BOOL 'f', BOOL 't') AND name BETWEEN TEXT 'a' AND TEXT 'z'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.Where.Kids) != 4 {
		t.Fatalf("predicate children = %d", len(statement.Where.Kids))
	}
	if got := statement.Where.Kids[0].Value; got.Kind != OperandBool || !got.Bool {
		t.Fatalf("BOOL comparison operand = %+v", got)
	}
	if got := statement.Where.Kids[1].Value; got.Kind != OperandString || got.Text != "x" {
		t.Fatalf("TEXT comparison operand = %+v", got)
	}
	if got := statement.Where.Kids[2].List; len(got) != 2 || got[0].Kind != OperandBool ||
		got[0].Bool || !got[1].Bool {
		t.Fatalf("BOOL IN operands = %+v", got)
	}
	if got := statement.Where.Kids[3].List; len(got) != 2 || got[0].Text != "a" || got[1].Text != "z" {
		t.Fatalf("TEXT BETWEEN operands = %+v", got)
	}

	left, err := Parse(`SELECT flag FROM docs WHERE BOOL 't' = flag`)
	if err != nil {
		t.Fatal(err)
	}
	if left.Where.Kind != ExprCompare || left.Where.Path == nil ||
		left.Where.Path.Spec() != "flag" || left.Where.Value.Kind != OperandBool ||
		!left.Where.Value.Bool || left.Where.ScalarLeft != nil || left.Where.ScalarRight != nil {
		t.Fatalf("left typed comparison = %+v", left.Where)
	}

	parsed, err := ParseStatement(
		`INSERT INTO docs (flag, name) VALUES (BOOL 't', TEXT 'x') RETURNING BOOL 'f', TEXT 'y'`)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Insert == nil || len(parsed.Insert.Rows) != 1 ||
		parsed.Insert.Rows[0].Values[0].Kind != OperandBool ||
		parsed.Insert.Rows[0].Values[1].Kind != OperandString {
		t.Fatalf("typed INSERT values = %+v", parsed.Insert)
	}
	if parsed.Insert.Returning == nil || parsed.Insert.Returning.Columns[0].Alias != "bool" ||
		parsed.Insert.Returning.Columns[1].Alias != "text" {
		t.Fatalf("typed RETURNING = %+v", parsed.Insert.Returning)
	}

	values, err := Parse(`VALUES (BOOL 't', TEXT 'x') UNION ALL VALUES (BOOL 'f', TEXT 'y')`)
	if err != nil {
		t.Fatal(err)
	}
	if values.Set == nil || values.Set.First == nil || len(values.Set.First.Columns) != 2 ||
		values.Set.First.Columns[0].Alias != "column1" ||
		values.Set.First.Columns[1].Alias != "column2" {
		t.Fatalf("typed VALUES metadata = %+v", values.Set)
	}
}

func TestPostgreSQLTypedBooleanSearchedCASETruthIsConstant(t *testing.T) {
	const source = `SELECT CASE WHEN BOOL 't' THEN TEXT 'yes' ELSE TEXT 'no' END`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.Columns) != 1 || statement.Columns[0].Scalar == nil ||
		statement.Columns[0].Scalar.Kind != ScalarCase ||
		len(statement.Columns[0].Scalar.Whens) != 1 {
		t.Fatalf("typed CASE = %+v", statement.Columns)
	}
	predicate := statement.Columns[0].Scalar.Whens[0].Predicate
	if predicate == nil || predicate.Kind != ExprConstant ||
		predicate.Value.Kind != OperandBool || !predicate.Value.Bool ||
		predicate.ScalarLeft != nil {
		t.Fatalf("typed CASE truth = %+v, want TRUE constant", predicate)
	}

	var parser Parser
	var reused SelectStmt
	for range 2 {
		if err := parser.Parse(&reused, source); err != nil {
			t.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if err := parser.Parse(&reused, source); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warmed typed CASE truth parse allocated %.2f/run", allocs)
	}
}

func TestPostgreSQLTypedBooleanInputErrorsArePrepareTime22P02Class(t *testing.T) {
	for _, text := range []string{"", "o", "truth", "2", "yesplease", "t rue"} {
		source := `SELECT BOOL '` + text + `' FROM docs WHERE 1 = 0`
		_, err := Parse(source)
		var typed *InvalidTextRepresentationError
		var positioned *ParseError
		wantPos := strings.Index(source, "'")
		if !errors.As(err, &typed) || typed.Target != "boolean" || typed.Pos != wantPos ||
			!errors.As(err, &positioned) || positioned.Pos != wantPos {
			t.Fatalf("BOOL %q error = %T %v, want typed 22P02 class at %d", text, err, err, wantPos)
		}
		if text != "" && (strings.Contains(err.Error(), `"`+text+`"`) ||
			strings.Contains(err.Error(), `'`+text+`'`)) {
			t.Fatalf("BOOL %q leaked input in error %q", text, err)
		}
	}
}

func TestPostgreSQLTypedConstantCastGraphRejectsImpossibleEdgesAtAnalysis(t *testing.T) {
	for _, source := range []string{
		`SELECT BOOL 't'::NUMERIC`,
		`SELECT BOOL 't'::JSON`,
		`SELECT CASE WHEN false THEN BOOL 't'::NUMERIC ELSE 1 END`,
		`SELECT CAST(BOOL 't' AS JSON)`,
	} {
		_, err := Parse(source)
		var cannot *CannotCoerceError
		var positioned *ParseError
		if !errors.As(err, &cannot) || cannot.Source != "boolean" ||
			(cannot.Target != "numeric" && cannot.Target != "json") ||
			!errors.As(err, &positioned) || positioned.Pos != cannot.Pos {
			t.Fatalf("%q error = %T %v, want positioned boolean cannot-coerce", source, err, err)
		}
	}

	// Both explicit edges exist through PostgreSQL's CoerceViaIO rule. Parsing
	// validates only the graph and must not evaluate the bad BOOL input.
	if _, err := Parse(`SELECT CASE WHEN false THEN TEXT 'o'::BOOL ELSE true END`); err != nil {
		t.Fatalf("valid dead TEXT-to-BOOL edge was evaluated during analysis: %v", err)
	}
	if _, err := Parse(`SELECT BOOL 't'::TEXT::JSON`); err != nil {
		t.Fatalf("valid boolean-to-text-to-json graph was rejected: %v", err)
	}
}

func TestPostgreSQLTypedConstantSubqueryErrorsPreserveClassAndAbsolutePosition(t *testing.T) {
	invalidSource := `SELECT id FROM docs WHERE EXISTS (SELECT BOOL 'o')`
	_, err := Parse(invalidSource)
	var invalid *InvalidTextRepresentationError
	var positioned *ParseError
	wantInvalid := strings.LastIndex(invalidSource, "'o'")
	if !errors.As(err, &invalid) || invalid.Target != "boolean" ||
		invalid.Pos != wantInvalid || !errors.As(err, &positioned) ||
		positioned.Pos != wantInvalid {
		t.Fatalf("nested invalid input = %T %v, want 22P02 class at %d", err, err, wantInvalid)
	}

	cannotSource := `SELECT id FROM docs WHERE id = (SELECT BOOL 't'::NUMERIC)`
	_, err = Parse(cannotSource)
	var cannot *CannotCoerceError
	wantCannot := strings.LastIndex(cannotSource, "NUMERIC")
	if !errors.As(err, &cannot) || cannot.Source != "boolean" ||
		cannot.Target != "numeric" || cannot.Pos != wantCannot ||
		!errors.As(err, &positioned) || positioned.Pos != wantCannot {
		t.Fatalf("nested cannot-coerce = %T %v, want 42846 class at %d", err, err, wantCannot)
	}
}

func TestPostgreSQLTypedCaseUndefinedOperatorSubqueryErrorPreservesClassAndPosition(t *testing.T) {
	const source = `SELECT id FROM docs WHERE id IN (SELECT CASE NULL WHEN BOOL 't' THEN 1 ELSE 0 END)`
	_, err := Parse(source)
	var undefined *UndefinedOperatorError
	want := strings.Index(source, "BOOL")
	if !errors.As(err, &undefined) || undefined.Pos != want ||
		undefined.Left != "text" || undefined.Right != "boolean" {
		t.Fatalf(
			"nested undefined operator = %T %v, want 42883 class at %d",
			err, err, want,
		)
	}
}

func TestPostgreSQLTypedConstantRHSChainsNormalizeOrRetainScalar(t *testing.T) {
	statement, err := Parse(`SELECT flag FROM docs WHERE
		flag = TEXT 't'::BOOL AND name = BOOL 't'::TEXT`)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Where == nil || statement.Where.Kind != ExprAnd || len(statement.Where.Kids) != 2 {
		t.Fatalf("typed-chain predicate = %+v", statement.Where)
	}
	if got := statement.Where.Kids[0]; got.Kind != ExprCompare ||
		got.Value.Kind != OperandBool || !got.Value.Bool {
		t.Fatalf("TEXT-to-BOOL RHS = %+v", got)
	}
	if got := statement.Where.Kids[1]; got.Kind != ExprCompare ||
		got.Value.Kind != OperandString || got.Value.Text != "true" {
		t.Fatalf("BOOL-to-TEXT RHS = %+v", got)
	}

	lazy, err := Parse(`SELECT flag FROM docs WHERE flag = TEXT 'o'::BOOL`)
	if err != nil {
		t.Fatalf("valid bad-input outer cast failed during parse: %v", err)
	}
	if lazy.Where == nil || lazy.Where.Kind != ExprScalarCompare ||
		lazy.Where.ScalarRight == nil || lazy.Where.ScalarRight.Kind != ScalarCast ||
		lazy.Where.ScalarRight.Cast != ScalarCastBoolean {
		t.Fatalf("lazy typed RHS = %+v", lazy.Where)
	}

	correlated, err := Parse(`SELECT o.id FROM outer_docs o WHERE EXISTS (
		SELECT i.id FROM inner_docs i
		WHERE i.owner = o.id AND i.keep = TEXT 't'::BOOL)`)
	if err != nil {
		t.Fatal(err)
	}
	inner := correlated.Where.Subquery.Where
	if inner == nil || inner.Kind != ExprAnd || len(inner.Kids) != 2 ||
		inner.Kids[1].Kind != ExprCompare || inner.Kids[1].Value.Kind != OperandBool ||
		!inner.Kids[1].Value.Bool {
		t.Fatalf("correlated-lane typed RHS = %+v", inner)
	}
}

func TestPostgreSQLUnsupportedTypedConstantSpellingsArePositioned0A000(t *testing.T) {
	for _, source := range []string{
		`SELECT pg_catalog.bool 't'`,
		`SELECT bool(1) 't'`,
		`SELECT bool[] 't'`,
		`SELECT BOOL E't'`,
		`SELECT BOOL U&'t'`,
		`SELECT VARCHAR 'x'`,
		`SELECT CHAR 'x'`,
		`SELECT CHARACTER 'x'`,
		`SELECT flag FROM docs WHERE flag = pg_catalog.bool 't'`,
	} {
		_, err := Parse(source)
		var unsupported *FeatureNotSupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("%q error = %T %v, want FeatureNotSupportedError", source, err, err)
		}
	}

	// Fixed future heads remain ordinary fields unless a string literal makes
	// them unambiguously PostgreSQL typed-constant syntax.
	statement, err := Parse(`SELECT varchar, char, character FROM docs
		WHERE varchar = 'x' AND char = 'y' AND character = 'z'`)
	if err != nil {
		t.Fatalf("future type-spelled fields lost native fallback: %v", err)
	}
	for i, want := range []string{"varchar", "char", "character"} {
		if got := statement.Columns[i]; got.Scalar != nil || got.Path == nil || got.Path.Spec() != want {
			t.Fatalf("future head column[%d] = %+v, want native %q", i, got, want)
		}
	}
}

func TestPostgreSQLTypedOperandChainsFoldOrRefuseExplicitly(t *testing.T) {
	statement, err := Parse(`SELECT flag FROM docs WHERE
		flag IN (TEXT 't'::BOOL, BOOL 'f') AND
		name BETWEEN BOOL 'f'::TEXT AND BOOL 't'::TEXT`)
	if err != nil {
		t.Fatal(err)
	}
	if got := statement.Where.Kids[0].List; len(got) != 2 ||
		got[0].Kind != OperandBool || !got[0].Bool || got[1].Bool {
		t.Fatalf("typed IN chain = %+v", got)
	}
	if got := statement.Where.Kids[1].List; len(got) != 2 ||
		got[0].Text != "false" || got[1].Text != "true" {
		t.Fatalf("typed BETWEEN chain = %+v", got)
	}

	_, err = Parse(`SELECT flag FROM docs WHERE flag IN (TEXT 'o'::BOOL)`)
	var unsupported *FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("non-foldable typed IN chain = %T %v, want explicit 0A000 class", err, err)
	}
	parsed, err := ParseStatement(
		`INSERT INTO docs (flag, name) VALUES (TEXT 't'::BOOL, BOOL 't'::TEXT) RETURNING TEXT 't'::BOOL`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.Insert.Rows[0].Values; len(got) != 2 ||
		got[0].Kind != OperandBool || !got[0].Bool || got[1].Text != "true" {
		t.Fatalf("typed DML VALUES chains = %+v", got)
	}
	if parsed.Insert.Returning == nil || parsed.Insert.Returning.Columns[0].Scalar == nil {
		t.Fatalf("typed RETURNING chain = %+v", parsed.Insert.Returning)
	}
}

func TestPostgreSQLUnsupportedTypedStringTargetsArePositioned0A000(t *testing.T) {
	for _, source := range []string{
		`SELECT NUMERIC '1'`,
		`SELECT JSON '{"x":1}'`,
		`SELECT JSONB '{}'`,
		`SELECT INT2 '1'`,
		`SELECT INT4 '1'`,
		`SELECT INT8 '1'`,
	} {
		_, err := Parse(source)
		var unsupported *FeatureNotSupportedError
		if !errors.As(err, &unsupported) || unsupported.Pos != len("SELECT ") {
			t.Fatalf("%q error = %T %v", source, err, err)
		}
	}
}

func TestPostgreSQLTypedStringConstantWarmParseIsAllocationFree(t *testing.T) {
	const source = `SELECT BOOL 'tr', TEXT 'x', TEXT 't'::BOOL::TEXT FROM docs
		WHERE flag = BOOL 't' AND name BETWEEN TEXT 'a' AND TEXT 'z'`
	var parser Parser
	var statement SelectStmt
	for range 2 {
		if err := parser.Parse(&statement, source); err != nil {
			t.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if err := parser.Parse(&statement, source); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("warmed typed-constant parse allocated %.2f/run", allocs)
	}
}
