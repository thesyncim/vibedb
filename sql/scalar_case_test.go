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
