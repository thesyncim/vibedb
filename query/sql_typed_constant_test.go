package query

import (
	"errors"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestSQLPostgreSQLTypedConstantsPrepareFoldMetadataAndValues(t *testing.T) {
	statement, err := PrepareStatement(
		`SELECT BOOL 'tr', BOOLEAN 'of', TEXT ' x ', TEXT 't'::BOOL::TEXT FROM docs`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := statement.Columns(), []string{"bool", "bool", "text", "text"}; !slices.Equal(got, want) {
		t.Fatalf("columns = %q, want %q", got, want)
	}
	schema := statement.AppendSchema(nil)
	wantTypes := []ValueType{TypeBool, TypeBool, TypeString, TypeString}
	wantReps := []OutputRepresentation{OutputSQLBool, OutputSQLBool, OutputSQLText, OutputSQLText}
	for i := range schema {
		if schema[i].Type != wantTypes[i] || schema[i].Representation != wantReps[i] {
			t.Fatalf("schema[%d] = %+v, want type=%d representation=%d",
				i, schema[i], wantTypes[i], wantReps[i])
		}
	}
	runtime := statement.nested.scalar
	if runtime == nil || len(runtime.nodes) != 4 {
		t.Fatalf("typed constants did not fold to one node each: %+v", runtime)
	}
	for i := range runtime.nodes {
		if runtime.nodes[i].kind != statementScalarLiteral ||
			runtime.nodes[i].representation == OutputJSON {
			t.Fatalf("node[%d] retained row-time conversion: %+v", i, runtime.nodes[i])
		}
	}

	segment := mustSegment(t, `{}`, `{}`)
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
		want := []string{"true", "false", `" x "`, `"true"`}
		for i := range want {
			if got := string(cursor.Cell(i).JSON()); got != want[i] {
				t.Fatalf("row %d column %d = %s, want %s", rows, i, got, want[i])
			}
		}
	}
	if rows != 2 {
		t.Fatalf("rows = %d, want source cardinality 2", rows)
	}
}

func TestSQLPostgreSQLTypedConstantsPredicateAndFromlessExecution(t *testing.T) {
	segment := mustSegment(t,
		`{"name":"amy","flag":true}`,
		`{"name":"bob","flag":false}`,
	)
	statement, err := PrepareStatement(`SELECT name FROM docs
		WHERE flag = BOOL 't' AND name BETWEEN TEXT 'a' AND TEXT 'z'`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil || !cursor.Next() {
		t.Fatalf("typed predicate = cursor/error %v", err)
	}
	if got, ok := cursor.Cell(0).Text(); !ok || got != "amy" || cursor.Next() {
		t.Fatalf("typed predicate row = %q/%v", got, ok)
	}

	fromless, err := PrepareStatement(`SELECT BOOL 'yes', TEXT 'exact'`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = fromless.RunInto(&exec, Source{}, nil)
	if err != nil || !cursor.Next() {
		t.Fatalf("FROM-less typed constants = cursor/error %v", err)
	}
	if value, ok := cursor.Cell(0).Bool(); !ok || !value {
		t.Fatalf("FROM-less BOOL = %v/%v", value, ok)
	}
	if value, ok := cursor.Cell(1).Text(); !ok || value != "exact" || cursor.Next() {
		t.Fatalf("FROM-less TEXT = %q/%v", value, ok)
	}
}

func TestSQLPostgreSQLTypedConstantsValuesExecution(t *testing.T) {
	statement, err := PrepareStatement(
		`VALUES (BOOL 't', TEXT 'x'), (BOOL 'f', TEXT 'y')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := statement.Columns(), []string{"column1", "column2"}; !slices.Equal(got, want) {
		t.Fatalf("VALUES columns = %q, want %q", got, want)
	}
	schema := statement.AppendSchema(nil)
	if len(schema) != 2 || schema[0].Type != TypeBool ||
		schema[0].Representation != OutputSQLBool || schema[1].Type != TypeString ||
		schema[1].Representation != OutputSQLText {
		t.Fatalf("VALUES schema = %+v", schema)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, Source{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := [][2]string{{"true", `"x"`}, {"false", `"y"`}}
	row := 0
	for cursor.Next() {
		if row >= len(want) || string(cursor.Cell(0).JSON()) != want[row][0] ||
			string(cursor.Cell(1).JSON()) != want[row][1] {
			t.Fatalf("VALUES row %d = %s/%s", row,
				cursor.Cell(0).JSON(), cursor.Cell(1).JSON())
		}
		row++
	}
	if row != len(want) {
		t.Fatalf("VALUES rows = %d, want %d", row, len(want))
	}
}

func TestSQLPostgreSQLTypedValuesInferParametersAndUnknownStrings(t *testing.T) {
	statement, err := PrepareStatement(
		`VALUES (BOOL 't', TEXT 'x'), (?, ?), ('off', 'plain')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	schema := statement.AppendSchema(nil)
	if len(schema) != 2 || schema[0].Type != TypeBool ||
		schema[0].Representation != OutputSQLBool || schema[1].Type != TypeString ||
		schema[1].Representation != OutputSQLText {
		t.Fatalf("inferred VALUES schema = %+v", schema)
	}

	boolean, text := "no", true
	args := []any{&boolean, &text}
	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, Source{}, args)
		if runErr != nil {
			t.Fatal(runErr)
		}
		want := [][2]string{{"true", `"x"`}, {"false", `"true"`}, {"false", `"plain"`}}
		row := 0
		for cursor.Next() {
			if row >= len(want) || string(cursor.Cell(0).JSON()) != want[row][0] ||
				string(cursor.Cell(1).JSON()) != want[row][1] {
				t.Fatalf("inferred VALUES row %d = %s/%s", row,
					cursor.Cell(0).JSON(), cursor.Cell(1).JSON())
			}
			row++
		}
		if row != len(want) {
			t.Fatalf("inferred VALUES rows = %d, want %d", row, len(want))
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed typed VALUES binding allocated %.2f/run", allocs)
	}

	badArgs := []any{"o", true}
	if _, err = statement.RunInto(&exec, Source{}, badArgs); !errors.Is(err, ErrScalarInvalidText) {
		t.Fatalf("invalid inferred BOOL parameter = %T %v, want ErrScalarInvalidText", err, err)
	}
	run()

	_, err = PrepareStatement(`VALUES (BOOL 't'), ('o')`)
	if !errors.Is(err, ErrScalarInvalidText) {
		t.Fatalf("invalid inferred BOOL literal = %T %v, want ErrScalarInvalidText", err, err)
	}
	_, err = PrepareStatement(`VALUES (TEXT 'x'), (1)`)
	if !errors.Is(err, ErrScalarType) {
		t.Fatalf("TEXT/integer common type = %T %v, want ErrScalarType", err, err)
	}
}

func TestSQLPostgreSQLTypedBooleanInvalidFailsAtPrepare(t *testing.T) {
	source := `SELECT BOOL 'not-bool' FROM docs WHERE 1 = 0`
	_, err := PrepareStatement(source)
	var typed *sqlast.InvalidTextRepresentationError
	if !errors.As(err, &typed) || typed.Pos != len("SELECT BOOL ") {
		t.Fatalf("prepare error = %T %v, want typed input error at string", err, err)
	}
}

func TestSQLPostgreSQLTypedTextOuterCastKeepsCaseLaziness(t *testing.T) {
	statement, err := PrepareStatement(
		`SELECT CASE WHEN false THEN TEXT 'o'::BOOL ELSE true END`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, Source{}, nil)
	if err != nil || !cursor.Next() {
		t.Fatalf("dead typed TEXT cast arm = cursor/error %v", err)
	}
	if value, ok := cursor.Cell(0).Bool(); !ok || !value || cursor.Next() {
		t.Fatalf("dead typed TEXT cast result = %v/%v", value, ok)
	}

	direct, err := PrepareStatement(`SELECT TEXT 'o'::BOOL`)
	if err != nil {
		t.Fatalf("ordinary outer cast failed eagerly: %v", err)
	}
	if _, err = direct.RunInto(&exec, Source{}, nil); !errors.Is(err, ErrScalarInvalidText) {
		t.Fatalf("executed invalid outer cast = %T %v, want ErrScalarInvalidText", err, err)
	}
}

func TestSQLPostgreSQLTypedConstantCastGraphFailsAtPrepare(t *testing.T) {
	for _, source := range []string{
		`SELECT BOOL 't'::NUMERIC`,
		`SELECT BOOL 't'::JSON`,
		`SELECT CASE WHEN false THEN BOOL 't'::NUMERIC ELSE 1 END`,
	} {
		_, err := PrepareStatement(source)
		var cannot *sqlast.CannotCoerceError
		if !errors.As(err, &cannot) || cannot.Source != "boolean" {
			t.Fatalf("%q prepare error = %T %v, want CannotCoerceError", source, err, err)
		}
	}
}

func TestSQLPostgreSQLTypedConstantSubqueryErrorClassesSurvivePrepare(t *testing.T) {
	invalidSource := `SELECT id FROM docs WHERE EXISTS (SELECT BOOL 'o')`
	_, err := PrepareStatement(invalidSource)
	var invalid *sqlast.InvalidTextRepresentationError
	if !errors.As(err, &invalid) || invalid.Pos != strings.LastIndex(invalidSource, "'o'") {
		t.Fatalf("nested invalid-input prepare = %T %v", err, err)
	}

	cannotSource := `SELECT id FROM docs WHERE id = (SELECT BOOL 't'::NUMERIC)`
	_, err = PrepareStatement(cannotSource)
	var cannot *sqlast.CannotCoerceError
	if !errors.As(err, &cannot) || cannot.Pos != strings.LastIndex(cannotSource, "NUMERIC") {
		t.Fatalf("nested cannot-coerce prepare = %T %v", err, err)
	}
}

func TestSQLPostgreSQLTypedConstantRHSChainsExecuteAndStayLazy(t *testing.T) {
	segment := mustSegment(t,
		`{"name":"true","flag":true}`,
		`{"name":"false","flag":false}`,
	)
	statement, err := PrepareStatement(`SELECT name FROM docs
		WHERE flag = TEXT 't'::BOOL AND name = BOOL 't'::TEXT`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil || !cursor.Next() {
		t.Fatalf("typed RHS chains = cursor/error %v", err)
	}
	if got, ok := cursor.Cell(0).Text(); !ok || got != "true" || cursor.Next() {
		t.Fatalf("typed RHS chain row = %q/%v", got, ok)
	}

	lazy, err := PrepareStatement(`SELECT name FROM docs WHERE flag = TEXT 'o'::BOOL`)
	if err != nil {
		t.Fatalf("lazy RHS prepare: %v", err)
	}
	if _, err = lazy.RunInto(&exec, FromSegment(segment), nil); !errors.Is(err, ErrScalarInvalidText) {
		t.Fatalf("live bad-input RHS = %T %v, want ErrScalarInvalidText", err, err)
	}
}

func TestSQLPostgreSQLTypedConstantLeftPredicateUsesNativeZeroCostLane(t *testing.T) {
	segment := mustSegment(t,
		`{"name":"yes","flag":true}`,
		`{"name":"no","flag":false}`,
	)
	statement, err := PrepareStatement(`SELECT name FROM docs WHERE BOOL 't' = flag`)
	if err != nil {
		t.Fatal(err)
	}
	if statement.tree.Where == nil || statement.tree.Where.Kind != sqlast.ExprCompare ||
		statement.tree.Where.Value.Kind != sqlast.OperandBool ||
		!statement.tree.Where.Value.Bool ||
		statement.nested != nil && statement.nested.scalar != nil {
		t.Fatalf("typed-left predicate retained scalar runtime: %+v / %+v",
			statement.tree.Where, statement.nested)
	}
	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil || !cursor.Next() {
			t.Fatalf("typed-left predicate = cursor/error %v", runErr)
		}
		if value, ok := cursor.Cell(0).Text(); !ok || value != "yes" || cursor.Next() {
			t.Fatalf("typed-left predicate row = %q/%v", value, ok)
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed typed-left predicate allocated %.2f/run", allocs)
	}
}

func TestSQLPostgreSQLTypedBooleanCASETruthPrunesWithoutScalarCondition(t *testing.T) {
	statement, err := PrepareStatement(
		`SELECT CASE WHEN BOOL 't' THEN TEXT 'yes' ELSE TEXT 'no' END`,
	)
	if err != nil {
		t.Fatal(err)
	}
	caseExpr := statement.tree.Columns[0].Scalar
	if caseExpr == nil || len(caseExpr.Whens) != 1 ||
		caseExpr.Whens[0].Predicate == nil ||
		caseExpr.Whens[0].Predicate.Kind != sqlast.ExprConstant {
		t.Fatalf("typed CASE truth AST = %+v", caseExpr)
	}
	for i := range statement.nested.scalar.nodes {
		node := &statement.nested.scalar.nodes[i]
		if node.representation == OutputSQLBool && node.cast == sqlast.ScalarCastBoolean {
			t.Fatalf("typed CASE truth retained row-time scalar node[%d] = %+v", i, node)
		}
	}
	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, Source{}, nil)
		if runErr != nil || !cursor.Next() {
			t.Fatalf("typed CASE truth = cursor/error %v", runErr)
		}
		if value, ok := cursor.Cell(0).Text(); !ok || value != "yes" || cursor.Next() {
			t.Fatalf("typed CASE result = %q/%v", value, ok)
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed typed CASE truth allocated %.2f/run", allocs)
	}
}

func TestSQLPostgreSQLTypedConstantsPreparedWarmZeroAlloc(t *testing.T) {
	segment := mustSegment(t, `{}`, `{}`)
	statement, err := PrepareStatement(
		`SELECT BOOL 'tr', TEXT 'x', TEXT 't'::BOOL::TEXT FROM docs`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		rows := 0
		for cursor.Next() {
			rows++
			if value, ok := cursor.Cell(0).Bool(); !ok || !value {
				t.Fatal("unexpected BOOL value")
			}
			if value, ok := cursor.Cell(1).Text(); !ok || value != "x" {
				t.Fatal("unexpected TEXT value")
			}
			if value, ok := cursor.Cell(2).Text(); !ok || value != "true" {
				t.Fatal("unexpected chained value")
			}
		}
		if rows != 2 {
			t.Fatalf("rows = %d", rows)
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed typed constants allocated %.2f/run", allocs)
	}
}
