package query

import (
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestSQLScalarCaseSearchedSimpleNestedExactValuesAndSchema(t *testing.T) {
	segment := mustSegment(t,
		`{"id":"a","active":true,"n":"2"}`,
		`{"id":"b","active":null,"n":"7"}`,
		`{"id":"c","active":false,"n":"4"}`,
	)
	statement, err := PrepareStatement(`SELECT
		CASE WHEN active THEN CAST(n AS NUMERIC) + 1
			WHEN active IS NULL THEN 0 ELSE CAST(n AS NUMERIC) END AS score,
		CASE id WHEN 'a' THEN 'first' WHEN 'b' THEN 'second' ELSE 'other' END AS label,
		CASE WHEN id = 'z' THEN 1 END AS absent,
		CASE WHEN active THEN CASE id WHEN 'a' THEN CAST(n AS NUMERIC) * 2 ELSE 9 END ELSE 0 END AS nested
		FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	schema := statement.AppendSchema(nil)
	if len(schema) != 4 || schema[0].Type != TypeNumber || schema[0].Representation != OutputSQLNumber ||
		schema[1].Type != TypeString || schema[1].Representation != OutputSQLText ||
		schema[2].Type != TypeNumber || schema[3].Type != TypeNumber {
		t.Fatalf("CASE schema = %+v", schema)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := [][4]string{
		{`3`, `"first"`, `null`, `4`},
		{`0`, `"second"`, `null`, `0`},
		{`4`, `"other"`, `null`, `0`},
	}
	for row := range want {
		if !cursor.Next() {
			t.Fatalf("missing CASE row %d", row)
		}
		for column := range want[row] {
			if got := string(cursor.Cell(column).JSON()); got != want[row][column] {
				t.Fatalf("row %d column %d = %s, want %s", row, column, got, want[row][column])
			}
		}
	}
	if cursor.Next() {
		t.Fatal("unexpected CASE row")
	}
}

func TestSQLScalarCasePathComparisonsMatchWhereDomainsAndErrors(t *testing.T) {
	segment := mustSegment(t,
		`{"id":"equal","a":1,"b":1}`,
		`{"id":"less","a":1,"b":2}`,
		`{"id":"null","a":null,"b":2}`,
		`{"id":"wide","a":9007199254740993,"b":9007199254740993.0}`,
	)
	statement, err := PrepareStatement(`SELECT id,
		CASE WHEN a < b THEN 'less' WHEN a = b THEN 'equal' ELSE 'unknown' END
		FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := [][2]string{
		{`"equal"`, `"equal"`},
		{`"less"`, `"less"`},
		{`"null"`, `"unknown"`},
		{`"wide"`, `"equal"`},
	}
	for row := range want {
		if !cursor.Next() {
			t.Fatalf("missing CASE path-comparison row %d", row)
		}
		for column := range want[row] {
			if got := string(cursor.Cell(column).JSON()); got != want[row][column] {
				t.Fatalf("row %d column %d = %s, want %s", row, column, got, want[row][column])
			}
		}
	}
	if cursor.Next() {
		t.Fatal("unexpected CASE path-comparison row")
	}

	filtered, err := PrepareStatement(`SELECT id FROM docs
		WHERE CASE WHEN NOT (a != b) THEN TRUE ELSE FALSE END = TRUE ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = filtered.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{`"equal"`, `"wide"`} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != id {
			t.Fatalf("CASE-in-WHERE row != %s", id)
		}
	}
	if cursor.Next() {
		t.Fatal("CASE-in-WHERE retained a false or UNKNOWN row")
	}

	const source = `SELECT CASE WHEN a != b THEN 1 ELSE 0 END FROM docs`
	mismatch, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = mismatch.RunInto(&exec, FromSegment(mustSegment(t,
		`{"a":1,"b":"1"}`,
	)), nil)
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) || undefined.Unpositioned ||
		undefined.Left != "numeric" || undefined.Operator != "<>" ||
		undefined.Right != "text" || undefined.Pos != strings.Index(source, "!=") {
		t.Fatalf("CASE path mismatch = %T %+v", err, undefined)
	}
	var positioned *sqlast.ParseError
	if !errors.As(err, &positioned) {
		t.Fatalf("direct CASE path mismatch lost ParseError position: %T %v", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed CASE path comparison published %d rows", exec.Result.RowCount)
	}
}

func TestSQLScalarCasePostgreSQLTypedCommonTypeValuesSchemaAndWarmAllocation(t *testing.T) {
	statement, err := PrepareStatement(`SELECT
		CASE WHEN TRUE THEN BOOL 't' ELSE 'off' END,
		CASE WHEN TRUE THEN 'no' ELSE BOOLEAN 't' END,
		CASE WHEN FALSE THEN NULL ELSE BOOL 'f' END,
		CASE WHEN TRUE THEN TEXT 'typed' ELSE 'plain' END,
		CASE WHEN TRUE THEN 'unknown' ELSE TEXT 'typed' END`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	schema := statement.AppendSchema(nil)
	if len(schema) != 5 {
		t.Fatalf("typed CASE schema = %+v", schema)
	}
	for i := 0; i < 3; i++ {
		if schema[i].Type != TypeBool || schema[i].Representation != OutputSQLBool {
			t.Fatalf("typed CASE boolean schema[%d] = %+v", i, schema[i])
		}
	}
	for i := 3; i < 5; i++ {
		if schema[i].Type != TypeString || schema[i].Representation != OutputSQLText {
			t.Fatalf("typed CASE text schema[%d] = %+v", i, schema[i])
		}
	}

	var exec Exec
	defer exec.Release()
	run := func() {
		cursor, runErr := statement.RunInto(&exec, Source{}, nil)
		if runErr != nil {
			panic(runErr)
		}
		if !cursor.Next() {
			panic("missing typed CASE row")
		}
		want := []string{"true", "false", "false", `"typed"`, `"unknown"`}
		for i := range want {
			if got := string(cursor.Cell(i).JSON()); got != want[i] {
				panic("wrong typed CASE value")
			}
		}
		if cursor.Next() {
			panic("extra typed CASE row")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed typed CASE execution allocated %.2f/run, want zero", allocs)
	}
}

func TestSQLScalarCasePostgreSQLTypedBooleanUnknownFailsAtPrepare(t *testing.T) {
	for _, source := range []string{
		`SELECT CASE WHEN TRUE THEN BOOL 't' ELSE 'not-bool' END`,
		`SELECT CASE WHEN FALSE THEN 'not-bool' ELSE BOOLEAN 'f' END`,
	} {
		_, err := PrepareStatement(source)
		var typed *sqlast.InvalidTextRepresentationError
		if !errors.As(err, &typed) || typed.Pos != strings.Index(source, "'not-bool'") {
			t.Fatalf("%q prepare error = %T %v, want typed boolean input error",
				source, err, err)
		}
	}
}

func TestSQLScalarSimpleCasePostgreSQLTypedSelectorCoercesUnknownWhen(t *testing.T) {
	statement, err := PrepareStatement(`SELECT
		CASE BOOL 't' WHEN 'off' THEN 0 WHEN 'yes' THEN 1 ELSE 2 END,
		CASE TEXT 'x' WHEN 'x' THEN TEXT 'matched' ELSE TEXT 'missed' END`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	defer exec.Release()
	cursor, err := statement.RunInto(&exec, Source{}, nil)
	if err != nil || !cursor.Next() {
		t.Fatalf("typed-selector simple CASE = cursor/error %v", err)
	}
	if got := string(cursor.Cell(0).JSON()); got != "1" {
		t.Fatalf("typed BOOL selector result = %s, want 1", got)
	}
	if got := string(cursor.Cell(1).JSON()); got != `"matched"` {
		t.Fatalf("typed TEXT selector result = %s, want matched", got)
	}
	if cursor.Next() {
		t.Fatal("typed-selector simple CASE returned an extra row")
	}

	_, err = PrepareStatement(
		`SELECT CASE BOOL 't' WHEN BOOL 't' THEN 1 WHEN 'not-bool' THEN 2 ELSE 0 END`,
	)
	var typed *sqlast.InvalidTextRepresentationError
	if !errors.As(err, &typed) {
		t.Fatalf("dead invalid simple CASE match = %T %v, want typed input error", err, err)
	}
}

func TestSQLScalarCaseNestedTypedResolutionSchemaValueAndOuterCast(t *testing.T) {
	statement, err := PrepareStatement(`SELECT
		CASE WHEN TRUE THEN CASE WHEN FALSE THEN BOOL 'f' ELSE 'yes' END ELSE 'off' END,
		CASE WHEN TRUE THEN CASE WHEN FALSE THEN TEXT 'x' ELSE 'inner' END ELSE 'outer' END`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	schema := statement.AppendSchema(nil)
	if len(schema) != 2 || schema[0].Type != TypeBool ||
		schema[0].Representation != OutputSQLBool ||
		schema[1].Type != TypeString || schema[1].Representation != OutputSQLText {
		t.Fatalf("nested typed CASE schema = %+v", schema)
	}
	var exec Exec
	defer exec.Release()
	cursor, err := statement.RunInto(&exec, Source{}, nil)
	if err != nil || !cursor.Next() {
		t.Fatalf("nested typed CASE run = %v", err)
	}
	if got := string(cursor.Cell(0).JSON()); got != "true" {
		t.Fatalf("nested BOOL CASE = %s, want true", got)
	}
	if got := string(cursor.Cell(1).JSON()); got != `"inner"` {
		t.Fatalf("nested TEXT CASE = %s, want inner", got)
	}

	for _, target := range []string{"NUMERIC", "JSON"} {
		source := `SELECT (CASE WHEN TRUE THEN CASE WHEN FALSE THEN BOOL 'f' ` +
			`ELSE 'yes' END ELSE 'off' END)::` + target
		_, err = PrepareStatement(source)
		var cannot *sqlast.CannotCoerceError
		if !errors.As(err, &cannot) || cannot.Source != "boolean" ||
			cannot.Target != strings.ToLower(target) {
			t.Fatalf("nested CASE ::%s = %T %v", target, err, err)
		}
	}
}

func TestSQLScalarCaseTypedParameterInferenceExecutionAndWarmAllocation(t *testing.T) {
	statement, err := PrepareStatement(`SELECT
		CASE BOOL 't' WHEN ? THEN BOOL 't' ELSE BOOL 'f' END,
		CASE WHEN FALSE THEN BOOL 'f' ELSE ? END,
		CASE TEXT 'x' WHEN ? THEN TEXT 'matched' ELSE ? END`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	wantTypes := []ParameterType{
		ParameterTypeBool, ParameterTypeBool,
		ParameterTypeText, ParameterTypeText,
	}
	for i, want := range wantTypes {
		if got := statement.ParameterType(i); got != want {
			t.Fatalf("CASE ParameterType(%d) = %s, want %s", i, got, want)
		}
	}

	match, fallback := true, true
	textMatch, deadText := "x", "unused"
	args := []any{&match, &fallback, &textMatch, &deadText}
	var exec Exec
	defer exec.Release()
	run := func() {
		cursor, runErr := statement.RunInto(&exec, Source{}, args)
		if runErr != nil || !cursor.Next() {
			panic("typed CASE parameter execution failed")
		}
		want := []string{"true", "true", `"matched"`}
		for i := range want {
			if got := string(cursor.Cell(i).JSON()); got != want[i] {
				panic("typed CASE parameter value mismatch")
			}
		}
		if cursor.Next() {
			panic("extra typed CASE parameter row")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed typed CASE parameter execution allocated %.2f/run", allocs)
	}
}

func TestSQLScalarSimpleCaseUnknownSelectorTypedOperatorFailsAtPrepare(t *testing.T) {
	for _, source := range []string{
		`SELECT CASE NULL WHEN BOOL 't' THEN 1 ELSE 0 END`,
		`SELECT CASE ? WHEN BOOL 't' THEN 1 ELSE 0 END`,
	} {
		_, err := PrepareStatement(source)
		var undefined *sqlast.UndefinedOperatorError
		if !errors.As(err, &undefined) || undefined.Left != "text" ||
			undefined.Right != "boolean" {
			t.Fatalf("%q prepare error = %T %v", source, err, err)
		}
	}
}

func TestSQLPostgreSQLTypedCaseResultMismatchClassAndPosition(t *testing.T) {
	const source = `SELECT CASE WHEN TRUE THEN BOOL 't' ELSE TEXT 'x' END`
	_, err := PrepareStatement(source)
	var mismatch *ScalarTypeError
	if !errors.As(err, &mismatch) || !errors.Is(err, ErrScalarType) ||
		mismatch.Operation != "CASE common type" ||
		mismatch.Position() != strings.Index(source, "BOOL") {
		t.Fatalf("typed CASE mismatch = %T %v, want 42804 at BOOL", err, err)
	}

	const declared = `SELECT CASE WHEN TRUE THEN TEXT 'x' ELSE ? END`
	tree, err := sqlast.ParseStatement(declared)
	if err != nil {
		t.Fatal(err)
	}
	_, err = PrepareParsedStatementWithParameterTypes(
		declared, tree.Select, []ParameterType{ParameterTypeBool},
	)
	if !errors.As(err, &mismatch) || mismatch.Position() != strings.Index(declared, "TEXT") {
		t.Fatalf("declared CASE mismatch = %T %v, want TEXT position", err, err)
	}
}

func TestSQLPostgreSQLTypedCaseDeclaredOtherIsUndefinedOperator(t *testing.T) {
	const source = `SELECT CASE BOOL 't' WHEN ? THEN 1 ELSE 0 END`
	tree, err := sqlast.ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = PrepareParsedStatementWithParameterTypes(
		source, tree.Select, []ParameterType{ParameterTypeOther},
	)
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) || undefined.Pos != strings.Index(source, "?") ||
		undefined.Left != "boolean" || undefined.Right != "other" {
		t.Fatalf("declared-other CASE comparison = %T %v, want 42883 at parameter", err, err)
	}
}

func TestSQLScalarCaseStrictBranchAndPredicateLaziness(t *testing.T) {
	segment := mustSegment(t, `{}`)
	statement, err := PrepareStatement(`SELECT
		CASE WHEN TRUE THEN 7 WHEN CAST('bad' AS BOOLEAN) THEN 8 ELSE 1 / 0 END,
		CASE 1 WHEN 1 THEN 9 WHEN CAST('bad' AS NUMERIC) THEN 8 ELSE 1 / 0 END,
		CASE WHEN FALSE AND CAST('bad' AS BOOLEAN) THEN 1 ELSE 11 END,
		CASE WHEN TRUE OR 1 / 0 = 0 THEN 12 ELSE 1 / 0 END,
		CASE WHEN NULL THEN CAST('bad' AS NUMERIC) ELSE 13 END
		FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	exec.Options.AggregateBytes = 512
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatalf("dead CASE branch raised %T: %v", err, err)
	}
	if !cursor.Next() {
		t.Fatal("missing lazy CASE row")
	}
	want := []string{"7", "9", "11", "12", "13"}
	for i := range want {
		if got := string(cursor.Cell(i).JSON()); got != want[i] {
			t.Fatalf("lazy CASE column %d = %s, want %s", i, got, want[i])
		}
	}
	if cursor.Next() {
		t.Fatal("extra lazy CASE row")
	}
}

func TestSQLScalarCaseSelectedZeroNumeratorDivisionTerminatesExactly(t *testing.T) {
	statement, err := PrepareStatement(`SELECT
		CASE WHEN TRUE THEN 0 / 3 ELSE 1 / 0 END,
		CASE 0 WHEN 0.0 THEN 0e999 / 7 ELSE 1 / 0 END
		FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	var exec Exec
	defer exec.Release()
	cursor, err := statement.RunInto(&exec, FromSegment(mustSegment(t, `{}`)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("missing CASE zero-division row")
	}
	for column := 0; column < 2; column++ {
		if got := string(cursor.Cell(column).JSON()); got != "0" {
			t.Fatalf("zero division column %d = %s, want canonical 0", column, got)
		}
	}
	if cursor.Next() {
		t.Fatal("unexpected extra CASE zero-division row")
	}
}

func TestSQLScalarCaseStaticDeadDependenciesPreserveSemanticsWithoutPayloadWork(t *testing.T) {
	largeNumber := strings.Repeat("9", 4096)
	segment := mustSegment(t,
		`{"id":"a","payload":`+largeNumber+`}`,
		`{"id":"b","payload":`+largeNumber+`}`,
		`{"id":"c","payload":`+largeNumber+`}`,
	)

	constant, err := PrepareStatement(`SELECT CASE WHEN TRUE THEN 1 ELSE payload END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	exec.Options.IntermediateBytes = 8 << 10
	cursor, err := constant.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatalf("dead path retained payload work: %T %v", err, err)
	}
	for row := 0; row < 3; row++ {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != "1" {
			t.Fatalf("constant CASE row %d missing or wrong", row)
		}
	}
	if cursor.Next() {
		t.Fatal("constant CASE returned an extra row")
	}

	aggregate, err := PrepareStatement(`SELECT CASE WHEN TRUE THEN 1 ELSE SUM(payload) END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	exec.Options.AggregateBytes = aggregateAccBaseBytes
	cursor, err = aggregate.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatalf("dead aggregate consumed exact-decimal budget: %T %v", err, err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != "1" || cursor.Next() {
		t.Fatal("dead aggregate did not preserve single-row aggregate cardinality")
	}

	empty := mustSegment(t)
	cursor, err = aggregate.RunInto(&exec, FromSegment(empty), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != "1" || cursor.Next() {
		t.Fatal("dead aggregate did not preserve the empty-input aggregate row")
	}
}

func TestSQLScalarCaseStaticDeadDependenciesStillEnforceAggregateAndGroupRules(t *testing.T) {
	_, err := PrepareStatement(`SELECT CASE WHEN TRUE THEN 1 ELSE value END, SUM(n) FROM docs`)
	if err == nil || !strings.Contains(err.Error(), "plain path cannot be selected alongside an aggregate") {
		t.Fatalf("dead projection bypassed aggregate validation: %T %v", err, err)
	}

	_, err = PrepareStatement(`SELECT CASE WHEN TRUE THEN 1 ELSE value END FROM docs GROUP BY other`)
	if err == nil || !strings.Contains(err.Error(), `path "value", which is not a GROUP BY key`) {
		t.Fatalf("dead projection bypassed GROUP BY validation: %T %v", err, err)
	}
}

func TestSQLScalarCaseSimpleStaticPrunesPathsAndUsesExactFirstMatch(t *testing.T) {
	largeNumber := strings.Repeat("9", 4096)
	segment := mustSegment(t,
		`{"candidate":2,"payload":`+largeNumber+`}`,
		`{"candidate":1,"payload":`+largeNumber+`}`,
		`{"candidate":0,"payload":`+largeNumber+`}`,
	)
	statement, err := PrepareStatement(`SELECT CASE 1
		WHEN 0 THEN payload
		WHEN 1.0 THEN 7
		WHEN candidate THEN payload
		WHEN 1 THEN 1 / 0
		ELSE payload END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	runtime := statement.scalarStatement()
	if runtime == nil || len(runtime.cases) != 1 || runtime.cases[0].armCount != 1 {
		t.Fatalf("simple CASE executable arms = %+v", runtime)
	}
	if len(runtime.deps) != 1 || !runtime.cardinality {
		t.Fatalf("dead simple CASE retained executable dependencies: %+v", runtime.deps)
	}
	if len(runtime.semanticDeps) != 2 {
		t.Fatalf("simple CASE semantic dependencies = %+v", runtime.semanticDeps)
	}

	var exec Exec
	exec.Options.IntermediateBytes = 8 << 10
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatalf("pruned simple CASE retained payload/division work: %T %v", err, err)
	}
	for row := 0; row < 3; row++ {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != "7" {
			t.Fatalf("exact first-match row %d missing or wrong", row)
		}
	}
	if cursor.Next() {
		t.Fatal("simple CASE returned an extra row")
	}

	ordered, err := PrepareStatement(`SELECT CASE 1
		WHEN candidate THEN 5
		WHEN 1.0 THEN 7
		WHEN 2 THEN payload
		ELSE payload END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	orderedRuntime := ordered.scalarStatement()
	if orderedRuntime == nil || orderedRuntime.cases[0].armCount != 2 ||
		len(orderedRuntime.deps) != 1 || orderedRuntime.deps[0].spec != "candidate" {
		t.Fatalf("ordered simple CASE executable shape = %+v", orderedRuntime)
	}
	for _, test := range []struct {
		doc  string
		want string
	}{
		{`{"candidate":1,"payload":` + largeNumber + `}`, "5"},
		{`{"candidate":2,"payload":` + largeNumber + `}`, "7"},
	} {
		cursor, err = ordered.RunInto(&exec, FromSegment(mustSegment(t, test.doc)), nil)
		if err != nil {
			t.Fatalf("ordered simple CASE retained post-terminal payload: %v", err)
		}
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != test.want || cursor.Next() {
			t.Fatalf("ordered simple CASE = %v, want %s", cursor, test.want)
		}
	}
}

func TestSQLScalarCaseSimpleStaticPrunesAggregateWorkPreservesCardinality(t *testing.T) {
	largeNumber := strings.Repeat("9", 4096)
	segment := mustSegment(t,
		`{"payload":`+largeNumber+`}`,
		`{"payload":`+largeNumber+`}`,
		`{"payload":`+largeNumber+`}`,
	)
	statement, err := PrepareStatement(`SELECT CASE 1 WHEN 1.0 THEN 1
		WHEN 2 THEN SUM(payload) ELSE SUM(payload) END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	runtime := statement.scalarStatement()
	if runtime == nil || runtime.hasAggregate || !runtime.hasSemanticAggregate() ||
		len(runtime.deps) != 0 || runtime.cases[0].armCount != 1 {
		t.Fatalf("dead aggregate remained executable: %+v", runtime)
	}

	var exec Exec
	exec.Options.AggregateBytes = aggregateAccBaseBytes
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatalf("dead simple CASE aggregate consumed budget: %T %v", err, err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != "1" || cursor.Next() {
		t.Fatal("dead simple CASE aggregate changed aggregate cardinality")
	}

	cursor, err = statement.RunInto(&exec, FromSegment(mustSegment(t)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != "1" || cursor.Next() {
		t.Fatal("dead simple CASE aggregate lost the empty-input aggregate row")
	}
}

func TestSQLScalarCaseSimpleStaticNullFirstMatchAndTypeSemantics(t *testing.T) {
	statement, err := PrepareStatement(`SELECT CASE NULL
		WHEN NULL THEN 1 WHEN 1 THEN 1 / 0 ELSE 3 END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	runtime := statement.scalarStatement()
	if runtime == nil || runtime.cases[0].armCount != 0 || runtime.cases[0].fallback.root < 0 {
		t.Fatalf("NULL simple CASE executable shape = %+v", runtime)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(mustSegment(t, `{}`)), nil)
	if err != nil {
		t.Fatalf("NULL compared equal or evaluated a dead arm: %v", err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != "3" || cursor.Next() {
		t.Fatal("NULL simple CASE did not select ELSE")
	}

	comparison := `SELECT CASE 1 WHEN 1 THEN 1 WHEN 'x' THEN 2 END FROM docs`
	_, err = PrepareStatement(comparison)
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) || unsupported.Pos != strings.Index(comparison, "'x'") {
		t.Fatalf("pruned comparison-domain error = %T %v", err, err)
	}
	results := `SELECT CASE 1 WHEN 1 THEN 1 ELSE 'x' END FROM docs`
	_, err = PrepareStatement(results)
	if !errors.As(err, &unsupported) || unsupported.Pos != strings.Index(results, "'x'") {
		t.Fatalf("pruned result-domain error = %T %v", err, err)
	}
}

func TestSQLScalarCaseSimpleStaticDeadDependenciesStillEnforceAggregateAndGroupRules(t *testing.T) {
	_, err := PrepareStatement(`SELECT CASE 1 WHEN 1 THEN 1 WHEN 2 THEN value END, SUM(n) FROM docs`)
	if err == nil || !strings.Contains(err.Error(), "plain path cannot be selected alongside an aggregate") {
		t.Fatalf("dead simple projection bypassed aggregate validation: %T %v", err, err)
	}

	_, err = PrepareStatement(`SELECT CASE 1 WHEN 1 THEN 1 WHEN 2 THEN value END FROM docs GROUP BY other`)
	if err == nil || !strings.Contains(err.Error(), `path "value", which is not a GROUP BY key`) {
		t.Fatalf("dead simple projection bypassed GROUP BY validation: %T %v", err, err)
	}
}

func TestSQLScalarCaseSimpleStaticWarmZeroAllocCancellationAndRecovery(t *testing.T) {
	segment := mustSegment(t, `{"payload":"not-a-number"}`)
	statement, err := PrepareStatement(`SELECT CASE 1 WHEN 1.0 THEN 7
		WHEN 2 THEN CAST(payload AS NUMERIC) ELSE 1 / 0 END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != "7" || cursor.Next() {
			t.Fatal("unexpected static simple CASE result")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed static simple CASE allocated %.2f/run", allocs)
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	if _, err := statement.RunInto(&exec, FromSegment(segment), nil); !errors.Is(err, ErrCanceled) {
		t.Fatalf("static simple CASE cancellation = %T %v", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("canceled static simple CASE published %d rows", exec.Result.RowCount)
	}
	cancel.Reset()
	if _, err := statement.RunInto(&exec, FromSegment(segment), nil); err != nil {
		t.Fatalf("static simple CASE recovery after cancellation: %v", err)
	}
}

func TestSQLScalarCaseInPredicatePositionUsesThreeValuedTruth(t *testing.T) {
	segment := mustSegment(t,
		`{"id":"a","flag":true}`,
		`{"id":"b","flag":false}`,
		`{"id":"c","flag":null}`,
		`{"id":"d"}`,
	)
	statement, err := PrepareStatement(`SELECT id FROM docs WHERE
		CASE WHEN flag THEN 1 WHEN flag IS NULL THEN NULL ELSE 0 END = 1
		ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != `"a"` || cursor.Next() {
		t.Fatal("predicate-position CASE did not retain exactly the TRUE row")
	}
}

func TestSQLScalarCaseStaticAndDynamicTypeErrorsAtomicRecovery(t *testing.T) {
	staticSource := `SELECT CASE WHEN flag THEN 'text' ELSE 1 END FROM docs`
	_, err := PrepareStatement(staticSource)
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) || unsupported.Pos != strings.Index(staticSource, "1 END") {
		t.Fatalf("static CASE error = %T %v at %d", err, err, unsupportedCasePos(unsupported))
	}

	mixedSimple := `SELECT CASE value WHEN 1 THEN 1 WHEN 'x' THEN 2 END FROM docs`
	_, err = PrepareStatement(mixedSimple)
	if !errors.As(err, &unsupported) || unsupported.Pos != strings.Index(mixedSimple, "'x'") {
		t.Fatalf("simple CASE type error = %T %v", err, err)
	}

	dynamicSource := `SELECT CASE WHEN id = 'a' THEN value ELSE 0 END FROM docs ORDER BY id`
	statement, err := PrepareStatement(dynamicSource)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	_, err = statement.RunInto(&exec, FromSegment(mustSegment(t,
		`{"id":"b","value":2}`, `{"id":"a","value":"bad"}`,
	)), nil)
	var mismatch *ScalarTypeError
	if !errors.As(err, &mismatch) || mismatch.Operation != "CASE result" ||
		mismatch.Pos != strings.Index(dynamicSource, "value") {
		t.Fatalf("dynamic CASE mismatch = %T %v", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed CASE published %d rows", exec.Result.RowCount)
	}
	cursor, err := statement.RunInto(&exec, FromSegment(mustSegment(t,
		`{"id":"a","value":3}`, `{"id":"b","value":"dead"}`,
	)), nil)
	if err != nil {
		t.Fatalf("CASE recovery/dead dynamic branch: %v", err)
	}
	for _, want := range []string{"3", "0"} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want {
			t.Fatalf("CASE recovery value != %s", want)
		}
	}
	if cursor.Next() {
		t.Fatal("CASE recovery returned extra row")
	}
}

func TestSQLScalarCaseEnforcesResolvedSimpleComparisonDomainBeforeMatch(t *testing.T) {
	const source = `SELECT CASE selector WHEN candidate THEN 1 WHEN 2 THEN 2 ELSE 0 END FROM docs`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	segment := mustSegment(t, `{"selector":"same","candidate":"same"}`)
	var exec Exec
	_, err = statement.RunInto(&exec, FromSegment(segment), nil)
	var mismatch *ScalarTypeError
	if !errors.As(err, &mismatch) || mismatch.Operation != "CASE comparison" ||
		mismatch.Left != TypeString || mismatch.Right != TypeNumber ||
		mismatch.Pos != strings.Index(source, "selector") {
		t.Fatalf("resolved simple CASE domain error = %T %+v", err, mismatch)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed simple CASE published %d rows", exec.Result.RowCount)
	}
}

func unsupportedCasePos(err *sqlast.FeatureNotSupportedError) int {
	if err == nil {
		return -1
	}
	return err.Pos
}

func TestSQLScalarCaseUnsupportedPredicateAndDerivedResolutionArePositioned(t *testing.T) {
	source := `SELECT CASE WHEN name LIKE 'a%' THEN 1 ELSE 0 END FROM docs`
	_, err := PrepareStatement(source)
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) || unsupported.Pos != strings.Index(source, "name") {
		t.Fatalf("unsupported searched predicate = %T %v", err, err)
	}

	resolved, err := PrepareStatement(`SELECT CASE WHEN d.score > 1 THEN d.score ELSE 0 END
		FROM (SELECT score FROM docs) AS d`)
	if err != nil {
		t.Fatalf("derived CASE relation path: %v", err)
	}
	resolved.Release()

	undefined := `SELECT CASE WHEN d.missing > 1 THEN 1 ELSE 0 END FROM (SELECT score FROM docs) AS d`
	_, err = PrepareStatement(undefined)
	var column *RelationColumnError
	if !errors.As(err, &column) || column.Pos != strings.Index(undefined, "missing") {
		t.Fatalf("undefined CASE relation path = %T %v at %d", err, err, relationCasePos(column))
	}
}

func relationCasePos(err *RelationColumnError) int {
	if err == nil {
		return -1
	}
	return err.Pos
}

func TestSQLScalarCasePreparedWarmZeroAllocAndBudgetRelease(t *testing.T) {
	segment := mustSegment(t, `{"flag":true,"n":7,"payload":"not-json"}`)
	statement, err := PrepareStatement(`SELECT CASE WHEN flag THEN n + 1 ELSE CAST(payload AS NUMERIC) END FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != "8" || cursor.Next() {
			t.Fatal("unexpected warmed CASE result")
		}
		if statement.nested.frame.intermediate.used != 0 {
			t.Fatalf("CASE retained %d intermediate bytes", statement.nested.frame.intermediate.used)
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed CASE allocated %.2f/run", allocs)
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	if _, err := statement.RunInto(&exec, FromSegment(segment), nil); !errors.Is(err, ErrCanceled) {
		t.Fatalf("CASE cancellation = %T %v", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("canceled CASE published %d rows", exec.Result.RowCount)
	}
	cancel.Reset()
	if _, err := statement.RunInto(&exec, FromSegment(segment), nil); err != nil {
		t.Fatalf("CASE recovery after cancellation: %v", err)
	}
}

func TestSQLScalarCaseGroupedConstantCardinalityHeapAndDurable(t *testing.T) {
	statement, err := PrepareStatement(`SELECT CASE WHEN TRUE THEN 1 ELSE 2 END
		FROM docs GROUP BY bucket ORDER BY bucket`)
	if err != nil {
		t.Fatal(err)
	}
	runtime := statement.scalarStatement()
	if runtime == nil || !runtime.cardinality || !runtime.groupedCardinality ||
		len(runtime.deps) != 1 || runtime.resultDependencyColumns() != 0 {
		t.Fatalf("grouped constant cardinality metadata = %+v", runtime)
	}

	assertRows := func(source Source, want int) {
		t.Helper()
		var exec Exec
		cursor, runErr := statement.RunInto(&exec, source, nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		for row := 0; row < want; row++ {
			if !cursor.Next() || string(cursor.Cell(0).JSON()) != "1" {
				t.Fatalf("grouped constant row %d missing or wrong", row)
			}
		}
		if cursor.Next() {
			t.Fatalf("grouped constant returned more than %d rows", want)
		}
	}

	heap := mustSegment(t,
		`{"bucket":"a","payload":"one"}`,
		`{"bucket":"b","payload":"two"}`,
		`{"bucket":"a","payload":"three"}`,
	)
	assertRows(FromSegment(heap), 2)
	assertRows(FromSegment(mustSegment(t)), 0)
	assertRows(FromFile(durableScanCorpus(t, 256)), 128)

	var exec Exec
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(heap), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if !cursor.Next() || !cursor.Next() || cursor.Next() {
			t.Fatal("warmed grouped constant cardinality changed")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed grouped constant CASE allocated %.2f/run", allocs)
	}
}

func TestSQLScalarCaseClearsBorrowedLazySlotsOnEveryBoundary(t *testing.T) {
	largeA := strings.Repeat("a", 256<<10)
	largeB := strings.Repeat("b", 256<<10)
	trueSource := mustSegment(t, `{"flag":true,"payload":"`+largeA+`","fallback":"small"}`)
	falseSource := mustSegment(t, `{"flag":false,"payload":"small","fallback":"`+largeB+`"}`)
	emptySource := mustSegment(t)
	errorSource := mustSegment(t, `{"flag":true,"payload":7,"fallback":"safe"}`)
	recoverySource := mustSegment(t, `{"flag":true,"payload":"recovered","fallback":"safe"}`)

	const source = `SELECT CASE WHEN flag THEN payload ELSE CAST(fallback AS TEXT) END FROM docs`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	runtime := statement.scalarStatement()
	if runtime == nil {
		t.Fatal("CASE scalar runtime missing")
	}
	var exec Exec

	assertSuccess := func(input Source, payloadBytes int) {
		t.Helper()
		cursor, runErr := statement.RunInto(&exec, input, nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		assertScalarCaseSlotsCleared(t, runtime)
		if !cursor.Next() || len(cursor.Cell(0).JSON()) != payloadBytes+2 || cursor.Next() {
			t.Fatalf("published CASE output was not preserved after slot clearing")
		}
	}
	assertSuccess(FromSegment(trueSource), len(largeA))
	assertSuccess(FromSegment(falseSource), len(largeB))

	cursor, err := statement.RunInto(&exec, FromSegment(emptySource), nil)
	if err != nil {
		t.Fatal(err)
	}
	assertScalarCaseSlotsCleared(t, runtime)
	if cursor.Next() {
		t.Fatal("zero-row CASE returned a row")
	}

	_, err = statement.RunInto(&exec, FromSegment(errorSource), nil)
	var mismatch *ScalarTypeError
	if !errors.As(err, &mismatch) || mismatch.Operation != "CASE result" {
		t.Fatalf("dynamic CASE error = %T %v", err, err)
	}
	assertScalarCaseSlotsCleared(t, runtime)
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed CASE published %d rows", exec.Result.RowCount)
	}
	assertSuccess(FromSegment(recoverySource), len("recovered"))

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	if _, err := statement.RunInto(&exec, FromSegment(trueSource), nil); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled CASE = %T %v", err, err)
	}
	assertScalarCaseSlotsCleared(t, runtime)
	cancel.Reset()
	exec.Options.Cancel = nil

	turn := false
	run := func() {
		turn = !turn
		input, size := FromSegment(trueSource), len(largeA)
		if !turn {
			input, size = FromSegment(falseSource), len(largeB)
		}
		cursor, runErr := statement.RunInto(&exec, input, nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if !cursor.Next() || len(cursor.Cell(0).JSON()) != size+2 || cursor.Next() {
			t.Fatal("warmed branch-switch result changed")
		}
		if scalarCaseSlotsRetainValues(runtime) {
			t.Fatal("warmed CASE retained a borrowed scalar slot")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed CASE branch switching allocated %.2f/run", allocs)
	}
}

func assertScalarCaseSlotsCleared(t testing.TB, runtime *statementScalar) {
	t.Helper()
	if scalarCaseSlotsRetainValues(runtime) {
		t.Fatal("CASE runtime retained a borrowed scalar value after execution")
	}
}

func scalarCaseSlotsRetainValues(runtime *statementScalar) bool {
	retains := func(values []statementScalarValue) bool {
		for i := range values {
			value := &values[i]
			if value.direct || value.exact || value.value.kind != kindNull ||
				value.value.bval || len(value.value.num) != 0 || value.value.isInt ||
				value.value.ival != 0 || value.value.sval != "" || len(value.value.raw) != 0 ||
				len(value.cell.raw) != 0 || value.cell.text != "" || value.cell.word != 0 ||
				value.cell.kind != TypeAny || value.cell.flag != 0 {
				return true
			}
		}
		return false
	}
	return retains(runtime.values) || retains(runtime.outputValues)
}

func BenchmarkSQLScalarCaseWarm(b *testing.B) {
	segment := mustSegment(b, `{"flag":true,"n":7,"payload":"not-json"}`)
	statement, err := PrepareStatement(`SELECT CASE WHEN flag THEN n + 1 ELSE CAST(payload AS NUMERIC) END FROM docs`)
	if err != nil {
		b.Fatal(err)
	}
	var exec Exec
	for range 2 {
		if _, err := statement.RunInto(&exec, FromSegment(segment), nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := statement.RunInto(&exec, FromSegment(segment), nil); err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzSQLScalarCaseNeverPanics(f *testing.F) {
	for _, seed := range []string{"true", "false", "0", "1", "x", `{"a":1}`, "\xff"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			t.Skip()
		}
		statement, err := PrepareStatement(`SELECT CASE WHEN ? THEN CAST(? AS TEXT)
			WHEN FALSE THEN CAST(CAST('bad' AS BOOLEAN) AS TEXT) ELSE CAST(? AS TEXT) END FROM docs`)
		if err != nil {
			t.Fatal(err)
		}
		var exec Exec
		_, _ = statement.RunInto(&exec, FromSegment(mustSegment(t, `{}`)), []any{value, value, value})
	})
}
