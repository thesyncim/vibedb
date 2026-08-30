package query

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestSQLSetLoweringAllSixModesStableOrderAndFirstSchema(t *testing.T) {
	database := setStatementDatabase(t)
	tests := []struct {
		syntax string
		want   []string
	}{
		{"UNION ALL", []string{"1", "2", "2", "4", "2", "2", "3", "4"}},
		{"UNION DISTINCT", []string{"1", "2", "4", "3"}},
		{"INTERSECT ALL", []string{"2", "2", "4"}},
		{"INTERSECT DISTINCT", []string{"2", "4"}},
		{"EXCEPT ALL", []string{"1"}},
		{"EXCEPT DISTINCT", []string{"1"}},
	}
	for _, test := range tests {
		t.Run(test.syntax, func(t *testing.T) {
			statement, err := PrepareStatement(
				`SELECT v AS first_name FROM set_left ` + test.syntax +
					` SELECT v AS ignored_name FROM set_right`,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if got := statement.Columns(); !reflect.DeepEqual(got, []string{"first_name"}) {
				t.Fatalf("columns = %v", got)
			}
			var execution Exec
			cursor, err := statement.RunInto(
				&execution,
				FromDatabase(database.Snapshot(), statement.Collection()), nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := setStatementCursorJSON(cursor); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("rows = %v, want %v", got, test.want)
			}
			if execution.Result.Columns[0].Header != "first_name" {
				t.Fatalf("header = %q", execution.Result.Columns[0].Header)
			}
		})
	}
}

func TestSQLSetOrdinaryPathsKeepTypedSidecarsAbsent(t *testing.T) {
	statement, err := PrepareStatement(
		`SELECT id FROM docs WHERE id = ? UNION ALL SELECT id FROM docs`,
	)
	if err != nil {
		t.Fatal(err)
	}
	runner := statement.setSQL()
	if statement.paramTypes != nil || runner == nil || runner.paramTypes != nil ||
		runner.runtime.sqlOwner != nil {
		statement.Release()
		t.Fatalf("ordinary parameterized set retained typed sidecars: statement=%v set=%v owner=%p",
			statement.paramTypes, runner.paramTypes, runner.runtime.sqlOwner)
	}
	statement.Release()

	values, err := PrepareStatement(`VALUES (1), (2)`)
	if err != nil {
		t.Fatal(err)
	}
	defer values.Release()
	valuesRunner := values.setSQL()
	if valuesRunner == nil || valuesRunner.paramTypes != nil ||
		valuesRunner.runtime.sqlOwner != nil || len(valuesRunner.values) != 1 ||
		valuesRunner.values[0].runner.parameterCasts != nil {
		t.Fatalf("ordinary VALUES retained typed sidecars: %+v", valuesRunner)
	}
}

func TestSQLSetTypedCommonTypeCompatibility(t *testing.T) {
	incompatible := []struct {
		source       string
		position     int
		unpositioned bool
	}{
		{`SELECT BOOL 't' UNION ALL SELECT TEXT 'x'`, strings.LastIndex(`SELECT BOOL 't' UNION ALL SELECT TEXT 'x'`, "TEXT"), false},
		{`SELECT TEXT 'x' UNION SELECT BOOL 't'`, strings.LastIndex(`SELECT TEXT 'x' UNION SELECT BOOL 't'`, "BOOL"), false},
		{`VALUES (BOOL 't') INTERSECT VALUES (TEXT 'x')`, -1, true},
		{`VALUES (TEXT 'x') EXCEPT SELECT BOOL 't'`, strings.LastIndex(`VALUES (TEXT 'x') EXCEPT SELECT BOOL 't'`, "BOOL"), false},
	}
	for _, test := range incompatible {
		t.Run(test.source, func(t *testing.T) {
			_, err := PrepareStatement(test.source)
			if !errors.Is(err, ErrScalarType) {
				t.Fatalf("prepare error = %T %v, want PostgreSQL datatype mismatch", err, err)
			}
			if test.unpositioned {
				var positioned interface{ Position() int }
				if errors.As(err, &positioned) {
					t.Fatalf("VALUES set mismatch unexpectedly has position %d", positioned.Position())
				}
				return
			}
			var mismatch *ScalarTypeError
			if !errors.As(err, &mismatch) {
				t.Fatalf("prepare error = %T %v, want PostgreSQL datatype mismatch", err, err)
			}
			if mismatch.Left == mismatch.Right ||
				!((mismatch.Left == TypeBool && mismatch.Right == TypeString) ||
					(mismatch.Left == TypeString && mismatch.Right == TypeBool)) {
				t.Fatalf("mismatch types = %d/%d, want bool/text", mismatch.Left, mismatch.Right)
			}
			if mismatch.Position() != test.position {
				t.Fatalf("mismatch position = %d, want %d", mismatch.Position(), test.position)
			}
		})
	}

	groupedSource := `SELECT ? UNION (SELECT ? ORDER BY 1)`
	groupedTree, err := sqlast.ParseStatement(groupedSource)
	if err != nil {
		t.Fatal(err)
	}
	_, err = PrepareParsedStatementWithParameterTypes(
		groupedSource, groupedTree.Select,
		[]ParameterType{ParameterTypeBool, ParameterTypeText},
	)
	var groupedMismatch *ScalarTypeError
	if !errors.As(err, &groupedMismatch) ||
		groupedMismatch.Position() != strings.LastIndex(groupedSource, "?") {
		t.Fatalf("grouped declared mismatch = %T %v, want right parameter position", err, err)
	}

	// Set-leaf deferral is local to that leaf. A nested derived query is its own
	// PostgreSQL query boundary and finalizes an unknown output to text; only the
	// explicit INSERT whole-document context may propagate preservation through
	// a relation graph.
	_, err = PrepareStatement(
		`SELECT BOOL 't' UNION ALL ` +
			`SELECT value FROM (SELECT 'f' AS value) AS q`,
	)
	if !errors.Is(err, ErrScalarType) {
		t.Fatalf("ordinary derived unknown crossed its query boundary: %T %v", err, err)
	}

	dynamicRight := `(VALUES (?) ORDER BY column1) UNION ALL ` +
		`SELECT n FROM set_a ORDER BY column1`
	_, err = PrepareStatement(dynamicRight)
	var dynamicMismatch *ScalarTypeError
	if !errors.As(err, &dynamicMismatch) ||
		dynamicMismatch.Position() != strings.LastIndex(dynamicRight, "n FROM") {
		t.Fatalf("dynamic right SELECT mismatch = %T %v, want right output position %d",
			err, err, strings.LastIndex(dynamicRight, "n FROM"))
	}
	for _, source := range []string{
		`(SELECT true UNION ALL SELECT 'x') UNION ALL SELECT BOOL 't'`,
		`SELECT BOOL 't' UNION ALL (SELECT false UNION ALL SELECT 'x')`,
	} {
		_, err := PrepareStatement(source)
		var invalid *ScalarInvalidTextError
		if !errors.As(err, &invalid) || !errors.Is(err, ErrScalarInvalidText) {
			t.Fatalf("heterogeneous subtree error = %T %v, want invalid bool text", err, err)
		}
	}

	compatible := []struct {
		source     string
		wantType   ValueType
		wantOutput OutputRepresentation
		wantRows   []string
	}{
		{
			`SELECT BOOL 't' AS value UNION ALL SELECT BOOLEAN 'f'`,
			TypeBool, OutputSQLBool, []string{"true", "false"},
		},
		{
			`VALUES (TEXT 'left') UNION ALL SELECT TEXT 'right'`,
			TypeString, OutputSQLText, []string{`"left"`, `"right"`},
		},
		{
			`SELECT true AS value UNION ALL VALUES (BOOL 'f')`,
			TypeBool, OutputSQLBool, []string{"true", "false"},
		},
		{
			`SELECT 't' AS value UNION ALL SELECT BOOL 'f'`,
			TypeBool, OutputSQLBool, []string{"true", "false"},
		},
		{
			`SELECT true AS value UNION SELECT 'f' UNION SELECT BOOL 't'`,
			TypeBool, OutputSQLBool, []string{"true", "false"},
		},
		{
			`SELECT NULL AS value UNION (SELECT NULL UNION SELECT BOOL 't')`,
			TypeBool, OutputSQLBool, []string{"null", "true"},
		},
		{
			`(SELECT true AS value UNION SELECT 't' ORDER BY 1) ` +
				`UNION ALL SELECT BOOL 'f'`,
			TypeBool, OutputSQLBool, []string{"true", "false"},
		},
	}
	for _, test := range compatible {
		t.Run(test.source, func(t *testing.T) {
			statement, err := PrepareStatement(test.source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			schema := statement.AppendSchema(nil)
			if len(schema) != 1 || schema[0].Type != test.wantType ||
				schema[0].Representation != test.wantOutput {
				t.Fatalf("schema = %+v, want type=%d representation=%d",
					schema, test.wantType, test.wantOutput)
			}
			var execution Exec
			cursor, err := statement.RunInto(&execution, Source{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := setStatementCursorJSON(cursor); !reflect.DeepEqual(got, test.wantRows) {
				t.Fatalf("rows = %v, want %v", got, test.wantRows)
			}
		})
	}

	// An entirely JSON-represented set retains the existing heterogeneous
	// semantics. SQL common-type checks activate only when an operand opts into
	// a SQL scalar representation.
	generic, err := PrepareStatement(`SELECT 1 AS value UNION ALL SELECT 'x'`)
	if err != nil {
		t.Fatal(err)
	}
	defer generic.Release()
	schema := generic.AppendSchema(nil)
	if len(schema) != 1 || schema[0].Type != TypeNumber ||
		schema[0].Representation != OutputJSON {
		t.Fatalf("generic set schema = %+v", schema)
	}
	var execution Exec
	cursor, err := generic.RunInto(&execution, Source{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{"1", `"x"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("generic set rows = %v, want %v", got, want)
	}
}

func TestSQLSetTypedUnknownParametersAndPairwiseBoundaries(t *testing.T) {
	statement, err := PrepareStatement(`SELECT ? AS value UNION ALL SELECT TEXT 'y'`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if got := statement.setSQL().paramRepresentation(0); got != OutputSQLText {
		t.Fatalf("parameter representation = %d, want SQL text", got)
	}
	value := "x"
	args := []any{&value}
	var execution Exec
	run := func() {
		_, err = statement.RunInto(&execution, Source{}, args)
	}
	run()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(statement.cursor(&execution.Result)),
		[]string{`"x"`, `"y"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("parameter rows = %v, want %v", got, want)
	}
	if allocations := testing.AllocsPerRun(100, run); allocations != 0 {
		t.Fatalf("warmed typed set parameter allocations = %.2f, want 0", allocations)
	}

	boolean, err := PrepareStatement(`SELECT ? AS value UNION ALL SELECT BOOL 't'`)
	if err != nil {
		t.Fatal(err)
	}
	defer boolean.Release()
	if got := boolean.setSQL().paramRepresentation(0); got != OutputSQLBool {
		t.Fatalf("boolean parameter representation = %d, want SQL bool", got)
	}
	invalid := "o"
	_, err = boolean.RunInto(&execution, Source{}, []any{&invalid})
	var invalidText *ScalarInvalidTextError
	if !errors.As(err, &invalidText) || !errors.Is(err, ErrScalarInvalidText) {
		t.Fatalf("invalid boolean parameter = %T %v", err, err)
	}

	for _, source := range []string{
		`SELECT NULL UNION SELECT NULL UNION SELECT BOOL 't'`,
		`VALUES ('t'), (?) UNION SELECT BOOL 'f'`,
	} {
		_, err = PrepareStatement(source)
		var mismatch *ScalarTypeError
		if !errors.As(err, &mismatch) || !errors.Is(err, ErrScalarType) {
			t.Fatalf("%q error = %T %v, want datatype mismatch", source, err, err)
		}
	}

	_, err = PrepareStatement(`SELECT 'o' UNION SELECT BOOL 't'`)
	if !errors.As(err, &invalidText) || !errors.Is(err, ErrScalarInvalidText) {
		t.Fatalf("known invalid boolean text = %T %v", err, err)
	}

	grouped, err := PrepareStatement(
		`(SELECT true AS value UNION SELECT ? ORDER BY 1) ` +
			`UNION ALL SELECT BOOL 'f'`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer grouped.Release()
	if got := grouped.ParameterType(0); got != ParameterTypeBool {
		t.Fatalf("grouped parameter type = %s, want boolean", got)
	}
	valid := "t"
	groupArgs := []any{&valid}
	var cursor Cursor
	groupRun := func() {
		cursor, err = grouped.RunInto(&execution, Source{}, groupArgs)
	}
	groupRun()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{"true", "false"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped coercion rows = %v, want %v", got, want)
	}
	if allocations := testing.AllocsPerRun(100, groupRun); allocations != 0 {
		t.Fatalf("warmed grouped typed set allocations = %.2f, want 0", allocations)
	}

	absolute, err := PrepareStatement(
		`SELECT ? AS value UNION ALL ` +
			`(SELECT true UNION SELECT ? ORDER BY 1) ` +
			`UNION ALL SELECT BOOL 'f'`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer absolute.Release()
	if absolute.ParameterType(0) != ParameterTypeBool ||
		absolute.ParameterType(1) != ParameterTypeBool {
		t.Fatalf("absolute grouped parameter types = %s/%s, want boolean/boolean",
			absolute.ParameterType(0), absolute.ParameterType(1))
	}
	left, right := "f", "t"
	cursor, err = absolute.RunInto(
		&execution, Source{}, []any{&left, &right},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor),
		[]string{"false", "true", "false"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("absolute grouped rows = %v, want %v", got, want)
	}

	topGroup, err := PrepareStatement(`(VALUES (BOOL 't'), (?) ORDER BY 1)`)
	if err != nil {
		t.Fatal(err)
	}
	defer topGroup.Release()
	if got := topGroup.ParameterType(0); got != ParameterTypeBool {
		t.Fatalf("top-level group parameter type = %s, want boolean", got)
	}
}

func TestSQLSetTypedParenthesizedTailBoundaryAndEmptyLeafValidation(t *testing.T) {
	for _, source := range []string{
		`SELECT BOOL 't' AS value UNION ALL (SELECT 'f' LIMIT 1)`,
		`SELECT BOOL 't' AS value UNION ALL (SELECT 'f' OFFSET 0)`,
		`SELECT BOOL 't' AS value UNION ALL (SELECT 'f' LIMIT 1 OFFSET 0)`,
	} {
		t.Run(source, func(t *testing.T) {
			statement, err := PrepareStatement(source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			schema := statement.AppendSchema(nil)
			if len(schema) != 1 || schema[0].Type != TypeBool ||
				schema[0].Representation != OutputSQLBool {
				t.Fatalf("schema = %+v, want SQL boolean", schema)
			}
			var execution Exec
			cursor, err := statement.RunInto(&execution, Source{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := setStatementCursorJSON(cursor), []string{"true", "false"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("rows = %v, want %v", got, want)
			}
		})
	}

	for _, source := range []string{
		`SELECT BOOL 't' UNION ALL (SELECT 'f' ORDER BY 1)`,
		`SELECT BOOL 't' UNION ALL (SELECT 'f' ORDER BY 1 LIMIT 1)`,
		`SELECT BOOL 't' UNION ALL (SELECT ? ORDER BY 1)`,
	} {
		_, err := PrepareStatement(source)
		var mismatch *ScalarTypeError
		if !errors.As(err, &mismatch) || mismatch.Left != TypeBool ||
			mismatch.Right != TypeString {
			t.Fatalf("ORDER BY boundary %q = %T %v, want bool/text mismatch", source, err, err)
		}
	}

	parameterized, err := PrepareStatement(
		`SELECT BOOL 't' AS value UNION ALL (SELECT ? LIMIT 1 OFFSET 0)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer parameterized.Release()
	if got := parameterized.ParameterType(0); got != ParameterTypeBool {
		t.Fatalf("LIMIT/OFFSET parameter type = %s, want boolean", got)
	}
	valid := "f"
	var execution Exec
	cursor, err := parameterized.RunInto(&execution, Source{}, []any{&valid})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{"true", "false"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("parameterized rows = %v, want %v", got, want)
	}

	emptyBool, err := PrepareStatement(
		`(SELECT ? AS value LIMIT 0) UNION ALL SELECT BOOL 't'`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer emptyBool.Release()
	if got := emptyBool.ParameterType(0); got != ParameterTypeBool {
		t.Fatalf("empty boolean leaf parameter type = %s, want boolean", got)
	}
	invalid := "o"
	_, err = emptyBool.RunInto(&execution, Source{}, []any{&invalid})
	var invalidText *ScalarInvalidTextError
	if !errors.As(err, &invalidText) || invalidText.Position() != strings.Index(
		`(SELECT ? AS value LIMIT 0) UNION ALL SELECT BOOL 't'`, "?",
	) {
		t.Fatalf("empty boolean leaf invalid input = %T %v", err, err)
	}

	valid = "f"
	boolArgs := []any{&valid}
	var boolCursor Cursor
	boolRun := func() {
		boolCursor, err = emptyBool.RunInto(&execution, Source{}, boolArgs)
	}
	boolRun()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(boolCursor), []string{"true"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("empty boolean leaf rows = %v, want %v", got, want)
	}
	if allocations := testing.AllocsPerRun(100, boolRun); allocations != 0 {
		t.Fatalf("warmed empty boolean leaf allocated %.2f, want 0", allocations)
	}

	emptyText, err := PrepareStatement(
		`(SELECT ? AS value LIMIT 0) UNION ALL SELECT TEXT 'x'`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer emptyText.Release()
	if got := emptyText.ParameterType(0); got != ParameterTypeText {
		t.Fatalf("empty text leaf parameter type = %s, want text", got)
	}
	_, err = emptyText.RunInto(&execution, Source{}, []any{Number("not-a-number")})
	if err == nil || !strings.Contains(err.Error(), "not a JSON number") {
		t.Fatalf("empty text leaf invalid input = %T %v", err, err)
	}

	var cancel CancelFlag
	execution.Options.Cancel = &cancel
	cancel.Cancel()
	_, err = emptyBool.RunInto(&execution, Source{}, []any{&invalid})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("pre-canceled validation = %T %v, want ErrCanceled", err, err)
	}
}

func TestSQLSetTypedSchemaPropagatesThroughDerivedAndCTE(t *testing.T) {
	for _, source := range []string{
		`SELECT value FROM (` +
			`SELECT BOOL 't' AS value UNION ALL SELECT BOOL 'f') AS q`,
		`WITH q AS (` +
			`SELECT BOOL 't' AS value UNION ALL SELECT BOOL 'f') SELECT * FROM q`,
	} {
		t.Run(source, func(t *testing.T) {
			statement, err := PrepareStatement(source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			schema := statement.AppendSchema(nil)
			if len(schema) != 1 || schema[0].Type != TypeBool ||
				schema[0].Representation != OutputSQLBool {
				t.Fatalf("propagated schema = %+v, want SQL bool", schema)
			}
			buffer := make([]OutputColumn, 0, 1)
			if allocations := testing.AllocsPerRun(100, func() {
				buffer = statement.AppendSchema(buffer[:0])
			}); allocations != 0 {
				t.Fatalf("warmed propagated schema allocated %.2f, want 0", allocations)
			}
			var execution Exec
			cursor, err := statement.RunInto(&execution, Source{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := setStatementCursorJSON(cursor), []string{"true", "false"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("propagated rows = %v, want %v", got, want)
			}
		})
	}

	mixed, err := PrepareStatement(
		`SELECT value, 1 + 0 FROM (` +
			`SELECT BOOL 't' AS value UNION ALL SELECT BOOL 'f') AS q`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer mixed.Release()
	schema := mixed.AppendSchema(nil)
	if len(schema) != 2 || schema[0].Type != TypeBool ||
		schema[0].Representation != OutputSQLBool ||
		schema[1].Representation != OutputSQLNumber {
		t.Fatalf("mixed propagated schema = %+v", schema)
	}
}

func TestSQLSetLoweringScopedTailsAndAbsoluteParameters(t *testing.T) {
	database := setStatementDatabase(t)
	statement, err := PrepareStatement(
		`(SELECT v AS value FROM set_left WHERE v >= ? ORDER BY value DESC LIMIT ?) ` +
			`UNION ALL SELECT v FROM set_right WHERE v <= ? ` +
			`ORDER BY 1 DESC LIMIT ? OFFSET ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.NumParams() != 5 {
		t.Fatalf("parameters = %d, want 5", statement.NumParams())
	}
	args := []any{int64(2), int64(2), int64(3), int64(3), int64(1)}
	var execution Exec
	cursor, err := statement.RunInto(
		&execution,
		FromDatabase(database.Snapshot(), statement.Collection()), args,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{"3", "2", "2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}

	explained, err := statement.ExplainBound(args)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"node":"set"`, `"operation":"union all"`,
		`"order_by":["value DESC"]`, `"limit":3`, `"offset":1`,
	} {
		if !strings.Contains(explained, fragment) {
			t.Fatalf("EXPLAIN %s does not contain %s", explained, fragment)
		}
	}
}

func TestSQLSetLoweringDeferredWildcardArityAndUTF8Position(t *testing.T) {
	valid := `SELECT d.* FROM (SELECT v, tag FROM set_left) d ` +
		`UNION ALL SELECT v, tag FROM set_right`
	statement, err := PrepareStatement(valid)
	if err != nil {
		t.Fatal(err)
	}
	if got := statement.Columns(); !reflect.DeepEqual(got, []string{"v", "tag"}) {
		statement.Release()
		t.Fatalf("expanded columns = %v", got)
	}
	statement.Release()

	invalid := `SELECT d.* FROM (SELECT v, tag FROM "café") d UNION SELECT v FROM set_right`
	_, err = PrepareStatement(invalid)
	var arity *SetSQLArityError
	if !errors.As(err, &arity) || !errors.Is(err, ErrSetTreeArity) {
		t.Fatalf("error = %T %v, want positioned set arity", err, err)
	}
	if want := strings.Index(invalid, "UNION"); arity.Position() != want {
		t.Fatalf("position = %d, want %d", arity.Position(), want)
	}
}

func TestSQLSetLoweringCatalogClassificationAndCTEReuse(t *testing.T) {
	single, err := PrepareStatement(
		`SELECT v FROM set_left WHERE v >= ? UNION ALL ` +
			`SELECT v FROM set_left WHERE v <= ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if single.RequiresCatalog() || single.Collection() != "set_left" {
		single.Release()
		t.Fatalf("single dependency classified as catalog: %v/%q",
			single.RequiresCatalog(), single.Collection())
	}
	single.Release()

	multi, err := PrepareStatement(
		`WITH c AS (SELECT v FROM set_left WHERE v >= ?) ` +
			`SELECT v FROM c UNION ALL SELECT v FROM set_right WHERE v <= ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer multi.Release()
	if !multi.RequiresCatalog() || !multi.UsesDirectCatalogExecution() {
		t.Fatal("multi-dependency set did not request direct coherent catalog execution")
	}
	database := setStatementDatabase(t)
	var execution Exec
	cursor, err := multi.RunInto(
		&execution,
		FromDatabase(database.Snapshot(), multi.Collection()),
		[]any{int64(1), int64(4)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor),
		[]string{"1", "2", "2", "4", "2", "2", "3", "4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CTE rows = %v, want %v", got, want)
	}

	shared, err := PrepareStatement(
		`WITH c AS (SELECT v FROM set_left WHERE v >= ?) ` +
			`SELECT v FROM c UNION ALL SELECT v FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Release()
	catalog := shared.cteCatalog()
	if catalog == nil || len(catalog.defs) != 1 || catalog.defs[0].references != 2 {
		t.Fatalf("shared CTE catalog/reference count = %+v", catalog)
	}
	var sharedExecution Exec
	sharedCursor, err := shared.RunInto(
		&sharedExecution,
		FromDatabase(database.Snapshot(), shared.Collection()),
		[]any{int64(1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(sharedCursor),
		[]string{"1", "2", "2", "4", "1", "2", "2", "4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shared CTE rows = %v, want %v", got, want)
	}
	if evaluations := catalog.defs[0].runEvaluations; evaluations != 1 {
		t.Fatalf("shared CTE evaluations = %d, want exactly 1", evaluations)
	}
	if catalog.defs[0].activeBytes != 0 || catalog.defs[0].spool.rows != 0 {
		t.Fatal("shared CTE publication survived completed set statement")
	}
}

func TestSQLSetLoweringGroupedTailUsesIntermediateLimits(t *testing.T) {
	database := setStatementDatabase(t)
	statement, err := PrepareStatement(
		`(SELECT v AS value FROM set_left ` +
			`UNION ALL SELECT v FROM set_right ORDER BY value) ` +
			`INTERSECT SELECT v FROM set_left WHERE v = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	var execution Exec
	execution.Options.ResultRows = 1
	execution.Options.ResultBytes = 256
	execution.Options.IntermediateBytes = 1 << 20
	cursor, err := statement.RunInto(
		&execution,
		FromDatabase(database.Snapshot(), statement.Collection()),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{"1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	if execution.Result.RowCount != 1 {
		t.Fatalf("published rows = %d, want 1", execution.Result.RowCount)
	}
	if used := statement.nested.frame.intermediate.used; used != 0 {
		t.Fatalf("statement retained %d intermediate bytes", used)
	}
}

func TestSQLSetLoweringCancellationNoPartialAndWarmZeroAlloc(t *testing.T) {
	database := setStatementDatabase(t)
	statement, err := PrepareStatement(
		`SELECT v AS value FROM set_left WHERE v >= ? ` +
			`UNION DISTINCT SELECT v FROM set_right WHERE v <= ? ` +
			`ORDER BY value DESC LIMIT ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	low, high, limit := int64(1), int64(4), int64(3)
	args := []any{&low, &high, &limit}
	source := FromDatabase(database.Snapshot(), statement.Collection())
	var cancel CancelFlag
	var execution Exec
	execution.Options.Cancel = &cancel
	run := func() {
		_, err = statement.RunInto(&execution, source, args)
	}
	run()
	if err != nil || execution.Result.RowCount != 3 {
		t.Fatalf("warm run rows/error = %d/%v", execution.Result.RowCount, err)
	}
	if allocations := testing.AllocsPerRun(100, run); allocations != 0 {
		t.Fatalf("warmed SQL set allocations = %.2f, want 0", allocations)
	}
	if err != nil {
		t.Fatal(err)
	}

	cancel.Cancel()
	run()
	if !errors.Is(err, ErrCanceled) || execution.Result.RowCount != 0 {
		t.Fatalf("canceled run rows/error = %d/%v", execution.Result.RowCount, err)
	}
	cancel.Reset()
	run()
	if err != nil || execution.Result.RowCount != 3 {
		t.Fatalf("recovery rows/error = %d/%v", execution.Result.RowCount, err)
	}
}
