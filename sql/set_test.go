package sql

import (
	"errors"
	"strings"
	"testing"
)

func parseSetForTest(t testing.TB, source string) *SelectStmt {
	t.Helper()
	statement, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	if statement.Set == nil {
		t.Fatalf("Parse(%q) returned no set-expression sidecar", source)
	}
	return statement
}

func TestSetExpressionPrecedenceAssociativityAndAuthoredGroups(t *testing.T) {
	statement := parseSetForTest(t,
		`SELECT id FROM one UNION SELECT id FROM two INTERSECT ALL `+
			`SELECT id FROM three EXCEPT SELECT id FROM four UNION ALL SELECT id FROM five`)
	root := statement.Set.Root
	if root.Kind != SetBinaryExpr || root.Operation != SetUnionAll {
		t.Fatalf("root = %+v, want final left-associative UNION ALL", root)
	}
	if root.Left.Kind != SetBinaryExpr || root.Left.Operation != SetExceptDistinct {
		t.Fatalf("root.Left = %+v, want EXCEPT DISTINCT", root.Left)
	}
	union := root.Left.Left
	if union.Kind != SetBinaryExpr || union.Operation != SetUnionDistinct {
		t.Fatalf("EXCEPT left = %+v, want UNION DISTINCT", union)
	}
	if union.Right.Kind != SetBinaryExpr || union.Right.Operation != SetIntersectAll {
		t.Fatalf("UNION right = %+v, want tighter INTERSECT ALL", union.Right)
	}
	if got, want := setLeafCollections(root), []string{"one", "two", "three", "four", "five"}; !equalStrings(got, want) {
		t.Fatalf("leaf order = %v, want %v", got, want)
	}

	grouped := parseSetForTest(t,
		`(SELECT id FROM one UNION ALL SELECT id FROM two) INTERSECT `+
			`(SELECT id FROM three EXCEPT ALL SELECT id FROM four)`)
	root = grouped.Set.Root
	if root.Operation != SetIntersectDistinct ||
		root.Left.Kind != SetGroupExpr || root.Right.Kind != SetGroupExpr ||
		root.Left.Child.Operation != SetUnionAll ||
		root.Right.Child.Operation != SetExceptAll {
		t.Fatalf("authored groups or operation shape lost: %s", dumpStmt(grouped))
	}
	if root.First != root.Left.Child.Left.Select || grouped.Set.First != root.First {
		t.Fatal("parentheses changed syntactic first-operand identity")
	}
}

func TestSetExpressionAllSixModesMapExactly(t *testing.T) {
	cases := []struct {
		syntax string
		want   SetOperation
	}{
		{`UNION ALL`, SetUnionAll},
		{`UNION`, SetUnionDistinct},
		{`INTERSECT ALL`, SetIntersectAll},
		{`INTERSECT DISTINCT`, SetIntersectDistinct},
		{`EXCEPT ALL`, SetExceptAll},
		{`EXCEPT DISTINCT`, SetExceptDistinct},
	}
	for _, test := range cases {
		t.Run(test.syntax, func(t *testing.T) {
			statement := parseSetForTest(t,
				`SELECT id FROM left_side `+test.syntax+` SELECT id FROM right_side`)
			if got := statement.Set.Root.Operation; got != test.want {
				t.Fatalf("operation = %d, want %d", got, test.want)
			}
		})
	}
}

func TestSetExpressionParameterRangesAndTailScopes(t *testing.T) {
	statement := parseSetForTest(t,
		`(SELECT id FROM one WHERE x = ? ORDER BY id LIMIT ?) `+
			`UNION ALL SELECT id FROM two WHERE y IN (?, ?) `+
			`INTERSECT DISTINCT (SELECT id FROM three WHERE z = ? LIMIT ?) `+
			`ORDER BY id DESC LIMIT ? OFFSET ?`)
	if statement.Params != 8 || statement.Set.Params != 8 {
		t.Fatalf("Params = %d/%d, want 8", statement.Params, statement.Set.Params)
	}
	root := statement.Set.Root
	if root.Operation != SetUnionAll || root.ParamBase != 0 || root.Params != 6 {
		t.Fatalf("root = %+v, want UNION ALL over placeholder range [0,6)", root)
	}
	left := root.Left
	if left.Kind != SetGroupExpr || left.ParamBase != 0 || left.Params != 2 ||
		left.Child.ParamBase != 0 || left.Child.Params != 1 ||
		left.Tail == nil || left.Tail.ParamBase != 1 || left.Tail.Params != 1 ||
		left.Tail.Limit.Ordinal != 1 {
		t.Fatalf("first grouped operand has wrong ranges: %+v", left)
	}
	right := root.Right
	if right.Operation != SetIntersectDistinct || right.ParamBase != 2 || right.Params != 4 ||
		right.Left.ParamBase != 2 || right.Left.Params != 2 ||
		right.Right.ParamBase != 4 || right.Right.Params != 2 ||
		right.Right.Tail.ParamBase != 5 || right.Right.Tail.Limit.Ordinal != 5 {
		t.Fatalf("right INTERSECT subtree has wrong ranges: %+v", right)
	}
	tail := statement.Set.Tail
	if tail == nil || tail.ParamBase != 6 || tail.Params != 2 ||
		len(tail.OrderBy) != 1 || tail.OrderBy[0].Output != 1 || !tail.OrderBy[0].Desc ||
		tail.Limit.Ordinal != 6 || tail.Offset.Ordinal != 7 {
		t.Fatalf("final tail = %+v, want ORDER then placeholder range [6,8)", tail)
	}
	if len(statement.OrderBy) != 0 || statement.Limit != nil || statement.Offset != nil {
		t.Fatal("final set tail leaked into the mirrored first SELECT")
	}
	checkStatementInvariants(t, statement)
}

func TestSetExpressionFirstOperandOwnsOutputNames(t *testing.T) {
	statement := parseSetForTest(t,
		`SELECT l.id AS key, r.id FROM left_side l JOIN right_side r ON l.id = r.id `+
			`UNION ALL SELECT a, b FROM fallback ORDER BY key, "r.id" DESC`)
	want := []SetOutputColumn{
		{Name: "key", Pos: strings.Index(statementTextForPositions(), "l.id")},
		{Name: "r.id"},
	}
	if len(statement.Set.Outputs) != len(want) ||
		statement.Set.Outputs[0].Name != want[0].Name ||
		statement.Set.Outputs[1].Name != want[1].Name {
		t.Fatalf("outputs = %+v, want names key and r.id", statement.Set.Outputs)
	}
	order := statement.Set.Tail.OrderBy
	if len(order) != 2 || order[0].Output != 1 || order[1].Output != 2 || !order[1].Desc {
		t.Fatalf("final ORDER BY = %+v", order)
	}

	_, err := Parse(`SELECT a AS id, b AS id FROM one UNION SELECT x, y FROM two ORDER BY id`)
	assertSetParseErrorAt(t, err, `SELECT a AS id, b AS id FROM one UNION SELECT x, y FROM two ORDER BY id`,
		strings.LastIndex(`SELECT a AS id, b AS id FROM one UNION SELECT x, y FROM two ORDER BY id`, "id"),
		"ambiguous")

	wildcard := parseSetForTest(t, `SELECT * FROM one UNION ALL SELECT id FROM two`)
	if !wildcard.Set.ArityDeferred || !wildcard.Set.Outputs[0].Deferred {
		t.Fatalf("wildcard set did not defer arity and output naming: %+v", wildcard.Set)
	}
}

func TestSetExpressionOrderByOutputPositions(t *testing.T) {
	statement := parseSetForTest(t,
		`SELECT a, b FROM left_side UNION ALL SELECT x, y FROM right_side ORDER BY 2 DESC, 1`)
	order := statement.Set.Tail.OrderBy
	if len(order) != 2 || order[0].Output != 2 || !order[0].Desc ||
		order[1].Output != 1 || order[1].Desc {
		t.Fatalf("set positional ORDER BY = %+v", order)
	}
	for _, source := range []string{
		`SELECT a FROM one UNION ALL SELECT b FROM two ORDER BY -1`,
		`SELECT a FROM one UNION ALL SELECT b FROM two ORDER BY 0`,
		`SELECT a FROM one UNION ALL SELECT b FROM two ORDER BY 2`,
		`SELECT a FROM one UNION ALL SELECT b FROM two ORDER BY 1.5`,
	} {
		_, err := Parse(source)
		var invalid *InvalidOrderPositionError
		if !errors.As(err, &invalid) || invalid.Outputs != 1 {
			t.Fatalf("%q error = %T %+v", source, err, err)
		}
	}
	var unsupported *FeatureNotSupportedError
	for _, source := range []string{
		`SELECT * FROM one UNION ALL SELECT * FROM two ORDER BY 1`,
		`TABLE one UNION ALL TABLE two ORDER BY 1`,
	} {
		_, err := Parse(source)
		if !errors.As(err, &unsupported) || !strings.Contains(unsupported.Msg, "wildcard") {
			t.Fatalf("deferred set ordinal %q error = %T %v", source, err, err)
		}
	}
	_, err := Parse(`SELECT * FROM one UNION ALL SELECT * FROM two ORDER BY 0`)
	var invalidWildcard *InvalidOrderPositionError
	if !errors.As(err, &invalidWildcard) {
		t.Fatalf("invalid deferred set ordinal precedence = %T %v", err, err)
	}
	_, err = Parse(`SELECT a FROM one UNION ALL SELECT b FROM two ORDER BY 1 + 0`)
	if !errors.As(err, &unsupported) || !strings.Contains(unsupported.Msg, "must stand alone") {
		t.Fatalf("set numeric ORDER expression error = %T %v", err, err)
	}
}

func TestSetExpressionNestedPositionsCTEVisibilityAndParamBase(t *testing.T) {
	const source = `SELECT id FROM outer_table WHERE tenant = ? AND id IN (` +
		`SELECT "café" AS id FROM live UNION ALL SELECT id FROM archive WHERE tenant = ?)`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	subquery := findSubquery(statement.Where)
	if subquery == nil || subquery.Set == nil {
		t.Fatalf("nested set subquery missing: %s", dumpStmt(statement))
	}
	if subquery.ParamBase != 1 || subquery.Params != 1 ||
		subquery.Set.Root.Left.ParamBase != 0 || subquery.Set.Root.Right.ParamBase != 0 {
		t.Fatalf("nested parameter bases = outer %d, tree %+v", subquery.ParamBase, subquery.Set.Root)
	}
	if got, want := subquery.Set.Root.Pos, strings.Index(source, "UNION ALL"); got != want {
		t.Fatalf("nested operator position = %d, want byte offset %d", got, want)
	}
	if got, want := subquery.Set.Outputs[0].Pos, strings.Index(source, `"café"`); got != want {
		t.Fatalf("nested output position = %d, want byte offset %d", got, want)
	}

	cte := parseSetForTest(t,
		`WITH visible AS (SELECT id FROM base) SELECT id FROM visible `+
			`UNION ALL SELECT id FROM visible`)
	leaves := setLeaves(cte.Set.Root)
	if len(leaves) != 2 || leaves[0].Select.From[0].Kind != RelationCTE ||
		leaves[1].Select.From[0].Kind != RelationCTE ||
		leaves[0].Select.From[0].Query != leaves[1].Select.From[0].Query {
		t.Fatalf("root WITH scope did not bind every set leaf: %s", dumpStmt(cte))
	}
}

func TestSetExpressionLateralCaptureRollsBackProbeExactlyOnce(t *testing.T) {
	const source = `SELECT a.id, d.id FROM accounts a LEFT JOIN LATERAL (` +
		`SELECT id FROM items i WHERE i.owner = a.id UNION ALL ` +
		`SELECT id FROM archived j WHERE j.owner = a.id` +
		`) d ON TRUE`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	lateral := statement.From[1].Lateral
	if lateral == nil || lateral.Decorrelated || len(lateral.Bindings) != 1 ||
		len(lateral.References) != 2 {
		t.Fatalf("set LATERAL capture = %+v, want one exact binding and two references", lateral)
	}
	if statement.From[1].Query.Set == nil {
		t.Fatal("LATERAL derived relation lost set root")
	}
	checkStatementInvariants(t, statement)
}

func TestSetExpressionCTEAliasArityUsesCompleteSetWidth(t *testing.T) {
	const source = `WITH q(a, b, c) AS (` +
		`SELECT x, y FROM one UNION ALL SELECT u, v FROM two` +
		`) SELECT a FROM q`
	var parser Parser
	var statement SelectStmt
	err := parser.Parse(&statement, source)
	assertSetParseErrorAt(t, err, source, strings.Index(source, "c)"), "3 column aliases")
	if statement.Set != nil || statement.With != nil {
		t.Fatal("rejected CTE set width retained a partial AST")
	}

	deferred, err := Parse(`WITH q(a, b) AS (` +
		`SELECT * FROM one UNION ALL SELECT u, v FROM two` +
		`) SELECT * FROM q`)
	if err != nil {
		t.Fatal(err)
	}
	if deferred.With == nil || !deferred.With.CTEs[0].ColumnArityDeferred ||
		deferred.With.CTEs[0].Query.Set == nil {
		t.Fatalf("wildcard set CTE arity metadata = %+v", deferred.With)
	}
}

func TestSetExpressionMalformedAndUnsupportedDiagnosticsAreTypedAndPositioned(t *testing.T) {
	cases := []struct {
		name    string
		source  string
		at      string
		message string
		typed   bool
	}{
		{"missing right operand", `SELECT id FROM one UNION`, "", "expected SELECT", false},
		{"double modifier", `SELECT id FROM one UNION ALL DISTINCT SELECT id FROM two`, "DISTINCT", "exactly one", false},
		{"unparenthesized WITH", `SELECT id FROM one UNION WITH q AS (SELECT id FROM two) SELECT id FROM q`, "WITH", "parenthesized", false},
		{"values expression", "SELECT \"café\" FROM one\nUNION ALL\nVALUES (missing)", "missing", "accept scalar literals", true},
		{"values default", `VALUES (DEFAULT) UNION VALUES (1)`, "DEFAULT", "not defined", true},
		{"qualified table", `SELECT id FROM one EXCEPT TABLE public.two`, ".", "qualified TABLE", true},
		{"with values root", `WITH c AS (SELECT id FROM one) VALUES (1)`, "VALUES", "lexical execution owner", true},
		{"with table root", `WITH c AS (SELECT id FROM one) TABLE c`, "TABLE", "lexical execution owner", true},
		{"arity mismatch", `SELECT a, b FROM one INTERSECT SELECT c FROM two`, "INTERSECT", "2 and 1", false},
		{"unknown final output", `SELECT id FROM one UNION SELECT id FROM two ORDER BY missing`, "missing", "not an output", false},
		{"operand local tail", `SELECT id FROM one ORDER BY id UNION SELECT id FROM two`, "ORDER", "operand-local", false},
		{"unclosed group", `(SELECT id FROM one UNION SELECT id FROM two`, "", "expected ')'", false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var parser Parser
			var statement SelectStmt
			err := parser.Parse(&statement, test.source)
			position := len(test.source)
			if test.at != "" {
				position = strings.Index(test.source, test.at)
			}
			parseError := assertSetParseErrorAt(t, err, test.source, position, test.message)
			var unsupported *FeatureNotSupportedError
			if errors.As(err, &unsupported) != test.typed {
				t.Fatalf("error type = %T, typed unsupported = %v", err, test.typed)
			}
			if test.name == "values expression" && (parseError.Line != 3 || parseError.Col != 9) {
				t.Fatalf("UTF-8 positioned error = %d:%d, want 3:9", parseError.Line, parseError.Col)
			}
			if statement.Set != nil || len(statement.Columns) != 0 || len(statement.From) != 0 {
				t.Fatal("rejected set expression left a consumable partial AST")
			}
		})
	}
}

func TestSetExpressionValuesAndTableOperandsPreserveShapeParamsAndMetadata(t *testing.T) {
	statement := parseSetForTest(t,
		`(VALUES (?, 'a'), (2, NULL) ORDER BY column1 LIMIT ?) `+
			`UNION ALL TABLE live INTERSECT DISTINCT VALUES (?, 'z') `+
			`ORDER BY column2 DESC OFFSET ?`)
	root := statement.Set.Root
	if root.Kind != SetBinaryExpr || root.Operation != SetUnionAll ||
		root.Left.Kind != SetGroupExpr || root.Left.Child.Kind != SetValuesExpr ||
		root.Right.Kind != SetBinaryExpr || root.Right.Operation != SetIntersectDistinct ||
		root.Right.Left.Kind != SetTableExpr || root.Right.Right.Kind != SetValuesExpr {
		t.Fatalf("VALUES/TABLE tree shape = %s", dumpStmt(statement))
	}
	if statement.Params != 4 || root.Left.ParamBase != 0 || root.Left.Params != 2 ||
		root.Right.Left.ParamBase != 2 || root.Right.Left.Params != 0 ||
		root.Right.Right.ParamBase != 2 || root.Right.Right.Params != 1 ||
		statement.Set.Tail.ParamBase != 3 || statement.Set.Tail.Params != 1 {
		t.Fatalf("VALUES/TABLE parameter ranges = %s", dumpStmt(statement))
	}
	values := root.Left.Child.Values
	if values == nil || len(values.Rows) != 2 || len(values.Rows[0].Values) != 2 ||
		values.Rows[0].Values[0].Operand.Ordinal != 0 ||
		!values.Rows[1].Values[1].Null {
		t.Fatalf("VALUES payload = %+v", values)
	}
	if got := statement.Set.Outputs; len(got) != 2 ||
		got[0].Name != "column1" || got[1].Name != "column2" ||
		got[0].Deferred || got[1].Deferred {
		t.Fatalf("VALUES first metadata = %+v", got)
	}
	table := root.Right.Left
	if table.Table == nil || table.Table.Ref.Name != "live" ||
		table.Select == nil || len(table.Select.From) != 1 ||
		table.Select.From[0].Name != "live" || !table.ArityDeferred {
		t.Fatalf("TABLE payload = %+v", table)
	}
	checkStatementInvariants(t, statement)

	for _, source := range []string{`VALUES (1)`, `TABLE live`} {
		root := parseSetForTest(t, source)
		if root.Set.Root.First != root.Set.First {
			t.Fatalf("bare %q lost first identity", source)
		}
		checkStatementInvariants(t, root)
	}
}

func TestSetExpressionParserReuseClearsColdSidecar(t *testing.T) {
	var parser Parser
	var statement SelectStmt
	compound := `SELECT id FROM one WHERE a = ? UNION ALL SELECT id FROM two WHERE b = ?`
	if err := parser.Parse(&statement, compound); err != nil {
		t.Fatal(err)
	}
	if statement.Set == nil || parser.set == nil {
		t.Fatal("compound parse did not install cold set state")
	}
	if err := parser.Parse(&statement, `VALUES (?, 'x'), (2, NULL) UNION ALL TABLE live`); err != nil {
		t.Fatal(err)
	}
	if statement.Set == nil || statement.Set.Root.Left.Kind != SetValuesExpr ||
		statement.Set.Root.Right.Kind != SetTableExpr || statement.Params != 1 {
		t.Fatalf("VALUES/TABLE reuse parse = %s", dumpStmt(&statement))
	}
	if err := parser.Parse(&statement, `SELECT name FROM ordinary WHERE id = ?`); err != nil {
		t.Fatal(err)
	}
	if statement.Set != nil || statement.Params != 1 || len(statement.From) != 1 || statement.From[0].Name != "ordinary" {
		t.Fatalf("ordinary parse retained stale compound state: %+v", statement.Set)
	}
	if err := parser.Parse(&statement, `SELECT id FROM one UNION ALL`); err == nil {
		t.Fatal("malformed compound parse succeeded")
	}
	if statement.Set != nil || len(statement.Columns) != 0 || statement.Params != 0 {
		t.Fatal("failed compound parse retained stale or partial state")
	}
	if err := parser.Parse(&statement, compound); err != nil {
		t.Fatal(err)
	}
	checkStatementInvariants(t, &statement)
	if err := parser.Parse(&statement, `TABLE after_compound`); err != nil {
		t.Fatal(err)
	}
	if statement.Set == nil || statement.Set.Root.Kind != SetTableExpr ||
		statement.Params != 0 || statement.Set.Root.Values != nil {
		t.Fatalf("TABLE parse retained stale VALUES state: %s", dumpStmt(&statement))
	}

	var ordinaryParser Parser
	if err := ordinaryParser.Parse(&statement, `SELECT id FROM ordinary`); err != nil {
		t.Fatal(err)
	}
	if ordinaryParser.set != nil || statement.Set != nil {
		t.Fatal("ordinary-only parser allocated or exposed cold set state")
	}
}

func TestSetExpressionArenaReuseRetainsExactTreeWithoutGrowth(t *testing.T) {
	var parser Parser
	var statement SelectStmt
	for i := 0; i < 2; i++ {
		if err := parser.Parse(&statement, benchSetExpression); err != nil {
			t.Fatal(err)
		}
	}
	firstDump := dumpStmt(&statement)
	firstLeaf := setLeaves(statement.Set.Root)[0]
	firstOutput := &statement.Set.Outputs[0]
	firstTail := statement.Set.Tail

	if err := parser.Parse(&statement, `SELECT id FROM ordinary`); err != nil {
		t.Fatal(err)
	}
	if err := parser.Parse(&statement, benchSetExpression); err != nil {
		t.Fatal(err)
	}
	if got := dumpStmt(&statement); got != firstDump {
		t.Fatalf("arena reuse changed set AST:\nfirst  %s\nsecond %s", firstDump, got)
	}
	if setLeaves(statement.Set.Root)[0] != firstLeaf ||
		&statement.Set.Outputs[0] != firstOutput || statement.Set.Tail != firstTail {
		t.Fatal("set parse did not refill its retained node/output/tail arenas")
	}
	checkStatementInvariants(t, &statement)
}

func TestSetExpressionParseStatementAndExplainPreserveRoot(t *testing.T) {
	cases := []string{
		`SELECT id FROM one UNION ALL SELECT id FROM two`,
		`(SELECT id FROM one EXCEPT SELECT id FROM two)`,
		`EXPLAIN SELECT id FROM one INTERSECT SELECT id FROM two`,
		`EXPLAIN ANALYZE (SELECT id FROM one UNION SELECT id FROM two)`,
		`VALUES (1), (2)`,
		`TABLE one`,
		`EXPLAIN VALUES (1) UNION ALL VALUES (2)`,
		`EXPLAIN ANALYZE TABLE one`,
	}
	for _, source := range cases {
		var parser Parser
		var statement Statement
		if err := parser.ParseStatement(&statement, source); err != nil {
			t.Fatalf("ParseStatement(%q): %v", source, err)
		}
		if statement.Kind != KindSelect || statement.Select == nil || statement.Select.Set == nil {
			t.Fatalf("ParseStatement(%q) lost set root: %+v", source, statement)
		}
		checkStatementInvariants(t, statement.Select)
	}
}

func assertSetParseErrorAt(
	t testing.TB,
	err error,
	source string,
	position int,
	contains string,
) *ParseError {
	t.Helper()
	if err == nil {
		t.Fatalf("Parse(%q) succeeded, want error", source)
	}
	var parseError *ParseError
	if !errors.As(err, &parseError) {
		t.Fatalf("error = %T, want positioned *ParseError", err)
	}
	if parseError.Pos != position {
		t.Fatalf("error position = %d, want %d: %v", parseError.Pos, position, err)
	}
	if !strings.Contains(parseError.Msg, contains) {
		t.Fatalf("error message %q does not contain %q", parseError.Msg, contains)
	}
	return parseError
}

func setLeaves(root *SetExpr) []*SetExpr {
	leaves := make([]*SetExpr, 0, 4)
	var walk func(*SetExpr)
	walk = func(expression *SetExpr) {
		switch expression.Kind {
		case SetSelectExpr:
			leaves = append(leaves, expression)
		case SetBinaryExpr:
			walk(expression.Left)
			walk(expression.Right)
		case SetGroupExpr:
			walk(expression.Child)
		}
	}
	walk(root)
	return leaves
}

func setLeafCollections(root *SetExpr) []string {
	leaves := setLeaves(root)
	names := make([]string, len(leaves))
	for i := range leaves {
		names[i] = leaves[i].Select.From[0].Name
	}
	return names
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func findSubquery(expression *Expr) *SelectStmt {
	if expression == nil {
		return nil
	}
	if expression.Subquery != nil {
		return expression.Subquery
	}
	for _, child := range expression.Kids {
		if subquery := findSubquery(child); subquery != nil {
			return subquery
		}
	}
	return nil
}

// statementTextForPositions exists only to keep the first output-name test's
// expected spelling visibly tied to the SQL it describes.
func statementTextForPositions() string {
	return `SELECT l.id AS key, r.id FROM left_side l JOIN right_side r ON l.id = r.id ` +
		`UNION ALL SELECT a, b FROM fallback ORDER BY key, "r.id" DESC`
}
