package query

import (
	"reflect"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const recursiveSQLCaseFullFrame = `WITH RECURSIVE
seed_rows(node) AS MATERIALIZED (
	SELECT node FROM seeds WHERE node = ?
),
walk(node) AS (
	SELECT CASE CAST(node + ? AS NUMERIC)
		WHEN CAST(? AS NUMERIC) THEN CAST(? AS NUMERIC)
		ELSE CAST(node + ? AS NUMERIC)
	END AS node
	FROM seed_rows
	UNION ALL
	SELECT CASE
		WHEN CAST(e.dst AS NUMERIC) = CAST(? AS NUMERIC)
			AND CAST(? AS NUMERIC) = CAST(? AS NUMERIC) THEN CAST(? AS NUMERIC)
		WHEN CAST(? AS NUMERIC) = CAST(? AS NUMERIC) THEN e.dst + CAST(? AS NUMERIC)
		ELSE CAST(? AS NUMERIC)
	END AS node
	FROM walk AS w JOIN edges AS e ON w.node = e.src
	WHERE e.enabled = ?
)
SELECT node FROM walk WHERE node <= ? ORDER BY node`

func TestRecursiveSQLCaseFullFrameCloneRebaseAndReuse(t *testing.T) {
	_, snapshot := recursiveStatementDatabase(
		t, [][2]int{{0, 1}, {1, 2}, {2, 3}},
	)
	var parser sqlast.Parser
	var tree sqlast.SelectStmt
	if err := parser.Parse(&tree, recursiveSQLCaseFullFrame); err != nil {
		t.Fatal(err)
	}
	authored := &tree.With.CTEs[1].Recursive
	authoredAnchor := authored.Anchor.Columns[0].Scalar
	authoredTerm := authored.Term.Columns[0].Scalar
	questionPositions := recursiveSQLQuestionPositions(recursiveSQLCaseFullFrame)
	assertRecursiveSQLScalarParams(
		t, "authored anchor", authoredAnchor,
		[]int{0, 1, 2, 3}, questionPositions[1:5],
	)
	assertRecursiveSQLScalarParams(
		t, "authored term", authoredTerm,
		[]int{0, 1, 2, 3, 4, 5, 6, 7}, questionPositions[5:13],
	)

	statement, err := PrepareParsedRecursiveSQLStatement(
		recursiveSQLCaseFullFrame, &tree,
		RecursiveSQLStatementOptions{Limits: RecursiveCTELimits{
			MaxIterations: 16, MaxRows: 64, MaxBytes: -1,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			statement.Release()
		}
	}()
	prepared := statement.cteCatalog().defs[1].recursiveDefinition
	if prepared == nil {
		t.Fatal("recursive CASE definition was not installed")
	}
	clonedAnchor := prepared.anchorStmt.tree.Columns[0].Scalar
	clonedTerm := prepared.recursiveStmt.tree.Columns[0].Scalar
	assertRecursiveSQLCaseOwned(t, "anchor", authoredAnchor, clonedAnchor)
	assertRecursiveSQLCaseOwned(t, "term", authoredTerm, clonedTerm)
	assertRecursiveSQLScalarParams(
		t, "cloned anchor", clonedAnchor,
		[]int{1, 2, 3, 4}, questionPositions[1:5],
	)
	assertRecursiveSQLScalarParams(
		t, "cloned term", clonedTerm,
		[]int{5, 6, 7, 8, 9, 10, 11, 12}, questionPositions[5:13],
	)
	if got := prepared.recursiveStmt.tree.Where.Value.Ordinal; got != 13 {
		t.Fatalf("cloned recursive WHERE ordinal = %d, want 13", got)
	}
	if got := prepared.recursiveStmt.tree.Where.Value.Pos; got != questionPositions[13] {
		t.Fatalf("cloned recursive WHERE position = %d, want %d", got, questionPositions[13])
	}
	// Full-frame preparation must not rewrite the parser-owned authored terms.
	assertRecursiveSQLScalarParams(
		t, "authored anchor after prepare", authoredAnchor,
		[]int{0, 1, 2, 3}, questionPositions[1:5],
	)
	assertRecursiveSQLScalarParams(
		t, "authored term after prepare", authoredTerm,
		[]int{0, 1, 2, 3, 4, 5, 6, 7}, questionPositions[5:13],
	)

	var execution Exec
	first := recursiveSQLCaseRows(
		t, statement, &execution,
		FromDatabase(snapshot, statement.Collection()),
		[]any{
			int64(0), int64(0), int64(99), int64(77), int64(0),
			int64(2), int64(1), int64(1), int64(2),
			int64(1), int64(1), int64(0), int64(99),
			true, int64(2),
		},
	)
	if want := []string{"0", "1", "2"}; !reflect.DeepEqual(first, want) {
		t.Fatalf("first recursive CASE rows = %v, want %v", first, want)
	}
	second := recursiveSQLCaseRows(
		t, statement, &execution,
		FromDatabase(snapshot, statement.Collection()),
		[]any{
			int64(1), int64(0), int64(99), int64(77), int64(0),
			int64(3), int64(1), int64(1), int64(3),
			int64(1), int64(1), int64(0), int64(99),
			true, int64(3),
		},
	)
	if want := []string{"1", "2", "3"}; !reflect.DeepEqual(second, want) {
		t.Fatalf("reused recursive CASE rows = %v, want %v", second, want)
	}
	execution.Release()
	statement.Release()
	released = true

	// A released prepared statement no longer borrows the Parser arena. Rewind
	// it and prepare the same CASE shape again to cover parser reuse explicitly.
	if err := parser.Parse(&tree, recursiveSQLCaseFullFrame); err != nil {
		t.Fatal(err)
	}
	reused, err := PrepareParsedRecursiveSQLStatement(
		recursiveSQLCaseFullFrame, &tree,
		RecursiveSQLStatementOptions{Limits: RecursiveCTELimits{
			MaxIterations: 16, MaxRows: 64, MaxBytes: -1,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reused.Release()
	var reusedExecution Exec
	if got := recursiveSQLCaseRows(
		t, reused, &reusedExecution,
		FromDatabase(snapshot, reused.Collection()),
		[]any{
			int64(0), int64(0), int64(99), int64(77), int64(0),
			int64(2), int64(1), int64(1), int64(2),
			int64(1), int64(1), int64(0), int64(99),
			true, int64(2),
		},
	); !reflect.DeepEqual(got, []string{"0", "1", "2"}) {
		t.Fatalf("parser-reused recursive CASE rows = %v", got)
	}
	reusedExecution.Release()
}

func recursiveSQLCaseRows(
	tb testing.TB,
	statement *Statement,
	execution *Exec,
	source Source,
	args []any,
) []string {
	tb.Helper()
	cursor, err := statement.RunInto(execution, source, args)
	if err != nil {
		tb.Fatal(err)
	}
	rows := make([]string, 0, execution.Result.RowCount)
	for cursor.Next() {
		rows = append(rows, cursor.Cell(0).String())
	}
	return rows
}

func assertRecursiveSQLCaseOwned(
	tb testing.TB,
	name string,
	authored *sqlast.ScalarExpr,
	cloned *sqlast.ScalarExpr,
) {
	tb.Helper()
	if authored == nil || cloned == nil || authored == cloned ||
		len(authored.Whens) == 0 || len(cloned.Whens) != len(authored.Whens) ||
		&authored.Whens[0] == &cloned.Whens[0] {
		tb.Fatalf("%s recursive CASE retained parser-owned scalar/arm storage", name)
	}
	for i := range authored.Whens {
		left, right := &authored.Whens[i], &cloned.Whens[i]
		if left.Predicate != nil && left.Predicate == right.Predicate {
			tb.Fatalf("%s recursive CASE arm %d retained predicate identity", name, i)
		}
		if left.Predicate != nil && len(left.Predicate.Kids) != 0 &&
			(len(right.Predicate.Kids) != len(left.Predicate.Kids) ||
				&left.Predicate.Kids[0] == &right.Predicate.Kids[0] ||
				left.Predicate.Kids[0] == right.Predicate.Kids[0]) {
			tb.Fatalf("%s recursive CASE arm %d retained predicate child storage", name, i)
		}
		if left.Match != nil && left.Match == right.Match {
			tb.Fatalf("%s recursive CASE arm %d retained match identity", name, i)
		}
		if left.Result != nil && left.Result == right.Result {
			tb.Fatalf("%s recursive CASE arm %d retained result identity", name, i)
		}
	}
	if authored.Else != nil && authored.Else == cloned.Else {
		tb.Fatalf("%s recursive CASE retained ELSE identity", name)
	}
	authoredPath := firstRecursiveSQLScalarPath(authored)
	clonedPath := firstRecursiveSQLScalarPath(cloned)
	if authoredPath == nil || clonedPath == nil || authoredPath == clonedPath ||
		len(authoredPath.Segments) == 0 ||
		len(clonedPath.Segments) != len(authoredPath.Segments) ||
		&authoredPath.Segments[0] == &clonedPath.Segments[0] {
		tb.Fatalf("%s recursive CASE retained parser-owned path storage", name)
	}
}

func assertRecursiveSQLScalarParams(
	tb testing.TB,
	name string,
	scalar *sqlast.ScalarExpr,
	wantOrdinals []int,
	wantPositions []int,
) {
	tb.Helper()
	var operands []sqlast.Operand
	collectRecursiveSQLScalarParams(scalar, &operands)
	if len(operands) != len(wantOrdinals) || len(operands) != len(wantPositions) {
		tb.Fatalf("%s parameter count = %d, want %d", name, len(operands), len(wantOrdinals))
	}
	for i := range operands {
		if operands[i].Ordinal != wantOrdinals[i] || operands[i].Pos != wantPositions[i] {
			tb.Fatalf("%s parameter %d = ordinal/position %d/%d, want %d/%d",
				name, i, operands[i].Ordinal, operands[i].Pos,
				wantOrdinals[i], wantPositions[i])
		}
	}
}

func collectRecursiveSQLScalarParams(
	scalar *sqlast.ScalarExpr,
	dst *[]sqlast.Operand,
) {
	if scalar == nil {
		return
	}
	if scalar.Value.Kind == sqlast.OperandParam {
		*dst = append(*dst, scalar.Value)
	}
	collectRecursiveSQLScalarParams(scalar.Left, dst)
	collectRecursiveSQLScalarParams(scalar.Right, dst)
	for i := range scalar.Whens {
		arm := &scalar.Whens[i]
		collectRecursiveSQLExprParams(arm.Predicate, dst)
		collectRecursiveSQLScalarParams(arm.Match, dst)
		collectRecursiveSQLScalarParams(arm.Result, dst)
	}
	collectRecursiveSQLScalarParams(scalar.Else, dst)
}

func collectRecursiveSQLExprParams(expr *sqlast.Expr, dst *[]sqlast.Operand) {
	if expr == nil {
		return
	}
	if expr.Value.Kind == sqlast.OperandParam {
		*dst = append(*dst, expr.Value)
	}
	for i := range expr.List {
		if expr.List[i].Kind == sqlast.OperandParam {
			*dst = append(*dst, expr.List[i])
		}
	}
	collectRecursiveSQLScalarParams(expr.ScalarLeft, dst)
	collectRecursiveSQLScalarParams(expr.ScalarRight, dst)
	for i := range expr.Kids {
		collectRecursiveSQLExprParams(expr.Kids[i], dst)
	}
}

func firstRecursiveSQLScalarPath(scalar *sqlast.ScalarExpr) *sqlast.PathExpr {
	if scalar == nil {
		return nil
	}
	if scalar.Path != nil {
		return scalar.Path
	}
	if path := firstRecursiveSQLScalarPath(scalar.Left); path != nil {
		return path
	}
	if path := firstRecursiveSQLScalarPath(scalar.Right); path != nil {
		return path
	}
	for i := range scalar.Whens {
		arm := &scalar.Whens[i]
		if path := firstRecursiveSQLExprPath(arm.Predicate); path != nil {
			return path
		}
		if path := firstRecursiveSQLScalarPath(arm.Match); path != nil {
			return path
		}
		if path := firstRecursiveSQLScalarPath(arm.Result); path != nil {
			return path
		}
	}
	return firstRecursiveSQLScalarPath(scalar.Else)
}

func firstRecursiveSQLExprPath(expr *sqlast.Expr) *sqlast.PathExpr {
	if expr == nil {
		return nil
	}
	if expr.Path != nil {
		return expr.Path
	}
	if path := firstRecursiveSQLScalarPath(expr.ScalarLeft); path != nil {
		return path
	}
	if path := firstRecursiveSQLScalarPath(expr.ScalarRight); path != nil {
		return path
	}
	for i := range expr.Kids {
		if path := firstRecursiveSQLExprPath(expr.Kids[i]); path != nil {
			return path
		}
	}
	return nil
}

func recursiveSQLQuestionPositions(source string) []int {
	positions := make([]int, 0, 16)
	for i := range source {
		if source[i] == '?' {
			positions = append(positions, i)
		}
	}
	return positions
}
