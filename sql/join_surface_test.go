package sql

import (
	"strings"
	"testing"
)

func TestRightJoinPreservesPublicASTSourceOrder(t *testing.T) {
	statement, err := ParseStatement(`
		SELECT u.name, o.total
		FROM users AS u
		RIGHT OUTER JOIN orders AS o ON u.id = o.user_id
		WHERE o.state = 'paid'
		ORDER BY u.name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(statement.Select.From) != 2 {
		t.Fatalf("FROM count = %d, want 2", len(statement.Select.From))
	}
	from := statement.Select.From
	if from[0].Alias != "u" || from[0].Join != JoinNone {
		t.Fatalf("driving table = %+v, want users/none", from[0])
	}
	if from[1].Alias != "o" || from[1].Join != JoinRight {
		t.Fatalf("joined table = %+v, want orders/right", from[1])
	}
	if from[1].On.Left.Source != 0 || from[1].On.Right.Source != 1 {
		t.Fatalf("ON sources = (%d, %d), want (0, 1)",
			from[1].On.Left.Source, from[1].On.Right.Source)
	}
	if statement.Select.Columns[0].Path.Source != 0 ||
		statement.Select.Columns[1].Path.Source != 1 {
		t.Fatalf("projection sources = (%d, %d), want (0, 1)",
			statement.Select.Columns[0].Path.Source,
			statement.Select.Columns[1].Path.Source)
	}
}

func TestRightJoinKeepsSharedDistinctOrderAliasStable(t *testing.T) {
	statement, err := ParseStatement(`
		SELECT DISTINCT r.id AS joined_id
		FROM lefts AS l
		RIGHT JOIN rights AS r ON l.id = r.id
		ORDER BY joined_id`)
	if err != nil {
		t.Fatal(err)
	}
	query := statement.Select
	projection := query.Columns[0].Path
	order := query.OrderBy[0].Path
	if projection != order {
		t.Fatal("ORDER BY alias no longer shares the resolved projection path")
	}
	if query.From[0].Alias != "l" || query.From[1].Alias != "r" ||
		query.From[1].Join != JoinRight || projection.Source != 1 {
		t.Fatalf("shared path = source %d over %s/%s (%d), want source 1 and RIGHT",
			projection.Source, query.From[0].Alias, query.From[1].Alias, query.From[1].Join)
	}
}

func TestUsingLowersToExplicitEquality(t *testing.T) {
	statement, err := ParseStatement(`
		SELECT u.id, o.total
		FROM users AS u
		JOIN orders AS o USING (id)`)
	if err != nil {
		t.Fatal(err)
	}
	cond := statement.Select.From[1].On
	if cond == nil || cond.Left.Source != 0 || cond.Right.Source != 1 {
		t.Fatalf("USING sources = %+v, want (0, 1)", cond)
	}
	if !cond.Using {
		t.Fatal("USING condition lost its merged-column semantics")
	}
	if got := cond.Left.Spec(); got != "id" {
		t.Fatalf("USING left path = %q, want id", got)
	}
	if got := cond.Right.Spec(); got != "id" {
		t.Fatalf("USING right path = %q, want id", got)
	}
}

func TestUsingResolvesOnlyItsUnqualifiedMergedColumn(t *testing.T) {
	statement, err := ParseStatement(`
		SELECT id, l.id, r.id, COUNT(*)
		FROM lefts AS l
		JOIN rights AS r USING (id)
		WHERE id >= 2
		GROUP BY id, l.id, r.id
		ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	query := statement.Select
	for _, path := range []*PathExpr{
		query.Columns[0].Path,
		query.Where.Path,
		query.GroupBy[0],
		query.OrderBy[0].Path,
	} {
		if path.Source != 0 || path.Spec() != "id" {
			t.Fatalf("merged USING path = source %d, %q; want source 0, id", path.Source, path.Spec())
		}
	}
	if query.Columns[1].Path.Source != 0 || query.Columns[2].Path.Source != 1 {
		t.Fatalf("qualified USING keys = (%d, %d), want (0, 1)",
			query.Columns[1].Path.Source, query.Columns[2].Path.Source)
	}
	if query.GroupBy[1].Source != 0 || query.GroupBy[2].Source != 1 {
		t.Fatalf("qualified GROUP BY keys = (%d, %d), want (0, 1)",
			query.GroupBy[1].Source, query.GroupBy[2].Source)
	}
}

func TestOuterUsingResolvesMergedColumnToPreservedSide(t *testing.T) {
	for _, tc := range []struct {
		name string
		join string
		kind JoinKind
	}{
		{"left", "LEFT JOIN", JoinLeft},
		{"right", "RIGHT JOIN", JoinRight},
		{"full", "FULL JOIN", JoinFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := ParseStatement(`
				SELECT id, l.id, r.id
				FROM lefts AS l ` + tc.join + ` rights AS r USING (id)
				WHERE id >= 2
				ORDER BY id`)
			if err != nil {
				t.Fatal(err)
			}
			query := statement.Select
			if query.From[0].Alias != "l" || query.From[1].Alias != "r" ||
				query.From[1].Join != tc.kind {
				t.Fatalf("sources = %s/%s (%d), want l/r (%d)",
					query.From[0].Alias, query.From[1].Alias, query.From[1].Join, tc.kind)
			}
			for _, path := range []*PathExpr{
				query.Columns[0].Path,
				query.Where.Path,
				query.OrderBy[0].Path,
			} {
				if path.MergedUsing != 1 || path.Spec() != "id" {
					t.Fatalf("merged USING path = source %d merge %d, %q; want merge 1, id",
						path.Source, path.MergedUsing, path.Spec())
				}
			}
			if query.Columns[1].Path.Source != 0 || query.Columns[2].Path.Source != 1 {
				t.Fatalf("qualified sources = %d/%d, want 0/1",
					query.Columns[1].Path.Source, query.Columns[2].Path.Source)
			}
		})
	}
}

func TestOnEqualityDoesNotCreateAnUnqualifiedMergedColumn(t *testing.T) {
	_, err := ParseStatement(`
		SELECT id
		FROM lefts AS l
		JOIN rights AS r ON l.id = r.id`)
	if err == nil {
		t.Fatal("equivalent ON equality made id unqualified; only USING merges the keys")
	}
}

func TestOrderByAliasTakesPrecedenceOverUsingColumn(t *testing.T) {
	statement, err := ParseStatement(`
		SELECT r.label AS id
		FROM lefts AS l
		JOIN rights AS r USING (id)
		ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	projection := statement.Select.Columns[0].Path
	order := statement.Select.OrderBy[0].Path
	if projection.Source != 1 || projection.Spec() != "label" || order != projection {
		t.Fatalf("ORDER BY alias = source %d, %q (%p), projection = source %d, %q (%p)",
			order.Source, order.Spec(), order,
			projection.Source, projection.Spec(), projection)
	}
}

func TestGeneralizedJoinASTCarriesKeysResidualsAndCross(t *testing.T) {
	statement, err := ParseStatement(`
		SELECT a.x
		FROM a
		FULL JOIN b ON a.k = b.k AND a.zone = b.zone AND a.enabled = TRUE
		CROSS JOIN c`)
	if err != nil {
		t.Fatal(err)
	}
	full := statement.Select.From[1]
	if full.Join != JoinFull || full.On == nil || len(full.On.Keys) != 2 || !full.On.Residual {
		t.Fatalf("FULL condition = %+v, want two keys plus a residual", full.On)
	}
	for i, key := range full.On.Keys {
		if key.Left.Source != 0 || key.Right.Source != 1 {
			t.Fatalf("key[%d] sources = %d/%d, want 0/1", i, key.Left.Source, key.Right.Source)
		}
	}
	cross := statement.Select.From[2]
	if cross.Join != JoinCross || cross.On != nil {
		t.Fatalf("CROSS entry = %+v, want a condition-free cross join", cross)
	}
}

func TestJoinConditionArenasOwnKeysAndUsingNames(t *testing.T) {
	var parser Parser
	var statement Statement
	if err := parser.ParseStatement(&statement,
		`SELECT a.x FROM a JOIN b USING (x, y)`); err != nil {
		t.Fatal(err)
	}
	cond := statement.Select.From[1].On
	if len(cond.Keys) != 2 || len(cond.UsingColumns) != 2 {
		t.Fatalf("condition = %+v, want two owned keys and names", cond)
	}
	parser.joinKeyScratch = append(parser.joinKeyScratch, JoinKeyCond{})
	parser.joinNameScratch = append(parser.joinNameScratch, "mutated")
	if cond.Keys[0].Left == nil || cond.UsingColumns[0] != "x" || cond.UsingColumns[1] != "y" {
		t.Fatalf("scratch reuse mutated condition: %+v", cond)
	}
}

func TestRepeatedUsingBindsAccumulatedMergedKey(t *testing.T) {
	statement, err := ParseStatement(`
		SELECT x, a.x, b.x, c.x
		FROM a JOIN b USING (x) FULL JOIN c USING (x)`)
	if err != nil {
		t.Fatal(err)
	}
	query := statement.Select
	if got := query.From[2].On.Keys[0].Left.MergedUsing; got != 1 {
		t.Fatalf("second USING left merge = %d, want first merge 1", got)
	}
	if got := query.Columns[0].Path.MergedUsing; got != 2 {
		t.Fatalf("unqualified projection merge = %d, want accumulated merge 2", got)
	}
}

func TestChainedUsingRejectsAmbiguousAccumulatedName(t *testing.T) {
	src := `SELECT a.x FROM a JOIN b ON a.y = b.y JOIN c USING (x)`
	_, err := ParseStatement(src)
	if err == nil || !strings.Contains(err.Error(), "ambiguous on the accumulated left relation") {
		t.Fatalf("ParseStatement = %v, want positioned accumulated-left ambiguity", err)
	}
	if parse, ok := err.(*ParseError); !ok || parse.Pos != strings.LastIndex(src, "x)") {
		t.Fatalf("ambiguity = %T %+v, want position %d", err, err, strings.LastIndex(src, "x)"))
	}
}

func TestOnMayReferenceOnlyTheAccumulatedSide(t *testing.T) {
	statement, err := ParseStatement(
		`SELECT a.x FROM a LEFT JOIN b ON a.enabled = TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	cond := statement.Select.From[1].On
	if len(cond.Keys) != 0 || !cond.Residual || cond.Expr == nil {
		t.Fatalf("old-side-only ON = %+v, want a residual nested-loop condition", cond)
	}
}

func TestOnBooleanConstantsAreResidualPredicates(t *testing.T) {
	for _, literal := range []string{"TRUE", "FALSE"} {
		statement, err := ParseStatement(
			`SELECT a.x FROM a LEFT JOIN b ON ` + literal)
		if err != nil {
			t.Fatalf("ON %s: %v", literal, err)
		}
		cond := statement.Select.From[1].On
		if cond.Expr == nil || cond.Expr.Kind != ExprConstant ||
			cond.Expr.Value.Kind != OperandBool || len(cond.Keys) != 0 || !cond.Residual {
			t.Fatalf("ON %s condition = %+v", literal, cond)
		}
	}
}

func TestNestedJoinKeyPositionsRebaseInUTF8Statement(t *testing.T) {
	src := `SELECT d.x FROM (/* é */ SELECT a.x FROM a JOIN b ON a.k = b.k AND a.y = b.y) AS d`
	statement, err := ParseStatement(src)
	if err != nil {
		t.Fatal(err)
	}
	keys := statement.Select.From[0].Query.From[1].On.Keys
	for i, marker := range []string{"a.k = b.k", "a.y = b.y"} {
		if got, want := keys[i].Pos, strings.Index(src, marker); got != want {
			t.Fatalf("key[%d] position = %d, want byte offset %d", i, got, want)
		}
	}
}
