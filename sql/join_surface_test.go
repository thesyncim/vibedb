package sql

import "testing"

func TestRightJoinNormalizesToLeftJoin(t *testing.T) {
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
	if from[0].Alias != "o" || from[0].Join != JoinNone {
		t.Fatalf("normalized driving table = %+v, want orders/none", from[0])
	}
	if from[1].Alias != "u" || from[1].Join != JoinLeft {
		t.Fatalf("normalized joined table = %+v, want users/left", from[1])
	}
	if from[1].On.Left.Source != 0 || from[1].On.Right.Source != 1 {
		t.Fatalf("normalized ON sources = (%d, %d), want (0, 1)",
			from[1].On.Left.Source, from[1].On.Right.Source)
	}
	if statement.Select.Columns[0].Path.Source != 1 ||
		statement.Select.Columns[1].Path.Source != 0 {
		t.Fatalf("normalized projection sources = (%d, %d), want (1, 0)",
			statement.Select.Columns[0].Path.Source,
			statement.Select.Columns[1].Path.Source)
	}
}

func TestRightJoinRemapsSharedDistinctOrderAliasOnce(t *testing.T) {
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
	if query.From[0].Alias != "r" || projection.Source != 0 {
		t.Fatalf("normalized shared path = source %d over %s, want source 0 over rights",
			projection.Source, query.From[0].Alias)
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
		name      string
		join      string
		preserved string
		nullable  string
	}{
		{"left", "LEFT JOIN", "l", "r"},
		{"right", "RIGHT JOIN", "r", "l"},
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
			if query.From[0].Alias != tc.preserved || query.From[1].Alias != tc.nullable ||
				query.From[1].Join != JoinLeft {
				t.Fatalf("normalized sources = %s/%s (%d), want %s/%s (LEFT)",
					query.From[0].Alias, query.From[1].Alias, query.From[1].Join,
					tc.preserved, tc.nullable)
			}
			for _, path := range []*PathExpr{
				query.Columns[0].Path,
				query.Where.Path,
				query.OrderBy[0].Path,
			} {
				if path.Source != 0 || path.Spec() != "id" {
					t.Fatalf("merged USING path = source %d, %q; want preserved source 0, id",
						path.Source, path.Spec())
				}
			}
			qualified := map[string]int{
				"l": query.Columns[1].Path.Source,
				"r": query.Columns[2].Path.Source,
			}
			if qualified[tc.preserved] != 0 || qualified[tc.nullable] != 1 {
				t.Fatalf("qualified sources = %#v after normalization", qualified)
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
