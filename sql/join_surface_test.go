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
	if got := cond.Left.Spec(); got != "id" {
		t.Fatalf("USING left path = %q, want id", got)
	}
	if got := cond.Right.Spec(); got != "id" {
		t.Fatalf("USING right path = %q, want id", got)
	}
}
