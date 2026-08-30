package sql

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCommaFromLowersExactlyToCrossJoin(t *testing.T) {
	comma, err := Parse(`SELECT a.id, b.id, c.id FROM a, b, c`)
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := Parse(`SELECT a.id, b.id, c.id FROM a CROSS JOIN b CROSS JOIN c`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dumpStmt(comma), dumpStmt(explicit); got != want {
		t.Fatalf("comma FROM AST differs from explicit CROSS JOIN:\n got %s\nwant %s", got, want)
	}
	wantJoins := []JoinKind{JoinNone, JoinCross, JoinCross}
	for i := range wantJoins {
		if comma.From[i].Join != wantJoins[i] || comma.From[i].On != nil {
			t.Fatalf("comma FROM item %d = join %d, condition %+v; want join %d without a condition",
				i, comma.From[i].Join, comma.From[i].On, wantJoins[i])
		}
	}
}

func TestCommaAfterJoinedTablePreservesPostgreSQLGrouping(t *testing.T) {
	comma, err := Parse(`SELECT a.id, b.id, c.id FROM a JOIN b ON a.k = b.k, c`)
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := Parse(`SELECT a.id, b.id, c.id FROM a JOIN b ON a.k = b.k CROSS JOIN c`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dumpStmt(comma), dumpStmt(explicit); got != want {
		t.Fatalf("joined-table comma AST differs from explicit tree:\n got %s\nwant %s", got, want)
	}
	wantJoins := []JoinKind{JoinNone, JoinInner, JoinCross}
	for i := range wantJoins {
		if comma.From[i].Join != wantJoins[i] {
			t.Fatalf("FROM item %d join = %d, want %d", i, comma.From[i].Join, wantJoins[i])
		}
	}
}

func TestCommaLateralMatchesExplicitCrossJoinLateral(t *testing.T) {
	const commaSQL = `SELECT a.id, d.id FROM accounts a, LATERAL (` +
		`SELECT i.id FROM items i WHERE i.owner = a.id) d`
	const explicitSQL = `SELECT a.id, d.id FROM accounts a CROSS JOIN LATERAL (` +
		`SELECT i.id FROM items i WHERE i.owner = a.id) d`
	comma, err := Parse(commaSQL)
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := Parse(explicitSQL)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dumpStmt(comma), dumpStmt(explicit); got != want {
		t.Fatalf("comma LATERAL AST differs from explicit CROSS JOIN LATERAL:\n got %s\nwant %s", got, want)
	}
	ref := &comma.From[1]
	if ref.Join != JoinCross || ref.Lateral == nil || ref.Lateral.Decorrelated ||
		len(ref.Lateral.Bindings) != 1 || ref.Lateral.Bindings[0].Source != 0 {
		t.Fatalf("comma LATERAL metadata = %+v", ref)
	}
}

func TestExplicitJoinAfterCommaRefusesWrongPrecedence(t *testing.T) {
	for _, test := range []struct {
		name   string
		sql    string
		marker string
	}{
		{"inner", `SELECT a.id FROM a, b JOIN c ON b.k = c.k`, "JOIN"},
		{"inner may not see prior comma item", `SELECT a.id FROM a, b JOIN c ON a.k = c.k`, "JOIN"},
		{"typed inner", `SELECT a.id FROM a, b INNER JOIN c ON b.k = c.k`, "INNER"},
		{"left", `SELECT a.id FROM a, b LEFT JOIN c ON b.k = c.k`, "LEFT"},
		{"right multiplicity", `SELECT a.id FROM a, b RIGHT JOIN c ON b.k = c.k`, "RIGHT"},
		{"full multiplicity", `SELECT a.id FROM a, b FULL JOIN c ON b.k = c.k`, "FULL"},
		{"cross lateral scope", `SELECT a.id FROM a, b CROSS JOIN c`, "CROSS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.sql)
			if err == nil {
				t.Fatal("Parse succeeded with an unrepresentable PostgreSQL join tree")
			}
			var unsupported *FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want *FeatureNotSupportedError", err, err)
			}
			if got, want := unsupported.Pos, strings.Index(test.sql, test.marker); got != want {
				t.Fatalf("error position = %d, want %d", got, want)
			}
			if !strings.Contains(unsupported.Msg, "right-hand join grouping") {
				t.Fatalf("error = %q, want the PostgreSQL precedence boundary", unsupported.Msg)
			}
		})
	}
}

func TestCommaFromKeepsAliasResolutionAndItemBound(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want string
	}{
		{"duplicate alias", `SELECT x.id FROM a x, b x`, "declared twice"},
		{"ambiguous unqualified path", `SELECT id FROM a, b`, "qualify it"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.sql)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse = %v, want an error containing %q", err, test.want)
			}
		})
	}

	var src strings.Builder
	src.WriteString(`SELECT t0.id FROM t0`)
	for i := 1; i <= maxClauseItems; i++ {
		fmt.Fprintf(&src, ",t%d", i)
	}
	_, err := Parse(src.String())
	if err == nil || !strings.Contains(err.Error(), "may join at most") {
		t.Fatalf("Parse of %d comma FROM items = %v, want the clause-item bound",
			maxClauseItems+1, err)
	}
}
