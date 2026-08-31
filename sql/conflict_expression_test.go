package sql

import (
	"strings"
	"testing"
)

func TestConflictExpressionsRetainExplicitCurrentAndExcludedNamespaces(t *testing.T) {
	const source = `INSERT INTO metrics VALUES (?) ON CONFLICT DO UPDATE SET
		total = metrics.base + EXCLUDED.delta * ?,
		label = CASE WHEN "excluded".active = TRUE THEN metrics.label || EXCLUDED.label ELSE metrics.label END`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	assignments := statement.Insert.OnConflictUpdate.Assignments
	if len(assignments) != 2 || statement.Params() != 2 {
		t.Fatalf("assignments=%d params=%d, want 2,2", len(assignments), statement.Params())
	}
	for i := range assignments {
		if assignments[i].Expr == nil || assignments[i].Value.Kind != OperandExpression {
			t.Fatalf("assignment %d = %+v, want computed expression marker", i, assignments[i])
		}
	}

	first := assignments[0].Expr
	if first.Kind != ScalarBinary || first.Op != ScalarAdd ||
		first.Left == nil || first.Left.Path == nil || first.Left.Path.Source != 0 ||
		first.Left.Path.Spec() != "base" || first.Right == nil ||
		first.Right.Kind != ScalarBinary || first.Right.Op != ScalarMultiply ||
		first.Right.Left == nil || first.Right.Left.Path == nil ||
		first.Right.Left.Path.Source != 1 || first.Right.Left.Path.Spec() != "delta" ||
		first.Right.Right == nil || first.Right.Right.Value.Kind != OperandParam ||
		first.Right.Right.Value.Ordinal != 1 {
		t.Fatalf("first conflict expression = %s", dumpAny(statement))
	}
	second := assignments[1].Expr
	if second.Kind != ScalarCase || len(second.Whens) != 1 ||
		second.Whens[0].Predicate == nil ||
		second.Whens[0].Predicate.Path == nil ||
		second.Whens[0].Predicate.Path.Source != 1 ||
		second.Whens[0].Predicate.Path.Spec() != "active" {
		t.Fatalf("CASE conflict expression = %s", dumpAny(statement))
	}
}

func TestConflictAssignmentDirectFastPathsRemainDirect(t *testing.T) {
	statement, err := ParseStatement(`INSERT INTO metrics VALUES (?)
		ON CONFLICT DO UPDATE SET
		a = EXCLUDED.a, b = ?, c = NULL, d = 'x', e = BOOL 't'`)
	if err != nil {
		t.Fatal(err)
	}
	assignments := statement.Insert.OnConflictUpdate.Assignments
	want := []OperandKind{
		OperandExcluded, OperandParam, OperandNull, OperandString, OperandBool,
	}
	if len(assignments) != len(want) {
		t.Fatalf("assignments=%d, want %d", len(assignments), len(want))
	}
	for i := range want {
		if assignments[i].Expr != nil || assignments[i].Value.Kind != want[i] {
			t.Fatalf("assignment %d = %+v, want direct kind %d", i, assignments[i], want[i])
		}
	}
	if assignments[0].Value.Text != "a" || assignments[1].Value.Ordinal != 1 {
		t.Fatalf("direct operands = %+v", assignments)
	}
}

func TestConflictExpressionBareColumnsRemainCatalogDeferred(t *testing.T) {
	const source = `INSERT INTO metrics VALUES (?) ON CONFLICT DO UPDATE SET n = n + 1`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	path := statement.Insert.OnConflictUpdate.Assignments[0].Expr.Left.Path
	want := strings.LastIndex(source, "n +")
	if path == nil || path.Source != ConflictUnresolvedSource ||
		path.Spec() != "n" || path.Pos != want {
		t.Fatalf("deferred path = %+v, want source %d name n at %d",
			path, ConflictUnresolvedSource, want)
	}
}

func TestConflictExpressionQuotedTargetKeepsBarePathDeferred(t *testing.T) {
	const source = `INSERT INTO "order items" VALUES (?) ON CONFLICT DO UPDATE SET n = n + 1`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	path := statement.Insert.OnConflictUpdate.Assignments[0].Expr.Left.Path
	if path == nil || path.Source != ConflictUnresolvedSource ||
		path.Pos != strings.LastIndex(source, "n +") {
		t.Fatalf("deferred quoted-target path = %+v", path)
	}
}

func TestConflictExpressionNamespacesStayBoundAcrossParserReuse(t *testing.T) {
	var parser Parser
	var statement Statement
	for _, source := range []string{
		`INSERT INTO metrics VALUES (?) ON CONFLICT DO UPDATE SET n = metrics.n + EXCLUDED.n`,
		`INSERT INTO metrics VALUES (?) ON CONFLICT DO UPDATE SET n = EXCLUDED.n`,
		`INSERT INTO metrics VALUES (?) ON CONFLICT DO NOTHING`,
		`INSERT INTO metrics VALUES (?) ON CONFLICT DO UPDATE SET n = metrics.n - EXCLUDED.n`,
	} {
		if err := parser.ParseStatement(&statement, source); err != nil {
			t.Fatalf("ParseStatement(%q): %v", source, err)
		}
	}
	assignment := statement.Insert.OnConflictUpdate.Assignments[0]
	if assignment.Expr == nil || assignment.Expr.Left.Path.Source != 0 ||
		assignment.Expr.Right.Path.Source != 1 {
		t.Fatalf("reused parser expression = %s", dumpAny(&statement))
	}
}
