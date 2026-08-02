package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestCreateViewASTOwnsNormalizedDefinition(t *testing.T) {
	const source = "CREATE VIEW \"recent orders\" (order_id, customer) AS\n" +
		"  SELECT o.id, o.customer FROM orders AS o WHERE o.state = 'open' ;  "
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Kind != KindCreateView || statement.CreateView == nil {
		t.Fatalf("statement = %#v, want CREATE VIEW", statement)
	}
	view := statement.CreateView
	if view.Name != "recent orders" || view.Materialized || view.Query == nil {
		t.Fatalf("view AST = %+v", *view)
	}
	if got, want := view.Columns, []string{"order_id", "customer"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %q, want %q", got, want)
	}
	if len(view.ColumnPos) != 2 || view.ColumnPos[0] != strings.Index(source, "order_id") ||
		view.ColumnPos[1] != strings.Index(source, "customer)") {
		t.Fatalf("column positions = %v", view.ColumnPos)
	}
	if view.Pos != strings.Index(source, `"recent orders"`) ||
		view.QueryPos != strings.Index(source, "SELECT") {
		t.Fatalf("positions = name %d query %d", view.Pos, view.QueryPos)
	}
	const normalized = "SELECT o.id, o.customer FROM orders AS o WHERE o.state = 'open'"
	if view.QuerySQL != normalized {
		t.Fatalf("normalized query = %q, want %q", view.QuerySQL, normalized)
	}
	if statement.Table() != "recent orders" || statement.Params() != 0 ||
		statement.ReturnsRows() || statement.Kind.String() != "CREATE VIEW" {
		t.Fatalf("statement contract = table %q params %d rows %t kind %q",
			statement.Table(), statement.Params(), statement.ReturnsRows(), statement.Kind)
	}
}

func TestDropViewASTAndRestrictContract(t *testing.T) {
	tests := []struct {
		source   string
		ifExists bool
		restrict bool
	}{
		{"DROP VIEW live_orders", false, false},
		{"DROP VIEW IF EXISTS live_orders", true, false},
		{"DROP VIEW IF EXISTS live_orders RESTRICT", true, true},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			statement, err := ParseStatement(test.source)
			if err != nil {
				t.Fatal(err)
			}
			if statement.Kind != KindDropView || statement.DropView == nil {
				t.Fatalf("statement = %#v, want DROP VIEW", statement)
			}
			drop := statement.DropView
			if drop.Name != "live_orders" || drop.IfExists != test.ifExists ||
				drop.Restrict != test.restrict {
				t.Fatalf("DROP VIEW AST = %+v", *drop)
			}
			if drop.Pos != strings.Index(test.source, "live_orders") {
				t.Fatalf("name position = %d", drop.Pos)
			}
			if test.restrict && drop.BehaviorPos != strings.Index(test.source, "RESTRICT") {
				t.Fatalf("behavior position = %d", drop.BehaviorPos)
			}
			if statement.Table() != "live_orders" || statement.Params() != 0 ||
				statement.ReturnsRows() || statement.Kind.String() != "DROP VIEW" {
				t.Fatalf("statement contract = %+v", statement)
			}
		})
	}
}

func TestViewParserReuseDoesNotRetainDefinitionState(t *testing.T) {
	var parser Parser
	var statement Statement
	if err := parser.ParseStatement(
		&statement,
		"CREATE VIEW first_view (renamed) AS SELECT id FROM first_table",
	); err != nil {
		t.Fatal(err)
	}
	if statement.CreateView == nil || len(statement.CreateView.Columns) != 1 {
		t.Fatalf("initial AST = %+v", statement)
	}
	if err := parser.ParseStatement(
		&statement,
		"CREATE VIEW second_view AS SELECT name FROM second_table",
	); err != nil {
		t.Fatal(err)
	}
	if statement.CreateView == nil || statement.CreateView.Name != "second_view" ||
		len(statement.CreateView.Columns) != 0 || statement.CreateView.Materialized {
		t.Fatalf("reused CREATE VIEW retained state: %+v", statement.CreateView)
	}
	if err := parser.ParseStatement(&statement, "DROP VIEW IF EXISTS second_view RESTRICT"); err != nil {
		t.Fatal(err)
	}
	if statement.CreateView != nil || statement.DropView == nil ||
		!statement.DropView.IfExists || !statement.DropView.Restrict {
		t.Fatalf("reused DROP VIEW AST = %+v", statement)
	}
	if err := parser.ParseStatement(&statement, "CREATE VIEW broken AS SELECT FROM"); err == nil {
		t.Fatal("invalid definition was accepted")
	}
	if statement != (Statement{}) {
		t.Fatalf("failed parse published partial AST: %+v", statement)
	}
}

func TestViewDefinitionsRejectParametersAtExactUTF8Position(t *testing.T) {
	const source = "CREATE VIEW café AS\nSELECT id FROM orders WHERE city = '東京' AND id = ?"
	_, err := ParseStatement(source)
	var unsupported *FeatureNotSupportedError
	var positioned *ParseError
	if !errors.As(err, &unsupported) || !errors.As(err, &positioned) {
		t.Fatalf("error = %T %v, want positioned feature refusal", err, err)
	}
	if want := strings.Index(source, "?"); positioned.Pos != want ||
		positioned.Line != 2 || positioned.Col != want-strings.Index(source, "SELECT")+1 {
		t.Fatalf("position = offset %d line %d col %d, want offset %d",
			positioned.Pos, positioned.Line, positioned.Col, want)
	}
}

func TestViewMaterializationAndCascadeAreStructurallyTypedRefusals(t *testing.T) {
	tests := []struct {
		source string
		at     string
	}{
		{
			"CREATE MATERIALIZED VIEW latest AS SELECT id FROM docs",
			"MATERIALIZED",
		},
		{"DROP MATERIALIZED VIEW IF EXISTS latest RESTRICT", "MATERIALIZED"},
		{"DROP VIEW latest CASCADE", "CASCADE"},
		{"REFRESH MATERIALIZED VIEW latest", "REFRESH"},
		{"REFRESH MATERIALIZED VIEW CONCURRENTLY public.latest WITH DATA", "REFRESH"},
		{"REFRESH MATERIALIZED VIEW latest WITH NO DATA", "REFRESH"},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			_, err := ParseStatement(test.source)
			var unsupported *FeatureNotSupportedError
			var positioned *ParseError
			if !errors.As(err, &unsupported) || !errors.As(err, &positioned) {
				t.Fatalf("error = %T %v, want positioned feature refusal", err, err)
			}
			if positioned.Pos != strings.Index(test.source, test.at) {
				t.Fatalf("position = %d, want %d", positioned.Pos, strings.Index(test.source, test.at))
			}
		})
	}

	for _, source := range []string{
		"CREATE MATERIALIZED latest AS SELECT id FROM docs",
		"CREATE MATERIALIZED VIEW latest AS SELECT FROM docs",
		"DROP MATERIALIZED latest",
		"DROP MATERIALIZED VIEW",
		"DROP VIEW latest CASCADE trailing",
		"REFRESH latest",
		"REFRESH MATERIALIZED latest",
		"REFRESH MATERIALIZED VIEW",
		"REFRESH MATERIALIZED VIEW latest WITH",
		"REFRESH MATERIALIZED VIEW latest WITH MAYBE DATA",
	} {
		t.Run("malformed/"+source, func(t *testing.T) {
			_, err := ParseStatement(source)
			var unsupported *FeatureNotSupportedError
			if err == nil || errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want syntax error", err, err)
			}
		})
	}
}

func TestViewGrammarRefusesAmbiguousOrUnboundedNames(t *testing.T) {
	tests := []struct {
		source string
		want   string
	}{
		{"CREATE VIEW AS SELECT id FROM docs", "view name"},
		{"CREATE VIEW public.latest AS SELECT id FROM docs", "qualified"},
		{"CREATE VIEW latest () AS SELECT id FROM docs", "may not be empty"},
		{"CREATE VIEW latest (id, id) AS SELECT id FROM docs", "declared twice"},
		{"DROP VIEW public.latest", "qualified"},
		{"DROP VIEW IF latest", "EXISTS"},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			_, err := ParseStatement(test.source)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
