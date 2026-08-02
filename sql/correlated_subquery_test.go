package sql

import (
	"errors"
	"strings"
	"testing"
	"unsafe"
)

const correlatedExistsSQL = `SELECT o.id FROM orders o WHERE EXISTS (` +
	`SELECT o.id FROM items i WHERE i.owner = o.id AND i.tenant = o.tenant AND o.id = i.account` +
	`)`

const multipleCorrelationCapturesSQL = `SELECT a.id, d.item, e.job
	FROM accounts a
	CROSS JOIN LATERAL (
		SELECT i.id AS item FROM items i WHERE i.owner = a.id
	) d
	CROSS JOIN LATERAL (
		SELECT j.id AS job FROM jobs j
		WHERE j.account = a.tenant AND j.item = d.item
	) e
	WHERE EXISTS (
		SELECT x.id FROM audits x WHERE x.account = a.id
	) AND EXISTS (
		SELECT y.id FROM flags y WHERE y.item = d.item AND y.job = e.job
	)`

func TestCorrelatedPredicateSubqueryCapturesExactOccurrences(t *testing.T) {
	stmt, err := Parse(correlatedExistsSQL)
	if err != nil {
		t.Fatal(err)
	}
	sub := stmt.Where.Subquery
	if sub == nil || sub.Correlation == nil || sub.Correlation.Decorrelated {
		t.Fatalf("correlated EXISTS metadata = %+v", sub)
	}
	spec := sub.Correlation
	if spec.Pos != strings.Index(correlatedExistsSQL, "SELECT o.id FROM items") {
		t.Fatalf("correlation position = %d", spec.Pos)
	}
	if len(spec.Bindings) != 2 || cap(spec.Bindings) != len(spec.Bindings) ||
		len(spec.References) != 4 || cap(spec.References) != len(spec.References) {
		t.Fatalf("correlation shape = bindings %d/%d, references %d/%d",
			len(spec.Bindings), cap(spec.Bindings), len(spec.References), cap(spec.References))
	}
	innerStart := strings.Index(correlatedExistsSQL, "SELECT o.id FROM items")
	idFirst := innerStart + strings.Index(correlatedExistsSQL[innerStart:], "o.id")
	tenantFirst := strings.Index(correlatedExistsSQL, "o.tenant")
	if got := &spec.Bindings[0]; got.Depth != 1 || got.Source != 0 ||
		(&PathExpr{Segments: got.Segments}).Spec() != "id" || got.Pos != idFirst {
		t.Fatalf("id binding = %+v", got)
	}
	if got := &spec.Bindings[1]; got.Depth != 1 || got.Source != 0 ||
		(&PathExpr{Segments: got.Segments}).Spec() != "tenant" || got.Pos != tenantFirst {
		t.Fatalf("tenant binding = %+v", got)
	}
	where := sub.Where
	if where == nil || where.Kind != ExprAnd || len(where.Kids) != 3 {
		t.Fatalf("inner WHERE = %+v", where)
	}
	assertCorrelationReference(t, spec, sub.Columns[0].Path, 0)
	assertCorrelationReference(t, spec, where.Kids[0].RightPath, 0)
	assertCorrelationReference(t, spec, where.Kids[1].RightPath, 1)
	assertCorrelationReference(t, spec, where.Kids[2].Path, 0)
	for _, local := range []*PathExpr{
		where.Kids[0].Path, where.Kids[1].Path, where.Kids[2].RightPath,
	} {
		if _, ok := correlationReferenceBinding(spec, local); ok {
			t.Fatalf("local path %+v was marked correlated", local)
		}
	}
	checkStatementInvariants(t, stmt)
}

func TestCorrelatedPredicateSubqueryLeafKindsAndCompleteOuterFrom(t *testing.T) {
	tests := []struct {
		name     string
		src      string
		rootKind ExprKind
		leafKind ExprKind
	}{
		{
			name: "exists",
			src: `SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT i.id FROM inner_docs i WHERE i.owner = o.id)`,
			rootKind: ExprExists,
			leafKind: ExprExists,
		},
		{
			name: "not exists",
			src: `SELECT o.id FROM outer_docs o WHERE NOT EXISTS (` +
				`SELECT i.id FROM inner_docs i WHERE i.owner = o.id)`,
			rootKind: ExprNot,
			leafKind: ExprExists,
		},
		{
			name: "in",
			src: `SELECT o.id FROM outer_docs o WHERE o.id IN (` +
				`SELECT i.id FROM inner_docs i WHERE i.owner = o.id)`,
			rootKind: ExprIn,
			leafKind: ExprIn,
		},
		{
			name: "scalar",
			src: `SELECT o.id FROM outer_docs o WHERE o.id = (` +
				`SELECT i.id FROM inner_docs i WHERE i.owner = o.id)`,
			rootKind: ExprCompare,
			leafKind: ExprCompare,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := Parse(test.src)
			if err != nil {
				t.Fatal(err)
			}
			if stmt.Where.Kind != test.rootKind {
				t.Fatalf("root kind = %d, want %d", stmt.Where.Kind, test.rootKind)
			}
			leaf := stmt.Where
			if leaf.Kind == ExprNot {
				leaf = leaf.Kids[0]
			}
			if leaf.Kind != test.leafKind || leaf.Subquery == nil {
				t.Fatalf("subquery leaf = %+v", leaf)
			}
			spec := leaf.Subquery.Correlation
			if spec == nil || len(spec.Bindings) != 1 || len(spec.References) != 1 {
				t.Fatalf("metadata = %+v", spec)
			}
			if spec.Bindings[0].Pos != strings.LastIndex(test.src, "o.id") ||
				spec.References[0].Path.Pos != spec.Bindings[0].Pos {
				t.Fatalf("positions = binding %d, reference %d",
					spec.Bindings[0].Pos, spec.References[0].Path.Pos)
			}
		})
	}

	const joined = `SELECT a.id FROM accounts a JOIN regions r ON a.region = r.id ` +
		`WHERE EXISTS (SELECT i.id FROM items i ` +
		`WHERE i.account = a.id AND i.region = r.id AND i.account = a.id)`
	stmt, err := Parse(joined)
	if err != nil {
		t.Fatal(err)
	}
	spec := stmt.Where.Subquery.Correlation
	if spec == nil || len(spec.Bindings) != 2 || len(spec.References) != 3 {
		t.Fatalf("complete-FROM correlation = %+v", spec)
	}
	if spec.Bindings[0].Source != 0 || spec.Bindings[1].Source != 1 ||
		spec.References[0].Binding != 0 || spec.References[1].Binding != 1 ||
		spec.References[2].Binding != 0 {
		t.Fatalf("complete-FROM bindings/references = %+v / %+v",
			spec.Bindings, spec.References)
	}
	if spec.Bindings[0].Pos != strings.Index(joined, "a.id AND") ||
		spec.Bindings[1].Pos != strings.Index(joined, "r.id AND") {
		t.Fatalf("complete-FROM binding positions = %+v", spec.Bindings)
	}
}

func TestMultiplePredicateCorrelationCapturesRetainDistinctPublishedSpecs(t *testing.T) {
	const src = `SELECT o.id FROM outer_docs o WHERE EXISTS (` +
		`SELECT i.id FROM inner_a i WHERE i.owner = o.id) AND EXISTS (` +
		`SELECT j.id FROM inner_b j WHERE j.tenant = o.tenant)`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Where == nil || stmt.Where.Kind != ExprAnd || len(stmt.Where.Kids) != 2 {
		t.Fatalf("outer predicate = %+v", stmt.Where)
	}
	first := stmt.Where.Kids[0].Subquery.Correlation
	second := stmt.Where.Kids[1].Subquery.Correlation
	if first == nil || second == nil || first == second {
		t.Fatalf("correlation specs alias: first=%p second=%p", first, second)
	}
	if len(first.Bindings) != 1 || len(first.References) != 1 ||
		(&PathExpr{Segments: first.Bindings[0].Segments}).Spec() != "id" ||
		first.Bindings[0].Pos != strings.Index(src, "o.id) AND") {
		t.Fatalf("first correlation was overwritten: %+v", first)
	}
	if len(second.Bindings) != 1 || len(second.References) != 1 ||
		(&PathExpr{Segments: second.Bindings[0].Segments}).Spec() != "tenant" ||
		second.Bindings[0].Pos != strings.LastIndex(src, "o.tenant") {
		t.Fatalf("second correlation = %+v", second)
	}
	if first.References[0].Path != stmt.Where.Kids[0].Subquery.Where.RightPath ||
		second.References[0].Path != stmt.Where.Kids[1].Subquery.Where.RightPath {
		t.Fatalf("reference occurrence identity crossed captures: first=%+v second=%+v",
			first.References, second.References)
	}
	checkStatementInvariants(t, stmt)
}

func TestPredicateAndMultipleLateralCapturesDoNotAliasAcrossReuse(t *testing.T) {
	assertCaptureSet := func(t *testing.T, stmt *SelectStmt) {
		t.Helper()
		if len(stmt.From) != 3 || stmt.Where == nil || stmt.Where.Kind != ExprAnd ||
			len(stmt.Where.Kids) != 2 {
			t.Fatalf("mixed capture statement shape = FROM %d WHERE %+v", len(stmt.From), stmt.Where)
		}
		specs := []*CorrelationSpec{
			stmt.From[1].Lateral,
			stmt.From[2].Lateral,
			stmt.Where.Kids[0].Subquery.Correlation,
			stmt.Where.Kids[1].Subquery.Correlation,
		}
		for i, spec := range specs {
			if spec == nil || len(spec.Bindings) == 0 || len(spec.References) == 0 {
				t.Fatalf("capture %d = %+v", i, spec)
			}
			for prior := 0; prior < i; prior++ {
				if spec == specs[prior] {
					t.Fatalf("capture %d aliases capture %d at %p", i, prior, spec)
				}
			}
		}
		wantBindings := [][]string{
			{"id"},
			{"tenant", "item"},
			{"id"},
			{"item", "job"},
		}
		for i, want := range wantBindings {
			if len(specs[i].Bindings) != len(want) {
				t.Fatalf("capture %d bindings = %+v, want %v", i, specs[i].Bindings, want)
			}
			for j := range want {
				if got := (&PathExpr{Segments: specs[i].Bindings[j].Segments}).Spec(); got != want[j] {
					t.Fatalf("capture %d binding %d = %q, want %q", i, j, got, want[j])
				}
			}
		}
	}

	var parser Parser
	var stmt SelectStmt
	if err := parser.Parse(&stmt, multipleCorrelationCapturesSQL); err != nil {
		t.Fatal(err)
	}
	assertCaptureSet(t, &stmt)

	malformed := multipleCorrelationCapturesSQL[:strings.LastIndex(multipleCorrelationCapturesSQL, ")")]
	if err := parser.Parse(&stmt, malformed); err == nil {
		t.Fatal("unterminated final correlated EXISTS was accepted")
	}
	if stmt.Correlation != nil || len(stmt.Columns) != 0 || len(stmt.From) != 0 || stmt.Where != nil {
		t.Fatalf("failed mixed-capture parse retained AST state: %+v", stmt)
	}
	if err := parser.Parse(&stmt, multipleCorrelationCapturesSQL); err != nil {
		t.Fatalf("reuse after mixed-capture error: %v", err)
	}
	assertCaptureSet(t, &stmt)

	if got := testing.AllocsPerRun(200, func() {
		if err := parser.Parse(&stmt, multipleCorrelationCapturesSQL); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("warmed mixed-capture parse allocated %.1f times, want zero", got)
	}
}

func TestCorrelatedPredicateSubqueryShadowingAndCTEPropagation(t *testing.T) {
	for _, src := range []string{
		`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
			`SELECT o.id FROM inner_docs o WHERE o.owner = o.id)`,
		`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
			`WITH o AS (SELECT id FROM shadow_docs) ` +
			`SELECT o.id FROM o WHERE o.id = o.id)`,
		`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
			`SELECT i.id FROM inner_docs i WHERE i.owner = i.id)`,
	} {
		stmt, err := Parse(src)
		if err != nil {
			t.Fatalf("Parse(%q): %v", src, err)
		}
		sub := predicateSubquery(stmt.Where)
		if sub == nil || sub.Correlation != nil {
			t.Fatalf("shadowed/uncorrelated subquery metadata = %+v", sub)
		}
	}

	const cteCapture = `SELECT o.id FROM outer_docs o WHERE EXISTS (` +
		`WITH picked AS (SELECT o.id FROM seed s) ` +
		`SELECT picked.id FROM picked)`
	stmt, err := Parse(cteCapture)
	if err != nil {
		t.Fatal(err)
	}
	sub := stmt.Where.Subquery
	if sub.Correlation == nil || len(sub.Correlation.Bindings) != 1 ||
		len(sub.Correlation.References) != 1 {
		t.Fatalf("CTE correlation = %+v", sub.Correlation)
	}
	path := sub.With.CTEs[0].Query.Columns[0].Path
	assertCorrelationReference(t, sub.Correlation, path, 0)
	if sub.Correlation.Bindings[0].Pos != strings.Index(cteCapture, "o.id FROM seed") {
		t.Fatalf("CTE binding position = %d", sub.Correlation.Bindings[0].Pos)
	}
}

func TestCorrelatedPredicateSubqueryUnsupportedShapesRemainAnnotated(t *testing.T) {
	tests := []struct {
		name string
		src  string
	}{
		{
			name: "in",
			src: `SELECT o.id FROM outer_docs o WHERE o.id IN (` +
				`SELECT i.id FROM inner_docs i WHERE i.owner = o.id)`,
		},
		{
			name: "scalar comparison",
			src: `SELECT o.id FROM outer_docs o WHERE o.id = (` +
				`SELECT i.id FROM inner_docs i WHERE i.owner = o.id)`,
		},
		{
			name: "or branch",
			src: `SELECT o.id FROM outer_docs o WHERE o.live = TRUE OR EXISTS (` +
				`SELECT 1 FROM inner_docs i WHERE i.owner = o.id)`,
		},
		{
			name: "not exists",
			src: `SELECT o.id FROM outer_docs o WHERE NOT EXISTS (` +
				`SELECT 1 FROM inner_docs i WHERE i.owner = o.id)`,
		},
		{
			name: "join on",
			src: `SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT i.id FROM inner_docs i JOIN tags t ON t.owner = o.id)`,
		},
		{
			name: "set expression",
			src: `SELECT o.id FROM outer_docs o WHERE EXISTS ((` +
				`SELECT i.id FROM inner_docs i WHERE i.owner = o.id) UNION ALL ` +
				`SELECT j.id FROM archive_docs j)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := Parse(test.src)
			if err != nil {
				t.Fatal(err)
			}
			sub := predicateSubquery(stmt.Where)
			if sub == nil || sub.Correlation == nil ||
				len(sub.Correlation.Bindings) == 0 || sub.Correlation.Decorrelated {
				t.Fatalf("correlation metadata = %+v", sub)
			}
			if got := sub.Correlation.Bindings[0].Pos; got != strings.LastIndex(test.src, "o.id") {
				t.Fatalf("first binding position = %d, want final o.id at %d",
					got, strings.LastIndex(test.src, "o.id"))
			}
			checkStatementInvariants(t, stmt)
		})
	}
}

func TestNestedPredicateCorrelationPreservesLexicalDepth(t *testing.T) {
	const src = `SELECT o.id FROM outer_docs o WHERE EXISTS (` +
		`SELECT i.id FROM inner_docs i WHERE EXISTS (` +
		`SELECT x.id FROM deep_docs x WHERE x.owner = o.id AND x.parent = i.id))`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	middle := stmt.Where.Subquery
	deep := middle.Where.Subquery
	if middle.Correlation == nil || len(middle.Correlation.Bindings) != 1 ||
		len(middle.Correlation.References) != 0 {
		t.Fatalf("transitive middle correlation = %+v", middle.Correlation)
	}
	if got := middle.Correlation.Bindings[0]; got.Depth != 1 || got.Source != 0 ||
		(&PathExpr{Segments: got.Segments}).Spec() != "id" {
		t.Fatalf("middle binding = %+v", got)
	}
	if deep.Correlation == nil || len(deep.Correlation.Bindings) != 2 ||
		len(deep.Correlation.References) != 2 {
		t.Fatalf("deep correlation = %+v", deep.Correlation)
	}
	if got := deep.Correlation.Bindings[0]; got.Depth != 2 || got.Source != 0 ||
		(&PathExpr{Segments: got.Segments}).Spec() != "id" {
		t.Fatalf("deep grandparent binding = %+v", got)
	}
	if got := deep.Correlation.Bindings[1]; got.Depth != 1 || got.Source != 0 ||
		(&PathExpr{Segments: got.Segments}).Spec() != "id" {
		t.Fatalf("deep parent binding = %+v", got)
	}
	assertCorrelationReference(t, deep.Correlation, deep.Where.Kids[0].RightPath, 0)
	assertCorrelationReference(t, deep.Correlation, deep.Where.Kids[1].RightPath, 1)
	checkStatementInvariants(t, stmt)
}

func TestPredicateCorrelationUTF8PositionRebasing(t *testing.T) {
	const src = `SELECT q.id FROM (` +
		`SELECT "é".id FROM outer_docs AS "é" WHERE EXISTS (` +
		`SELECT i.id FROM inner_docs i WHERE i.owner = "é".id)` +
		`) q WHERE q.id = '✓'`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	inner := stmt.From[0].Query
	sub := inner.Where.Subquery
	wantQuery := strings.Index(src, "SELECT i.id")
	wantBinding := strings.Index(src, `"é".id)`)
	if sub.Correlation == nil || sub.Correlation.Pos != wantQuery ||
		len(sub.Correlation.Bindings) != 1 ||
		sub.Correlation.Bindings[0].Pos != wantBinding ||
		sub.Correlation.References[0].Path.Pos != wantBinding {
		t.Fatalf("UTF-8 rebased metadata = %+v, want query %d binding %d",
			sub.Correlation, wantQuery, wantBinding)
	}
}

func TestPredicateSubqueryInJoinOnIsPositionedFeatureRefusal(t *testing.T) {
	valid := []struct {
		name string
		src  string
		at   string
	}{
		{
			name: "exists",
			src: `SELECT a.id FROM accounts a JOIN regions r ON EXISTS (` +
				`SELECT i.id FROM items i WHERE i.region = r.id)`,
			at: "EXISTS",
		},
		{
			name: "not exists",
			src: `SELECT a.id FROM accounts a JOIN regions r ON NOT EXISTS (` +
				`SELECT i.id FROM items i WHERE i.region = r.id)`,
			at: "EXISTS",
		},
		{
			name: "exists validates nested join",
			src: `SELECT a.id FROM accounts a JOIN regions r ON EXISTS (` +
				`SELECT i.id FROM items i JOIN tags t ON i.id = t.item ` +
				`WHERE t.region = r.id)`,
			at: "EXISTS",
		},
		{
			name: "in",
			src: `SELECT a.id FROM accounts a JOIN regions r ON a.id IN (` +
				`SELECT i.account FROM items i WHERE i.region = r.id)`,
			at: "IN (",
		},
		{
			name: "not in",
			src: `SELECT a.id FROM accounts a JOIN regions r ON a.id NOT IN (` +
				`SELECT i.account FROM items i WHERE i.region = r.id)`,
			at: "IN (",
		},
		{
			name: "scalar",
			src: `SELECT a.id FROM accounts a JOIN regions r ON a.id = (` +
				`SELECT i.account FROM items i WHERE i.region = r.id)`,
			at: "(SELECT",
		},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.src)
			var unsupported *FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want *FeatureNotSupportedError", err, err)
			}
			want := strings.Index(test.src, test.at)
			if unsupported.Pos != want {
				t.Fatalf("feature position = %d, want %d", unsupported.Pos, want)
			}
		})
	}

	malformed := []struct {
		name string
		src  string
	}{
		{
			name: "exists missing close",
			src: `SELECT a.id FROM accounts a JOIN regions r ON EXISTS (` +
				`SELECT i.id FROM items i WHERE i.region = r.id`,
		},
		{
			name: "not exists invalid child",
			src: `SELECT a.id FROM accounts a JOIN regions r ON NOT EXISTS (` +
				`SELECT FROM items i)`,
		},
		{
			name: "in missing close",
			src: `SELECT a.id FROM accounts a JOIN regions r ON a.id IN (` +
				`SELECT i.account FROM items i`,
		},
		{
			name: "not in invalid child",
			src: `SELECT a.id FROM accounts a JOIN regions r ON a.id NOT IN (` +
				`SELECT FROM items i)`,
		},
		{
			name: "scalar invalid child",
			src: `SELECT a.id FROM accounts a JOIN regions r ON a.id = (` +
				`SELECT FROM items i)`,
		},
	}
	for _, test := range malformed {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.src)
			var parse *ParseError
			if !errors.As(err, &parse) {
				t.Fatalf("malformed error = %T %v, want *ParseError", err, err)
			}
			var unsupported *FeatureNotSupportedError
			if errors.As(err, &unsupported) {
				t.Fatalf("malformed subquery was classified as a supported grammar feature: %v", err)
			}
		})
	}
}

func TestPredicateCorrelationReuseAndAbsentPathCost(t *testing.T) {
	const uncorrelated = `SELECT o.id FROM orders o WHERE EXISTS (` +
		`SELECT 1 FROM items i WHERE i.active = TRUE)`
	var parser Parser
	var statement SelectStmt
	for attempt, src := range []string{
		correlatedExistsSQL, uncorrelated, correlatedExistsSQL, uncorrelated,
	} {
		if err := parser.Parse(&statement, src); err != nil {
			t.Fatalf("parse %d: %v", attempt, err)
		}
		sub := statement.Where.Subquery
		if (sub.Correlation != nil) != (src == correlatedExistsSQL) {
			t.Fatalf("parse %d stale correlation = %+v", attempt, sub.Correlation)
		}
	}
	if got := unsafe.Sizeof(PathExpr{}); got != unsafe.Sizeof(int(0))*3+unsafe.Sizeof([]Segment(nil)) {
		t.Fatalf("PathExpr widened to %d bytes", got)
	}
	if got := testing.AllocsPerRun(200, func() {
		if err := parser.Parse(&statement, correlatedExistsSQL); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Fatalf("warmed correlated parse allocated %.1f times, want zero", got)
	}
}

func TestPredicateCorrelationParserReuseAfterErrorAndCancellation(t *testing.T) {
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, correlatedExistsSQL); err != nil {
		t.Fatal(err)
	}
	if err := parser.Parse(&statement, `SELECT o.id FROM orders o WHERE EXISTS (`+
		`SELECT i.id FROM items i WHERE i.owner = o.id`); err == nil {
		t.Fatal("unterminated correlated query was accepted")
	}
	if statement.Correlation != nil || len(statement.Columns) != 0 || len(statement.From) != 0 {
		t.Fatalf("failed parse retained AST state: %+v", statement)
	}
	if err := parser.Parse(&statement, correlatedExistsSQL); err != nil {
		t.Fatalf("reuse after syntax error: %v", err)
	}

	// Use a fresh parser and arm cancellation only after predicate correlation
	// state exists. The top-level admission scan therefore succeeds and the
	// child parser is cancelled with an active capture, exercising rollback and
	// reuse rather than only the trivial pre-parse cancellation path.
	var cancelParser Parser
	if err := cancelParser.Parse(&statement, `SELECT id FROM docs`); err != nil {
		t.Fatal(err)
	}
	cancelled := errors.New("test parser cancellation")
	cancelParser.SetCancellationCheck(func() error {
		if cancelParser.correlation != nil {
			return cancelled
		}
		return nil
	})
	if err := cancelParser.Parse(&statement, correlatedExistsSQL); !errors.Is(err, cancelled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if statement.Correlation != nil || len(statement.Columns) != 0 || len(statement.From) != 0 {
		t.Fatalf("cancelled parse retained AST state: %+v", statement)
	}
	cancelParser.SetCancellationCheck(nil)
	if err := cancelParser.Parse(&statement, correlatedExistsSQL); err != nil {
		t.Fatalf("reuse after cancellation: %v", err)
	}
	if statement.Where.Subquery.Correlation == nil {
		t.Fatal("reuse after cancellation lost correlation metadata")
	}
}

func assertCorrelationReference(
	t *testing.T,
	spec *CorrelationSpec,
	path *PathExpr,
	want int,
) {
	t.Helper()
	got, ok := correlationReferenceBinding(spec, path)
	if !ok || got != want {
		t.Fatalf("path %+v reference = %d/%t, want %d/true", path, got, ok, want)
	}
}

func correlationReferenceBinding(
	spec *CorrelationSpec,
	path *PathExpr,
) (int, bool) {
	if spec == nil || path == nil {
		return 0, false
	}
	for i := range spec.References {
		if spec.References[i].Path == path {
			return spec.References[i].Binding, true
		}
	}
	return 0, false
}

func predicateSubquery(expr *Expr) *SelectStmt {
	if expr == nil {
		return nil
	}
	if expr.Subquery != nil {
		return expr.Subquery
	}
	for _, child := range expr.Kids {
		if sub := predicateSubquery(child); sub != nil {
			return sub
		}
	}
	return nil
}

func BenchmarkCorrelatedPredicateParse(b *testing.B) {
	var parser Parser
	var statement SelectStmt
	for range 2 {
		if err := parser.Parse(&statement, correlatedExistsSQL); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := parser.Parse(&statement, correlatedExistsSQL); err != nil {
			b.Fatal(err)
		}
	}
}
