package sql

import (
	"strings"
	"testing"
)

const correlatedValueCompositeSQL = `SELECT o.id FROM outer_docs AS o WHERE ` +
	`o.wanted NOT IN (SELECT i.value FROM inner_docs AS i WHERE ` +
	`i.tenant = o.tenant AND o.bucket = i.bucket AND i.active = ?)`

func TestCorrelatedValueCompositeCaptureRetainsEveryEqualityOccurrence(t *testing.T) {
	statement, err := Parse(correlatedValueCompositeSQL)
	if err != nil {
		t.Fatal(err)
	}
	leaf := statement.Where
	if leaf == nil || leaf.Kind != ExprIn || !leaf.Negated || leaf.Subquery == nil {
		t.Fatalf("outer predicate = %+v", leaf)
	}
	child := leaf.Subquery
	if child.Correlation == nil || len(child.Correlation.Bindings) != 2 ||
		len(child.Correlation.References) != 2 {
		t.Fatalf("composite correlation = %+v", child.Correlation)
	}
	if child.Where == nil || child.Where.Kind != ExprAnd || len(child.Where.Kids) != 3 {
		t.Fatalf("child WHERE = %+v", child.Where)
	}
	first, second := child.Where.Kids[0], child.Where.Kids[1]
	if first.RightPath == nil || second.Path == nil ||
		child.Correlation.References[0].Path != first.RightPath ||
		child.Correlation.References[1].Path != second.Path ||
		child.Correlation.References[0].Binding != 0 ||
		child.Correlation.References[1].Binding != 1 {
		t.Fatalf("correlation reference identity/order = %+v", child.Correlation.References)
	}
	wantTenant := strings.Index(correlatedValueCompositeSQL, "o.tenant")
	wantBucket := strings.Index(correlatedValueCompositeSQL, "o.bucket")
	if child.Correlation.Bindings[0].Pos != wantTenant ||
		child.Correlation.Bindings[1].Pos != wantBucket ||
		first.RightPath.Pos != wantTenant || second.Path.Pos != wantBucket {
		t.Fatalf("binding/reference positions = %+v / %+v, want %d/%d",
			child.Correlation.Bindings, child.Correlation.References,
			wantTenant, wantBucket)
	}
	if child.ParamBase != 0 || child.Params != 1 ||
		child.Where.Kids[2].Value.Ordinal != 0 {
		t.Fatalf("child placeholder range = base:%d params:%d operand:%+v",
			child.ParamBase, child.Params, child.Where.Kids[2].Value)
	}
	checkStatementInvariants(t, statement)
}

func TestCorrelatedValueCapturesHaveIndependentArenaLifetimes(t *testing.T) {
	const source = `SELECT o.id FROM outer_docs o WHERE ` +
		`o.a IN (SELECT i.v FROM inner_a i WHERE i.k = o.a) AND ` +
		`o.b = (SELECT j.v FROM inner_b j WHERE j.k = o.b)`
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, source); err != nil {
		t.Fatal(err)
	}
	if statement.Where == nil || statement.Where.Kind != ExprAnd ||
		len(statement.Where.Kids) != 2 {
		t.Fatalf("outer WHERE = %+v", statement.Where)
	}
	first := statement.Where.Kids[0].Subquery.Correlation
	second := statement.Where.Kids[1].Subquery.Correlation
	if first == nil || second == nil || first == second ||
		len(first.Bindings) != 1 || len(second.Bindings) != 1 ||
		first.References[0].Path == second.References[0].Path {
		t.Fatalf("captures alias: first=%p %+v second=%p %+v",
			first, first, second, second)
	}
	firstPos := first.Bindings[0].Pos
	if firstPos != strings.Index(source, "o.a)") ||
		second.Bindings[0].Pos != strings.LastIndex(source, "o.b") {
		t.Fatalf("capture positions = %d/%d", firstPos, second.Bindings[0].Pos)
	}

	// A failed reuse must publish no stale tree, and a successful reuse must
	// rebuild both sidecars without the later capture overwriting the earlier.
	if err := parser.Parse(&statement, source[:len(source)-1]); err == nil {
		t.Fatal("unterminated scalar subquery was accepted")
	}
	if statement.Where != nil || len(statement.Columns) != 0 || len(statement.From) != 0 {
		t.Fatalf("failed parse published a partial tree: %+v", statement)
	}
	if err := parser.Parse(&statement, source); err != nil {
		t.Fatal(err)
	}
	first = statement.Where.Kids[0].Subquery.Correlation
	second = statement.Where.Kids[1].Subquery.Correlation
	if first == nil || second == nil || first == second ||
		first.Bindings[0].Pos != firstPos ||
		first.References[0].Path != statement.Where.Kids[0].Subquery.Where.RightPath ||
		second.References[0].Path != statement.Where.Kids[1].Subquery.Where.RightPath {
		t.Fatalf("reused capture identity = first:%+v second:%+v", first, second)
	}
	checkStatementInvariants(t, &statement)

	for range 2 {
		if err := parser.Parse(&statement, source); err != nil {
			t.Fatal(err)
		}
	}
	if got := testing.AllocsPerRun(200, func() {
		if err := parser.Parse(&statement, source); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("warmed composite value-subquery parse allocated %.2f times", got)
	}
}

func TestCorrelatedValueUTF8PositionsAreByteExact(t *testing.T) {
	const source = `SELECT "外".id FROM outer_docs AS "外" WHERE "外".v IN (` +
		`SELECT "内".v FROM inner_docs AS "内" WHERE ` +
		`"内".tenant = "外".tenant AND "内".bucket = "外".bucket)`
	statement, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	child := statement.Where.Subquery
	spec := child.Correlation
	wantQuery := strings.Index(source, `SELECT "内".v`)
	wantTenant := strings.Index(source, `"外".tenant`)
	wantBucket := strings.Index(source, `"外".bucket`)
	if spec == nil || spec.Pos != wantQuery || len(spec.Bindings) != 2 ||
		len(spec.References) != 2 || spec.Bindings[0].Pos != wantTenant ||
		spec.Bindings[1].Pos != wantBucket ||
		spec.References[0].Path.Pos != wantTenant ||
		spec.References[1].Path.Pos != wantBucket {
		t.Fatalf("UTF-8 correlation = %+v, want query/key positions %d/%d/%d",
			spec, wantQuery, wantTenant, wantBucket)
	}
}
