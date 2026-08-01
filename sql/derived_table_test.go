package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestDerivedTableAliasFormsProduceExplicitRelationAST(t *testing.T) {
	for _, tc := range []struct {
		name  string
		src   string
		alias string
	}{
		{
			name:  "AS alias",
			src:   `SELECT d.id FROM (SELECT id FROM docs) AS d`,
			alias: "d",
		},
		{
			name:  "bare alias",
			src:   `SELECT derived.id FROM (SELECT id FROM docs) derived`,
			alias: "derived",
		},
		{
			name:  "quoted alias",
			src:   `SELECT "derived rows".id FROM (SELECT id FROM docs) AS "derived rows"`,
			alias: "derived rows",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.src)
			if err != nil {
				t.Fatal(err)
			}
			if len(stmt.From) != 1 {
				t.Fatalf("FROM entries = %d, want 1", len(stmt.From))
			}
			ref := &stmt.From[0]
			if ref.Kind != RelationDerived || ref.Name != "" || ref.Query == nil {
				t.Fatalf("derived relation payload = %+v", ref)
			}
			if ref.Alias != tc.alias || !ref.HasAlias {
				t.Fatalf("derived alias = %q/%v, want %q/true", ref.Alias, ref.HasAlias, tc.alias)
			}
			if ref.Join != JoinNone || ref.On != nil {
				t.Fatalf("sole derived relation carries join state: %+v", ref)
			}
			if ref.Query.ParamBase != 0 || ref.Query.Params != 0 {
				t.Fatalf("nested placeholder range = base %d, count %d", ref.Query.ParamBase, ref.Query.Params)
			}
			if stmt.Columns[0].Path.Source != 0 || stmt.Columns[0].Path.Spec() != "id" {
				t.Fatalf("outer projection did not resolve against derived alias: %+v", stmt.Columns[0].Path)
			}
			want := "select path(0:id) from derived(select path(0:id) from docs)/" + tc.alias
			if got := dumpStmt(stmt); got != want {
				t.Fatalf("AST dump:\n got %s\nwant %s", got, want)
			}
		})
	}
}

func TestNestedDerivedTablesPreservePositionsAndPlaceholderRanges(t *testing.T) {
	const src = `SELECT d.id FROM (
		SELECT innerq.id FROM (
			SELECT b.id FROM base AS b WHERE b.flag = ?
		) innerq
		WHERE innerq.keep = ? AND innerq.id IN (
			SELECT p.id FROM permitted AS p WHERE p.tag = ?
		)
	) AS d WHERE d.id = ? LIMIT ?`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Params != 5 {
		t.Fatalf("outer Params = %d, want 5", stmt.Params)
	}
	outerRef := &stmt.From[0]
	if outerRef.Pos != strings.Index(src, "(") {
		t.Fatalf("outer relation Pos = %d, want %d", outerRef.Pos, strings.Index(src, "("))
	}
	middle := outerRef.Query
	if middle == nil || middle.ParamBase != 0 || middle.Params != 3 {
		t.Fatalf("middle query placeholder range = %+v", middle)
	}
	if middle.Columns[0].Pos != strings.Index(src, "innerq.id") {
		t.Fatalf("middle projection Pos = %d", middle.Columns[0].Pos)
	}
	innerRef := &middle.From[0]
	wantInnerRefPos := strings.Index(src[strings.Index(src, "(")+1:], "(") + strings.Index(src, "(") + 1
	if innerRef.Pos != wantInnerRefPos {
		t.Fatalf("inner relation Pos = %d, want %d", innerRef.Pos, wantInnerRefPos)
	}
	inner := innerRef.Query
	if inner == nil || inner.ParamBase != 0 || inner.Params != 1 {
		t.Fatalf("inner query placeholder range = %+v", inner)
	}
	if inner.Columns[0].Pos != strings.Index(src, "b.id") ||
		inner.From[0].Pos != strings.Index(src, "base AS b") {
		t.Fatalf("inner absolute positions = column %d, source %d", inner.Columns[0].Pos, inner.From[0].Pos)
	}

	questionMarks := positionsOf(src, '?')
	assertParam := func(name string, operand Operand, ordinal, pos int) {
		t.Helper()
		if operand.Kind != OperandParam || operand.Ordinal != ordinal || operand.Pos != pos {
			t.Fatalf("%s = kind %d ordinal %d pos %d, want parameter %d at %d",
				name, operand.Kind, operand.Ordinal, operand.Pos, ordinal, pos)
		}
	}
	assertParam("inner WHERE", inner.Where.Value, 0, questionMarks[0])
	assertParam("middle WHERE", middle.Where.Kids[0].Value, 1, questionMarks[1])
	predicateSubquery := middle.Where.Kids[1].Subquery
	if predicateSubquery == nil || predicateSubquery.ParamBase != 2 || predicateSubquery.Params != 1 {
		t.Fatalf("predicate subquery placeholder range = %+v", predicateSubquery)
	}
	assertParam("predicate subquery WHERE", predicateSubquery.Where.Value, 0, questionMarks[2])
	assertParam("outer WHERE", stmt.Where.Value, 3, questionMarks[3])
	assertParam("outer LIMIT", *stmt.Limit, 4, questionMarks[4])
	if stmt.Where.Path.Pos != strings.LastIndex(src, "d.id") {
		t.Fatalf("outer WHERE path Pos = %d, want %d", stmt.Where.Path.Pos, strings.LastIndex(src, "d.id"))
	}
}

func TestDerivedTableSyntaxErrorsArePrecise(t *testing.T) {
	runRejections(t, []rejection{
		{
			name: "empty body", src: `SELECT * FROM () AS d`, pos: 15,
			want: "expected SELECT after '('",
		},
		{
			name: "non SELECT body", src: `SELECT * FROM (docs) AS d`, pos: 15,
			want: "expected SELECT after '('",
		},
		{
			name: "missing alias", src: `SELECT * FROM (SELECT * FROM docs)`, pos: 34,
			want: "requires a non-empty alias",
		},
		{
			name: "missing alias before WHERE", src: `SELECT * FROM (SELECT * FROM docs) WHERE x = 1`, pos: 35,
			want: "requires a non-empty alias",
		},
		{
			name: "AS without alias", src: `SELECT * FROM (SELECT * FROM docs) AS`, pos: 37,
			want: "expected an alias after AS",
		},
		{
			name: "empty quoted alias", src: `SELECT * FROM (SELECT * FROM docs) AS ""`, pos: 38,
			want: "alias may not be empty",
		},
		{
			name: "unterminated body", src: `SELECT * FROM (SELECT * FROM docs`, pos: 33,
			want: "unterminated subquery",
		},
		{
			name: "extra body parenthesis", src: `SELECT * FROM (SELECT * FROM docs)) AS d`, pos: 34,
			want: "requires a non-empty alias",
		},
		{
			name: "double opening parenthesis", src: `SELECT * FROM ((SELECT * FROM docs)) AS d`, pos: 15,
			want: "expected SELECT after '('",
		},
	})
}

func TestDerivedJoinAndLateralRefusalsStayTyped(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  string
		pos  int
		want string
	}{
		{
			name: "derived driving join",
			src:  `SELECT d.id FROM (SELECT id FROM docs) d JOIN other o ON d.id = o.id`,
			pos:  41,
			want: "joining a derived table",
		},
		{
			name: "derived joined relation",
			src:  `SELECT d.id FROM docs JOIN (SELECT id FROM other) d ON docs.id = d.id`,
			pos:  27,
			want: "derived table is not supported in a JOIN position",
		},
		{
			name: "leading lateral",
			src:  `SELECT d.id FROM LATERAL (SELECT id FROM docs) d`,
			pos:  17,
			want: "LATERAL derived tables are not supported",
		},
		{
			name: "nested lateral keeps absolute position",
			src:  `SELECT d.id FROM (SELECT x.id FROM LATERAL (SELECT id FROM docs) x) d`,
			pos:  35,
			want: "LATERAL derived tables are not supported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.src)
			var unsupported *FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("Parse error = %T %v, want *FeatureNotSupportedError", err, err)
			}
			if unsupported.Pos != tc.pos || !strings.Contains(unsupported.Msg, tc.want) {
				t.Fatalf("typed refusal = offset %d, %q; want %d containing %q",
					unsupported.Pos, unsupported.Msg, tc.pos, tc.want)
			}
			var parse *ParseError
			if !errors.As(err, &parse) || parse.Pos != tc.pos {
				t.Fatalf("typed refusal lost ParseError at %d: %+v", tc.pos, parse)
			}
		})
	}
}

func TestDerivedTableNestingBound(t *testing.T) {
	build := func(depth int) string {
		return strings.Repeat(`SELECT * FROM (`, depth) +
			`SELECT * FROM base` + strings.Repeat(`) AS d`, depth)
	}
	if _, err := Parse(build(maxSubqueryDepth)); err != nil {
		t.Fatalf("Parse(at nesting bound) = %v", err)
	}
	_, err := Parse(build(maxSubqueryDepth + 1))
	var parse *ParseError
	if !errors.As(err, &parse) || !strings.Contains(parse.Msg, "subqueries nest deeper") {
		t.Fatalf("Parse(over nesting bound) = %T %v, want positioned depth rejection", err, err)
	}
}

func positionsOf(src string, needle byte) []int {
	positions := make([]int, 0, strings.Count(src, string(needle)))
	for i := 0; i < len(src); i++ {
		if src[i] == needle {
			positions = append(positions, i)
		}
	}
	return positions
}
