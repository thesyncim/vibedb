package sql

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

// FuzzParseSQL is this package's highest-value test.
//
// A parser is the one component that is guaranteed to be handed bytes nobody
// designed: a database/sql driver forwards whatever the application built, and
// an application builds statements from user input. So the properties asserted
// here are the ones a caller depends on without ever writing them down — that
// no input panics, that no input loops, and that a statement the parser did
// accept is internally consistent enough for a lowering pass to walk without
// re-checking every index.
//
// Termination is structural rather than empirical, and worth naming so the
// property is not lost in a later change: the lexer never returns a token
// without consuming a byte, so the token stream is finite; every parser loop
// advances on that stream or returns; and predicate recursion is bounded by
// maxExprDepth, so a wall of open parentheses is a rejection rather than a
// stack overflow. The fuzz engine's own timeout is the backstop, not the
// argument.
func FuzzParseSQL(f *testing.F) {
	seeds := []string{
		``,
		`SELECT`,
		`SELECT a FROM t`,
		`SELECT * FROM t`,
		`SELECT a FROM t WHERE b = 1 AND c IN ('x', 'y') OR NOT d IS NULL`,
		`SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.uid WHERE o.total BETWEEN ? AND ?`,
		`SELECT team, COUNT(*), SUM(s) FROM d GROUP BY team HAVING COUNT(*) > 1 ORDER BY team DESC LIMIT 5 OFFSET 2`,
		`SELECT a FROM t WHERE m @> {"k": [1, 2, {"n": null}]}`,
		`SELECT id FROM orders WHERE customer IN (SELECT id FROM customers WHERE tier = ?)`,
		`SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE active = TRUE)`,
		`SELECT d.id FROM (SELECT id FROM customers WHERE tier = ?) AS d WHERE d.id = ?`,
		`SELECT a.id, d.id FROM accounts a LEFT JOIN LATERAL (` +
			`SELECT i.id FROM items i WHERE i.owner = a.id AND a.region = ?` +
			`) d ON TRUE`,
		`WITH active(id) AS MATERIALIZED (SELECT id FROM customers WHERE tier = ?), ` +
			`selected AS NOT MATERIALIZED (SELECT id FROM active) SELECT id FROM selected WHERE id = ?`,
		`WITH outer_cte AS (WITH inner_cte AS (SELECT id FROM docs) SELECT id FROM inner_cte) SELECT id FROM outer_cte`,
		`SELECT team, ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC NULLS LAST), ` +
			`SUM(score) OVER (PARTITION BY team ORDER BY score ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM scores`,
		`SELECT LAG(value, ?, NULL) OVER (ORDER BY seq NULLS FIRST) FROM events`,
		`SELECT NTILE(?) OVER (ORDER BY score), PERCENT_RANK() OVER (ORDER BY score), ` +
			`NTH_VALUE(value, 2) OVER (ORDER BY score GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM events`,
		`SELECT SUM(value) OVER framed, SUM(value) OVER (` +
			`ordered RANGE BETWEEN ? PRECEDING AND 1.2500 FOLLOWING EXCLUDE TIES) FROM events ` +
			`WINDOW partitioned AS (PARTITION BY team), ordered AS (partitioned ORDER BY score), ` +
			`framed AS (ordered ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP)`,
		`SELECT "select"."from" FROM "where" WHERE a['x.y'] = 'it''s'`,
		`SELECT a.b[0].c FROM t`,
		`-- comment` + "\n" + `SELECT a /* x */ FROM t`,
		`SELECT a FROM t WHERE b LIKE 'x'`,
		`(((((((((((`,
		`SELECT a FROM t WHERE b = '`,
		`SELECT a FROM t WHERE m @> {`,
		"\x00\x80\xff",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, src string) {
		var p Parser
		var stmt SelectStmt
		err := p.Parse(&stmt, src)
		if !utf8.ValidString(src) && err == nil {
			t.Fatal("Parse accepted invalid UTF-8")
		}
		if err != nil {
			var parseErr *ParseError
			if !errors.As(err, &parseErr) {
				t.Fatalf("Parse returned %T, want *ParseError", err)
			}
			if parseErr.Pos < 0 || parseErr.Pos > len(src) {
				t.Fatalf("error offset %d is outside [0, %d]", parseErr.Pos, len(src))
			}
			if parseErr.Line < 1 || parseErr.Col < 1 {
				t.Fatalf("error position %d:%d is not 1-based", parseErr.Line, parseErr.Col)
			}
			if parseErr.Msg == "" {
				t.Fatal("error carries no message")
			}
			// Formatting must not panic either; a driver logs this.
			_ = parseErr.Error()
			if len(stmt.Columns) != 0 || len(stmt.From) != 0 {
				t.Fatal("a rejected statement left fields behind")
			}
			return
		}
		checkStatementInvariants(t, &stmt)

		// A second parse into the same warmed Parser must produce the same
		// tree. This is where a stale arena would show: a chunk reused without
		// being cleared, or a scratch buffer that outlived its statement,
		// changes the answer on the second pass and nothing else would catch
		// it.
		first := dumpStmt(&stmt)
		if err := p.Parse(&stmt, src); err != nil {
			t.Fatalf("reparse of an accepted statement failed: %v", err)
		}
		if second := dumpStmt(&stmt); second != first {
			t.Fatalf("reparse differs:\nfirst  %s\nsecond %s", first, second)
		}
	})
}

// checkStatementInvariants asserts what an accepted statement promises, so a
// lowering pass may walk it without re-validating every index.
func checkStatementInvariants(t *testing.T, s *SelectStmt) {
	checkStatementInvariantsScoped(t, s, nil)
}

func checkStatementInvariantsScoped(
	t *testing.T,
	s *SelectStmt,
	outer *LateralSpec,
) {
	t.Helper()
	if len(s.Columns) == 0 {
		t.Fatal("an accepted statement projects nothing")
	}
	if len(s.From) == 0 {
		t.Fatal("an accepted statement reads no relation")
	}
	seen := 0
	if s.With != nil {
		if len(s.With.CTEs) == 0 {
			t.Fatal("WITH clause has no definitions")
		}
		for i := range s.With.CTEs {
			cte := &s.With.CTEs[i]
			if cte.Name == "" || cte.Query == nil {
				t.Fatalf("WITH[%d] has invalid payload: %+v", i, cte)
			}
			if len(cte.Columns) != len(cte.ColumnPos) {
				t.Fatalf("WITH[%d] has %d aliases and %d positions", i, len(cte.Columns), len(cte.ColumnPos))
			}
			for prior := 0; prior < i; prior++ {
				if s.With.CTEs[prior].Name == cte.Name {
					t.Fatalf("WITH[%d] duplicates name %q", i, cte.Name)
				}
			}
			if cte.ColumnArityDeferred {
				if cteOutputArityKnown(cte.Query) {
					t.Fatalf("WITH[%d] deferred a statically known output arity", i)
				}
			} else if len(cte.Columns) > len(cte.Query.Columns) {
				t.Fatalf("WITH[%d] has %d aliases for %d outputs", i, len(cte.Columns), len(cte.Query.Columns))
			}
			checkStatementInvariantsScoped(t, cte.Query, nil)
			if cte.Query.ParamBase != seen {
				t.Fatalf("WITH[%d] ParamBase = %d, want %d", i, cte.Query.ParamBase, seen)
			}
			seen += cte.Query.Params
		}
	}
	for i := range s.Windows {
		window := &s.Windows[i]
		if window.Name == "" {
			t.Fatalf("Windows[%d] has no name", i)
		}
		for prior := 0; prior < i; prior++ {
			if s.Windows[prior].Name == window.Name {
				t.Fatalf("Windows[%d] duplicates name %q", i, window.Name)
			}
		}
		if window.Spec.Name != "" {
			found := false
			for prior := 0; prior < i; prior++ {
				found = found || s.Windows[prior].Name == window.Spec.Name
			}
			if !found {
				t.Fatalf("Windows[%d] inherits unknown/later window %q", i, window.Spec.Name)
			}
		}
		seen += checkWindowSpecInvariants(t, s, &window.Spec, true, outer)
	}
	for i := range s.From {
		ref := &s.From[i]
		if ref.Alias == "" {
			t.Fatalf("From[%d] has no range-variable name", i)
		}
		if (ref.Join == JoinNone) != (i == 0) {
			t.Fatalf("From[%d] has join kind %d", i, ref.Join)
		}
		switch ref.Kind {
		case RelationCollection:
			if ref.Name == "" || ref.Query != nil {
				t.Fatalf("From[%d] has invalid collection payload: %+v", i, ref)
			}
			if ref.Lateral != nil {
				t.Fatalf("From[%d] physical relation carries LATERAL state: %+v", i, ref)
			}
		case RelationDerived:
			if ref.Name != "" || ref.Query == nil || ref.UnresolvedCTE.Kind != CTEReferenceNone || !ref.HasAlias {
				t.Fatalf("From[%d] has invalid derived payload: %+v", i, ref)
			}
			if ref.Lateral != nil {
				if i == 0 || ref.Lateral.Pos < 0 ||
					ref.Lateral.Decorrelated != (len(ref.Lateral.Bindings) == 0) {
					t.Fatalf("From[%d] has invalid LATERAL metadata: %+v", i, ref)
				}
				if len(ref.Lateral.Bindings) != 0 &&
					ref.Join != JoinInner && ref.Join != JoinLeft && ref.Join != JoinCross {
					t.Fatalf("From[%d] correlated unsupported join kind %d", i, ref.Join)
				}
				for binding := range ref.Lateral.Bindings {
					item := &ref.Lateral.Bindings[binding]
					if item.Depth < 1 || item.Source < 0 ||
						item.Depth == 1 && item.Source >= i {
						t.Fatalf("From[%d] binding[%d] = %+v", i, binding, item)
					}
				}
				for reference := range ref.Lateral.References {
					item := &ref.Lateral.References[reference]
					if item.Path == nil || item.Binding < 0 || item.Binding >= len(ref.Lateral.Bindings) {
						t.Fatalf("From[%d] reference[%d] = %+v", i, reference, item)
					}
					binding := &ref.Lateral.Bindings[item.Binding]
					if item.Path.Source != binding.Source ||
						!sameSegments(item.Path.Segments, binding.Segments) {
						t.Fatalf("From[%d] reference[%d] %+v disagrees with binding %+v",
							i, reference, item, binding)
					}
				}
			}
			checkStatementInvariantsScoped(t, ref.Query, ref.Lateral)
			if ref.Query.ParamBase != seen {
				t.Fatalf("From[%d] derived ParamBase = %d, want %d", i, ref.Query.ParamBase, seen)
			}
			seen += ref.Query.Params
		case RelationCTE:
			if ref.Name == "" || ref.Query == nil || ref.UnresolvedCTE.Kind != CTEReferenceNone || i != 0 || len(s.From) != 1 {
				t.Fatalf("From[%d] has invalid CTE payload: %+v", i, ref)
			}
			if ref.Lateral != nil {
				t.Fatalf("From[%d] CTE relation carries LATERAL state: %+v", i, ref)
			}
		default:
			t.Fatalf("From[%d] has unknown relation kind %d", i, ref.Kind)
		}
		if i == 0 {
			continue
		}
		if ref.Join == JoinCross {
			if ref.On != nil {
				t.Fatalf("From[%d] CROSS JOIN carries a condition", i)
			}
			continue
		}
		if ref.On == nil {
			t.Fatalf("From[%d] is a join with no condition", i)
		}
		for key := range ref.On.Keys {
			checkPath(t, s, ref.On.Keys[key].Left, outer)
			checkPath(t, s, ref.On.Keys[key].Right, outer)
		}
		if ref.On.Expr != nil {
			seen += checkExprScoped(t, s, ref.On.Expr, false, outer)
		}
	}
	for i := range s.Columns {
		if window := s.Columns[i].Window; window != nil {
			if s.Columns[i].Path != nil || s.Columns[i].Agg != AggNone {
				t.Fatalf("Columns[%d] mixes a window with an ordinary expression", i)
			}
			seen += checkWindowInvariants(t, s, window, outer)
			continue
		}
		if s.Columns[i].Path == nil {
			if s.Columns[i].Agg != AggCount {
				t.Fatalf("Columns[%d] has no path and is not COUNT(*)", i)
			}
			continue
		}
		checkPath(t, s, s.Columns[i].Path, outer)
	}
	for _, key := range s.GroupBy {
		checkPath(t, s, key, outer)
	}
	for i := range s.OrderBy {
		if s.OrderBy[i].Output == 0 {
			checkPath(t, s, s.OrderBy[i].Path, outer)
		} else if s.OrderBy[i].Output > len(s.Columns) {
			t.Fatalf("OrderBy[%d] output %d is outside SELECT list", i, s.OrderBy[i].Output)
		}
	}
	if s.Where != nil {
		seen += checkExprScoped(t, s, s.Where, false, outer)
	}
	if s.Having != nil {
		seen += checkExprScoped(t, s, s.Having, true, outer)
	}
	seen += checkRowCount(t, s.Limit)
	seen += checkRowCount(t, s.Offset)
	if seen != s.Params {
		t.Fatalf("statement reports %d placeholders and holds %d", s.Params, seen)
	}
}

// checkWindowInvariants validates the function-specific optional fields and
// returns the number of placeholders owned by the window expression. Window
// operands live outside WHERE/HAVING, so omitting them here would let Params
// disagree with the accepted tree even though ordinary predicates are sound.
func checkWindowInvariants(
	t *testing.T,
	s *SelectStmt,
	w *WindowExpr,
	outer *LateralSpec,
) int {
	t.Helper()
	requiresArgument := true
	switch w.Kind {
	case WindowRowNumber, WindowRank, WindowDenseRank,
		WindowNTile, WindowPercentRank, WindowCumeDist:
		requiresArgument = false
	case WindowCount:
		// COUNT(*) deliberately has no path; COUNT(path) does.
		requiresArgument = false
	}
	if requiresArgument && w.Argument == nil {
		t.Fatalf("%s has no argument", w.Kind)
	}
	if w.Argument != nil {
		checkPath(t, s, w.Argument, outer)
	}

	seen := 0
	if w.HasOffset {
		seen += checkWindowCount(t, w.Offset, "LAG/LEAD offset")
	}
	if w.HasBuckets {
		seen += checkWindowCount(t, w.Buckets, "NTILE bucket count")
	}
	if w.HasNth {
		seen += checkWindowCount(t, w.Nth, "NTH_VALUE position")
	}
	if w.HasDefault && !w.DefaultNull && w.Default.Kind == OperandParam {
		seen++
	}
	seen += checkWindowSpecInvariants(t, s, &w.Spec, !w.Spec.FrameInherited, outer)
	return seen
}

func checkWindowSpecInvariants(
	t *testing.T,
	s *SelectStmt,
	spec *WindowSpec,
	countFrame bool,
	outer *LateralSpec,
) int {
	t.Helper()
	for _, path := range spec.PartitionBy {
		checkPath(t, s, path, outer)
	}
	for i := range spec.OrderBy {
		checkPath(t, s, spec.OrderBy[i].Path, outer)
	}
	if !spec.Frame.Explicit {
		return 0
	}
	if spec.Frame.Unit > WindowFrameRange ||
		spec.Frame.Exclusion > WindowExcludeTies {
		t.Fatalf("invalid window frame = %+v", spec.Frame)
	}
	if !countFrame {
		return 0
	}
	return checkWindowFrameBound(t, spec.Frame.Start) +
		checkWindowFrameBound(t, spec.Frame.End)
}

func checkWindowCount(t *testing.T, op Operand, clause string) int {
	t.Helper()
	switch op.Kind {
	case OperandNumber:
		return 0
	case OperandParam:
		return 1
	default:
		t.Fatalf("%s has operand kind %d", clause, op.Kind)
		return 0
	}
}

func checkWindowFrameBound(t *testing.T, bound WindowFrameBound) int {
	t.Helper()
	if bound.Kind != WindowPreceding && bound.Kind != WindowFollowing {
		return 0
	}
	return checkWindowCount(t, bound.Offset, "window frame offset")
}

func checkRowCount(t *testing.T, op *Operand) int {
	t.Helper()
	if op == nil {
		return 0
	}
	switch op.Kind {
	case OperandNumber:
		return 0
	case OperandParam:
		return 1
	}
	t.Fatalf("a row count has operand kind %d", op.Kind)
	return 0
}

// checkExpr walks a predicate and returns the number of placeholders in it.
func checkExpr(t *testing.T, s *SelectStmt, e *Expr, having bool) int {
	return checkExprScoped(t, s, e, having, nil)
}

func checkExprScoped(
	t *testing.T,
	s *SelectStmt,
	e *Expr,
	having bool,
	outer *LateralSpec,
) int {
	t.Helper()
	if e.Subquery != nil {
		checkStatementInvariantsScoped(t, e.Subquery, nil)
		if e.Kind != ExprExists && e.Path == nil {
			t.Fatalf("a subquery leaf of kind %d has no outer path", e.Kind)
		}
		if e.Path != nil {
			checkPath(t, s, e.Path, outer)
		}
		return e.Subquery.Params
	}
	switch e.Kind {
	case ExprAnd, ExprOr:
		if len(e.Kids) < 2 {
			t.Fatalf("an n-ary boolean node holds %d operands", len(e.Kids))
		}
	case ExprNot:
		if len(e.Kids) != 1 {
			t.Fatalf("a NOT node holds %d operands", len(e.Kids))
		}
	case ExprConstant:
		if e.Path != nil || e.Value.Kind != OperandBool {
			t.Fatalf("constant predicate = %+v, want a path-free boolean", e)
		}
	default:
		if e.Agg != AggNone && !having {
			t.Fatal("an aggregate leaf appears outside HAVING")
		}
		if e.Path == nil && e.Agg != AggCount {
			t.Fatalf("a leaf of kind %d has no path", e.Kind)
		}
		if e.Path != nil {
			checkPath(t, s, e.Path, outer)
		}
		if e.RightPath != nil {
			checkPath(t, s, e.RightPath, outer)
		}
		if e.Column < -1 || e.Column >= len(s.Columns) {
			t.Fatalf("a leaf binds output column %d of %d", e.Column, len(s.Columns))
		}
		if having && e.Agg != AggNone && e.Column < 0 {
			t.Fatal("a HAVING aggregate leaf is unbound")
		}
		if e.Kind == ExprBetween && len(e.List) != 2 {
			t.Fatalf("BETWEEN holds %d bounds", len(e.List))
		}
		if e.Kind == ExprIn && len(e.List) == 0 {
			t.Fatal("IN holds no alternatives")
		}
		count := 0
		for _, value := range e.List {
			if value.Kind == OperandParam {
				count++
			}
		}
		if e.Kind == ExprCompare && e.Value.Kind == OperandParam {
			count++
		}
		return count
	}
	total := 0
	for _, kid := range e.Kids {
		total += checkExprScoped(t, s, kid, having, outer)
	}
	return total
}

func checkPath(t *testing.T, s *SelectStmt, p *PathExpr, outer *LateralSpec) {
	t.Helper()
	if p == nil {
		t.Fatal("a required path is nil")
	}
	if bindingIndex, ok := lateralReferenceIndex(outer, p); ok {
		if bindingIndex < 0 || bindingIndex >= len(outer.Bindings) {
			t.Fatalf("an outer path binds slot %d of %d", bindingIndex, len(outer.Bindings))
		}
		binding := &outer.Bindings[bindingIndex]
		if p.Source != binding.Source || !sameSegments(p.Segments, binding.Segments) {
			t.Fatalf("outer path %+v disagrees with binding %+v", p, binding)
		}
	} else if p.Source < 0 || p.Source >= len(s.From) {
		t.Fatalf("a path binds source %d of %d", p.Source, len(s.From))
	}
	spec := p.Spec()
	if got := string(p.AppendSpec(nil)); got != spec {
		t.Fatalf("AppendSpec = %q but Spec = %q", got, spec)
	}
	// A dotted spec must round-trip through query's compilePath, which reads a
	// leading '/' as JSON Pointer syntax. A dotted spec that began with '/'
	// would silently become a pointer and address a different value.
	if spec != "" && !strings.HasPrefix(spec, "/") && strings.ContainsAny(spec, "/~") {
		t.Fatalf("dotted spec %q holds a pointer metacharacter", spec)
	}
	for i := range p.Segments {
		if p.Segments[i].IsIndex && p.Segments[i].Index < 0 {
			t.Fatalf("segment %d has a negative subscript", i)
		}
	}
}

func lateralReferenceIndex(spec *LateralSpec, path *PathExpr) (int, bool) {
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
