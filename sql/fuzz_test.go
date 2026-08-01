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
		`WITH active(id) AS MATERIALIZED (SELECT id FROM customers WHERE tier = ?), ` +
			`selected AS NOT MATERIALIZED (SELECT id FROM active) SELECT id FROM selected WHERE id = ?`,
		`WITH outer_cte AS (WITH inner_cte AS (SELECT id FROM docs) SELECT id FROM inner_cte) SELECT id FROM outer_cte`,
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
			checkStatementInvariants(t, cte.Query)
			if cte.Query.ParamBase != seen {
				t.Fatalf("WITH[%d] ParamBase = %d, want %d", i, cte.Query.ParamBase, seen)
			}
			seen += cte.Query.Params
		}
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
		case RelationDerived:
			if ref.Name != "" || ref.Query == nil || ref.UnresolvedCTE.Kind != CTEReferenceNone || !ref.HasAlias || i != 0 || len(s.From) != 1 {
				t.Fatalf("From[%d] has invalid derived payload: %+v", i, ref)
			}
			checkStatementInvariants(t, ref.Query)
			if ref.Query.ParamBase != seen {
				t.Fatalf("From[%d] derived ParamBase = %d, want %d", i, ref.Query.ParamBase, seen)
			}
			seen += ref.Query.Params
		case RelationCTE:
			if ref.Name == "" || ref.Query == nil || ref.UnresolvedCTE.Kind != CTEReferenceNone || i != 0 || len(s.From) != 1 {
				t.Fatalf("From[%d] has invalid CTE payload: %+v", i, ref)
			}
		default:
			t.Fatalf("From[%d] has unknown relation kind %d", i, ref.Kind)
		}
		if i == 0 {
			continue
		}
		if ref.On == nil {
			t.Fatalf("From[%d] is a join with no condition", i)
		}
		checkPath(t, s, ref.On.Left)
		checkPath(t, s, ref.On.Right)
	}
	for i := range s.Columns {
		if s.Columns[i].Path == nil {
			if s.Columns[i].Agg != AggCount {
				t.Fatalf("Columns[%d] has no path and is not COUNT(*)", i)
			}
			continue
		}
		checkPath(t, s, s.Columns[i].Path)
	}
	for _, key := range s.GroupBy {
		checkPath(t, s, key)
	}
	for i := range s.OrderBy {
		checkPath(t, s, s.OrderBy[i].Path)
	}
	if s.Where != nil {
		seen += checkExpr(t, s, s.Where, false)
	}
	if s.Having != nil {
		seen += checkExpr(t, s, s.Having, true)
	}
	seen += checkRowCount(t, s.Limit)
	seen += checkRowCount(t, s.Offset)
	if seen != s.Params {
		t.Fatalf("statement reports %d placeholders and holds %d", s.Params, seen)
	}
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
	t.Helper()
	if e.Subquery != nil {
		checkStatementInvariants(t, e.Subquery)
		if e.Kind != ExprExists && e.Path == nil {
			t.Fatalf("a subquery leaf of kind %d has no outer path", e.Kind)
		}
		if e.Path != nil {
			checkPath(t, s, e.Path)
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
	default:
		if e.Agg != AggNone && !having {
			t.Fatal("an aggregate leaf appears outside HAVING")
		}
		if e.Path == nil && e.Agg != AggCount {
			t.Fatalf("a leaf of kind %d has no path", e.Kind)
		}
		if e.Path != nil {
			checkPath(t, s, e.Path)
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
		total += checkExpr(t, s, kid, having)
	}
	return total
}

func checkPath(t *testing.T, s *SelectStmt, p *PathExpr) {
	t.Helper()
	if p == nil {
		t.Fatal("a required path is nil")
	}
	if p.Source < 0 || p.Source >= len(s.From) {
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
