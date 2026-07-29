package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

// Given a battery of SQL statements paired with the builder query each is meant
// to denote, when both run over a set of corpora, then their column-oriented
// Results are identical — SQL and builder syntax lower to one typed plan.
//
// Given null-bearing documents and a battery of predicates whose reading
// differs between SQL's three-valued logic and this engine's two-valued
// comparison, when a lowered statement runs, then it keeps exactly the rows an
// independent Kleene evaluator over encoding/json values keeps. That second
// differential is the one that matters: the whole point of the lowering is that
// "WHERE NOT (x = 1)" drops a null x, and nothing but a reference that
// implements UNKNOWN separately can show that it does.

// sqlCorpora returns the corpora the SQL differential runs every pair over;
// between them they carry every field, nesting, array, and container the
// battery names.
func sqlCorpora(t *testing.T) []*store.Segment {
	t.Helper()
	return []*store.Segment{
		mustSegment(t,
			`{"a":1,"b":2,"c":3,"active":true}`,
			`{"a":2,"b":1,"active":false}`,
			`{"a":1,"b":5,"c":1}`,
			`{"a":null,"b":2,"c":9}`,
			`{"b":7}`,
			`{"a":2,"b":2,"c":2}`,
		),
		mustSegment(t,
			`{"user":{"name":"amy"},"xs":[10,20,30],"tags":["a","b","c"],"m":{"x":1}}`,
			`{"user":{"name":"bob"},"xs":[40,50],"tags":["a"],"m":{"x":2}}`,
			`{"tags":["x"],"n":9007199254740993,"p":1.5}`,
			`{"n":9007199254740992,"p":2.5,"tags":["a","b"]}`,
			`{"user":{"name":"amy"},"m":{"x":1,"y":9}}`,
		),
	}
}

// resultKey renders a Result to a canonical string for equality: two Results
// with the same headers, row count, and per-cell kind and JSON bytes are equal.
// Both sides run the same executor, so a projected or computed cell that agrees
// on value agrees byte for byte.
func resultKey(r Result) string {
	var b strings.Builder
	for _, c := range r.Columns {
		b.WriteByte('|')
		b.WriteString(c.Header)
	}
	b.WriteByte('\n')
	for row := 0; row < r.RowCount; row++ {
		for _, c := range r.Columns {
			cell := c.Cells[row]
			fmt.Fprintf(&b, "%d:%s|", cell.Kind(), cell.JSON())
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// cursorKey renders the rows a Statement's cursor yields, which is the Result
// after HAVING, OFFSET, and a cursor-applied LIMIT. It deliberately reads
// through Cursor.Cell rather than the Result, so a hidden HAVING column would
// show up as a difference rather than being quietly skipped by the comparison.
func cursorKey(s *Statement, c Cursor) string {
	var b strings.Builder
	for _, name := range s.Columns() {
		b.WriteByte('|')
		b.WriteString(name)
	}
	b.WriteByte('\n')
	for c.Next() {
		for i := range s.Columns() {
			cell := c.Cell(i)
			fmt.Fprintf(&b, "%d:%s|", cell.Kind(), cell.JSON())
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func runStatement(t testing.TB, s *Statement, src Source, args ...any) string {
	t.Helper()
	var e Exec
	cursor, err := s.RunInto(&e, src, args)
	if err != nil {
		t.Fatalf("RunInto(%q): %v", s.SQL(), err)
	}
	return cursorKey(s, cursor)
}

// TestSQLMatchesBuilder is the lowering differential: a prepared statement must
// return exactly what the builder query it denotes returns.
func TestSQLMatchesBuilder(t *testing.T) {
	cases := []struct {
		sql     string
		builder *Query
	}{
		{`SELECT a FROM t`, Select(Path("a"))},
		{`SELECT a, b, c FROM t`, Select(Path("a"), Path("b"), Path("c"))},
		{`SELECT * FROM t`, Select(Path(""))},
		{`SELECT COUNT(*) FROM t`, Select(Count())},
		{`SELECT COUNT(a), SUM(b), AVG(b), MIN(c), MAX(c) FROM t`,
			Select(Count("a"), Sum("b"), Avg("b"), Min("c"), Max("c"))},
		{`SELECT a FROM t WHERE a = 1`, Select(Path("a")).Where(Cmp("a", Eq, 1))},
		{`SELECT a FROM t WHERE a <> 1`, Select(Path("a")).Where(Cmp("a", Ne, 1))},
		{`SELECT a FROM t WHERE b >= 2 AND b <= 5`,
			Select(Path("a")).Where(And(Cmp("b", Ge, 2), Cmp("b", Le, 5)))},
		{`SELECT a FROM t WHERE b BETWEEN 2 AND 5`,
			Select(Path("a")).Where(And(Cmp("b", Ge, 2), Cmp("b", Le, 5)))},
		{`SELECT a FROM t WHERE a IN (1, 2)`, Select(Path("a")).Where(In("a", 1, 2))},
		{`SELECT a FROM t WHERE a IS NULL`, Select(Path("a")).Where(IsNull("a"))},
		{`SELECT a FROM t WHERE a IS NOT NULL`, Select(Path("a")).Where(Not(IsNull("a")))},
		{`SELECT a FROM t WHERE a IS MISSING`, Select(Path("a")).Where(Not(Exists("a")))},
		{`SELECT a FROM t WHERE a IS NOT MISSING`, Select(Path("a")).Where(Exists("a"))},
		{`SELECT a FROM t WHERE active = true OR b = 7`,
			Select(Path("a")).Where(Or(Cmp("active", Eq, true), Cmp("b", Eq, 7)))},
		{`SELECT b FROM t WHERE a = 1 AND b = 2`,
			Select(Path("b")).Where(And(Cmp("a", Eq, 1), Cmp("b", Eq, 2)))},
		{`SELECT a FROM t ORDER BY a`, Select(Path("a")).OrderBy("a", Asc)},
		{`SELECT a FROM t ORDER BY a DESC, b ASC`,
			Select(Path("a")).OrderBy("a", Desc).OrderBy("b", Asc)},
		{`SELECT a FROM t ORDER BY a LIMIT 3`,
			Select(Path("a")).OrderBy("a", Asc).Limit(3)},
		{`SELECT a, COUNT(*) FROM t GROUP BY a`,
			Select(Path("a"), Count()).GroupBy("a")},
		{`SELECT a, SUM(b) FROM t GROUP BY a ORDER BY a DESC`,
			Select(Path("a"), Sum("b")).GroupBy("a").OrderBy("a", Desc)},
		{`SELECT user.name FROM t`, Select(Path("user.name"))},
		{`SELECT xs[1] FROM t`, Select(Path("/xs/1"))},
		{`SELECT m FROM t WHERE m @> {"x":1}`,
			Select(Path("m")).Where(Contains("m", `{"x":1}`))},
		{`SELECT n FROM t WHERE n = 9007199254740993`,
			Select(Path("n")).Where(Cmp("n", Eq, Number("9007199254740993")))},
		{`SELECT p FROM t WHERE p > 1.5`, Select(Path("p")).Where(Cmp("p", Gt, 1.5))},
		{`SELECT tags FROM t WHERE tags @> ["a"]`,
			Select(Path("tags")).Where(Contains("tags", `["a"]`))},
	}
	corpora := sqlCorpora(t)
	for _, tc := range cases {
		stmt, err := PrepareStatement(tc.sql)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", tc.sql, err)
		}
		for i, set := range corpora {
			want, err := tc.builder.Run(FromSegment(set))
			if err != nil {
				t.Fatalf("builder for %q on corpus %d: %v", tc.sql, i, err)
			}
			got := runStatement(t, stmt, FromSegment(set))
			// The builder's headers are its own path spellings; the statement's
			// are the SQL ones. Compare rows, which is what the caller reads.
			if gotRows, wantRows := rowsOf(got), rowsOf(resultKey(want)); gotRows != wantRows {
				t.Fatalf("%q on corpus %d:\n got %s\nwant %s", tc.sql, i, gotRows, wantRows)
			}
		}
	}
}

// rowsOf drops the header line from a rendered result, leaving the rows two
// front ends must agree on even when they name their columns differently.
func rowsOf(key string) string {
	if i := strings.IndexByte(key, '\n'); i >= 0 {
		return key[i+1:]
	}
	return key
}

// --- three-valued logic ----------------------------------------------------

// nullPool is the null-bearing document domain the three-valued differential
// runs over. It is docPool — the pool the exhaustive query differential already
// uses, which carries explicit nulls, absent fields, duplicate keys, and a
// non-object root — plus the shapes that only matter once NOT is involved: a
// field that is null in one document and present in another, and a field no
// document has at all.
var nullPool = func() [][]byte {
	pool := make([][]byte, 0, len(docPool)+4)
	pool = append(pool, docPool...)
	return append(pool,
		[]byte(`{"a":null}`),
		[]byte(`{"a":1,"b":null}`),
		[]byte(`{"b":null,"c":null}`),
		[]byte(`{"a":"1"}`), // a string that a numeric comparison must not match
	)
}()

// sqlPredicates is the battery the three-valued differential evaluates. Every
// entry either contains a negation or is a shape whose negation appears
// elsewhere, because a positive predicate is where the two dialects already
// agreed and proves nothing.
var sqlPredicates = []string{
	`a = 1`,
	`NOT (a = 1)`,
	`NOT (a <> 1)`,
	`NOT (a < 2)`,
	`NOT (a >= 2)`,
	`NOT (a = 'x')`,
	`NOT (a = true)`,
	`a IN (1, 2)`,
	`a NOT IN (1, 2)`,
	`NOT (a IN (1, 2))`,
	`a BETWEEN 1 AND 2`,
	`a NOT BETWEEN 1 AND 2`,
	`NOT (a BETWEEN 1 AND 2)`,
	`a IS NULL`,
	`a IS NOT NULL`,
	`NOT (a IS NULL)`,
	`a IS MISSING`,
	`a IS NOT MISSING`,
	`NOT (a IS MISSING)`,
	`NOT (a = 1) AND NOT (b = 1)`,
	`NOT (a = 1) OR NOT (b = 1)`,
	`NOT (a = 1 AND b = 1)`,
	`NOT (a = 1 OR b = 1)`,
	`NOT (NOT (a = 1))`,
	`NOT (a = 1 AND NOT (b = 2))`,
	`a = 1 OR NOT (b = 1)`,
	`NOT (a IS NULL AND b = 1)`,
	`NOT (a IS NOT NULL OR b = 2)`,
	`NOT (c = 0)`,
	`NOT (a @> 1)`,
	`a @> 1`,
	`a IS NOT MISSING AND NOT (a = 1)`,
	`NOT (a = ?)`,
	`a NOT IN (1, ?)`,
	`NOT (a BETWEEN ? AND 2)`,
}

// TestSQLThreeValuedLogicMatchesKleene is the divergence-one differential.
//
// The reference is deliberately independent of the lowering it checks: it walks
// the parsed SQL tree, resolves paths against encoding/json values, compares
// numbers with math/big through refCompare, and implements Kleene's tables
// directly with an explicit UNKNOWN. The engine, meanwhile, evaluates a
// two-valued compiled predicate. Agreement over a null-bearing corpus is
// therefore evidence that the T/F recursion in sqllower.go is Kleene and not
// merely plausible.
func TestSQLThreeValuedLogicMatchesKleene(t *testing.T) {
	segments, docsets := nullSegments(t)
	args := []any{int64(1)}
	checked := 0
	for _, predicate := range sqlPredicates {
		src := `SELECT a, b, c FROM t WHERE ` + predicate
		stmt, err := PrepareStatement(src)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", src, err)
		}
		tree, err := sqlast.Parse(src)
		if err != nil {
			t.Fatalf("reference parse(%q): %v", src, err)
		}
		bound := args[:stmt.NumParams()]
		for i, set := range segments {
			got := runStatement(t, stmt, FromSegment(set), bound...)
			want := kleeneReference(tree, docsets[i], bound)
			if got != want {
				t.Fatalf("WHERE %s over corpus %d:\n got %s\nwant %s",
					predicate, i, got, want)
			}
			checked++
		}
	}
	t.Logf("checked %d (predicate x corpus) pairs over %d documents",
		checked, len(nullPool))
}

// TestSQLThreeValuedLogicWithNullArguments checks the shape a literal cannot
// express: the parser refuses "x = NULL" precisely because its reading is
// ambiguous, so a placeholder bound to nil is the only way a NULL comparison
// reaches the lowering, and it must behave as SQL's does — never TRUE, never
// FALSE, and therefore never kept by WHERE nor by the negation of WHERE.
func TestSQLThreeValuedLogicWithNullArguments(t *testing.T) {
	segments, docsets := nullSegments(t)
	for _, predicate := range []string{
		`a = ?`,
		`NOT (a = ?)`,
		`a IN (1, ?)`,
		`a NOT IN (1, ?)`,
		`a BETWEEN ? AND 2`,
		`NOT (a BETWEEN ? AND 2)`,
		`a = ? OR b = 1`,
		`a = ? AND b = 1`,
		`NOT (a = ? OR b = 1)`,
		`NOT (a = ? AND b = 1)`,
	} {
		src := `SELECT a, b, c FROM t WHERE ` + predicate
		stmt, err := PrepareStatement(src)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", src, err)
		}
		tree, err := sqlast.Parse(src)
		if err != nil {
			t.Fatalf("reference parse(%q): %v", src, err)
		}
		bound := []any{nil}
		for i, set := range segments {
			got := runStatement(t, stmt, FromSegment(set), bound...)
			want := kleeneReference(tree, docsets[i], bound)
			if got != want {
				t.Fatalf("WHERE %s (NULL argument) over corpus %d:\n got %s\nwant %s",
					predicate, i, got, want)
			}
		}
	}
}

// nullSegments builds the null-bearing corpora in every storage mode, paired
// with the decoded documents the reference reads.
func nullSegments(t *testing.T) ([]*store.Segment, [][]any) {
	t.Helper()
	var segments []*store.Segment
	var docsets [][]any
	for _, mode := range storageModes {
		segments = append(segments, buildSegment(t, nullPool, mode))
		docs := make([]any, 0, len(nullPool))
		for _, raw := range nullPool {
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.UseNumber()
			var v any
			if err := dec.Decode(&v); err != nil {
				t.Fatalf("reference decode %s: %v", raw, err)
			}
			docs = append(docs, v)
		}
		docsets = append(docsets, docs)
	}
	return segments, docsets
}

// kleeneReference renders the rows "SELECT a, b, c WHERE <e>" must produce,
// evaluating the filter with an explicit UNKNOWN.
func kleeneReference(tree *sqlast.SelectStmt, docs []any, args []any) string {
	var b strings.Builder
	for i := range tree.Columns {
		b.WriteByte('|')
		b.WriteString(tree.Columns[i].Path.Spec())
	}
	b.WriteByte('\n')
	for _, doc := range docs {
		if refTri(tree.Where, doc, args) != triTrue {
			continue
		}
		for i := range tree.Columns {
			cell := refCellFromScalar(refClassify(refResolve(tree.Columns[i].Path.Spec(), doc)))
			fmt.Fprintf(&b, "%d:%s|", cell.kind, refCellJSON(cell))
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// refCellJSON renders a reference cell the way Cell.JSON renders the engine's.
func refCellJSON(c refCell) string {
	switch c.kind {
	case KindNull:
		return "null"
	case KindBool:
		if c.b {
			return "true"
		}
		return "false"
	case KindNumber:
		return c.num
	case KindString:
		raw, _ := json.Marshal(c.s)
		return string(raw)
	default:
		return string(c.raw)
	}
}

// refTri is the independent Kleene evaluator. It shares nothing with the
// lowering under test but the parsed tree: paths resolve through refResolve,
// values classify through refClassify, and numbers compare through refCompare's
// math/big rational order.
func refTri(e *sqlast.Expr, doc any, args []any) tri {
	switch e.Kind {
	case sqlast.ExprAnd:
		out := triTrue
		for _, kid := range e.Kids {
			switch refTri(kid, doc, args) {
			case triFalse:
				return triFalse
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case sqlast.ExprOr:
		out := triFalse
		for _, kid := range e.Kids {
			switch refTri(kid, doc, args) {
			case triTrue:
				return triTrue
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case sqlast.ExprNot:
		return notTri(refTri(e.Kids[0], doc, args))
	}

	value, present := refResolve(e.Path.Spec(), doc)
	cell := refClassify(value, present)
	var out tri
	switch e.Kind {
	case sqlast.ExprIsNull:
		out = boolTri(cell.kind == kindNull)
	case sqlast.ExprIsMissing:
		// The engine's existence test, defined over every document and so
		// never UNKNOWN. It is the dialect's answer to SQL having no absent
		// column at all.
		out = boolTri(!present)
	case sqlast.ExprContains:
		// Containment is two-valued here by design; see leafForm. The
		// reference only has to agree about when it is TRUE, which for the
		// scalar needles in the battery is exact equality against a present
		// value.
		out = refContainsTri(cell, e.Value.Text)
	case sqlast.ExprCompare:
		out = refCompareTri(cell, e.Op, e.Value, args)
	case sqlast.ExprBetween:
		out = andTri(
			refCompareTri(cell, sqlast.OpGe, e.List[0], args),
			refCompareTri(cell, sqlast.OpLe, e.List[1], args))
	default: // sqlast.ExprIn
		out = refInTri(cell, e.List, args)
	}
	if e.Negated {
		return notTri(out)
	}
	return out
}

func refCompareTri(cell refScalar, op sqlast.CmpOp, o sqlast.Operand, args []any) tri {
	lit, known := refOperand(o, args)
	if !known || cell.kind == kindNull {
		return triUnknown
	}
	return boolTri(acceptSign(refCompare(cell, lit), Op(op)))
}

func refInTri(cell refScalar, list []sqlast.Operand, args []any) tri {
	if cell.kind == kindNull {
		return triUnknown
	}
	unknown := false
	for _, o := range list {
		lit, known := refOperand(o, args)
		if !known {
			unknown = true
			continue
		}
		if refCompare(cell, lit) == 0 {
			return triTrue
		}
	}
	if unknown {
		return triUnknown
	}
	return triFalse
}

// refContainsTri answers '@>' for the scalar needles the battery uses. An
// absent haystack contains nothing; a present one contains a scalar needle
// exactly when it equals it.
func refContainsTri(cell refScalar, needle string) tri {
	if !cell.present {
		return triFalse
	}
	var v any
	dec := json.NewDecoder(strings.NewReader(needle))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return triFalse
	}
	return boolTri(refCompare(cell, refClassify(v, true)) == 0)
}

// refOperand resolves one operand to a reference scalar, reporting known=false
// for SQL NULL.
func refOperand(o sqlast.Operand, args []any) (refScalar, bool) {
	switch o.Kind {
	case sqlast.OperandString:
		return refScalar{kind: kindString, present: true, s: o.Text}, true
	case sqlast.OperandNumber:
		return refScalar{kind: kindNumber, present: true, num: o.Text}, true
	case sqlast.OperandBool:
		return refScalar{kind: kindBool, present: true, b: o.Bool}, true
	default: // sqlast.OperandParam
		switch v := args[o.Ordinal].(type) {
		case nil:
			return refScalar{}, false
		case bool:
			return refScalar{kind: kindBool, present: true, b: v}, true
		case int64:
			return refScalar{kind: kindNumber, present: true, num: fmt.Sprint(v)}, true
		case string:
			return refScalar{kind: kindString, present: true, s: v}, true
		default:
			panic(fmt.Sprintf("reference: unbound operand %T", v))
		}
	}
}

// --- HAVING, OFFSET, and LIMIT ---------------------------------------------

// TestSQLHavingOffsetLimit checks the three clauses applied outside the plan.
// Each expectation is written out rather than derived, because a derivation
// would share the arithmetic it is meant to check.
func TestSQLHavingOffsetLimit(t *testing.T) {
	set := mustSegment(t,
		`{"team":"a","score":1}`,
		`{"team":"a","score":2}`,
		`{"team":"b","score":10}`,
		`{"team":"b","score":20}`,
		`{"team":"c","score":100}`,
	)
	cases := []struct {
		sql  string
		rows []string
	}{
		{`SELECT team, SUM(score) FROM t GROUP BY team ORDER BY team`,
			[]string{`"a" 3`, `"b" 30`, `"c" 100`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team HAVING SUM(score) > 3 ORDER BY team`,
			[]string{`"b" 30`, `"c" 100`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team HAVING SUM(score) >= 3 AND SUM(score) < 100 ORDER BY team`,
			[]string{`"a" 3`, `"b" 30`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team HAVING NOT (SUM(score) > 3) ORDER BY team`,
			[]string{`"a" 3`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team HAVING SUM(score) IN (3, 100) ORDER BY team`,
			[]string{`"a" 3`, `"c" 100`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team HAVING SUM(score) BETWEEN 3 AND 30 ORDER BY team`,
			[]string{`"a" 3`, `"b" 30`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team HAVING team <> 'a' ORDER BY team`,
			[]string{`"b" 30`, `"c" 100`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team HAVING SUM(score) > 3 ORDER BY team LIMIT 1`,
			[]string{`"b" 30`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team HAVING SUM(score) > 3 ORDER BY team OFFSET 1`,
			[]string{`"c" 100`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team ORDER BY team OFFSET 1`,
			[]string{`"b" 30`, `"c" 100`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team ORDER BY team LIMIT 1 OFFSET 1`,
			[]string{`"b" 30`}},
		{`SELECT team, SUM(score) FROM t GROUP BY team ORDER BY team OFFSET 5`, nil},
		{`SELECT team, SUM(score) FROM t GROUP BY team ORDER BY team LIMIT 0`, nil},
		{`SELECT score FROM t ORDER BY score LIMIT 2 OFFSET 2`,
			[]string{`10`, `20`}},
		{`SELECT score FROM t ORDER BY score LIMIT ? OFFSET ?`,
			[]string{`10`, `20`}},
	}
	for _, tc := range cases {
		stmt, err := PrepareStatement(tc.sql)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", tc.sql, err)
		}
		args := make([]any, stmt.NumParams())
		for i := range args {
			args[i] = int64(2)
		}
		var e Exec
		cursor, err := stmt.RunInto(&e, FromSegment(set), args)
		if err != nil {
			t.Fatalf("RunInto(%q): %v", tc.sql, err)
		}
		var got []string
		for cursor.Next() {
			var parts []string
			for i := range stmt.Columns() {
				parts = append(parts, string(cursor.Cell(i).JSON()))
			}
			got = append(got, strings.Join(parts, " "))
		}
		if strings.Join(got, ";") != strings.Join(tc.rows, ";") {
			t.Fatalf("%q:\n got %q\nwant %q", tc.sql, got, tc.rows)
		}
	}
}

// TestSQLHavingIsNotIgnored is the regression the parser's own documentation
// asks for: a lowering that dropped HAVING would return every group, which is
// a strictly larger answer and therefore invisible to a test that only checked
// that the rows it wanted were present.
func TestSQLHavingIsNotIgnored(t *testing.T) {
	set := mustSegment(t, `{"t":"a","n":1}`, `{"t":"b","n":9}`)
	stmt, err := PrepareStatement(`SELECT t, SUM(n) FROM x GROUP BY t HAVING SUM(n) > 5`)
	if err != nil {
		t.Fatal(err)
	}
	var e Exec
	cursor, err := stmt.RunInto(&e, FromSegment(set), nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
		if got := string(cursor.Cell(0).JSON()); got != `"b"` {
			t.Fatalf("HAVING kept group %s, which does not satisfy it", got)
		}
	}
	if rows != 1 {
		t.Fatalf("HAVING kept %d groups, want 1; a dropped clause returns them all", rows)
	}
}

// --- rejections ------------------------------------------------------------

// TestSQLRejections asserts each unsupported shape is refused, and refused
// where it can still be explained, rather than executed as something else.
func TestSQLRejections(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{`SELECT u.name FROM users u JOIN orders o ON o."$key" = u.oid JOIN parts p ON p."$key" = o.pid`,
			"chained join"},
		{`SELECT u.name FROM users u JOIN orders o ON o."$key" = u.oid WHERE u.a = 1 OR o.b = 2`,
			"two collections"},
		{`SELECT u.name, o.t, p.t FROM users u JOIN orders o ON o.uid = u.id JOIN parts p ON p.uid = u.id`,
			"only once"},
		{`SELECT u.name FROM users u JOIN orders u ON u.uid = u.id`,
			"declared twice"},
		{`SELECT team, COUNT(*) FROM t GROUP BY team HAVING team IS MISSING`,
			"IS MISSING is not available in HAVING"},
		{`SELECT team, COUNT(*) FROM t GROUP BY team HAVING team @> 1`,
			"'@>' is not available in HAVING"},
	} {
		_, err := PrepareStatement(tc.sql)
		if err == nil {
			t.Fatalf("PrepareStatement(%q) succeeded; want a rejection naming %q", tc.sql, tc.want)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("PrepareStatement(%q) = %v, want a message naming %q", tc.sql, err, tc.want)
		}
	}
}

// TestSQLParseErrorsCarryAPosition asserts a syntax error is the parser's own
// *ParseError, so a driver can report a line and column rather than a sentence.
func TestSQLParseErrorsCarryAPosition(t *testing.T) {
	for _, src := range []string{
		`SELECT`,
		`SELECT a FROM`,
		`SELECT a FROM t WHERE`,
		`DELETE FROM t`,
		`SELECT DISTINCT a FROM t`,
		`SELECT a FROM t WHERE a LIKE 'x%'`,
		`SELECT a FROM t WHERE a = NULL`,
		`SELECT a FROM t LEFT JOIN u ON u.a = t.a`,
	} {
		_, err := PrepareStatement(src)
		if err == nil {
			t.Fatalf("PrepareStatement(%q) succeeded; want a parse error", src)
		}
		var pe *sqlast.ParseError
		if !errors.As(err, &pe) {
			t.Fatalf("PrepareStatement(%q) = %v (%T), want a *sql.ParseError", src, err, err)
		}
		if pe.Pos < 0 || pe.Pos > len(src) {
			t.Fatalf("PrepareStatement(%q) reported offset %d, outside the source", src, pe.Pos)
		}
	}
}

// TestSQLPlaceholderCountIsCheckedAtBind asserts the count check happens before
// anything executes, which is what SelectStmt.Params is exposed for.
func TestSQLPlaceholderCountIsCheckedAtBind(t *testing.T) {
	set := mustSegment(t, `{"a":1}`)
	stmt, err := PrepareStatement(`SELECT a FROM t WHERE a = ? AND a < ?`)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.NumParams() != 2 {
		t.Fatalf("NumParams = %d, want 2", stmt.NumParams())
	}
	var e Exec
	for _, args := range [][]any{nil, {int64(1)}, {int64(1), int64(2), int64(3)}} {
		if _, err := stmt.RunInto(&e, FromSegment(set), args); err == nil {
			t.Fatalf("binding %d arguments to a 2-placeholder statement succeeded", len(args))
		}
	}
	if _, err := stmt.RunInto(&e, FromSegment(set), []any{int64(1), int64(2)}); err != nil {
		t.Fatalf("binding 2 arguments: %v", err)
	}
}

// TestSQLColumnNames asserts the output schema: an AS alias wins, a projection
// takes its engine path spelling, and an aggregate takes the engine's header.
func TestSQLColumnNames(t *testing.T) {
	stmt, err := PrepareStatement(
		`SELECT a, u.b AS bee, COUNT(*), SUM(c), MIN(u.d) FROM u GROUP BY a, u.b`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "bee", "count(*)", "sum(c)", "min(d)"}
	got := stmt.Columns()
	if len(got) != len(want) {
		t.Fatalf("Columns() = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Columns()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestSQLBindReuseIsStable asserts a statement re-bound with different
// arguments answers each binding, rather than caching the first plan.
func TestSQLBindReuseIsStable(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":2}`, `{"a":3}`)
	stmt, err := PrepareStatement(`SELECT a FROM t WHERE a >= ? ORDER BY a`)
	if err != nil {
		t.Fatal(err)
	}
	var e Exec
	for _, tc := range []struct {
		arg  int64
		want string
	}{{1, "1,2,3"}, {3, "3"}, {2, "2,3"}, {4, ""}, {1, "1,2,3"}} {
		cursor, err := stmt.RunInto(&e, FromSegment(set), []any{tc.arg})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for cursor.Next() {
			got = append(got, string(cursor.Cell(0).JSON()))
		}
		if strings.Join(got, ",") != tc.want {
			t.Fatalf("a >= %d gave %q, want %q", tc.arg, got, tc.want)
		}
	}
}

// --- allocation ------------------------------------------------------------

// TestSQLStatementRebindZeroCost asserts the steady state the whole design is
// for: a prepared statement re-bound with fresh arguments, executed, and read
// to exhaustion allocates nothing at all.
//
// It is the contract that makes the driver's per-row cost purely database/sql's
// interface boxing. A statement without placeholders never re-lowers, so it is
// the easy half; a statement with them rebuilds the plan on every bind, and the
// only reason that is free is that the Statement owns a compiler whose arenas a
// warmed compile refills rather than reallocates.
func TestSQLStatementRebindZeroCost(t *testing.T) {
	set := mustSegment(t,
		`{"a":1,"b":"x","c":{"n":1}}`,
		`{"a":2,"b":"y"}`,
		`{"a":3,"b":"z","c":{"n":3}}`,
		`{"a":null,"b":"w"}`,
		`{"b":"v","c":[1,2]}`,
	)
	joinDB, _, _ := sqlJoinDatabase(t)
	for _, tc := range []struct {
		name string
		src  string
		args []any
		// join marks a case that runs over the two-collection database rather
		// than the segment, because a fan-out expansion is a whole extra
		// execution phase and has its own buffers to hold at a high-water mark.
		join bool
	}{
		{"literal", `SELECT a, b, c FROM t WHERE a >= 2 ORDER BY a`, nil, false},
		{"placeholder", `SELECT a, b, c FROM t WHERE a >= ? ORDER BY a`, []any{int64(2)}, false},
		{"negation", `SELECT a, b FROM t WHERE NOT (a = ?) ORDER BY a`, []any{int64(2)}, false},
		{"membership", `SELECT a, b FROM t WHERE a NOT IN (?, 3) ORDER BY a`, []any{int64(1)}, false},
		{"grouped", `SELECT b, COUNT(*), SUM(a) FROM t GROUP BY b ORDER BY b`, nil, false},
		{"having", `SELECT b, SUM(a) FROM t GROUP BY b HAVING SUM(a) > ? ORDER BY b`,
			[]any{int64(0)}, false},
		{"limited", `SELECT a FROM t ORDER BY a LIMIT ? OFFSET ?`,
			[]any{int64(2), int64(1)}, false},
		{"join", `SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k`, nil, true},
		{"join filtered", `SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE o.b >= ?`,
			[]any{int64(0)}, true},
		{"join grouped", `SELECT d.k, SUM(o.b) FROM d JOIN j o ON o.fk = d.k ` +
			`GROUP BY d.k ORDER BY d.k`, nil, true},
		{"dollar-key field join", `SELECT d.a FROM d JOIN j o ON o."$key" = d.k`, nil, true},
	} {
		stmt, err := PrepareStatement(tc.src)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		source := FromSegment(set)
		if tc.join {
			// The snapshot is taken once, outside the measured closure: a
			// Database.Snapshot allocates its own entry list, which is the
			// source's cost rather than the statement's.
			source = FromDatabase(joinDB.Snapshot(), "d")
		}
		var e Exec
		run := func() {
			cursor, err := stmt.RunInto(&e, source, tc.args)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			for cursor.Next() {
				for i := range stmt.Columns() {
					sqlSink += len(cursor.Cell(i).Payload())
				}
			}
		}
		// Two warm-up executions: the first grows every arena to its high-water
		// mark, and the second proves the mark was reached rather than merely
		// approached.
		run()
		run()
		if got := testing.AllocsPerRun(50, run); got != 0 {
			t.Fatalf("%s: %.1f allocations per bind-and-execute, want 0", tc.name, got)
		}
		stmt.Release()
	}
}

var sqlSink int

// --- joins -----------------------------------------------------------------

// sqlJoinDriving and sqlJoinJoined are the two collections the join
// differentials run over. Between them they carry a driving row with no
// partner, a driving row with several, a null and an absent join key on both
// sides, a null in the projected joined column, and a joined document that no
// driving row references — the shapes a fan-out expansion and a pushed-down
// filter can each get wrong in a different way.
var (
	sqlJoinDriving = []string{
		`{"a":1,"k":"x"}`,
		`{"a":2,"k":"y"}`,
		`{"a":null,"k":"x"}`,
		`{"a":4,"k":"none"}`,
		`{"a":5}`,
		`{"a":6,"k":null}`,
		`{"a":7,"k":"z"}`,
	}
	sqlJoinJoined = []string{
		`{"b":10,"fk":"x"}`,
		`{"b":null,"fk":"x"}`,
		`{"b":30,"fk":"y"}`,
		`{"b":40,"fk":"unused"}`,
		`{"b":50}`,
		`{"b":60,"fk":null}`,
		`{"fk":"z"}`,
	}
)

// sqlJoinDatabase publishes the two collections into one Database and returns
// it with the decoded documents the reference walks.
func sqlJoinDatabase(t *testing.T) (*store.Database, []any, []any) {
	t.Helper()
	db := &store.Database{}
	decode := func(name string, docs []string) []any {
		// Two documents per chunk, so the expansion crosses a chunk edge on a
		// collection this small.
		collection, err := db.CreateCollection(name, store.Options{ChunkDocuments: 2})
		if err != nil {
			t.Fatalf("CreateCollection(%s): %v", name, err)
		}
		out := make([]any, 0, len(docs))
		for i, doc := range docs {
			if _, err := collection.Put(fmt.Sprintf("%s%d", name, i), []byte(doc)); err != nil {
				t.Fatalf("Put: %v", err)
			}
			dec := json.NewDecoder(strings.NewReader(doc))
			dec.UseNumber()
			var v any
			if err := dec.Decode(&v); err != nil {
				t.Fatalf("decode %s: %v", doc, err)
			}
			out = append(out, v)
		}
		return out
	}
	return db, decode("d", sqlJoinDriving), decode("j", sqlJoinJoined)
}

// sqlJoinPredicates is the battery the join differential evaluates. Every entry
// either reads the joined collection under a negation or is the positive shape
// its negation is compared against, because a null in a joined column under NOT
// is the case the pushdown is most likely to get wrong: SQL evaluates it over
// the pair, and lowering evaluates it over the joined document before the pair
// exists.
var sqlJoinPredicates = []string{
	``,
	`o.b = 10`,
	`NOT (o.b = 10)`,
	`o.b IS NULL`,
	`o.b IS NOT NULL`,
	`NOT (o.b IS NULL)`,
	`o.b IS MISSING`,
	`NOT (o.b IS MISSING)`,
	`o.b NOT IN (10, 30)`,
	`NOT (o.b IN (10, 30))`,
	`o.b NOT BETWEEN 10 AND 30`,
	`NOT (o.b BETWEEN 10 AND 30)`,
	`d.a = 1`,
	`NOT (d.a = 1)`,
	`NOT (d.a = 1) AND NOT (o.b = 10)`,
	`d.a >= 2 AND NOT (o.b = 30)`,
	`NOT (o.b = 10) AND NOT (o.b = 30)`,
	`NOT (o.b = 10 OR o.b = 30)`,
	`o.b = 10 OR NOT (o.b IS NULL)`,
	`NOT (o.b = ?)`,
	`o.b NOT IN (?, 30)`,
}

// TestSQLJoinThreeValuedLogicMatchesKleene is the join half of the
// divergence-one differential, and the check that the WHERE pushdown is sound.
//
// The reference builds every pair by nested loop and then applies the predicate
// to the pair, which is SQL's evaluation order. The lowering does the opposite:
// it moves a conjunct reading only the joined collection into that clause's own
// filter, which runs before any pair exists. Agreement over a corpus carrying
// nulls and absences on both sides of the join is what makes that reordering a
// theorem rather than an optimism, and the negated leaves are there because a
// two-valued engine and a three-valued dialect disagree precisely under NOT.
func TestSQLJoinThreeValuedLogicMatchesKleene(t *testing.T) {
	db, driving, joined := sqlJoinDatabase(t)
	args := []any{int64(10)}
	checked := 0
	for _, predicate := range sqlJoinPredicates {
		src := `SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k`
		if predicate != "" {
			src += ` WHERE ` + predicate
		}
		stmt, err := PrepareStatement(src)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", src, err)
		}
		tree, err := sqlast.Parse(src)
		if err != nil {
			t.Fatalf("reference parse(%q): %v", src, err)
		}
		bound := args[:stmt.NumParams()]
		got := runStatement(t, stmt, FromDatabase(db.Snapshot(), "d"), bound...)
		want := joinKleeneReference(tree, driving, joined, bound)
		if got != want {
			t.Fatalf("WHERE %s:\n got %s\nwant %s", predicate, got, want)
		}
		checked++
	}
	t.Logf("checked %d predicates over %d x %d documents",
		checked, len(sqlJoinDriving), len(sqlJoinJoined))
}

func TestSQLLeftJoinNullExtendsUnmatchedRows(t *testing.T) {
	db, driving, joined := sqlJoinDatabase(t)
	for _, predicate := range []string{"", "d.a >= 2", "d.k IS NULL"} {
		src := `SELECT d.a, o.b FROM d LEFT JOIN j o ON o.fk = d.k`
		if predicate != "" {
			src += ` WHERE ` + predicate
		}
		stmt, err := PrepareStatement(src)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", src, err)
		}
		tree, err := sqlast.Parse(src)
		if err != nil {
			t.Fatalf("reference parse(%q): %v", src, err)
		}
		got := runStatement(t, stmt, FromDatabase(db.Snapshot(), "d"))
		want := joinKleeneReference(tree, driving, joined, nil)
		if got != want {
			t.Fatalf("WHERE %s:\n got %s\nwant %s", predicate, got, want)
		}
	}
}

func TestSQLLeftJoinRejectsNullableSideWhere(t *testing.T) {
	_, err := PrepareStatement(
		`SELECT d.a, o.b FROM d LEFT JOIN j o ON o.fk = d.k WHERE o.b IS NULL`,
	)
	if err == nil || !strings.Contains(err.Error(), "nullable side of a LEFT JOIN") {
		t.Fatalf("PrepareStatement nullable-side WHERE = %v", err)
	}
}

// joinKleeneReference renders the rows the statement must produce, by nested
// loop over the two decoded collections with the predicate applied to each pair
// under Kleene's tables.
func joinKleeneReference(tree *sqlast.SelectStmt, driving, joined []any, args []any) string {
	var b strings.Builder
	for i := range tree.Columns {
		b.WriteByte('|')
		// A joined column's output name is qualified by the range variable, so
		// the header line checks the alias resolution as well as the rows.
		if source := tree.Columns[i].Path.Source; source != 0 {
			b.WriteString(tree.From[source].Alias)
			b.WriteByte('.')
		}
		b.WriteString(tree.Columns[i].Path.Spec())
	}
	b.WriteByte('\n')
	// Driving ordinal, then joined ordinal: the order the engine defines, and
	// the order a nested loop produces without sorting anything.
	for _, outer := range driving {
		key := refClassify(refResolve("k", outer))
		matched := false
		for _, inner := range joined {
			partner := refClassify(refResolve("fk", inner))
			if key.kind == kindNull || partner.kind == kindNull || refCompare(key, partner) != 0 {
				continue
			}
			matched = true
			pair := [2]any{outer, inner}
			if tree.Where != nil && joinRefTri(tree.Where, pair, args) != triTrue {
				continue
			}
			for i := range tree.Columns {
				path := tree.Columns[i].Path
				cell := refCellFromScalar(
					refClassify(refResolve(path.Spec(), pair[path.Source])))
				fmt.Fprintf(&b, "%d:%s|", cell.kind, refCellJSON(cell))
			}
			b.WriteByte('\n')
		}
		if matched || tree.From[1].Join != sqlast.JoinLeft {
			continue
		}
		pair := [2]any{outer, nil}
		if tree.Where != nil && joinRefTri(tree.Where, pair, args) != triTrue {
			continue
		}
		for i := range tree.Columns {
			path := tree.Columns[i].Path
			cell := refCellFromScalar(
				refClassify(refResolve(path.Spec(), pair[path.Source])))
			fmt.Fprintf(&b, "%d:%s|", cell.kind, refCellJSON(cell))
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// joinRefTri is refTri over a pair: a leaf resolves against the driving or the
// joined document according to the range variable its path names, and the
// boolean tables are the same three-valued ones.
func joinRefTri(e *sqlast.Expr, pair [2]any, args []any) tri {
	switch e.Kind {
	case sqlast.ExprAnd:
		out := triTrue
		for _, kid := range e.Kids {
			switch joinRefTri(kid, pair, args) {
			case triFalse:
				return triFalse
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case sqlast.ExprOr:
		out := triFalse
		for _, kid := range e.Kids {
			switch joinRefTri(kid, pair, args) {
			case triTrue:
				return triTrue
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case sqlast.ExprNot:
		return notTri(joinRefTri(e.Kids[0], pair, args))
	}
	return refTri(e, pair[e.Path.Source], args)
}

// TestSQLJoinMatchesBuilder is the join lowering differential: a prepared
// statement must return exactly what the builder query it denotes returns,
// including the argument order of JoinOn, which is driving-then-joined and the
// reverse of the order ON writes its two sides in.
func TestSQLJoinMatchesBuilder(t *testing.T) {
	db, _, _ := sqlJoinDatabase(t)
	source := FromDatabase(db.Snapshot(), "d")
	for _, tc := range []struct {
		sql     string
		builder *Query
	}{
		{`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k`,
			Select(Path("a"), Path("o.b")).Join(JoinOn("j", "k", "fk").As("o"))},
		{`SELECT o.b, d.a FROM d JOIN j o ON d.k = o.fk`,
			Select(Path("o.b"), Path("a")).Join(JoinOn("j", "k", "fk").As("o"))},
		{`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE o.b > 10`,
			Select(Path("a"), Path("o.b")).
				Join(JoinOn("j", "k", "fk").As("o").Where(Cmp("b", Gt, 10)))},
		{`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE d.a >= 2 AND o.b > 10`,
			Select(Path("a"), Path("o.b")).
				Where(Cmp("a", Ge, 2)).
				Join(JoinOn("j", "k", "fk").As("o").Where(Cmp("b", Gt, 10)))},
		{`SELECT COUNT(*) FROM d JOIN j o ON o.fk = d.k`,
			Select(Count()).Join(JoinOn("j", "k", "fk").As("o"))},
		{`SELECT SUM(o.b), COUNT(o.b) FROM d JOIN j o ON o.fk = d.k`,
			Select(Sum("o.b"), Count("o.b")).Join(JoinOn("j", "k", "fk").As("o"))},
		{`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k ORDER BY o.b DESC`,
			Select(Path("a"), Path("o.b")).
				Join(JoinOn("j", "k", "fk").As("o")).OrderBy("o.b", Desc)},
		{`SELECT d.k, SUM(o.b) FROM d JOIN j o ON o.fk = d.k GROUP BY d.k ORDER BY d.k`,
			Select(Path("k"), Sum("o.b")).
				Join(JoinOn("j", "k", "fk").As("o")).GroupBy("k").OrderBy("k", Asc)},
		{`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k LIMIT 2`,
			Select(Path("a"), Path("o.b")).Join(JoinOn("j", "k", "fk").As("o")).Limit(2)},
	} {
		stmt, err := PrepareStatement(tc.sql)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", tc.sql, err)
		}
		got := runStatement(t, stmt, source)
		want, err := tc.builder.Run(source)
		if err != nil {
			t.Fatalf("builder for %q: %v", tc.sql, err)
		}
		if gotRows, wantRows := rowsOf(got), rowsOf(resultKey(want)); gotRows != wantRows {
			t.Fatalf("%q:\n got %s\nwant %s", tc.sql, gotRows, wantRows)
		}
	}
}

// TestSQLJoinAliasDoesNotShadowADrivingField pins the one place SQL's name
// resolution and the engine's could disagree.
//
// Both say a declared alias wins over a field of the same name, and that
// agreement is the problem: "d.o.b" says unambiguously that o is a field of the
// driving collection, but it renders as "o.b", which the engine reads as the
// joined collection's b. Lowering renders such a path as a JSON Pointer, which
// the engine never treats as qualified, so the statement means what it says.
func TestSQLJoinAliasDoesNotShadowADrivingField(t *testing.T) {
	db := &store.Database{}
	driving, err := db.CreateCollection("d", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driving.Put("d0", []byte(`{"k":"x","o":{"b":"driving"}}`)); err != nil {
		t.Fatal(err)
	}
	joined, err := db.CreateCollection("j", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := joined.Put("j0", []byte(`{"fk":"x","b":"joined"}`)); err != nil {
		t.Fatal(err)
	}
	stmt, err := PrepareStatement(
		`SELECT d.o.b, o.b FROM d JOIN j o ON o.fk = d.k`)
	if err != nil {
		t.Fatal(err)
	}
	got := runStatement(t, stmt, FromDatabase(db.Snapshot(), "d"))
	want := "|/o/b|o.b\n64:\"driving\"|64:\"joined\"|\n"
	if got != want {
		t.Fatalf("got %q, want %q; a qualified driving path must not read the joined side", got, want)
	}
}

// TestSQLJoinWhereRedirectionIsOnlyForTopLevelConjuncts asserts the boundary of
// the pushdown. A condition reading the joined collection is moved into the
// join clause's filter only where that is provably the same question — a
// top-level ANDed term over one collection — and every other placement is
// refused rather than moved anyway.
func TestSQLJoinWhereRedirectionIsOnlyForTopLevelConjuncts(t *testing.T) {
	for _, src := range []string{
		`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE o.b = 10`,
		`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE d.a = 1 AND o.b = 10`,
		`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE NOT (o.b = 10) AND d.a = 1`,
		`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE (o.b = 10 OR o.b = 30)`,
	} {
		if _, err := PrepareStatement(src); err != nil {
			t.Fatalf("%q must lower: %v", src, err)
		}
	}
	for _, src := range []string{
		`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE d.a = 1 OR o.b = 10`,
		`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE NOT (d.a = 1 AND o.b = 10)`,
		`SELECT d.a, o.b FROM d JOIN j o ON o.fk = d.k WHERE NOT (o.b = 10 OR d.a = 1)`,
	} {
		_, err := PrepareStatement(src)
		if err == nil {
			t.Fatalf("%q lowered; a joined condition outside a top-level AND is not the "+
				"join clause's own filter", src)
		}
		if !strings.Contains(err.Error(), "two collections") {
			t.Fatalf("%q = %v, want a message naming the mixed condition", src, err)
		}
	}
}

// TestSQLJoinAliasMustBeSpellable asserts a range-variable name the engine's
// path language cannot carry is refused rather than silently misresolved.
//
// A quoted alias holding a dot renders "o.x".b as "o.x.b", whose head is "o" —
// a name no clause declared — so the engine reads the whole path as a nested
// field of the driving collection and projects null for every row. The wrong
// answer arrives with no error, which is why the check is worth its lines.
func TestSQLJoinAliasMustBeSpellable(t *testing.T) {
	for _, src := range []string{
		`SELECT "o.x".b FROM d JOIN j AS "o.x" ON "o.x".fk = d.k`,
		`SELECT "o/x".b FROM d JOIN j AS "o/x" ON "o/x".fk = d.k`,
	} {
		_, err := PrepareStatement(src)
		if err == nil {
			t.Fatalf("%q lowered; its alias cannot be spelled in a path", src)
		}
		if !strings.Contains(err.Error(), "range variable") {
			t.Fatalf("%q = %v, want a message naming the alias", src, err)
		}
	}
	if _, err := PrepareStatement(
		`SELECT "o x".b FROM d JOIN j AS "o x" ON "o x".fk = d.k`,
	); err != nil {
		t.Fatalf("a quoted alias with no separator byte must lower: %v", err)
	}
}
