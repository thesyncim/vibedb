package sql

import (
	"errors"
	"strings"
	"testing"
)

// The rejection contract.
//
// These matter as much as the acceptance tests and are checked as strictly. A
// parser that accepts a construct the engine cannot execute has only moved the
// failure to a place with less information: lowering has no statement text, so
// its error cannot point at a byte, and by then the author has been told their
// SQL was fine. Every case here therefore asserts three things — that the
// statement is refused, that the refusal names a position, and that the message
// says something the author can act on.

// rejection is one refused statement. pos is the byte offset the error must
// name; -1 skips the check for the few cases whose position is an
// implementation detail rather than a contract.
type rejection struct {
	name string
	src  string
	pos  int
	want string // required substring of the message
}

func (r rejection) check(t *testing.T) {
	t.Helper()
	_, err := Parse(r.src)
	if err == nil {
		t.Fatalf("Parse(%q) = nil, want a rejection", r.src)
	}
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("Parse(%q) = %T, want *ParseError", r.src, err)
	}
	if r.pos >= 0 && parseErr.Pos != r.pos {
		t.Errorf("Parse(%q) reported offset %d, want %d (message: %s)", r.src, parseErr.Pos, r.pos, parseErr.Msg)
	}
	if parseErr.Line < 1 || parseErr.Col < 1 {
		t.Errorf("Parse(%q) reported line %d column %d, want both 1-based", r.src, parseErr.Line, parseErr.Col)
	}
	if !strings.Contains(parseErr.Msg, r.want) {
		t.Errorf("Parse(%q) said %q, want it to mention %q", r.src, parseErr.Msg, r.want)
	}
}

func runRejections(t *testing.T, cases []rejection) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.check(t) })
	}
}

func TestRejectsSyntaxErrors(t *testing.T) {
	runRejections(t, []rejection{
		{"empty input", ``, 0, "expected SELECT"},
		{"nothing after SELECT", `SELECT`, 6, "expected a field path"},
		{"no FROM", `SELECT a`, 8, "expected FROM"},
		{"nothing after FROM", `SELECT a FROM`, 13, "expected a collection name"},
		{"nothing after WHERE", `SELECT a FROM t WHERE`, 21, "expected a field path"},
		{"nothing after a comparison", `SELECT a FROM t WHERE b =`, 25, "expected a literal"},
		{"unclosed parenthesis", `SELECT a FROM t WHERE (b = 1`, 28, "expected ')'"},
		{"extra closing parenthesis", `SELECT a FROM t WHERE b = 1)`, 27, "unexpected trailing input"},
		{"unclosed bracket", `SELECT a[0 FROM t`, 11, "expected ']'"},
		{"nothing after a dot", `SELECT a.1 FROM t`, 9, "expected a field name after '.'"},
		{"a reserved word after a dot", `SELECT a.from FROM t`, 9, "reserved word"},
		{"a reserved word as a field", `SELECT order FROM t`, 7, `write "order" to use it as a name`},
		{"a reserved word as a collection", `SELECT a FROM order`, 14, "expected a collection name"},
		{"trailing comma in the select list", `SELECT a, FROM t`, 10, "reserved word FROM"},
		{"two statements", `SELECT a FROM t; SELECT b FROM u`, 17, "only one statement"},
		{"unterminated string", `SELECT a FROM t WHERE b = 'x`, 26, "unterminated string literal"},
		{"unterminated quoted identifier", `SELECT "a FROM t`, 7, "unterminated quoted identifier"},
		{"unterminated block comment", `SELECT a FROM t /* x`, 16, "unterminated block comment"},
		{"numeric literal with a leading zero", `SELECT a FROM t WHERE b = 007`, 26, "leading zero"},
		{"numeric literal with no fraction digits", `SELECT a FROM t WHERE b = 1.`, 26, "digit after '.'"},
		{"numeric literal with no exponent digits", `SELECT a FROM t WHERE b = 1e`, 26, "exponent"},
		{"number butted against a name", `SELECT a FROM t WHERE b = 1x`, 27, "after a numeric literal"},
		{"lone bang", `SELECT a FROM t WHERE b ! 1`, 24, "expected '='"},
		{"lone at sign", `SELECT a FROM t WHERE b @ 1`, 24, "'@>'"},
	})
}

func TestRejectsNonPredicateExpressions(t *testing.T) {
	runRejections(t, []rejection{
		{"a bare path is not a condition", `SELECT a FROM t WHERE flag`, 26, "flag = TRUE"},
		{"a constant is not a condition", `SELECT a FROM t WHERE 1 = 1`, 22, "must begin with a path"},
		{"two paths do not compare", `SELECT a FROM t WHERE b = c`, 26, "right side of a comparison is a constant"},
		{"NULL is not an operand", `SELECT a FROM t WHERE b = NULL`, 26, "IS NULL"},
		{"NULL is not a membership alternative", `SELECT a FROM t WHERE b IN (1, NULL)`, 31, "IS NULL"},
		{"an empty membership", `SELECT a FROM t WHERE b IN ()`, 28, "no alternatives"},
		{"a double-quoted operand is an identifier", `SELECT a FROM t WHERE b = "x"`, 26, "single quotes"},
		{"IS wants NULL or MISSING", `SELECT a FROM t WHERE b IS 1`, 27, "NULL or MISSING"},
		{"IS TRUE", `SELECT a FROM t WHERE b IS TRUE`, 27, "flag = TRUE"},
		{"NOT wants a leaf operator", `SELECT a FROM t WHERE b NOT 1`, 28, "IN, BETWEEN, or LIKE"},
		{"BETWEEN wants AND", `SELECT a FROM t WHERE b BETWEEN 1, 2`, 33, "AND between the bounds"},
	})
}

func TestRejectsConstructsTheEngineCannotExecute(t *testing.T) {
	runRejections(t, []rejection{
		{"DISTINCT and ALL", `SELECT DISTINCT ALL a FROM t`, 16, "mutually exclusive"},
		{"DISTINCT with GROUP BY", `SELECT DISTINCT a FROM t GROUP BY a`, 35, "grouping changes"},
		{"COUNT DISTINCT", `SELECT COUNT(DISTINCT a) FROM t`, 13, "no distinct variant"},
		{"LIKE ESCAPE", `SELECT a FROM t WHERE b LIKE 'x%' ESCAPE '!'`, 34, "LIKE ... ESCAPE"},
		{"regular expressions", `SELECT a FROM t WHERE b ~ 'x'`, 24, "regular-expression"},
		{"a scalar subquery on the left", `SELECT a FROM t WHERE (SELECT 1 FROM u) = 1`, 23, "cannot stand alone"},
		{"a subquery in FROM", `SELECT a FROM (SELECT b FROM u)`, 14, "subqueries are not supported"},
		{"CASE", `SELECT a FROM t WHERE CASE WHEN b THEN 1 END = 1`, 22, "CASE"},
		{"CAST", `SELECT a FROM t WHERE CAST(b AS text) = 1`, 22, "CAST"},
		{"the cast operator", `SELECT a FROM t WHERE b::text = 'x'`, 23, "::"},
		{"scalar functions", `SELECT lower(a) FROM t`, 12, "not a supported function"},
		{"window functions", `SELECT SUM(a) OVER () FROM t`, 14, "OVER"},
		{"arithmetic", `SELECT a FROM t WHERE b + 1 = 2`, 24, "arithmetic"},
		{"concatenation", `SELECT a FROM t WHERE b || 'x' = 'y'`, 24, "concatenation"},
		{"set operations", `SELECT a FROM t UNION SELECT b FROM u`, 16, "UNION"},
		{"common table expressions", `WITH x AS (SELECT 1) SELECT a FROM x`, 0, "common table expressions are not supported"},
		// Parse is the SELECT-only entry point. These three are statements the
		// dialect does support, through ParseStatement, so the message names
		// that entry point rather than claiming the engine cannot run them.
		{"INSERT", `INSERT INTO t VALUES (1)`, 0, "parsed by ParseStatement"},
		{"UPDATE", `UPDATE t SET a = 1`, 0, "parsed by ParseStatement"},
		{"DELETE", `DELETE FROM t`, 0, "parsed by ParseStatement"},
		{"CREATE TABLE", `CREATE TABLE t (a STRING)`, 0, "parsed by ParseStatement"},
		{"FETCH FIRST", `SELECT a FROM t FETCH FIRST 1 ROWS ONLY`, 16, "write LIMIT"},
		{"NULLS FIRST", `SELECT a FROM t ORDER BY a NULLS FIRST`, 27, "NULLS FIRST/LAST"},
		{"COLLATE", `SELECT a FROM t ORDER BY a COLLATE "C"`, 27, "COLLATE"},
		{"ORDER BY an output position", `SELECT a FROM t ORDER BY 1`, 25, "output position"},
		{"GROUP BY an output position", `SELECT a FROM t GROUP BY 1`, 25, "output position"},
		{"ORDER BY an aggregate", `SELECT team, SUM(a) FROM t GROUP BY team ORDER BY SUM(a)`, 53, "not by their reduction"},
		{"GROUP BY an aggregate", `SELECT SUM(a) FROM t GROUP BY SUM(a)`, 33, "computed per group"},
	})
}

func TestRejectsUnsupportedJoins(t *testing.T) {
	runRejections(t, []rejection{
		{"FULL JOIN", `SELECT t.a FROM t FULL JOIN u ON t.k = u.k`, 18, "FULL outer joins"},
		{"CROSS JOIN", `SELECT t.a FROM t CROSS JOIN u`, 18, "unrestricted product"},
		{"NATURAL JOIN", `SELECT t.a FROM t NATURAL JOIN u ON t.k = u.k`, 18, "write ON explicitly"},
		{"composite USING", `SELECT t.a FROM t JOIN u USING (k, x)`, 33, "composite USING joins"},
		{"comma joins", `SELECT t.a FROM t, u`, 17, "explicit JOIN"},
		{"a missing ON", `SELECT t.a FROM t JOIN u`, 24, "expected ON"},
		{"an inequality join", `SELECT t.a FROM t JOIN u ON t.k > u.k`, 32, "single key equality"},
		{"a conjunctive join", `SELECT t.a FROM t JOIN u ON t.k = u.k AND t.j = u.j`, 38, "one key per join"},
		{"a join against one side", `SELECT t.a FROM t JOIN u ON t.k = t.j`, 28, "an equi-join matches a key"},
		{"a join that ignores the joined collection",
			`SELECT a.x FROM a JOIN b ON a.k = a.k2 JOIN c ON a.k = b.k`, 28, "an equi-join matches a key"},
		{"a forward reference in ON",
			`SELECT a.x FROM a JOIN b ON c.k = b.k JOIN c ON a.k = c.k`, 28, "joins later"},
		{"a duplicate range variable", `SELECT x.a FROM t AS x JOIN u AS x ON x.k = x.j`, 28, "declared twice"},
		{"a qualified collection name", `SELECT a FROM db.t`, 16, "schema.table"},
		{"a whole document as a join key", `SELECT a.x FROM a JOIN b ON a.* = b.k`, -1, "SELECT list"},
	})
}

func TestRejectsAmbiguousOrUnresolvablePaths(t *testing.T) {
	runRejections(t, []rejection{
		{"an unqualified path across a join",
			`SELECT x FROM a JOIN b ON a.k = b.k`, 7, "qualify it with a range variable"},
		{"a bare star across a join",
			`SELECT * FROM a JOIN b ON a.k = b.k`, 7, "write alias.*"},
		{"a star on something that is not a range variable",
			`SELECT u.* FROM docs`, 7, "projects nothing"},
		{"a star outside the select list",
			`SELECT a FROM t WHERE b.* = 1`, 24, "only allowed in the SELECT list"},
		{"a placeholder cannot name a path",
			`SELECT ? FROM t`, 7, "cannot name a path"},
		{"a placeholder cannot be a subscript",
			`SELECT a[?] FROM t`, 9, "cannot be a subscript"},
		{"a negative subscript",
			`SELECT a[-1] FROM t`, 9, "non-negative integer"},
		{"an output name needs AS",
			`SELECT a b FROM t`, 9, "requires AS"},
		{"an aggregate takes one path",
			`SELECT COUNT(a, b) FROM t`, 14, "exactly one path"},
		{"SUM has no star form",
			`SELECT SUM(*) FROM t`, 11, "must stand alone"},
	})
}

func TestRejectsInvalidGroupingAndAggregation(t *testing.T) {
	runRejections(t, []rejection{
		{"a projection outside GROUP BY",
			`SELECT team, city FROM t GROUP BY team`, 13, "not a GROUP BY key"},
		{"the whole document under GROUP BY",
			`SELECT * FROM t GROUP BY team`, 7, "not a GROUP BY key"},
		{"a projection alongside an aggregate",
			`SELECT team, COUNT(*) FROM t`, 7, "collapses every row"},
		{"ORDER BY outside GROUP BY",
			`SELECT team FROM t GROUP BY team ORDER BY city`, 42, "not a GROUP BY key"},
		{"ORDER BY over a single-row aggregate",
			`SELECT SUM(a) FROM t ORDER BY a`, 30, "returns exactly one row"},
		{"an aggregate in WHERE",
			`SELECT SUM(a) FROM t WHERE SUM(a) > 1`, 30, "use HAVING"},
		{"HAVING without grouping",
			`SELECT a FROM t HAVING a = 1`, 23, "HAVING requires GROUP BY"},
		{"HAVING an aggregate the SELECT list omits",
			`SELECT team FROM t GROUP BY team HAVING SUM(a) > 1`, 40, "add it to the SELECT list"},
		{"HAVING a path that is not a key",
			`SELECT team, COUNT(*) FROM t GROUP BY team HAVING city = 'x'`, 50, "belongs in WHERE"},
		{"HAVING COUNT(*) when the SELECT list omits it",
			`SELECT team FROM t GROUP BY team HAVING COUNT(*) > 1`, 40, "HAVING tests COUNT(*)"},
	})
}

func TestRejectsInvalidRowCounts(t *testing.T) {
	runRejections(t, []rejection{
		{"a negative limit", `SELECT a FROM t LIMIT -1`, 22, "must not be negative"},
		{"a fractional limit", `SELECT a FROM t LIMIT 1.5`, 22, "whole number"},
		{"a limit past 64 bits", `SELECT a FROM t LIMIT 99999999999999999999`, 22, "64 bits"},
		{"a string limit", `SELECT a FROM t LIMIT 'x'`, 22, "expected a non-negative integer"},
		{"a repeated limit", `SELECT a FROM t LIMIT 1 LIMIT 2`, 24, "LIMIT is given twice"},
		{"a negative offset", `SELECT a FROM t OFFSET -1`, 23, "must not be negative"},
		{"a repeated offset", `SELECT a FROM t OFFSET 1 OFFSET 2`, 25, "OFFSET is given twice"},
	})
}

func TestRejectsUnsupportedPlaceholderSpellings(t *testing.T) {
	runRejections(t, []rejection{
		{"numbered", `SELECT a FROM t WHERE b = $1`, 26, "use '?' placeholders"},
		{"named", `SELECT a FROM t WHERE b = :name`, 26, "use '?' placeholders"},
	})
}

// TestRejectsOversizedClauses checks the bounds that keep the post-parse
// consistency checks — which compare each clause against the others, and are
// therefore quadratic — from turning a large statement into a long wait.
func TestRejectsOversizedClauses(t *testing.T) {
	tooManyColumns := "SELECT " + strings.Repeat("a,", maxClauseItems) + "a FROM t"
	if _, err := Parse(tooManyColumns); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("Parse(%d columns) = %v, want a rejection naming the bound", maxClauseItems+1, err)
	}
	tooManyParams := "SELECT a FROM t WHERE b = ?" + strings.Repeat(" AND b = ?", maxParams)
	if _, err := Parse(tooManyParams); err == nil || !strings.Contains(err.Error(), "placeholders") {
		t.Fatalf("Parse(%d placeholders) = %v, want a rejection naming the bound", maxParams+1, err)
	}
	tooManyBytes := strings.Repeat(" ", maxStatementBytes+1)
	for name, parse := range map[string]func(string) error{
		"SELECT": func(src string) error {
			_, err := Parse(src)
			return err
		},
		"statement": func(src string) error {
			_, err := ParseStatement(src)
			return err
		},
	} {
		err := parse(tooManyBytes)
		var parseErr *ParseError
		if !errors.As(err, &parseErr) ||
			parseErr.Pos != maxStatementBytes ||
			!strings.Contains(parseErr.Msg, "16 MiB") {
			t.Fatalf("%s byte-bound error = %#v", name, err)
		}
	}
}

// TestRejectsExcessiveNesting checks the recursion bound. Without it the
// recursive-descent predicate parser turns a deeply parenthesized input into a
// stack overflow, which is a process kill rather than a recoverable panic.
func TestRejectsExcessiveNesting(t *testing.T) {
	const depth = 5000
	src := `SELECT a FROM t WHERE ` + strings.Repeat("(", depth) + `b = 1` + strings.Repeat(")", depth)
	_, err := Parse(src)
	if err == nil {
		t.Fatal("Parse(deeply nested) = nil, want a rejection")
	}
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("Parse(deeply nested) = %T, want *ParseError", err)
	}
	if !strings.Contains(parseErr.Msg, "nests deeper") {
		t.Fatalf("Parse(deeply nested) said %q, want it to mention the nesting bound", parseErr.Msg)
	}
}

func TestRejectsExcessiveSubqueryNesting(t *testing.T) {
	src := strings.Repeat(
		`SELECT a FROM t WHERE EXISTS (`, maxSubqueryDepth+1,
	) + `SELECT a FROM t` +
		strings.Repeat(`)`, maxSubqueryDepth+1)
	_, err := Parse(src)
	if err == nil || !strings.Contains(err.Error(), "subqueries nest deeper") {
		t.Fatalf("Parse(deep subqueries) = %v, want bounded-depth rejection", err)
	}
}

// TestRejectsMalformedContainmentNeedles checks the '@>' right-hand side, which
// is read as raw JSON rather than as SQL tokens.
func TestRejectsMalformedContainmentNeedles(t *testing.T) {
	runRejections(t, []rejection{
		{"no needle", `SELECT a FROM t WHERE m @>`, 26, "expected a JSON document"},
		{"an unterminated object", `SELECT a FROM t WHERE m @> {"a": 1`, 27, "unterminated JSON document"},
		{"an unterminated string", `SELECT a FROM t WHERE m @> "abc`, 27, "unterminated JSON string"},
		{"containment against an aggregate",
			`SELECT SUM(a) FROM t GROUP BY b HAVING SUM(a) @> 1`, 46, "does not apply to an aggregate"},
	})
}

// TestErrorPositionsAreLineAndColumn checks that a multi-line statement reports
// the line and column an editor would show, not just a byte offset.
func TestErrorPositionsAreLineAndColumn(t *testing.T) {
	src := "SELECT a\nFROM t\nWHERE b LIKE 'x' ESCAPE '!'"
	_, err := Parse(src)
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("Parse = %v, want *ParseError", err)
	}
	if parseErr.Line != 3 || parseErr.Col != 18 {
		t.Fatalf("reported %d:%d, want 3:18", parseErr.Line, parseErr.Col)
	}
	if !strings.Contains(parseErr.Error(), "3:18") {
		t.Fatalf("Error() = %q, want it to show 3:18", parseErr.Error())
	}
}
