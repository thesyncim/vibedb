package sql

import "testing"

// TestGrammarShapes is the acceptance contract: every statement in the
// supported subset, asserted as an exact tree rather than as "no error". See
// dump_test.go for the rendering.
func TestGrammarShapes(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "minimal projection",
			src:  `SELECT name FROM docs`,
			want: `select path(0:name) from docs`,
		},
		{
			name: "whole document",
			src:  `SELECT * FROM docs`,
			want: `select path(0:) from docs`,
		},
		{
			name: "qualified whole document",
			src:  `SELECT u.* FROM docs AS u`,
			want: `select path(0:) from docs/u`,
		},
		{
			name: "several projections",
			src:  `SELECT team, score FROM docs`,
			want: `select path(0:team) path(0:score) from docs`,
		},
		{
			name: "distinct projection",
			src:  `SELECT DISTINCT team FROM docs ORDER BY team`,
			want: `select distinct path(0:team) from docs group 0:team order 0:team:asc`,
		},
		{
			name: "explicit all projection",
			src:  `SELECT ALL team FROM docs`,
			want: `select path(0:team) from docs`,
		},
		{
			name: "output alias",
			src:  `SELECT team AS t FROM docs`,
			want: `select path(0:team) as t from docs`,
		},
		{
			name: "every aggregate",
			src:  `SELECT COUNT(*), COUNT(x), SUM(x), AVG(x), MIN(x), MAX(x) FROM docs`,
			want: `select count(*) count(0:x) sum(0:x) avg(0:x) min(0:x) max(0:x) from docs`,
		},
		{
			name: "aggregate keyword as a field name",
			src:  `SELECT count, sum.total FROM docs`,
			want: `select path(0:count) path(0:sum.total) from docs`,
		},
		{
			name: "comparison operators",
			src:  `SELECT a FROM t WHERE b = 1 AND c != 2 AND d <> 3 AND e < 4 AND f <= 5 AND g > 6 AND h >= 7`,
			want: `select path(0:a) from t where (and (cmp = 0:b n1) (cmp <> 0:c n2) (cmp <> 0:d n3) ` +
				`(cmp < 0:e n4) (cmp <= 0:f n5) (cmp > 0:g n6) (cmp >= 0:h n7))`,
		},
		{
			name: "literal kinds",
			src:  `SELECT a FROM t WHERE s = 'x' AND n = -1.5e3 AND b = TRUE AND c = false AND p = ?`,
			want: `select path(0:a) from t where (and (cmp = 0:s s"x") (cmp = 0:n n-1.5e3) ` +
				`(cmp = 0:b true) (cmp = 0:c false) (cmp = 0:p ?0)) params=1`,
		},
		{
			name: "null and existence tests",
			src:  `SELECT a FROM t WHERE w IS NULL AND x IS NOT NULL AND y IS MISSING AND z IS NOT MISSING`,
			want: `select path(0:a) from t where (and (isnull 0:w) (isnotnull 0:x) ` +
				`(ismissing 0:y) (isnotmissing 0:z))`,
		},
		{
			name: "membership",
			src:  `SELECT a FROM t WHERE tier IN ('pro', 'team') AND rank NOT IN (1, 2)`,
			want: `select path(0:a) from t where (and (in 0:tier s"pro" s"team") (notin 0:rank n1 n2))`,
		},
		{
			name: "IN subquery",
			src:  `SELECT id FROM orders WHERE customer IN (SELECT id FROM customers WHERE tier = ?)`,
			want: `select path(0:id) from orders where (in 0:customer (` +
				`select path(0:id) from customers where (cmp = 0:tier ?0) params=1)) params=1`,
		},
		{
			name: "EXISTS subquery with discarded literal",
			src:  `SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE active = TRUE)`,
			want: `select path(0:id) from orders where (exists (` +
				`select path(0:) from customers where (cmp = 0:active true)))`,
		},
		{
			name: "scalar subquery",
			src:  `SELECT id FROM orders WHERE customer = (SELECT id FROM customers WHERE active = TRUE)`,
			want: `select path(0:id) from orders where (cmp = 0:customer (` +
				`select path(0:id) from customers where (cmp = 0:active true)))`,
		},
		{
			name: "range",
			src:  `SELECT a FROM t WHERE n BETWEEN 1 AND 9 AND m NOT BETWEEN ? AND ?`,
			want: `select path(0:a) from t where (and (between 0:n n1 n9) (notbetween 0:m ?0 ?1)) params=2`,
		},
		{
			name: "containment",
			src:  `SELECT a FROM t WHERE meta @> {"k": [1, "}"]}`,
			want: `select path(0:a) from t where (contains 0:meta j{"k": [1, "}"]})`,
		},
		{
			name: "grouped aggregate",
			src:  `SELECT team, SUM(score) AS total FROM docs GROUP BY team ORDER BY team DESC LIMIT 10`,
			want: `select path(0:team) sum(0:score) as total from docs group 0:team order 0:team:desc limit n10`,
		},
		{
			name: "having binds to output columns",
			src:  `SELECT team, COUNT(*), SUM(score) FROM docs GROUP BY team HAVING COUNT(*) > 2 AND SUM(score) >= 10 AND team <> 'red'`,
			want: `select path(0:team) count(*) sum(0:score) from docs group 0:team ` +
				`having (and (cmp > count(*)@1 n2) (cmp >= sum(0:score)@2 n10) (cmp <> 0:team@0 s"red"))`,
		},
		{
			name: "having on an unprojected group key",
			src:  `SELECT COUNT(*) FROM docs GROUP BY team HAVING team = 'red'`,
			want: `select count(*) from docs group 0:team having (cmp = 0:team s"red")`,
		},
		{
			name: "limit and offset",
			src:  `SELECT a FROM t LIMIT 5 OFFSET 10`,
			want: `select path(0:a) from t limit n5 offset n10`,
		},
		{
			name: "offset before limit",
			src:  `SELECT a FROM t OFFSET 10 LIMIT 5`,
			want: `select path(0:a) from t limit n5 offset n10`,
		},
		{
			name: "placeholders number in source order",
			src:  `SELECT a FROM t WHERE b = ? AND c IN (?, ?) LIMIT ? OFFSET ?`,
			want: `select path(0:a) from t where (and (cmp = 0:b ?0) (in 0:c ?1 ?2)) limit ?3 offset ?4 params=5`,
		},
		{
			name: "trailing semicolon",
			src:  `SELECT a FROM t;`,
			want: `select path(0:a) from t`,
		},
		{
			name: "comments",
			src: "-- leading\nSELECT a /* inline */, b FROM t -- trailing\n" +
				"WHERE c = 1 /* multi\nline */ AND d = 2",
			want: `select path(0:a) path(0:b) from t where (and (cmp = 0:c n1) (cmp = 0:d n2))`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.src)
			if err != nil {
				t.Fatalf("Parse(%q) = %v, want success", tc.src, err)
			}
			if got := dumpStmt(stmt); got != tc.want {
				t.Fatalf("Parse(%q):\n got %s\nwant %s", tc.src, got, tc.want)
			}
		})
	}
}

// TestOperatorPrecedence pins the boolean ladder. Every case here is a shape a
// reader could reasonably read two ways, which is exactly why the parser must
// only ever produce one of them.
func TestOperatorPrecedence(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "AND binds tighter than OR",
			src:  `a = 1 OR b = 2 AND c = 3`,
			want: `(or (cmp = 0:a n1) (and (cmp = 0:b n2) (cmp = 0:c n3)))`,
		},
		{
			name: "AND binds tighter than OR on the left",
			src:  `a = 1 AND b = 2 OR c = 3`,
			want: `(or (and (cmp = 0:a n1) (cmp = 0:b n2)) (cmp = 0:c n3))`,
		},
		{
			name: "parentheses override",
			src:  `(a = 1 OR b = 2) AND c = 3`,
			want: `(and (or (cmp = 0:a n1) (cmp = 0:b n2)) (cmp = 0:c n3))`,
		},
		{
			name: "NOT is looser than comparison",
			src:  `NOT a = 1`,
			want: `(not (cmp = 0:a n1))`,
		},
		{
			name: "NOT binds tighter than AND",
			src:  `NOT a = 1 AND b = 2`,
			want: `(and (not (cmp = 0:a n1)) (cmp = 0:b n2))`,
		},
		{
			name: "NOT stacks",
			src:  `NOT NOT a = 1`,
			want: `(not (not (cmp = 0:a n1)))`,
		},
		{
			name: "NOT over a parenthesized disjunction",
			src:  `NOT (a = 1 OR b = 2)`,
			want: `(not (or (cmp = 0:a n1) (cmp = 0:b n2)))`,
		},
		{
			name: "BETWEEN's AND is not a conjunction",
			src:  `a BETWEEN 1 AND 2 AND b = 3`,
			want: `(and (between 0:a n1 n2) (cmp = 0:b n3))`,
		},
		{
			name: "BETWEEN inside a disjunction",
			src:  `a BETWEEN 1 AND 2 OR b = 3`,
			want: `(or (between 0:a n1 n2) (cmp = 0:b n3))`,
		},
		{
			name: "IN is a leaf, not a conjunction",
			src:  `a IN (1, 2) AND b = 3`,
			want: `(and (in 0:a n1 n2) (cmp = 0:b n3))`,
		},
		{
			name: "NOT over IN differs from NOT IN",
			src:  `NOT a IN (1)`,
			want: `(not (in 0:a n1))`,
		},
		{
			name: "conjunction flattens",
			src:  `a = 1 AND b = 2 AND c = 3`,
			want: `(and (cmp = 0:a n1) (cmp = 0:b n2) (cmp = 0:c n3))`,
		},
		{
			name: "disjunction flattens",
			src:  `a = 1 OR b = 2 OR c = 3`,
			want: `(or (cmp = 0:a n1) (cmp = 0:b n2) (cmp = 0:c n3))`,
		},
		{
			name: "redundant parentheses flatten too",
			src:  `(a = 1 OR b = 2) OR c = 3`,
			want: `(or (cmp = 0:a n1) (cmp = 0:b n2) (cmp = 0:c n3))`,
		},
		{
			name: "unlike kinds do not flatten",
			src:  `(a = 1 AND b = 2) OR c = 3`,
			want: `(or (and (cmp = 0:a n1) (cmp = 0:b n2)) (cmp = 0:c n3))`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(`SELECT x FROM t WHERE ` + tc.src)
			if err != nil {
				t.Fatalf("Parse(WHERE %s) = %v, want success", tc.src, err)
			}
			got := dumpStmt(stmt)
			want := `select path(0:x) from t where ` + tc.want
			if got != want {
				t.Fatalf("WHERE %s:\n got %s\nwant %s", tc.src, got, want)
			}
		})
	}
}

// TestNestedPathSpelling checks that a parsed path renders to exactly the
// spelling query's compilePath expects, including the two cases where the
// dotted form cannot express the path and a JSON Pointer must be used instead.
func TestNestedPathSpelling(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{"single field stays a bare name", `price`, `price`},
		{"nested fields stay dotted", `user.address.city`, `user.address.city`},
		{"an array subscript forces a pointer", `tags[0]`, `/tags/0`},
		{"a subscript anywhere forces a pointer", `a.b[1].c`, `/a/b/1/c`},
		{"a key holding a dot forces a pointer", `a['x.y']`, `/a/x.y`},
		{"a key holding a slash is escaped", `a['p/q']`, `/a/p~1q`},
		{"a key holding a tilde is escaped", `a['~']`, `/a/~0`},
		{"a quoted key may hold a dot", `a."x.y"`, `/a/x.y`},
		{"an empty key is not the whole document", `""`, `/`},
		{"a keyword may be quoted as a key", `"select"."from"`, `select.from`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(`SELECT ` + tc.src + ` FROM t`)
			if err != nil {
				t.Fatalf("Parse(SELECT %s) = %v, want success", tc.src, err)
			}
			path := stmt.Columns[0].Path
			if got := path.Spec(); got != tc.want {
				t.Fatalf("Spec(%s) = %q, want %q", tc.src, got, tc.want)
			}
			if got := string(path.AppendSpec(nil)); got != tc.want {
				t.Fatalf("AppendSpec(%s) = %q, want %q", tc.src, got, tc.want)
			}
		})
	}
}

// TestWholeDocumentPathIsEmpty checks that '*' renders to the empty spec, which
// is how query names the whole document, and not to something a path compiler
// would reject.
func TestWholeDocumentPathIsEmpty(t *testing.T) {
	stmt, err := Parse(`SELECT * FROM t`)
	if err != nil {
		t.Fatalf("Parse = %v, want success", err)
	}
	path := stmt.Columns[0].Path
	if path == nil {
		t.Fatal("'*' produced no path; want a path naming the whole document")
	}
	if len(path.Segments) != 0 || path.Spec() != "" {
		t.Fatalf("'*' = %d segments, spec %q; want 0 segments and the empty spec",
			len(path.Segments), path.Spec())
	}
}

// TestRangeVariableResolution is the ambiguity rule stated as tests. It is the
// one decision this dialect has to make that a schema'd SQL does not, so each
// clause of the rule gets a case.
func TestRangeVariableResolution(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "the collection name is a range variable without AS",
			src:  `SELECT users.city FROM users`,
			want: `select path(0:city) from users`,
		},
		{
			name: "an explicit alias replaces the collection name",
			src:  `SELECT u.city FROM users AS u`,
			want: `select path(0:city) from users/u`,
		},
		{
			name: "a bare alias needs no AS",
			src:  `SELECT u.city FROM users u`,
			want: `select path(0:city) from users/u`,
		},
		{
			name: "a bare alias may be a non-reserved keyword",
			src:  `SELECT count.city FROM users count`,
			want: `select path(0:city) from users/count`,
		},
		{
			name: "an unqualified path binds to the only source",
			src:  `SELECT city FROM users u`,
			want: `select path(0:city) from users/u`,
		},
		{
			name: "a head that names no range variable stays a field",
			src:  `SELECT address.city FROM users u`,
			want: `select path(0:address.city) from users/u`,
		},
		{
			name: "a field shadowed by a range variable is reachable by qualifying it",
			src:  `SELECT u.u.city FROM users u`,
			want: `select path(0:u.city) from users/u`,
		},
		{
			name: "a range variable name without a dot is a field",
			src:  `SELECT u FROM users u`,
			want: `select path(0:u) from users/u`,
		},
		{
			name: "a subscript on a range variable name is a field",
			src:  `SELECT u[0] FROM users u`,
			want: `select path(0:/u/0) from users/u`,
		},
		{
			name: "resolution is case-sensitive",
			src:  `SELECT U.city FROM users u`,
			want: `select path(0:U.city) from users/u`,
		},
		{
			name: "a join binds each side to its own source",
			src:  `SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id`,
			want: `select path(0:name) path(1:total) from users/u orders/o join(0:id=1:user_id)`,
		},
		{
			name: "ON operands are normalized so the right side is the joined table",
			src:  `SELECT u.name FROM users u JOIN orders o ON o.user_id = u.id`,
			want: `select path(0:name) from users/u orders/o join(0:id=1:user_id)`,
		},
		{
			name: "a left outer join records its preserving operator",
			src:  `SELECT u.name, o.total FROM users u LEFT OUTER JOIN orders o ON o.user_id = u.id`,
			want: `select path(0:name) path(1:total) from users/u orders/o left-join(0:id=1:user_id)`,
		},
		{
			name: "two joins chain against earlier sources",
			src: `SELECT a.x FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k ` +
				`WHERE c.z = 1`,
			want: `select path(0:x) from a b join(0:k=1:k) c join(1:k=2:k) where (cmp = 2:z n1)`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.src)
			if err != nil {
				t.Fatalf("Parse(%q) = %v, want success", tc.src, err)
			}
			if got := dumpStmt(stmt); got != tc.want {
				t.Fatalf("Parse(%q):\n got %s\nwant %s", tc.src, got, tc.want)
			}
		})
	}
}

// TestLexicalConventions covers the parts of SQL's lexis that are easy to get
// subtly wrong and impossible to notice when they are: quote doubling, keyword
// folding, and the fact that identifier case is not folded because identifiers
// are JSON keys.
func TestLexicalConventions(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "keywords fold case",
			src:  `sElEcT a FrOm t WhErE b Is NoT nUlL oRdEr By a DeSc`,
			want: `select path(0:a) from t where (isnotnull 0:b) order 0:a:desc`,
		},
		{
			name: "identifiers do not fold case",
			src:  `SELECT Name, name FROM t`,
			want: `select path(0:Name) path(0:name) from t`,
		},
		{
			name: "a doubled single quote is one quote",
			src:  `SELECT a FROM t WHERE b = 'it''s'`,
			want: `select path(0:a) from t where (cmp = 0:b s"it's")`,
		},
		{
			name: "a string may be empty",
			src:  `SELECT a FROM t WHERE b = ''`,
			want: `select path(0:a) from t where (cmp = 0:b s"")`,
		},
		{
			name: "a string of only a doubled quote is one quote",
			src:  `SELECT a FROM t WHERE b = ''''`,
			want: `select path(0:a) from t where (cmp = 0:b s"'")`,
		},
		{
			name: "a quoted identifier may spell a keyword",
			src:  `SELECT "select" FROM "from" WHERE "where" = 1`,
			want: `select path(0:select) from from where (cmp = 0:where n1)`,
		},
		{
			name: "a doubled double quote is one quote in an identifier",
			src:  `SELECT "a""b" FROM t`,
			want: `select path(0:a"b) from t`,
		},
		{
			name: "a non-reserved keyword is still a field",
			src:  `SELECT count, missing, first FROM t WHERE last = 1`,
			want: `select path(0:count) path(0:missing) path(0:first) from t where (cmp = 0:last n1)`,
		},
		{
			name: "a reserved keyword is a field once quoted",
			src:  `SELECT "order", "group" FROM "from" WHERE "limit" = 1`,
			want: `select path(0:order) path(0:group) from from where (cmp = 0:limit n1)`,
		},
		{
			name: "AS accepts a keyword as an output name",
			src:  `SELECT a AS order FROM t`,
			want: `select path(0:a) as order from t`,
		},
		{
			name: "non-ASCII identifiers need no quoting",
			src:  `SELECT café.ville FROM t`,
			want: `select path(0:café.ville) from t`,
		},
		{
			name: "numbers keep their exact spelling",
			src:  `SELECT a FROM t WHERE b = 123456789012345678901234567890`,
			want: `select path(0:a) from t where (cmp = 0:b n123456789012345678901234567890)`,
		},
		{
			name: "a negative exponent is one literal",
			src:  `SELECT a FROM t WHERE b = -1.25E-7`,
			want: `select path(0:a) from t where (cmp = 0:b n-1.25E-7)`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.src)
			if err != nil {
				t.Fatalf("Parse(%q) = %v, want success", tc.src, err)
			}
			if got := dumpStmt(stmt); got != tc.want {
				t.Fatalf("Parse(%q):\n got %s\nwant %s", tc.src, got, tc.want)
			}
		})
	}
}

// TestOrderByAliasResolvesToItsProjection checks that a sort key naming an
// output alias is rewritten to the path that alias projects, and shares that
// projection's own node so the two can never bind to different sources.
func TestOrderByAliasResolvesToItsProjection(t *testing.T) {
	stmt, err := Parse(`SELECT u.team AS t, COUNT(*) FROM docs u GROUP BY u.team ORDER BY t`)
	if err != nil {
		t.Fatalf("Parse = %v, want success", err)
	}
	if got, want := dumpStmt(stmt),
		`select path(0:team) as t count(*) from docs/u group 0:team order 0:team:asc`; got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
	if stmt.OrderBy[0].Path != stmt.Columns[0].Path {
		t.Fatal("ORDER BY alias did not share the projection's path node")
	}
}

// TestOrderByPrefersAFieldOverAnAliasItDoesNotHave checks the negative half of
// alias resolution: a sort key that matches no alias is an ordinary path.
func TestOrderByFallsBackToAPath(t *testing.T) {
	stmt, err := Parse(`SELECT team AS t FROM docs GROUP BY team ORDER BY team`)
	if err != nil {
		t.Fatalf("Parse = %v, want success", err)
	}
	if got, want := dumpStmt(stmt),
		`select path(0:team) as t from docs group 0:team order 0:team:asc`; got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
}

// TestParserReuseProducesIdenticalTrees checks the reuse contract from the
// correctness side: a warmed Parser writing into recycled arenas must produce
// the same tree as a cold one, so nothing stale from the previous statement can
// survive into the next.
func TestParserReuseProducesIdenticalTrees(t *testing.T) {
	statements := []string{
		`SELECT a FROM t WHERE b = 1`,
		`SELECT team, SUM(score) FROM docs WHERE tier IN ('a','b','c') GROUP BY team HAVING SUM(score) > 1`,
		`SELECT u.name FROM users u JOIN orders o ON u.id = o.uid WHERE o.total BETWEEN ? AND ?`,
		`SELECT * FROM docs LIMIT 1`,
	}
	want := make([]string, len(statements))
	for i, src := range statements {
		stmt, err := Parse(src)
		if err != nil {
			t.Fatalf("Parse(%q) = %v", src, err)
		}
		want[i] = dumpStmt(stmt)
	}

	var p Parser
	var stmt SelectStmt
	// Several passes, so a chunk left dirty by an earlier statement of a
	// different shape has every chance to show up.
	for pass := 0; pass < 4; pass++ {
		for i, src := range statements {
			if err := p.Parse(&stmt, src); err != nil {
				t.Fatalf("pass %d: Parse(%q) = %v", pass, src, err)
			}
			if got := dumpStmt(&stmt); got != want[i] {
				t.Fatalf("pass %d, %q:\n got %s\nwant %s", pass, src, got, want[i])
			}
		}
	}
}

// TestParseFailureLeavesNoPartialStatement checks that a rejected statement
// clears dst, so a caller that ignores the error cannot lower half a query.
func TestParseFailureLeavesNoPartialStatement(t *testing.T) {
	var p Parser
	var stmt SelectStmt
	if err := p.Parse(&stmt, `SELECT a FROM t WHERE`); err == nil {
		t.Fatal("Parse(unterminated WHERE) = nil, want an error")
	}
	if len(stmt.Columns) != 0 || len(stmt.From) != 0 {
		t.Fatalf("failed parse left %d columns and %d sources; want an empty statement",
			len(stmt.Columns), len(stmt.From))
	}
}

// TestStatementDoesNotBorrowSource checks the documented lifetime rule: nothing
// a statement retains points into the caller's text, so overwriting the source
// after Parse cannot corrupt the tree.
func TestStatementDoesNotBorrowSource(t *testing.T) {
	source := []byte(`SELECT alpha.beta FROM docs WHERE gamma = 'delta'`)
	stmt, err := Parse(string(source))
	if err != nil {
		t.Fatalf("Parse = %v, want success", err)
	}
	want := dumpStmt(stmt)
	for i := range source {
		source[i] = 'z'
	}
	if got := dumpStmt(stmt); got != want {
		t.Fatalf("statement changed after the source was overwritten:\n got %s\nwant %s", got, want)
	}
}
