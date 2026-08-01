package sql

import "testing"

// The statement shapes the package is measured on. They are shared with
// the allocation tests so the two never drift apart: a shape that is fast but
// untested for allocation, or allocation-free but unmeasured, would be half a
// result.
const (
	benchSimple   = `SELECT name FROM docs`
	benchFiltered = `SELECT name, score FROM docs WHERE active = TRUE AND score >= 10 AND tier <> 'free'`
	benchJoin     = `SELECT u.name, o.total FROM users AS u JOIN orders AS o ON u.id = o.user_id ` +
		`WHERE o.total > ? AND u.address.city = 'Lisbon'`
	benchLeftJoin = `SELECT u.name, o.total FROM users AS u LEFT JOIN orders AS o ON u.id = o.user_id ` +
		`WHERE u.address.city = 'Lisbon'`
	benchGrouped = `SELECT team, COUNT(*), SUM(score) AS total FROM docs WHERE tier IN ('pro', 'team') ` +
		`GROUP BY team HAVING SUM(score) > 100 ORDER BY team DESC LIMIT 10`
	benchRich = `SELECT u.profile.name FROM users u WHERE u.meta @> {"tier": "pro"} ` +
		`AND u.tags[0] IS NOT MISSING AND u.age BETWEEN ? AND ? AND u.rank IN (1, 2, 3, 5, 8)`
	benchInsertDocument = `INSERT INTO docs VALUES (?), ({"id":"inline","n":2})`
	benchInsertFlat     = `INSERT INTO docs (id, name, active, score) VALUES (?, 'Ana', TRUE, 42), (?, 'Bo', FALSE, 7)`
	benchUpdate         = `UPDATE docs SET "$doc" = ? WHERE tenant = ? AND score >= 10`
	benchDelete         = `DELETE FROM docs WHERE tenant = ? AND state IN ('done', 'failed')`
	benchCreateTable    = `CREATE TABLE docs (id STRING PRIMARY KEY, tenant STRING NOT NULL, score NUMBER, meta OBJECT)`
	benchCreateIndex    = `CREATE INDEX by_tenant_state ON docs (tenant, meta.state)`
	benchSetExpression  = `(SELECT id AS key FROM live_docs WHERE tenant = ? ORDER BY key LIMIT ?) ` +
		`UNION ALL SELECT id FROM archive WHERE tenant IN (?, ?) ` +
		`INTERSECT DISTINCT (SELECT id FROM allowed WHERE active = TRUE LIMIT ?) ` +
		`ORDER BY key OFFSET ?`
)

// BenchmarkParse measures a warmed Parser writing into its own arenas, which is
// the shape a prepared-statement cache uses.
func BenchmarkParse(b *testing.B) {
	cases := []struct {
		name string
		src  string
	}{
		{"Simple", benchSimple},
		{"Filtered", benchFiltered},
		{"Join", benchJoin},
		{"GroupedAggregate", benchGrouped},
		{"Rich", benchRich},
		{"SetExpression", benchSetExpression},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var p Parser
			var stmt SelectStmt
			if err := p.Parse(&stmt, tc.src); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(tc.src)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.Parse(&stmt, tc.src); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkParseStatement measures the warmed all-statement parser used by the
// SQL adapters. INSERT rows are included explicitly because their retained
// operand vectors use a different arena path from SELECT predicates.
func BenchmarkParseStatement(b *testing.B) {
	cases := []struct {
		name string
		src  string
	}{
		{"InsertDocument", benchInsertDocument},
		{"InsertFlat", benchInsertFlat},
		{"Update", benchUpdate},
		{"Delete", benchDelete},
		{"CreateTable", benchCreateTable},
		{"CreateIndex", benchCreateIndex},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var p Parser
			var stmt Statement
			if err := p.ParseStatement(&stmt, tc.src); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(tc.src)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.ParseStatement(&stmt, tc.src); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkParseOneShot measures the package-level entry point, which owns
// everything it returns. It is the honest number for a caller that parses once
// and keeps the statement, and the contrast with BenchmarkParse is the price of
// that ownership.
func BenchmarkParseOneShot(b *testing.B) {
	cases := []struct {
		name string
		src  string
	}{
		{"Simple", benchSimple},
		{"GroupedAggregate", benchGrouped},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(tc.src)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				stmt, err := Parse(tc.src)
				if err != nil {
					b.Fatal(err)
				}
				_ = stmt
			}
		})
	}
}

// BenchmarkParseRejection measures the refusal path, which a driver exercises
// whenever an application sends SQL outside the subset.
func BenchmarkParseRejection(b *testing.B) {
	const src = `SELECT a FROM t WHERE b SIMILAR TO 'x%'`
	var p Parser
	var stmt SelectStmt
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := p.Parse(&stmt, src); err == nil {
			b.Fatal("expected a rejection")
		}
	}
}
