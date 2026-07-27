package sql

import "testing"

// The four statement shapes the package is measured on. They are shared with
// the allocation tests so the two never drift apart: a shape that is fast but
// untested for allocation, or allocation-free but unmeasured, would be half a
// result.
const (
	benchSimple   = `SELECT name FROM docs`
	benchFiltered = `SELECT name, score FROM docs WHERE active = TRUE AND score >= 10 AND tier <> 'free'`
	benchJoin     = `SELECT u.name, o.total FROM users AS u JOIN orders AS o ON u.id = o.user_id ` +
		`WHERE o.total > ? AND u.address.city = 'Lisbon'`
	benchGrouped = `SELECT team, COUNT(*), SUM(score) AS total FROM docs WHERE tier IN ('pro', 'team') ` +
		`GROUP BY team HAVING SUM(score) > 100 ORDER BY team DESC LIMIT 10`
	benchRich = `SELECT u.profile.name FROM users u WHERE u.meta @> {"tier": "pro"} ` +
		`AND u.tags[0] IS NOT MISSING AND u.age BETWEEN ? AND ? AND u.rank IN (1, 2, 3, 5, 8)`
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
	const src = `SELECT a FROM t WHERE b LIKE 'x%'`
	var p Parser
	var stmt SelectStmt
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := p.Parse(&stmt, src); err == nil {
			b.Fatal("expected a rejection")
		}
	}
}
