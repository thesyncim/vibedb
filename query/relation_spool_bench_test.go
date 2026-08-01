package query

import "testing"

func BenchmarkRelationSpoolFoundation(b *testing.B) {
	rows := [][]string{
		{`1`, `{"x":"one"}`},
		{`3.00`, `{"x":"three"}`},
		{`2e0`, `{"x":"two"}`},
		{`null`, `{"x":"nil"}`},
	}
	spool := buildRelationSpoolForTest(b, rows)
	segment := mustSegment(b,
		`{"0":1,"1":{"x":"one"}}`,
		`{"0":3.00,"1":{"x":"three"}}`,
		`{"0":2e0,"1":{"x":"two"}}`,
		`{"0":null,"1":{"x":"nil"}}`,
	)
	plan := Select(Path("/0"), Path("/1/x")).
		Where(Cmp("/0", Gt, 1)).
		OrderBy("/0", Desc)

	b.Run("ordinary-segment", func(b *testing.B) {
		var exec Exec
		if err := plan.RunInto(&exec, FromSegment(segment)); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := plan.RunInto(&exec, FromSegment(segment)); err != nil {
				b.Fatal(err)
			}
			sqlSink += exec.Result.RowCount
		}
	})

	b.Run("columnar-relation", func(b *testing.B) {
		var exec Exec
		if err := plan.RunInto(&exec, fromRelationSpool(&spool)); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := plan.RunInto(&exec, fromRelationSpool(&spool)); err != nil {
				b.Fatal(err)
			}
			sqlSink += exec.Result.RowCount
		}
	})
}
