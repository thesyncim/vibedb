package query

import "testing"

func BenchmarkSQLSetLoweringWarm(b *testing.B) {
	segment := mustSegment(b,
		`{"v":1}`, `{"v":2}`, `{"v":2}`, `{"v":4}`,
	)
	statement, err := PrepareStatement(
		`SELECT v AS value FROM docs WHERE v >= ? ` +
			`UNION DISTINCT SELECT v FROM docs WHERE v <= ? ` +
			`ORDER BY value DESC LIMIT ? OFFSET ?`,
	)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	low, high, limit, offset := int64(1), int64(4), int64(3), int64(1)
	args := []any{&low, &high, &limit, &offset}
	var execution Exec
	if _, err = statement.RunInto(&execution, FromSegment(segment), args); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err = statement.RunInto(&execution, FromSegment(segment), args); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if execution.Result.RowCount != 3 {
		b.Fatalf("rows = %d, want 3", execution.Result.RowCount)
	}
}
