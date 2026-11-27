package query

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/document"
)

func buildExtAggBenchSegment(tb testing.TB, rows, groups, tags int) *store.Segment {
	tb.Helper()
	seg := &store.Segment{ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}
	for i := range rows {
		dept := fmt.Sprintf("dept_%02d", i%groups)
		tag := fmt.Sprintf("tag_%02d", (i*7)%tags)
		active := i%3 != 0
		amount := (i%500)*10 + (i % 7)
		doc := fmt.Sprintf(
			`{"id":%d,"dept":%q,"tag":%q,"active":%t,"amount":%d,"ts":%d}`,
			i, dept, tag, active, amount, rows-i,
		)
		if _, err := seg.Append([]byte(doc)); err != nil {
			tb.Fatalf("append doc %d: %v", i, err)
		}
	}
	return seg
}

func countCursorRows(cur *Cursor) int {
	n := 0
	for cur.Next() {
		n++
	}
	return n
}

func BenchmarkExtAggGroupedCountDistinctSumFilter(b *testing.B) {
	seg := buildExtAggBenchSegment(b, 5000, 50, 23)
	src := FromSegment(seg)
	stmt, err := PrepareStatement(`
		SELECT dept,
		       COUNT(DISTINCT tag) AS dtags,
		       SUM(amount) FILTER (WHERE active = TRUE) AS active_sum,
		       AVG(amount) AS avg_amt,
		       MIN(amount) AS min_amt,
		       MAX(amount) AS max_amt
		FROM events
		GROUP BY dept
		HAVING COUNT(DISTINCT tag) >= 5
		ORDER BY active_sum DESC, dept ASC
		LIMIT 20
	`)
	if err != nil {
		b.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()

	// Warmup
	cur, err := stmt.RunInto(&exec, src, nil)
	if err != nil {
		b.Fatalf("warmup RunInto: %v", err)
	}
	if n := countCursorRows(&cur); n != 20 {
		b.Fatalf("unexpected row count: got %d, want 20", n)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := stmt.RunInto(&exec, src, nil)
		if err != nil {
			b.Fatalf("RunInto: %v", err)
		}
		if n := countCursorRows(&cur); n != 20 {
			b.Fatalf("unexpected row count: %d", n)
		}
	}
}

func BenchmarkExtAggStringArrayJsonBoolAgg(b *testing.B) {
	seg := buildExtAggBenchSegment(b, 2000, 50, 10)
	src := FromSegment(seg)
	stmt, err := PrepareStatement(`
		SELECT dept,
		       STRING_AGG(DISTINCT tag, ',') AS tags,
		       ARRAY_AGG(DISTINCT tag) AS tag_arr,
		       JSON_AGG(amount) FILTER (WHERE active = TRUE) AS amounts,
		       BOOL_AND(active) AS all_active,
		       BOOL_OR(active) AS any_active
		FROM events
		GROUP BY dept
		ORDER BY dept ASC
	`)
	if err != nil {
		b.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()

	cur, err := stmt.RunInto(&exec, src, nil)
	if err != nil {
		b.Fatalf("warmup RunInto: %v", err)
	}
	if n := countCursorRows(&cur); n != 50 {
		b.Fatalf("unexpected row count: got %d, want 50", n)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := stmt.RunInto(&exec, src, nil)
		if err != nil {
			b.Fatalf("RunInto: %v", err)
		}
		if n := countCursorRows(&cur); n != 50 {
			b.Fatalf("unexpected row count: %d", n)
		}
	}
}

func BenchmarkExtAggUngroupedCountDistinct(b *testing.B) {
	seg := buildExtAggBenchSegment(b, 5000, 50, 200)
	src := FromSegment(seg)
	stmt, err := PrepareStatement(`
		SELECT COUNT(DISTINCT tag) AS dtags,
		       SUM(DISTINCT amount) AS dsum,
		       AVG(amount) AS avg_amt
		FROM events
	`)
	if err != nil {
		b.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()

	cur, err := stmt.RunInto(&exec, src, nil)
	if err != nil {
		b.Fatalf("warmup RunInto: %v", err)
	}
	if n := countCursorRows(&cur); n != 1 {
		b.Fatalf("unexpected row count: got %d, want 1", n)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := stmt.RunInto(&exec, src, nil)
		if err != nil {
			b.Fatalf("RunInto: %v", err)
		}
		if n := countCursorRows(&cur); n != 1 {
			b.Fatalf("unexpected row count: %d", n)
		}
	}
}

func BenchmarkDistinctOnWarm(b *testing.B) {
	seg := buildExtAggBenchSegment(b, 5000, 50, 20)
	src := FromSegment(seg)
	stmt, err := PrepareStatement(`
		SELECT DISTINCT ON (dept) dept, id, ts
		FROM events
		ORDER BY dept ASC, ts DESC
	`)
	if err != nil {
		b.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	defer exec.Release()

	cur, err := stmt.RunInto(&exec, src, nil)
	if err != nil {
		b.Fatalf("warmup RunInto: %v", err)
	}
	if n := countCursorRows(&cur); n != 50 {
		b.Fatalf("unexpected row count: got %d, want 50", n)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := stmt.RunInto(&exec, src, nil)
		if err != nil {
			b.Fatalf("RunInto: %v", err)
		}
		if n := countCursorRows(&cur); n != 50 {
			b.Fatalf("unexpected row count: %d", n)
		}
	}
}
