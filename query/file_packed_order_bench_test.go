package query

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

type filePackedOrderCase struct {
	name   string
	query  *Query
	want   int64
	needle int64
}

func filePackedOrderCases(
	rows int,
	value func(int) int64,
	sparseNeedle int64,
) []filePackedOrderCase {
	var cases []filePackedOrderCase
	for _, set := range []struct {
		name   string
		needle int64
	}{
		{name: "sparse", needle: sparseNeedle},
		{name: "half", needle: 0},
	} {
		for _, op := range []struct {
			name string
			op   Op
		}{
			{name: "lt", op: Lt},
			{name: "le", op: Le},
			{name: "gt", op: Gt},
			{name: "ge", op: Ge},
		} {
			want := int64(0)
			for row := 0; row < rows; row++ {
				got := value(row)
				match := false
				switch op.op {
				case Lt:
					match = got < set.needle
				case Le:
					match = got <= set.needle
				case Gt:
					match = got > set.needle
				case Ge:
					match = got >= set.needle
				}
				if match {
					want++
				}
			}
			cases = append(cases, filePackedOrderCase{
				name:   fmt.Sprintf("%s/%s", set.name, op.name),
				query:  Select(Count()).Where(Cmp("n", op.op, set.needle)),
				want:   want,
				needle: set.needle,
			})
		}
	}
	return cases
}

func runFilePackedOrderCases(
	t *testing.T,
	snapshot *durable.Snapshot,
	rows int,
	cases []filePackedOrderCase,
) {
	t.Helper()
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.query.RunInto(&e, FromFile(snapshot)); err != nil {
				t.Fatal(err)
			}
			assertFilePackedCount(t, &e, uint64(rows), tc.want)
		})
	}
}

// assertFilePackedOrderBenchmark keeps the identical benchmark source
// runnable on the pre-ordered baseline, whose generic executor reports zero
// token-filter rows. Candidate correctness tests above require the strict
// ordered token count; the benchmark additionally accepts the baseline's
// ordinary full scan so the timing comparison measures the complete query.
func assertFilePackedOrderBenchmark(
	tb testing.TB, e *Exec, rows uint64, want int64,
) {
	tb.Helper()
	column, ok := e.Result.Column("count(*)")
	got, gotOK := int64(0), false
	if ok && len(column.Cells) == 1 {
		got, gotOK = column.Cells[0].Int64()
	}
	if e.Result.RowCount != 1 || !ok || !gotOK || got != want {
		tb.Fatalf("ordered benchmark count result = %+v, want %d", e.Result, want)
	}
	stats := e.Stats
	if stats.Workers != 1 || stats.RowsTotal != rows ||
		stats.RowsScanned != rows || stats.TokenFilterFallbackRows != 0 ||
		(stats.TokenFilterRows != 0 && stats.TokenFilterRows != rows) ||
		stats.PrimaryRangeBounded || stats.IndexBounded ||
		stats.IndexLookups != 0 || stats.IndexPostingPages != 0 ||
		stats.IndexCertificateRows != 0 || stats.IndexRecheckRows != 0 ||
		stats.CandidateRows != 0 || stats.CandidateChunks != 0 ||
		stats.CoveringColumns != 0 || stats.DataSkippedRows != 0 ||
		stats.DataSkippedStripes != 0 {
		tb.Fatalf("ordered benchmark stats = %+v", stats)
	}
	if os.Getenv("VIBEDB_EXPECT_ORDERED") == "1" && stats.TokenFilterRows != rows {
		tb.Fatalf("candidate did not use ordered token lane: %+v", stats)
	}
	if os.Getenv("VIBEDB_EXPECT_ORDERED") == "1" && stats.Batches != 0 {
		tb.Fatalf("candidate ordered lane unexpectedly batched: %+v", stats)
	}
}

// TestFilePackedOrderedCount exercises the strict durable FOR10 ordered
// COUNT lane at sparse and roughly half-selective thresholds for all four
// signed operators. The fixture is the same clean immutable snapshot used by
// the existing equality benchmark.
func TestFilePackedOrderedCount(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountSnapshot(t, rows)
	cases := filePackedOrderCases(rows, func(row int) int64 {
		return int64(filePackedCountNumber(row))
	}, -500)
	runFilePackedOrderCases(t, snapshot, rows, cases)
}

// TestFilePackedOrderedCountWide exercises signed FOR16 values, including a
// negative selective threshold and a zero threshold crossing both signs.
func TestFilePackedOrderedCountWide(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountWideSnapshot(t, rows)
	cases := filePackedOrderCases(rows, filePackedCountWideNumber, -32760)
	runFilePackedOrderCases(t, snapshot, rows, cases)
}

func TestFilePackedOrderedCountDeclinesNonFOR(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountWideSnapshot(t, rows)
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()
	query := Select(Count()).Where(Cmp("label", Lt, int64(0)))
	if err := query.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	column, ok := e.Result.Column("count(*)")
	if !ok || len(column.Cells) != 1 {
		t.Fatalf("declined ordered result = %+v", e.Result)
	}
	got, ok := column.Cells[0].Int64()
	if !ok || got != 0 {
		t.Fatalf("declined dictionary ordering count=%d ok=%t, want 0", got, ok)
	}
	if e.Stats.TokenFilterRows != 0 || e.Stats.TokenFilterFallbackRows != 0 ||
		e.Stats.RowsScanned != rows {
		t.Fatalf("declined dictionary ordering stats=%+v, want generic full scan", e.Stats)
	}

	query = Select(Count()).Where(Cmp("n", Lt, Number("0.0")))
	if err := query.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if e.Stats.TokenFilterRows != 0 || e.Stats.TokenFilterFallbackRows != 0 {
		t.Fatalf("fractional ordered literal entered token lane: %+v", e.Stats)
	}

	query = Select(Count()).Where(Cmp("", Gt, int64(0)))
	if err := query.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	column, ok = e.Result.Column("count(*)")
	if !ok || len(column.Cells) != 1 {
		t.Fatalf("root ordered result = %+v", e.Result)
	}
	got, ok = column.Cells[0].Int64()
	if !ok || got != rows {
		t.Fatalf("root ordered count=%d ok=%t, want %d", got, ok, rows)
	}
	if e.Stats.TokenFilterRows != 0 || e.Stats.TokenFilterFallbackRows != 0 ||
		e.Stats.RowsScanned != rows {
		t.Fatalf("root ordered predicate entered token lane: %+v", e.Stats)
	}

	query = Select(Count()).Where(Cmp("/", Lt, int64(0)))
	if err := query.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	column, ok = e.Result.Column("count(*)")
	if !ok || len(column.Cells) != 1 {
		t.Fatalf("empty-key ordered result = %+v", e.Result)
	}
	got, ok = column.Cells[0].Int64()
	if !ok || got != 0 {
		t.Fatalf("empty-key ordered count=%d ok=%t, want 0", got, ok)
	}
	if e.Stats.TokenFilterRows != 0 || e.Stats.TokenFilterFallbackRows != 0 ||
		e.Stats.RowsScanned != rows {
		t.Fatalf("empty-key ordered predicate entered token lane: %+v", e.Stats)
	}
}

func benchmarkFilePackedOrderCases(
	b *testing.B,
	snapshot *durable.Snapshot,
	rows int,
	cases []filePackedOrderCase,
) {
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			e := Exec{Options: ExecOptions{Workers: 1}}
			defer e.Release()
			source := FromFile(snapshot)
			if err := tc.query.RunInto(&e, source); err != nil {
				b.Fatal(err)
			}
			assertFilePackedOrderBenchmark(b, &e, uint64(rows), tc.want)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := tc.query.RunInto(&e, source); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			assertFilePackedOrderBenchmark(b, &e, uint64(rows), tc.want)
			b.ReportMetric(float64(rows), "rows")
			b.ReportMetric(float64(e.Result.RowCount), "result-rows")
		})
	}
}

func BenchmarkFilePackedOrderedCount(b *testing.B) {
	const rows = filePackedCountBenchRows
	snapshot := filePackedCountSnapshot(b, rows)
	cases := filePackedOrderCases(rows, func(row int) int64 {
		return int64(filePackedCountNumber(row))
	}, -500)
	benchmarkFilePackedOrderCases(b, snapshot, rows, cases)
}

func BenchmarkFilePackedOrderedCountWide(b *testing.B) {
	const rows = filePackedCountBenchRows
	snapshot := filePackedCountWideSnapshot(b, rows)
	cases := filePackedOrderCases(rows, filePackedCountWideNumber, -32760)
	benchmarkFilePackedOrderCases(b, snapshot, rows, cases)
}
