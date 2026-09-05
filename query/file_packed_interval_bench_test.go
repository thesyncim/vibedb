package query

import (
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

type filePackedIntervalCase struct {
	name  string
	query *Query
	want  int64
}

func filePackedIntegerIntervalCases(
	rows int,
	value func(int) int64,
) []filePackedIntervalCase {
	const (
		minInt64Value = int64(-1 << 63)
		maxInt64Value = int64(1<<63 - 1)
	)
	type bounds struct {
		name       string
		lower      int64
		upper      int64
		lowerOp    Op
		upperOp    Op
		unbounded  bool
		swapInputs bool
	}
	boundsList := []bounds{
		{name: "empty", lower: 100, upper: 101, lowerOp: Gt, upperOp: Lt},
		{name: "sparse", lower: 17, upper: 18, lowerOp: Ge, upperOp: Lt},
		{name: "half", lower: -513, upper: -1, lowerOp: Gt, upperOp: Le},
		{name: "full", lower: minInt64Value, upper: maxInt64Value, lowerOp: Ge, upperOp: Le, unbounded: true, swapInputs: true},
	}
	cases := make([]filePackedIntervalCase, 0, len(boundsList))
	for _, bound := range boundsList {
		want := int64(0)
		for row := 0; row < rows; row++ {
			got := value(row)
			lowerMatch := false
			switch bound.lowerOp {
			case Gt:
				lowerMatch = got > bound.lower
			case Ge:
				lowerMatch = got >= bound.lower
			}
			upperMatch := false
			switch bound.upperOp {
			case Lt:
				upperMatch = got < bound.upper
			case Le:
				upperMatch = got <= bound.upper
			}
			if lowerMatch && (bound.unbounded || upperMatch) {
				want++
			}
		}
		lower := Cmp("n", bound.lowerOp, bound.lower)
		upper := Cmp("n", bound.upperOp, bound.upper)
		predicate := And(lower, upper)
		if bound.swapInputs {
			predicate = And(upper, lower)
		}
		cases = append(cases, filePackedIntervalCase{
			name: bound.name, query: Select(Count()).Where(predicate), want: want,
		})
	}
	return cases
}

func runFilePackedIntegerIntervalCases(
	t *testing.T,
	snapshot *durable.Snapshot,
	rows int,
	cases []filePackedIntervalCase,
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

// Benchmark assertions accept the generic executor's full scan so the same
// query source can be compiled against a pre-interval baseline. Candidate
// timing runs set VIBEDB_EXPECT_INTERVAL=1 to require the storage-native lane.
func assertFilePackedIntegerIntervalBenchmark(
	tb testing.TB, e *Exec, rows uint64, want int64,
) {
	tb.Helper()
	column, ok := e.Result.Column("count(*)")
	got, gotOK := int64(0), false
	if ok && len(column.Cells) == 1 {
		got, gotOK = column.Cells[0].Int64()
	}
	if e.Result.RowCount != 1 || !ok || !gotOK || got != want {
		tb.Fatalf("interval benchmark count result = %+v, want %d", e.Result, want)
	}
	stats := e.Stats
	if stats.Workers != 1 || stats.RowsTotal != rows || stats.RowsScanned != rows ||
		stats.TokenFilterFallbackRows != 0 ||
		(stats.TokenFilterRows != 0 && stats.TokenFilterRows != rows) ||
		stats.PrimaryRangeBounded || stats.IndexBounded || stats.IndexLookups != 0 ||
		stats.IndexPostingPages != 0 || stats.IndexCertificateRows != 0 ||
		stats.IndexRecheckRows != 0 || stats.CandidateRows != 0 ||
		stats.CandidateChunks != 0 || stats.CoveringColumns != 0 ||
		stats.DataSkippedRows != 0 || stats.DataSkippedStripes != 0 {
		tb.Fatalf("interval benchmark stats = %+v", stats)
	}
	if os.Getenv("VIBEDB_EXPECT_INTERVAL") == "1" && stats.TokenFilterRows != rows {
		tb.Fatalf("candidate did not use interval token lane: %+v", stats)
	}
}

func benchmarkFilePackedIntegerIntervalCases(
	b *testing.B,
	snapshot *durable.Snapshot,
	rows int,
	cases []filePackedIntervalCase,
) {
	b.Helper()
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			e := Exec{Options: ExecOptions{Workers: 1}}
			defer e.Release()
			source := FromFile(snapshot)
			if err := tc.query.RunInto(&e, source); err != nil {
				b.Fatal(err)
			}
			assertFilePackedIntegerIntervalBenchmark(b, &e, uint64(rows), tc.want)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := tc.query.RunInto(&e, source); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			assertFilePackedIntegerIntervalBenchmark(b, &e, uint64(rows), tc.want)
			b.ReportMetric(float64(rows), "rows")
			b.ReportMetric(float64(e.Result.RowCount), "result-rows")
		})
	}
}

func TestFilePackedIntegerIntervalCount(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountSnapshot(t, rows)
	runFilePackedIntegerIntervalCases(t, snapshot, rows,
		filePackedIntegerIntervalCases(rows, func(row int) int64 {
			return int64(filePackedCountNumber(row))
		}),
	)
}

func TestFilePackedIntegerIntervalCountWide(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountWideSnapshot(t, rows)
	runFilePackedIntegerIntervalCases(t, snapshot, rows,
		filePackedIntegerIntervalCases(rows, filePackedCountWideNumber),
	)
}

func TestFilePackedIntegerIntervalDeclinesUnsupported(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountWideSnapshot(t, rows)
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()
	cases := []struct {
		name  string
		query *Query
	}{
		{
			name: "dictionary",
			query: Select(Count()).Where(And(
				Cmp("label", Ge, int64(0)), Cmp("label", Lt, int64(1)),
			)),
		},
		{
			name: "fractional",
			query: Select(Count()).Where(And(
				Cmp("n", Ge, Number("-1.0")), Cmp("n", Lt, Number("1.0")),
			)),
		},
		{
			name: "root",
			query: Select(Count()).Where(And(
				Cmp("", Ge, int64(0)), Cmp("", Lt, int64(1)),
			)),
		},
		{
			name: "mismatched-path",
			query: Select(Count()).Where(And(
				Cmp("n", Ge, int64(0)), Cmp("label", Lt, int64(1)),
			)),
		},
		{
			name: "residual",
			query: Select(Count()).Where(And(
				Cmp("n", Ge, int64(0)), Cmp("n", Lt, int64(1)), Exists("n"),
			)),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.query.RunInto(&e, FromFile(snapshot)); err != nil {
				t.Fatal(err)
			}
			column, ok := e.Result.Column("count(*)")
			if !ok || len(column.Cells) != 1 {
				t.Fatalf("declined interval result = %+v", e.Result)
			}
			if _, ok := column.Cells[0].Int64(); !ok {
				t.Fatalf("declined interval count is not integer: %+v", e.Result)
			}
			if e.Stats.RowsScanned != rows || e.Stats.TokenFilterRows != 0 ||
				e.Stats.TokenFilterFallbackRows != 0 {
				t.Fatalf("declined %s stats=%+v", tc.name, e.Stats)
			}
		})
	}
}

func BenchmarkFilePackedIntegerIntervalCount(b *testing.B) {
	const rows = filePackedCountBenchRows
	snapshot := filePackedCountSnapshot(b, rows)
	benchmarkFilePackedIntegerIntervalCases(b, snapshot, rows,
		filePackedIntegerIntervalCases(rows, func(row int) int64 {
			return int64(filePackedCountNumber(row))
		}),
	)
}

func BenchmarkFilePackedIntegerIntervalCountWide(b *testing.B) {
	const rows = filePackedCountBenchRows
	snapshot := filePackedCountWideSnapshot(b, rows)
	benchmarkFilePackedIntegerIntervalCases(b, snapshot, rows,
		filePackedIntegerIntervalCases(rows, filePackedCountWideNumber),
	)
}
