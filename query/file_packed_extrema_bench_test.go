package query

import (
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

type filePackedExtremaBenchmarkCase struct {
	name     string
	query    *Query
	wantMin  int64
	wantMax  int64
	checkMin bool
	checkMax bool
}

func filePackedExtremaBenchmarkExpected(
	rows int, value func(int) int64,
) (minimum, maximum int64) {
	minimum, maximum = value(0), value(0)
	for row := 1; row < rows; row++ {
		current := value(row)
		if current < minimum {
			minimum = current
		}
		if current > maximum {
			maximum = current
		}
	}
	return minimum, maximum
}

func filePackedExtremaBenchmarkCases(
	rows int, value func(int) int64, suffix string,
) []filePackedExtremaBenchmarkCase {
	minimum, maximum := filePackedExtremaBenchmarkExpected(rows, value)
	return []filePackedExtremaBenchmarkCase{
		{
			name:     "min/" + suffix,
			query:    Select(Min("n")),
			wantMin:  minimum,
			checkMin: true,
		},
		{
			name:     "max/" + suffix,
			query:    Select(Max("n")),
			wantMax:  maximum,
			checkMax: true,
		},
		{
			name:     "min-max/" + suffix,
			query:    Select(Min("n"), Max("n")),
			wantMin:  minimum,
			wantMax:  maximum,
			checkMin: true,
			checkMax: true,
		},
	}
}

func assertFilePackedExtremaBenchmark(
	tb testing.TB,
	e *Exec,
	rows uint64,
	tc filePackedExtremaBenchmarkCase,
) {
	tb.Helper()
	if e.Result.RowCount != 1 {
		tb.Fatalf("extrema benchmark result rows=%d, want 1", e.Result.RowCount)
	}
	if tc.checkMin {
		column, ok := e.Result.Column("min(n)")
		if !ok || len(column.Cells) != 1 {
			tb.Fatalf("min(n) result=%+v", e.Result)
		}
		got, isInt := column.Cells[0].Int64()
		if !isInt || got != tc.wantMin {
			tb.Fatalf("min(n)=%d/%t, want %d", got, isInt, tc.wantMin)
		}
	}
	if tc.checkMax {
		column, ok := e.Result.Column("max(n)")
		if !ok || len(column.Cells) != 1 {
			tb.Fatalf("max(n) result=%+v", e.Result)
		}
		got, isInt := column.Cells[0].Int64()
		if !isInt || got != tc.wantMax {
			tb.Fatalf("max(n)=%d/%t, want %d", got, isInt, tc.wantMax)
		}
	}
	stats := e.Stats
	if stats.Workers != 1 || stats.RowsTotal != rows || stats.RowsScanned != rows ||
		stats.TokenFilterRows != 0 ||
		stats.TokenFilterFallbackRows != 0 || stats.PrimaryRangeBounded ||
		stats.IndexBounded || stats.IndexLookups != 0 ||
		stats.IndexPostingPages != 0 || stats.IndexCertificateRows != 0 ||
		stats.IndexRecheckRows != 0 || stats.CandidateRows != 0 ||
		stats.CandidateChunks != 0 || stats.DataSkippedRows != 0 ||
		stats.DataSkippedStripes != 0 ||
		(stats.CoveringColumns != 0 && stats.CoveringColumns != 1) {
		tb.Fatalf("extrema benchmark stats=%+v", stats)
	}
	if os.Getenv("VIBEDB_EXPECT_EXTREMA") == "1" && stats.CoveringColumns != 1 {
		tb.Fatalf("candidate did not use extrema token lane: %+v", stats)
	}
	if os.Getenv("VIBEDB_EXPECT_EXTREMA") == "1" && stats.Batches != 0 {
		tb.Fatalf("candidate extrema lane unexpectedly batched: %+v", stats)
	}
}

func benchmarkFilePackedExtremaCases(
	b *testing.B,
	snapshot *durable.Snapshot,
	rows int,
	cases []filePackedExtremaBenchmarkCase,
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
			assertFilePackedExtremaBenchmark(b, &e, uint64(rows), tc)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := tc.query.RunInto(&e, source); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			assertFilePackedExtremaBenchmark(b, &e, uint64(rows), tc)
			b.ReportMetric(float64(rows), "rows")
			b.ReportMetric(float64(e.Result.RowCount), "result-rows")
		})
	}
}

func BenchmarkFilePackedIntegerExtremaCount(b *testing.B) {
	const rows = filePackedCountBenchRows
	snapshot := filePackedCountSnapshot(b, rows)
	cases := filePackedExtremaBenchmarkCases(rows, func(row int) int64 {
		return int64(filePackedCountNumber(row))
	}, "FOR10")
	benchmarkFilePackedExtremaCases(b, snapshot, rows, cases)
}

func BenchmarkFilePackedIntegerExtremaCountWide(b *testing.B) {
	const rows = filePackedCountBenchRows
	snapshot := filePackedCountWideSnapshot(b, rows)
	cases := filePackedExtremaBenchmarkCases(rows, filePackedCountWideNumber, "FOR16")
	benchmarkFilePackedExtremaCases(b, snapshot, rows, cases)
}
