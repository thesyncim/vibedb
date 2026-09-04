package query

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	filePackedCountBenchRows = 16 << 10
	filePackedCountTestRows  = 4 << 10
	filePackedCountLabelSalt = uint64(0x9e3779b97f4a7c15)
)

// filePackedCountLabel is the 128-value dictionary lane used by both the
// query benchmark and its integration test. Multiplication by 73 permutes
// the low seven bits, so every label is spread evenly through every stripe.
func filePackedCountLabel(row int) string {
	id := uint64((row * 73) & 127)
	return fmt.Sprintf("c%016x", (id+1)*filePackedCountLabelSalt)
}

func filePackedCountNumber(row int) int {
	return ((row * 73) & 1023) - 512
}

// filePackedCountSnapshot builds the immutable compact-primary fixture used
// by the durable query benchmark and test. CreateFromPrimary leaves the
// snapshot with compact stripes and no mutable overlay or declared index.
func filePackedCountSnapshot(tb testing.TB, rows int) *durable.Snapshot {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-packed-count-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })

	source := &store.Collection{}
	for row := range rows {
		doc := fmt.Appendf(nil,
			`{"label":"c%016x","n":%d}`,
			(uint64((row*73)&127)+1)*filePackedCountLabelSalt,
			filePackedCountNumber(row),
		)
		if _, err := source.Put(fmt.Sprintf("row-%07d", row), doc); err != nil {
			tb.Fatalf("source row %d: %v", row, err)
		}
	}
	options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		tb.Fatalf("CreateFromPrimary: %v", err)
	}
	collection, err := durable.Open(file, options)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() { _ = collection.Close() })
	snapshot, err := collection.Snapshot()
	if err != nil {
		tb.Fatalf("Snapshot: %v", err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

type filePackedCountCase struct {
	name  string
	query *Query
	want  int64
}

func filePackedCountCases(rows int) []filePackedCountCase {
	return []filePackedCountCase{
		{
			name:  "label/dictionary7",
			query: Select(Count()).Where(Cmp("label", Eq, filePackedCountLabel(17))),
			want:  int64(rows / 128),
		},
		{
			name:  "n/FOR10",
			query: Select(Count()).Where(Cmp("n", Eq, int64(17))),
			want:  int64(rows / 1024),
		},
	}
}

func assertFilePackedCount(tb testing.TB, e *Exec, rows uint64, want int64) {
	tb.Helper()
	column, ok := e.Result.Column("count(*)")
	got, gotOK := int64(0), false
	if ok && len(column.Cells) == 1 {
		got, gotOK = column.Cells[0].Int64()
	}
	if e.Result.RowCount != 1 || !ok || !gotOK || got != want {
		tb.Fatalf("count result = %+v, want %d", e.Result, want)
	}
	stats := e.Stats
	if stats.Workers != 1 || stats.RowsTotal != rows ||
		stats.RowsScanned != rows || stats.TokenFilterRows != rows ||
		stats.TokenFilterFallbackRows != 0 || stats.Batches != 0 ||
		stats.PrimaryRangeBounded || stats.IndexBounded ||
		stats.IndexLookups != 0 || stats.IndexPostingPages != 0 ||
		stats.IndexCertificateRows != 0 || stats.IndexRecheckRows != 0 ||
		stats.CandidateRows != 0 || stats.CandidateChunks != 0 ||
		stats.CoveringColumns != 0 || stats.DataSkippedRows != 0 ||
		stats.DataSkippedStripes != 0 {
		tb.Fatalf("packed count stats = %+v, want one-worker full token scan", stats)
	}
}

// TestFilePackedEqualityCount checks the full-token durable query path over
// the same data whose dictionary7 and FOR10 choices are checked by storeio's
// stripe fixture. The smaller corpus has four complete 1,024-row number
// periods and one complete 4,096-row stripe.
func TestFilePackedEqualityCount(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountSnapshot(t, rows)
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()
	for _, tc := range filePackedCountCases(rows) {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.query.RunInto(&e, FromFile(snapshot)); err != nil {
				t.Fatal(err)
			}
			assertFilePackedCount(t, &e, rows, tc.want)
		})
	}
}

// BenchmarkFilePackedEqualityCount measures the actual durable query path,
// including primary graph traversal, template resolution, and packed equality
// counting. Setup and one warm RunInto are outside the timed loop; the Exec
// and snapshot are reused, with the executor forced to one worker because the
// storage-native token count is serial.
func BenchmarkFilePackedEqualityCount(b *testing.B) {
	const rows = filePackedCountBenchRows
	snapshot := filePackedCountSnapshot(b, rows)
	for _, tc := range filePackedCountCases(rows) {
		b.Run(tc.name, func(b *testing.B) {
			e := Exec{Options: ExecOptions{Workers: 1}}
			defer e.Release()
			source := FromFile(snapshot)
			if err := tc.query.RunInto(&e, source); err != nil {
				b.Fatal(err)
			}
			assertFilePackedCount(b, &e, rows, tc.want)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := tc.query.RunInto(&e, source); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			assertFilePackedCount(b, &e, rows, tc.want)
			b.ReportMetric(float64(rows), "rows")
			b.ReportMetric(float64(e.Result.RowCount), "result-rows")
		})
	}
}
