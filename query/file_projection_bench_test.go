package query

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	fileProjectionBenchRows = 8192
	fileProjectionBenchPage = 64
)

// fileProjectionKeepAll forces the ordinary durable batch executor to remain
// a correctness oracle. It preserves every key, while FromFileFiltered
// bypasses the native primary-range projection lane on the same immutable
// snapshot. The benchmark comparison is range-source on each revision; this
// generic path must not be reported as a before/after performance arm.
type fileProjectionKeepAll struct{}

func (fileProjectionKeepAll) Keep([]byte) bool { return true }

func fileProjectionBenchSnapshot(tb testing.TB) *durable.Snapshot {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-projection-")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })

	source := &store.Collection{}
	payload := strings.Repeat("p", 256)
	for row := range fileProjectionBenchRows {
		id := fmt.Sprintf("%05d", row)
		document := fmt.Appendf(nil,
			`{"id":%q,"bucket":%d,"score":%d,"payload":%q}`,
			id, row%16, row, payload,
		)
		if _, err := source.Put(id, document); err != nil {
			tb.Fatalf("source row %d: %v", row, err)
		}
	}
	options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		tb.Fatalf("CreateFromPrimary: %v", err)
	}
	database, err := durable.Open(file, options)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() { _ = database.Close() })
	snapshot, err := database.Snapshot()
	if err != nil {
		tb.Fatalf("Snapshot: %v", err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

func fileProjectionBenchQuery() *Query {
	return Select(Path("/id"), Path("/bucket"), Path("/score")).
		Where(And(
			Cmp("/id", Ge, "00000"),
			Cmp("/id", Lt, "00064"),
		)).
		OrderBy("/id", Asc).
		Limit(fileProjectionBenchPage)
}

func fileProjectionBenchRange() FileRangeSource {
	span := NewFileRangeSource([]byte("00000"), []byte("00064"), false)
	span.BindPrimaryOrder("/id")
	span.BindPrimaryPredicate("/id")
	return span
}

// fileProjectionResultKey compares headers, scalar kinds, and exact JSON
// bytes. Both arms therefore have to produce the same ordered snapshot result
// before any benchmark numbers are considered.
func fileProjectionResultKey(result Result) string {
	var key strings.Builder
	for _, column := range result.Columns {
		key.WriteByte('|')
		key.WriteString(column.Header)
	}
	key.WriteByte('\n')
	for row := 0; row < result.RowCount; row++ {
		for _, column := range result.Columns {
			cell := column.Cells[row]
			fmt.Fprintf(&key, "%d:%s|", cell.Kind(), cell.JSON())
		}
		key.WriteByte('\n')
	}
	return key.String()
}

func verifyFileProjectionResult(tb testing.TB, execution *Exec) string {
	tb.Helper()
	if execution.Result.RowCount != fileProjectionBenchPage || len(execution.Result.Columns) != 3 {
		tb.Fatalf("result shape=%d rows/%d columns, want %d/3",
			execution.Result.RowCount, len(execution.Result.Columns), fileProjectionBenchPage)
	}
	for row := range fileProjectionBenchPage {
		id := execution.Result.Columns[0].Cells[row].String()
		bucket, bucketOK := execution.Result.Columns[1].Cells[row].Int64()
		score, scoreOK := execution.Result.Columns[2].Cells[row].Int64()
		wantID := fmt.Sprintf("%05d", row)
		if execution.Result.Columns[0].Cells[row].Kind() != TypeString || !bucketOK || !scoreOK || id != fmt.Sprintf("%q", wantID) ||
			bucket != int64(row%16) || score != int64(row) {
			tb.Fatalf("row %d=(%q,%d,%d), want (%q,%d,%d)",
				row, id, bucket, score, wantID, row%16, row)
		}
	}
	return fileProjectionResultKey(execution.Result)
}

// projectedRowsStat reads the optional candidate stat without making this
// benchmark file uncompilable on the immutable pre-projection revision. The
// candidate correctness test requires the field once it is present; baseline
// benchmark runs simply omit the metric.
func projectedRowsStat(stats any) (uint64, bool) {
	value := reflect.ValueOf(stats)
	if !value.IsValid() {
		return 0, false
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return 0, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return 0, false
	}
	field := value.FieldByName("ProjectedRows")
	if !field.IsValid() || field.Kind() != reflect.Uint64 {
		return 0, false
	}
	return field.Uint(), true
}

func TestFileProjectionMatchesGenericOnOneSnapshot(t *testing.T) {
	snapshot := fileProjectionBenchSnapshot(t)
	plan := fileProjectionBenchQuery()
	filter := NewFileFilterSource(fileProjectionKeepAll{})
	span := fileProjectionBenchRange()

	var baseline, projected Exec
	defer baseline.Release()
	defer projected.Release()
	var baselineCancel, projectedCancel CancelFlag
	baseline.Options = ExecOptions{Workers: 1, Cancel: &baselineCancel}
	projected.Options = ExecOptions{Workers: 1, Cancel: &projectedCancel}

	if err := plan.RunInto(&baseline, FromFileFiltered(snapshot, &filter)); err != nil {
		t.Fatalf("generic baseline: %v", err)
	}
	baselineKey := verifyFileProjectionResult(t, &baseline)
	if baseline.Stats.PrimaryRangeBounded {
		t.Fatalf("generic baseline unexpectedly used a primary range: %+v", baseline.Stats)
	}
	if projectedRows, available := projectedRowsStat(baseline.Stats); available && projectedRows != 0 {
		t.Fatalf("generic oracle unexpectedly reported projected rows=%d", projectedRows)
	}
	if err := plan.RunInto(&projected, FromFileRange(snapshot, &span)); err != nil {
		t.Fatalf("projected primary range: %v", err)
	}
	if got := verifyFileProjectionResult(t, &projected); got != baselineKey {
		t.Fatalf("projected result differs from generic result")
	}
	if !projected.Stats.PrimaryRangeBounded || projected.Stats.RowsScanned != fileProjectionBenchPage {
		t.Fatalf("projected stats=%+v, want bounded %d-row primary range",
			projected.Stats, fileProjectionBenchPage)
	}
	projectedRows, available := projectedRowsStat(projected.Stats)
	if !available {
		t.Fatalf("projected stats=%+v, want ProjectedRows on candidate", projected.Stats)
	}
	if projectedRows != fileProjectionBenchPage {
		t.Fatalf("projected stats=%+v, want %d projected rows",
			projected.Stats, fileProjectionBenchPage)
	}

	// Reusing both Exec values must preserve the same snapshot result and the
	// same source distinction after retained buffers have been warmed once.
	for range 2 {
		if err := plan.RunInto(&baseline, FromFileFiltered(snapshot, &filter)); err != nil {
			t.Fatalf("reused generic baseline: %v", err)
		}
		if got := verifyFileProjectionResult(t, &baseline); got != baselineKey {
			t.Fatalf("reused generic result differs from first result")
		}
		if err := plan.RunInto(&projected, FromFileRange(snapshot, &span)); err != nil {
			t.Fatalf("reused projected primary range: %v", err)
		}
		if got := verifyFileProjectionResult(t, &projected); got != baselineKey {
			t.Fatalf("reused projected result differs from generic result")
		}
		if !projected.Stats.PrimaryRangeBounded || projected.Stats.RowsScanned != fileProjectionBenchPage {
			t.Fatalf("reused projected stats=%+v, want bounded %d-row primary range",
				projected.Stats, fileProjectionBenchPage)
		}
		if projectedRows, available := projectedRowsStat(projected.Stats); available && projectedRows != fileProjectionBenchPage {
			t.Fatalf("reused projected stats=%+v, want %d projected rows",
				projected.Stats, fileProjectionBenchPage)
		}
	}
}

func benchmarkFileProjectionArm(
	b *testing.B,
	plan *Query,
	source Source,
	fresh bool,
	want string,
) {
	var warm Exec
	var warmCancel CancelFlag
	var freshCancel CancelFlag
	if !fresh {
		warm.Options = ExecOptions{Workers: 1, Cancel: &warmCancel}
		if err := plan.RunInto(&warm, source); err != nil {
			b.Fatal(err)
		}
		if got := verifyFileProjectionResult(b, &warm); got != want {
			b.Fatal("warm-up result differs from verified snapshot result")
		}
		defer warm.Release()
	}

	b.ReportAllocs()
	var scanned, projected uint64
	var projectedStatsAvailable bool
	b.ResetTimer()
	for b.Loop() {
		execution := &warm
		if fresh {
			execution = &Exec{Options: ExecOptions{Workers: 1, Cancel: &freshCancel}}
		}
		if err := plan.RunInto(execution, source); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if got := verifyFileProjectionResult(b, execution); got != want {
			b.Fatal("benchmark result differs from verified snapshot result")
		}
		scanned = execution.Stats.RowsScanned
		if rows, available := projectedRowsStat(execution.Stats); available {
			projected = rows
			projectedStatsAvailable = true
		}
		if fresh {
			execution.Release()
		}
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(fileProjectionBenchPage), "rows")
	b.ReportMetric(float64(scanned), "scanned/op")
	if projectedStatsAvailable {
		b.ReportMetric(float64(projected), "projected/op")
	}
}

func BenchmarkFileProjectionFreshAndWarm(b *testing.B) {
	snapshot := fileProjectionBenchSnapshot(b)
	plan := fileProjectionBenchQuery()
	filter := NewFileFilterSource(fileProjectionKeepAll{})
	span := fileProjectionBenchRange()

	// Establish one verified generic result before timing either arm. The same
	// immutable snapshot, plan, and result key are then used by every case.
	var oracle Exec
	oracle.Options.Workers = 1
	if err := plan.RunInto(&oracle, FromFileFiltered(snapshot, &filter)); err != nil {
		b.Fatal(err)
	}
	want := verifyFileProjectionResult(b, &oracle)
	oracle.Release()

	cases := []struct {
		name   string
		source Source
		fresh  bool
	}{
		{name: "oracle_generic/fresh_exec", source: FromFileFiltered(snapshot, &filter), fresh: true},
		{name: "oracle_generic/warm_exec", source: FromFileFiltered(snapshot, &filter)},
		{name: "range_source/fresh_exec", source: FromFileRange(snapshot, &span), fresh: true},
		{name: "range_source/warm_exec", source: FromFileRange(snapshot, &span)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkFileProjectionArm(b, plan, tc.source, tc.fresh, want)
		})
	}
}
