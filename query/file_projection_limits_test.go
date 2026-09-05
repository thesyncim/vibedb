package query

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestFileProjectionCancellationResetAndEmptyRange(t *testing.T) {
	snapshot := fileProjectionBenchSnapshot(t)
	plan := fileProjectionBenchQuery()
	span := fileProjectionBenchRange()
	empty := NewFileRangeSource([]byte("00064"), []byte("00064"), false)
	empty.BindPrimaryOrder("/id")
	empty.BindPrimaryPredicate("/id")

	var cancel CancelFlag
	var execution Exec
	execution.Options = ExecOptions{Workers: 1, Cancel: &cancel}
	defer execution.Release()

	if err := plan.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatal(err)
	}
	want := verifyFileProjectionResult(t, &execution)
	if execution.Stats.ProjectedRows != fileProjectionBenchPage {
		t.Fatalf("initial stats=%+v, want native projection", execution.Stats)
	}

	cancel.Cancel()
	if err := plan.RunInto(&execution, FromFileRange(snapshot, &span)); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled range error=%v, want ErrCanceled", err)
	}
	if execution.Result.RowCount != 0 || len(execution.Result.fileData) != 0 {
		t.Fatalf("canceled range exposed result rows=%d bytes=%d", execution.Result.RowCount, len(execution.Result.fileData))
	}

	cancel.Reset()
	if err := plan.RunInto(&execution, FromFileRange(snapshot, &empty)); err != nil {
		t.Fatalf("empty range after reset: %v", err)
	}
	if execution.Result.RowCount != 0 || execution.Stats.ProjectedRows != 0 {
		t.Fatalf("empty range result rows=%d stats=%+v", execution.Result.RowCount, execution.Stats)
	}
	if execution.file.small == nil || execution.file.small.projection == nil {
		t.Fatal("empty range did not enter the native projection lane")
	}

	cancel.Cancel()
	if err := plan.RunInto(&execution, FromFileRange(snapshot, &empty)); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled empty range error=%v, want ErrCanceled", err)
	}
	if execution.Result.RowCount != 0 {
		t.Fatalf("canceled empty range exposed %d rows", execution.Result.RowCount)
	}

	cancel.Reset()
	if err := plan.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatalf("range after empty cancellation: %v", err)
	}
	if got := verifyFileProjectionResult(t, &execution); got != want {
		t.Fatal("range after cancellation differs from the first result")
	}
	if execution.Stats.ProjectedRows != fileProjectionBenchPage {
		t.Fatalf("reused stats=%+v, want native projection", execution.Stats)
	}
}

func TestFileProjectionResultBudgetRollbackAndReuse(t *testing.T) {
	snapshot := fileProjectionBenchSnapshot(t)
	plan := fileProjectionBenchQuery()
	span := fileProjectionBenchRange()
	for _, tc := range []struct {
		name string
		opts ExecOptions
	}{
		{
			name: "rows",
			opts: ExecOptions{Workers: 1, ResultRows: 8, ResultBytes: -1},
		},
		{
			name: "bytes",
			opts: ExecOptions{
				Workers:     1,
				ResultRows:  -1,
				ResultBytes: resultColumnBytes*3 + resultCellBytes*3,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var execution Exec
			execution.Options = tc.opts
			defer execution.Release()

			err := plan.RunInto(&execution, FromFileRange(snapshot, &span))
			var budgetErr *ResultBudgetError
			if !errors.As(err, &budgetErr) || !errors.Is(err, ErrResultBudget) {
				t.Fatalf("budget error=%v, want ResultBudgetError", err)
			}
			if tc.name == "rows" {
				if budgetErr.Rows <= tc.opts.ResultRows || budgetErr.RowLimit != tc.opts.ResultRows {
					t.Fatalf("row budget=%+v, want row limit %d", budgetErr, tc.opts.ResultRows)
				}
			} else if budgetErr.Bytes <= tc.opts.ResultBytes || budgetErr.ByteLimit != tc.opts.ResultBytes {
				t.Fatalf("byte budget=%+v, want byte limit %d", budgetErr, tc.opts.ResultBytes)
			}
			if execution.Result.RowCount != 0 || len(execution.Result.fileData) != 0 {
				t.Fatalf("failed projection exposed rows=%d bytes=%d", execution.Result.RowCount, len(execution.Result.fileData))
			}
			if execution.Stats.ProjectedRows != 0 {
				t.Fatalf("failed projection stats=%+v, want no published projected rows", execution.Stats)
			}
			if execution.file.small == nil || execution.file.small.projection == nil {
				t.Fatal("result-budget failure did not enter the native projection lane")
			}

			execution.Options = ExecOptions{Workers: 1}
			if err := plan.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
				t.Fatalf("reuse after %s budget: %v", tc.name, err)
			}
			verifyFileProjectionResult(t, &execution)
			if execution.Stats.ProjectedRows != fileProjectionBenchPage {
				t.Fatalf("reused stats=%+v, want native projection", execution.Stats)
			}
		})
	}
}

func fileProjectionTightMemoryQuery() *Query {
	columns := make([]Column, 512)
	for i := range columns {
		columns[i] = Path("/id")
	}
	return Select(columns...).
		Where(And(
			Cmp("/id", Ge, "00000"),
			Cmp("/id", Lt, "00064"),
		)).
		OrderBy("/id", Asc).
		Limit(fileProjectionBenchPage)
}

func TestFileProjectionDeduplicatedPathsFitTightMemory(t *testing.T) {
	snapshot := fileProjectionBenchSnapshot(t)
	plan := fileProjectionTightMemoryQuery()
	span := fileProjectionBenchRange()

	want, err := plan.Run(FromFile(snapshot))
	if err != nil {
		t.Fatalf("generic oracle: %v", err)
	}
	defer want.Release()
	wantKey := fileProjectionResultKey(want)

	var execution Exec
	execution.Options = ExecOptions{
		Workers:     1,
		MemoryBytes: minimumFileMemory,
		ResultRows:  -1,
		ResultBytes: -1,
	}
	defer execution.Release()
	if err := plan.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatalf("tight-memory fallback: %v", err)
	}
	if got := fileProjectionResultKey(execution.Result); got != wantKey {
		t.Fatal("tight-memory fallback differs from generic oracle")
	}
	if execution.Stats.ProjectedRows != fileProjectionBenchPage {
		t.Fatalf("tight-memory stats=%+v, want native projection", execution.Stats)
	}
	if execution.file.small == nil || execution.file.small.projection == nil {
		t.Fatal("tight-memory execution did not retain a native projection filter")
	}

	execution.Options = ExecOptions{
		Workers:     1,
		MemoryBytes: minimumFileMemory,
		ResultRows:  8,
		ResultBytes: -1,
	}
	err = plan.RunInto(&execution, FromFileRange(snapshot, &span))
	if !errors.Is(err, ErrResultBudget) {
		t.Fatalf("tight-memory budget error=%v, want ErrResultBudget", err)
	}
	if execution.Result.RowCount != 0 || len(execution.Result.fileData) != 0 {
		t.Fatalf("tight-memory budget exposed rows=%d bytes=%d", execution.Result.RowCount, len(execution.Result.fileData))
	}
	if execution.Stats.ProjectedRows != 0 {
		t.Fatalf("tight-memory budget stats=%+v, want generic fallback", execution.Stats)
	}

	execution.Options = ExecOptions{
		Workers:     1,
		MemoryBytes: minimumFileMemory,
		ResultRows:  -1,
		ResultBytes: -1,
	}
	if err := plan.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatalf("tight-memory reuse after budget: %v", err)
	}
	if got := fileProjectionResultKey(execution.Result); got != wantKey {
		t.Fatal("tight-memory reuse differs from generic oracle")
	}
	if execution.Stats.ProjectedRows != fileProjectionBenchPage {
		t.Fatalf("tight-memory reuse stats=%+v, want native projection", execution.Stats)
	}
}

func TestFileProjectionEligibilityGuards(t *testing.T) {
	snapshot := fileProjectionBenchSnapshot(t)
	plan := fileProjectionBenchQuery()
	want, err := plan.Run(FromFile(snapshot))
	if err != nil {
		t.Fatalf("generic oracle: %v", err)
	}
	defer want.Release()
	wantKey := fileProjectionResultKey(want)

	filter := NewFileFilterSource(fileProjectionKeepAll{})
	uncertified := NewFileRangeSource([]byte("00000"), []byte("00064"), false)
	uncertified.BindPrimaryOrder("/id")
	wrongCertificate := NewFileRangeSource([]byte("00000"), []byte("00064"), false)
	wrongCertificate.BindPrimaryOrder("/bucket")
	wrongCertificate.BindPrimaryPredicate("/bucket")
	cases := []struct {
		name   string
		source Source
	}{
		{name: "filtered_source", source: FromFileFiltered(snapshot, &filter)},
		{name: "uncertified_range", source: FromFileRange(snapshot, &uncertified)},
		{name: "wrong_certificate", source: FromFileRange(snapshot, &wrongCertificate)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var execution Exec
			execution.Options = ExecOptions{Workers: 1}
			defer execution.Release()
			if err := plan.RunInto(&execution, tc.source); err != nil {
				t.Fatal(err)
			}
			if got := fileProjectionResultKey(execution.Result); got != wantKey {
				t.Fatal("eligibility guard changed the generic result")
			}
			wantProjected := uint64(0)
			if tc.name == "uncertified_range" {
				wantProjected = uint64(fileProjectionBenchPage)
			}
			if execution.Stats.ProjectedRows != wantProjected {
				t.Fatalf("eligibility guard stats=%+v, want ProjectedRows=%d", execution.Stats, wantProjected)
			}
			if tc.name == "filtered_source" && execution.Stats.PrimaryRangeBounded {
				t.Fatalf("filtered source unexpectedly reports primary range: %+v", execution.Stats)
			}
		})
	}
}

func fileProjectionIndexedSnapshot(tb testing.TB) (*durable.Snapshot, *store.Segment) {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-projection-indexed-")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })

	source := &store.Collection{}
	var docs store.Segment
	for row := range 128 {
		id := fmt.Sprintf("%05d", row)
		document := fmt.Appendf(nil,
			`{"id":%q,"bucket":%d,"score":%d}`,
			id, row%16, row,
		)
		if _, err := source.Put(id, document); err != nil {
			tb.Fatalf("source row %d: %v", row, err)
		}
		if _, err := docs.Append(document); err != nil {
			tb.Fatalf("oracle row %d: %v", row, err)
		}
	}
	options := durable.Options{
		Collection: store.Options{ChunkDocuments: 64},
		Indexes:    []store.IndexDefinition{{Name: "bucket", Paths: []string{"/bucket"}}},
	}
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
	return snapshot, &docs
}

func TestFileProjectionCandidateMaskDoesNotEnterNativeLane(t *testing.T) {
	snapshot, docs := fileProjectionIndexedSnapshot(t)
	plan := Select(Path("/id"), Path("/bucket"), Path("/score")).
		Where(Cmp("/bucket", Eq, 3)).
		OrderBy("/id", Asc).
		Limit(8)
	want, err := plan.Run(FromSegment(docs))
	if err != nil {
		t.Fatalf("generic oracle: %v", err)
	}
	defer want.Release()

	var execution Exec
	execution.Options = ExecOptions{Workers: 1}
	defer execution.Release()
	if err := plan.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if got, wantKey := fileProjectionResultKey(execution.Result), fileProjectionResultKey(want); got != wantKey {
		t.Fatal("candidate-mask result differs from generic oracle")
	}
	if !execution.Stats.IndexBounded || execution.Stats.CandidateRows == 0 || execution.Stats.CandidateRows > 8 {
		t.Fatalf("candidate-mask stats=%+v, want a bounded candidate set", execution.Stats)
	}
	if execution.Stats.ProjectedRows != 0 {
		t.Fatalf("candidate-mask stats=%+v, want no native projection", execution.Stats)
	}
}
