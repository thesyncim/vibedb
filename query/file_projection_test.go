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

func TestFileProjectionNativeLaneReportsProjectedRows(t *testing.T) {
	snapshot := fileProjectionBenchSnapshot(t)
	span := fileProjectionBenchRange()
	query := fileProjectionBenchQuery()
	var execution Exec
	execution.Options = ExecOptions{Workers: 1}
	defer execution.Release()
	if err := query.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.ProjectedRows != fileProjectionBenchPage ||
		execution.Stats.RowsScanned != fileProjectionBenchPage {
		t.Fatalf("native stats=%+v, want %d projected rows", execution.Stats, fileProjectionBenchPage)
	}
}

func TestFileProjectionRecompiledPlanUsesCurrentPaths(t *testing.T) {
	snapshot := fileProjectionBenchSnapshot(t)
	span := fileProjectionBenchRange()
	var c compiler
	defer c.release()
	var execution Exec
	execution.Options = ExecOptions{Workers: 1}
	defer execution.Release()
	var first *plan
	for _, paths := range [][]string{
		{"/score", "/bucket"}, {"/bucket", "/score"},
		{"/score", "/score"}, {"/score", "/bucket"},
	} {
		q := Select(Path(paths[0]), Path(paths[1])).
			Where(And(Cmp("/id", Ge, "00000"), Cmp("/id", Lt, "00064"))).
			OrderBy("/id", Asc).Limit(64)
		p, err := c.compilePlan(q)
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = p
		} else if p != first {
			t.Fatal("fixture did not reuse the plan object")
		}
		q.built = &compileResult{plan: p}
		if err := q.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
			t.Fatal(err)
		}
		if execution.Stats.ProjectedRows != 64 {
			t.Fatalf("paths=%v stats=%+v", paths, execution.Stats)
		}
		for column, path := range paths {
			for row := 0; row < 64; row++ {
				want := row
				if path == "/bucket" {
					want %= 16
				}
				if got := string(execution.Result.Columns[column].Cells[row].JSON()); got != fmt.Sprint(want) {
					t.Fatalf("paths=%v column=%d row=%d got=%s want=%d", paths, column, row, got, want)
				}
			}
		}
	}
}

func TestFileProjectionDeclinedReplacementPreservesFilterCharge(t *testing.T) {
	snapshot := fileProjectionBenchSnapshot(t)
	span := fileProjectionBenchRange()
	q := fileProjectionBenchQuery()
	var execution Exec
	execution.Options = ExecOptions{Workers: 1}
	defer execution.Release()
	if err := q.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatal(err)
	}
	s := execution.file.small
	oldFilter, oldPlan, oldBytes := s.projection, s.projectionPlan, s.projectionFilter
	oldPaths := append([]string(nil), s.projectionPaths...)
	replacement := Select(Path("/id")).OrderBy("/id", Asc).Limit(64)
	p, err := replacement.compiled()
	if err != nil {
		t.Fatal(err)
	}
	if fileProjectionFilterBytesForPlan(p) >= oldBytes {
		t.Fatal("replacement must have a smaller filter charge")
	}
	s.p, s.ordered = p, true
	s.work.heapWorkBudget.begin(1)
	defer s.work.heapWorkBudget.disable()
	var stats ExecStats
	// No snapshot access or allocation is permitted before admission fails.
	if allocs := testing.AllocsPerRun(100, func() {
		handled, err := s.tryFileProjected(nil, &span, normalizedFileOptions{batchBytes: 4096}, &stats)
		if handled || err != nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
	}); allocs != 0 {
		t.Fatalf("declined admission allocated %v times", allocs)
	}
	if s.projection != oldFilter || s.projectionPlan != oldPlan || s.projectionFilter != oldBytes || !reflect.DeepEqual(s.projectionPaths, oldPaths) {
		t.Fatal("declined replacement changed the retained filter configuration or charge")
	}
	s.p = nil
	s.work.heapWorkBudget.disable()
	if err := q.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.ProjectedRows != 64 || s.projectionFilter != oldBytes {
		t.Fatal("original plan was not reusable after declined replacement")
	}
}

func TestFileProjectionFilterEstimateCoversRetainedCapacity(t *testing.T) {
	// Count each owned allocation once. Slice backing arrays include inline
	// elements; recurse only to count allocations referenced by those elements.
	var ownedBytes func(reflect.Value) int64
	ownedBytes = func(v reflect.Value) int64 {
		var n int64
		switch v.Kind() {
		case reflect.Pointer:
			if !v.IsNil() {
				n = int64(v.Elem().Type().Size()) + ownedBytes(v.Elem())
			}
		case reflect.Slice:
			n = int64(v.Cap()) * int64(v.Type().Elem().Size())
			for i := 0; i < v.Len(); i++ {
				n += ownedBytes(v.Index(i))
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				n += ownedBytes(v.Field(i))
			}
		}
		return n
	}
	for _, count := range []int{1, 3, 17, 65, 257} {
		for _, path := range []string{"/a", "/a~1b/~0c", "/" + strings.Repeat("long", 200), strings.Repeat("/a", 260)} {
			columns, paths := make([]Column, count), make([]string, count)
			for i := range columns {
				columns[i], paths[i] = Path(path), path
			}
			p, err := Select(columns...).compiled()
			if err != nil {
				t.Fatal(err)
			}
			filter, err := durable.NewProjectionFilter(paths)
			if err != nil {
				t.Fatal(err)
			}
			if estimate, actual := fileProjectionFilterBytesForPlan(p), ownedBytes(reflect.ValueOf(filter)); estimate < actual {
				t.Fatalf("columns=%d path bytes=%d estimate=%d retained=%d", count, len(path), estimate, actual)
			}
		}
	}
}

func TestFileProjectionPreservesWideIntegerSpelling(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-wide-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	const wide = "9007199254740993"
	var docs store.Segment
	for row := range 4 {
		key := fmt.Sprintf("%05d", row)
		doc := fmt.Appendf(nil, `{"id":%q,"wide":%s}`, key, wide)
		if err := builder.Append(key, doc); err != nil {
			t.Fatal(err)
		}
		if _, err := docs.Append(doc); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.CreateFromPrimary(built, file, durable.Options{}); err != nil {
		t.Fatal(err)
	}
	fs, err := durable.Open(file, durable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	query := Select(Path("/id"), Path("/wide")).
		Where(And(Cmp("/id", Ge, "00000"), Cmp("/id", Lt, "00004"))).
		OrderBy("/id", Asc).Limit(4)
	span := NewFileRangeSource([]byte("00000"), []byte("00004"), false)
	span.BindPrimaryOrder("/id")
	span.BindPrimaryPredicate("/id")
	var execution Exec
	execution.Options = ExecOptions{Workers: 1}
	defer execution.Release()
	if err := query.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.ProjectedRows != 4 {
		t.Fatalf("stats=%+v, want native projected rows", execution.Stats)
	}
	for row := range 4 {
		if got := string(execution.Result.Columns[1].Cells[row].JSON()); got != wide {
			t.Fatalf("row %d wide=%s, want %s", row, got, wide)
		}
	}
	want, err := query.Run(FromSegment(&docs))
	if err != nil {
		t.Fatal(err)
	}
	if got, wantKey := fileProjectionResultKey(execution.Result), fileProjectionResultKey(want); got != wantKey {
		t.Fatalf("native result differs:\n%s\nwant:\n%s", got, wantKey)
	}
}

func TestFileProjectionFallsBackAfterLateUnsupportedShape(t *testing.T) {
	const rows = 256
	file, err := os.CreateTemp(t.TempDir(), "query-file-projection-fallback-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var docs store.Segment
	for row := range rows {
		key := fmt.Sprintf("%05d", row)
		var doc []byte
		if row == rows-1 {
			doc = fmt.Appendf(nil, `{"id":%q,"wide":{"value":%d}}`, key, row)
		} else {
			doc = fmt.Appendf(nil, `{"id":%q,"wide":%d}`, key, row)
		}
		if err := builder.Append(key, doc); err != nil {
			t.Fatal(err)
		}
		if _, err := docs.Append(doc); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.CreateFromPrimary(built, file, durable.Options{}); err != nil {
		t.Fatal(err)
	}
	fs, err := durable.Open(file, durable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	query := Select(Path("/id"), Path("/wide")).
		Where(And(Cmp("/id", Ge, "00000"), Cmp("/id", Lt, "00256"))).
		OrderBy("/id", Asc).Limit(rows)
	span := NewFileRangeSource([]byte("00000"), []byte("00256"), false)
	span.BindPrimaryOrder("/id")
	span.BindPrimaryPredicate("/id")
	var execution Exec
	execution.Options = ExecOptions{Workers: 1}
	defer execution.Release()
	if err := query.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.ProjectedRows != 0 || execution.Result.RowCount != rows {
		t.Fatalf("fallback stats=%+v rows=%d, want complete generic result", execution.Stats, execution.Result.RowCount)
	}
	want, err := query.Run(FromSegment(&docs))
	if err != nil {
		t.Fatal(err)
	}
	if got, wantKey := fileProjectionResultKey(execution.Result), fileProjectionResultKey(want); got != wantKey {
		t.Fatalf("fallback result differs:\n%s\nwant:\n%s", got, wantKey)
	}
}
