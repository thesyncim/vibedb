package query

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func denseContainmentArray(values int) string {
	if values <= 0 {
		return "[]"
	}
	return "[" + strings.Repeat("0,", values-1) + "0]"
}

func requireWorkBudgetResource(t *testing.T, err error, resource string) {
	t.Helper()
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
		t.Fatalf("error = %v, want WorkBudgetError", err)
	}
	if budgetErr.Resource != resource {
		t.Fatalf("budget resource = %q, want %q", budgetErr.Resource, resource)
	}
	if budgetErr.Bytes <= budgetErr.Limit {
		t.Fatalf("non-exhausted work budget error: %+v", budgetErr)
	}
}

// A punctuation-dense container has far more index entries than its byte
// length seed predicts. Admission must happen before the first tape allocation:
// otherwise a single legal JSON value can bypass MemoryBytes during predicate
// evaluation even though the row and result themselves are tiny.
func TestContainsTapeBudgetRejectsBeforeGrowth(t *testing.T) {
	raw := []byte(denseContainmentArray(20_000))
	var budget heapWorkBudget
	budget.begin(minimumHeapMemoryBytes)
	var scratch evalScratch
	scratch.setWork(&budget)

	_, err := scratch.containsTape(raw)
	requireWorkBudgetResource(t, err, "JSON containment index workspace")
	if cap(scratch.entries) != 0 {
		t.Fatalf("rejected containment allocated %d tape entries", cap(scratch.entries))
	}
	if got := budget.used.Load(); got != 0 {
		t.Fatalf("failed admission consumed %d budget bytes", got)
	}
}

// Re-arming the per-execution account must not undo the containment hot-path
// contract. The tape capacity survives, its current width is admitted again,
// and the build itself remains allocation-free.
func TestContainsTapeWarmBudgetedBuildIsAllocationFree(t *testing.T) {
	raw := []byte(`{"a":[1,2,3],"b":{"c":4},"d":"text"}`)
	var budget heapWorkBudget
	var scratch evalScratch
	run := func() {
		budget.begin(1 << 20)
		scratch.setWork(&budget)
		if _, err := scratch.containsTape(raw); err != nil {
			panic(err)
		}
	}
	run()
	if allocs := testing.AllocsPerRun(200, run); allocs != 0 {
		t.Fatalf("warm budgeted containment allocated %.2f times, want 0", allocs)
	}
}

// Segment and heap-snapshot execution share the same evaluator admission
// contract. Both must surface the parked evaluator error rather than silently
// treating a budget-rejected containment predicate as false.
func TestHeapContainmentBudgetPropagatesAcrossSources(t *testing.T) {
	haystack := denseContainmentArray(20_000)
	document := []byte(`{"id":1,"hay":` + haystack + `}`)

	var segment store.Segment
	if _, err := segment.Append(document); err != nil {
		t.Fatal(err)
	}
	collection, err := store.New(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("row", document); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	query := Select(Count()).Where(Contains("hay", "0"))
	for _, tc := range []struct {
		name   string
		source Source
	}{
		{name: "segment", source: FromSegment(&segment)},
		{name: "snapshot", source: FromSnapshot(snapshot)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := Exec{Options: ExecOptions{
				Workers:     1,
				MemoryBytes: minimumHeapMemoryBytes,
				ResultRows:  -1,
				ResultBytes: -1,
			}}
			err := query.RunInto(&exec, tc.source)
			requireWorkBudgetResource(t, err, "JSON containment index workspace")
			if cap(exec.Workspace.eval.entries) != 0 {
				t.Fatalf(
					"rejected containment allocated %d root tape entries",
					cap(exec.Workspace.eval.entries),
				)
			}
			if exec.Result.RowCount != 0 {
				t.Fatalf("rejected containment exposed %d rows", exec.Result.RowCount)
			}
		})
	}
}

// Durable raw batches and scratch Segments are already bounded by BatchRows,
// BatchBytes, and the collection's durable MaxDocumentBytes schema contract.
// MemoryBytes remains their batch/merge target, so containment must apply its
// own admission account without adding a second validation pass to every
// ordinary row.
func TestDurablePunctuationDenseContainmentIsBounded(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "containment-budget-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	document := []byte(`{"id":1,"hay":` + denseContainmentArray(5_000) + `}`)
	if _, err := collection.Put([]byte("row"), document); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	query := Select(Count()).Where(Contains("hay", "0"))
	exec := Exec{Options: ExecOptions{
		Workers:     1,
		BatchRows:   1,
		BatchBytes:  int64(len(document) + 1),
		MemoryBytes: minimumHeapMemoryBytes,
		ResultRows:  -1,
		ResultBytes: -1,
	}}
	err = query.RunInto(&exec, FromFile(snapshot))
	requireWorkBudgetResource(t, err, "JSON containment index workspace")
	if len(exec.file.segments) != 1 || exec.file.segments[0] == nil {
		t.Fatal("durable worker did not reach its scratch Segment")
	}
	if got := exec.file.segments[0].Len(); got != 1 {
		t.Fatalf("durable scratch Segment contains %d documents, want 1", got)
	}
	tape := cap(exec.file.workers[0].eval.entries)
	if tape == 0 || tape >= 5_000 {
		t.Fatalf(
			"durable containment retained %d entries; want an admitted prefix below 5000",
			tape,
		)
	}
}

// A containment predicate may run on either side of a join. The inner binding
// path and the outer driving path both park evaluator errors behind a boolean
// predicate API, so each needs an explicit post-scan error sweep.
func TestJoinedContainmentBudgetPropagatesFromInnerAndOuter(t *testing.T) {
	haystack := denseContainmentArray(20_000)
	for _, side := range []string{"inner", "outer"} {
		t.Run(side, func(t *testing.T) {
			var database store.Database
			outer, err := database.CreateCollection("outer", store.Options{})
			if err != nil {
				t.Fatal(err)
			}
			inner, err := database.CreateCollection("inner", store.Options{})
			if err != nil {
				t.Fatal(err)
			}
			outerDocument := []byte(`{"id":"o","join":"match"}`)
			innerDocument := []byte(`{"join":"match"}`)
			if side == "outer" {
				outerDocument = []byte(`{"id":"o","join":"match","hay":` + haystack + `}`)
			} else {
				innerDocument = []byte(`{"join":"match","hay":` + haystack + `}`)
			}
			if _, err := outer.Put("outer-key", outerDocument); err != nil {
				t.Fatal(err)
			}
			if _, err := inner.Put("inner-key", innerDocument); err != nil {
				t.Fatal(err)
			}

			join := JoinOn("inner", "join", "join")
			query := Select(Path("id")).Join(join)
			if side == "inner" {
				query = Select(Path("id")).Join(
					JoinOn("inner", "join", "join").
						Where(Contains("hay", "0")),
				)
			} else {
				query = query.Where(Contains("hay", "0"))
			}
			exec := Exec{Options: ExecOptions{
				Workers:     1,
				MemoryBytes: minimumHeapMemoryBytes,
				ResultRows:  -1,
				ResultBytes: -1,
			}}
			err = query.RunInto(
				&exec,
				FromDatabase(database.Snapshot(), "outer"),
			)
			requireWorkBudgetResource(t, err, "JSON containment index workspace")
			if exec.Result.RowCount != 0 {
				t.Fatalf("rejected joined containment exposed %d rows", exec.Result.RowCount)
			}
		})
	}
}

// Mutation filters use the same compiled predicate but run outside RunInto.
// They must arm the same budget explicitly and surface a parked evaluator
// failure; treating the rejected predicate as false would silently leave a row
// that DELETE or UPDATE was asked to visit.
func TestMutationFilterPropagatesContainmentBudget(t *testing.T) {
	statement, err := PrepareDML(`DELETE FROM docs WHERE hay @> 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	visits := 0
	exec := Exec{Options: ExecOptions{
		MemoryBytes: minimumHeapMemoryBytes,
	}}
	filter, err := statement.Filter(&exec, nil, func(_, _ []byte) error {
		visits++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"hay":` + denseContainmentArray(20_000) + `}`)
	if err := filter.Add([]byte("row"), document); err != nil {
		requireWorkBudgetResource(t, err, "JSON containment index workspace")
	} else {
		err = filter.Done()
		requireWorkBudgetResource(t, err, "JSON containment index workspace")
	}
	if visits != 0 {
		t.Fatalf("budget-rejected mutation visited %d rows", visits)
	}
}

func TestMutationContainmentFilterWarmAllocations(t *testing.T) {
	statement, err := PrepareDML(`DELETE FROM docs WHERE hay @> 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	var exec Exec
	key := []byte("row")
	document := []byte(`{"hay":[0,1,2],"escaped":"a\\nb"}`)
	visits := 0
	visit := func(_, _ []byte) error {
		visits++
		return nil
	}
	run := func() {
		filter, filterErr := statement.Filter(&exec, nil, visit)
		if filterErr != nil {
			panic(filterErr)
		}
		if filterErr = filter.Add(key, document); filterErr != nil {
			panic(filterErr)
		}
		if filterErr = filter.Done(); filterErr != nil {
			panic(filterErr)
		}
	}
	run()
	visits = 0
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warm mutation containment allocated %.2f times, want 0", allocs)
	}
	if visits < 100 {
		t.Fatalf("warm mutation containment visited %d rows, want at least 100", visits)
	}
}
