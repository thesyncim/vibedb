package query

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// The parallel filter phase's contract: the worker count a scan chooses, and
// that splitting a scan changes nothing an observer can see — not the rows, not
// their order, not the ties an ORDER BY has to keep stable.

// TestScanWorkerCount pins the parallelism policy. The property that matters is
// that the default never splits a small scan: a query over a few hundred rows
// finishes in less time than starting a goroutine costs, so a split there is a
// straight regression on the interactive workload the default has to protect.
func TestScanWorkerCount(t *testing.T) {
	procs := runtime.GOMAXPROCS(0)
	cases := []struct {
		rows, requested, want int
	}{
		{rows: 0, requested: 0, want: 1},
		{rows: 1, requested: 0, want: 1},
		{rows: 256, requested: 0, want: 1},
		{rows: parallelScanMinRows - 1, requested: 0, want: 1},
		{rows: parallelScanMinRows, requested: 0,
			want: min(parallelScanMinRows/parallelScanRowsPerWorker, procs)},
		{rows: 4 * parallelScanRowsPerWorker, requested: 0, want: min(4, procs)},
		{rows: 1 << 22, requested: 0, want: procs},
		// An explicit request wins, including a request to stay serial and a
		// request larger than the scan has rows to give.
		{rows: 1 << 22, requested: 1, want: 1},
		{rows: 1 << 22, requested: 3, want: 3},
		{rows: 4, requested: 64, want: 4},
		{rows: 0, requested: 64, want: 1},
	}
	for _, test := range cases {
		if got := scanWorkerCount(test.rows, test.requested); got != test.want {
			t.Errorf("scanWorkerCount(%d, %d) = %d, want %d",
				test.rows, test.requested, got, test.want)
		}
	}
}

// TestSplitScanCoversEveryRowOnce checks the property the merge order rests on:
// the ranges are ascending, contiguous, and cover the scan exactly once. A gap
// would silently drop rows and an overlap would duplicate them, and neither
// would look like a failure anywhere else — the result would just be wrong.
func TestSplitScanCoversEveryRowOnce(t *testing.T) {
	var w Workspace
	defer w.Release()
	for _, rows := range []int{0, 1, 2, 7, 8, 63, 64, 65, 1000, 65536} {
		for _, workers := range []int{1, 2, 3, 8, 16} {
			scan := w.scanPoolFor(workers).split(rows, workers)
			if len(scan) != workers {
				t.Fatalf("rows=%d workers=%d: got %d ranges", rows, workers, len(scan))
			}
			at := 0
			for i := range scan {
				if scan[i].lo != at || scan[i].hi < scan[i].lo {
					t.Fatalf("rows=%d workers=%d: range %d is [%d,%d) after %d",
						rows, workers, i, scan[i].lo, scan[i].hi, at)
				}
				at = scan[i].hi
			}
			if at != rows {
				t.Fatalf("rows=%d workers=%d: ranges cover %d rows", rows, workers, at)
			}
			// The split must be balanced to within one row, or one worker
			// becomes the critical path and the others idle.
			widest, narrowest := 0, rows+1
			for i := range scan {
				width := scan[i].hi - scan[i].lo
				widest, narrowest = max(widest, width), min(narrowest, width)
			}
			if widest-narrowest > 1 {
				t.Fatalf("rows=%d workers=%d: widest %d, narrowest %d",
					rows, workers, widest, narrowest)
			}
		}
	}
}

// parallelCorpus is large enough that the default policy splits it, so the
// battery below exercises the automatic path and not only an explicit request.
func parallelCorpus(rows int) [][]byte {
	docs := make([][]byte, rows)
	for i := range docs {
		docs[i] = fmt.Appendf(nil,
			`{"id":%d,"sel":%d,"tag":"t\u%04x","score":%d,"nested":{"bucket":%d},"obj":{"x":%d}}`,
			i, i%1000, 'A'+i%13, i*7%997, i%23, i%3)
	}
	return docs
}

// parallelQueries covers every shape a worker has a distinct path for: a single
// top-level field read through the shape cache, a nested path read through a
// compiled pointer, a predicate the columnar span selector declines (boolean
// trees and containment, which fall back to per-row eval with per-worker
// containment scratch), escaped strings that decode into a per-worker text
// arena, and orderings whose ties can only stay put if the merge is in scan
// order.
func parallelQueries() []*Query {
	return []*Query{
		Select(Path("id")).Where(Cmp("sel", Lt, 10)),
		Select(Path("id"), Path("tag")).Where(Cmp("sel", Lt, 500)),
		Select(Path("id")).Where(Cmp("nested.bucket", Eq, 7)),
		Select(Path("id")).Where(And(Cmp("sel", Ge, 100), Cmp("sel", Lt, 200))),
		Select(Path("id")).Where(Or(Cmp("sel", Eq, 1), Cmp("sel", Eq, 999))),
		Select(Path("id")).Where(In("sel", 3, 17, 42, 900)),
		Select(Path("id")).Where(Contains("obj", `{"x":1}`)),
		Select(Path("id")).Where(Exists("score")),
		Select(Path("id")).Where(IsNull("absent")),
		Select(Path("tag"), Path("id")).Where(Cmp("tag", Ge, "tB")),
		// Ordering by a key with thousands of ties per value: a merge that is
		// not in scan order shows up here and nowhere else.
		Select(Path("sel"), Path("id")).Where(Cmp("sel", Lt, 40)).OrderBy("sel", Asc).Limit(500),
		Select(Path("nested.bucket"), Count(), Sum("score")).GroupBy("nested.bucket").Where(Cmp("sel", Lt, 300)),
		Select(Count(), Sum("score"), Min("score"), Max("score")).Where(Cmp("sel", Lt, 250)),
		Select(Path("id")).Where(Cmp("sel", Lt, 900)).Limit(37),
	}
}

// TestParallelFilterMatchesSerial is the correctness oracle for the split: the
// serial executor, which the reference differential already validates, run
// against every worker count over the same collection. Any divergence in rows
// or in their order fails here.
func TestParallelFilterMatchesSerial(t *testing.T) {
	const rows = 24000
	docs := parallelCorpus(rows)
	set := buildSegment(t, docs, storageMode{"hashed+shaped", true, true})

	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 64, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range docs {
		if err := builder.Append(fmt.Sprintf("k%08d", i), doc); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	sources := []struct {
		name string
		src  Source
	}{
		{"segment", FromSegment(set)},
		{"snapshot", FromSnapshot(snapshot)},
	}
	for _, source := range sources {
		t.Run(source.name, func(t *testing.T) {
			for qi, q := range parallelQueries() {
				serial := Exec{Options: ExecOptions{Workers: 1}}
				if err := q.RunInto(&serial, source.src); err != nil {
					t.Fatalf("query %d serial: %v", qi, err)
				}
				want := resultKey(serial.Result)
				for _, workers := range []int{0, 2, 3, 5, 8, 17} {
					e := Exec{Options: ExecOptions{Workers: workers}}
					if err := q.RunInto(&e, source.src); err != nil {
						t.Fatalf("query %d workers=%d: %v", qi, workers, err)
					}
					if got := resultKey(e.Result); got != want {
						t.Fatalf("query %d workers=%d:\n got: %s\nwant: %s",
							qi, workers, got, want)
					}
				}
			}
		})
	}
}

// TestParallelFilterReusesOneWorkspace checks the split against a reused Exec,
// the shape a hot loop actually runs: every worker buffer is retained in the
// Workspace across executions, so a worker that read a stale bound or a stale
// column from the previous execution would answer a plausible wrong result
// rather than fail. Alternating worker counts is what makes the retained state
// disagree with the current split.
// TestWorkspaceReleaseRetiresScanWorkers is the other half of the parked-worker
// design: the workers are parked for the Workspace's life, so releasing one has
// to retire them or every released Workspace leaves goroutines behind for the
// rest of the program.
//
// The proof is the closed wake channel rather than a goroutine count, because a
// count is not a fact about this code in a shared test binary: other tests park
// their own pools, and the cleanup that collects an abandoned one fires
// whenever the collector happens to run. The repeated-cycle check below is the
// count assertion that does survive that — unbounded growth shows through the
// noise, a goroutine or two does not.
func TestWorkspaceReleaseRetiresScanWorkers(t *testing.T) {
	docs := parallelCorpus(4000)
	set := buildSegment(t, docs, storageMode{"hashed+shaped", true, true})
	q := Select(Path("id")).Where(Cmp("sel", Lt, 50))

	e := Exec{Options: ExecOptions{Workers: 8}}
	if err := q.RunInto(&e, FromSegment(set)); err != nil {
		t.Fatal(err)
	}
	if e.Workspace.pool == nil || len(e.Workspace.pool.wake) != 8 {
		t.Fatalf("a split scan parked %v workers, want 8", e.Workspace.pool)
	}
	parked := append([]chan struct{}(nil), e.Workspace.pool.wake...)
	e.Release()
	if e.Workspace.pool != nil {
		t.Fatal("Release left the pool attached to the Workspace")
	}
	for i, wake := range parked {
		select {
		case _, open := <-wake:
			if open {
				t.Fatalf("worker %d was woken instead of retired", i)
			}
		default:
			t.Fatalf("Release left worker %d's wake channel open", i)
		}
	}

	// A released Exec must still execute, growing a fresh pool, and doing so
	// repeatedly must not accumulate goroutines.
	runtime.GC()
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		if err := q.RunInto(&e, FromSegment(set)); err != nil {
			t.Fatal(err)
		}
		e.Release()
	}
	runtime.GC()
	for i := 0; i < 100 && runtime.NumGoroutine() > before+8; i++ {
		runtime.Gosched()
	}
	if after := runtime.NumGoroutine(); after > before+8 {
		t.Fatalf("50 release cycles left %d goroutines, started from %d", after, before)
	}
}

func TestParallelFilterReusesOneWorkspace(t *testing.T) {
	docs := parallelCorpus(24000)
	set := buildSegment(t, docs, storageMode{"hashed+shaped", true, true})
	q := Select(Path("id"), Path("tag")).Where(Cmp("sel", Lt, 50))

	var e Exec
	e.Options.Workers = 1
	if err := q.RunInto(&e, FromSegment(set)); err != nil {
		t.Fatal(err)
	}
	want := resultKey(e.Result)
	for _, workers := range []int{4, 1, 7, 2, 0, 3, 1, 16} {
		e.Options.Workers = workers
		if err := q.RunInto(&e, FromSegment(set)); err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		if got := resultKey(e.Result); got != want {
			t.Fatalf("workers=%d on a reused Exec:\n got: %s\nwant: %s", workers, got, want)
		}
	}
}
