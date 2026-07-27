package query

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// durableScanCorpus builds a page-backed snapshot of n documents carrying no
// persistent index at all. That is the shape the general batched executor
// exists for: every pushdown lane declines, so the query admits and parses the
// whole corpus in worker batches, which is the path whose per-document cost
// this file pins down.
//
// The document is deliberately wide. A batch's scan Segment indexes one
// structural entry per key and per value, its entry arena stops doubling at
// 65,536 entries, and its source arena stops at one mebibyte — so a document of
// six flat fields lets a default 4,096-row batch fit inside one generation of
// each and never exercise the arena recycling at all. Eighteen fields at about
// three hundred bytes puts a batch at roughly 151,000 entries and 1.2 MB of
// source, which crosses both ceilings, which is the case that was silently
// re-buying a megabyte of arena per batch forever. A fixture that fits is a
// fixture that proves nothing about real documents.
func durableScanCorpus(tb testing.TB, n int) *durable.Snapshot {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-scan-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	source := &store.Collection{}
	for i := range n {
		doc := fmt.Appendf(nil,
			`{"id":%d,"bucket":%d,"score":%d,"label":"row-%06d","active":%t,`+
				`"note":"lorem ipsum dolor sit amet consectetur adipiscing elit sed do %d",`+
				`"a1":%d,"a2":%d,"a3":%d,"a4":%d,"a5":%d,"a6":%d,"a7":%d,"a8":%d,`+
				`"b1":"tag-%06d","b2":"tag-%06d","b3":"tag-%06d","b4":"tag-%06d"}`,
			i, i%128, i*3, i, i%3 != 0, i,
			i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i, i%97, i%31, i%7)
		if _, err := source.Put(fmt.Sprintf("key-%07d", i), doc); err != nil {
			tb.Fatal(err)
		}
	}
	options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	if _, err := durable.CreateFrom(source, file, options); err != nil {
		tb.Fatal(err)
	}
	fs, err := durable.Open(file, options)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = fs.Close() })
	snapshot, err := fs.Snapshot()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

// scanAllocsPerRun reports what runs additional warm executions of q over
// snapshot cost, in heap allocations and bytes. It reads MemStats rather than
// using testing.AllocsPerRun because the batched executor runs on scanner and
// worker goroutines, which AllocsPerRun's single-goroutine accounting does not
// see — the very allocations this file is about.
//
// It is a *marginal* measurement: two windows are timed, one of runs executions
// and one of twice that, and the answer is the difference. That is what makes a
// budget of zero meaningful. MemStats is process-wide and the measurement is not
// free of the process — ReadMemStats stops the world, and starting it again can
// cost the runtime a machine descriptor — so a window pays a small fixed cost
// once, whatever it contains. Measured here as exactly one 112-byte allocation
// per window, systematically, on a path that allocates nothing. Subtracting a
// short window from a long one cancels that exactly, while anything an execution
// does survives multiplied by the extra executions.
//
// Reporting the difference rather than a per-execution average is the other half
// of it. Dividing by the run count would turn up to runs-1 real allocations into
// a reported zero, which is how a shape that allocated on nearly every execution
// could pass a test whose message says it allocated on none.
//
// The cheapest of several attempts is taken, because two of the retained buffers
// converge over several executions rather than one: the scan Segment's arena
// free list needs a fill to supersede a generation and a Reset to release it
// before a later fill can draw on it, and a worker's row arena is sized by the
// share of batches that worker wins, which the scheduler redistributes from run
// to run. Both are monotone and bounded, so a path that really allocates nothing
// reaches a zero attempt; a path that allocates per row or per batch has none,
// because every attempt pays for every execution in it.
func scanAllocsPerRun(tb testing.TB, q *Query, snapshot *durable.Snapshot, workers, runs int) (allocs, bytes uint64) {
	tb.Helper()
	e := Exec{Options: ExecOptions{Workers: workers}}
	// The collector is paused for the measurement. A garbage collection landing
	// inside one window and not the other would not cancel, and on a path whose
	// budget is zero that is indistinguishable from the bug. Nothing here
	// allocates when it is warm, so pausing cannot let the heap run away: that
	// is the very property being measured.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	measured := false
	window := func(n int) (uint64, uint64) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range n {
			if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
				tb.Fatal(err)
			}
		}
		runtime.ReadMemStats(&after)
		return after.Mallocs - before.Mallocs, after.TotalAlloc - before.TotalAlloc
	}
	for range 30 {
		// Warm-up first, and untimed. The retained buffers converge over several
		// executions, and a convergence allocation landing in the shorter of the
		// two windows would make the difference negative — which, saturated,
		// reads as a pass. That is not hypothetical: it silently defeated this
		// harness the first time it was written this way.
		for range runs {
			if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
				tb.Fatal(err)
			}
		}
		shortAllocs, shortBytes := window(runs)
		longAllocs, longBytes := window(2 * runs)
		if longAllocs < shortAllocs || longBytes < shortBytes {
			// Doing twice the work cost less, which only a decaying warm-up cost
			// can produce. Discard the attempt rather than record a difference
			// this measurement did not earn.
			continue
		}
		a, b := longAllocs-shortAllocs, longBytes-shortBytes
		if !measured || a < allocs || (a == allocs && b < bytes) {
			allocs, bytes, measured = a, b, true
		}
		if allocs == 0 && bytes == 0 {
			return allocs, bytes
		}
	}
	if !measured {
		tb.Fatal("no window pair converged: every attempt cost less for twice the " +
			"work, so this shape's retained buffers never reached a steady state")
	}
	return allocs, bytes
}

// BenchmarkFileFilteredCountAlloc is the durable backend's counterpart to the
// zero-allocation contract the two in-memory backends already meet: an
// unindexed filtered COUNT over a corpus far larger than any one batch, run
// repeatedly through a single warm Exec. Everything it reports is
// per-execution steady-state cost, so it is a regression detector first and a
// throughput number second.
//
// The three corpus sizes are what separate a fixed per-execution cost from a
// per-batch one: batches scale with documents, so a residual that scales with
// batches shows up as a number that roughly doubles from 50k to 100k while a
// genuinely fixed cost stays flat. Reading one size alone cannot tell the two
// apart.
func BenchmarkFileFilteredCountAlloc(b *testing.B) {
	for _, documents := range []int{10000, 50000, 100000} {
		b.Run(fmt.Sprintf("docs=%d", documents), func(b *testing.B) {
			snapshot := durableScanCorpus(b, documents)
			q := Select(Count()).Where(Cmp("bucket", Lt, 2))
			e := Exec{Options: ExecOptions{Workers: 4}}
			// A generous warm-up, because "warm" here means more than the first
			// pass. The scan Segment's arena free list needs a fill to supersede
			// a generation and a Reset to release it before a later fill can
			// draw on it; the batch ring's slots reach their high-water batch
			// only once the corpus has been walked with every slot in play; and
			// a worker's Segment is minted on the execution that first gives
			// that worker a batch, which on a corpus of fewer batches than
			// workers can be many executions in. The smallest corpus here is
			// three batches against four workers, so the fourth worker is warmed
			// only by repetition.
			for range 64 {
				if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Given a warm Exec, when a durable execution of any shape scans a corpus far
// larger than one batch, then it must allocate nothing at all.
//
// The budget is zero, and that is a contract rather than a micro-optimization.
// Every buffer the path needs is retained by the Exec and reused: the scan
// buffers, the per-worker Workspace, the per-worker scan Segment and its arena
// generations, the per-worker row and group arena, the batch ring, the merge
// frontier, the aggregate accumulators, the result cells — and the scanner, the
// workers, and the four channels between them, which are parked in the Exec's
// pool rather than minted per call. Nothing is left to buy.
//
// Both budgets are checked, because each catches what the other cannot. The
// count catches a small per-row or per-group allocation, which is invisible in
// bytes. The byte budget catches a handful of enormous ones, which is invisible
// in the count: a scan Segment whose entry arena outgrows its ceiling used to
// discard a whole megabyte per batch, which on a 13-batch execution is 25 MB
// hiding behind 25 allocations — a number no sane count budget would ever flag.
// The byte budget is not exactly zero only because MemStats is process-wide and
// picks up whatever the runtime's own goroutines do during the window; it sits
// three orders of magnitude below one arena generation, which is the thing it
// exists to catch.
//
// The shapes differ in what they retain past the batch that produced them, which
// is the whole difficulty: an aggregate keeps a fixed number of accumulators, a
// projection keeps every selected row's detached bytes, an ordered projection
// keeps them and sorts them, and a grouped reduction keeps one key, one scalar
// set, and one accumulator set per distinct group per batch. Each of those was a
// separate allocation site.
func TestFileExecutionSteadyAllocs(t *testing.T) {
	// A corpus of several batches, not a large one: what this test needs is a
	// batch that overflows an arena generation, which is a property of the
	// document width and the batch size, not of the corpus. Six batches wrap the
	// ring and exercise the arena free list across fills, and keep the test
	// affordable to run under the race detector, where every execution is an
	// order of magnitude slower.
	const documents = 25000
	const byteBudget = 4 << 10
	snapshot := durableScanCorpus(t, documents)
	shapes := []struct {
		name string
		q    *Query
	}{
		{"count", Select(Count()).Where(Cmp("bucket", Lt, 2))},
		{"aggregates", Select(Count(), Sum("score"), Min("score"), Max("score"))},
		{"projection", Select(Path("id"), Path("label"), Path("note")).
			Where(Cmp("bucket", Lt, 2))},
		{"ordered-limit", Select(Path("id"), Path("label")).
			Where(Cmp("bucket", Lt, 4)).OrderBy("score", Desc).OrderBy("id", Asc).Limit(25)},
		{"grouped", Select(Path("bucket"), Count(), Sum("score"), Max("score")).
			GroupBy("bucket")},
		// An unordered LIMIT is its own shape and not a weaker ordered one: it
		// keeps the earliest source ordinals, so the consumer trims the frontier
		// as it goes instead of spilling it, and the trim is the only place the
		// executor releases row storage mid-execution.
		{"unordered-limit", Select(Path("id"), Path("label"), Path("note")).Limit(20)},
		// A NOT is here for the candidate planner rather than for the executor.
		// It is the one predicate shape whose planning consults the live-mask
		// capability, which durable does not have — materializing its live-row
		// universe would be page I/O rather than a metadata read — so it is
		// where a capability discovered by type assertion instead of declared
		// by the call site shows up as an allocation per execution. Durable
		// does have the other optional capability, chunk summaries, which this
		// corpus deliberately cannot exercise: its `bucket` values repeat in
		// every chunk, so probes here prune nothing. The pruning shapes are
		// measured on the same harness by zone_file_alloc_test.go. See
		// sourceCaps.
		{"negated", Select(Count()).Where(Not(Cmp("bucket", Eq, 2)))},
	}
	for _, workers := range []int{1, 4} {
		for _, shape := range shapes {
			t.Run(fmt.Sprintf("%s/workers=%d", shape.name, workers), func(t *testing.T) {
				const runs = 6
				allocs, bytes := scanAllocsPerRun(t, shape.q, snapshot, workers, runs)
				t.Logf("%s over %d documents at %d workers: %d allocs, %d B for %d further warm executions",
					shape.name, documents, workers, allocs, bytes, runs)
				if allocs != 0 {
					t.Fatalf("%s at %d workers allocated %d times (%d B) for %d further warm "+
						"executions, budget 0: something on this path is no longer retained "+
						"across executions", shape.name, workers, allocs, bytes, runs)
				}
				if bytes > byteBudget {
					t.Fatalf("%s at %d workers allocated %d B for %d further warm executions, "+
						"budget %d: a few very large allocations are hiding behind a zero count, "+
						"which is what a superseded scan arena generation looks like",
						shape.name, workers, bytes, runs, byteBudget)
				}
			})
		}
	}
}

// Given a batch ring narrow enough to wrap many times, when an ordered
// projection spills and merges, then the durable result must be byte-identical
// to the in-memory one.
//
// This is the ring's safety proof. Batch scratch — scan buffers, the reorder
// slot, the per-batch row headers — is reused by sequence modulo the ring
// length, and the ring is only sound because the credit protocol guarantees a
// sequence has been scanned, reduced, and consumed before its slot comes round
// again. One worker makes the ring three slots wide and a small BatchRows makes
// the corpus wrap it dozens of times, so an off-by-one in that argument
// corrupts a row rather than staying theoretical. The ordering and LIMIT make
// the corruption observable: rows are compared by content, not counted.
func TestRunFileSnapshotBatchRingReuseDifferential(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-ring-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	set := &store.Segment{ShapeTapes: true, Postings: true}
	for i := range 600 {
		doc := fmt.Appendf(nil,
			`{"id":%d,"bucket":%d,"score":%d,"label":"lbl-%03d-%c","active":%t,"esc":"a\nb%d"}`,
			i, i%13, (i*7)%600, i, 'a'+i%26, i%3 != 0, i)
		if _, err := set.Append(doc); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.Put(fmt.Sprintf("key-%04d", i), doc); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	queries := []*Query{
		Select(Path("id"), Path("label"), Path("esc")).OrderBy("label", Desc).OrderBy("id", Asc),
		Select(Path("id"), Path("esc")).Where(Cmp("active", Eq, true)).OrderBy("score", Asc).Limit(37),
		Select(Path("id")).Where(Cmp("bucket", Eq, 5)),
		Select(Path("label"), Count(), Sum("score")).GroupBy("label").OrderBy("label", Asc),
		Select(Count(), Sum("score"), Min("score"), Max("score")).Where(Cmp("score", Ge, 100)),
	}
	// One worker gives the narrowest ring the executor builds, and eleven rows
	// a batch wraps it more than fifty times over this corpus. The Exec is
	// shared across queries so each one runs against slots another query left
	// filled, which is where stale retained storage would surface.
	e := Exec{Options: ExecOptions{
		Workers: 1, BatchRows: 11, BatchBytes: 4 << 10,
		MemoryBytes: 64 << 10, SpillDirectory: t.TempDir(),
	}}
	for pass := range 2 {
		for i, q := range queries {
			want, err := q.Run(FromSegment(set))
			if err != nil {
				t.Fatalf("pass %d query %d baseline: %v", pass, i, err)
			}
			if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
				t.Fatalf("pass %d query %d file execution: %v", pass, i, err)
			}
			if got, expected := resultKey(e.Result), resultKey(want); got != expected {
				t.Fatalf("pass %d query %d mismatch:\n got: %s\nwant: %s", pass, i, got, expected)
			}
			if e.Stats.Batches < 50 {
				t.Fatalf("pass %d query %d ran %d batches, too few to wrap the ring",
					pass, i, e.Stats.Batches)
			}
		}
	}
}
