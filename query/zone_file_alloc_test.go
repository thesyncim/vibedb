package query

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// The durable chunk-summary probe is on the query path, so it is held to the
// same contract as everything else there: a warm execution allocates nothing
// that scales with the collection, and probing the summaries allocates nothing
// at all.
//
// The distinction matters because the probe walks a radix tree with a
// callback, and a closure that escapes would put its captured mask buffer,
// counter, and decoded summary on the heap once per directory page rather than
// once per process. That is exactly the shape of regression a plain
// correctness test cannot see.

// zoneAllocCorpus is a clustered page-backed corpus with no declared index, so
// every predicate below reaches the chunk-summary tier rather than an index.
func zoneAllocCorpus(tb testing.TB, documents int) *durable.Snapshot {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-zone-alloc-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	source := &store.Collection{}
	for i := range documents {
		// Three members, which is exactly the durable summary's path budget, and
		// the third is carried by only the last tenth of the corpus so that
		// EXISTS and IS NULL have chunks to separate. Without a sparse member
		// every chunk carries every path and the presence bits — the only
		// predicates no index in this store can bound — would never prune.
		doc := fmt.Appendf(nil, `{"id":%d,"score":%d}`, i, i*3)
		if i >= documents-documents/10 {
			doc = fmt.Appendf(nil, `{"id":%d,"score":%d,"tail":%d}`, i, i*3, i)
		}
		if _, err := source.Put(fmt.Sprintf("key-%07d", i), doc); err != nil {
			tb.Fatal(err)
		}
	}
	options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	if _, err := durable.CreateFrom(source, file, options); err != nil {
		tb.Fatal(err)
	}
	collection, err := durable.Open(file, options)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = collection.Close() })
	snapshot, err := collection.Snapshot()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

// Given a durable snapshot carrying chunk summaries, when the summaries are
// probed repeatedly with a reused mask buffer, then the probe allocates
// nothing.
func TestFileZoneProbeAllocFree(t *testing.T) {
	snapshot := zoneAllocCorpus(t, 20000)
	code, ok := store.ZoneCodeNumber([]byte("1000"))
	if !ok {
		t.Fatal("literal has no zone code")
	}
	probe := store.ZoneProbe{Path: store.ZonePathHash("id"), Lo: code, Hi: code, Op: store.ZoneOpEq}
	masks := make([]store.Mask, 0, 4096)
	// Warm: the first probe fills the page cache with the directory pages.
	if out, pruned, bounded := snapshot.AppendZoneMasks(masks[:0], probe); !bounded || pruned == 0 || len(out) == 0 {
		t.Fatalf("probe did not prune: %d masks, %d pruned, bounded %v", len(out), pruned, bounded)
	}
	allocs := testing.AllocsPerRun(50, func() {
		if _, _, bounded := snapshot.AppendZoneMasks(masks[:0], probe); !bounded {
			t.Fatal("probe stopped pruning")
		}
	})
	if allocs != 0 {
		t.Fatalf("AppendZoneMasks allocated %.1f times per probe", allocs)
	}
}

// Given a warm Exec, when a durable query that actually prunes runs
// repeatedly, then it allocates nothing at all.
//
// This is TestFileExecutionSteadyAllocs's contract applied to the one thing
// that harness's corpus cannot reach. Its `bucket` values repeat in every
// chunk, so its probes walk the summaries, prune nothing, and report
// unbounded — which exercises the walk and the decode but never the branch
// that appends a surviving chunk's mask, intersects it with an index mask, or
// hands a narrowed mask set to the scanner. The corpus here is clustered and
// the predicate selects 0.1% of it, so every one of those runs on every
// execution.
//
// It borrows scanAllocsPerRun deliberately rather than reading MemStats
// directly: an earlier version of this test divided a single window by its run
// count and reported 2 allocations at 20k documents and 4 at 100k on a path
// that allocates nothing, because a process-wide counter over an undivided
// window is mostly the runtime's own goroutines. The differenced,
// collector-paused, cheapest-of-thirty form is what makes a budget of zero
// mean anything.
func TestFilePrunedQuerySteadyAllocs(t *testing.T) {
	const documents = 25000
	const byteBudget = 4 << 10
	snapshot := zoneAllocCorpus(t, documents)
	shapes := []struct {
		name string
		q    *Query
	}{
		// A pruned count: the mask set is narrow and the scanner reads a
		// handful of chunks.
		{"pruned-count", Select(Count()).Where(Cmp("id", Lt, 100))},
		// A pruned projection retains detached row bytes past the batch that
		// produced them, which is a different retention shape from an
		// aggregate's fixed accumulators.
		{"pruned-projection", Select(Path("id"), Path("score")).Where(Cmp("id", Lt, 100))},
		// A disjunction runs the leaf probe twice and unions the two mask sets,
		// which is the only shape that draws a second buffer out of the
		// Workspace's mask pool.
		{"pruned-or", Select(Count()).Where(Or(Cmp("id", Lt, 100), Cmp("id", Ge, 24900)))},
		// EXISTS and IS NULL are answered by the presence bits alone and by
		// nothing else in the planner, so they reach the tier by a path no
		// comparison does. The sparse member makes EXISTS keep a tenth of the
		// chunks; IS NULL on a member every document carries prunes all of
		// them, which is the empty-mask-set path and its own retention shape.
		{"pruned-exists", Select(Count()).Where(Exists("tail"))},
		{"pruned-is-null", Select(Count()).Where(IsNull("id"))},
	}
	for _, workers := range []int{1, 4} {
		for _, shape := range shapes {
			t.Run(fmt.Sprintf("%s/workers=%d", shape.name, workers), func(t *testing.T) {
				// Non-vacuity first: a shape that stopped pruning would meet the
				// budget trivially and prove nothing.
				e := Exec{Options: ExecOptions{Workers: workers}}
				if err := shape.q.RunInto(&e, FromFile(snapshot)); err != nil {
					t.Fatal(err)
				}
				if e.Workspace.zonePruned == 0 {
					t.Fatalf("%s pruned no chunk: the allocation budget is vacuous", shape.name)
				}
				const runs = 6
				allocs, bytes := scanAllocsPerRun(t, shape.q, snapshot, workers, runs)
				t.Logf("%s over %d documents at %d workers: %d allocs, %d B for %d further warm executions",
					shape.name, documents, workers, allocs, bytes, runs)
				if allocs != 0 {
					t.Fatalf("%s at %d workers allocated %d times (%d B) for %d further warm "+
						"executions, budget 0: the chunk-summary tier is buying something "+
						"per execution", shape.name, workers, allocs, bytes, runs)
				}
				if bytes > byteBudget {
					t.Fatalf("%s at %d workers allocated %d B for %d further warm executions, "+
						"budget %d", shape.name, workers, bytes, runs, byteBudget)
				}
			})
		}
	}
}
