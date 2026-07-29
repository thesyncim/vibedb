package query

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// durableJoinOptions is the collection geometry the durable join differential
// runs against.
//
// ChunkDocuments is 2 for the reason buildJoinDatabase uses it on the heap
// side: the mask walk, the scan's batch boundaries, and the inner side's own
// batching all have to cross a chunk edge on collections this small, and a
// single-chunk collection would exercise none of those seams.
//
// Asynchronous visibility is enabled. These tests compare read paths; every one of them writes
// its corpus, reads it back within the same process, and throws the directory
// away, so an fsync per Put buys nothing the test asserts and costs enough to
// force the sweep below to be smaller than it should be. Durability itself is
// covered by store/durable's own reliability and crash tests.
func durableJoinOptions(indexes ...store.IndexDefinition) durable.Options {
	return durable.Options{
		Collection:       store.Options{ChunkDocuments: 2},
		Indexes:          indexes,
		PageSize:         4096,
		MaxPageSize:      64 << 10,
		ResidentBytes:    4 << 20,
		MaxDocumentBytes: 64 << 10,
		MaxKeyBytes:      128,
		InlineValueBytes: 512,
		ReadConcurrency:  2,
		PrefetchQueue:    8,
		BufferCount:      1024,
		QueueSlots:       4,
		GroupLimit:       2,
		Backend:          durable.BackendPortable,

		MaxBatchDocuments: 1,
		MaxSnapshotLeases: 64,
		MaxRetiredExtents: 4096,
	}
}

// buildDurableJoinDatabase writes the same corpus buildJoinDatabase writes into
// a heap catalog, into a durable database instead. Taking the identical inputs
// is what makes the two comparable row for row.
func buildDurableJoinDatabase(t testing.TB, outerDocs []string, innerRows []int, index bool) *durable.Database {
	t.Helper()
	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	outerOptions := durableJoinOptions()
	if index {
		outerOptions = durableJoinOptions(store.IndexDefinition{Name: "ref", Paths: []string{"/ref"}})
	}
	outer, err := db.CreateCollection("outer", outerOptions)
	if err != nil {
		t.Fatalf("CreateCollection(outer): %v", err)
	}
	for i, doc := range outerDocs {
		if _, err := outer.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			t.Fatalf("Put(o%d, %s): %v", i, doc, err)
		}
	}
	inner, err := db.CreateCollection("inner", durableJoinOptions())
	if err != nil {
		t.Fatalf("CreateCollection(inner): %v", err)
	}
	for _, r := range innerRows {
		if _, err := inner.Put(innerJoinPool[r].key, []byte(innerJoinPool[r].doc)); err != nil {
			t.Fatalf("Put(%s): %v", innerJoinPool[r].key, err)
		}
	}
	return db
}

// durableJoinOuterShapes are the driving corpora the differential sweeps.
//
// The heap sweep enumerates every sequence of outer documents because a heap
// collection is cheap to build; a durable one is a file, two directory trees,
// and a commit per Put, so the same enumeration would take hours. These are
// chosen to keep what that enumeration was buying: every value shape in the
// pool appears, the collection sizes straddle the two-document chunk boundary
// so the last chunk is both full and partial, and the orderings differ so a
// given join key lands at a different chunk and slot in different runs.
var durableJoinOuterShapes = [][]string{
	outerJoinPool,
	{outerJoinPool[0]},
	{outerJoinPool[0], outerJoinPool[1], outerJoinPool[2]},
	{outerJoinPool[8], outerJoinPool[6], outerJoinPool[3], outerJoinPool[0]},
	{outerJoinPool[5], outerJoinPool[4], outerJoinPool[7], outerJoinPool[1], outerJoinPool[2]},
}

// TestExhaustiveDurableJoinDifferential is TestExhaustiveJoinDifferential's
// claim carried across the backend boundary: the same query over the same data
// must return the same rows, in the same order, whether both collections came
// from a heap catalog or from a durable database.
//
// It compares against the heap executor rather than against the naive reference
// the heap sweep uses, and that is deliberate. The heap result is already
// pinned to the reference by TestExhaustiveJoinDifferential over a far larger
// input space than this can afford; chaining to it makes this test's job the
// narrow one it should be — proving the durable inner scan, the durable probe,
// and the parallel binding split introduce no divergence — rather than
// re-deriving semantics that are already established.
//
// Every axis the heap sweep varies that can change a durable answer is varied
// here: both binding strategies, the declared outer index that lowers a
// membership to a mask intersection, and a worker count above one, because the
// durable filter phase is parallel by construction and a binding shared wrong
// across workers is exactly the bug this has to catch.
func TestExhaustiveDurableJoinDifferential(t *testing.T) {
	inners := enumerateInnerCollections(len(innerJoinPool), joinIterations(2, 1))
	if testing.Short() {
		// Lookup mode activates only after the collected primary-key set grows
		// past JoinMembershipMax=1. The short subset sweep otherwise contains
		// at most one row and makes its own non-vacuity assertion impossible.
		// One deterministic pair crosses that seam without expanding short CI
		// to every two-row combination.
		inners = append(inners, []int{0, 1})
	}
	battery := joinBattery()
	workerCounts := []int{1, 4}

	checks := 0
	var bound, probed ExecStats
	for _, outerDocs := range durableJoinOuterShapes {
		for _, innerRows := range inners {
			for _, indexed := range joinIndexModes {
				heap := buildJoinDatabase(t, outerDocs, innerRows, indexed)
				heapCatalog := heap.Snapshot()
				files := buildDurableJoinDatabase(t, outerDocs, innerRows, indexed)
				fileCatalog, err := files.Snapshot()
				if err != nil {
					t.Fatalf("durable Snapshot: %v", err)
				}

				for _, strategy := range joinStrategies {
					for _, workers := range workerCounts {
						var heapExec, fileExec Exec
						heapExec.Options.JoinMembershipMax = strategy.membershipMax
						fileExec.Options.JoinMembershipMax = strategy.membershipMax
						fileExec.Options.Workers = workers
						for qi, q := range battery {
							if err := q.RunInto(&heapExec, FromDatabase(heapCatalog, "outer")); err != nil {
								t.Fatalf("heap query %d: %v", qi, err)
							}
							if err := q.RunInto(&fileExec, FromFileDatabase(fileCatalog, "outer")); err != nil {
								t.Fatalf("durable query %d %s workers=%d indexed=%v over %v / %v: %v",
									qi, strategy.name, workers, indexed, outerDocs, innerRows, err)
							}
							if diff := diffResults(fileExec.Result, heapExec.Result); diff != "" {
								t.Fatalf("query %d %s workers=%d indexed=%v over %v / %v: durable and heap disagree: %s",
									qi, strategy.name, workers, indexed, outerDocs, innerRows, diff)
							}
							bound.JoinMemberships += fileExec.Stats.JoinMemberships
							bound.JoinKeys += fileExec.Stats.JoinKeys
							probed.JoinLookups += fileExec.Stats.JoinLookups
							probed.JoinProbes += fileExec.Stats.JoinProbes
							probed.JoinFilters += fileExec.Stats.JoinFilters
							probed.JoinFilterRejected += fileExec.Stats.JoinFilterRejected
							checks++
						}
					}
				}
				_ = fileCatalog.Close()
			}
		}
	}

	// The same non-vacuity assertions the heap sweep makes. A differential that
	// only ever bound one strategy would compare two spellings of the same code
	// path and prove nothing about the other.
	if bound.JoinMemberships == 0 || bound.JoinKeys == 0 {
		t.Fatalf("no durable membership binding was ever exercised: %+v", bound)
	}
	if probed.JoinLookups == 0 || probed.JoinProbes == 0 {
		t.Fatalf("no durable lookup binding was ever exercised: %+v", probed)
	}
	t.Logf("durable strategies exercised: %d membership bindings collecting %d values; "+
		"%d lookup bindings performing %d probes, %d of them carrying a filter "+
		"that rejected %d rows outright",
		bound.JoinMemberships, bound.JoinKeys, probed.JoinLookups, probed.JoinProbes,
		probed.JoinFilters, probed.JoinFilterRejected)
	t.Logf("durable join differential: %d outer shapes × %d inner sets × %d index modes × "+
		"%d strategies × %d worker counts × %d queries = %d checks",
		len(durableJoinOuterShapes), len(inners), len(joinIndexModes),
		len(joinStrategies), len(workerCounts), len(battery), checks)
}

// TestDurableJoinInnerScanSpansManyBatchesUnderEviction pins the copy-out
// discipline on the inner scan's keys and documents.
//
// A durable scan's key and value slices borrow a page lease that is released
// when the callback returns, so everything kept past the callback must be
// copied. The differential above cannot catch a violation of that: its
// collections hold fewer rows than joinBatchRows, so the scan drains exactly
// once, after every page it read is still resident — borrowing the page's bytes
// is indistinguishable from copying them. Removing the copy passes that sweep
// unchanged, which was verified rather than assumed.
//
// This closes the gap from both ends. The inner side is several batches wide,
// so a drain runs while later pages are still to be read, and the resident
// budget is small enough that the pages an earlier batch borrowed from are
// evicted and their frames handed to a later read. A binding that kept a
// borrowed view would then be reading another document's bytes as a join key.
func TestDurableJoinInnerScanSpansManyBatchesUnderEviction(t *testing.T) {
	const (
		outerRows = 1500
		innerRows = 4000
	)
	options := durableJoinOptions()
	// Tight enough that the corpus cannot stay resident, so a frame borrowed by
	// an early batch is genuinely recycled before the scan ends. Three MiB also
	// retains the point writer's complete worst-case free-image fold: the
	// collection must be able to publish any admitted point mutation even
	// though this read test deliberately keeps its cache far below the corpus.
	options.ResidentBytes = 3 << 20
	// Four documents per chunk with a two-kilobyte document makes a chunk page
	// about eight kilobytes, so two consecutive 1024-row batches span roughly
	// four megabytes of pages against a three-megabyte resident budget. That
	// ratio is the point: the frames an early page was read into are handed to a
	// later read before the scan finishes, so a borrowed key is reading another
	// document's bytes.
	options.Collection = store.Options{ChunkDocuments: 4}

	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{Options: options})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer func() { _ = db.Close() }()
	heap := &store.Database{}

	outer, err := db.CreateCollection("outer", options)
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	heapOuter, err := heap.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatalf("heap CreateCollection: %v", err)
	}
	for i := range outerRows {
		key := fmt.Sprintf("o%04d", i)
		doc := fmt.Sprintf(`{"id":%d,"ref":"k%04d"}`, i, i%innerRows)
		if _, err := outer.Put(key, []byte(doc)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := heapOuter.Put(key, []byte(doc)); err != nil {
			t.Fatalf("heap Put: %v", err)
		}
	}

	inner, err := db.CreateCollection("inner", options)
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	heapInner, err := heap.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatalf("heap CreateCollection: %v", err)
	}
	for i := range innerRows {
		key := fmt.Sprintf("k%04d", i)
		// Half the inner rows are rejected by the clause's own filter, so the
		// collected set is a genuine subset and a corrupted key cannot be
		// masked by everything matching anyway.
		tier := "free"
		if i%2 == 0 {
			tier = "pro"
		}
		doc := fmt.Sprintf(`{"tier":%q,"seat":%d,"pad":%q}`, tier, i, innerJoinPadding)
		if _, err := inner.Put(key, []byte(doc)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := heapInner.Put(key, []byte(doc)); err != nil {
			t.Fatalf("heap Put: %v", err)
		}
	}

	catalog, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = catalog.Close() }()
	heapCatalog := heap.Snapshot()

	q := Select(Path("id")).
		Join(JoinOn("inner", "ref", JoinKey).Where(Cmp("tier", Eq, "pro"))).
		OrderBy("id", Asc)

	for _, strategy := range joinStrategies {
		var fileExec, heapExec Exec
		fileExec.Options.JoinMembershipMax = strategy.membershipMax
		heapExec.Options.JoinMembershipMax = strategy.membershipMax
		if err := q.RunInto(&heapExec, FromDatabase(heapCatalog, "outer")); err != nil {
			t.Fatalf("%s heap: %v", strategy.name, err)
		}
		if err := q.RunInto(&fileExec, FromFileDatabase(catalog, "outer")); err != nil {
			t.Fatalf("%s durable: %v", strategy.name, err)
		}
		// Half the outer rows reference an even-numbered inner key, so a result
		// anywhere near zero or near outerRows would mean the join collapsed
		// rather than ran.
		if got := fileExec.Result.RowCount; got != outerRows/2 {
			t.Fatalf("%s: %d rows, want %d", strategy.name, got, outerRows/2)
		}
		if diff := diffResults(fileExec.Result, heapExec.Result); diff != "" {
			t.Fatalf("%s: durable and heap disagree: %s", strategy.name, diff)
		}
	}
}

// TestDurableJoinRejectsASingleFileSource pins that a joined query cannot be
// run against one durable snapshot. There is no catalog behind FromFile to
// resolve the inner collection from, and resolving it from a second,
// independently taken snapshot is exactly the skew a join must not be able to
// express.
func TestDurableJoinRejectsASingleFileSource(t *testing.T) {
	files := buildDurableJoinDatabase(t, outerJoinPool, []int{0, 1}, false)
	outer, _ := files.Collection("outer")
	snapshot, err := outer.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = snapshot.Close() }()

	q := Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey))
	var e Exec
	err = q.RunInto(&e, FromFile(snapshot))
	if err == nil {
		t.Fatal("a joined query ran against a single durable snapshot")
	}
	if got := err.Error(); !contains(got, "FromFileDatabase") {
		t.Fatalf("error %q does not point at the constructor that works", got)
	}
}

// TestDurableJoinNamesAnAbsentCollection pins that an inner collection missing
// from the cut is an error rather than an empty join.
func TestDurableJoinNamesAnAbsentCollection(t *testing.T) {
	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer func() { _ = db.Close() }()
	outer, err := db.CreateCollection("outer", durableJoinOptions())
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := outer.Put("o1", []byte(`{"id":1,"ref":"k1"}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	catalog, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = catalog.Close() }()

	q := Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey))
	var e Exec
	if err := q.RunInto(&e, FromFileDatabase(catalog, "outer")); err == nil {
		t.Fatal("a join against an uncataloged collection succeeded")
	}

	var missing Exec
	if err := q.RunInto(&missing, FromFileDatabase(catalog, "absent")); err == nil {
		t.Fatal("a query naming an uncataloged driving collection succeeded")
	}
}

// TestDurableFanOutJoinIsRefused pins that an aliased clause — SQL's inner join,
// which projects joined columns — is rejected rather than silently answered as
// a semi-join. Returning the driving rows without their joined columns would be
// a different query, and a wrong one.
func TestDurableFanOutJoinIsRefused(t *testing.T) {
	files := buildDurableJoinDatabase(t, outerJoinPool, []int{0, 1, 2}, false)
	catalog, err := files.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = catalog.Close() }()

	q := Select(Path("id"), Path("i.tier")).
		Join(JoinOn("inner", "ref", JoinKey).As("i"))
	var e Exec
	err = q.RunInto(&e, FromFileDatabase(catalog, "outer"))
	if err == nil {
		t.Fatal("a fan-out join ran against a durable database")
	}
	if got := err.Error(); !contains(got, "does not yet materialize") {
		t.Fatalf("error %q does not name the missing capability", got)
	}
}

func TestDurableJoinBatchIsBoundedBeforeDocumentArenaGrowth(t *testing.T) {
	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	outer, err := db.CreateCollection("outer", durableJoinOptions())
	if err != nil {
		t.Fatalf("CreateCollection(outer): %v", err)
	}
	if _, err := outer.Put(
		"outer-0",
		[]byte(`{"id":"outer-0","join":"x","ref":"inner-0"}`),
	); err != nil {
		t.Fatalf("Put(outer): %v", err)
	}
	inner, err := db.CreateCollection("inner", durableJoinOptions())
	if err != nil {
		t.Fatalf("CreateCollection(inner): %v", err)
	}
	// The raw document is small enough for the collection but structurally
	// wider than a 64 KiB execution can index in its reusable join batch.
	bomb := `{"join":"x","items":[` + strings.Repeat("0,", 2048) + `0]}`
	if _, err := inner.Put("inner-0", []byte(bomb)); err != nil {
		t.Fatalf("Put(inner): %v", err)
	}
	catalog, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Cleanup(func() { _ = catalog.Close() })

	q := Select(Path("id")).Join(JoinOn("inner", "join", "join"))
	exec := Exec{Options: ExecOptions{
		Workers:     1,
		MemoryBytes: minimumHeapMemoryBytes,
		ResultRows:  -1,
		ResultBytes: -1,
	}}
	err = q.RunInto(&exec, FromFileDatabase(catalog, "outer"))
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
		t.Fatalf("durable hostile join error = %v, want WorkBudgetError", err)
	}
	if len(exec.Workspace.joins) != 1 {
		t.Fatalf("join bindings = %d, want 1", len(exec.Workspace.joins))
	}
	side := &exec.Workspace.joins[0].file
	if side.batchRows != 0 || len(side.data) != 0 || side.docs.Len() != 0 {
		t.Fatalf(
			"rejected durable batch grew rows=%d data=%d docs=%d",
			side.batchRows,
			len(side.data),
			side.docs.Len(),
		)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("rejected durable join exposed %d result rows", exec.Result.RowCount)
	}

	primary := Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey))
	lookup := Exec{Options: ExecOptions{
		Workers:             1,
		MemoryBytes:         minimumHeapMemoryBytes,
		JoinMembershipMax:   1 << 20,
		JoinFilterScanRatio: -1,
		ResultRows:          -1,
		ResultBytes:         -1,
	}}
	if err := primary.RunInto(&lookup, FromFileDatabase(catalog, "outer")); err != nil {
		t.Fatalf("durable primary-key fallback: %v", err)
	}
	if lookup.Result.RowCount != 1 || lookup.Stats.JoinLookups != 1 {
		t.Fatalf(
			"durable fallback rows=%d stats=%+v, want one lookup match",
			lookup.Result.RowCount,
			lookup.Stats,
		)
	}
	binding := &lookup.Workspace.joins[0]
	if cap(binding.lits) != 0 ||
		cap(binding.file.data) != 0 ||
		cap(binding.file.ends) != 0 ||
		cap(binding.file.keys) != 0 ||
		binding.file.docs.Len() != 0 ||
		cap(binding.bloom.blocks) != 0 {
		t.Fatalf(
			"durable fallback retained abandoned capacities: lits=%d data=%d ends=%d keys=%d docs=%d bloom=%d",
			cap(binding.lits),
			cap(binding.file.data),
			cap(binding.file.ends),
			cap(binding.file.keys),
			binding.file.docs.Len(),
			cap(binding.bloom.blocks),
		)
	}
}

// durableJoinCorpus builds a two-collection durable database of the given size
// by bulk-loading each collection from a heap one and then opening the
// directory. Bulk loading is one commit per collection rather than one per
// document, which is what makes a corpus large enough to be worth measuring
// cheap enough to build in a test.
//
// It writes the files under the names OpenDatabase expects and then discovers
// them, so the layout convention is exercised rather than bypassed.
func durableJoinCorpus(tb testing.TB, outerRows, innerRows int) *durable.Database {
	tb.Helper()
	dir := tb.TempDir()
	options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}

	load := func(name string, fill func(*store.Collection)) {
		source := &store.Collection{}
		fill(source)
		file, err := os.Create(filepath.Join(dir, name+".vjc"))
		if err != nil {
			tb.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		if _, err := durable.CreateFrom(source, file, options); err != nil {
			tb.Fatal(err)
		}
	}

	load("orders", func(c *store.Collection) {
		for i := range outerRows {
			doc := fmt.Appendf(nil, `{"id":%d,"customer":"c%06d","total":%d}`,
				i, i%innerRows, i%997)
			if _, err := c.Put(fmt.Sprintf("o%07d", i), doc); err != nil {
				tb.Fatal(err)
			}
		}
	})
	load("customers", func(c *store.Collection) {
		for i := range innerRows {
			tier := "free"
			if i%2 == 0 {
				tier = "pro"
			}
			doc := fmt.Appendf(nil, `{"tier":%q,"seat":%d}`, tier, i)
			if _, err := c.Put(fmt.Sprintf("c%06d", i), doc); err != nil {
				tb.Fatal(err)
			}
		}
	})

	db, err := durable.OpenDatabase(dir, durable.DatabaseOptions{Options: options})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	if db.Len() != 2 {
		tb.Fatalf("OpenDatabase discovered %d collections, want 2", db.Len())
	}
	return db
}

// joinAllocsPerRun reports the heap allocations one warm joined execution
// costs. It reads MemStats rather than using testing.AllocsPerRun for the
// reason scanAllocsPerRun gives: the batched executor allocates on worker and
// scanner goroutines, which single-goroutine accounting does not see.
//
// It uses the same marginal-window method as scanAllocsPerRun: after warming,
// the cost of runs executions is subtracted from the cost of twice as many.
// That cancels fixed ReadMemStats/runtime noise without dividing away a real
// allocation that happens in fewer than runs executions. GC is paused and
// decaying window pairs are rejected before the cheapest converged pair wins.
//
// The catalog is taken once, outside the measured region. That is deliberate
// and is what the number means: a database snapshot allocates one *Snapshot per
// collection, exactly as a single-collection Collection.Snapshot does, and
// charging that to the join would measure snapshot acquisition rather than join
// execution. A caller in a hot loop reuses one catalog across executions, or
// refreshes it with SnapshotInto, which is why that is the shape measured.
func joinAllocsPerRun(tb testing.TB, q *Query, catalog durable.DatabaseSnapshot, e *Exec, runs int) (allocs, bytes uint64) {
	tb.Helper()
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	measured := false
	run := func() {
		if err := q.RunInto(e, FromFileDatabase(catalog, "orders")); err != nil {
			tb.Fatal(err)
		}
	}
	window := func(n int) (uint64, uint64) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range n {
			run()
		}
		runtime.ReadMemStats(&after)
		return after.Mallocs - before.Mallocs, after.TotalAlloc - before.TotalAlloc
	}
	for range 30 {
		for range runs {
			run()
		}
		shortAllocs, shortBytes := window(runs)
		longAllocs, longBytes := window(2 * runs)
		if longAllocs < shortAllocs || longBytes < shortBytes {
			continue
		}
		a, b := longAllocs-shortAllocs, longBytes-shortBytes
		if !measured || a < allocs || a == allocs && b < bytes {
			allocs, bytes, measured = a, b, true
		}
		if allocs == 0 && bytes == 0 {
			return allocs, bytes
		}
	}
	if !measured {
		tb.Fatal("no join allocation window pair converged")
	}
	return allocs, bytes
}

// Given a warm Exec, when a durable semi-join runs repeatedly, then it must
// allocate nothing at all.
//
// Zero is the assertion, not a budget, and that is what changed when the pooled
// executor landed. An unjoined durable execution used to cost a fixed eighteen
// to twenty-one allocations of per-execution channels and goroutine frames, and
// this test inherited that as its budget; the pool parks its scanner and its
// workers instead of minting them, so the residual is gone and a budget of
// sixty-four would now pass while a per-probe allocation crept back in
// underneath it. A join adds a bound inner side and a per-worker probe scratch,
// every buffer of which is retained by the Exec's Workspace and refilled in
// place, so the join's own contribution to that total is nothing and the test
// should say so exactly.
//
// Bytes are reported but not asserted. runtime.MemStats is process-wide; the
// paired windows and bounded retry remove fixed noise, while the malloc count
// is the quantity a retained-buffer regression moves.
//
// Both strategies are measured because they allocate in different places. A
// membership binding grows a collected set and a needle list; a lookup binding
// grows a Bloom filter and a per-worker copy-out buffer for the documents it
// probes. A regression in either would be invisible in the other. Both worker
// counts are measured because the pool retains per-worker state across
// executions, so a buffer that is per-execution rather than retained shows up
// as a count that scales with workers.
func TestDurableJoinSteadyStateAllocs(t *testing.T) {
	const (
		outerRows = 20000
		innerRows = 4000
	)
	db := durableJoinCorpus(t, outerRows, innerRows)
	catalog, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = catalog.Close() }()

	q := Select(Count()).Where(Cmp("total", Ge, 0)).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))

	for _, strategy := range joinStrategies {
		for _, workers := range []int{1, 4} {
			e := Exec{Options: ExecOptions{
				Workers:           workers,
				JoinMembershipMax: strategy.membershipMax,
			}}
			allocs, bytes := joinAllocsPerRun(t, q, catalog, &e, 8)
			t.Logf("%s workers=%d: %d marginal allocs, %d B across 8 executions (%d bound)",
				strategy.name, workers, allocs, bytes,
				e.Stats.JoinMemberships+e.Stats.JoinLookups)
			if allocs != 0 {
				t.Errorf("%s workers=%d allocated %d times (%d B) across 8 marginal executions, want 0: "+
					"the join is allocating per execution, per row, or per probe again",
					strategy.name, workers, allocs, bytes)
			}
		}
	}
}

// Given an Exec reused for an unjoined query after a joined one, when the
// second execution runs, then no parked worker's evaluator still points at the
// first execution's join bindings.
//
// This pins the one line in runFileSnapshotBatched that passes nil rather than
// the Workspace's retained bindings when the plan has no joins, and it is a
// retention contract rather than a correctness one — which is exactly why it
// needs a test of its own. An unjoined plan compiles no predInBound leaf, so a
// stale binding is never read and no result is ever wrong; what happens instead
// is that every parked worker keeps a live reference to the previous
// execution's collected set, Bloom filter, and inner snapshot, and through the
// snapshot to a generation lease that blocks the inner collection's extent
// reclaimer. Nothing observable goes wrong until a writer fills
// MaxRetiredExtents and starts failing.
//
// The hazard is specific to this backend and became live when the executor
// began parking its workers. A pool that mints fresh goroutines and Workspaces
// per execution drops the stale reference on its own; one that retains them
// across executions by design holds it until something explicitly clears it.
//
// It is written white-box, reading the evaluators directly, because the
// property is unobservable through the public surface by construction. A test
// that could see it through a Result would be testing something else.
func TestDurableJoinBindingsAreNotRetainedByAnUnjoinedQuery(t *testing.T) {
	db := durableJoinCorpus(t, 2000, 500)
	catalog, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = catalog.Close() }()

	joined := Select(Count()).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
	unjoined := Select(Count()).Where(Cmp("total", Ge, 0))

	e := Exec{Options: ExecOptions{Workers: 4}}
	if err := joined.RunInto(&e, FromFileDatabase(catalog, "orders")); err != nil {
		t.Fatalf("joined: %v", err)
	}
	bound := 0
	for i := range e.file.workers {
		if e.file.workers[i].eval.binds != nil {
			bound++
		}
	}
	if bound == 0 {
		t.Fatal("no worker was bound by the joined execution: the test cannot " +
			"show that the next one unbinds them")
	}

	if err := unjoined.RunInto(&e, FromFile(mustDrivingSnapshot(t, db))); err != nil {
		t.Fatalf("unjoined: %v", err)
	}
	for i := range e.file.workers {
		if e.file.workers[i].eval.binds != nil {
			t.Fatalf("worker %d still points at the joined execution's bindings "+
				"after an unjoined query: the inner collection and its generation "+
				"lease stay pinned for the Exec's lifetime", i)
		}
	}
}

// mustDrivingSnapshot opens a single-collection snapshot of the driving side,
// for the unjoined half of the test above. It is registered for close on the
// test rather than returned with a closer because a durable snapshot left open
// would trip the collection's own close check.
func mustDrivingSnapshot(t *testing.T, db *durable.Database) *durable.Snapshot {
	t.Helper()
	orders, ok := db.Collection("orders")
	if !ok {
		t.Fatal("orders is not cataloged")
	}
	snapshot, err := orders.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

// innerJoinPadding widens each inner document so a chunk page holds few of
// them, which is what makes one inner batch span more pages than the resident
// budget can hold.
var innerJoinPadding = func() string {
	pad := make([]byte, 2048)
	for i := range pad {
		pad[i] = 'x'
	}
	return string(pad)
}()

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
