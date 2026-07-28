package query

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Differential evidence for chunk-summary pruning on the durable backend.
//
// zone_differential_test.go makes the same argument for the heap backend and
// this file deliberately reuses its query battery, its adversarial corpus, its
// predicate generator, and its cell-for-cell comparison, so the two backends
// are held to one standard rather than two. What differs is everything the
// durable summary does differently: it is 30 bytes rather than 144, it tracks
// three paths rather than eight, it stores 24-bit rather than 32-bit codes,
// and it survives a process restart. Each of those is a way to be too narrow,
// and a summary that is too narrow drops rows silently.
//
// The switch is store.SetZonePruning, exactly as on the heap, and it is read
// on both the write and the read path here: with it off nothing is folded and
// nothing is written, so a store built with it off has no summaries at all.
// The differentials below therefore always *build* with pruning on and only
// toggle it for the two reads, which is what makes them a comparison of two
// plans over one set of stored bytes rather than of two stores.

func zoneFileCollection(t testing.TB, chunkDocuments int, docs [][]byte) *durable.Snapshot {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "query-zone-file-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: chunkDocuments},
	})
	if err != nil {
		t.Fatalf("durable.Create: %v", err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	for i, doc := range docs {
		if _, err := collection.Put(fmt.Sprintf("k%06d", i), doc); err != nil {
			t.Fatalf("Put %d (%s): %v", i, doc, err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

// zoneFileBatchedCollection builds the same corpus through the batched Update
// path. It is a separate construction path rather than a convenience: a batch
// rebuilds one chunk page per touched chunk and folds several documents into
// one summary in a single commit, which is a different maintenance sequence
// from the one-document-per-commit path above and has its own way of losing a
// document's contribution.
func zoneFileBatchedCollection(t testing.TB, chunkDocuments int, docs [][]byte) *durable.Snapshot {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "query-zone-file-batch-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: chunkDocuments},
	})
	if err != nil {
		t.Fatalf("durable.Create: %v", err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	if err := collection.Update(func(batch *durable.WriteBatch) error {
		for i, doc := range docs {
			if err := batch.Put(fmt.Sprintf("k%06d", i), doc); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

// zoneFileReopenedCollection builds through Put, closes the collection, and
// reopens it. Every summary the reopened snapshot probes was read back off
// disk rather than carried in memory, which is the only way to test that the
// encoding round-trips through a real file.
func zoneFileReopenedCollection(t testing.TB, chunkDocuments int, docs [][]byte) *durable.Snapshot {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "query-zone-file-reopen-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	options := durable.Options{
		Collection: store.Options{ChunkDocuments: chunkDocuments},
	}
	collection, err := durable.Create(file, options)
	if err != nil {
		t.Fatalf("durable.Create: %v", err)
	}
	for i, doc := range docs {
		if _, err := collection.Put(fmt.Sprintf("k%06d", i), doc); err != nil {
			t.Fatalf("Put %d (%s): %v", i, doc, err)
		}
	}
	if err := collection.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := durable.Open(file, options)
	if err != nil {
		t.Fatalf("durable.Open: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

var zoneFileConstructors = []struct {
	name  string
	build func(testing.TB, int, [][]byte) *durable.Snapshot
}{
	{"put", zoneFileCollection},
	{"batch", zoneFileBatchedCollection},
	{"reopen", zoneFileReopenedCollection},
}

// zoneFileRunBothWays is zoneRunBothWays against a durable snapshot: one query
// body, one snapshot, two executions, and an assertion that they agree cell for
// cell in the same order.
func zoneFileRunBothWays(t testing.TB, q *Query, snapshot *durable.Snapshot) (Result, int) {
	t.Helper()

	previous := store.SetZonePruning(true)
	var pruned Exec
	prunedErr := q.RunInto(&pruned, FromFile(snapshot))
	skipped := pruned.Workspace.zonePruned

	store.SetZonePruning(false)
	var plain Exec
	plainErr := q.RunInto(&plain, FromFile(snapshot))
	store.SetZonePruning(previous)

	if (prunedErr == nil) != (plainErr == nil) {
		t.Fatalf("pruned error %v, unpruned error %v", prunedErr, plainErr)
	}
	if prunedErr != nil {
		if prunedErr.Error() != plainErr.Error() {
			t.Fatalf("pruned error %q, unpruned error %q", prunedErr, plainErr)
		}
		return Result{}, skipped
	}
	if plain.Workspace.zonePruned != 0 {
		t.Fatalf("pruning reported %d skipped chunks while disabled", plain.Workspace.zonePruned)
	}
	if diff := zoneCompareResults(pruned.Result, plain.Result); diff != "" {
		t.Fatalf("pruned and unpruned results differ (%d chunks skipped): %s", skipped, diff)
	}
	return pruned.Result, skipped
}

// Given randomly assembled durable collections drawn from the shared
// adversarial corpus and randomly assembled predicates, when each query runs
// both ways, then the results are identical.
func TestZonePruningFileFuzzDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xF11E, 0x2ABC))
	pruningRuns, executions := 0, 0
	// Each iteration builds its own durable collection, and on the io_uring
	// backend every open collection pins a fixed-buffer ring. Sixty of them
	// held open at once — which is what a flat loop does, because build()
	// defers Close through t.Cleanup to the end of the *test* — exhausts the
	// ring/locked-memory budget on Linux (io_uring_setup: ENOMEM) before a
	// single differential is ever compared. Scoping each iteration to a
	// subtest runs that iteration's cleanups the moment it returns, so at most
	// one collection is open at a time and the comparison is what runs, not the
	// allocator.
	for iteration := 0; iteration < 60; iteration++ {
		t.Run(fmt.Sprintf("iteration-%02d", iteration), func(t *testing.T) {
			count := 1 + rng.IntN(20)
			docs := make([][]byte, count)
			narrow := rng.IntN(2) == 0
			base := rng.IntN(len(zoneAdversarialDocs))
			for i := range docs {
				if narrow {
					docs[i] = []byte(zoneAdversarialDocs[(base+rng.IntN(3))%len(zoneAdversarialDocs)])
				} else {
					docs[i] = []byte(zoneAdversarialDocs[rng.IntN(len(zoneAdversarialDocs))])
				}
			}
			chunkDocuments := []int{1, 2, 3, 8, 64}[rng.IntN(5)]
			constructor := zoneFileConstructors[rng.IntN(len(zoneFileConstructors))]
			snapshot := constructor.build(t, chunkDocuments, docs)

			for probe := 0; probe < 8; probe++ {
				q := Select(Path("v"), Path("w")).Where(zoneFuzzPredicates(rng))
				if rng.IntN(3) == 0 {
					q = Select(Count(), Sum("v"), Min("v"), Max("v")).Where(zoneFuzzPredicates(rng))
				}
				executions++
				if _, skipped := zoneFileRunBothWays(t, q, snapshot); skipped > 0 {
					pruningRuns++
				}
			}
		})
	}
	if pruningRuns == 0 {
		t.Fatal("no execution pruned a chunk: the durable fuzz differential is vacuous")
	}
	t.Logf("durable zone fuzz differential: %d executions, %d of them pruned at least one chunk",
		executions, pruningRuns)
}

// Given the shared query battery over every construction path, when each query
// runs both ways over a durable snapshot, then the results are identical and
// both equal the independent reference executor's.
func TestZonePruningFileBatteryDifferential(t *testing.T) {
	battery := queryBattery()
	docs := make([][]byte, 0, len(zoneAdversarialDocs))
	for _, doc := range zoneAdversarialDocs {
		docs = append(docs, []byte(doc))
	}
	pruningRuns := 0
	// One subtest per (constructor, chunk size) so each durable collection's
	// io_uring ring is closed before the next is built. Held open together they
	// overrun the Linux ring/locked-memory budget (io_uring_setup: ENOMEM); see
	// the note in TestZonePruningFileFuzzDifferential.
	for _, constructor := range zoneFileConstructors {
		for _, chunkDocuments := range []int{1, 4, 64} {
			t.Run(fmt.Sprintf("%s/chunk-%d", constructor.name, chunkDocuments), func(t *testing.T) {
				snapshot := constructor.build(t, chunkDocuments, docs)
				for _, q := range battery {
					if _, skipped := zoneFileRunBothWays(t, q, snapshot); skipped > 0 {
						pruningRuns++
					}
				}
			})
		}
	}
	if pruningRuns == 0 {
		t.Fatal("no execution pruned a chunk: the durable battery differential is vacuous")
	}
}

// Given a durable collection whose chunks hold disjoint value ranges, when a
// range predicate straddling them runs, then pruning skips exactly the chunks
// that cannot match and the answer is unchanged. Its shuffled twin is the
// unfavourable case and is asserted just as loudly: min/max proves nothing
// when every chunk spans the whole range, and the tier must decline rather
// than pretend.
func TestZonePruningFileSkipsDisjointChunks(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 256; i++ {
		docs = append(docs, fmt.Appendf(nil, `{"v":%d}`, i))
	}
	snapshot := zoneFileCollection(t, 64, docs) // four chunks: 0-63, 64-127, ...

	q := Select(Path("v")).Where(Cmp("v", Ge, 200))
	result, skipped := zoneFileRunBothWays(t, q, snapshot)
	if result.RowCount != 56 {
		t.Fatalf("rows: got %d want 56", result.RowCount)
	}
	if skipped != 3 {
		t.Fatalf("skipped chunks: got %d want 3", skipped)
	}

	shuffled := make([][]byte, len(docs))
	copy(shuffled, docs)
	rng := rand.New(rand.NewPCG(1, 2))
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	shuffledSnapshot := zoneFileCollection(t, 64, shuffled)
	shuffledResult, shuffledSkipped := zoneFileRunBothWays(t, q, shuffledSnapshot)
	if shuffledResult.RowCount != 56 {
		t.Fatalf("shuffled rows: got %d want 56", shuffledResult.RowCount)
	}
	t.Logf("durable clustered skipped %d/4 chunks, shuffled skipped %d/4", skipped, shuffledSkipped)
}

// Given a durable collection where one chunk never carries a path, another
// always carries it as null, and a third mixes present and absent, when EXISTS
// and IS NULL run, then the presence bits prune the chunks that cannot match.
// These are the two predicates no index in this store can bound.
func TestZonePruningFilePresenceBits(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 64; i++ {
		docs = append(docs, []byte(`{"v":1}`))
	}
	for i := 0; i < 64; i++ {
		docs = append(docs, []byte(`{"other":1}`))
	}
	for i := 0; i < 64; i++ {
		docs = append(docs, []byte(`{"v":null}`))
	}
	for i := 0; i < 32; i++ {
		docs = append(docs, []byte(`{"v":2}`))
		docs = append(docs, []byte(`{"other":2}`))
	}
	snapshot := zoneFileCollection(t, 64, docs)

	exists, existsSkipped := zoneFileRunBothWays(t, Select(Path("v")).Where(Exists("v")), snapshot)
	if exists.RowCount != 160 {
		t.Fatalf("EXISTS rows: got %d want 160", exists.RowCount)
	}
	if existsSkipped != 1 {
		t.Fatalf("EXISTS skipped: got %d want 1 (the all-absent chunk)", existsSkipped)
	}

	isNull, isNullSkipped := zoneFileRunBothWays(t, Select(Path("v")).Where(IsNull("v")), snapshot)
	if isNull.RowCount != 160 {
		t.Fatalf("IS NULL rows: got %d want 160", isNull.RowCount)
	}
	if isNullSkipped != 1 {
		t.Fatalf("IS NULL skipped: got %d want 1 (the always-present, never-null chunk)", isNullSkipped)
	}
}
