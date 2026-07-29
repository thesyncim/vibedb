package query

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// poolReuseCorpus builds a durable snapshot and the in-memory Segment holding
// the same documents, so a durable answer can be held against the reference one.
func poolReuseCorpus(t *testing.T, documents int) (*durable.Snapshot, *store.Segment) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "query-file-pool-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	options := durable.Options{Collection: store.Options{ChunkDocuments: 8}}
	source := &store.Collection{}
	set := &store.Segment{ShapeTapes: true, Postings: true}
	for i := range documents {
		doc := fmt.Appendf(nil,
			`{"id":%d,"bucket":%d,"score":%d,"label":"lbl-%04d-%c","active":%t,`+
				`"note":"lorem ipsum dolor sit amet consectetur adipiscing elit %d"}`,
			i, i%13, (i*7)%documents, i, 'a'+i%26, i%3 != 0, i)
		if _, err := set.Append(doc); err != nil {
			t.Fatal(err)
		}
		if _, err := source.Put(fmt.Sprintf("key-%05d", i), doc); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := durable.Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return snapshot, set
}

// poolQueries is the shape set the pool-reuse tests compare on. It spans every
// consumer branch — grouped, single-row, ordered projection, unordered
// projection with a limit — because each one retains different storage past the
// batch that produced it, and a pipeline left dirty by a previous execution
// surfaces in whichever of them runs next.
func poolQueries() []*Query {
	return []*Query{
		Select(Path("id"), Path("label")).Where(Cmp("bucket", Eq, 5)),
		Select(Path("id"), Path("note")).OrderBy("label", Desc).OrderBy("id", Asc).Limit(31),
		Select(Path("label"), Count(), Sum("score")).GroupBy("label").OrderBy("label", Asc),
		Select(Count(), Sum("score"), Min("score"), Max("score")).Where(Cmp("score", Ge, 100)),
		Select(Path("id")).Where(Cmp("active", Eq, true)).Limit(17),
	}
}

// assertPoolDrained checks the pool's channels directly. It is a white-box
// assertion on purpose: the balance argument the reused channels rest on is
// what makes an execution's results its own, and a leftover credit or partial
// corrupts the next execution rather than failing, so nothing downstream would
// report it.
func assertPoolDrained(t *testing.T, e *Exec, when string) {
	t.Helper()
	pool := e.file.pool
	if pool == nil {
		t.Fatalf("%s: no pool was built", when)
	}
	if n := len(pool.jobs); n != 0 {
		t.Fatalf("%s: %d batches left queued", when, n)
	}
	if n := len(pool.partials); n != 0 {
		t.Fatalf("%s: %d partials left queued", when, n)
	}
	if n := len(pool.credits); n != 0 {
		t.Fatalf("%s: %d credits left outstanding", when, n)
	}
	if n := len(pool.scanDone); n != 0 {
		t.Fatalf("%s: %d scan results left queued", when, n)
	}
}

// Given an execution that aborts partway, when the same Exec is used again,
// then the pipeline it shares must be empty and every later answer exact.
//
// This is the abort path the reused channels make dangerous, exercised rather
// than reasoned about. A spill directory that does not exist makes the first
// ordered projection fail on its first spill, which cancels the scan with
// batches already queued, partials already in flight, and credits already taken
// — precisely the state in which a design that abandoned its queues would leave
// one behind. The queries that follow run against the same channels, and their
// results are compared row by row against the in-memory backend, so a leftover
// partial consumed as if it belonged to them is a mismatch and not merely a
// count that looks plausible.
func TestRunFileSnapshotAbortedExecutionLeavesPipelineClean(t *testing.T) {
	snapshot, set := poolReuseCorpus(t, 900)
	e := Exec{Options: ExecOptions{
		Workers: 4, BatchRows: 16, BatchBytes: 2 << 10,
		MemoryBytes:    64 << 10,
		SpillDirectory: filepath.Join(t.TempDir(), "no-such-directory"),
	}}
	aborting := Select(Path("id"), Path("label"), Path("note")).OrderBy("label", Asc)
	// Three aborts in a row: one abort leaving the pipeline dirty would be
	// enough to corrupt the next execution, and an abort that only misbehaves
	// after a previous abort would survive a single-shot test.
	for pass := range 3 {
		if err := aborting.RunInto(&e, FromFile(snapshot)); err == nil {
			t.Fatalf("pass %d: the spill was expected to fail", pass)
		}
		assertPoolDrained(t, &e, fmt.Sprintf("after abort %d", pass))
	}
	e.Options.SpillDirectory = t.TempDir()
	for i, q := range poolQueries() {
		want, err := q.Run(FromSegment(set))
		if err != nil {
			t.Fatalf("query %d baseline: %v", i, err)
		}
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatalf("query %d after aborts: %v", i, err)
		}
		if got, expected := resultKey(e.Result), resultKey(want); got != expected {
			t.Fatalf("query %d after aborts:\n got: %s\nwant: %s", i, got, expected)
		}
		assertPoolDrained(t, &e, fmt.Sprintf("after query %d", i))
	}
}

// Given one Exec run at several different worker counts, when each execution
// finishes, then every answer must be identical to the in-memory backend's and
// the shared pipeline must come back empty.
//
// Changing the worker count is the one thing that rebuilds the pool's channels,
// and the credit channel's capacity is what the batch ring's length is derived
// from — so a width change that left the two disagreeing would let a scanner run
// further ahead than there are slots to hold it, overwriting a batch still being
// reduced. The counts go up and down, not just up, because shrinking is the
// direction in which a high-water capacity would silently survive.
func TestRunFileSnapshotWorkerCountChangeDifferential(t *testing.T) {
	snapshot, set := poolReuseCorpus(t, 900)
	e := Exec{Options: ExecOptions{BatchRows: 11, BatchBytes: 4 << 10,
		MemoryBytes: 64 << 10, SpillDirectory: t.TempDir()}}
	queries := poolQueries()
	for _, workers := range []int{1, 4, 2, 8, 1, 3} {
		e.Options.Workers = workers
		for i, q := range queries {
			want, err := q.Run(FromSegment(set))
			if err != nil {
				t.Fatalf("workers %d query %d baseline: %v", workers, i, err)
			}
			if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
				t.Fatalf("workers %d query %d: %v", workers, i, err)
			}
			if got, expected := resultKey(e.Result), resultKey(want); got != expected {
				t.Fatalf("workers %d query %d mismatch:\n got: %s\nwant: %s",
					workers, i, got, expected)
			}
			if slots, credits := len(e.file.slots), cap(e.file.pool.credits); slots != credits+1 {
				t.Fatalf("workers %d: ring is %d slots for a credit bound of %d; "+
					"the ring must be one longer than the bound or a slot returns "+
					"before its sequence has been consumed", workers, slots, credits)
			}
			assertPoolDrained(t, &e, fmt.Sprintf("workers %d query %d", workers, i))
		}
	}
}

// Given an Exec released and then reused, when it runs again, then it must
// rebuild its pool and answer exactly as before.
//
// Release retires the parked goroutines, which is the one operation that can
// leave the Exec holding a pool whose goroutines have exited. Reusing it after
// that must build a new one rather than wake the dead.
func TestRunFileSnapshotReleasedExecRebuildsPool(t *testing.T) {
	snapshot, set := poolReuseCorpus(t, 300)
	e := Exec{Options: ExecOptions{Workers: 3, BatchRows: 13}}
	q := Select(Path("id"), Path("label")).Where(Cmp("bucket", Eq, 5)).OrderBy("id", Asc)
	want, err := q.Run(FromSegment(set))
	if err != nil {
		t.Fatal(err)
	}
	for pass := range 3 {
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if got, expected := resultKey(e.Result), resultKey(want); got != expected {
			t.Fatalf("pass %d mismatch:\n got: %s\nwant: %s", pass, got, expected)
		}
		e.Release()
		if e.file.pool != nil {
			t.Fatalf("pass %d: Release left the pool in place", pass)
		}
	}
}

// Given an unordered projection under a LIMIT over a corpus where almost every
// row matches, when the execution finishes, then the storage it retained must
// be bounded by the limit rather than by the corpus.
//
// The rows a partial hands over are packed into the worker's arena, and a worker
// arena is append-only for as long as the consumer still holds rows in it — so
// the trim that keeps the frontier at the limit only bounds memory if the bytes
// behind the discarded rows are given back too. They cannot be given back by the
// consumer, which does not own that storage, so the trim re-detaches the
// survivors into its own pair of arenas and asks the workers to abandon theirs.
// Without that step this shape retains every row it ever selected, which for a
// LIMIT over a large corpus is the whole projection: the exact unbounded
// frontier the trim exists to prevent, reintroduced one level down.
func TestRunFileUnorderedLimitBoundsRetainedRows(t *testing.T) {
	const documents = 50000
	const limit = 10
	snapshot, set := poolReuseCorpus(t, documents)
	q := Select(Path("id"), Path("label"), Path("note")).Limit(limit)
	e := Exec{Options: ExecOptions{Workers: 4, BatchRows: 64}}
	want, err := q.Run(FromSegment(set))
	if err != nil {
		t.Fatal(err)
	}
	for pass := range 3 {
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if got, expected := resultKey(e.Result), resultKey(want); got != expected {
			t.Fatalf("pass %d mismatch:\n got: %s\nwant: %s", pass, got, expected)
		}
	}
	held := 0
	for i := range e.file.arenas {
		held += e.file.arenas[i].retained()
	}
	for i := range e.file.compact {
		held += cap(e.file.compact[i].bytes)
	}
	// One document projects about a hundred bytes across the three columns, so
	// retaining the scan is 1.2 MB here and grows with the corpus, while the
	// bounded form measures 16 kB and does not. The budget sits between them
	// with room for the batch and worker geometry to move.
	const budget = 64 << 10
	t.Logf("row arenas hold %d B after projecting %d documents under LIMIT %d",
		held, documents, limit)
	if held > budget {
		t.Fatalf("row arenas hold %d B after a LIMIT %d over %d documents, budget %d: "+
			"the trim is dropping row headers without the bytes behind them",
			held, limit, documents, budget)
	}
}
